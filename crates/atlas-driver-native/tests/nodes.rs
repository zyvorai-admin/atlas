// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! `RealNativeDriver` against an in-process atlas-native cluster (three metadata, three data
//! nodes on localhost).

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use atlas_api_types::{
    CloneSnapshotRequest, CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest,
    DeleteVolumeRequest, ExpandVolumeRequest, Health, VolumeKind,
};
use atlas_driver_core::{DriverError, StorageDriver};
use atlas_driver_native::{HttpApi, HttpApiConfig, RealNativeDriver, POOL_NAME};
use atlas_native::node::{
    DataNodeRole, DataNodeSpec, Listeners, MetadataRole, NativeNode, NodeConfig,
};

const TOKEN: &str = "driver-test-token";

fn bind() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

struct Cluster {
    _td: tempfile::TempDir,
    _data: Vec<NativeNode>,
    meta: Vec<NativeNode>,
}

impl Cluster {
    fn start() -> Self {
        let td = tempfile::tempdir().unwrap();
        std::fs::write(td.path().join("token"), TOKEN).unwrap();
        let base = |id: &str| NodeConfig {
            node_id: id.into(),
            data_dir: td.path().join(id),
            http_listen: "127.0.0.1:0".parse().unwrap(),
            api_token_file: Some(td.path().join("token")),
            tls: None,
            http_tls: None,
            data_node: None,
            metadata: None,
            max_request_bytes: 1 << 20,
        };
        let data_l: Vec<(String, TcpListener)> =
            (1..=3).map(|i| (format!("d{i}"), bind())).collect();
        let meta_l: Vec<(String, TcpListener)> =
            (1..=3).map(|i| (format!("m{i}"), bind())).collect();
        let specs: Vec<DataNodeSpec> = data_l
            .iter()
            .map(|(id, l)| DataNodeSpec {
                id: id.clone(),
                addr: l.local_addr().unwrap().to_string(),
                zone: None,
                rack: None,
                host: None,
                free_bytes: 1 << 30,
            })
            .collect();
        let peers: BTreeMap<String, String> = meta_l
            .iter()
            .map(|(id, l)| (id.clone(), l.local_addr().unwrap().to_string()))
            .collect();
        let data = data_l
            .into_iter()
            .map(|(id, l)| {
                let mut cfg = base(&id);
                cfg.data_node = Some(DataNodeRole {
                    listen: l.local_addr().unwrap(),
                });
                let listeners = Listeners {
                    http: Some(bind()),
                    data_node: Some(l),
                    metadata: None,
                };
                NativeNode::start_with(cfg, listeners).unwrap()
            })
            .collect();
        let meta = meta_l
            .into_iter()
            .map(|(id, l)| {
                let mut cfg = base(&id);
                cfg.metadata = Some(MetadataRole {
                    listen: l.local_addr().unwrap(),
                    peers: peers.clone(),
                    bootstrap: None,
                    data_nodes: specs.clone(),
                    replicas: 3,
                    extent_bytes: 4096,
                    tick_ms: 10,
                    proposal_timeout_ms: 3000,
                    repair_interval_secs: 0,
                    gc_interval_secs: 0,
                });
                let listeners = Listeners {
                    http: Some(bind()),
                    data_node: None,
                    metadata: Some(l),
                };
                NativeNode::start_with(cfg, listeners).unwrap()
            })
            .collect();
        Self {
            _td: td,
            _data: data,
            meta,
        }
    }

    fn endpoints(&self) -> Vec<String> {
        self.meta
            .iter()
            .map(|n| format!("http://{}", n.http_addr()))
            .collect()
    }
}

fn driver(endpoints: Vec<String>, token: &str) -> RealNativeDriver {
    let api = HttpApi::new(HttpApiConfig {
        endpoints,
        token: Some(token.into()),
        ca_pem: None,
        identity_pem: None,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    RealNativeDriver::new("bkd_native", api)
}

async fn wait_healthy(d: &RealNativeDriver) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(h) = d.health().await {
            if h.status == Health::Ok {
                return;
            }
        }
        assert!(Instant::now() < deadline, "cluster never became healthy");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn create(name: &str) -> CreateVolumeRequest {
    CreateVolumeRequest {
        tenant_id: "t".into(),
        name: name.into(),
        size_bytes: 1 << 20,
        kind: VolumeKind::Block,
        policy: None,
        pool: None,
        owner: None,
        kubernetes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_driver_manages_volumes_on_a_live_cluster() {
    let c = Cluster::start();
    // A dead endpoint first: the driver moves on to the live ones.
    let mut endpoints = vec![format!("http://{}", bind().local_addr().unwrap())];
    endpoints.extend(c.endpoints());
    let d = driver(endpoints, TOKEN);
    assert!(!d.is_fixture());
    wait_healthy(&d).await;

    // Followers answer mutations with 421; the driver retries until the leader takes it.
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(d.create_volume(create(&format!("vol{i}"))).await.unwrap());
    }
    let disc = d.discover().await.unwrap();
    assert_eq!(disc.health.status, Health::Ok);
    assert_eq!(disc.pools[0].name, POOL_NAME);
    assert_eq!(disc.pools[0].replica_size, Some(3));
    assert_eq!(disc.volumes.len(), 4);
    let v = disc
        .volumes
        .iter()
        .find(|v| v.id == ids[0].volume_id)
        .expect("created volume is discovered");
    assert_eq!(
        v.backend_native_id.as_deref(),
        Some(ids[0].backend_native_id.as_str())
    );
    assert_eq!(v.size_bytes, 1 << 20);
    assert_eq!(v.used_bytes, Some(0));

    let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
    d.write_volume(&ids[1].volume_id, 1000, data.clone())
        .await
        .unwrap();
    let back = d
        .read_volume(&ids[1].volume_id, 1000, data.len() as u64)
        .await
        .unwrap();
    assert_eq!(back, data);
    assert_eq!(
        d.read_volume(&ids[1].volume_id, 0, 8).await.unwrap(),
        vec![0u8; 8]
    );
    match d.read_volume(&ids[1].volume_id, 1 << 20, 1).await {
        Err(DriverError::Backend(m)) => assert!(m.starts_with("invalid"), "{m}"),
        other => panic!("read past the end: {other:?}"),
    }

    let snap = d
        .create_snapshot(CreateSnapshotRequest {
            volume_id: ids[0].volume_id.clone(),
            name: "s1".into(),
        })
        .await
        .unwrap();
    d.expand_volume(ExpandVolumeRequest {
        volume_id: ids[0].volume_id.clone(),
        new_size_bytes: 2 << 20,
    })
    .await
    .unwrap();
    assert!(matches!(
        d.expand_volume(ExpandVolumeRequest {
            volume_id: ids[0].volume_id.clone(),
            new_size_bytes: 4096,
        })
        .await,
        Err(DriverError::Backend(_))
    ));
    let clone = d
        .clone_snapshot(CloneSnapshotRequest {
            snapshot_id: snap.snapshot_id.clone(),
            new_volume_name: "copy".into(),
            size_bytes: Some(3 << 20),
        })
        .await
        .unwrap();
    let sizes: BTreeMap<String, i64> = d
        .list_volumes(POOL_NAME)
        .await
        .unwrap()
        .into_iter()
        .map(|v| (v.id, v.size_bytes))
        .collect();
    assert_eq!(sizes[&ids[0].volume_id], 2 << 20);
    assert_eq!(sizes[&clone.volume_id], 3 << 20);
    match d
        .clone_snapshot(CloneSnapshotRequest {
            snapshot_id: "snap_native_missing".into(),
            new_volume_name: "x".into(),
            size_bytes: None,
        })
        .await
    {
        Err(DriverError::Backend(m)) => assert!(m.starts_with("not found"), "{m}"),
        other => panic!("clone of a missing snapshot: {other:?}"),
    }
    d.delete_volume(DeleteVolumeRequest {
        volume_id: clone.volume_id,
    })
    .await
    .unwrap();
    d.delete_snapshot(DeleteSnapshotRequest {
        snapshot_id: snap.snapshot_id,
    })
    .await
    .unwrap();
    d.delete_volume(DeleteVolumeRequest {
        volume_id: ids[0].volume_id.clone(),
    })
    .await
    .unwrap();
    assert_eq!(d.list_volumes(POOL_NAME).await.unwrap().len(), 3);
    assert!(matches!(
        d.delete_volume(DeleteVolumeRequest {
            volume_id: ids[0].volume_id.clone(),
        })
        .await,
        Err(DriverError::Backend(_))
    ));

    let metrics = d.metrics().await.unwrap();
    let up = metrics
        .iter()
        .find(|m| m.name == "native_data_nodes_up")
        .unwrap();
    assert_eq!(up.value, 3.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_token_and_dead_cluster_are_unreachable() {
    let c = Cluster::start();
    let d = driver(c.endpoints(), "wrong");
    assert!(matches!(d.health().await, Err(DriverError::Unreachable(_))));

    let dead = driver(
        vec![format!("http://{}", bind().local_addr().unwrap())],
        TOKEN,
    );
    assert!(matches!(
        dead.discover().await,
        Err(DriverError::Unreachable(_))
    ));
}
