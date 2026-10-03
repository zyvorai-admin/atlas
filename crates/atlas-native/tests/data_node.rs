// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use atlas_native::{
    BlockStore, DataNodeServer, EngineConfig, FailureDomain, MetaBackend, NativeEngine,
    NativeError, Node, RaftConfig, RaftError, RaftServer, RemoteDevice,
};

const TICK: Duration = Duration::from_millis(10);
const WAIT: Duration = Duration::from_secs(15);
const IO_TIMEOUT: Duration = Duration::from_secs(2);

fn bind() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn node(i: usize) -> Node {
    Node {
        id: format!("n{i}"),
        failure_domain: FailureDomain {
            zone: "z1".into(),
            rack: format!("r{i}"),
            host: format!("h{i}"),
        },
        free_bytes: 1 << 30,
        healthy: true,
    }
}

struct DataNodes {
    servers: Vec<Option<DataNodeServer>>,
    addrs: Vec<SocketAddr>,
}

impl DataNodes {
    fn start(root: &Path, n: usize) -> Self {
        let servers: Vec<_> = (1..=n)
            .map(|i| {
                DataNodeServer::start(format!("n{i}"), root.join(format!("dn{i}")), bind()).unwrap()
            })
            .collect();
        let addrs = servers.iter().map(DataNodeServer::local_addr).collect();
        Self {
            servers: servers.into_iter().map(Some).collect(),
            addrs,
        }
    }

    fn stores(&self) -> Vec<(Node, Arc<dyn BlockStore>)> {
        self.addrs
            .iter()
            .enumerate()
            .map(|(i, a)| {
                (
                    node(i + 1),
                    Arc::new(RemoteDevice::new(*a, IO_TIMEOUT)) as Arc<dyn BlockStore>,
                )
            })
            .collect()
    }
}

fn cfg(root: &Path) -> EngineConfig {
    let mut c = EngineConfig::new(root);
    c.extent_bytes = 4096;
    c
}

#[test]
fn remote_device_round_trip_and_bounds() {
    let td = tempfile::tempdir().unwrap();
    let dn = DataNodeServer::start("n1", td.path(), bind()).unwrap();
    let dev = RemoteDevice::new(dn.local_addr(), IO_TIMEOUT);

    assert!(dev.is_empty().unwrap());
    assert_eq!(dev.append(1, b"hello").unwrap(), 0);
    assert_eq!(dev.append(1, b"world").unwrap(), 5);
    dev.write_at(1, 0, b"HELLO").unwrap();
    assert_eq!(dev.read_exact_at(0, 10).unwrap(), b"HELLOworld");
    assert_eq!(dev.len().unwrap(), 10);

    assert!(matches!(
        dev.write_at(1, 8, b"xyz"),
        Err(NativeError::Remote(_))
    ));
    assert!(matches!(
        dev.read_exact_at(8, 4),
        Err(NativeError::Remote(_))
    ));
    // The connection survives server-side errors.
    assert_eq!(dev.read_exact_at(5, 5).unwrap(), b"world");

    let m = dn.render_metrics().unwrap();
    assert!(m.contains("atlas_native_data_requests_total{node=\"n1\",op=\"append\"} 2"));
    assert!(m.contains("atlas_native_data_device_bytes{node=\"n1\"} 10"));
}

#[test]
fn stale_fence_is_rejected_and_survives_restart() {
    let td = tempfile::tempdir().unwrap();
    let mut dn = DataNodeServer::start("n1", td.path(), bind()).unwrap();
    let dev = RemoteDevice::new(dn.local_addr(), IO_TIMEOUT);

    dev.append(3, &[1u8; 8]).unwrap();
    dev.write_at(5, 0, &[2u8; 8]).unwrap();
    assert_eq!(dn.fence(), 5);
    assert!(matches!(
        dev.write_at(3, 0, &[9u8; 8]),
        Err(NativeError::Fenced { current: 5 })
    ));
    assert!(matches!(
        dev.append(4, &[9u8; 8]),
        Err(NativeError::Fenced { current: 5 })
    ));
    assert_eq!(dev.read_exact_at(0, 8).unwrap(), vec![2u8; 8]);

    dn.shutdown();
    drop(dn);
    let dn = DataNodeServer::start("n1", td.path(), bind()).unwrap();
    assert_eq!(dn.fence(), 5);
    let dev = RemoteDevice::new(dn.local_addr(), IO_TIMEOUT);
    assert!(matches!(
        dev.write_at(4, 0, &[9u8; 8]),
        Err(NativeError::Fenced { current: 5 })
    ));
    assert_eq!(dev.read_exact_at(0, 8).unwrap(), vec![2u8; 8]);
    assert!(dn
        .render_metrics()
        .unwrap()
        .contains("atlas_native_data_fence{node=\"n1\"} 5"));
}

#[test]
fn engine_over_remote_data_nodes_survives_a_lost_replica() {
    let td = tempfile::tempdir().unwrap();
    let mut dns = DataNodes::start(td.path(), 3);
    let e = NativeEngine::open_with(
        cfg(&td.path().join("meta")),
        dns.stores(),
        MetaBackend::Local,
    )
    .unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, &[1u8; 4096]).unwrap();
    e.write(&v, 4096, &[2u8; 4096]).unwrap();
    e.write(&v, 0, &[3u8; 4096]).unwrap();
    assert_eq!(e.gc_once().unwrap().reclaimed, 1);
    e.write(&v, 0, &[4u8; 4096]).unwrap();
    for i in 1..=3 {
        assert_eq!(e.device_len(&format!("n{i}")).unwrap(), 3 * 4096);
    }

    dns.servers[0].take().unwrap().shutdown();
    assert_eq!(e.read(&v, 0, 4096).unwrap(), vec![4u8; 4096]);
    assert_eq!(e.read(&v, 4096, 4096).unwrap(), vec![2u8; 4096]);
    // Three replicas are required and only two data nodes remain.
    assert!(e.write(&v, 0, &[5u8; 4096]).is_err());
}

struct RaftGroup {
    _td: tempfile::TempDir,
    dns: DataNodes,
    raft_addrs: BTreeMap<String, SocketAddr>,
    engines: BTreeMap<String, Option<NativeEngine>>,
}

impl RaftGroup {
    fn new() -> Self {
        let td = tempfile::tempdir().unwrap();
        let dns = DataNodes::start(td.path(), 3);
        let ids: Vec<String> = (1..=3).map(|i| format!("m{i}")).collect();
        let listeners: BTreeMap<String, TcpListener> =
            ids.iter().map(|id| (id.clone(), bind())).collect();
        let raft_addrs: BTreeMap<String, SocketAddr> = listeners
            .iter()
            .map(|(id, l)| (id.clone(), l.local_addr().unwrap()))
            .collect();
        let mut engines = BTreeMap::new();
        for (id, l) in listeners {
            let peers: BTreeMap<String, SocketAddr> = raft_addrs
                .iter()
                .filter(|(p, _)| **p != id)
                .map(|(p, a)| (p.clone(), *a))
                .collect();
            let rcfg = RaftConfig::new(
                id.clone(),
                peers.keys().cloned().collect(),
                td.path().join(&id).join("raft"),
            );
            let server = Arc::new(RaftServer::start(rcfg, l, peers, TICK).unwrap());
            let e = NativeEngine::open_with(
                cfg(&td.path().join(&id).join("engine")),
                dns.stores(),
                MetaBackend::Raft {
                    server,
                    timeout: Duration::from_secs(5),
                },
            )
            .unwrap();
            engines.insert(id, Some(e));
        }
        Self {
            _td: td,
            dns,
            raft_addrs,
            engines,
        }
    }

    fn live(&self) -> impl Iterator<Item = (&String, &NativeEngine)> {
        self.engines
            .iter()
            .filter_map(|(id, e)| e.as_ref().map(|e| (id, e)))
    }

    /// Runs `op` against each live engine until one (the leader) accepts it.
    fn on_leader<T>(
        &self,
        mut op: impl FnMut(&NativeEngine) -> Result<T, NativeError>,
    ) -> (String, T) {
        let deadline = Instant::now() + WAIT;
        loop {
            for (id, e) in self.live() {
                match op(e) {
                    Ok(v) => return (id.clone(), v),
                    Err(NativeError::Raft(
                        RaftError::NotLeader { .. }
                        | RaftError::LeadershipLost { .. }
                        | RaftError::TermChanged { .. }
                        | RaftError::Timeout { .. },
                    )) => {}
                    Err(other) => panic!("{id}: {other}"),
                }
            }
            assert!(
                Instant::now() < deadline,
                "no leader accepted the operation"
            );
            thread::sleep(TICK);
        }
    }

    fn wait_readable(&self, id: &str, v: &str, offset: u64, want: &[u8]) {
        let e = self.engines[id].as_ref().unwrap();
        let deadline = Instant::now() + WAIT;
        loop {
            if e.read(v, offset, want.len()).ok().as_deref() == Some(want) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{id} never saw the write at {offset}"
            );
            thread::sleep(TICK);
        }
    }
}

#[test]
fn raft_engines_share_data_nodes_across_leader_failover() {
    let mut g = RaftGroup::new();
    assert_eq!(g.raft_addrs.len(), 3);
    let (leader, v) = g.on_leader(|e| e.create_volume("v", 8192));
    g.on_leader(|e| e.write(&v, 0, &[7u8; 4096]));
    for id in g.raft_addrs.keys() {
        g.wait_readable(id, &v, 0, &[7u8; 4096]);
    }

    let follower = g
        .live()
        .map(|(id, _)| id.clone())
        .find(|id| *id != leader)
        .unwrap();
    let err = g.engines[&follower]
        .as_ref()
        .unwrap()
        .write(&v, 0, &[0u8; 4096])
        .unwrap_err();
    assert!(
        matches!(err, NativeError::Raft(RaftError::NotLeader { .. })),
        "follower write: {err}"
    );

    let old_term = g.dns.servers[0].as_ref().unwrap().fence();
    assert!(
        old_term > 0,
        "leader writes must carry its term as the fence"
    );

    // Dropping the engine drops the last handle to its RaftServer, which shuts it down.
    g.engines.insert(leader.clone(), None);
    let (new_leader, ()) = g.on_leader(|e| e.write(&v, 4096, &[8u8; 4096]));
    assert_ne!(new_leader, leader);
    let new_term = g.dns.servers[0].as_ref().unwrap().fence();
    assert!(
        new_term > old_term,
        "fence did not advance: {old_term} -> {new_term}"
    );

    // Overwrite and GC on the new leader reuse space freed by the old one.
    g.on_leader(|e| e.write(&v, 0, &[9u8; 4096]));
    // A retry after `LeadershipLost` can commit a write twice or split a GC pass across leaders,
    // so count reclaims until nothing is left rather than expecting exactly one pass.
    let mut reclaimed = 0;
    loop {
        let (_, st) = g.on_leader(|e| e.gc_once());
        reclaimed += st.reclaimed;
        if st.candidates == 0 {
            break;
        }
    }
    assert!(reclaimed >= 1, "the overwritten extent was never reclaimed");
    for id in g.live().map(|(id, _)| id.clone()).collect::<Vec<_>>() {
        g.wait_readable(&id, &v, 0, &[9u8; 4096]);
        g.wait_readable(&id, &v, 4096, &[8u8; 4096]);
    }

    // A deposed leader still holding a stale term cannot touch the data nodes.
    let stale = RemoteDevice::new(g.dns.addrs[0], IO_TIMEOUT);
    assert!(matches!(
        stale.write_at(old_term, 0, &[0u8; 4096]),
        Err(NativeError::Fenced { current }) if current == new_term
    ));
}
