// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Atlas Native storage driver: the `StorageDriver` for an `atlas-native-node` cluster
//! (`docs/NATIVE_NODE.md`), spoken to over its HTTP API.
//!
//! A native cluster maps to one Atlas cluster with one replicated pool (`native`); every native
//! volume is a block `StorageVolume` with id `vol_native_<native id>`, snapshots are
//! `snap_native_<native id>`. Capacity is not reported by the nodes, so it stays `None` rather
//! than being made up; pool and volume `used_bytes` are the logical bytes of written extents.
//!
//! Two implementations behind the same mapping, as with the other drivers:
//! - [`FakeNativeDriver`] keeps volumes in memory (fixture; no network).
//! - [`RealNativeDriver`] calls the node API on a list of endpoints. Reads go to any node;
//!   mutations are retried across endpoints until the leader accepts them (followers answer 421).

mod fake;
mod http;

use async_trait::async_trait;
use atlas_api_types::{
    CloneSnapshotRequest, CreateSnapshotRequest, CreateSnapshotResult, CreateVolumeRequest,
    CreateVolumeResult, DeleteSnapshotRequest, DeleteVolumeRequest, DiscoveryResult,
    ExpandVolumeRequest, Health, MetricSample, StorageCluster, StorageHealth, StoragePool,
    StorageVolume, VolumeKind,
};
use atlas_driver_core::{DriverError, StorageDriver};
use serde::Deserialize;

pub use fake::FakeApi;
pub use http::{HttpApi, HttpApiConfig};

pub const POOL_NAME: &str = "native";
const VOLUME_PREFIX: &str = "vol_native_";
const SNAPSHOT_PREFIX: &str = "snap_native_";

/// `GET /v1/status`, the fields the driver uses.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub metadata: Option<RaftInfo>,
    #[serde(default)]
    pub layout: Option<Layout>,
    #[serde(default)]
    pub data_nodes: Option<Vec<DataNodeInfo>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RaftInfo {
    pub role: String,
    pub term: u64,
    pub leader: Option<String>,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Layout {
    pub extent_bytes: u64,
    pub replicas: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataNodeInfo {
    pub id: String,
    pub up: bool,
}

/// An entry of `GET /v1/volumes`.
#[derive(Debug, Clone, Deserialize)]
pub struct NativeVolume {
    pub id: String,
    pub name: String,
    pub size_bytes: u64,
    pub extents: u64,
}

/// The node API operations the driver needs.
#[async_trait]
pub trait NativeApi: Send + Sync {
    async fn status(&self) -> Result<NodeStatus, DriverError>;
    async fn volumes(&self) -> Result<Vec<NativeVolume>, DriverError>;
    /// Creates take the new id from the caller so a retried create is a no-op.
    async fn create_volume(
        &self,
        id: &str,
        name: &str,
        size_bytes: u64,
    ) -> Result<String, DriverError>;
    async fn delete_volume(&self, id: &str) -> Result<(), DriverError>;
    async fn resize_volume(&self, id: &str, size_bytes: u64) -> Result<(), DriverError>;
    async fn create_snapshot(
        &self,
        id: &str,
        volume_id: &str,
        name: &str,
    ) -> Result<String, DriverError>;
    async fn clone_snapshot(
        &self,
        id: &str,
        snapshot_id: &str,
        name: &str,
        size_bytes: Option<u64>,
    ) -> Result<String, DriverError>;
    async fn delete_snapshot(&self, id: &str) -> Result<(), DriverError>;
}

pub struct NativeDriver<A> {
    backend_id: String,
    api: A,
    fixture: bool,
}

pub type RealNativeDriver = NativeDriver<HttpApi>;
pub type FakeNativeDriver = NativeDriver<FakeApi>;

impl RealNativeDriver {
    pub fn new(backend_id: impl Into<String>, api: HttpApi) -> Self {
        Self {
            backend_id: backend_id.into(),
            api,
            fixture: false,
        }
    }
}

impl FakeNativeDriver {
    pub fn new(backend_id: impl Into<String>) -> Self {
        Self {
            backend_id: backend_id.into(),
            api: FakeApi::default(),
            fixture: true,
        }
    }
}

/// The native volume id behind an Atlas volume id (or a bare native id).
pub fn native_volume_id(id: &str) -> &str {
    id.strip_prefix(VOLUME_PREFIX).unwrap_or(id)
}

/// The native snapshot id behind an Atlas snapshot id (or a bare native id).
pub fn native_snapshot_id(id: &str) -> &str {
    id.strip_prefix(SNAPSHOT_PREFIX).unwrap_or(id)
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn health_of(status: &NodeStatus) -> (Health, String) {
    let Some(m) = &status.metadata else {
        return (
            Health::Critical,
            format!("{} does not run the metadata role", status.node_id),
        );
    };
    let Some(leader) = &m.leader else {
        return (Health::Critical, "no metadata leader".into());
    };
    let nodes = status.data_nodes.as_deref().unwrap_or_default();
    let down: Vec<&str> = nodes
        .iter()
        .filter(|n| !n.up)
        .map(|n| n.id.as_str())
        .collect();
    if down.is_empty() {
        (
            Health::Ok,
            format!(
                "leader {leader} (term {}), {} data node(s) up",
                m.term,
                nodes.len()
            ),
        )
    } else {
        (
            Health::Warn,
            format!(
                "leader {leader}; {} of {} data node(s) down: {}",
                down.len(),
                nodes.len(),
                down.join(", ")
            ),
        )
    }
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn positive(size: i64) -> Result<u64, DriverError> {
    u64::try_from(size)
        .ok()
        .filter(|s| *s > 0)
        .ok_or_else(|| DriverError::Backend("size_bytes must be > 0".into()))
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

impl<A: NativeApi> NativeDriver<A> {
    fn cluster_id(&self) -> String {
        format!("cls_native_{}", sanitize(&self.backend_id))
    }

    fn pool_id(&self) -> String {
        format!("pool_native_{}", sanitize(&self.backend_id))
    }

    fn used(layout: Option<Layout>, v: &NativeVolume) -> Option<i64> {
        layout.map(|l| to_i64(v.extents.saturating_mul(l.extent_bytes).min(v.size_bytes)))
    }

    fn volume(&self, layout: Option<Layout>, v: &NativeVolume) -> StorageVolume {
        StorageVolume {
            id: format!("{VOLUME_PREFIX}{}", v.id),
            cluster_id: Some(self.cluster_id()),
            pool_id: Some(self.pool_id()),
            name: v.name.clone(),
            kind: VolumeKind::Block,
            backend_native_id: Some(v.id.clone()),
            size_bytes: to_i64(v.size_bytes),
            used_bytes: Self::used(layout, v),
            state: "available".into(),
            health: Health::Ok,
            kubernetes_namespace: None,
            pvc_name: None,
            storage_class_name: None,
        }
    }

    fn pool(&self, status: &NodeStatus, vols: &[NativeVolume]) -> StoragePool {
        let used = status
            .layout
            .map(|l| vols.iter().filter_map(|v| Self::used(Some(l), v)).sum());
        StoragePool {
            id: self.pool_id(),
            cluster_id: self.cluster_id(),
            name: POOL_NAME.into(),
            kind: "replicated".into(),
            device_class: None,
            replica_size: status.layout.map(|l| i64::from(l.replicas)),
            used_bytes: used,
            max_bytes: None,
            health: health_of(status).0,
        }
    }

    fn storage_health(status: &NodeStatus) -> StorageHealth {
        let (status, summary) = health_of(status);
        StorageHealth {
            status,
            summary,
            raw_capacity_bytes: None,
            used_capacity_bytes: None,
            available_capacity_bytes: None,
            recovering: false,
            degraded_objects: 0,
        }
    }
}

#[async_trait]
impl<A: NativeApi> StorageDriver for NativeDriver<A> {
    fn backend_id(&self) -> &str {
        &self.backend_id
    }

    fn is_fixture(&self) -> bool {
        self.fixture
    }

    async fn discover(&self) -> Result<DiscoveryResult, DriverError> {
        let status = self.api.status().await?;
        let vols = self.api.volumes().await?;
        let health = Self::storage_health(&status);
        Ok(DiscoveryResult {
            cluster: StorageCluster {
                id: self.cluster_id(),
                backend_id: self.backend_id.clone(),
                name: format!("atlas-native ({})", self.backend_id),
                native_fsid: None,
                health: health.status,
                raw_capacity_bytes: None,
                used_capacity_bytes: None,
                available_capacity_bytes: None,
            },
            pools: vec![self.pool(&status, &vols)],
            osds: vec![],
            volumes: vols.iter().map(|v| self.volume(status.layout, v)).collect(),
            health,
        })
    }

    async fn health(&self) -> Result<StorageHealth, DriverError> {
        Ok(Self::storage_health(&self.api.status().await?))
    }

    async fn list_pools(&self) -> Result<Vec<StoragePool>, DriverError> {
        let status = self.api.status().await?;
        let vols = self.api.volumes().await?;
        Ok(vec![self.pool(&status, &vols)])
    }

    async fn list_volumes(&self, pool: &str) -> Result<Vec<StorageVolume>, DriverError> {
        if pool != POOL_NAME && pool != self.pool_id() {
            return Ok(vec![]);
        }
        let layout = self.api.status().await?.layout;
        Ok(self
            .api
            .volumes()
            .await?
            .iter()
            .map(|v| self.volume(layout, v))
            .collect())
    }

    async fn metrics(&self) -> Result<Vec<MetricSample>, DriverError> {
        let status = self.api.status().await?;
        let vols = self.api.volumes().await?;
        let m = |name: &str, value: f64| MetricSample {
            name: name.into(),
            value,
            labels: [("backend".to_string(), self.backend_id.clone())].into(),
        };
        let nodes = status.data_nodes.as_deref().unwrap_or_default();
        let mut out = vec![
            m("native_volumes_total", vols.len() as f64),
            m("native_data_nodes_total", nodes.len() as f64),
            m(
                "native_data_nodes_up",
                nodes.iter().filter(|n| n.up).count() as f64,
            ),
        ];
        if let Some(r) = &status.metadata {
            out.push(m("native_raft_term", r.term as f64));
            out.push(m("native_raft_commit_index", r.commit_index as f64));
            out.push(m(
                "native_raft_leader_known",
                f64::from(u8::from(r.leader.is_some())),
            ));
        }
        Ok(out)
    }

    async fn create_volume(
        &self,
        req: CreateVolumeRequest,
    ) -> Result<CreateVolumeResult, DriverError> {
        if req.kind != VolumeKind::Block {
            return Err(DriverError::Backend(
                "atlas-native provides block volumes only".into(),
            ));
        }
        let size = positive(req.size_bytes)?;
        let id = self.api.create_volume(&new_id(), &req.name, size).await?;
        Ok(CreateVolumeResult {
            volume_id: format!("{VOLUME_PREFIX}{id}"),
            backend_native_id: id,
        })
    }

    async fn expand_volume(&self, req: ExpandVolumeRequest) -> Result<(), DriverError> {
        let size = positive(req.new_size_bytes)?;
        self.api
            .resize_volume(native_volume_id(&req.volume_id), size)
            .await
    }

    async fn clone_snapshot(
        &self,
        req: CloneSnapshotRequest,
    ) -> Result<CreateVolumeResult, DriverError> {
        let size = req.size_bytes.map(positive).transpose()?;
        let id = self
            .api
            .clone_snapshot(
                &new_id(),
                native_snapshot_id(&req.snapshot_id),
                &req.new_volume_name,
                size,
            )
            .await?;
        Ok(CreateVolumeResult {
            volume_id: format!("{VOLUME_PREFIX}{id}"),
            backend_native_id: id,
        })
    }

    async fn delete_volume(&self, req: DeleteVolumeRequest) -> Result<(), DriverError> {
        self.api
            .delete_volume(native_volume_id(&req.volume_id))
            .await
    }

    async fn create_snapshot(
        &self,
        req: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResult, DriverError> {
        let id = self
            .api
            .create_snapshot(&new_id(), native_volume_id(&req.volume_id), &req.name)
            .await?;
        Ok(CreateSnapshotResult {
            snapshot_id: format!("{SNAPSHOT_PREFIX}{id}"),
            backend_native_id: id,
        })
    }

    async fn delete_snapshot(&self, req: DeleteSnapshotRequest) -> Result<(), DriverError> {
        self.api
            .delete_snapshot(native_snapshot_id(&req.snapshot_id))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(name: &str, size: i64) -> CreateVolumeRequest {
        CreateVolumeRequest {
            tenant_id: "t".into(),
            name: name.into(),
            size_bytes: size,
            kind: VolumeKind::Block,
            policy: None,
            pool: None,
            owner: None,
            kubernetes: None,
        }
    }

    #[tokio::test]
    async fn fake_driver_round_trips_volumes_and_snapshots() {
        let d = FakeNativeDriver::new("bkd_native");
        assert!(d.is_fixture());
        let created = d.create_volume(create("disk", 8 << 20)).await.unwrap();
        assert!(created.volume_id.starts_with("vol_native_"));

        let disc = d.discover().await.unwrap();
        assert_eq!(disc.cluster.id, "cls_native_bkd_native");
        assert_eq!(disc.pools.len(), 1);
        assert_eq!(disc.pools[0].replica_size, Some(3));
        assert_eq!(disc.health.status, Health::Ok);
        let v = &disc.volumes[0];
        assert_eq!(v.id, created.volume_id);
        assert_eq!(
            v.backend_native_id.as_deref(),
            Some(created.backend_native_id.as_str())
        );
        assert_eq!(v.size_bytes, 8 << 20);
        assert_eq!(v.pool_id.as_deref(), Some("pool_native_bkd_native"));

        let snap = d
            .create_snapshot(CreateSnapshotRequest {
                volume_id: created.volume_id.clone(),
                name: "s1".into(),
            })
            .await
            .unwrap();
        assert!(snap.snapshot_id.starts_with("snap_native_"));

        d.expand_volume(ExpandVolumeRequest {
            volume_id: created.volume_id.clone(),
            new_size_bytes: 16 << 20,
        })
        .await
        .unwrap();
        let clone = d
            .clone_snapshot(CloneSnapshotRequest {
                snapshot_id: snap.snapshot_id.clone(),
                new_volume_name: "copy".into(),
                size_bytes: None,
            })
            .await
            .unwrap();
        let sizes: Vec<(String, i64)> = d
            .list_volumes(POOL_NAME)
            .await
            .unwrap()
            .into_iter()
            .map(|v| (v.id, v.size_bytes))
            .collect();
        assert!(sizes.contains(&(created.volume_id.clone(), 16 << 20)));
        assert!(
            sizes.contains(&(clone.volume_id.clone(), 8 << 20)),
            "{sizes:?}"
        );
        assert!(d
            .clone_snapshot(CloneSnapshotRequest {
                snapshot_id: snap.snapshot_id.clone(),
                new_volume_name: "small".into(),
                size_bytes: Some(4096),
            })
            .await
            .is_err());
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
            volume_id: created.volume_id,
        })
        .await
        .unwrap();
        assert!(d.list_volumes(POOL_NAME).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_requests_are_refused() {
        let d = FakeNativeDriver::new("bkd_native");
        assert!(d.create_volume(create("x", 0)).await.is_err());
        let mut fs = create("x", 4096);
        fs.kind = VolumeKind::Filesystem;
        assert!(d.create_volume(fs).await.is_err());
        assert!(matches!(
            d.delete_volume(DeleteVolumeRequest {
                volume_id: "vol_native_missing".into()
            })
            .await,
            Err(DriverError::Backend(_))
        ));
    }

    #[test]
    fn health_reflects_leader_and_data_nodes() {
        let mut s = NodeStatus {
            node_id: "m1".into(),
            metadata: Some(RaftInfo {
                role: "follower".into(),
                term: 2,
                leader: Some("m2".into()),
                commit_index: 5,
            }),
            layout: None,
            data_nodes: Some(vec![
                DataNodeInfo {
                    id: "d1".into(),
                    up: true,
                },
                DataNodeInfo {
                    id: "d2".into(),
                    up: false,
                },
            ]),
        };
        assert_eq!(health_of(&s).0, Health::Warn);
        s.metadata.as_mut().unwrap().leader = None;
        assert_eq!(health_of(&s).0, Health::Critical);
        s.metadata = None;
        assert_eq!(health_of(&s).0, Health::Critical);
    }
}
