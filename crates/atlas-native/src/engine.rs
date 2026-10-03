// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    checksum,
    device::FileDevice,
    placement::{select_replicas, Node, PlacementPolicy},
    telemetry::NativeIoCounters,
};

pub type VolumeId = String;
pub type SnapshotId = String;

#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] serde_json::Error),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("insufficient healthy replicas: need {needed}, found {found}")]
    InsufficientReplicas { needed: usize, found: usize },
    #[error("checksum mismatch for extent {0}")]
    Checksum(String),
    #[error("lock poisoned: {0}")]
    Poisoned(&'static str),
    #[error("invalid request: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub root: PathBuf,
    pub extent_bytes: usize,
    pub placement: PlacementPolicy,
}

impl EngineConfig {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            extent_bytes: 4 * 1024 * 1024,
            placement: PlacementPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicaRef {
    node_id: String,
    device_index: usize,
    offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtentRef {
    id: String,
    logical_offset: u64,
    len: usize,
    checksum: [u8; 32],
    replicas: Vec<ReplicaRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeMeta {
    id: VolumeId,
    name: String,
    size_bytes: u64,
    extents: BTreeMap<u64, ExtentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotMeta {
    id: SnapshotId,
    volume_id: VolumeId,
    name: String,
    extents: BTreeMap<u64, ExtentRef>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Catalog {
    volumes: BTreeMap<VolumeId, VolumeMeta>,
    snapshots: BTreeMap<SnapshotId, SnapshotMeta>,
}

#[derive(Debug)]
struct NodeRuntime {
    spec: Node,
    devices: Vec<Arc<FileDevice>>,
}

#[derive(Debug)]
pub struct NativeEngine {
    cfg: EngineConfig,
    nodes: Vec<NodeRuntime>,
    catalog: RwLock<Catalog>,
    pub telemetry: NativeIoCounters,
}

impl NativeEngine {
    pub fn open(cfg: EngineConfig, nodes: Vec<Node>) -> Result<Self, NativeError> {
        if cfg.extent_bytes == 0 {
            return Err(NativeError::Invalid("extent_bytes must be > 0".into()));
        }
        fs::create_dir_all(&cfg.root)?;
        let mut runtimes = Vec::new();
        for n in nodes {
            let d = FileDevice::open(cfg.root.join("nodes").join(&n.id).join("nvme0.data"))?;
            runtimes.push(NodeRuntime {
                spec: n,
                devices: vec![Arc::new(d)],
            });
        }
        let catalog_path = cfg.root.join("catalog.json");
        let catalog = if catalog_path.exists() {
            serde_json::from_slice(&fs::read(&catalog_path)?)?
        } else {
            Catalog::default()
        };
        Ok(Self {
            cfg,
            nodes: runtimes,
            catalog: RwLock::new(catalog),
            telemetry: NativeIoCounters::default(),
        })
    }

    pub fn create_volume(
        &self,
        name: impl Into<String>,
        size_bytes: u64,
    ) -> Result<VolumeId, NativeError> {
        if size_bytes == 0 {
            return Err(NativeError::Invalid("volume size must be > 0".into()));
        }
        let id = Uuid::new_v4().to_string();
        let mut c = self
            .catalog
            .write()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        c.volumes.insert(
            id.clone(),
            VolumeMeta {
                id: id.clone(),
                name: name.into(),
                size_bytes,
                extents: BTreeMap::new(),
            },
        );
        self.persist(&c)?;
        Ok(id)
    }

    pub fn write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        if data.is_empty() {
            return Ok(());
        }
        let mut c = self
            .catalog
            .write()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        let vol = c
            .volumes
            .get_mut(volume_id)
            .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
        if offset.saturating_add(data.len() as u64) > vol.size_bytes {
            return Err(NativeError::Invalid("write exceeds volume size".into()));
        }

        for (idx, chunk) in data.chunks(self.cfg.extent_bytes).enumerate() {
            let logical = offset + (idx * self.cfg.extent_bytes) as u64;
            let selected = select_replicas(
                &self
                    .nodes
                    .iter()
                    .map(|n| n.spec.clone())
                    .collect::<Vec<_>>(),
                chunk.len() as u64,
                self.cfg.placement,
            );
            if selected.len() < self.cfg.placement.replicas {
                return Err(NativeError::InsufficientReplicas {
                    needed: self.cfg.placement.replicas,
                    found: selected.len(),
                });
            }
            let mut replicas = Vec::new();
            for node_id in selected {
                let node = self
                    .nodes
                    .iter()
                    .find(|n| n.spec.id == node_id)
                    .ok_or_else(|| NativeError::NotFound(node_id.clone()))?;
                let off = node.devices[0].append(chunk)?;
                replicas.push(ReplicaRef {
                    node_id,
                    device_index: 0,
                    offset: off,
                });
            }
            vol.extents.insert(
                logical,
                ExtentRef {
                    id: Uuid::new_v4().to_string(),
                    logical_offset: logical,
                    len: chunk.len(),
                    checksum: checksum::sha256(chunk),
                    replicas,
                },
            );
            self.telemetry.record_write(chunk.len());
        }
        self.persist(&c)?;
        Ok(())
    }

    pub fn read(&self, volume_id: &str, offset: u64, len: usize) -> Result<Vec<u8>, NativeError> {
        let c = self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        let vol = c
            .volumes
            .get(volume_id)
            .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
        self.read_map(&vol.extents, offset, len)
    }

    pub fn create_snapshot(
        &self,
        volume_id: &str,
        name: impl Into<String>,
    ) -> Result<SnapshotId, NativeError> {
        let mut c = self
            .catalog
            .write()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        let vol = c
            .volumes
            .get(volume_id)
            .ok_or_else(|| NativeError::NotFound(volume_id.into()))?
            .clone();
        let id = Uuid::new_v4().to_string();
        c.snapshots.insert(
            id.clone(),
            SnapshotMeta {
                id: id.clone(),
                volume_id: volume_id.into(),
                name: name.into(),
                extents: vol.extents,
            },
        );
        self.persist(&c)?;
        Ok(id)
    }

    pub fn read_snapshot(
        &self,
        snapshot_id: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NativeError> {
        let c = self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        let s = c
            .snapshots
            .get(snapshot_id)
            .ok_or_else(|| NativeError::NotFound(snapshot_id.into()))?;
        self.read_map(&s.extents, offset, len)
    }

    fn read_map(
        &self,
        extents: &BTreeMap<u64, ExtentRef>,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NativeError> {
        let ext = extents
            .get(&offset)
            .ok_or_else(|| NativeError::NotFound(format!("extent at {offset}")))?;
        if len > ext.len {
            return Err(NativeError::Invalid(
                "cross-extent reads are not implemented in phase 1".into(),
            ));
        }
        for (i, r) in ext.replicas.iter().enumerate() {
            let Some(node) = self
                .nodes
                .iter()
                .find(|n| n.spec.id == r.node_id && n.spec.healthy)
            else {
                continue;
            };
            match node.devices[r.device_index].read_exact_at(r.offset, ext.len) {
                Ok(buf) if checksum::verify(&buf, &ext.checksum) => {
                    if i > 0 {
                        self.telemetry.replica_fallback();
                    }
                    self.telemetry.record_read(len);
                    return Ok(buf[..len].to_vec());
                }
                Ok(_) => self.telemetry.checksum_failure(),
                Err(_) => continue,
            }
        }
        Err(NativeError::Checksum(ext.id.clone()))
    }

    fn persist(&self, catalog: &Catalog) -> Result<(), NativeError> {
        let tmp = self.cfg.root.join("catalog.json.tmp");
        let dst = self.cfg.root.join("catalog.json");
        let bytes = serde_json::to_vec_pretty(catalog)?;
        fs::write(&tmp, bytes)?;
        fs::rename(tmp, dst)?;
        Ok(())
    }
}
