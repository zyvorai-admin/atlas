// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};
use uuid::Uuid;

use crate::{
    checksum,
    device::FileDevice,
    durable, gc,
    metadata::{Catalog, ExtentRef, MetaCommand, MetaError, ReplicaRef, SnapshotId, VolumeId},
    placement::{select_replicas, Node, PlacementPolicy},
    telemetry::NativeIoCounters,
    wal::{Wal, WalError, WalRecord},
};

#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata json error: {0}")]
    MetadataJson(#[from] serde_json::Error),
    #[error("metadata state error: {0}")]
    Metadata(#[from] MetaError),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
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
    /// Compact the WAL once it retains this many records (0 disables automatic compaction).
    pub wal_compact_after: u64,
}

impl EngineConfig {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            extent_bytes: 4 * 1024 * 1024,
            placement: PlacementPolicy::default(),
            wal_compact_after: 1024,
        }
    }
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
    wal: Mutex<Wal>,
    /// Serializes free-list allocation through data write and `InstallExtent` commit, so two
    /// writers can never be handed the same free range.
    write_lock: Mutex<()>,
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

        let snapshot_path = cfg.root.join("catalog.json");
        let mut catalog: Catalog = if snapshot_path.exists() {
            serde_json::from_slice(&fs::read(&snapshot_path)?)?
        } else {
            Catalog::default()
        };
        let mut wal = Wal::open(cfg.root.join("wal"))?;
        for rec in wal.replay::<MetaCommand>()? {
            if rec.index > catalog.applied_index {
                catalog.apply(rec.term, rec.index, &rec.command)?;
            }
        }
        wal.raise_floor(catalog.applied_index);
        let engine = Self {
            cfg,
            nodes: runtimes,
            catalog: RwLock::new(catalog),
            wal: Mutex::new(wal),
            write_lock: Mutex::new(()),
            telemetry: NativeIoCounters::default(),
        };
        engine.persist_catalog()?;
        Ok(engine)
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
        self.commit(MetaCommand::CreateVolume {
            id: id.clone(),
            name: name.into(),
            size_bytes,
        })?;
        Ok(id)
    }

    pub fn delete_volume(&self, volume_id: &str) -> Result<(), NativeError> {
        self.commit(MetaCommand::DeleteVolume {
            volume_id: volume_id.to_string(),
        })?;
        Ok(())
    }

    pub fn write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        if data.is_empty() {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| NativeError::Poisoned("write"))?;
        let size = {
            let c = self
                .catalog
                .read()
                .map_err(|_| NativeError::Poisoned("catalog"))?;
            c.volumes
                .get(volume_id)
                .ok_or_else(|| NativeError::NotFound(volume_id.into()))?
                .size_bytes
        };
        if offset.saturating_add(data.len() as u64) > size {
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
            let reuse: Vec<Option<u64>> = {
                let c = self
                    .catalog
                    .read()
                    .map_err(|_| NativeError::Poisoned("catalog"))?;
                selected
                    .iter()
                    .map(|id| c.free.find(id, 0, chunk.len() as u64))
                    .collect()
            };
            let mut replicas = Vec::new();
            for (node_id, free_off) in selected.into_iter().zip(reuse) {
                let node = self
                    .nodes
                    .iter()
                    .find(|n| n.spec.id == node_id)
                    .ok_or_else(|| NativeError::NotFound(node_id.clone()))?;
                let off = match free_off {
                    Some(off) => {
                        node.devices[0].write_at(off, chunk)?;
                        off
                    }
                    None => node.devices[0].append(chunk)?,
                };
                replicas.push(ReplicaRef {
                    node_id,
                    device_index: 0,
                    offset: off,
                });
            }
            let extent = ExtentRef {
                id: Uuid::new_v4().to_string(),
                logical_offset: logical,
                len: chunk.len(),
                checksum: checksum::sha256(chunk),
                replicas,
            };
            self.commit(MetaCommand::InstallExtent {
                volume_id: volume_id.to_string(),
                logical_offset: logical,
                extent,
            })?;
            self.telemetry.record_write(chunk.len());
        }
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
        let eid = vol
            .extents
            .get(&offset)
            .ok_or_else(|| NativeError::NotFound(format!("extent at {offset}")))?;
        let ext = &c
            .extents
            .get(eid)
            .ok_or_else(|| NativeError::NotFound(eid.clone()))?
            .extent;
        self.read_extent(ext, len)
    }

    pub fn create_snapshot(
        &self,
        volume_id: &str,
        name: impl Into<String>,
    ) -> Result<SnapshotId, NativeError> {
        let id = Uuid::new_v4().to_string();
        self.commit(MetaCommand::CreateSnapshot {
            id: id.clone(),
            volume_id: volume_id.to_string(),
            name: name.into(),
        })?;
        Ok(id)
    }

    pub fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), NativeError> {
        self.commit(MetaCommand::DeleteSnapshot {
            snapshot_id: snapshot_id.to_string(),
        })?;
        Ok(())
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
        let eid = s
            .extents
            .get(&offset)
            .ok_or_else(|| NativeError::NotFound(format!("extent at {offset}")))?;
        let ext = &c
            .extents
            .get(eid)
            .ok_or_else(|| NativeError::NotFound(eid.clone()))?
            .extent;
        self.read_extent(ext, len)
    }

    pub fn gc_once(&self) -> Result<gc::GcStats, NativeError> {
        let (candidates, free_before) = {
            let c = self
                .catalog
                .read()
                .map_err(|_| NativeError::Poisoned("catalog"))?;
            (gc::collect_candidates(&c), c.free.total_bytes())
        };
        let mut stats = gc::GcStats {
            candidates: candidates.len() as u64,
            ..Default::default()
        };
        for eid in candidates {
            self.commit(MetaCommand::MarkExtentReclaimed { extent_id: eid })?;
            stats.reclaimed += 1;
        }
        stats.freed_bytes = self.free_bytes()?.saturating_sub(free_before);
        Ok(stats)
    }

    pub fn applied_index(&self) -> Result<u64, NativeError> {
        Ok(self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?
            .applied_index)
    }

    /// Device bytes (summed over replicas) currently on the free lists.
    pub fn free_bytes(&self) -> Result<u64, NativeError> {
        Ok(self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?
            .free
            .total_bytes())
    }

    /// Size of the backing file for `node_id`'s first device.
    pub fn device_len(&self, node_id: &str) -> Result<u64, NativeError> {
        self.nodes
            .iter()
            .find(|n| n.spec.id == node_id)
            .ok_or_else(|| NativeError::NotFound(node_id.into()))?
            .devices[0]
            .len()
    }

    /// Records currently retained in the WAL.
    pub fn wal_records(&self) -> Result<u64, NativeError> {
        Ok(self
            .wal
            .lock()
            .map_err(|_| NativeError::Poisoned("wal"))?
            .len())
    }

    /// Persists the catalog and drops every WAL record it covers. Returns the records removed.
    pub fn checkpoint(&self) -> Result<u64, NativeError> {
        let mut wal = self.wal.lock().map_err(|_| NativeError::Poisoned("wal"))?;
        let c = self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        self.persist_locked(&c)?;
        Ok(wal.compact_through(c.applied_index)?)
    }

    fn commit(&self, command: MetaCommand) -> Result<Vec<String>, NativeError> {
        let mut wal = self.wal.lock().map_err(|_| NativeError::Poisoned("wal"))?;
        let mut c = self
            .catalog
            .write()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        let index = wal.last_index() + 1;
        let term = c.current_term.max(1);
        let rec = WalRecord {
            term,
            index,
            command,
        };
        // A record that fails to apply must never reach the WAL, or every later replay fails on it.
        // The WAL still reaches stable storage before the new state becomes visible.
        let mut next = c.clone();
        let gc = next.apply(term, index, &rec.command)?;
        wal.append(&rec)?;
        *c = next;
        self.persist_locked(&c)?;
        if self.cfg.wal_compact_after > 0 && wal.len() >= self.cfg.wal_compact_after {
            // catalog.json now durably covers `index`, so every record up to it is redundant.
            wal.compact_through(index)?;
        }
        Ok(gc)
    }

    fn read_extent(&self, ext: &ExtentRef, len: usize) -> Result<Vec<u8>, NativeError> {
        if len > ext.len {
            return Err(NativeError::Invalid(
                "cross-extent reads are not implemented in phase 2".into(),
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

    fn persist_catalog(&self) -> Result<(), NativeError> {
        let c = self
            .catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        self.persist_locked(&c)
    }

    fn persist_locked(&self, catalog: &Catalog) -> Result<(), NativeError> {
        let bytes = serde_json::to_vec_pretty(catalog)?;
        durable::write_atomic(&self.cfg.root.join("catalog.json"), &bytes)?;
        Ok(())
    }
}
