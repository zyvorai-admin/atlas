// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use uuid::Uuid;

use crate::{
    checksum,
    device::{BlockStore, FileDevice},
    durable, gc,
    metadata::{Catalog, ExtentRef, MetaCommand, MetaError, ReplicaRef, SnapshotId, VolumeId},
    metrics::PromText,
    placement::{select_replicas, Node, PlacementPolicy},
    raft::RaftError,
    raft_server::RaftServer,
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
    #[error("raft error: {0}")]
    Raft(#[from] RaftError),
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
    #[error("write fenced: a data node has accepted writes from term {current}")]
    Fenced { current: u64 },
    #[error("data node error: {0}")]
    Remote(String),
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

/// Where the engine commits metadata.
#[derive(Clone)]
pub enum MetaBackend {
    /// A local WAL and `catalog.json` under `EngineConfig::root`.
    Local,
    /// A replicated log. Only the engine attached to the current leader can mutate; every
    /// engine serves reads from its replica's applied catalog, which can lag the leader.
    Raft {
        server: Arc<RaftServer>,
        /// Bounds waiting for leader readiness and for each proposal to apply.
        timeout: Duration,
    },
}

enum Meta {
    Local {
        catalog: RwLock<Catalog>,
        wal: Mutex<Wal>,
    },
    Raft {
        server: Arc<RaftServer>,
        timeout: Duration,
    },
}

impl std::fmt::Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Meta::Local { .. } => "Meta::Local",
            Meta::Raft { .. } => "Meta::Raft",
        })
    }
}

#[derive(Debug)]
struct NodeRuntime {
    spec: Node,
    devices: Vec<Arc<dyn BlockStore>>,
}

#[derive(Debug)]
pub struct NativeEngine {
    cfg: EngineConfig,
    nodes: Vec<NodeRuntime>,
    meta: Meta,
    /// Serializes free-list allocation through data write and `InstallExtent` commit, so two
    /// writers can never be handed the same free range.
    write_lock: Mutex<()>,
    pub telemetry: NativeIoCounters,
}

impl NativeEngine {
    /// Local devices under `root/nodes/<id>/nvme0.data` and a local metadata WAL.
    pub fn open(cfg: EngineConfig, nodes: Vec<Node>) -> Result<Self, NativeError> {
        let mut stores = Vec::new();
        for n in nodes {
            let d = FileDevice::open(cfg.root.join("nodes").join(&n.id).join("nvme0.data"))?;
            stores.push((n, Arc::new(d) as Arc<dyn BlockStore>));
        }
        Self::open_with(cfg, stores, MetaBackend::Local)
    }

    /// Opens an engine over caller-supplied devices (e.g. [`crate::data_node::RemoteDevice`])
    /// with the given metadata backend. Every engine sharing a Raft group must be given the
    /// same node ids and devices.
    pub fn open_with(
        cfg: EngineConfig,
        nodes: Vec<(Node, Arc<dyn BlockStore>)>,
        meta: MetaBackend,
    ) -> Result<Self, NativeError> {
        if cfg.extent_bytes == 0 {
            return Err(NativeError::Invalid("extent_bytes must be > 0".into()));
        }
        let runtimes = nodes
            .into_iter()
            .map(|(spec, d)| NodeRuntime {
                spec,
                devices: vec![d],
            })
            .collect();
        let meta = match meta {
            MetaBackend::Local => {
                fs::create_dir_all(&cfg.root)?;
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
                Meta::Local {
                    catalog: RwLock::new(catalog),
                    wal: Mutex::new(wal),
                }
            }
            MetaBackend::Raft { server, timeout } => Meta::Raft { server, timeout },
        };
        let engine = Self {
            cfg,
            nodes: runtimes,
            meta,
            write_lock: Mutex::new(()),
            telemetry: NativeIoCounters::default(),
        };
        if let Meta::Local { catalog, .. } = &engine.meta {
            let c = catalog
                .read()
                .map_err(|_| NativeError::Poisoned("catalog"))?;
            engine.persist_locked(&c)?;
        }
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
        self.commit(
            MetaCommand::CreateVolume {
                id: id.clone(),
                name: name.into(),
                size_bytes,
            },
            None,
        )?;
        Ok(id)
    }

    pub fn delete_volume(&self, volume_id: &str) -> Result<(), NativeError> {
        self.commit(
            MetaCommand::DeleteVolume {
                volume_id: volume_id.to_string(),
            },
            None,
        )
    }

    pub fn write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        if data.is_empty() {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| NativeError::Poisoned("write"))?;
        // Allocation reads the applied free list, so under Raft every earlier entry (including
        // other terms') must be applied first; the term then fences the data writes.
        let fence = match &self.meta {
            Meta::Local { .. } => 0,
            Meta::Raft { server, timeout } => server.leader_ready(*timeout)?,
        };
        let size = self
            .with_catalog(|c| c.volumes.get(volume_id).map(|v| v.size_bytes))?
            .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
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
            let reuse: Vec<Option<u64>> = self.with_catalog(|c| {
                selected
                    .iter()
                    .map(|id| c.free.find(id, 0, chunk.len() as u64))
                    .collect()
            })?;
            let mut replicas = Vec::new();
            for (node_id, free_off) in selected.into_iter().zip(reuse) {
                let device = &self.node(&node_id)?.devices[0];
                let off = match free_off {
                    Some(off) => {
                        device.write_at(fence, off, chunk)?;
                        off
                    }
                    None => device.append(fence, chunk)?,
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
            self.commit(
                MetaCommand::InstallExtent {
                    volume_id: volume_id.to_string(),
                    logical_offset: logical,
                    extent,
                },
                Some(fence),
            )?;
            self.telemetry.record_write(chunk.len());
        }
        Ok(())
    }

    pub fn read(&self, volume_id: &str, offset: u64, len: usize) -> Result<Vec<u8>, NativeError> {
        let ext = self.with_catalog(|c| {
            let vol = c
                .volumes
                .get(volume_id)
                .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
            Self::extent_at(c, &vol.extents, offset)
        })??;
        self.read_extent(&ext, len)
    }

    pub fn create_snapshot(
        &self,
        volume_id: &str,
        name: impl Into<String>,
    ) -> Result<SnapshotId, NativeError> {
        let id = Uuid::new_v4().to_string();
        self.commit(
            MetaCommand::CreateSnapshot {
                id: id.clone(),
                volume_id: volume_id.to_string(),
                name: name.into(),
            },
            None,
        )?;
        Ok(id)
    }

    pub fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), NativeError> {
        self.commit(
            MetaCommand::DeleteSnapshot {
                snapshot_id: snapshot_id.to_string(),
            },
            None,
        )
    }

    pub fn read_snapshot(
        &self,
        snapshot_id: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NativeError> {
        let ext = self.with_catalog(|c| {
            let s = c
                .snapshots
                .get(snapshot_id)
                .ok_or_else(|| NativeError::NotFound(snapshot_id.into()))?;
            Self::extent_at(c, &s.extents, offset)
        })??;
        self.read_extent(&ext, len)
    }

    pub fn gc_once(&self) -> Result<gc::GcStats, NativeError> {
        if let Meta::Raft { server, timeout } = &self.meta {
            // Candidates come from applied state; an extent already reclaimed by an applied-but-
            // unseen entry would otherwise be proposed again and rejected.
            server.leader_ready(*timeout)?;
        }
        let (candidates, free_before) =
            self.with_catalog(|c| (gc::collect_candidates(c), c.free.total_bytes()))?;
        let mut stats = gc::GcStats {
            candidates: candidates.len() as u64,
            ..Default::default()
        };
        for eid in candidates {
            self.commit(MetaCommand::MarkExtentReclaimed { extent_id: eid }, None)?;
            stats.reclaimed += 1;
        }
        stats.freed_bytes = self.free_bytes()?.saturating_sub(free_before);
        self.telemetry.gc_reclaimed(stats.reclaimed);
        Ok(stats)
    }

    pub fn applied_index(&self) -> Result<u64, NativeError> {
        self.with_catalog(|c| c.applied_index)
    }

    /// Device bytes (summed over replicas) currently on the free lists.
    pub fn free_bytes(&self) -> Result<u64, NativeError> {
        self.with_catalog(|c| c.free.total_bytes())
    }

    /// Size of `node_id`'s first device.
    pub fn device_len(&self, node_id: &str) -> Result<u64, NativeError> {
        self.node(node_id)?.devices[0].len()
    }

    /// Records currently retained in the metadata log (the local WAL, or the Raft log above its
    /// compaction point).
    pub fn wal_records(&self) -> Result<u64, NativeError> {
        match &self.meta {
            Meta::Local { wal, .. } => {
                Ok(wal.lock().map_err(|_| NativeError::Poisoned("wal"))?.len())
            }
            Meta::Raft { server, .. } => Ok(server.log_records()?),
        }
    }

    /// Prometheus text exposition for I/O counters, metadata and free space.
    pub fn render_metrics(&self) -> Result<String, NativeError> {
        let t = self.telemetry.snapshot();
        let (applied, volumes, snapshots, extents, free_bytes, free_ranges) =
            self.with_catalog(|c| {
                (
                    c.applied_index,
                    c.volumes.len(),
                    c.snapshots.len(),
                    c.extents.len(),
                    c.free.total_bytes(),
                    c.free.ranges().len(),
                )
            })?;
        let wal_records = self.wal_records()?;
        let mut p = PromText::new();
        for (name, help, v) in [
            ("atlas_native_reads_total", "Extent reads served.", t.reads),
            ("atlas_native_writes_total", "Extents written.", t.writes),
            (
                "atlas_native_read_bytes_total",
                "Bytes returned by reads.",
                t.read_bytes,
            ),
            (
                "atlas_native_write_bytes_total",
                "Bytes written (before replication).",
                t.write_bytes,
            ),
            (
                "atlas_native_checksum_failures_total",
                "Replica reads that failed checksum verification.",
                t.checksum_failures,
            ),
            (
                "atlas_native_replica_fallbacks_total",
                "Reads served by a non-primary replica.",
                t.replica_fallbacks,
            ),
            (
                "atlas_native_gc_reclaimed_extents_total",
                "Extents reclaimed by GC.",
                t.gc_reclaimed,
            ),
        ] {
            p.single(name, "counter", help, v);
        }
        p.single(
            "atlas_native_metadata_applied_index",
            "gauge",
            "Highest metadata index applied.",
            applied,
        );
        p.single(
            "atlas_native_wal_records",
            "gauge",
            "Records retained in the metadata log.",
            wal_records,
        );
        p.single(
            "atlas_native_volumes",
            "gauge",
            "Volumes in the catalog.",
            volumes,
        );
        p.single(
            "atlas_native_snapshots",
            "gauge",
            "Snapshots in the catalog.",
            snapshots,
        );
        p.single(
            "atlas_native_extents",
            "gauge",
            "Live physical extents.",
            extents,
        );
        p.single(
            "atlas_native_allocator_free_bytes",
            "gauge",
            "Device bytes (summed over replicas) on the free lists.",
            free_bytes,
        );
        p.single(
            "atlas_native_allocator_free_ranges",
            "gauge",
            "Free ranges across all devices.",
            free_ranges,
        );
        p.family(
            "atlas_native_device_bytes",
            "gauge",
            "Size of each node's backing device.",
        );
        for n in &self.nodes {
            p.sample(
                "atlas_native_device_bytes",
                &[("node", n.spec.id.as_str())],
                n.devices[0].len()?,
            );
        }
        Ok(p.finish())
    }

    /// Persists the catalog and drops every WAL record it covers. Returns the records removed.
    /// Raft-backed engines compact their log inside the Raft node instead.
    pub fn checkpoint(&self) -> Result<u64, NativeError> {
        let Meta::Local { catalog, wal } = &self.meta else {
            return Err(NativeError::Invalid(
                "checkpoint applies to the local WAL; Raft compacts its own log".into(),
            ));
        };
        let mut wal = wal.lock().map_err(|_| NativeError::Poisoned("wal"))?;
        let c = catalog
            .read()
            .map_err(|_| NativeError::Poisoned("catalog"))?;
        self.persist_locked(&c)?;
        Ok(wal.compact_through(c.applied_index)?)
    }

    fn node(&self, node_id: &str) -> Result<&NodeRuntime, NativeError> {
        self.nodes
            .iter()
            .find(|n| n.spec.id == node_id)
            .ok_or_else(|| NativeError::NotFound(node_id.into()))
    }

    fn with_catalog<R>(&self, f: impl FnOnce(&Catalog) -> R) -> Result<R, NativeError> {
        match &self.meta {
            Meta::Local { catalog, .. } => {
                let c = catalog
                    .read()
                    .map_err(|_| NativeError::Poisoned("catalog"))?;
                Ok(f(&c))
            }
            Meta::Raft { server, .. } => Ok(server.with_catalog(f)?),
        }
    }

    fn extent_at(
        c: &Catalog,
        extents: &std::collections::BTreeMap<u64, String>,
        offset: u64,
    ) -> Result<ExtentRef, NativeError> {
        let eid = extents
            .get(&offset)
            .ok_or_else(|| NativeError::NotFound(format!("extent at {offset}")))?;
        Ok(c.extents
            .get(eid)
            .ok_or_else(|| NativeError::NotFound(eid.clone()))?
            .extent
            .clone())
    }

    /// `fence` is the term data was written under; a Raft proposal is refused if leadership
    /// moved to another term since.
    fn commit(&self, command: MetaCommand, fence: Option<u64>) -> Result<(), NativeError> {
        let (catalog, wal) = match &self.meta {
            Meta::Local { catalog, wal } => (catalog, wal),
            Meta::Raft { server, timeout } => {
                match fence {
                    Some(term) => server.propose_in_term(command, term, *timeout)?,
                    None => server.propose(command, *timeout)?,
                };
                return Ok(());
            }
        };
        let mut wal = wal.lock().map_err(|_| NativeError::Poisoned("wal"))?;
        let mut c = catalog
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
        next.apply(term, index, &rec.command)?;
        wal.append(&rec)?;
        *c = next;
        self.persist_locked(&c)?;
        if self.cfg.wal_compact_after > 0 && wal.len() >= self.cfg.wal_compact_after {
            // catalog.json now durably covers `index`, so every record up to it is redundant.
            wal.compact_through(index)?;
        }
        Ok(())
    }

    fn read_extent(&self, ext: &ExtentRef, len: usize) -> Result<Vec<u8>, NativeError> {
        if len > ext.len {
            return Err(NativeError::Invalid(
                "cross-extent reads are not implemented in phase 2".into(),
            ));
        }
        for (i, r) in ext.replicas.iter().enumerate() {
            let Some(device) = self
                .nodes
                .iter()
                .find(|n| n.spec.id == r.node_id && n.spec.healthy)
                .and_then(|n| n.devices.get(r.device_index))
            else {
                continue;
            };
            match device.read_exact_at(r.offset, ext.len) {
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

    fn persist_locked(&self, catalog: &Catalog) -> Result<(), NativeError> {
        let bytes = serde_json::to_vec_pretty(catalog)?;
        durable::write_atomic(&self.cfg.root.join("catalog.json"), &bytes)?;
        Ok(())
    }
}
