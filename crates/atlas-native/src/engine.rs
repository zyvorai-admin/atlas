// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::{
    checksum,
    device::{BlockStore, FileDevice},
    durable, gc,
    metadata::{Catalog, ExtentRef, MetaCommand, MetaError, ReplicaRef, SnapshotId, VolumeId},
    metrics::PromText,
    namespace::{FsOp, InodeKind},
    placement::{select_replicas, Node, PlacementPolicy},
    raft::RaftError,
    raft_server::RaftServer,
    telemetry::NativeIoCounters,
    wal::{Wal, WalError, WalRecord},
};

mod files;
pub use files::{Attr, DirEntry, FsInfo, FsSnapshotInfo, FsStat, NewNode};

/// What a data write lands in.
enum Target<'a> {
    Volume(&'a str),
    File { fs: &'a str, ino: u64 },
}

impl Target<'_> {
    /// The extent currently at grid cell `cell`, if any.
    fn extent_at(&self, c: &Catalog, cell: u64) -> Result<Option<ExtentRef>, NativeError> {
        let id = match self {
            Target::Volume(v) => c.volumes.get(*v).and_then(|v| v.extents.get(&cell)),
            Target::File { fs, ino } => match &c.filesystem(fs)?.inode(*ino)?.kind {
                InodeKind::File { extents, .. } => extents.get(&cell),
                _ => {
                    return Err(
                        MetaError::IsDir(format!("inode {ino} is not a regular file")).into(),
                    )
                }
            },
        };
        Ok(id
            .and_then(|id| c.extents.get(id))
            .map(|m| m.extent.clone()))
    }
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

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
    #[error("read-only: {0}")]
    ReadOnly(String),
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub root: PathBuf,
    pub extent_bytes: usize,
    pub placement: PlacementPolicy,
    /// Compact the WAL once it retains this many records (0 disables automatic compaction).
    pub wal_compact_after: u64,
    /// After an I/O failure a node is skipped for placement (and tried last for reads) for this
    /// long, then given another chance.
    pub node_retry_after: Duration,
}

impl EngineConfig {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            extent_bytes: 4 * 1024 * 1024,
            placement: PlacementPolicy::default(),
            wal_compact_after: 1024,
            node_retry_after: Duration::from_secs(5),
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
        catalog: Box<RwLock<Catalog>>,
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

#[derive(Debug, Default)]
struct NodeHealth {
    failures: AtomicU64,
    down_until: Mutex<Option<Instant>>,
}

#[derive(Debug)]
struct NodeRuntime {
    spec: Node,
    devices: Vec<Arc<dyn BlockStore>>,
    health: NodeHealth,
}

/// A node's health as the engine currently sees it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NodeStatus {
    pub id: String,
    /// Configured healthy and not inside an I/O-failure back-off window.
    pub up: bool,
    pub failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VolumeInfo {
    pub id: VolumeId,
    pub name: String,
    pub size_bytes: u64,
    pub extents: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct RepairStats {
    pub extents_checked: u64,
    pub replicas_repaired: u64,
    /// Extents with no replica that still reads back with a valid checksum.
    pub unrecoverable: u64,
    /// Bad replicas left for a later pass: no eligible target, or the extent changed meanwhile.
    pub deferred: u64,
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
                health: NodeHealth::default(),
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
                    catalog: Box::new(RwLock::new(catalog)),
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
        self.create_volume_as(Uuid::new_v4().to_string(), name, size_bytes)
    }

    /// [`Self::create_volume`] with a caller-chosen id: repeating the same call is a no-op, so a
    /// client can safely retry after an ambiguous failure.
    pub fn create_volume_as(
        &self,
        id: String,
        name: impl Into<String>,
        size_bytes: u64,
    ) -> Result<VolumeId, NativeError> {
        if size_bytes == 0 {
            return Err(NativeError::Invalid("volume size must be > 0".into()));
        }
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

    /// Grows `volume_id` to `size_bytes` (never shrinks).
    pub fn resize_volume(&self, volume_id: &str, size_bytes: u64) -> Result<(), NativeError> {
        self.commit(
            MetaCommand::ResizeVolume {
                volume_id: volume_id.to_string(),
                size_bytes,
            },
            None,
        )
    }

    /// A new volume with the snapshot's contents, sharing its extents until either side is
    /// written. `size_bytes` defaults to the snapshot's size.
    pub fn clone_snapshot(
        &self,
        snapshot_id: &str,
        name: impl Into<String>,
        size_bytes: Option<u64>,
    ) -> Result<VolumeId, NativeError> {
        self.clone_snapshot_as(Uuid::new_v4().to_string(), snapshot_id, name, size_bytes)
    }

    /// [`Self::clone_snapshot`] with a caller-chosen id for the new volume (idempotent).
    pub fn clone_snapshot_as(
        &self,
        id: String,
        snapshot_id: &str,
        name: impl Into<String>,
        size_bytes: Option<u64>,
    ) -> Result<VolumeId, NativeError> {
        self.commit(
            MetaCommand::CloneSnapshot {
                id: id.clone(),
                name: name.into(),
                snapshot_id: snapshot_id.to_string(),
                size_bytes,
            },
            None,
        )?;
        Ok(id)
    }

    pub fn write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        if data.is_empty() {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| NativeError::Poisoned("write"))?;
        let fence = self.write_fence()?;
        let size = self
            .with_catalog(|c| c.volumes.get(volume_id).map(|v| v.size_bytes))?
            .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
        if offset.saturating_add(data.len() as u64) > size {
            return Err(NativeError::Invalid("write exceeds volume size".into()));
        }
        self.write_locked(&Target::Volume(volume_id), offset, data, fence)
    }

    /// Allocation reads the applied free list, so under Raft every earlier entry (including
    /// other terms') must be applied first; the term then fences the data writes. Call with
    /// `write_lock` held.
    fn write_fence(&self) -> Result<u64, NativeError> {
        Ok(match &self.meta {
            Meta::Local { .. } => 0,
            Meta::Raft { server, timeout } => server.leader_ready(*timeout)?,
        })
    }

    /// Writes `data` at `offset` of `target` under `write_lock`.
    fn write_locked(
        &self,
        target: &Target,
        offset: u64,
        data: &[u8],
        fence: u64,
    ) -> Result<(), NativeError> {
        // Extents sit on a fixed grid (extent i covers [i*E, (i+1)*E)), so a write never leaves
        // two extents covering the same bytes. A write that covers only part of an existing
        // extent's bytes rewrites the whole extent with the old bytes merged in.
        let grid = self.cfg.extent_bytes as u64;
        let mut pos = offset;
        let mut rest = data;
        while !rest.is_empty() {
            let cell = pos - pos % grid;
            let within = (pos - cell) as usize;
            let n = rest.len().min(self.cfg.extent_bytes - within);
            let (part, tail) = rest.split_at(n);
            let existing = self.with_catalog(|c| target.extent_at(c, cell))??;
            let merged;
            let content: &[u8] = match existing {
                Some(ext) if within > 0 || n < ext.len => {
                    let mut buf = self.read_extent(&ext)?;
                    if buf.len() < within + n {
                        buf.resize(within + n, 0);
                    }
                    buf[within..within + n].copy_from_slice(part);
                    merged = buf;
                    &merged
                }
                None if within > 0 => {
                    let mut buf = vec![0u8; within + n];
                    buf[within..].copy_from_slice(part);
                    merged = buf;
                    &merged
                }
                _ => part,
            };
            self.install_extent(target, cell, content, fence)?;
            pos += n as u64;
            rest = tail;
        }
        Ok(())
    }

    /// Writes `chunk` to fresh replicas and commits it as the extent at `logical`.
    fn install_extent(
        &self,
        target: &Target,
        logical: u64,
        chunk: &[u8],
        fence: u64,
    ) -> Result<(), NativeError> {
        let needed = self.cfg.placement.replicas;
        let order = self.placement_order(chunk.len() as u64, |_| false);
        if order.len() < needed {
            return Err(NativeError::InsufficientReplicas {
                needed,
                found: order.len(),
            });
        }
        // Write to the first `needed` nodes in parallel (each replica write is fsynced on its
        // node); a node that fails is replaced by the next eligible one in preference order.
        let mut replicas = Vec::new();
        let mut order = order.into_iter();
        while replicas.len() < needed {
            let batch: Vec<String> = order.by_ref().take(needed - replicas.len()).collect();
            if batch.is_empty() {
                break;
            }
            let placed: Vec<Result<Option<ReplicaRef>, NativeError>> = std::thread::scope(|s| {
                let handles: Vec<_> = batch
                    .iter()
                    .map(|node_id| s.spawn(|| self.place_replica(node_id, fence, chunk)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .unwrap_or(Err(NativeError::Poisoned("replica writer")))
                    })
                    .collect()
            });
            for r in placed {
                if let Some(r) = r? {
                    replicas.push(r);
                }
            }
        }
        if replicas.len() < needed {
            return Err(NativeError::InsufficientReplicas {
                needed,
                found: replicas.len(),
            });
        }
        let extent = ExtentRef {
            id: Uuid::new_v4().to_string(),
            logical_offset: logical,
            len: chunk.len(),
            checksum: checksum::sha256(chunk),
            replicas,
        };
        let cmd = match target {
            Target::Volume(volume_id) => MetaCommand::InstallExtent {
                volume_id: volume_id.to_string(),
                logical_offset: logical,
                extent,
            },
            Target::File { fs, ino } => MetaCommand::Fs {
                op: FsOp::InstallFileExtent {
                    fs: fs.to_string(),
                    ino: *ino,
                    logical_offset: logical,
                    size: logical + chunk.len() as u64,
                    extent,
                    now_ns: now_ns(),
                },
            },
        };
        self.commit(cmd, Some(fence))?;
        self.telemetry.record_write(chunk.len());
        Ok(())
    }

    /// Reads `len` bytes at any `offset` within the volume, across extents. Never-written ranges
    /// read as zeros.
    pub fn read(&self, volume_id: &str, offset: u64, len: usize) -> Result<Vec<u8>, NativeError> {
        let extents = self.with_catalog(|c| {
            let vol = c
                .volumes
                .get(volume_id)
                .ok_or_else(|| NativeError::NotFound(volume_id.into()))?;
            self.extents_in(c, &vol.extents, vol.size_bytes, offset, len)
        })??;
        self.read_range(extents, offset, len)
    }

    pub fn create_snapshot(
        &self,
        volume_id: &str,
        name: impl Into<String>,
    ) -> Result<SnapshotId, NativeError> {
        self.create_snapshot_as(Uuid::new_v4().to_string(), volume_id, name)
    }

    /// [`Self::create_snapshot`] with a caller-chosen id (idempotent).
    pub fn create_snapshot_as(
        &self,
        id: String,
        volume_id: &str,
        name: impl Into<String>,
    ) -> Result<SnapshotId, NativeError> {
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
        let extents = self.with_catalog(|c| {
            let s = c
                .snapshots
                .get(snapshot_id)
                .ok_or_else(|| NativeError::NotFound(snapshot_id.into()))?;
            let size = if s.size_bytes > 0 {
                s.size_bytes
            } else {
                c.written_end(&s.extents)
            };
            self.extents_in(c, &s.extents, size, offset, len)
        })??;
        self.read_range(extents, offset, len)
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

    /// Scrubs every extent (reads each replica and verifies its checksum) and re-replicates
    /// replicas that are unreachable or corrupt onto healthy nodes, preserving host diversity.
    /// Under Raft only the leader's engine can repair.
    pub fn repair_once(&self) -> Result<RepairStats, NativeError> {
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| NativeError::Poisoned("write"))?;
        let fence = match &self.meta {
            Meta::Local { .. } => 0,
            Meta::Raft { server, timeout } => server.leader_ready(*timeout)?,
        };
        let extents: Vec<ExtentRef> =
            self.with_catalog(|c| c.extents.values().map(|e| e.extent.clone()).collect())?;
        let mut st = RepairStats::default();
        for ext in extents {
            st.extents_checked += 1;
            let mut good: Option<Vec<u8>> = None;
            let mut bad = Vec::new();
            for r in &ext.replicas {
                let Some((node, device)) = self
                    .nodes
                    .iter()
                    .find(|n| n.spec.id == r.node_id && n.spec.healthy)
                    .and_then(|n| n.devices.get(r.device_index).map(|d| (n, d)))
                else {
                    bad.push(r.clone());
                    continue;
                };
                match device.read_exact_at(r.offset, ext.len) {
                    Ok(buf) if checksum::verify(&buf, &ext.checksum) => {
                        self.mark_up(node);
                        good.get_or_insert(buf);
                    }
                    Ok(_) => {
                        self.telemetry.checksum_failure();
                        bad.push(r.clone());
                    }
                    Err(_) => {
                        self.mark_down(node);
                        bad.push(r.clone());
                    }
                }
            }
            if bad.is_empty() {
                continue;
            }
            let Some(data) = good else {
                st.unrecoverable += 1;
                continue;
            };
            let mut current = ext.replicas.clone();
            for old in bad {
                let others: Vec<&ReplicaRef> = current.iter().filter(|r| **r != old).collect();
                let taken_nodes: BTreeSet<&str> =
                    others.iter().map(|r| r.node_id.as_str()).collect();
                let taken_hosts: BTreeSet<&str> = others
                    .iter()
                    .filter_map(|r| self.node(&r.node_id).ok())
                    .map(|n| n.spec.failure_domain.host.as_str())
                    .collect();
                let distinct = self.cfg.placement.require_distinct_hosts;
                let order = self.placement_order(ext.len as u64, |n| {
                    taken_nodes.contains(n.id.as_str())
                        || (distinct && taken_hosts.contains(n.failure_domain.host.as_str()))
                });
                let mut placed = None;
                for node_id in order {
                    if let Some(r) = self.place_replica(&node_id, fence, &data)? {
                        placed = Some(r);
                        break;
                    }
                }
                let Some(new) = placed else {
                    st.deferred += 1;
                    continue;
                };
                let cmd = MetaCommand::ReplaceReplica {
                    extent_id: ext.id.clone(),
                    old: old.clone(),
                    new: new.clone(),
                };
                match self.commit(cmd, Some(fence)) {
                    Ok(()) => {
                        st.replicas_repaired += 1;
                        self.telemetry.replica_repaired();
                        if let Some(slot) = current.iter_mut().find(|r| **r == old) {
                            *slot = new;
                        }
                    }
                    // The extent was reclaimed or rewritten meanwhile; the next pass sees the new state.
                    Err(NativeError::Metadata(_) | NativeError::Raft(RaftError::Rejected(_))) => {
                        st.deferred += 1;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(st)
    }

    /// Errors with "not the leader" unless this engine is the leader and has applied its whole
    /// log (so it serves every committed change, even right after an election). Always true for
    /// a local WAL.
    pub fn ensure_leader(&self) -> Result<(), NativeError> {
        if let Meta::Raft { server, timeout } = &self.meta {
            server.leader_ready(*timeout)?;
        }
        Ok(())
    }

    /// Volumes in the applied catalog, ordered by id.
    pub fn volumes(&self) -> Result<Vec<VolumeInfo>, NativeError> {
        self.with_catalog(|c| {
            c.volumes
                .values()
                .map(|v| VolumeInfo {
                    id: v.id.clone(),
                    name: v.name.clone(),
                    size_bytes: v.size_bytes,
                    extents: v.extents.len(),
                })
                .collect()
        })
    }

    /// Current per-node health.
    pub fn node_status(&self) -> Vec<NodeStatus> {
        self.nodes
            .iter()
            .map(|n| NodeStatus {
                id: n.spec.id.clone(),
                up: self.is_up(n),
                failures: n.health.failures.load(Ordering::Relaxed),
            })
            .collect()
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
            (
                "atlas_native_replica_write_failures_total",
                "Replica writes that failed and moved to another node.",
                t.replica_write_failures,
            ),
            (
                "atlas_native_replicas_repaired_total",
                "Replicas re-created by repair.",
                t.replicas_repaired,
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
            "Size of each reachable node's backing device.",
        );
        for n in &self.nodes {
            // An unreachable data node has no sample rather than failing the whole scrape.
            if let Ok(len) = n.devices[0].len() {
                p.sample(
                    "atlas_native_device_bytes",
                    &[("node", n.spec.id.as_str())],
                    len,
                );
            }
        }
        p.family(
            "atlas_native_node_up",
            "gauge",
            "1 if the node is eligible for placement, 0 while it is backed off after failures.",
        );
        for st in self.node_status() {
            p.sample(
                "atlas_native_node_up",
                &[("node", st.id.as_str())],
                u8::from(st.up),
            );
        }
        p.family(
            "atlas_native_node_failures_total",
            "counter",
            "I/O failures observed against each node.",
        );
        for st in self.node_status() {
            p.sample(
                "atlas_native_node_failures_total",
                &[("node", st.id.as_str())],
                st.failures,
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

    fn is_up(&self, n: &NodeRuntime) -> bool {
        n.spec.healthy
            && n.health
                .down_until
                .lock()
                .map(|d| d.is_none_or(|t| Instant::now() >= t))
                .unwrap_or(false)
    }

    fn mark_down(&self, n: &NodeRuntime) {
        n.health.failures.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut d) = n.health.down_until.lock() {
            *d = Some(Instant::now() + self.cfg.node_retry_after);
        }
    }

    fn mark_up(&self, n: &NodeRuntime) {
        if let Ok(mut d) = n.health.down_until.lock() {
            *d = None;
        }
    }

    /// Every eligible node not excluded by `skip`, in placement preference order (rack spread
    /// first, distinct hosts when the policy requires them).
    fn placement_order(&self, len: u64, skip: impl Fn(&Node) -> bool) -> Vec<String> {
        let specs: Vec<Node> = self
            .nodes
            .iter()
            .filter(|n| self.is_up(n) && !skip(&n.spec))
            .map(|n| n.spec.clone())
            .collect();
        let policy = PlacementPolicy {
            replicas: specs.len(),
            ..self.cfg.placement
        };
        select_replicas(&specs, len, policy)
    }

    /// Writes one replica of `data` to `node_id`, reusing free space when there is some. An I/O
    /// failure backs the node off and returns `None` so the caller can try the next node; a
    /// fenced write means this engine was deposed and is returned as an error.
    fn place_replica(
        &self,
        node_id: &str,
        fence: u64,
        data: &[u8],
    ) -> Result<Option<ReplicaRef>, NativeError> {
        let node = self.node(node_id)?;
        let free_off = self.with_catalog(|c| c.free.find(node_id, 0, data.len() as u64))?;
        let device = &node.devices[0];
        let written = match free_off {
            Some(off) => device.write_at(fence, off, data).map(|()| off),
            None => device.append(fence, data),
        };
        match written {
            Ok(offset) => {
                self.mark_up(node);
                Ok(Some(ReplicaRef {
                    node_id: node_id.to_string(),
                    device_index: 0,
                    offset,
                }))
            }
            Err(e @ NativeError::Fenced { .. }) => Err(e),
            Err(_) => {
                self.mark_down(node);
                self.telemetry.replica_write_failure();
                Ok(None)
            }
        }
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

    /// The extents of a map overlapping `[offset, offset + len)`, which must lie within `size`.
    fn extents_in(
        &self,
        c: &Catalog,
        extents: &std::collections::BTreeMap<u64, String>,
        size: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<ExtentRef>, NativeError> {
        let end = offset
            .checked_add(len as u64)
            .filter(|e| *e <= size)
            .ok_or_else(|| NativeError::Invalid("read exceeds volume size".into()))?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let first = offset - offset % self.cfg.extent_bytes as u64;
        extents
            .range(first..end)
            .map(|(_, eid)| {
                c.extents
                    .get(eid)
                    .map(|m| m.extent.clone())
                    .ok_or_else(|| NativeError::NotFound(eid.clone()))
            })
            .collect()
    }

    /// One past the last byte any extent of the map covers.
    fn read_range(
        &self,
        extents: Vec<ExtentRef>,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NativeError> {
        let mut out = vec![0u8; len];
        let end = offset + len as u64;
        for ext in extents {
            let start = ext.logical_offset.max(offset);
            let stop = (ext.logical_offset + ext.len as u64).min(end);
            if start >= stop {
                continue;
            }
            let buf = self.read_extent(&ext)?;
            out[(start - offset) as usize..(stop - offset) as usize].copy_from_slice(
                &buf[(start - ext.logical_offset) as usize..(stop - ext.logical_offset) as usize],
            );
        }
        Ok(out)
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

    /// The whole extent from the first replica whose checksum verifies.
    fn read_extent(&self, ext: &ExtentRef) -> Result<Vec<u8>, NativeError> {
        let mut order: Vec<(usize, &ReplicaRef, &NodeRuntime)> = ext
            .replicas
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                self.nodes
                    .iter()
                    .find(|n| n.spec.id == r.node_id && n.spec.healthy)
                    .map(|n| (i, r, n))
            })
            .collect();
        // Backed-off nodes are tried last rather than skipped: they may be the only copy left.
        order.sort_by_key(|(_, _, n)| !self.is_up(n));
        for (i, r, node) in order {
            let Some(device) = node.devices.get(r.device_index) else {
                continue;
            };
            match device.read_exact_at(r.offset, ext.len) {
                Ok(buf) if checksum::verify(&buf, &ext.checksum) => {
                    self.mark_up(node);
                    if i > 0 {
                        self.telemetry.replica_fallback();
                    }
                    self.telemetry.record_read(buf.len());
                    return Ok(buf);
                }
                Ok(_) => self.telemetry.checksum_failure(),
                Err(_) => self.mark_down(node),
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
