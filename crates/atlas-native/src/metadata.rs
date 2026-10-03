// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::alloc::FreeList;

pub type VolumeId = String;
pub type SnapshotId = String;
pub type ExtentId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRef {
    pub node_id: String,
    pub device_index: usize,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtentRef {
    pub id: ExtentId,
    pub logical_offset: u64,
    pub len: usize,
    pub checksum: [u8; 32],
    pub replicas: Vec<ReplicaRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMeta {
    pub id: VolumeId,
    pub name: String,
    pub size_bytes: u64,
    pub extents: BTreeMap<u64, ExtentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: SnapshotId,
    pub volume_id: VolumeId,
    pub name: String,
    pub extents: BTreeMap<u64, ExtentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentMeta {
    pub extent: ExtentRef,
    pub refs: u64,
    pub tombstoned: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub volumes: BTreeMap<VolumeId, VolumeMeta>,
    pub snapshots: BTreeMap<SnapshotId, SnapshotMeta>,
    pub extents: BTreeMap<ExtentId, ExtentMeta>,
    pub applied_index: u64,
    pub current_term: u64,
    #[serde(default)]
    pub free: FreeList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetaCommand {
    CreateVolume {
        id: VolumeId,
        name: String,
        size_bytes: u64,
    },
    InstallExtent {
        volume_id: VolumeId,
        logical_offset: u64,
        extent: ExtentRef,
    },
    CreateSnapshot {
        id: SnapshotId,
        volume_id: VolumeId,
        name: String,
    },
    DeleteSnapshot {
        snapshot_id: SnapshotId,
    },
    DeleteVolume {
        volume_id: VolumeId,
    },
    MarkExtentReclaimed {
        extent_id: ExtentId,
    },
    /// Moves one replica of an extent to `new` (already written and verified by the repairer):
    /// reserves the new range and returns the old one to the free list.
    ReplaceReplica {
        extent_id: ExtentId,
        old: ReplicaRef,
        new: ReplicaRef,
    },
    /// Appended by a new Raft leader so entries from earlier terms can be committed.
    Noop,
}

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("invalid metadata transition: {0}")]
    Invalid(String),
}

impl Catalog {
    pub fn apply(
        &mut self,
        term: u64,
        index: u64,
        cmd: &MetaCommand,
    ) -> Result<Vec<ExtentId>, MetaError> {
        if index <= self.applied_index {
            return Ok(Vec::new());
        }
        let mut gc_candidates = Vec::new();
        match cmd {
            MetaCommand::CreateVolume {
                id,
                name,
                size_bytes,
            } => {
                self.volumes.insert(
                    id.clone(),
                    VolumeMeta {
                        id: id.clone(),
                        name: name.clone(),
                        size_bytes: *size_bytes,
                        extents: BTreeMap::new(),
                    },
                );
            }
            MetaCommand::InstallExtent {
                volume_id,
                logical_offset,
                extent,
            } => {
                let old = {
                    let vol = self
                        .volumes
                        .get_mut(volume_id)
                        .ok_or_else(|| MetaError::NotFound(volume_id.clone()))?;
                    vol.extents.insert(*logical_offset, extent.id.clone())
                };
                if !self.extents.contains_key(&extent.id) {
                    for r in &extent.replicas {
                        self.free
                            .reserve(&r.node_id, r.device_index, r.offset, extent.len as u64);
                    }
                }
                self.extents
                    .entry(extent.id.clone())
                    .and_modify(|e| e.refs += 1)
                    .or_insert(ExtentMeta {
                        extent: extent.clone(),
                        refs: 1,
                        tombstoned: false,
                    });
                if let Some(old_id) = old {
                    self.dec_ref(&old_id, &mut gc_candidates)?;
                }
            }
            MetaCommand::CreateSnapshot {
                id,
                volume_id,
                name,
            } => {
                let vol = self
                    .volumes
                    .get(volume_id)
                    .ok_or_else(|| MetaError::NotFound(volume_id.clone()))?
                    .clone();
                for eid in vol.extents.values() {
                    let e = self
                        .extents
                        .get_mut(eid)
                        .ok_or_else(|| MetaError::NotFound(eid.clone()))?;
                    e.refs += 1;
                }
                self.snapshots.insert(
                    id.clone(),
                    SnapshotMeta {
                        id: id.clone(),
                        volume_id: volume_id.clone(),
                        name: name.clone(),
                        extents: vol.extents,
                    },
                );
            }
            MetaCommand::DeleteSnapshot { snapshot_id } => {
                let s = self
                    .snapshots
                    .remove(snapshot_id)
                    .ok_or_else(|| MetaError::NotFound(snapshot_id.clone()))?;
                for eid in s.extents.values() {
                    self.dec_ref(eid, &mut gc_candidates)?;
                }
            }
            MetaCommand::DeleteVolume { volume_id } => {
                let v = self
                    .volumes
                    .remove(volume_id)
                    .ok_or_else(|| MetaError::NotFound(volume_id.clone()))?;
                for eid in v.extents.values() {
                    self.dec_ref(eid, &mut gc_candidates)?;
                }
            }
            MetaCommand::MarkExtentReclaimed { extent_id } => {
                let e = self
                    .extents
                    .get(extent_id)
                    .ok_or_else(|| MetaError::NotFound(extent_id.clone()))?;
                if e.refs != 0 {
                    return Err(MetaError::Invalid(format!(
                        "extent {extent_id} still has {} refs",
                        e.refs
                    )));
                }
                let e = self.extents.remove(extent_id).expect("checked above");
                for r in &e.extent.replicas {
                    self.free
                        .release(&r.node_id, r.device_index, r.offset, e.extent.len as u64)
                        .map_err(MetaError::Invalid)?;
                }
            }
            MetaCommand::ReplaceReplica {
                extent_id,
                old,
                new,
            } => {
                if old == new {
                    return Err(MetaError::Invalid(
                        "replacement replica is the same range".into(),
                    ));
                }
                let e = self
                    .extents
                    .get_mut(extent_id)
                    .ok_or_else(|| MetaError::NotFound(extent_id.clone()))?;
                let pos = e
                    .extent
                    .replicas
                    .iter()
                    .position(|r| r == old)
                    .ok_or_else(|| {
                        MetaError::Invalid(format!(
                            "extent {extent_id} has no replica on {} at {}",
                            old.node_id, old.offset
                        ))
                    })?;
                if e.extent
                    .replicas
                    .iter()
                    .enumerate()
                    .any(|(i, r)| i != pos && r.node_id == new.node_id)
                {
                    return Err(MetaError::Invalid(format!(
                        "extent {extent_id} already has a replica on {}",
                        new.node_id
                    )));
                }
                let len = e.extent.len as u64;
                e.extent.replicas[pos] = new.clone();
                self.free
                    .reserve(&new.node_id, new.device_index, new.offset, len);
                self.free
                    .release(&old.node_id, old.device_index, old.offset, len)
                    .map_err(MetaError::Invalid)?;
            }
            MetaCommand::Noop => {}
        }
        self.current_term = term;
        self.applied_index = index;
        Ok(gc_candidates)
    }

    /// Applies an already-committed entry. A command that fails validation still consumes its
    /// index and leaves the catalog otherwise untouched, so every replica that applies the same
    /// log reaches the same state.
    pub fn apply_committed(
        &mut self,
        term: u64,
        index: u64,
        cmd: &MetaCommand,
    ) -> Result<Vec<ExtentId>, MetaError> {
        let mut next = self.clone();
        match next.apply(term, index, cmd) {
            Ok(gc) => {
                *self = next;
                Ok(gc)
            }
            Err(e) => {
                if index > self.applied_index {
                    self.applied_index = index;
                    self.current_term = term;
                }
                Err(e)
            }
        }
    }

    fn dec_ref(&mut self, extent_id: &str, gc: &mut Vec<ExtentId>) -> Result<(), MetaError> {
        let e = self
            .extents
            .get_mut(extent_id)
            .ok_or_else(|| MetaError::NotFound(extent_id.to_string()))?;
        if e.refs == 0 {
            return Err(MetaError::Invalid(format!(
                "extent {extent_id} refcount underflow"
            )));
        }
        e.refs -= 1;
        if e.refs == 0 {
            gc.push(extent_id.to_string());
        }
        Ok(())
    }
}
