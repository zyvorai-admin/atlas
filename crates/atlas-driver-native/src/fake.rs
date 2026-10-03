// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! In-memory stand-in for a healthy three-node native cluster.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use atlas_driver_core::DriverError;

use crate::{DataNodeInfo, Layout, NativeApi, NativeVolume, NodeStatus, RaftInfo};

#[derive(Default)]
pub struct FakeApi {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next: u64,
    volumes: BTreeMap<String, NativeVolume>,
    /// Snapshot id → the volume as it was (size, extents).
    snapshots: BTreeMap<String, (u64, u64)>,
}

fn lock(m: &Mutex<State>) -> Result<std::sync::MutexGuard<'_, State>, DriverError> {
    m.lock()
        .map_err(|_| DriverError::Backend("fake native state poisoned".into()))
}

#[async_trait]
impl NativeApi for FakeApi {
    async fn status(&self) -> Result<NodeStatus, DriverError> {
        Ok(NodeStatus {
            node_id: "fake-0".into(),
            metadata: Some(RaftInfo {
                role: "leader".into(),
                term: 1,
                leader: Some("fake-0".into()),
                commit_index: lock(&self.state)?.next,
            }),
            layout: Some(Layout {
                extent_bytes: 4 << 20,
                replicas: 3,
            }),
            data_nodes: Some(
                (0..3)
                    .map(|i| DataNodeInfo {
                        id: format!("fake-{i}"),
                        up: true,
                    })
                    .collect(),
            ),
        })
    }

    async fn volumes(&self) -> Result<Vec<NativeVolume>, DriverError> {
        Ok(lock(&self.state)?.volumes.values().cloned().collect())
    }

    async fn create_volume(
        &self,
        id: &str,
        name: &str,
        size_bytes: u64,
    ) -> Result<String, DriverError> {
        let mut s = lock(&self.state)?;
        if s.volumes.contains_key(id) {
            return Ok(id.to_string());
        }
        if s.volumes.values().any(|v| v.name == name) {
            return Err(DriverError::Backend(format!(
                "volume {name} already exists"
            )));
        }
        s.next += 1;
        let id = id.to_string();
        s.volumes.insert(
            id.clone(),
            NativeVolume {
                id: id.clone(),
                name: name.into(),
                size_bytes,
                extents: 0,
            },
        );
        Ok(id)
    }

    async fn delete_volume(&self, id: &str) -> Result<(), DriverError> {
        lock(&self.state)?
            .volumes
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| DriverError::Backend(format!("not found: volume {id}")))
    }

    async fn resize_volume(&self, id: &str, size_bytes: u64) -> Result<(), DriverError> {
        let mut s = lock(&self.state)?;
        let v = s
            .volumes
            .get_mut(id)
            .ok_or_else(|| DriverError::Backend(format!("not found: volume {id}")))?;
        if size_bytes < v.size_bytes {
            return Err(DriverError::Backend(format!("volume {id} cannot shrink")));
        }
        v.size_bytes = size_bytes;
        Ok(())
    }

    async fn create_snapshot(
        &self,
        id: &str,
        volume_id: &str,
        _name: &str,
    ) -> Result<String, DriverError> {
        let mut s = lock(&self.state)?;
        let v = s
            .volumes
            .get(volume_id)
            .map(|v| (v.size_bytes, v.extents))
            .ok_or_else(|| DriverError::Backend(format!("not found: volume {volume_id}")))?;
        s.next += 1;
        s.snapshots.insert(id.to_string(), v);
        Ok(id.to_string())
    }

    async fn clone_snapshot(
        &self,
        id: &str,
        snapshot_id: &str,
        name: &str,
        size_bytes: Option<u64>,
    ) -> Result<String, DriverError> {
        let (size, extents) = *lock(&self.state)?
            .snapshots
            .get(snapshot_id)
            .ok_or_else(|| DriverError::Backend(format!("not found: snapshot {snapshot_id}")))?;
        let size_bytes = size_bytes.unwrap_or(size);
        if size_bytes < size {
            return Err(DriverError::Backend(format!(
                "clone of {snapshot_id} needs at least {size} bytes"
            )));
        }
        let id = self.create_volume(id, name, size_bytes).await?;
        if let Some(v) = lock(&self.state)?.volumes.get_mut(&id) {
            v.extents = extents;
        }
        Ok(id)
    }

    async fn delete_snapshot(&self, id: &str) -> Result<(), DriverError> {
        if lock(&self.state)?.snapshots.remove(id).is_some() {
            Ok(())
        } else {
            Err(DriverError::Backend(format!("not found: snapshot {id}")))
        }
    }
}
