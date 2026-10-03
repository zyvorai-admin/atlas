// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! In-memory stand-in for a healthy three-node native cluster.

use std::collections::{BTreeMap, BTreeSet};
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
    snapshots: BTreeSet<String>,
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

    async fn create_volume(&self, name: &str, size_bytes: u64) -> Result<String, DriverError> {
        let mut s = lock(&self.state)?;
        if s.volumes.values().any(|v| v.name == name) {
            return Err(DriverError::Backend(format!(
                "volume {name} already exists"
            )));
        }
        s.next += 1;
        let id = format!("fake-vol-{}", s.next);
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

    async fn create_snapshot(&self, volume_id: &str, _name: &str) -> Result<String, DriverError> {
        let mut s = lock(&self.state)?;
        if !s.volumes.contains_key(volume_id) {
            return Err(DriverError::Backend(format!(
                "not found: volume {volume_id}"
            )));
        }
        s.next += 1;
        let id = format!("fake-snap-{}", s.next);
        s.snapshots.insert(id.clone());
        Ok(id)
    }

    async fn delete_snapshot(&self, id: &str) -> Result<(), DriverError> {
        if lock(&self.state)?.snapshots.remove(id) {
            Ok(())
        } else {
            Err(DriverError::Backend(format!("not found: snapshot {id}")))
        }
    }
}
