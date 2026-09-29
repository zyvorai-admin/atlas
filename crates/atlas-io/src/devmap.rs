// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Map kernel `major:minor` to an Atlas volume id and a human device name.

use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct DeviceMap {
    by_dev: HashMap<u64, Binding>,
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub name: String,
    pub volume_id: Option<String>,
}

impl DeviceMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, major: u32, minor: u32, name: impl Into<String>, volume_id: Option<String>) {
        self.by_dev.insert(
            key(major, minor),
            Binding {
                name: name.into(),
                volume_id,
            },
        );
    }

    pub fn resolve(&self, major: u32, minor: u32) -> Binding {
        self.by_dev.get(&key(major, minor)).cloned().unwrap_or(Binding {
            name: format!("{major}:{minor}"),
            volume_id: None,
        })
    }

    /// Seed a lab-shaped map used by the fake agent and unit tests.
    pub fn lab() -> Self {
        let mut m = Self::new();
        m.insert(8, 16, "rbd0", Some("vol_vm_web01".into()));
        m.insert(8, 32, "zd0", Some("vol_zfs_tank0".into()));
        m
    }

    pub fn len(&self) -> usize {
        self.by_dev.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_dev.is_empty()
    }
}

fn key(major: u32, minor: u32) -> u64 {
    ((major as u64) << 32) | minor as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_falls_back_to_majmin() {
        let m = DeviceMap::new();
        let b = m.resolve(8, 1);
        assert_eq!(b.name, "8:1");
        assert!(b.volume_id.is_none());
    }

    #[test]
    fn lab_binds_rbd() {
        let m = DeviceMap::lab();
        assert_eq!(m.resolve(8, 16).volume_id.as_deref(), Some("vol_vm_web01"));
    }
}
