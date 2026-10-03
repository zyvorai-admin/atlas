// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct NativeIoCounters {
    reads: AtomicU64,
    writes: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    checksum_failures: AtomicU64,
    replica_fallbacks: AtomicU64,
    gc_reclaimed: AtomicU64,
    replica_write_failures: AtomicU64,
    replicas_repaired: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub checksum_failures: u64,
    pub replica_fallbacks: u64,
    pub gc_reclaimed: u64,
    pub replica_write_failures: u64,
    pub replicas_repaired: u64,
}

impl NativeIoCounters {
    pub fn record_read(&self, bytes: usize) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn record_write(&self, bytes: usize) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn checksum_failure(&self) {
        self.checksum_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn replica_fallback(&self) {
        self.replica_fallbacks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn gc_reclaimed(&self, extents: u64) {
        self.gc_reclaimed.fetch_add(extents, Ordering::Relaxed);
    }
    /// A replica write that failed and was retried on another node.
    pub fn replica_write_failure(&self) {
        self.replica_write_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn replica_repaired(&self) {
        self.replicas_repaired.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> NativeIoSnapshot {
        NativeIoSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            replica_fallbacks: self.replica_fallbacks.load(Ordering::Relaxed),
            gc_reclaimed: self.gc_reclaimed.load(Ordering::Relaxed),
            replica_write_failures: self.replica_write_failures.load(Ordering::Relaxed),
            replicas_repaired: self.replicas_repaired.load(Ordering::Relaxed),
        }
    }
}
