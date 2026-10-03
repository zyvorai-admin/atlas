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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub checksum_failures: u64,
    pub replica_fallbacks: u64,
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
    pub fn snapshot(&self) -> NativeIoSnapshot {
        NativeIoSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            replica_fallbacks: self.replica_fallbacks.load(Ordering::Relaxed),
        }
    }
}
