// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Observe-first storage I/O sensor for Atlas.
//!
//! The control plane stays intent-based. This crate is an optional node agent that
//! joins block-layer issue/complete events, attributes them to a volume via a
//! device map, and feeds histograms + RCA. Live eBPF attach is opt-in
//! (`ATLAS_IO_MODE=live`); CI and `make run` use the in-process fake source so
//! nothing here requires `CAP_BPF`.
//!
//! Enforcement (`write_freeze` leases) is time-limited and fails open.

pub mod collector;
pub mod devmap;
pub mod hist;
pub mod http;
pub mod lease;
pub mod rca;
pub mod source;

pub use collector::Collector;
pub use source::{FakeSource, IoSource};

/// Default pin directory advertised in health (maps are not loaded in fake mode).
pub const PIN_DIR: &str = "/sys/fs/bpf/atlas";

/// Hard cap on (dev, op) histogram keys and workload keys.
pub const CARDINALITY_CAP: u64 = 4096;

/// Log2-microsecond bucket count (1µs … ~35s).
pub const HIST_BUCKETS: usize = 26;
