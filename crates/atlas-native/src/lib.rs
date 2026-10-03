// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Atlas Native: an append-only replicated extent engine for Atlas.
//!
//! Phase 1 deliberately keeps the metadata plane local and deterministic while
//! establishing the on-disk/data invariants that later Raft/RDMA/SPDK layers can reuse.

pub mod checksum;
pub mod device;
pub mod engine;
pub mod placement;
pub mod telemetry;

pub use device::{DeviceId, FileDevice};
pub use engine::{EngineConfig, NativeEngine, NativeError, SnapshotId, VolumeId};
pub use placement::{FailureDomain, Node, PlacementPolicy};
