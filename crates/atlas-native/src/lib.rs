// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Atlas Native distributed-storage core.
//!
//! Phase 2 adds a durable metadata command log, replayable state machine,
//! extent reference counting and safe metadata reclamation. The command-log
//! boundary is intentionally compatible with a future multi-node Raft layer:
//! Raft will replicate `MetaCommand`; the state machine below does not need to
//! change when consensus is introduced.

pub mod checksum;
pub mod device;
pub mod engine;
pub mod gc;
pub mod metadata;
pub mod placement;
pub mod telemetry;
pub mod wal;

pub use device::{DeviceId, FileDevice};
pub use engine::{EngineConfig, NativeEngine, NativeError};
pub use metadata::{SnapshotId, VolumeId};
pub use placement::{FailureDomain, Node, PlacementPolicy};
