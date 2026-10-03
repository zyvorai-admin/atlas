// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Atlas Native distributed-storage core.
//!
//! Phase 2 added a durable metadata command log, replayable state machine, extent reference
//! counting and safe metadata reclamation. Phase 3 adds WAL checkpoint/compaction, device-space
//! free lists so reclaimed extents are physically reused, and a sans-IO Raft core ([`raft`]) that
//! replicates the same `MetaCommand` log across metadata replicas and applies it through the same
//! `Catalog` state machine. Phase 4 puts replicas on other hosts through [`data_node`] and lets
//! [`NativeEngine`] commit through Raft ([`MetaBackend::Raft`]); both transports support mutual TLS
//! ([`tls`]).

pub mod alloc;
pub mod checksum;
pub mod data_node;
pub mod device;
mod durable;
pub mod engine;
pub mod gc;
pub mod membership;
pub mod metadata;
pub mod metrics;
pub mod placement;
pub mod raft;
pub mod raft_server;
pub mod telemetry;
pub mod tls;
pub mod wal;

pub use alloc::{FreeList, FreeRange};
pub use data_node::{DataNodeServer, RemoteDevice};
pub use device::{BlockStore, DeviceId, FileDevice};
pub use engine::{EngineConfig, MetaBackend, NativeEngine, NativeError};
pub use gc::GcStats;
pub use membership::Membership;
pub use metadata::{Catalog, MetaCommand, SnapshotId, VolumeId};
pub use placement::{FailureDomain, Node, PlacementPolicy};
pub use raft::{Envelope, Message, RaftConfig, RaftCounters, RaftError, RaftNode, Role};
pub use raft_server::{RaftServer, RaftStatus};
pub use tls::TlsIdentity;
