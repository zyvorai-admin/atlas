// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! FUSE client for atlas-native filesystems (binary `atlas-native-mount`). [`ops::Ops`] holds
//! the logic (caching, write-back, leader retry) and is tested without a kernel; the `fuse`
//! feature adds the Linux kernel adapter.

pub mod cache;
pub mod client;
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub mod fuse;
pub mod ops;
pub mod writeback;
