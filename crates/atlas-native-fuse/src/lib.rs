// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! FUSE client for atlas-native filesystems (binary `atlas-native-mount`). [`ops::Ops`] holds
//! the logic (caching, write-back, leader retry) and is tested without a kernel; the `fuse`
//! feature adds the Linux kernel adapter.

pub mod cache;
pub mod client;
pub mod dirs;
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub mod fuse;
pub mod locks;
pub mod ops;
pub mod pipeline;
pub mod writeback;
