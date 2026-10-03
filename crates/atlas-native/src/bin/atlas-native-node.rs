// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! `atlas-native-node --config <file.json>`: runs a native storage node (see `docs/NATIVE_NODE.md`).

use std::{process::ExitCode, thread, time::Duration};

use atlas_native::node::{NativeNode, NodeConfig};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let path = match args.as_slice() {
        [_, flag, path] if flag == "--config" => path,
        _ => {
            eprintln!("usage: atlas-native-node --config <file.json>");
            return ExitCode::from(2);
        }
    };
    let cfg = match NodeConfig::from_file(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("atlas-native-node: invalid config {path}: {e}");
            return ExitCode::from(2);
        }
    };
    let node = match NativeNode::start(cfg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("atlas-native-node: failed to start: {e}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!(
        "atlas-native-node {}: http={} data_node={:?} metadata={:?}",
        node.id(),
        node.http_addr(),
        node.data_node_addr(),
        node.metadata_addr()
    );
    // Raft state and data are crash-safe on disk, so termination needs no graceful path; a fatal
    // storage error exits non-zero so the supervisor restarts the node from disk.
    loop {
        if let Some(err) = node.fatal_error() {
            eprintln!("atlas-native-node {}: fatal: {err}", node.id());
            return ExitCode::FAILURE;
        }
        thread::sleep(Duration::from_secs(1));
    }
}
