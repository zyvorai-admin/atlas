// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use atlas_native::{EngineConfig, FailureDomain, NativeEngine, Node};

fn test_root(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("atlas-native-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn nodes() -> Vec<Node> {
    (0..3)
        .map(|i| Node {
            id: format!("n{i}"),
            failure_domain: FailureDomain {
                zone: "z1".into(),
                rack: format!("r{i}"),
                host: format!("h{i}"),
            },
            free_bytes: 1 << 30,
            healthy: true,
        })
        .collect()
}

#[test]
fn replicated_write_and_read_round_trip() {
    let root = test_root("rw");
    let e = NativeEngine::open(EngineConfig::new(&root), nodes()).unwrap();
    let v = e.create_volume("train", 16 * 1024 * 1024).unwrap();
    let payload = vec![0x5a; 4096];
    e.write(&v, 0, &payload).unwrap();
    assert_eq!(e.read(&v, 0, payload.len()).unwrap(), payload);
    assert_eq!(e.telemetry.snapshot().writes, 1);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn snapshot_is_copy_on_write_metadata() {
    let root = test_root("snap");
    let e = NativeEngine::open(EngineConfig::new(&root), nodes()).unwrap();
    let v = e.create_volume("train", 16 * 1024 * 1024).unwrap();
    e.write(&v, 0, b"before").unwrap();
    let s = e.create_snapshot(&v, "s1").unwrap();
    e.write(&v, 0, b"after!").unwrap();
    assert_eq!(e.read(&v, 0, 6).unwrap(), b"after!");
    assert_eq!(e.read_snapshot(&s, 0, 6).unwrap(), b"before");
    let _ = std::fs::remove_dir_all(root);
}
