// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;

use atlas_native::{EngineConfig, FailureDomain, NativeEngine, Node};

fn nodes() -> Vec<Node> {
    (1..=3)
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

fn cfg(root: &std::path::Path) -> EngineConfig {
    let mut c = EngineConfig::new(root);
    c.extent_bytes = 4096;
    c
}

#[test]
fn reclaimed_extent_space_is_reused() {
    let td = tempfile::tempdir().unwrap();
    let e = NativeEngine::open(cfg(td.path()), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();

    e.write(&v, 0, &[1u8; 4096]).unwrap();
    e.write(&v, 0, &[2u8; 4096]).unwrap();
    assert_eq!(e.device_len("n1").unwrap(), 8192);

    let st = e.gc_once().unwrap();
    assert_eq!(st.reclaimed, 1);
    assert_eq!(st.freed_bytes, 3 * 4096);

    e.write(&v, 0, &[3u8; 4096]).unwrap();
    for n in ["n1", "n2", "n3"] {
        assert_eq!(
            e.device_len(n).unwrap(),
            8192,
            "{n} grew instead of reusing"
        );
    }
    assert_eq!(e.free_bytes().unwrap(), 0);
    assert_eq!(e.read(&v, 0, 4096).unwrap(), vec![3u8; 4096]);

    drop(e);
    let e = NativeEngine::open(cfg(td.path()), nodes()).unwrap();
    assert_eq!(e.read(&v, 0, 4096).unwrap(), vec![3u8; 4096]);
    assert_eq!(e.free_bytes().unwrap(), 0);
}

#[test]
fn partial_reuse_splits_free_range() {
    let td = tempfile::tempdir().unwrap();
    let e = NativeEngine::open(cfg(td.path()), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, &[1u8; 4096]).unwrap();
    e.write(&v, 0, &[2u8; 4096]).unwrap();
    e.gc_once().unwrap();

    e.write(&v, 4096, &[9u8; 1000]).unwrap();
    assert_eq!(e.free_bytes().unwrap(), 3 * (4096 - 1000));
    assert_eq!(e.device_len("n1").unwrap(), 8192);
    assert_eq!(e.read(&v, 4096, 1000).unwrap(), vec![9u8; 1000]);
    assert_eq!(e.read(&v, 0, 4096).unwrap(), vec![2u8; 4096]);
}

#[test]
fn snapshot_protects_space_from_reuse() {
    let td = tempfile::tempdir().unwrap();
    let e = NativeEngine::open(cfg(td.path()), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, &[0xa; 4096]).unwrap();
    let s = e.create_snapshot(&v, "s").unwrap();
    e.write(&v, 0, &[0xb; 4096]).unwrap();
    assert_eq!(e.gc_once().unwrap().reclaimed, 0);
    e.write(&v, 0, &[0xc; 4096]).unwrap();
    assert_eq!(e.read_snapshot(&s, 0, 4096).unwrap(), vec![0xa; 4096]);
    assert_eq!(e.read(&v, 0, 4096).unwrap(), vec![0xc; 4096]);
}

#[test]
fn wal_compacts_and_state_survives_reopen() {
    let td = tempfile::tempdir().unwrap();
    let mut c = cfg(td.path());
    c.wal_compact_after = 4;
    let e = NativeEngine::open(c.clone(), nodes()).unwrap();
    let v = e.create_volume("v", 64 * 1024).unwrap();
    for i in 0..10u8 {
        e.write(&v, i as u64 * 4096, &[i; 16]).unwrap();
    }
    assert!(e.wal_records().unwrap() < 4);
    let applied = e.applied_index().unwrap();
    drop(e);

    let e = NativeEngine::open(c, nodes()).unwrap();
    assert_eq!(e.applied_index().unwrap(), applied);
    for i in 0..10u8 {
        assert_eq!(e.read(&v, i as u64 * 4096, 16).unwrap(), vec![i; 16]);
    }
    e.create_volume("w", 4096).unwrap();
    assert_eq!(e.applied_index().unwrap(), applied + 1);
}

#[test]
fn checkpoint_empties_wal_and_keeps_indexes_monotonic() {
    let td = tempfile::tempdir().unwrap();
    let mut c = cfg(td.path());
    c.wal_compact_after = 0;
    let e = NativeEngine::open(c.clone(), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, b"x").unwrap();
    assert_eq!(e.checkpoint().unwrap(), 2);
    assert_eq!(e.wal_records().unwrap(), 0);
    drop(e);

    let e = NativeEngine::open(c, nodes()).unwrap();
    assert_eq!(e.applied_index().unwrap(), 2);
    e.create_volume("w", 4096).unwrap();
    assert_eq!(e.applied_index().unwrap(), 3);
    assert_eq!(e.read(&v, 0, 1).unwrap(), b"x");
}

#[test]
fn replays_wal_after_crash_before_checkpoint() {
    let td = tempfile::tempdir().unwrap();
    let mut c = cfg(td.path());
    c.wal_compact_after = 0;
    let catalog = td.path().join("catalog.json");

    let e = NativeEngine::open(c.clone(), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, b"one").unwrap();
    let stale = std::fs::read(&catalog).unwrap();
    e.write(&v, 4096, b"two").unwrap();
    let s = e.create_snapshot(&v, "s").unwrap();
    let applied = e.applied_index().unwrap();
    drop(e);

    // Simulate a crash after the WAL fsyncs but before catalog.json is replaced.
    std::fs::write(&catalog, stale).unwrap();

    let e = NativeEngine::open(c, nodes()).unwrap();
    assert_eq!(e.applied_index().unwrap(), applied);
    assert_eq!(e.read(&v, 0, 3).unwrap(), b"one");
    assert_eq!(e.read(&v, 4096, 3).unwrap(), b"two");
    assert_eq!(e.read_snapshot(&s, 4096, 3).unwrap(), b"two");
}

#[test]
fn torn_wal_tail_is_discarded() {
    let td = tempfile::tempdir().unwrap();
    let c = cfg(td.path());
    let e = NativeEngine::open(c.clone(), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, b"ok").unwrap();
    drop(e);

    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(td.path().join("wal/metadata.wal"))
        .unwrap();
    wal.write_all(br#"{"term":1,"index":3,"comm"#).unwrap();
    drop(wal);

    let e = NativeEngine::open(c, nodes()).unwrap();
    assert_eq!(e.read(&v, 0, 2).unwrap(), b"ok");
    e.write(&v, 4096, b"next").unwrap();
    assert_eq!(e.read(&v, 4096, 4).unwrap(), b"next");
}

#[test]
fn engine_metrics_reflect_io_gc_and_free_space() {
    let td = tempfile::tempdir().unwrap();
    let e = NativeEngine::open(cfg(td.path()), nodes()).unwrap();
    let v = e.create_volume("v", 8192).unwrap();
    e.write(&v, 0, &[1u8; 4096]).unwrap();
    e.write(&v, 0, &[2u8; 4096]).unwrap();
    e.gc_once().unwrap();
    e.read(&v, 0, 4096).unwrap();

    let m = e.render_metrics().unwrap();
    for want in [
        "# TYPE atlas_native_writes_total counter",
        "atlas_native_writes_total 2\n",
        "atlas_native_reads_total 1\n",
        "atlas_native_gc_reclaimed_extents_total 1\n",
        "atlas_native_allocator_free_bytes 12288\n",
        "atlas_native_extents 1\n",
        "atlas_native_volumes 1\n",
        "atlas_native_device_bytes{node=\"n1\"} 8192\n",
    ] {
        assert!(m.contains(want), "missing {want:?} in:\n{m}");
    }
}
