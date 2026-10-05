// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Engine data-path throughput over real data-node connections on localhost (3 replicas).
//! `cargo test --release -p atlas-native --test datapath_bench -- --ignored --nocapture`

use std::{
    net::TcpListener,
    sync::Arc,
    time::{Duration, Instant},
};

use atlas_native::{
    BlockStore, DataNodeServer, EngineConfig, FailureDomain, MetaBackend, NativeEngine, Node,
    RemoteDevice,
};

const MIB: usize = 1 << 20;

/// An engine over three data nodes on localhost, with 1 MiB extents.
fn engine(td: &std::path::Path) -> (Vec<DataNodeServer>, NativeEngine) {
    let servers: Vec<DataNodeServer> = (1..=3)
        .map(|i| {
            DataNodeServer::start(
                format!("n{i}"),
                td.join(format!("dn{i}")),
                TcpListener::bind("127.0.0.1:0").unwrap(),
            )
            .unwrap()
        })
        .collect();
    let stores: Vec<(Node, Arc<dyn BlockStore>)> = servers
        .iter()
        .enumerate()
        .map(|(i, s)| {
            (
                Node {
                    id: format!("n{}", i + 1),
                    failure_domain: FailureDomain {
                        zone: "z1".into(),
                        rack: format!("r{i}"),
                        host: format!("h{i}"),
                    },
                    free_bytes: 1 << 40,
                    healthy: true,
                },
                Arc::new(RemoteDevice::new(s.local_addr(), Duration::from_secs(30)))
                    as Arc<dyn BlockStore>,
            )
        })
        .collect();
    let mut cfg = EngineConfig::new(td.join("meta"));
    cfg.extent_bytes = MIB;
    let e = NativeEngine::open_with(cfg, stores, MetaBackend::Local).unwrap();
    (servers, e)
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore]
fn datapath_throughput() {
    let total_mib = env("BENCH_MIB", 256);
    let io = 8 * MIB;
    let td = tempfile::tempdir().unwrap();
    let (_servers, e) = engine(td.path());
    let size = (total_mib * MIB) as u64;
    let v = e.create_volume("bench", size).unwrap();
    let buf: Vec<u8> = (0..io).map(|i| (i * 13 % 251) as u8).collect();

    let t = Instant::now();
    let mut off = 0u64;
    while off < size {
        e.write(&v, off, &buf).unwrap();
        off += io as u64;
    }
    let w = t.elapsed();

    let t = Instant::now();
    let mut off = 0u64;
    while off < size {
        let got = e.read(&v, off, io).unwrap();
        assert_eq!(got.len(), io);
        off += io as u64;
    }
    let r = t.elapsed();

    let mb = total_mib as f64;
    println!(
        "datapath: {total_mib} MiB, 8 MiB I/Os, 1 MiB extents, 3 replicas: \
         write {:.0} MiB/s, read {:.0} MiB/s",
        mb / w.as_secs_f64(),
        mb / r.as_secs_f64()
    );
}

/// A checkpoint: `BENCH_WRITERS` ranks each stream their own shard in aligned 8 MiB writes.
/// Aligned whole-extent writes place their data concurrently, so the total should scale with
/// writers until the devices or the network saturate.
/// `BENCH_WRITERS=8 cargo test --release -p atlas-native --test datapath_bench checkpoint -- --ignored --nocapture`
#[test]
#[ignore]
fn checkpoint_throughput() {
    let shard_mib = env("BENCH_MIB", 128);
    let io = 8 * MIB;
    let td = tempfile::tempdir().unwrap();
    let (_servers, e) = engine(td.path());
    let buf: Vec<u8> = (0..io).map(|i| (i * 13 % 251) as u8).collect();
    for writers in [1, env("BENCH_WRITERS", 8)] {
        let vols: Vec<String> = (0..writers)
            .map(|w| {
                e.create_volume(format!("ckpt-{writers}-{w}"), (shard_mib * MIB) as u64)
                    .unwrap()
            })
            .collect();
        let t = Instant::now();
        std::thread::scope(|s| {
            for v in &vols {
                let (e, buf) = (&e, &buf);
                s.spawn(move || {
                    let mut off = 0u64;
                    while off < (shard_mib * MIB) as u64 {
                        e.write(v, off, buf).unwrap();
                        off += io as u64;
                    }
                });
            }
        });
        let secs = t.elapsed().as_secs_f64();
        println!(
            "checkpoint: {writers} writer(s) x {shard_mib} MiB, 8 MiB I/Os, 1 MiB extents, \
             3 replicas: {:.0} MiB/s",
            (writers * shard_mib) as f64 / secs
        );
    }
}
