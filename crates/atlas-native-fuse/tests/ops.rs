// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! The FUSE operations layer against a live in-process cluster: 3 Raft metadata nodes and 3
//! data nodes on localhost.

use std::{collections::BTreeMap, net::TcpListener, path::Path, time::Duration};

use atlas_native::{
    node::{DataNodeRole, DataNodeSpec, Listeners, MetadataRole, NativeNode, NodeConfig},
    NodeType, SetAttr, ROOT_INO,
};
use atlas_native_fuse::{
    client::{Body, Client, ClientConfig, Retry},
    ops::{Ops, OpsConfig},
};
use reqwest::Method;
use serde_json::json;

const TOKEN: &str = "fuse-test-token";

fn bind() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

struct Cluster {
    _td: tempfile::TempDir,
    meta: BTreeMap<String, Option<NativeNode>>,
    _data: Vec<NativeNode>,
}

impl Cluster {
    fn start() -> Self {
        let td = tempfile::tempdir().unwrap();
        std::fs::write(td.path().join("token"), TOKEN).unwrap();
        let data_l: Vec<(String, TcpListener)> =
            (1..=3).map(|i| (format!("d{i}"), bind())).collect();
        let meta_l: Vec<(String, TcpListener)> =
            (1..=3).map(|i| (format!("m{i}"), bind())).collect();
        let specs: Vec<DataNodeSpec> = data_l
            .iter()
            .map(|(id, l)| DataNodeSpec {
                id: id.clone(),
                addr: l.local_addr().unwrap().to_string(),
                zone: None,
                rack: None,
                host: None,
                free_bytes: 1 << 30,
            })
            .collect();
        let peers: BTreeMap<String, String> = meta_l
            .iter()
            .map(|(id, l)| (id.clone(), l.local_addr().unwrap().to_string()))
            .collect();
        let base = |id: &str, root: &Path| NodeConfig {
            node_id: id.into(),
            data_dir: root.join(id),
            http_listen: "127.0.0.1:0".parse().unwrap(),
            api_token_file: Some(root.join("token")),
            tls: None,
            http_tls: None,
            data_node: None,
            metadata: None,
            max_request_bytes: 1 << 20,
        };
        let data = data_l
            .into_iter()
            .map(|(id, l)| {
                let mut cfg = base(&id, td.path());
                cfg.data_node = Some(DataNodeRole {
                    listen: l.local_addr().unwrap(),
                });
                NativeNode::start_with(
                    cfg,
                    Listeners {
                        http: Some(bind()),
                        data_node: Some(l),
                        metadata: None,
                    },
                )
                .unwrap()
            })
            .collect();
        let meta = meta_l
            .into_iter()
            .map(|(id, l)| {
                let mut cfg = base(&id, td.path());
                cfg.metadata = Some(MetadataRole {
                    listen: l.local_addr().unwrap(),
                    peers: peers.clone(),
                    bootstrap: None,
                    data_nodes: specs.clone(),
                    replicas: 3,
                    extent_bytes: 64 << 10,
                    tick_ms: 10,
                    proposal_timeout_ms: 3000,
                    repair_interval_secs: 0,
                    gc_interval_secs: 0,
                });
                let n = NativeNode::start_with(
                    cfg,
                    Listeners {
                        http: Some(bind()),
                        data_node: None,
                        metadata: Some(l),
                    },
                )
                .unwrap();
                (id, Some(n))
            })
            .collect();
        Self {
            _td: td,
            meta,
            _data: data,
        }
    }

    fn endpoints(&self) -> Vec<String> {
        self.meta
            .values()
            .flatten()
            .map(|n| format!("http://{}", n.http_addr()))
            .collect()
    }

    fn client(&self) -> Client {
        let mut cfg = ClientConfig::new(self.endpoints());
        cfg.token = Some(TOKEN.into());
        cfg.retry_for = Duration::from_secs(20);
        Client::new(cfg).unwrap()
    }

    /// Stops the current Raft leader.
    fn kill_leader(&mut self) -> String {
        loop {
            let leader = self.meta.iter().find_map(|(id, n)| {
                let mut cfg =
                    ClientConfig::new(vec![format!("http://{}", n.as_ref()?.http_addr())]);
                cfg.token = Some(TOKEN.into());
                let s = Client::new(cfg)
                    .ok()?
                    .json(Method::GET, "/v1/status", Body::Empty, Retry::Idempotent)
                    .ok()?;
                (s["metadata"]["role"] == "leader").then(|| id.clone())
            });
            if let Some(id) = leader {
                self.meta.insert(id.clone(), None);
                return id;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn mount(c: &Cluster, fs: &str, cfg: OpsConfig) -> Ops {
    Ops::new(c.client(), fs, cfg)
}

fn create_fs(c: &Cluster, id: &str) {
    c.client()
        .json(
            Method::POST,
            "/v1/fs",
            Body::Json(json!({ "id": id, "name": id })),
            Retry::Idempotent,
        )
        .unwrap();
}

#[test]
fn posix_operations_through_the_ops_layer() {
    let c = Cluster::start();
    create_fs(&c, "f1");
    let ops = mount(&c, "f1", OpsConfig::default());

    let d = ops
        .mknode(ROOT_INO, "dir", NodeType::Dir, None, 0o755, 1000, 1000)
        .unwrap();
    assert_eq!((d.kind, d.mode, d.nlink), (NodeType::Dir, 0o755, 2));
    let f = ops
        .mknode(d.ino, "file.txt", NodeType::File, None, 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(
        ops.mknode(d.ino, "file.txt", NodeType::File, None, 0o644, 0, 0),
        Err(libc::EEXIST)
    );

    // Many small sequential writes are buffered into one run; the size includes them at once.
    let mut want = Vec::new();
    for i in 0..300u32 {
        let chunk = format!("line {i}\n").into_bytes();
        assert_eq!(
            ops.write(f.ino, want.len() as u64, &chunk).unwrap(),
            chunk.len()
        );
        want.extend_from_slice(&chunk);
    }
    assert_eq!(ops.getattr(f.ino).unwrap().size, want.len() as u64);
    ops.flush(f.ino).unwrap();
    assert_eq!(ops.read(f.ino, 0, 1 << 20).unwrap(), want);
    // Another client sees the flushed data.
    let other = mount(&c, "f1", OpsConfig::default());
    assert_eq!(other.read(f.ino, 0, 1 << 20).unwrap(), want);

    // A large write crossing extents, then a read past EOF is short.
    let big: Vec<u8> = (0..300_000u32).map(|i| (i * 7 % 256) as u8).collect();
    let g = ops
        .mknode(ROOT_INO, "big.bin", NodeType::File, None, 0o600, 0, 0)
        .unwrap();
    ops.write(g.ino, 0, &big).unwrap();
    ops.flush(g.ino).unwrap();
    assert_eq!(ops.read(g.ino, 299_990, 100).unwrap(), &big[299_990..]);
    assert_eq!(ops.read(g.ino, 0, 400_000).unwrap(), big);

    // Truncate drops buffered bytes past the new size too.
    ops.write(f.ino, 0, b"HEAD").unwrap();
    let a = ops
        .setattr(
            f.ino,
            SetAttr {
                size: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(a.size, 2);
    assert_eq!(ops.read(f.ino, 0, 100).unwrap(), b"HE");

    let l = ops
        .mknode(
            ROOT_INO,
            "link",
            NodeType::Symlink,
            Some("dir/file.txt".into()),
            0o777,
            0,
            0,
        )
        .unwrap();
    assert_eq!(ops.readlink(l.ino).unwrap(), "dir/file.txt");
    assert_eq!(ops.link(f.ino, ROOT_INO, "hard").unwrap().nlink, 2);
    ops.rename(d.ino, "file.txt", ROOT_INO, "moved.txt")
        .unwrap();
    assert_eq!(ops.lookup(d.ino, "file.txt"), Err(libc::ENOENT));
    assert_eq!(ops.lookup(ROOT_INO, "moved.txt").unwrap().ino, f.ino);
    let names: Vec<String> = ops
        .readdir(ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["big.bin", "dir", "hard", "link", "moved.txt"]);

    assert_eq!(ops.rmdir(ROOT_INO, "moved.txt"), Err(libc::ENOTDIR));
    ops.mknode(d.ino, "x", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    assert_eq!(ops.rmdir(ROOT_INO, "dir"), Err(libc::ENOTEMPTY));
    assert_eq!(ops.unlink(ROOT_INO, "dir"), Err(libc::EISDIR));
    assert_eq!(
        ops.lookup(ROOT_INO, &"n".repeat(300)),
        Err(libc::ENAMETOOLONG)
    );
    ops.unlink(d.ino, "x").unwrap();
    ops.rmdir(ROOT_INO, "dir").unwrap();
    let st = ops.statfs().unwrap();
    assert_eq!(st.inodes, 4);

    // A snapshot mount is frozen and read-only.
    c.client()
        .json(
            Method::POST,
            "/v1/fs/f1/snapshots",
            Body::Json(json!({ "id": "s1", "name": "s1" })),
            Retry::Idempotent,
        )
        .unwrap();
    ops.write(g.ino, 0, b"changed").unwrap();
    ops.flush(g.ino).unwrap();
    let snap = mount(&c, "f1@s1", OpsConfig::default());
    assert!(snap.read_only());
    assert_eq!(snap.read(g.ino, 0, 7).unwrap(), &big[..7]);
    assert_eq!(snap.write(g.ino, 0, b"x"), Err(libc::EROFS));
    assert_eq!(
        snap.mknode(ROOT_INO, "n", NodeType::File, None, 0, 0, 0),
        Err(libc::EROFS)
    );
}

#[test]
fn calls_ride_through_a_leader_failure() {
    let mut c = Cluster::start();
    create_fs(&c, "f2");
    let ops = mount(
        &c,
        "f2",
        OpsConfig {
            writeback_bytes: 0,
            ..OpsConfig::default()
        },
    );
    let f = ops
        .mknode(ROOT_INO, "a", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    ops.write(f.ino, 0, b"before").unwrap();
    let killed = c.kill_leader();
    // The client still lists the dead node; it must skip it and find the new leader.
    let g = ops
        .mknode(ROOT_INO, "b", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    ops.write(g.ino, 0, b"after").unwrap();
    assert_eq!(
        ops.read(f.ino, 0, 64).unwrap(),
        b"before",
        "killed {killed}"
    );
    assert_eq!(ops.read(g.ino, 0, 64).unwrap(), b"after");
    ops.unlink(ROOT_INO, "a").unwrap();
    assert_eq!(ops.lookup(ROOT_INO, "a"), Err(libc::ENOENT));
}
