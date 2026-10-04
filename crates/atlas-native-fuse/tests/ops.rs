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

/// The test nodes accept at most 1 MiB per request.
fn mount(c: &Cluster, fs: &str, cfg: OpsConfig) -> Ops {
    Ops::new(
        c.client(),
        fs,
        OpsConfig {
            max_io_bytes: 1 << 20,
            ..cfg
        },
    )
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

    // A file unlinked while open stays readable through the handle until the last close.
    let o = ops
        .mknode(ROOT_INO, "open.txt", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    ops.opened(o.ino);
    ops.write(o.ino, 0, b"still here").unwrap();
    ops.unlink(ROOT_INO, "open.txt").unwrap();
    assert_eq!(ops.lookup(ROOT_INO, "open.txt"), Err(libc::ENOENT));
    assert_eq!(ops.getattr(o.ino).unwrap().nlink, 0);
    assert_eq!(ops.read(o.ino, 0, 64).unwrap(), b"still here");
    assert!(ops
        .readdir(ROOT_INO)
        .unwrap()
        .iter()
        .all(|e| e.name != "open.txt" && !e.name.starts_with(".atlas_hidden_")));
    ops.released(o.ino).unwrap();
    assert_eq!(ops.getattr(o.ino), Err(libc::ENOENT));

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
    // g was read (and read ahead) above; this mount's own write must be visible at once.
    ops.write(g.ino, 0, b"changed").unwrap();
    assert_eq!(ops.read(g.ino, 0, 7).unwrap(), b"changed");
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

#[test]
fn snapshot_mounts_are_read_only_and_clones_are_isolated() {
    let c = Cluster::start();
    create_fs(&c, "src");
    let ops = mount(&c, "src", OpsConfig::default());
    let d = ops
        .mknode(ROOT_INO, "d", NodeType::Dir, None, 0o755, 0, 0)
        .unwrap();
    let f = ops
        .mknode(d.ino, "f", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    // Spans several 64 KiB extents so overwrites, truncates and clones share some of them.
    let original: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    ops.write(f.ino, 0, &original).unwrap();
    ops.flush(f.ino).unwrap();

    let snap = c
        .client()
        .json(
            Method::POST,
            "/v1/fs/src/snapshots",
            Body::Json(json!({ "id": "s1", "name": "before" })),
            Retry::Idempotent,
        )
        .unwrap();
    assert_eq!(snap["id"], "s1");

    // Change the source after the snapshot: overwrite the middle, truncate, add and remove names.
    ops.write(f.ino, 70_000, &[0xAA; 1000]).unwrap();
    ops.flush(f.ino).unwrap();
    ops.setattr(
        f.ino,
        SetAttr {
            size: Some(150_000),
            ..SetAttr::default()
        },
    )
    .unwrap();
    ops.mknode(ROOT_INO, "new", NodeType::File, None, 0o644, 0, 0)
        .unwrap();

    let ro = mount(&c, "src@s1", OpsConfig::default());
    assert!(ro.read_only());
    let rd = ro.lookup(ROOT_INO, "d").unwrap();
    let rf = ro.lookup(rd.ino, "f").unwrap();
    assert_eq!(rf.size, original.len() as u64);
    assert_eq!(ro.read(rf.ino, 0, original.len()).unwrap(), original);
    assert_eq!(ro.lookup(ROOT_INO, "new"), Err(libc::ENOENT));
    assert_eq!(
        ro.mknode(ROOT_INO, "x", NodeType::File, None, 0o644, 0, 0),
        Err(libc::EROFS)
    );
    assert_eq!(ro.unlink(rd.ino, "f"), Err(libc::EROFS));
    assert_eq!(ro.write(rf.ino, 0, b"no"), Err(libc::EROFS));

    // A clone starts from the snapshot; writes to it touch neither the snapshot nor the source.
    let cl = c
        .client()
        .json(
            Method::POST,
            "/v1/fs-snapshots/s1/clone",
            Body::Json(json!({ "id": "cl", "name": "clone" })),
            Retry::Idempotent,
        )
        .unwrap();
    assert_eq!(cl["id"], "cl");
    let clone = mount(&c, "cl", OpsConfig::default());
    let cd = clone.lookup(ROOT_INO, "d").unwrap();
    let cf = clone.lookup(cd.ino, "f").unwrap();
    assert_eq!(clone.read(cf.ino, 0, original.len()).unwrap(), original);
    clone.write(cf.ino, 0, &[0x55; 70_000]).unwrap();
    clone.flush(cf.ino).unwrap();
    clone
        .mknode(ROOT_INO, "only-in-clone", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    assert_eq!(ro.lookup(ROOT_INO, "only-in-clone"), Err(libc::ENOENT));
    assert_eq!(ops.lookup(ROOT_INO, "only-in-clone"), Err(libc::ENOENT));
    assert_eq!(clone.read(cf.ino, 0, 4).unwrap(), [0x55; 4]);
    assert_eq!(ro.read(rf.ino, 0, original.len()).unwrap(), original);
    let now = ops.read(f.ino, 0, 200_000).unwrap();
    assert_eq!(now.len(), 150_000);
    assert_eq!(now[..70_000], original[..70_000]);
    assert_eq!(now[70_000..71_000], [0xAA; 1000]);

    // Deleting the source keeps the snapshot readable; deleting the snapshot keeps the clone.
    c.client()
        .request(Method::DELETE, "/v1/fs/src", Body::Empty, Retry::Remove)
        .unwrap();
    let ro = mount(&c, "src@s1", OpsConfig::default());
    assert_eq!(ro.read(rf.ino, 0, original.len()).unwrap(), original);
    c.client()
        .request(
            Method::DELETE,
            "/v1/fs-snapshots/s1",
            Body::Empty,
            Retry::Remove,
        )
        .unwrap();
    assert_eq!(ro.getattr(rf.ino), Err(libc::ENOENT));
    let after = clone.read(cf.ino, 0, original.len()).unwrap();
    assert_eq!(after[..70_000], [0x55; 70_000]);
    assert_eq!(after[70_000..], original[70_000..]);
}

#[test]
fn concurrent_creates_do_not_starve_each_other() {
    let c = Cluster::start();
    create_fs(&c, "busy");
    // Retries ride out an election on a slow runner; a request starved behind the others kept
    // timing out for longer than the whole retry budget.
    let mut cfg = ClientConfig::new(c.endpoints());
    cfg.token = Some(TOKEN.into());
    let ops = std::sync::Arc::new(Ops::new(
        Client::new(cfg).unwrap(),
        "busy",
        OpsConfig::default(),
    ));
    let threads: Vec<_> = (0..16)
        .map(|t| {
            let ops = ops.clone();
            std::thread::spawn(move || {
                for i in 0..25 {
                    ops.mknode(
                        ROOT_INO,
                        &format!("f{t}-{i}"),
                        NodeType::File,
                        None,
                        0o644,
                        0,
                        0,
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(ops.readdir(ROOT_INO).unwrap().len(), 16 * 25);
}

#[test]
fn extended_attributes_round_trip_and_follow_snapshots() {
    let c = Cluster::start();
    create_fs(&c, "x");
    let ops = mount(&c, "x", OpsConfig::default());
    let f = ops
        .mknode(ROOT_INO, "f", NodeType::File, None, 0o644, 0, 0)
        .unwrap();
    assert_eq!(ops.listxattr(f.ino).unwrap(), Vec::<String>::new());
    assert_eq!(ops.getxattr(f.ino, "user.a"), Err(libc::ENODATA));
    ops.setxattr(f.ino, "user.a", b"one", false, false).unwrap();
    ops.setxattr(f.ino, "user.b c", &[0, 255, 7], true, false)
        .unwrap();
    assert_eq!(ops.getxattr(f.ino, "user.a").unwrap(), b"one");
    assert_eq!(ops.getxattr(f.ino, "user.b c").unwrap(), [0, 255, 7]);
    assert_eq!(
        ops.setxattr(f.ino, "user.a", b"x", true, false),
        Err(libc::EEXIST)
    );
    assert_eq!(
        ops.setxattr(f.ino, "user.zz", b"x", false, true),
        Err(libc::ENODATA)
    );
    ops.setxattr(f.ino, "user.a", b"two", false, true).unwrap();
    assert_eq!(ops.getxattr(f.ino, "user.a").unwrap(), b"two");
    assert_eq!(
        ops.setxattr(f.ino, "system.posix_acl_access", b"x", false, false),
        Err(libc::EOPNOTSUPP)
    );
    assert_eq!(
        ops.setxattr(f.ino, "user.big", &vec![1; 65 << 10], false, false),
        Err(libc::E2BIG)
    );
    assert_eq!(ops.listxattr(f.ino).unwrap(), ["user.a", "user.b c"]);

    c.client()
        .json(
            Method::POST,
            "/v1/fs/x/snapshots",
            Body::Json(json!({ "id": "xs", "name": "s" })),
            Retry::Idempotent,
        )
        .unwrap();
    ops.removexattr(f.ino, "user.a").unwrap();
    assert_eq!(ops.removexattr(f.ino, "user.a"), Err(libc::ENODATA));
    assert_eq!(ops.listxattr(f.ino).unwrap(), ["user.b c"]);

    let snap = mount(&c, "x@xs", OpsConfig::default());
    assert_eq!(snap.getxattr(f.ino, "user.a").unwrap(), b"two");
    assert_eq!(
        snap.setxattr(f.ino, "user.n", b"x", false, false),
        Err(libc::EROFS)
    );
}
