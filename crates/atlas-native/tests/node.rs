// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    thread,
    time::{Duration, Instant},
};

use atlas_native::node::{
    DataNodeRole, DataNodeSpec, Listeners, MetadataRole, NativeNode, NodeConfig,
};

const WAIT: Duration = Duration::from_secs(20);
const TOKEN: &str = "s3cret-token";

fn bind() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: test\r\n{auth}Content-Length: {}\r\n\r\n",
        body.len()
    );
    // The server may refuse a request before reading its body; the reply is still readable.
    let _ = s
        .write_all(head.as_bytes())
        .and_then(|()| s.write_all(body));
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    let split = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let status = std::str::from_utf8(&out[9..12]).unwrap().parse().unwrap();
    (status, out[split + 4..].to_vec())
}

fn api(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    http(addr, method, path, Some(TOKEN), body)
}

fn json(b: &[u8]) -> serde_json::Value {
    serde_json::from_slice(b).unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(b)))
}

struct Cluster {
    _td: tempfile::TempDir,
    meta: BTreeMap<String, Option<NativeNode>>,
    data: BTreeMap<String, Option<NativeNode>>,
}

impl Cluster {
    /// `meta` metadata-only nodes and `data` data-only nodes, all on localhost.
    fn start(meta: usize, data: usize, repair_interval_secs: u64) -> Self {
        let td = tempfile::tempdir().unwrap();
        std::fs::write(td.path().join("token"), format!("{TOKEN}\n")).unwrap();
        let data_l: BTreeMap<String, TcpListener> =
            (1..=data).map(|i| (format!("d{i}"), bind())).collect();
        let meta_l: BTreeMap<String, TcpListener> =
            (1..=meta).map(|i| (format!("m{i}"), bind())).collect();
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
        let raft_addrs: BTreeMap<String, String> = meta_l
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
        let mut data_nodes = BTreeMap::new();
        for (id, l) in data_l {
            let mut cfg = base(&id, td.path());
            cfg.data_node = Some(DataNodeRole {
                listen: l.local_addr().unwrap(),
            });
            let n = NativeNode::start_with(
                cfg,
                Listeners {
                    http: Some(bind()),
                    data_node: Some(l),
                    metadata: None,
                },
            )
            .unwrap();
            data_nodes.insert(id, Some(n));
        }
        let mut meta_nodes = BTreeMap::new();
        for (id, l) in meta_l {
            let mut cfg = base(&id, td.path());
            cfg.metadata = Some(MetadataRole {
                listen: l.local_addr().unwrap(),
                // Every voter, this node included: the node ignores its own entry.
                peers: raft_addrs.clone(),
                bootstrap: None,
                data_nodes: specs.clone(),
                replicas: 3,
                extent_bytes: 4096,
                tick_ms: 10,
                proposal_timeout_ms: 3000,
                repair_interval_secs,
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
            meta_nodes.insert(id, Some(n));
        }
        Self {
            _td: td,
            meta: meta_nodes,
            data: data_nodes,
        }
    }

    fn meta_addrs(&self) -> Vec<(String, SocketAddr)> {
        self.meta
            .iter()
            .filter_map(|(id, n)| n.as_ref().map(|n| (id.clone(), n.http_addr())))
            .collect()
    }

    /// Retries `method path` across metadata nodes until one (the leader) answers `want`.
    fn on_leader(&self, method: &str, path: &str, body: &[u8], want: u16) -> (String, Vec<u8>) {
        let deadline = Instant::now() + WAIT;
        loop {
            for (id, addr) in self.meta_addrs() {
                let (st, b) = api(addr, method, path, body);
                if st == want {
                    return (id, b);
                }
                assert!(
                    st == 421 || st == 503,
                    "{id} {method} {path}: {st} {}",
                    String::from_utf8_lossy(&b)
                );
            }
            assert!(
                Instant::now() < deadline,
                "no leader answered {method} {path}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Sends `method path` to whichever metadata node is the leader and returns its answer.
    fn on_leader_any(&self, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let deadline = Instant::now() + WAIT;
        loop {
            for (_, addr) in self.meta_addrs() {
                let (st, b) = api(addr, method, path, body);
                if st != 421 && st != 503 {
                    return (st, b);
                }
            }
            assert!(
                Instant::now() < deadline,
                "no leader answered {method} {path}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_read(&self, path: &str, want: &[u8]) {
        for (id, addr) in self.meta_addrs() {
            let deadline = Instant::now() + WAIT;
            loop {
                let (st, b) = api(addr, "GET", path, b"");
                if st == 200 && b == want {
                    break;
                }
                assert!(Instant::now() < deadline, "{id} never served {path} ({st})");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

#[test]
fn http_api_round_trip_with_auth_and_leader_redirect() {
    let c = Cluster::start(3, 3, 0);
    let (_, first) = c.meta_addrs()[0].clone();
    assert_eq!(http(first, "GET", "/healthz", None, b"").0, 200);
    assert_eq!(http(first, "GET", "/v1/status", None, b"").0, 401);
    assert_eq!(http(first, "GET", "/v1/status", Some("wrong"), b"").0, 401);
    assert_eq!(api(first, "GET", "/v1/status", b"").0, 200);

    for (id, addr) in c.meta_addrs() {
        let deadline = Instant::now() + WAIT;
        while http(addr, "GET", "/readyz", None, b"").0 != 200 {
            assert!(Instant::now() < deadline, "{id} never became ready");
            thread::sleep(Duration::from_millis(20));
        }
    }

    let (leader, body) = c.on_leader(
        "POST",
        "/v1/volumes",
        br#"{"name":"vol","size_bytes":8192}"#,
        201,
    );
    let v = json(&body)["id"].as_str().unwrap().to_string();

    // A follower refuses mutations and names the leader.
    let (fid, faddr) = c
        .meta_addrs()
        .into_iter()
        .find(|(id, _)| *id != leader)
        .unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let (st, b) = api(
            faddr,
            "POST",
            "/v1/volumes",
            br#"{"name":"x","size_bytes":4096}"#,
        );
        assert_eq!(st, 421, "{fid}: {}", String::from_utf8_lossy(&b));
        if json(&b)["leader"] == leader.as_str() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{fid} never named leader {leader}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    c.on_leader(
        "PUT",
        &format!("/v1/volumes/{v}/data?offset=0"),
        &[9u8; 4096],
        204,
    );
    c.wait_read(
        &format!("/v1/volumes/{v}/data?offset=0&len=4096"),
        &[9u8; 4096],
    );
    let (_, snap) = c.on_leader(
        "POST",
        &format!("/v1/volumes/{v}/snapshots"),
        br#"{"name":"s1"}"#,
        201,
    );
    let s = json(&snap)["id"].as_str().unwrap().to_string();
    c.on_leader(
        "PUT",
        &format!("/v1/volumes/{v}/data?offset=0"),
        &[1u8; 4096],
        204,
    );
    c.wait_read(
        &format!("/v1/snapshots/{s}/data?offset=0&len=4096"),
        &[9u8; 4096],
    );
    c.wait_read(
        &format!("/v1/volumes/{v}/data?offset=0&len=4096"),
        &[1u8; 4096],
    );

    let (st, list) = api(faddr, "GET", "/v1/volumes", b"");
    assert_eq!(st, 200);
    assert_eq!(json(&list)["volumes"][0]["size_bytes"], 8192);

    // Bad requests are rejected without touching storage.
    let laddr = c.meta[&leader].as_ref().unwrap().http_addr();
    assert_eq!(
        api(laddr, "PUT", &format!("/v1/volumes/{v}/data"), b"x").0,
        400
    );
    assert_eq!(api(laddr, "POST", "/v1/volumes", b"not json").0, 400);
    assert_eq!(
        api(laddr, "GET", "/v1/volumes/nope/data?offset=0&len=1", b"").0,
        404
    );
    assert_eq!(
        api(
            laddr,
            "PUT",
            &format!("/v1/volumes/{v}/data?offset=0"),
            &vec![0u8; 2 << 20]
        )
        .0,
        413
    );
    assert_eq!(
        api(
            laddr,
            "GET",
            &format!("/v1/volumes/{v}/data?offset=0&len={}", 2 << 20),
            b""
        )
        .0,
        413
    );
    assert_eq!(
        api(
            laddr,
            "GET",
            &format!("/v1/volumes/{v}/data?offset=8000&len=193"),
            b""
        )
        .0,
        400,
        "read past the end of the volume"
    );

    // Unaligned writes and reads that cross extent boundaries.
    c.on_leader(
        "PUT",
        &format!("/v1/volumes/{v}/data?offset=4000"),
        &[7u8; 200],
        204,
    );
    let mut want = vec![1u8; 96];
    want.extend_from_slice(&[7u8; 200]);
    want.extend_from_slice(&[0u8; 4]);
    c.wait_read(&format!("/v1/volumes/{v}/data?offset=3904&len=300"), &want);

    // Clone from the snapshot and grow the source.
    let (_, cl) = c.on_leader(
        "POST",
        &format!("/v1/snapshots/{s}/clone"),
        br#"{"name":"copy","size_bytes":12288}"#,
        201,
    );
    let cv = json(&cl)["id"].as_str().unwrap().to_string();
    let mut want = vec![9u8; 4096];
    want.extend_from_slice(&[0u8; 8192]);
    c.wait_read(&format!("/v1/volumes/{cv}/data?offset=0&len=12288"), &want);
    c.on_leader(
        "POST",
        &format!("/v1/volumes/{v}/resize"),
        br#"{"size_bytes":16384}"#,
        200,
    );
    c.on_leader(
        "PUT",
        &format!("/v1/volumes/{v}/data?offset=16000"),
        &[3u8; 384],
        204,
    );
    c.on_leader(
        "POST",
        &format!("/v1/volumes/{v}/resize"),
        br#"{"size_bytes":4096}"#,
        409,
    );
    c.on_leader(
        "POST",
        "/v1/volumes/nope/resize",
        br#"{"size_bytes":1}"#,
        404,
    );
    c.on_leader("POST", "/v1/snapshots/nope/clone", br#"{"name":"x"}"#, 404);
    assert_eq!(
        api(
            laddr,
            "POST",
            &format!("/v1/snapshots/{s}/clone"),
            br#"{"name":"x","size_bytes":"big"}"#
        )
        .0,
        400
    );

    let (st, m) = http(laddr, "GET", "/metrics", None, b"");
    let m = String::from_utf8(m).unwrap();
    assert_eq!(st, 200);
    for series in [
        "atlas_native_raft_role{",
        "atlas_native_volumes ",
        "atlas_native_node_up{node=\"d1\"} 1",
        "atlas_native_repair_runs_total{",
    ] {
        assert!(m.contains(series), "missing {series}:\n{m}");
    }
    let d1 = c.data["d1"].as_ref().unwrap().http_addr();
    let (_, dm) = http(d1, "GET", "/metrics", None, b"");
    assert!(String::from_utf8(dm)
        .unwrap()
        .contains("atlas_native_data_fence{node=\"d1\"}"));
    assert_eq!(api(d1, "GET", "/v1/volumes", b"").0, 404);
}

#[test]
fn background_repair_restores_replicas_after_losing_a_data_node() {
    let mut c = Cluster::start(3, 4, 1);
    let (_, body) = c.on_leader(
        "POST",
        "/v1/volumes",
        br#"{"name":"vol","size_bytes":4096}"#,
        201,
    );
    let v = json(&body)["id"].as_str().unwrap().to_string();
    c.on_leader(
        "PUT",
        &format!("/v1/volumes/{v}/data?offset=0"),
        &[5u8; 4096],
        204,
    );

    // d1..d3 hold the replicas (placement order); losing d2 leaves the spare d4 to repair onto.
    c.data.insert("d2".into(), None);
    let deadline = Instant::now() + WAIT;
    loop {
        let repaired = c.meta_addrs().into_iter().any(|(_, addr)| {
            let (_, b) = api(addr, "GET", "/v1/status", b"");
            json(&b)["last_repair"]["replicas_repaired"].as_u64() == Some(1)
        });
        if repaired {
            break;
        }
        assert!(Instant::now() < deadline, "background repair never ran");
        thread::sleep(Duration::from_millis(100));
    }
    c.data.insert("d1".into(), None);
    c.wait_read(
        &format!("/v1/volumes/{v}/data?offset=0&len=4096"),
        &[5u8; 4096],
    );
}

#[test]
fn config_validation_rejects_bad_files() {
    let td = tempfile::tempdir().unwrap();
    let write = |name: &str, body: &str| {
        let p = td.path().join(name);
        std::fs::write(&p, body).unwrap();
        p
    };
    let ok = write(
        "ok.json",
        r#"{"node_id":"d1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0",
            "data_node":{"listen":"127.0.0.1:0"}}"#,
    );
    assert!(NodeConfig::from_file(&ok).is_ok());
    for (name, body) in [
        (
            "unknown.json",
            r#"{"node_id":"d1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0","bogus":1,
                "data_node":{"listen":"127.0.0.1:0"}}"#,
        ),
        (
            "norole.json",
            r#"{"node_id":"d1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0"}"#,
        ),
        (
            "replicas.json",
            r#"{"node_id":"m1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0",
                "metadata":{"listen":"127.0.0.1:0","peers":{},"replicas":3,
                "data_nodes":[{"id":"d1","addr":"127.0.0.1:1"}]}}"#,
        ),
        (
            "badaddr.json",
            r#"{"node_id":"m1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0",
                "metadata":{"listen":"127.0.0.1:0","peers":{"m2":"no-port"},"replicas":1,
                "data_nodes":[{"id":"d1","addr":"d1.svc:7481"}]}}"#,
        ),
        (
            "unsetenv.json",
            r#"{"node_id":"${ATLAS_NATIVE_TEST_UNSET_VAR}","data_dir":"/tmp/x",
                "http_listen":"127.0.0.1:0","data_node":{"listen":"127.0.0.1:0"}}"#,
        ),
    ] {
        assert!(
            NodeConfig::from_file(write(name, body)).is_err(),
            "{name} accepted"
        );
    }
}

#[test]
fn http_members_list_and_remove_a_voter() {
    let c = Cluster::start(3, 3, 0);
    let (_, first) = c.meta_addrs()[0].clone();
    let (st, b) = api(first, "GET", "/v1/members", b"");
    assert_eq!(st, 200);
    let m = json(&b);
    assert_eq!(m["membership"]["type"], "stable");
    assert_eq!(
        m["membership"]["voters"],
        serde_json::json!(["m1", "m2", "m3"])
    );

    for bad in [
        &br#"{"voters": ["m1"]}"#[..],
        br#"{"voters": {"m1": "x", "m9": "no-port"}}"#,
    ] {
        let (st, b) = api(first, "POST", "/v1/members", bad);
        assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    }

    let addrs = m["addrs"].as_object().unwrap().clone();
    let keep: serde_json::Map<String, serde_json::Value> =
        addrs.into_iter().filter(|(id, _)| id != "m3").collect();
    let mut body = serde_json::json!({ "voters": keep });
    // The local node's own address is optional.
    for id in ["m1", "m2"] {
        body["voters"]
            .as_object_mut()
            .unwrap()
            .entry(id)
            .or_insert("127.0.0.1:1".into());
    }
    let (_, b) = c.on_leader("POST", "/v1/members", body.to_string().as_bytes(), 200);
    assert_eq!(
        json(&b)["membership"]["voters"],
        serde_json::json!(["m1", "m2"])
    );

    for (id, addr) in c.meta_addrs().into_iter().filter(|(id, _)| id != "m3") {
        let deadline = Instant::now() + WAIT;
        loop {
            let m = json(&api(addr, "GET", "/v1/members", b"").1);
            if m["membership"]["voters"] == serde_json::json!(["m1", "m2"]) {
                break;
            }
            assert!(Instant::now() < deadline, "{id}: {m}");
            thread::sleep(Duration::from_millis(20));
        }
    }
    c.on_leader(
        "POST",
        "/v1/volumes",
        br#"{"name":"after","size_bytes":4096}"#,
        201,
    );
}

#[test]
fn http_file_api_on_a_three_node_cluster() {
    let c = Cluster::start(3, 3, 0);
    let _ = c.on_leader("POST", "/v1/fs", br#"{"id":"f1","name":"home"}"#, 201);
    let call = |method: &str, path: &str, body: &[u8]| {
        // Leadership may move under load; follow it like a client would.
        c.on_leader_any(method, path, body)
    };
    let code = |method: &str, path: &str, body: &[u8]| {
        let (st, b) = c.on_leader_any(method, path, body);
        (
            st,
            json(&b)["code"].as_str().unwrap_or_default().to_string(),
        )
    };
    // A retried create is a no-op.
    assert_eq!(
        call("POST", "/v1/fs", br#"{"id":"f1","name":"home"}"#).0,
        201
    );

    let mk = |parent: u64, body: &str| {
        let (st, b) = call(
            "POST",
            &format!("/v1/fs/f1/inodes/{parent}/entries"),
            body.as_bytes(),
        );
        assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
        json(&b)["ino"].as_u64().unwrap()
    };
    let d = mk(1, r#"{"name":"d","op_id":"o1","kind":"dir","mode":493}"#);
    let a = mk(
        d,
        r#"{"name":"my file é","op_id":"o2","kind":"file","mode":420}"#,
    );
    assert_eq!(
        mk(
            d,
            r#"{"name":"my file é","op_id":"o2","kind":"file","mode":420}"#
        ),
        a
    );
    let l = mk(
        1,
        r#"{"name":"ln","op_id":"o3","kind":"symlink","target":"d/my file é"}"#,
    );

    // Unaligned write across extent boundaries, then read it back from every replica.
    let payload: Vec<u8> = (0..6000u32).map(|i| (i % 251) as u8).collect();
    let (st, b) = call(
        "PUT",
        &format!("/v1/fs/f1/inodes/{a}/data?offset=100"),
        &payload,
    );
    assert_eq!(st, 200);
    assert_eq!(json(&b)["size"], 6100);
    let mut want = vec![0u8; 100];
    want.extend_from_slice(&payload);
    c.wait_read(
        &format!("/v1/fs/f1/inodes/{a}/data?offset=0&len=9000&stale=1"),
        &want,
    );

    let (st, b) = call(
        "GET",
        &format!("/v1/fs/f1/inodes/{d}/lookup?name=my%20file+%C3%A9"),
        b"",
    );
    assert_eq!((st, json(&b)["ino"].as_u64()), (200, Some(a)));
    let (_, b) = call("GET", "/v1/fs/f1/inodes/1/entries", b"");
    let names = json(&b)["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, ["d", "ln"]);
    let (_, b) = call("GET", &format!("/v1/fs/f1/inodes/{l}/target"), b"");
    assert_eq!(json(&b)["target"], "d/my file é");

    // Errors carry a stable code for errno mapping.
    let entries = format!("/v1/fs/f1/inodes/{d}/entries");
    assert_eq!(
        code(
            "POST",
            &entries,
            br#"{"name":"my file \u00e9","op_id":"x","kind":"file"}"#
        ),
        (409, "exists".into())
    );
    assert_eq!(
        code("POST", "/v1/fs/f1/inodes/1/rmdir", br#"{"name":"d"}"#),
        (409, "not_empty".into())
    );
    assert_eq!(
        code("POST", "/v1/fs/f1/inodes/1/unlink", br#"{"name":"d"}"#),
        (409, "is_dir".into())
    );
    assert_eq!(
        code("GET", "/v1/fs/f1/inodes/1/lookup?name=nope", b""),
        (404, "not_found".into())
    );
    assert_eq!(
        code(
            "POST",
            &format!("/v1/fs/f1/inodes/{a}/entries"),
            br#"{"name":"x","op_id":"y","kind":"file"}"#
        ),
        (409, "not_dir".into())
    );
    assert_eq!(
        code(
            "POST",
            "/v1/fs/f1/inodes/1/entries",
            br#"{"name":"a/b","op_id":"z","kind":"file"}"#
        ),
        (409, "invalid".into())
    );

    // Hard link, rename, truncate.
    let (st, b) = call(
        "POST",
        &format!("/v1/fs/f1/inodes/{a}/links"),
        br#"{"parent":1,"name":"hard"}"#,
    );
    assert_eq!((st, json(&b)["nlink"].as_u64()), (200, Some(2)));
    let rename =
        format!(r#"{{"parent":{d},"name":"my file é","new_parent":1,"new_name":"moved"}}"#);
    assert_eq!(call("POST", "/v1/fs/f1/rename", rename.as_bytes()).0, 204);
    let (st, b) = call(
        "POST",
        &format!("/v1/fs/f1/inodes/{a}/attr"),
        br#"{"size":50,"mode":384}"#,
    );
    assert_eq!(st, 200);
    assert_eq!(
        (json(&b)["size"].as_u64(), json(&b)["mode"].as_u64()),
        (Some(50), Some(0o600))
    );

    // Snapshots are frozen, readable as fs@snap, and refuse writes.
    let (st, b) = call(
        "POST",
        "/v1/fs/f1/snapshots",
        br#"{"id":"s1","name":"before"}"#,
    );
    assert_eq!((st, json(&b)["id"].as_str()), (201, Some("s1")));
    assert_eq!(
        call(
            "PUT",
            &format!("/v1/fs/f1/inodes/{a}/data?offset=0"),
            b"CHANGED"
        )
        .0,
        200
    );
    c.wait_read(
        &format!("/v1/fs/f1@s1/inodes/{a}/data?offset=0&len=50&stale=1"),
        &want[..50],
    );
    assert_eq!(
        code(
            "PUT",
            &format!("/v1/fs/f1@s1/inodes/{a}/data?offset=0"),
            b"x"
        ),
        (409, "read_only".into())
    );
    let (st, _) = call(
        "POST",
        "/v1/fs-snapshots/s1/clone",
        br#"{"id":"c1","name":"copy"}"#,
    );
    assert_eq!(st, 201);
    c.wait_read(
        &format!("/v1/fs/c1/inodes/{a}/data?offset=0&len=50&stale=1"),
        &want[..50],
    );
    let (_, b) = call("GET", "/v1/fs", b"");
    assert_eq!(json(&b)["filesystems"].as_array().unwrap().len(), 2);

    assert_eq!(call("DELETE", "/v1/fs/c1", b"").0, 204);
    assert_eq!(call("DELETE", "/v1/fs-snapshots/s1", b"").0, 204);
    assert_eq!(
        call("POST", "/v1/fs/f1/inodes/1/unlink", br#"{"name":"moved"}"#).0,
        204
    );
    assert_eq!(
        call("POST", "/v1/fs/f1/inodes/1/unlink", br#"{"name":"hard"}"#).0,
        204
    );
    let (st, b) = call("POST", "/v1/gc", b"");
    assert_eq!(st, 200);
    assert!(json(&b)["reclaimed"].as_u64().unwrap() > 0);
}
