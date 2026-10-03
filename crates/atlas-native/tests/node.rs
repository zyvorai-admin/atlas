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
                addr: l.local_addr().unwrap(),
                zone: None,
                rack: None,
                host: None,
                free_bytes: 1 << 30,
            })
            .collect();
        let raft_addrs: BTreeMap<String, SocketAddr> = meta_l
            .iter()
            .map(|(id, l)| (id.clone(), l.local_addr().unwrap()))
            .collect();
        let base = |id: &str, root: &Path| NodeConfig {
            node_id: id.into(),
            data_dir: root.join(id),
            http_listen: "127.0.0.1:0".parse().unwrap(),
            api_token_file: Some(root.join("token")),
            tls: None,
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
                peers: raft_addrs
                    .iter()
                    .filter(|(p, _)| **p != id)
                    .map(|(p, a)| (p.clone(), *a))
                    .collect(),
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
            "selfpeer.json",
            r#"{"node_id":"m1","data_dir":"/tmp/x","http_listen":"127.0.0.1:0",
                "metadata":{"listen":"127.0.0.1:0","peers":{"m1":"127.0.0.1:2"},"replicas":1,
                "data_nodes":[{"id":"d1","addr":"127.0.0.1:1"}]}}"#,
        ),
    ] {
        assert!(
            NodeConfig::from_file(write(name, body)).is_err(),
            "{name} accepted"
        );
    }
}
