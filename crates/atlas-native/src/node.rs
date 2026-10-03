// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! A runnable native storage node (`atlas-native-node`): any combination of a data node and a
//! metadata replica (Raft + engine), plus an HTTP endpoint for health, metrics, status and a small
//! volume API, and periodic repair/GC on the metadata leader.

use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::json;

use crate::{
    data_node::{DataNodeServer, RemoteDevice},
    device::BlockStore,
    engine::{EngineConfig, MetaBackend, NativeEngine, NativeError, RepairStats},
    http::{Handler, HttpServer, Request, Response},
    metrics::PromText,
    placement::{FailureDomain, Node, PlacementPolicy},
    raft::{RaftConfig, RaftError, Role},
    raft_server::RaftServer,
    tls::TlsIdentity,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub node_id: String,
    pub data_dir: PathBuf,
    pub http_listen: SocketAddr,
    /// File holding a bearer token required on every `/v1/*` request.
    #[serde(default)]
    pub api_token_file: Option<PathBuf>,
    /// Mutual TLS for the Raft and data-node transports (not the HTTP endpoint).
    #[serde(default)]
    pub tls: Option<TlsFiles>,
    #[serde(default)]
    pub data_node: Option<DataNodeRole>,
    #[serde(default)]
    pub metadata: Option<MetadataRole>,
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataNodeRole {
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRole {
    pub listen: SocketAddr,
    /// Every metadata voter: Raft node id to `host:port` (resolved on each connect). This node's
    /// own entry, if listed, is ignored, so one file can describe the whole group.
    pub peers: BTreeMap<String, String>,
    pub data_nodes: Vec<DataNodeSpec>,
    #[serde(default = "default_replicas")]
    pub replicas: usize,
    #[serde(default = "default_extent_bytes")]
    pub extent_bytes: usize,
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "default_proposal_timeout_ms")]
    pub proposal_timeout_ms: u64,
    /// 0 disables the background scrub/repair loop.
    #[serde(default = "default_repair_interval_secs")]
    pub repair_interval_secs: u64,
    /// 0 disables the background GC loop.
    #[serde(default = "default_gc_interval_secs")]
    pub gc_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataNodeSpec {
    /// Must equal that data node's `node_id` (it is also its TLS server name).
    pub id: String,
    /// `host:port`, resolved on each connect.
    pub addr: String,
    #[serde(default)]
    pub zone: Option<String>,
    #[serde(default)]
    pub rack: Option<String>,
    /// Defaults to `id`: one data node per host.
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_free_bytes")]
    pub free_bytes: u64,
}

fn default_max_request_bytes() -> usize {
    64 << 20
}
fn default_replicas() -> usize {
    3
}
fn default_extent_bytes() -> usize {
    4 << 20
}
fn default_tick_ms() -> u64 {
    50
}
fn default_proposal_timeout_ms() -> u64 {
    5000
}
fn default_repair_interval_secs() -> u64 {
    300
}
fn default_gc_interval_secs() -> u64 {
    60
}
fn default_free_bytes() -> u64 {
    1 << 40
}

impl NodeConfig {
    /// Reads a JSON config, first replacing every `${NAME}` with the environment variable
    /// `NAME` (e.g. `${POD_NAME}` in a StatefulSet); an unset variable is an error.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, NativeError> {
        let raw = std::fs::read_to_string(path)?;
        let expanded = expand_env(&raw, |k| std::env::var(k).ok())?;
        let cfg: Self = serde_json::from_str(&expanded)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// The other voters (this node's own entry removed).
    pub fn raft_peers(&self) -> BTreeMap<String, String> {
        self.metadata
            .as_ref()
            .map(|m| {
                m.peers
                    .iter()
                    .filter(|(id, _)| **id != self.node_id)
                    .map(|(id, a)| (id.clone(), a.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn validate(&self) -> Result<(), NativeError> {
        let invalid = |m: String| Err(NativeError::Invalid(m));
        if self.node_id.is_empty() {
            return invalid("node_id must not be empty".into());
        }
        if self.data_node.is_none() && self.metadata.is_none() {
            return invalid("enable at least one of data_node or metadata".into());
        }
        if let Some(m) = &self.metadata {
            for (what, addr) in m.peers.values().map(|a| ("metadata.peers", a)).chain(
                m.data_nodes
                    .iter()
                    .map(|d| ("metadata.data_nodes", &d.addr)),
            ) {
                let port_ok = addr
                    .rsplit_once(':')
                    .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
                if !port_ok {
                    return invalid(format!("{what}: {addr:?} is not host:port"));
                }
            }
            if m.replicas == 0 || m.replicas > m.data_nodes.len() {
                return invalid(format!(
                    "metadata.replicas ({}) must be between 1 and the number of data_nodes ({})",
                    m.replicas,
                    m.data_nodes.len()
                ));
            }
            if m.extent_bytes == 0 || m.extent_bytes as u64 > crate::data_node::MAX_PAYLOAD {
                return invalid("metadata.extent_bytes out of range".into());
            }
            if m.tick_ms == 0 {
                return invalid("metadata.tick_ms must be > 0".into());
            }
            let mut ids: Vec<&str> = m.data_nodes.iter().map(|d| d.id.as_str()).collect();
            ids.sort_unstable();
            if ids.windows(2).any(|w| w[0] == w[1]) {
                return invalid("metadata.data_nodes ids must be unique".into());
            }
        }
        Ok(())
    }
}

/// Pre-bound sockets, for callers (tests, socket activation) that must know addresses up front.
/// A missing listener is bound from the config.
#[derive(Debug, Default)]
pub struct Listeners {
    pub http: Option<TcpListener>,
    pub data_node: Option<TcpListener>,
    pub metadata: Option<TcpListener>,
}

#[derive(Debug, Default)]
struct TaskStats {
    runs: AtomicU64,
    errors: AtomicU64,
}

struct NodeShared {
    id: String,
    token: Option<String>,
    /// Caps request bodies and read lengths alike.
    max_io_bytes: usize,
    raft: Option<Arc<RaftServer>>,
    engine: Option<NativeEngine>,
    data: Mutex<Option<DataNodeServer>>,
    stop: AtomicBool,
    repair: TaskStats,
    gc: TaskStats,
    last_repair: Mutex<Option<RepairStats>>,
}

pub struct NativeNode {
    shared: Arc<NodeShared>,
    http: Option<HttpServer>,
    http_addr: SocketAddr,
    data_addr: Option<SocketAddr>,
    raft_addr: Option<SocketAddr>,
    loops: Vec<JoinHandle<()>>,
}

impl NativeNode {
    pub fn start(cfg: NodeConfig) -> Result<Self, NativeError> {
        Self::start_with(cfg, Listeners::default())
    }

    pub fn start_with(cfg: NodeConfig, listeners: Listeners) -> Result<Self, NativeError> {
        cfg.validate()?;
        std::fs::create_dir_all(&cfg.data_dir)?;
        let tls = cfg
            .tls
            .as_ref()
            .map(|t| TlsIdentity::from_pem_files(&t.ca, &t.cert, &t.key))
            .transpose()?;
        let token = cfg
            .api_token_file
            .as_ref()
            .map(|p| std::fs::read_to_string(p).map(|t| t.trim().to_string()))
            .transpose()?;
        if token.as_deref() == Some("") {
            return Err(NativeError::Invalid("api_token_file is empty".into()));
        }

        let mut data_addr = None;
        let data = match &cfg.data_node {
            Some(role) => {
                let l = match listeners.data_node {
                    Some(l) => l,
                    None => TcpListener::bind(role.listen)?,
                };
                let srv = DataNodeServer::start_with(
                    cfg.node_id.clone(),
                    cfg.data_dir.join("data"),
                    l,
                    tls.clone(),
                )?;
                data_addr = Some(srv.local_addr());
                Some(srv)
            }
            None => None,
        };

        let mut raft_addr = None;
        let (raft, engine, intervals) = match &cfg.metadata {
            Some(m) => {
                let l = match listeners.metadata {
                    Some(l) => l,
                    None => TcpListener::bind(m.listen)?,
                };
                let peers = cfg.raft_peers();
                let rcfg = RaftConfig::new(
                    cfg.node_id.clone(),
                    peers.keys().cloned().collect(),
                    cfg.data_dir.join("raft"),
                );
                let server = Arc::new(RaftServer::start_with(
                    rcfg,
                    l,
                    peers,
                    Duration::from_millis(m.tick_ms),
                    tls.clone(),
                )?);
                raft_addr = Some(server.local_addr());
                let io_timeout = Duration::from_millis(m.proposal_timeout_ms);
                let mut stores = Vec::new();
                for d in &m.data_nodes {
                    let dev: Arc<dyn BlockStore> = match &tls {
                        Some(id) => {
                            Arc::new(RemoteDevice::with_tls(&d.addr, &d.id, id, io_timeout)?)
                        }
                        None => Arc::new(RemoteDevice::new(&d.addr, io_timeout)),
                    };
                    let spec = Node {
                        id: d.id.clone(),
                        failure_domain: FailureDomain {
                            zone: d.zone.clone().unwrap_or_default(),
                            rack: d.rack.clone().unwrap_or_else(|| d.id.clone()),
                            host: d.host.clone().unwrap_or_else(|| d.id.clone()),
                        },
                        free_bytes: d.free_bytes,
                        healthy: true,
                    };
                    stores.push((spec, dev));
                }
                let mut ecfg = EngineConfig::new(cfg.data_dir.join("engine"));
                ecfg.extent_bytes = m.extent_bytes;
                ecfg.placement = PlacementPolicy {
                    replicas: m.replicas,
                    ..PlacementPolicy::default()
                };
                let engine = NativeEngine::open_with(
                    ecfg,
                    stores,
                    MetaBackend::Raft {
                        server: server.clone(),
                        timeout: io_timeout,
                    },
                )?;
                (
                    Some(server),
                    Some(engine),
                    Some((m.repair_interval_secs, m.gc_interval_secs)),
                )
            }
            None => (None, None, None),
        };

        let shared = Arc::new(NodeShared {
            id: cfg.node_id.clone(),
            token,
            max_io_bytes: cfg.max_request_bytes,
            raft,
            engine,
            data: Mutex::new(data),
            stop: AtomicBool::new(false),
            repair: TaskStats::default(),
            gc: TaskStats::default(),
            last_repair: Mutex::new(None),
        });

        let mut loops = Vec::new();
        if let Some((repair_secs, gc_secs)) = intervals {
            if repair_secs > 0 {
                let sh = shared.clone();
                loops.push(thread::spawn(move || {
                    maintenance(
                        &sh,
                        Duration::from_secs(repair_secs),
                        |sh, e| {
                            let st = e.repair_once()?;
                            if let Ok(mut last) = sh.last_repair.lock() {
                                *last = Some(st);
                            }
                            Ok(())
                        },
                        |sh| &sh.repair,
                    )
                }));
            }
            if gc_secs > 0 {
                let sh = shared.clone();
                loops.push(thread::spawn(move || {
                    maintenance(
                        &sh,
                        Duration::from_secs(gc_secs),
                        |_, e| e.gc_once().map(|_| ()),
                        |sh| &sh.gc,
                    )
                }));
            }
        }

        let http_listener = match listeners.http {
            Some(l) => l,
            None => TcpListener::bind(cfg.http_listen)?,
        };
        let handler: Handler = {
            let sh = shared.clone();
            Arc::new(move |req| handle(&sh, req))
        };
        let http = HttpServer::start(http_listener, cfg.max_request_bytes, handler)?;
        Ok(Self {
            shared,
            http_addr: http.local_addr(),
            http: Some(http),
            data_addr,
            raft_addr,
            loops,
        })
    }

    pub fn id(&self) -> &str {
        &self.shared.id
    }
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }
    pub fn data_node_addr(&self) -> Option<SocketAddr> {
        self.data_addr
    }
    pub fn metadata_addr(&self) -> Option<SocketAddr> {
        self.raft_addr
    }

    /// A storage error that stopped the metadata replica (e.g. a failed fsync), if any. The
    /// process should exit so its supervisor restarts it from disk.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.raft.as_ref().and_then(|r| r.fatal_error())
    }

    pub fn shutdown(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        if let Some(mut h) = self.http.take() {
            h.shutdown();
        }
        for l in self.loops.drain(..) {
            let _ = l.join();
        }
        if let Ok(mut d) = self.shared.data.lock() {
            if let Some(mut srv) = d.take() {
                srv.shutdown();
            }
        }
    }
}

impl Drop for NativeNode {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Runs `task` every `every` while this replica is the metadata leader.
fn maintenance(
    sh: &NodeShared,
    every: Duration,
    task: impl Fn(&NodeShared, &NativeEngine) -> Result<(), NativeError>,
    stats: impl Fn(&NodeShared) -> &TaskStats,
) {
    let (Some(engine), Some(raft)) = (&sh.engine, &sh.raft) else {
        return;
    };
    let mut next = Instant::now() + every;
    while !sh.stop.load(Ordering::SeqCst) {
        if Instant::now() < next {
            thread::sleep(Duration::from_millis(50).min(every));
            continue;
        }
        next = Instant::now() + every;
        if !raft.status().is_ok_and(|s| s.role == Role::Leader) {
            continue;
        }
        let st = stats(sh);
        st.runs.fetch_add(1, Ordering::Relaxed);
        match task(sh, engine) {
            Ok(()) | Err(NativeError::Raft(RaftError::NotLeader { .. })) => {}
            Err(_) => {
                st.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Replaces `${NAME}` with `lookup(NAME)`. `$` not followed by `{` is left alone.
fn expand_env(raw: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String, NativeError> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| NativeError::Invalid("unterminated ${ in config".into()))?;
        let name = &after[..end];
        let value = lookup(name).ok_or_else(|| {
            NativeError::Invalid(format!(
                "config references unset environment variable {name}"
            ))
        })?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn token_ok(expected: &str, header: Option<&String>) -> bool {
    let Some(got) = header.and_then(|h| h.strip_prefix("Bearer ")) else {
        return false;
    };
    let (a, b) = (expected.as_bytes(), got.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn error_response(e: NativeError) -> Response {
    let (status, leader) = match &e {
        NativeError::NotFound(_) => (404, None),
        NativeError::Invalid(_) => (400, None),
        NativeError::Raft(RaftError::NotLeader { leader }) => (421, leader.clone()),
        NativeError::Raft(RaftError::Rejected(_)) | NativeError::Metadata(_) => (409, None),
        NativeError::InsufficientReplicas { .. }
        | NativeError::Fenced { .. }
        | NativeError::Raft(
            RaftError::LeadershipLost { .. }
            | RaftError::TermChanged { .. }
            | RaftError::Timeout { .. },
        ) => (503, None),
        _ => (500, None),
    };
    Response::json(status, &json!({ "error": e.to_string(), "leader": leader }))
}

fn query_u64(req: &Request, key: &str) -> Result<u64, Response> {
    req.query
        .get(key)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Response::text(400, format!("query parameter {key} (integer) is required")))
}

fn read_range(sh: &NodeShared, req: &Request) -> Result<(u64, usize), Response> {
    let offset = query_u64(req, "offset")?;
    let len = query_u64(req, "len")?;
    if len > sh.max_io_bytes as u64 {
        return Err(Response::text(
            413,
            format!("len exceeds {} bytes", sh.max_io_bytes),
        ));
    }
    Ok((offset, len as usize))
}

fn body_json(req: &Request) -> Result<serde_json::Value, Response> {
    serde_json::from_slice(&req.body)
        .map_err(|e| Response::text(400, format!("invalid JSON body: {e}")))
}

fn handle(sh: &NodeShared, req: Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/healthz") => return Response::text(200, "ok"),
        ("GET", "/readyz") => return readiness(sh),
        ("GET", "/metrics") => return metrics(sh),
        _ => {}
    }
    if let Some(t) = &sh.token {
        if !token_ok(t, req.headers.get("authorization")) {
            return Response::text(401, "missing or invalid bearer token");
        }
    }
    let segs: Vec<&str> = req.path.trim_matches('/').split('/').collect();
    if let ("GET", ["v1", "status"]) = (req.method.as_str(), segs.as_slice()) {
        return status(sh);
    }
    let Some(e) = &sh.engine else {
        return Response::text(404, "the metadata role is not enabled on this node");
    };
    let result = match (req.method.as_str(), segs.as_slice()) {
        ("GET", ["v1", "volumes"]) => e
            .volumes()
            .map(|v| Response::json(200, &json!({ "volumes": v }))),
        ("POST", ["v1", "volumes"]) => {
            let body = match body_json(&req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let (Some(name), Some(size)) = (body["name"].as_str(), body["size_bytes"].as_u64())
            else {
                return Response::text(
                    400,
                    "body must be {\"name\": string, \"size_bytes\": integer}",
                );
            };
            e.create_volume(name, size)
                .map(|id| Response::json(201, &json!({ "id": id })))
        }
        ("DELETE", ["v1", "volumes", id]) => e.delete_volume(id).map(|()| Response::text(204, "")),
        ("PUT", ["v1", "volumes", id, "data"]) => {
            let offset = match query_u64(&req, "offset") {
                Ok(o) => o,
                Err(r) => return r,
            };
            e.write(id, offset, &req.body)
                .map(|()| Response::text(204, ""))
        }
        ("GET", ["v1", "volumes", id, "data"]) => {
            let (offset, len) = match read_range(sh, &req) {
                Ok(r) => r,
                Err(r) => return r,
            };
            e.read(id, offset, len).map(|b| Response::bytes(200, b))
        }
        ("POST", ["v1", "volumes", id, "snapshots"]) => {
            let body = match body_json(&req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let Some(name) = body["name"].as_str() else {
                return Response::text(400, "body must be {\"name\": string}");
            };
            e.create_snapshot(id, name)
                .map(|sid| Response::json(201, &json!({ "id": sid })))
        }
        ("DELETE", ["v1", "snapshots", id]) => {
            e.delete_snapshot(id).map(|()| Response::text(204, ""))
        }
        ("GET", ["v1", "snapshots", id, "data"]) => {
            let (offset, len) = match read_range(sh, &req) {
                Ok(r) => r,
                Err(r) => return r,
            };
            e.read_snapshot(id, offset, len)
                .map(|b| Response::bytes(200, b))
        }
        ("POST", ["v1", "repair"]) => e.repair_once().map(|st| {
            if let Ok(mut last) = sh.last_repair.lock() {
                *last = Some(st);
            }
            Response::json(200, &json!(st))
        }),
        ("POST", ["v1", "gc"]) => e.gc_once().map(|st| Response::json(200, &json!(st))),
        _ => return Response::text(404, "no such route"),
    };
    result.unwrap_or_else(error_response)
}

fn readiness(sh: &NodeShared) -> Response {
    if let Some(r) = &sh.raft {
        if let Some(err) = r.fatal_error() {
            return Response::text(503, format!("metadata replica stopped: {err}"));
        }
        match r.status() {
            Ok(s) if s.leader.is_some() => {}
            _ => return Response::text(503, "no metadata leader known"),
        }
    }
    if sh.data.lock().map(|d| d.is_none()).unwrap_or(true) && sh.raft.is_none() {
        return Response::text(503, "no role running");
    }
    Response::text(200, "ready")
}

fn status(sh: &NodeShared) -> Response {
    let raft = sh.raft.as_ref().and_then(|r| r.status().ok()).map(|s| {
        json!({
            "role": format!("{:?}", s.role).to_lowercase(),
            "term": s.term,
            "leader": s.leader,
            "commit_index": s.commit_index,
            "applied_index": s.applied_index,
        })
    });
    let nodes = sh.engine.as_ref().map(NativeEngine::node_status);
    let last_repair = sh.last_repair.lock().ok().and_then(|l| *l);
    let fence = sh
        .data
        .lock()
        .ok()
        .and_then(|d| d.as_ref().map(DataNodeServer::fence));
    Response::json(
        200,
        &json!({
            "node_id": sh.id,
            "metadata": raft,
            "data_nodes": nodes,
            "last_repair": last_repair,
            "data_node": fence.map(|f| json!({ "fence": f })),
        }),
    )
}

fn metrics(sh: &NodeShared) -> Response {
    let mut out = String::new();
    if let Some(r) = &sh.raft {
        if let Ok(m) = r.render_metrics() {
            out.push_str(&m);
        }
    }
    if let Some(e) = &sh.engine {
        if let Ok(m) = e.render_metrics() {
            out.push_str(&m);
        }
        let node = [("node", sh.id.as_str())];
        let mut p = PromText::new();
        for (task, st) in [("repair", &sh.repair), ("gc", &sh.gc)] {
            for (suffix, help, v) in [
                (
                    "runs",
                    "Background maintenance runs on the leader.",
                    &st.runs,
                ),
                (
                    "errors",
                    "Background maintenance runs that failed.",
                    &st.errors,
                ),
            ] {
                let name = format!("atlas_native_{task}_{suffix}_total");
                p.family(&name, "counter", help)
                    .sample(&name, &node, v.load(Ordering::Relaxed));
            }
        }
        out.push_str(&p.finish());
    }
    if let Ok(d) = sh.data.lock() {
        if let Some(m) = d.as_ref().and_then(|d| d.render_metrics().ok()) {
            out.push_str(&m);
        }
    }
    Response {
        status: 200,
        content_type: "text/plain; version=0.0.4",
        body: out.into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::expand_env;

    #[test]
    fn expands_environment_references() {
        let env = |k: &str| (k == "POD_NAME").then(|| "atlas-native-1".to_string());
        assert_eq!(
            expand_env(r#"{"id":"${POD_NAME}","cost":"$5"}"#, env).unwrap(),
            r#"{"id":"atlas-native-1","cost":"$5"}"#
        );
        assert!(expand_env("${MISSING}", env).is_err());
        assert!(expand_env("${POD_NAME", env).is_err());
    }
}
