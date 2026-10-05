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
    ec::EcScheme,
    engine::{EngineConfig, MetaBackend, NativeEngine, NativeError, ObjectKind, RepairStats},
    gc::GcStats,
    http::{Handler, HttpServer, Request, Response},
    metadata::MetaError,
    metrics::{self, PromText},
    placement::{FailureDomain, Node, PlacementPolicy},
    raft::{RaftConfig, RaftError, Role},
    raft_server::{RaftMux, RaftServer},
    raw::{open_store, DeviceBackend},
    tls::TlsIdentity,
};

mod cache_leases;
mod fs_api;
mod shards;

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
    /// Serve the HTTP API over TLS.
    #[serde(default)]
    pub http_tls: Option<HttpTlsFiles>,
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
pub struct HttpTlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// When set, every `/v1/*` request must present a client certificate signed by this CA
    /// (on top of the bearer token, if one is configured). `/healthz`, `/readyz` and `/metrics`
    /// stay reachable without one so probes and scrapers keep working.
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataNodeRole {
    pub listen: SocketAddr,
    /// Devices to serve, in index order. Empty: one `file` device at `<data_dir>/data/nvme0.data`.
    #[serde(default)]
    pub devices: Vec<DeviceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    /// A regular file, or a block device for the `aligned` and `io_uring` backends.
    pub path: PathBuf,
    #[serde(default)]
    pub backend: DeviceBackend,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRole {
    pub listen: SocketAddr,
    /// Every metadata voter: Raft node id to `host:port` (resolved on each connect). This node's
    /// own entry, if listed, is ignored, so one file can describe the whole group.
    pub peers: BTreeMap<String, String>,
    /// Initial voters, used until the first membership change (default: every `peers` entry).
    /// A node not listed starts as a non-voter and waits to be added through `POST /v1/members`.
    #[serde(default)]
    pub bootstrap: Option<Vec<String>>,
    pub data_nodes: Vec<DataNodeSpec>,
    #[serde(default = "default_replicas")]
    pub replicas: usize,
    /// Erasure-code extents of at least `erasure_min_bytes` (default 64 KiB) as `"<data>+<parity>"`
    /// shards (e.g. `"4+2"`, `"8+3"`), each on its own data node; smaller extents keep
    /// `replicas` copies. Applies to extents written from then on.
    #[serde(default)]
    pub erasure: Option<EcScheme>,
    #[serde(default = "default_erasure_min_bytes")]
    pub erasure_min_bytes: usize,
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
    /// Raft groups the namespace is sharded across, all on `listen`. Every metadata node must
    /// run the same number. Raising it later is safe (existing objects stay in their group);
    /// lowering it would strand the objects of the groups dropped.
    #[serde(default = "default_groups")]
    pub groups: u32,
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
    /// How many devices that data node serves (its `data_node.devices`). Raise this only once
    /// the data node runs a version that serves several devices.
    #[serde(default = "default_devices")]
    pub devices: usize,
}

fn default_devices() -> usize {
    1
}
fn default_groups() -> u32 {
    1
}
/// Upper bound on `metadata.groups`: each group runs its own Raft log and engine.
pub const MAX_GROUPS: u32 = 64;

fn default_max_request_bytes() -> usize {
    64 << 20
}
fn default_replicas() -> usize {
    3
}
fn default_erasure_min_bytes() -> usize {
    crate::engine::DEFAULT_ERASURE_MIN_BYTES
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
                if !valid_host_port(addr) {
                    return invalid(format!("{what}: {addr:?} is not host:port"));
                }
            }
            if let Some(b) = &m.bootstrap {
                if b.is_empty() {
                    return invalid("metadata.bootstrap must not be empty".into());
                }
                if let Some(id) = b
                    .iter()
                    .find(|id| **id != self.node_id && !m.peers.contains_key(*id))
                {
                    return invalid(format!(
                        "metadata.bootstrap: {id} has no metadata.peers entry"
                    ));
                }
            }
            if m.replicas == 0 || m.replicas > m.data_nodes.len() {
                return invalid(format!(
                    "metadata.replicas ({}) must be between 1 and the number of data_nodes ({})",
                    m.replicas,
                    m.data_nodes.len()
                ));
            }
            if let Some(e) = m.erasure {
                if e.shards() > m.data_nodes.len() {
                    return invalid(format!(
                        "metadata.erasure {e} needs {} data_nodes, {} configured",
                        e.shards(),
                        m.data_nodes.len()
                    ));
                }
            }
            if m.extent_bytes == 0 || m.extent_bytes as u64 > crate::data_node::MAX_PAYLOAD {
                return invalid("metadata.extent_bytes out of range".into());
            }
            if m.tick_ms == 0 {
                return invalid("metadata.tick_ms must be > 0".into());
            }
            if !(1..=MAX_GROUPS).contains(&m.groups) {
                return invalid(format!(
                    "metadata.groups must be between 1 and {MAX_GROUPS}"
                ));
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

fn valid_host_port(addr: &str) -> bool {
    addr.rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
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
    require_client_cert: bool,
    /// Bounds `POST /v1/members` (joint and final entries, including a new voter catching up).
    membership_timeout: Duration,
    /// `(extent_bytes, replicas)` of the metadata role, reported by `/v1/status`.
    layout: Option<(usize, usize)>,
    /// The metadata groups this node replicates (empty without the metadata role).
    groups: Vec<MetaGroup>,
    data: Mutex<Option<DataNodeServer>>,
    stop: AtomicBool,
    repair: TaskStats,
    gc: TaskStats,
    last_repair: Mutex<Option<RepairStats>>,
    /// Client sessions this node expired as a group leader.
    sessions_expired: AtomicU64,
}

/// One metadata Raft group on this node and the engine committing through it.
struct MetaGroup {
    raft: Arc<RaftServer>,
    engine: NativeEngine,
    /// Cache leases this replica granted while leading the group.
    leases: cache_leases::CacheLeases,
}

/// Group 0 keeps the directories of a node that predates groups.
fn group_dir(root: &Path, name: &str, group: u32) -> PathBuf {
    if group == 0 {
        root.join(name)
    } else {
        root.join(format!("{name}-g{group}"))
    }
}

/// Objects in a group above `groups` would become unreachable, so a lowered count is refused.
fn refuse_fewer_groups(root: &Path, groups: u32) -> Result<(), NativeError> {
    for entry in std::fs::read_dir(root)? {
        let name = entry?.file_name();
        let Some(g) = name
            .to_str()
            .and_then(|n| n.strip_prefix("raft-g"))
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        if g >= groups {
            return Err(NativeError::Invalid(format!(
                "metadata.groups is {groups} but {} holds group {g}; the group count can be \
                 raised but never lowered",
                root.display()
            )));
        }
    }
    Ok(())
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
        let http_tls = cfg
            .http_tls
            .as_ref()
            .map(|t| -> Result<_, NativeError> {
                let ca = t.client_ca.as_ref().map(std::fs::read).transpose()?;
                Ok(crate::tls::http_server_config(
                    &std::fs::read(&t.cert)?,
                    &std::fs::read(&t.key)?,
                    ca.as_deref(),
                )?)
            })
            .transpose()?;

        let mut data_addr = None;
        let data = match &cfg.data_node {
            Some(role) => {
                let l = match listeners.data_node {
                    Some(l) => l,
                    None => TcpListener::bind(role.listen)?,
                };
                let root = cfg.data_dir.join("data");
                let srv = if role.devices.is_empty() {
                    DataNodeServer::start_with(cfg.node_id.clone(), root, l, tls.clone())?
                } else {
                    let devices = role
                        .devices
                        .iter()
                        .enumerate()
                        .map(|(i, d)| open_store(&d.path, d.backend, &root.join(format!("dev{i}"))))
                        .collect::<Result<Vec<_>, _>>()?;
                    DataNodeServer::start_devices(
                        cfg.node_id.clone(),
                        root,
                        devices,
                        l,
                        tls.clone(),
                    )?
                };
                data_addr = Some(srv.local_addr());
                Some(srv)
            }
            None => None,
        };

        let mut raft_addr = None;
        let mut groups = Vec::new();
        let intervals = match &cfg.metadata {
            Some(m) => {
                let l = match listeners.metadata {
                    Some(l) => l,
                    None => TcpListener::bind(m.listen)?,
                };
                refuse_fewer_groups(&cfg.data_dir, m.groups)?;
                let mux = RaftMux::start(l, tls.as_ref())?;
                raft_addr = Some(mux.local_addr());
                let peers = cfg.raft_peers();
                let io_timeout = Duration::from_millis(m.proposal_timeout_ms);
                for g in 0..m.groups {
                    let mut rcfg = RaftConfig::new(
                        cfg.node_id.clone(),
                        peers.keys().cloned().collect(),
                        group_dir(&cfg.data_dir, "raft", g),
                    );
                    rcfg.bootstrap = m.bootstrap.clone();
                    let server = Arc::new(RaftServer::start_in(
                        &mux,
                        g,
                        rcfg,
                        peers.clone(),
                        Duration::from_millis(m.tick_ms),
                        tls.clone(),
                    )?);
                    let mut stores = Vec::new();
                    for d in &m.data_nodes {
                        let mut devs: Vec<Arc<dyn BlockStore>> = Vec::new();
                        for index in 0..d.devices.max(1) {
                            let dev = match &tls {
                                Some(id) => RemoteDevice::with_tls(&d.addr, &d.id, id, io_timeout)?,
                                None => RemoteDevice::new(&d.addr, io_timeout),
                            };
                            devs.push(Arc::new(dev.on_device(index).in_group(g)));
                        }
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
                        stores.push((spec, devs));
                    }
                    let mut ecfg = EngineConfig::new(group_dir(&cfg.data_dir, "engine", g));
                    ecfg.extent_bytes = m.extent_bytes;
                    ecfg.erasure = m.erasure;
                    ecfg.erasure_min_bytes = m.erasure_min_bytes;
                    ecfg.placement = PlacementPolicy {
                        replicas: m.replicas,
                        ..PlacementPolicy::default()
                    };
                    let engine = NativeEngine::open_with_devices(
                        ecfg,
                        stores,
                        MetaBackend::Raft {
                            server: server.clone(),
                            timeout: io_timeout,
                        },
                    )?;
                    groups.push(MetaGroup {
                        raft: server,
                        engine,
                        leases: Default::default(),
                    });
                }
                Some((m.repair_interval_secs, m.gc_interval_secs))
            }
            None => None,
        };

        let shared = Arc::new(NodeShared {
            id: cfg.node_id.clone(),
            token,
            max_io_bytes: cfg.max_request_bytes,
            membership_timeout: cfg
                .metadata
                .as_ref()
                .map(|m| Duration::from_millis(m.proposal_timeout_ms.saturating_mul(6)))
                .unwrap_or_default(),
            require_client_cert: cfg.http_tls.as_ref().is_some_and(|t| t.client_ca.is_some()),
            layout: cfg.metadata.as_ref().map(|m| (m.extent_bytes, m.replicas)),
            groups,
            data: Mutex::new(data),
            stop: AtomicBool::new(false),
            repair: TaskStats::default(),
            gc: TaskStats::default(),
            last_repair: Mutex::new(None),
            sessions_expired: AtomicU64::new(0),
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
            {
                let sh = shared.clone();
                loops.push(thread::spawn(move || expire_leases(&sh)));
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
        let http = HttpServer::start_with(http_listener, cfg.max_request_bytes, handler, http_tls)?;
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
        self.shared.groups.iter().find_map(|g| g.raft.fatal_error())
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

/// Runs `task` every `every` on each metadata group this replica leads.
fn maintenance(
    sh: &NodeShared,
    every: Duration,
    task: impl Fn(&NodeShared, &NativeEngine) -> Result<(), NativeError>,
    stats: impl Fn(&NodeShared) -> &TaskStats,
) {
    let mut next = Instant::now() + every;
    while !sh.stop.load(Ordering::SeqCst) {
        if Instant::now() < next {
            thread::sleep(Duration::from_millis(50).min(every));
            continue;
        }
        next = Instant::now() + every;
        for g in &sh.groups {
            if !g.raft.status().is_ok_and(|s| s.role == Role::Leader) {
                continue;
            }
            let st = stats(sh);
            st.runs.fetch_add(1, Ordering::Relaxed);
            match task(sh, &g.engine) {
                Ok(()) | Err(NativeError::Raft(RaftError::NotLeader { .. })) => {}
                Err(_) => {
                    st.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// How often a leader looks for client sessions to expire.
const LEASE_TICK: Duration = Duration::from_secs(1);

/// Expires client sessions in each group this replica leads. A new leader first waits out the
/// longest session TTL, so every live session (last renewed against the old leader, whose clock
/// may differ) has been renewed against this one before anything is expired.
fn expire_leases(sh: &NodeShared) {
    let mut since: Vec<Option<(u64, Instant)>> = vec![None; sh.groups.len()];
    let mut next = Instant::now();
    while !sh.stop.load(Ordering::SeqCst) {
        if Instant::now() < next {
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        next = Instant::now() + LEASE_TICK;
        for (g, tenure) in sh.groups.iter().zip(&mut since) {
            let term = match g.raft.status() {
                Ok(s) if s.role == Role::Leader => s.term,
                _ => {
                    *tenure = None;
                    continue;
                }
            };
            let start = match *tenure {
                Some((t, at)) if t == term => at,
                _ => tenure.insert((term, Instant::now())).1,
            };
            let Ok(ttl) = g.engine.max_session_ttl_ms() else {
                continue;
            };
            if ttl == 0 || start.elapsed() < Duration::from_millis(ttl) {
                continue;
            }
            if let Ok(n) = g.engine.expire_sessions() {
                sh.sessions_expired.fetch_add(n as u64, Ordering::Relaxed);
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
    let meta = match &e {
        NativeError::Metadata(m) | NativeError::Raft(RaftError::Rejected(m)) => Some(m),
        _ => None,
    };
    // `code` is stable for clients (the FUSE client maps it to an errno); `error` is for humans.
    let (status, code, leader) = match (&e, meta) {
        (NativeError::NotFound(_), _) | (_, Some(MetaError::NotFound(_))) => {
            (404, "not_found", None)
        }
        (_, Some(MetaError::NoAttr(_))) => (404, "no_attr", None),
        (_, Some(MetaError::NoSession)) => (410, "no_session", None),
        (_, Some(m)) => (
            409,
            match m {
                MetaError::Exists(_) => "exists",
                MetaError::NotEmpty(_) => "not_empty",
                MetaError::NotDir(_) => "not_dir",
                MetaError::IsDir(_) => "is_dir",
                MetaError::TooBig(_) => "too_big",
                MetaError::Unsupported(_) => "unsupported",
                MetaError::Locked(_) => "locked",
                _ => "invalid",
            },
            None,
        ),
        (NativeError::Invalid(_) | NativeError::Raft(RaftError::Config(_)), _) => {
            (400, "invalid", None)
        }
        (NativeError::ReadOnly(_), _) => (409, "read_only", None),
        (NativeError::Raft(RaftError::NotLeader { leader }), _) => {
            (421, "not_leader", leader.clone())
        }
        (NativeError::Raft(RaftError::MembershipBusy), _) => (409, "busy", None),
        (
            NativeError::InsufficientReplicas { .. }
            | NativeError::Fenced { .. }
            | NativeError::Raft(
                RaftError::LeadershipLost { .. }
                | RaftError::TermChanged { .. }
                | RaftError::Timeout { .. },
            ),
            _,
        ) => (503, "unavailable", None),
        _ => (500, "internal", None),
    };
    Response::json(
        status,
        &json!({ "error": e.to_string(), "code": code, "leader": leader }),
    )
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

/// `{"voters": {"<id>": "<raft host:port>", ...}}`: the complete new voter set. Each group's
/// leader applies it to its group: this node changes the groups it leads, skips those already
/// there, and answers 421 while another node's groups still need it, so a client repeating the
/// request across the metadata nodes (as it does for any 421) completes the change.
fn change_members(sh: &NodeShared, req: &Request) -> Response {
    let body = match body_json(req) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let Some(voters) = body["voters"].as_object().and_then(|o| {
        o.iter()
            .map(|(k, v)| v.as_str().map(|a| (k.clone(), a.to_string())))
            .collect::<Option<BTreeMap<String, String>>>()
    }) else {
        return Response::text(
            400,
            "body must be {\"voters\": {\"<id>\": \"<host:port>\"}}",
        );
    };
    if let Some((id, a)) = voters
        .iter()
        .find(|(id, a)| **id != sh.id && !valid_host_port(a))
    {
        return Response::text(400, format!("voter {id}: address {a:?} is not host:port"));
    }
    let target = crate::membership::Membership::stable(voters.keys().cloned());
    let mut first = None;
    let mut elsewhere = None;
    for g in &sh.groups {
        match g
            .raft
            .change_membership(voters.clone(), sh.membership_timeout)
        {
            Ok(m) => {
                first.get_or_insert(m);
            }
            Err(RaftError::NotLeader { leader }) => match g.raft.membership() {
                Ok((m, _)) if m == target => {
                    first.get_or_insert(m);
                }
                _ => {
                    elsewhere.get_or_insert(leader);
                }
            },
            Err(e) => return error_response(e.into()),
        }
    }
    if let Some(leader) = elsewhere {
        return error_response(RaftError::NotLeader { leader }.into());
    }
    Response::json(200, &json!({ "membership": first }))
}

/// The optional client-chosen `"id"` of a create (repeating a create with the same id is a
/// no-op), else a fresh UUID.
fn client_id(body: &serde_json::Value) -> Result<String, Response> {
    match &body["id"] {
        serde_json::Value::Null => Ok(uuid::Uuid::new_v4().to_string()),
        serde_json::Value::String(id)
            if (1..=64).contains(&id.len())
                && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') =>
        {
            Ok(id.clone())
        }
        _ => Err(Response::text(
            400,
            "\"id\" must be 1-64 characters of [A-Za-z0-9-]",
        )),
    }
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
    if sh.require_client_cert && !req.client_verified {
        return Response::text(
            401,
            "a client certificate signed by http_tls.client_ca is required",
        );
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
    let Some(g0) = sh.groups.first() else {
        return Response::text(404, "the metadata role is not enabled on this node");
    };
    if let ["v1", "members"] = segs.as_slice() {
        return match req.method.as_str() {
            "GET" => {
                let groups: Result<Vec<_>, _> = sh
                    .groups
                    .iter()
                    .map(|g| {
                        g.raft
                            .membership()
                            .map(|(m, _)| json!({ "group": g.raft.group(), "membership": m }))
                    })
                    .collect();
                match (g0.raft.membership(), groups) {
                    (Ok((m, addrs)), Ok(groups)) => Response::json(
                        200,
                        &json!({ "membership": m, "addrs": addrs, "groups": groups }),
                    ),
                    (Err(e), _) | (_, Err(e)) => error_response(e.into()),
                }
            }
            "POST" => change_members(sh, &req),
            _ => Response::text(405, "GET or POST"),
        };
    }
    if let ["v1", "fs" | "fs-snapshots", ..] = segs.as_slice() {
        return fs_api::route(sh, &req, &segs).unwrap_or_else(error_response);
    }
    volume_route(sh, &req, &segs)
}

/// The engine of the group holding volume `id`, else of the group a new one goes to.
fn volume_engine<'a>(sh: &'a NodeShared, id: &str) -> Result<&'a NativeEngine, NativeError> {
    Ok(&sh.route(ObjectKind::Volume, id)?.1.engine)
}

fn snapshot_engine<'a>(sh: &'a NodeShared, id: &str) -> Result<&'a NativeEngine, NativeError> {
    Ok(&sh.route(ObjectKind::Snapshot, id)?.1.engine)
}

fn volume_route(sh: &NodeShared, req: &Request, segs: &[&str]) -> Response {
    let result = match (req.method.as_str(), segs) {
        ("GET", ["v1", "volumes"]) => sh
            .groups
            .iter()
            .map(|g| g.engine.volumes())
            .collect::<Result<Vec<_>, _>>()
            .map(|v| {
                let mut all: Vec<_> = v.into_iter().flatten().collect();
                all.sort_by(|a, b| a.id.cmp(&b.id));
                Response::json(200, &json!({ "volumes": all }))
            }),
        ("POST", ["v1", "volumes"]) => {
            let body = match body_json(req) {
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
            let id = match client_id(&body) {
                Ok(id) => id,
                Err(r) => return r,
            };
            volume_engine(sh, &id)
                .and_then(|e| e.create_volume_as(id, name, size))
                .map(|id| Response::json(201, &json!({ "id": id })))
        }
        ("DELETE", ["v1", "volumes", id]) => volume_engine(sh, id)
            .and_then(|e| e.delete_volume(id))
            .map(|()| Response::text(204, "")),
        ("POST", ["v1", "volumes", id, "resize"]) => {
            let body = match body_json(req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let Some(size) = body["size_bytes"].as_u64() else {
                return Response::text(400, "body must be {\"size_bytes\": integer}");
            };
            volume_engine(sh, id)
                .and_then(|e| e.resize_volume(id, size))
                .map(|()| Response::json(200, &json!({ "id": id, "size_bytes": size })))
        }
        ("PUT", ["v1", "volumes", id, "data"]) => {
            let offset = match query_u64(req, "offset") {
                Ok(o) => o,
                Err(r) => return r,
            };
            volume_engine(sh, id)
                .and_then(|e| e.write(id, offset, &req.body))
                .map(|()| Response::text(204, ""))
        }
        ("GET", ["v1", "volumes", id, "data"]) => {
            let (offset, len) = match read_range(sh, req) {
                Ok(r) => r,
                Err(r) => return r,
            };
            volume_engine(sh, id)
                .and_then(|e| e.read(id, offset, len))
                .map(|b| Response::bytes(200, b))
        }
        ("POST", ["v1", "volumes", id, "snapshots"]) => {
            let body = match body_json(req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let Some(name) = body["name"].as_str() else {
                return Response::text(400, "body must be {\"name\": string}");
            };
            let sid = match client_id(&body) {
                Ok(id) => id,
                Err(r) => return r,
            };
            // A snapshot shares its volume's extents, so it lives in the volume's group.
            sh.route(ObjectKind::Volume, id)
                .and_then(|(g, group)| {
                    sh.claim(ObjectKind::Snapshot, &sid, g)?;
                    group.engine.create_snapshot_as(sid, id, name)
                })
                .map(|sid| Response::json(201, &json!({ "id": sid })))
        }
        ("DELETE", ["v1", "snapshots", id]) => snapshot_engine(sh, id)
            .and_then(|e| e.delete_snapshot(id))
            .map(|()| Response::text(204, "")),
        ("POST", ["v1", "snapshots", id, "clone"]) => {
            let body = match body_json(req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let size = &body["size_bytes"];
            let (Some(name), true) = (body["name"].as_str(), size.is_null() || size.is_u64())
            else {
                return Response::text(
                    400,
                    "body must be {\"name\": string, \"size_bytes\"?: integer}",
                );
            };
            let vid = match client_id(&body) {
                Ok(id) => id,
                Err(r) => return r,
            };
            // A clone shares the snapshot's extents, so it lives in the snapshot's group.
            sh.route(ObjectKind::Snapshot, id)
                .and_then(|(g, group)| {
                    sh.claim(ObjectKind::Volume, &vid, g)?;
                    group.engine.clone_snapshot_as(vid, id, name, size.as_u64())
                })
                .map(|vid| Response::json(201, &json!({ "id": vid })))
        }
        ("GET", ["v1", "snapshots", id, "data"]) => {
            let (offset, len) = match read_range(sh, req) {
                Ok(r) => r,
                Err(r) => return r,
            };
            snapshot_engine(sh, id)
                .and_then(|e| e.read_snapshot(id, offset, len))
                .map(|b| Response::bytes(200, b))
        }
        ("POST", ["v1", "repair"]) => repair_all(sh).map(|st| Response::json(200, &json!(st))),
        ("POST", ["v1", "gc"]) => gc_all(sh).map(|st| Response::json(200, &json!(st))),
        _ => return Response::text(404, "no such route"),
    };
    result.unwrap_or_else(error_response)
}

/// The groups this node leads, or `NotLeader` (pointing at group 0's leader) if none.
fn led_groups(sh: &NodeShared) -> Result<Vec<&MetaGroup>, NativeError> {
    let led: Vec<&MetaGroup> = sh
        .groups
        .iter()
        .filter(|g| g.raft.status().is_ok_and(|s| s.role == Role::Leader))
        .collect();
    if led.is_empty() {
        let leader = sh
            .groups
            .first()
            .and_then(|g| g.raft.status().ok())
            .and_then(|s| s.leader);
        return Err(RaftError::NotLeader { leader }.into());
    }
    Ok(led)
}

/// One repair pass over each group this node leads.
fn repair_all(sh: &NodeShared) -> Result<RepairStats, NativeError> {
    let mut st = RepairStats::default();
    for g in led_groups(sh)? {
        let r = g.engine.repair_once()?;
        st.extents_checked += r.extents_checked;
        st.replicas_repaired += r.replicas_repaired;
        st.unrecoverable += r.unrecoverable;
        st.deferred += r.deferred;
    }
    if let Ok(mut last) = sh.last_repair.lock() {
        *last = Some(st);
    }
    Ok(st)
}

/// One GC pass over each group this node leads.
fn gc_all(sh: &NodeShared) -> Result<GcStats, NativeError> {
    let mut st = GcStats::default();
    for g in led_groups(sh)? {
        let r = g.engine.gc_once()?;
        st.candidates += r.candidates;
        st.reclaimed += r.reclaimed;
        st.freed_bytes += r.freed_bytes;
    }
    Ok(st)
}

fn readiness(sh: &NodeShared) -> Response {
    for g in &sh.groups {
        let n = g.raft.group();
        if let Some(err) = g.raft.fatal_error() {
            return Response::text(503, format!("metadata replica (group {n}) stopped: {err}"));
        }
        match g.raft.status() {
            // A non-voter hears from no leader until it is added; it is ready to be added.
            Ok(s) if s.leader.is_some() || !s.voter => {}
            _ => return Response::text(503, format!("no metadata leader known for group {n}")),
        }
    }
    if sh.data.lock().map(|d| d.is_none()).unwrap_or(true) && sh.groups.is_empty() {
        return Response::text(503, "no role running");
    }
    Response::text(200, "ready")
}

fn status(sh: &NodeShared) -> Response {
    let groups: Vec<serde_json::Value> = sh
        .groups
        .iter()
        .filter_map(|g| {
            g.raft.status().ok().map(|s| {
                json!({
                    "group": g.raft.group(),
                    "role": format!("{:?}", s.role).to_lowercase(),
                    "term": s.term,
                    "leader": s.leader,
                    "commit_index": s.commit_index,
                    "applied_index": s.applied_index,
                    "voter": s.voter,
                })
            })
        })
        .collect();
    let nodes = sh.groups.first().map(|g| g.engine.node_status());
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
            // Group 0, as before groups existed; `metadata_groups` lists every group.
            "metadata": groups.first(),
            "metadata_groups": (!groups.is_empty()).then_some(&groups),
            "layout": sh.layout.map(|(e, r)| json!({ "extent_bytes": e, "replicas": r })),
            "data_nodes": nodes,
            "last_repair": last_repair,
            "data_node": fence.map(|f| json!({ "fence": f })),
        }),
    )
}

fn metrics(sh: &NodeShared) -> Response {
    let mut parts = Vec::new();
    for g in &sh.groups {
        if let Ok(m) = g.raft.render_metrics() {
            parts.push(m);
        }
    }
    for g in &sh.groups {
        if let Ok(m) = g.engine.render_metrics() {
            // Several groups' engines are told apart by a group label.
            parts.push(if sh.groups.len() > 1 {
                metrics::with_label(&m, "group", &g.raft.group().to_string())
            } else {
                m
            });
        }
    }
    if !sh.groups.is_empty() {
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
        let name = "atlas_native_cache_leases";
        let fam = p.family(
            name,
            "gauge",
            "Cache leases this node holds out as a group leader.",
        );
        for g in &sh.groups {
            let group = g.raft.group().to_string();
            fam.sample(
                name,
                &[("node", sh.id.as_str()), ("group", group.as_str())],
                g.leases.held(),
            );
        }
        let name = "atlas_native_client_sessions_expired_total";
        p.family(
            name,
            "counter",
            "Client sessions expired for not renewing their lease.",
        )
        .sample(name, &node, sh.sessions_expired.load(Ordering::Relaxed));
        parts.push(p.finish());
    }
    if let Ok(d) = sh.data.lock() {
        if let Some(m) = d.as_ref().and_then(|d| d.render_metrics().ok()) {
            parts.push(m);
        }
    }
    Response {
        status: 200,
        content_type: "text/plain; version=0.0.4",
        body: metrics::merge(&parts).into_bytes(),
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

    #[test]
    fn an_erasure_scheme_needs_a_data_node_per_shard() {
        let cfg = |erasure: &str| {
            let nodes: Vec<_> = (1..=5)
                .map(|i| serde_json::json!({ "id": format!("d{i}"), "addr": format!("d{i}:7000") }))
                .collect();
            serde_json::from_value::<super::NodeConfig>(serde_json::json!({
                "node_id": "m1",
                "data_dir": "/tmp/x",
                "http_listen": "127.0.0.1:0",
                "metadata": {
                    "listen": "127.0.0.1:0",
                    "peers": {},
                    "data_nodes": nodes,
                    "erasure": erasure,
                },
            }))
        };
        cfg("4+1").unwrap().validate().unwrap();
        let e = cfg("4+2").unwrap().validate().unwrap_err().to_string();
        assert!(e.contains("needs 6 data_nodes, 5 configured"), "{e}");
        assert!(cfg("4-2").is_err());
    }
}
