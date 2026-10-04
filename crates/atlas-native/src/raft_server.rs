// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Runs a [`RaftNode`] over TCP: a driver thread owns the clock and inbound queue, one sender
//! thread per peer keeps a connection open and drops messages while the peer is unreachable
//! (Raft tolerates loss), and the listener accepts peer connections.
//!
//! Wire format: a 4-byte big-endian length followed by a JSON [`Envelope`]. Inbound envelopes are
//! dropped unless they come from a configured peer and are addressed to this node.
//!
//! Without TLS there is no authentication or encryption; bind to a private metadata network only.
//! With a [`TlsIdentity`] every connection is mutual TLS against the cluster CA, a peer is dialled
//! as its node id (which must be a DNS SAN on its certificate), and an inbound envelope's `from`
//! must be a name the sending connection's client certificate is valid for.

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender},
        Arc, Condvar, Mutex, MutexGuard,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rustls::{pki_types::ServerName, ClientConfig, ServerConfig};

use crate::{
    membership::Membership,
    metadata::{Catalog, MetaCommand},
    metrics::PromText,
    raft::{Envelope, Message, NodeId, RaftConfig, RaftError, RaftNode, Role},
    tls::{self, Conn, TlsIdentity},
};

const MAX_FRAME: usize = 256 << 20;
const PEER_QUEUE: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStatus {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    /// Whether this node is in the current voter set.
    pub voter: bool,
}

#[derive(Debug, Default)]
struct PeerStats {
    connect_failures: AtomicU64,
    write_failures: AtomicU64,
    /// Messages dropped because the peer's send queue was full.
    dropped: AtomicU64,
    sent: AtomicU64,
    /// Connections dropped as leading nowhere (see [`PeerLiveness`]).
    stale_reconnects: AtomicU64,
}

/// Detects outbound connections that lead nowhere. A peer that vanishes without closing its
/// sockets (a deleted pod, a host that lost power) leaves a connection whose writes keep
/// succeeding into the kernel buffer for many minutes, while its replacement at the same name
/// may already be talking to us over its own connection. A sender drops its connection when
///
/// - it is sending a request and the peer has not answered anything for `stale_after` (every
///   Raft request gets a response, even a rejection), or
/// - the peer reconnected to us (any inbound connection after its first) at least `stale_after`
///   after ours was established, i.e. it restarted (this also covers a node that only ever sends
///   responses to that peer). Its first connection is ignored: peers starting a few seconds
///   apart is not a restart.
///
/// The `stale_after` gap keeps two senders from endlessly resetting each other.
struct PeerLiveness {
    epoch: Instant,
    last_response_ms: AtomicU64,
    inbound_conns: AtomicU64,
    last_reconnect_ms: AtomicU64,
    stale_after: Duration,
}

impl PeerLiveness {
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn responded(&self) {
        self.last_response_ms
            .store(self.now_ms(), Ordering::Relaxed);
    }

    fn inbound_connected(&self) {
        if self.inbound_conns.fetch_add(1, Ordering::Relaxed) > 0 {
            self.last_reconnect_ms
                .store(self.now_ms(), Ordering::Relaxed);
        }
    }

    /// Whether a connection established at `connected_ms` should be dropped before sending `msg`.
    fn is_stale(&self, connected_ms: u64, msg: &Message) -> bool {
        let stale = self.stale_after.as_millis() as u64;
        let now = self.now_ms();
        let unanswered = !msg.is_response()
            && now.saturating_sub(connected_ms) >= stale
            && now.saturating_sub(self.last_response_ms.load(Ordering::Relaxed)) >= stale;
        let peer_reconnected =
            self.last_reconnect_ms.load(Ordering::Relaxed) >= connected_ms + stale;
        unanswered || peer_reconnected
    }
}

/// An outbound peer: its send queue (drained by a dedicated sender thread) and bookkeeping.
struct Peer {
    tx: SyncSender<Envelope>,
    stats: Arc<PeerStats>,
    live: Arc<PeerLiveness>,
    target: Arc<Mutex<String>>,
}

/// Everything needed to start a sender for a peer learned at runtime (a membership change).
struct PeerSpawner {
    tls_client: Option<Arc<ClientConfig>>,
    handshake_failures: Arc<AtomicU64>,
    epoch: Instant,
    stale_after: Duration,
    /// Set once the drive loop exits; senders stop on it.
    stop: Arc<AtomicBool>,
    senders: Mutex<Vec<JoinHandle<()>>>,
    /// Addresses from the local config; they win over addresses learned from the log.
    configured: BTreeMap<NodeId, String>,
}

struct Shared {
    id: NodeId,
    node: Mutex<RaftNode>,
    changed: Condvar,
    stop: AtomicBool,
    fatal: Mutex<Option<String>>,
    peers: Mutex<BTreeMap<NodeId, Peer>>,
    spawner: PeerSpawner,
    /// Inbound frames dropped for a wrong addressee, an unknown sender, or a sender the
    /// connection's certificate does not vouch for.
    rejected_frames: AtomicU64,
    tls_server: Option<Arc<ServerConfig>>,
    tls_handshake_failures: Arc<AtomicU64>,
    /// Accepted connections, kept so shutdown can unblock their readers; removed on reader exit.
    conns: Mutex<BTreeMap<u64, TcpStream>>,
    next_conn: AtomicU64,
    readers: Mutex<Vec<JoinHandle<()>>>,
}

impl Shared {
    fn lock(&self) -> Result<MutexGuard<'_, RaftNode>, RaftError> {
        if self.stop.load(Ordering::SeqCst) {
            return Err(RaftError::Shutdown);
        }
        self.node.lock().map_err(|_| RaftError::Shutdown)
    }

    fn flush(&self, node: &mut RaftNode) -> Result<(), RaftError> {
        let msgs = node.take_messages()?;
        if msgs.is_empty() {
            return Ok(());
        }
        let Ok(peers) = self.peers.lock() else {
            return Ok(());
        };
        for env in msgs {
            if let Some(p) = peers.get(&env.to) {
                // A full queue means the peer is unreachable; Raft retransmits.
                if p.tx.try_send(env).is_err() {
                    p.stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    fn is_known_peer(&self, id: &str) -> bool {
        self.peers.lock().is_ok_and(|p| p.contains_key(id))
    }

    fn with_liveness(&self, id: &str, f: impl FnOnce(&PeerLiveness)) {
        if let Some(live) = self
            .peers
            .lock()
            .ok()
            .and_then(|p| p.get(id).map(|p| p.live.clone()))
        {
            f(&live);
        }
    }

    /// Starts a sender for `id`, or points the existing one at `addr`.
    fn ensure_peer(&self, id: &NodeId, addr: &str) -> Result<(), RaftError> {
        let mut peers = self.peers.lock().map_err(|_| RaftError::Shutdown)?;
        if let Some(p) = peers.get(id) {
            if let Ok(mut t) = p.target.lock() {
                if *t != addr {
                    *t = addr.to_string();
                }
            }
            return Ok(());
        }
        let sp = &self.spawner;
        let tls = match &sp.tls_client {
            Some(c) => Some((
                c.clone(),
                tls::server_name(id).map_err(|e| RaftError::Config(e.to_string()))?,
            )),
            None => None,
        };
        let (tx, rx) = mpsc::sync_channel(PEER_QUEUE);
        let stats = Arc::new(PeerStats::default());
        let live = Arc::new(PeerLiveness {
            epoch: sp.epoch,
            last_response_ms: AtomicU64::new(0),
            inbound_conns: AtomicU64::new(0),
            last_reconnect_ms: AtomicU64::new(0),
            stale_after: sp.stale_after,
        });
        let target = Arc::new(Mutex::new(addr.to_string()));
        let pt = PeerTarget {
            target: target.clone(),
            tls,
            handshake_failures: sp.handshake_failures.clone(),
        };
        let (stop, st, lv) = (sp.stop.clone(), stats.clone(), live.clone());
        let h = thread::spawn(move || peer_sender(&pt, rx, &stop, &st, &lv));
        if let Ok(mut senders) = sp.senders.lock() {
            senders.push(h);
        }
        peers.insert(
            id.clone(),
            Peer {
                tx,
                stats,
                live,
                target,
            },
        );
        Ok(())
    }

    /// Makes sure every voter (and every configured peer) has a sender with a current address.
    fn sync_peers(&self, node: &RaftNode) {
        let mut addrs = node.peer_addrs();
        addrs.extend(
            self.spawner
                .configured
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        for (id, addr) in addrs {
            if id != self.id {
                let _ = self.ensure_peer(&id, &addr);
            }
        }
    }

    fn fail(&self, err: RaftError) {
        if let Ok(mut f) = self.fatal.lock() {
            f.get_or_insert_with(|| err.to_string());
        }
        self.stop.store(true, Ordering::SeqCst);
        self.changed.notify_all();
    }
}

pub struct RaftServer {
    shared: Arc<Shared>,
    addr: SocketAddr,
    threads: Vec<JoinHandle<()>>,
}

impl RaftServer {
    /// Starts serving `cfg` on an already-bound `listener` without TLS. `peers` maps every id
    /// in `cfg.peers` to its address: a socket address or a `host:port` re-resolved on every
    /// connection attempt.
    pub fn start<A: std::fmt::Display>(
        cfg: RaftConfig,
        listener: TcpListener,
        peers: BTreeMap<NodeId, A>,
        tick: Duration,
    ) -> Result<Self, RaftError> {
        Self::start_with(cfg, listener, peers, tick, None)
    }

    /// Like [`Self::start`], with mutual TLS when `tls` is set. Node ids must then be DNS names.
    pub fn start_with<A: std::fmt::Display>(
        cfg: RaftConfig,
        listener: TcpListener,
        peers: BTreeMap<NodeId, A>,
        tick: Duration,
        tls: Option<TlsIdentity>,
    ) -> Result<Self, RaftError> {
        for p in &cfg.peers {
            if !peers.contains_key(p) {
                return Err(RaftError::Config(format!("no address for peer {p}")));
            }
        }
        let (tls_server, tls_client) = match &tls {
            Some(id) => (Some(id.server_config()?), Some(id.client_config()?)),
            None => (None, None),
        };
        if tls.is_some() {
            for p in cfg.peers.iter().chain([&cfg.id]) {
                tls::server_name(p).map_err(|e| RaftError::Config(e.to_string()))?;
            }
        }
        let tls_handshake_failures = Arc::new(AtomicU64::new(0));
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let node = RaftNode::open(cfg.clone())?;

        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        let shared = Arc::new(Shared {
            id: cfg.id.clone(),
            node: Mutex::new(node),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
            fatal: Mutex::new(None),
            peers: Mutex::new(BTreeMap::new()),
            spawner: PeerSpawner {
                tls_client,
                handshake_failures: tls_handshake_failures.clone(),
                epoch: Instant::now(),
                stale_after: (tick * 40).max(Duration::from_secs(1)),
                stop: stop.clone(),
                senders: Mutex::new(Vec::new()),
                configured: peers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect(),
            },
            rejected_frames: AtomicU64::new(0),
            tls_server,
            tls_handshake_failures,
            conns: Mutex::new(BTreeMap::new()),
            next_conn: AtomicU64::new(0),
            readers: Mutex::new(Vec::new()),
        });
        {
            let node = shared.lock()?;
            shared.sync_peers(&node);
        }

        let (in_tx, in_rx) = mpsc::channel();
        {
            let shared = shared.clone();
            threads.push(thread::spawn(move || accept_loop(listener, &shared, in_tx)));
        }
        {
            let shared = shared.clone();
            let stop = stop.clone();
            threads.push(thread::spawn(move || {
                drive(&shared, in_rx, tick);
                stop.store(true, Ordering::SeqCst);
            }));
        }
        Ok(Self {
            shared,
            addr,
            threads,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn status(&self) -> Result<RaftStatus, RaftError> {
        let n = self.shared.lock()?;
        Ok(RaftStatus {
            id: n.id().to_string(),
            role: n.role(),
            term: n.term(),
            leader: n.leader().map(str::to_string),
            commit_index: n.commit_index(),
            applied_index: n.applied_index(),
            voter: n.is_voter(),
        })
    }

    /// The current voter configuration and every known peer transport address.
    pub fn membership(&self) -> Result<(Membership, BTreeMap<NodeId, String>), RaftError> {
        let n = self.shared.lock()?;
        let mut addrs = n.peer_addrs();
        addrs.extend(
            self.shared
                .spawner
                .configured
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        Ok((n.membership().clone(), addrs))
    }

    /// Moves the voter set to the keys of `voters` (values: their Raft `host:port`; this node's
    /// own entry is ignored) through joint consensus, and blocks until the final configuration
    /// has applied here. A leader that removes itself steps down once that happens.
    pub fn change_membership(
        &self,
        voters: BTreeMap<NodeId, String>,
        timeout: Duration,
    ) -> Result<Membership, RaftError> {
        let deadline = Instant::now() + timeout;
        let term = self.leader_ready(timeout)?;
        let mut node = self.shared.lock()?;
        if node.term() != term {
            return Err(RaftError::NotLeader {
                leader: node.leader().map(str::to_string),
            });
        }
        let target: std::collections::BTreeSet<NodeId> = voters.keys().cloned().collect();
        let addrs = voters
            .into_iter()
            .filter(|(id, _)| id != node.id())
            .collect();
        let index = node.change_membership(target.clone(), addrs)?;
        self.shared.sync_peers(&node);
        self.shared.flush(&mut node)?;
        loop {
            if self.shared.stop.load(Ordering::SeqCst) {
                return Err(RaftError::Shutdown);
            }
            let done = node.commit_index() >= index.max(node.latest_config_index())
                && matches!(node.membership(), Membership::Stable { voters } if *voters == target);
            if done {
                return Ok(node.membership().clone());
            }
            if !node.is_leader() || node.term() != term {
                return Err(RaftError::LeadershipLost { index });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RaftError::Timeout { index });
            }
            node = self
                .shared
                .changed
                .wait_timeout(node, deadline - now)
                .map_err(|_| RaftError::Shutdown)?
                .0;
        }
    }

    pub fn catalog(&self) -> Result<Catalog, RaftError> {
        Ok(self.shared.lock()?.catalog().clone())
    }

    /// Runs `f` against the applied catalog without cloning it. `f` holds the node lock, so it
    /// must not block.
    pub fn with_catalog<R>(&self, f: impl FnOnce(&Catalog) -> R) -> Result<R, RaftError> {
        Ok(f(self.shared.lock()?.catalog()))
    }

    /// Log entries retained above the last compaction point.
    pub fn log_records(&self) -> Result<u64, RaftError> {
        let n = self.shared.lock()?;
        Ok(n.last_index() - n.snapshot_index())
    }

    /// Blocks until this node is leader and has applied its log as of the call, including the
    /// no-op that commits earlier terms' entries. Returns the term, which callers use as a data write fence
    /// and pass to [`Self::propose_in_term`].
    pub fn leader_ready(&self, timeout: Duration) -> Result<u64, RaftError> {
        let deadline = Instant::now() + timeout;
        let mut node = self.shared.lock()?;
        // The log as of this call: entries proposed meanwhile must not hold the caller back, or
        // a steady stream of writes starves every reader.
        let target = node.last_index();
        let term = node.term();
        loop {
            if self.shared.stop.load(Ordering::SeqCst) {
                return Err(RaftError::Shutdown);
            }
            if !node.is_leader() || node.term() != term {
                return Err(RaftError::NotLeader {
                    leader: node.leader().map(str::to_string),
                });
            }
            if node.applied_index() >= target {
                return Ok(term);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RaftError::Timeout { index: target });
            }
            node = self
                .shared
                .changed
                .wait_timeout(node, deadline - now)
                .map_err(|_| RaftError::Shutdown)?
                .0;
        }
    }

    /// Like [`Self::propose`], but refuses to propose unless the node is still leader in `term`.
    pub fn propose_in_term(
        &self,
        command: MetaCommand,
        term: u64,
        timeout: Duration,
    ) -> Result<u64, RaftError> {
        self.propose_inner(command, Some(term), timeout)
    }

    /// Prometheus text exposition for this node's Raft state and transport.
    pub fn render_metrics(&self) -> Result<String, RaftError> {
        let n = self.shared.lock()?;
        let id = n.id().to_string();
        let node = [("node", id.as_str())];
        let mut p = PromText::new();
        p.family("atlas_native_raft_term", "gauge", "Current Raft term.")
            .sample("atlas_native_raft_term", &node, n.term());
        p.family(
            "atlas_native_raft_commit_index",
            "gauge",
            "Highest committed log index.",
        )
        .sample("atlas_native_raft_commit_index", &node, n.commit_index());
        p.family(
            "atlas_native_raft_applied_index",
            "gauge",
            "Highest log index applied to the catalog.",
        )
        .sample("atlas_native_raft_applied_index", &node, n.applied_index());
        p.family(
            "atlas_native_raft_last_index",
            "gauge",
            "Last log index (including uncommitted).",
        )
        .sample("atlas_native_raft_last_index", &node, n.last_index());
        p.family(
            "atlas_native_raft_snapshot_index",
            "gauge",
            "Log compaction point.",
        )
        .sample(
            "atlas_native_raft_snapshot_index",
            &node,
            n.snapshot_index(),
        );
        p.family(
            "atlas_native_raft_role",
            "gauge",
            "1 for the node's current role, 0 otherwise.",
        );
        for (role, name) in [
            (Role::Follower, "follower"),
            (Role::PreCandidate, "pre_candidate"),
            (Role::Candidate, "candidate"),
            (Role::Leader, "leader"),
        ] {
            p.sample(
                "atlas_native_raft_role",
                &[("node", id.as_str()), ("role", name)],
                u8::from(n.role() == role),
            );
        }
        let c = n.counters();
        p.family(
            "atlas_native_raft_elections_total",
            "counter",
            "Elections started after a successful pre-vote.",
        )
        .sample("atlas_native_raft_elections_total", &node, c.elections);
        p.family(
            "atlas_native_raft_leader_terms_total",
            "counter",
            "Times this node became leader.",
        )
        .sample(
            "atlas_native_raft_leader_terms_total",
            &node,
            c.leader_terms,
        );
        p.family(
            "atlas_native_raft_append_rejections_total",
            "counter",
            "AppendEntries rejections received while leader.",
        )
        .sample(
            "atlas_native_raft_append_rejections_total",
            &node,
            c.append_rejections,
        );
        p.family(
            "atlas_native_raft_peer_match_index",
            "gauge",
            "Highest index replicated on each peer (leader only).",
        );
        for (peer, m) in n.peer_match_index() {
            p.sample(
                "atlas_native_raft_peer_match_index",
                &[("node", id.as_str()), ("peer", peer.as_str())],
                m,
            );
        }
        drop(n);
        let stats: Vec<(NodeId, Arc<PeerStats>)> = self
            .shared
            .peers
            .lock()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.clone(), v.stats.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for (name, help, get) in [
            (
                "atlas_native_transport_sent_total",
                "Raft frames written to each peer.",
                (|s: &PeerStats| s.sent.load(Ordering::Relaxed)) as fn(&PeerStats) -> u64,
            ),
            (
                "atlas_native_transport_connect_failures_total",
                "Failed connection attempts to each peer.",
                |s| s.connect_failures.load(Ordering::Relaxed),
            ),
            (
                "atlas_native_transport_write_failures_total",
                "Frame writes that failed and dropped the connection.",
                |s| s.write_failures.load(Ordering::Relaxed),
            ),
            (
                "atlas_native_transport_stale_reconnects_total",
                "Connections re-established because the peer had stopped answering.",
                |s| s.stale_reconnects.load(Ordering::Relaxed),
            ),
            (
                "atlas_native_transport_dropped_total",
                "Messages dropped while the peer was unreachable or its queue was full.",
                |s| s.dropped.load(Ordering::Relaxed),
            ),
        ] {
            p.family(name, "counter", help);
            for (peer, st) in &stats {
                p.sample(
                    name,
                    &[("node", id.as_str()), ("peer", peer.as_str())],
                    get(st),
                );
            }
        }
        p.family(
            "atlas_native_transport_rejected_frames_total",
            "counter",
            "Inbound frames from unknown senders or for another node.",
        )
        .sample(
            "atlas_native_transport_rejected_frames_total",
            &node,
            self.shared.rejected_frames.load(Ordering::Relaxed),
        );
        p.family(
            "atlas_native_transport_tls_handshake_failures_total",
            "counter",
            "Inbound and outbound TLS handshakes that failed.",
        )
        .sample(
            "atlas_native_transport_tls_handshake_failures_total",
            &node,
            self.shared.tls_handshake_failures.load(Ordering::Relaxed),
        );
        Ok(p.finish())
    }

    /// The first fatal error (e.g. a failed fsync) that stopped this server, if any.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.fatal.lock().ok().and_then(|f| f.clone())
    }

    /// Proposes `command` and blocks until it is applied locally. Returns `LeadershipLost` if
    /// this node stops being leader (or changes term) first: the entry may or may not commit.
    pub fn propose(&self, command: MetaCommand, timeout: Duration) -> Result<u64, RaftError> {
        self.propose_inner(command, None, timeout)
    }

    fn propose_inner(
        &self,
        command: MetaCommand,
        expected_term: Option<u64>,
        timeout: Duration,
    ) -> Result<u64, RaftError> {
        let deadline = Instant::now() + timeout;
        let mut node = self.shared.lock()?;
        let term = node.term();
        if let Some(expected) = expected_term.filter(|t| *t != term) {
            return Err(RaftError::TermChanged {
                expected,
                current: term,
            });
        }
        let index = node.propose(command)?;
        self.shared.flush(&mut node)?;
        loop {
            if self.shared.stop.load(Ordering::SeqCst) {
                return Err(RaftError::Shutdown);
            }
            if !node.is_leader() || node.term() != term {
                return Err(RaftError::LeadershipLost { index });
            }
            if node.applied_index() >= index {
                return Ok(index);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RaftError::Timeout { index });
            }
            node = self
                .shared
                .changed
                .wait_timeout(node, deadline - now)
                .map_err(|_| RaftError::Shutdown)?
                .0;
        }
    }

    pub fn shutdown(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.shared.changed.notify_all();
        // Join the acceptor first so no connection can be registered after the sweep below.
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        if let Ok(conns) = self.shared.conns.lock() {
            for c in conns.values() {
                let _ = c.shutdown(std::net::Shutdown::Both);
            }
        }
        let readers: Vec<_> = self
            .shared
            .readers
            .lock()
            .map(|mut r| r.drain(..).collect())
            .unwrap_or_default();
        for r in readers {
            let _ = r.join();
        }
        let senders: Vec<_> = self
            .shared
            .spawner
            .senders
            .lock()
            .map(|mut s| s.drain(..).collect())
            .unwrap_or_default();
        for s in senders {
            let _ = s.join();
        }
    }
}

impl Drop for RaftServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn drive(shared: &Shared, inbound: Receiver<Envelope>, tick: Duration) {
    let mut next_tick = Instant::now() + tick;
    let mut config_seen = None;
    while !shared.stop.load(Ordering::SeqCst) {
        let wait = next_tick.saturating_duration_since(Instant::now());
        let first = match inbound.recv_timeout(wait) {
            Ok(env) => Some(env),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        let Ok(mut node) = shared.node.lock() else {
            return;
        };
        let mut result = Ok(());
        for env in first.into_iter().chain(inbound.try_iter()) {
            if env.msg.is_response() {
                shared.with_liveness(&env.from, PeerLiveness::responded);
            }
            result = result.and_then(|_| node.step(env));
        }
        if Instant::now() >= next_tick {
            result = result.and_then(|_| node.tick());
            next_tick += tick;
            if next_tick < Instant::now() {
                next_tick = Instant::now() + tick;
            }
        }
        let key = (
            node.latest_config_index(),
            node.membership().clone(),
            node.catalog().raft_addrs.len(),
        );
        if config_seen.as_ref() != Some(&key) {
            shared.sync_peers(&node);
            config_seen = Some(key);
        }
        let result = result.and_then(|_| shared.flush(&mut node));
        drop(node);
        if let Err(e) = result {
            shared.fail(e);
            return;
        }
        shared.changed.notify_all();
    }
}

fn accept_loop(listener: TcpListener, shared: &Arc<Shared>, inbound: Sender<Envelope>) {
    while !shared.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                let _ = stream.set_nodelay(true);
                let Ok(clone) = stream.try_clone() else {
                    continue;
                };
                let conn_id = shared.next_conn.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut conns) = shared.conns.lock() {
                    conns.insert(conn_id, clone);
                }
                let inbound = inbound.clone();
                let reader_shared = shared.clone();
                let handle = thread::spawn(move || {
                    match Conn::accept(stream, reader_shared.tls_server.as_ref(), HANDSHAKE_TIMEOUT)
                    {
                        Ok(conn) => read_loop(conn, &inbound, &reader_shared),
                        Err(_) => {
                            reader_shared
                                .tls_handshake_failures
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if let Ok(mut conns) = reader_shared.conns.lock() {
                        conns.remove(&conn_id);
                    }
                });
                if let Ok(mut r) = shared.readers.lock() {
                    r.retain(|h| !h.is_finished());
                    r.push(handle);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Delivers frames addressed to this node from known peers. Over TLS a sender may only speak
/// for node ids its certificate is valid for (checked once per id per connection).
fn read_loop(mut stream: Conn, inbound: &Sender<Envelope>, shared: &Shared) {
    let mut vouched: BTreeMap<NodeId, bool> = BTreeMap::new();
    while let Ok(env) = read_frame(&mut stream) {
        let ok = env.to == shared.id
            && shared.is_known_peer(&env.from)
            && *vouched.entry(env.from.clone()).or_insert_with(|| {
                let ok = !stream.is_tls()
                    || !stream
                        .peer_names(std::slice::from_ref(&env.from))
                        .is_empty();
                if ok {
                    shared.with_liveness(&env.from, PeerLiveness::inbound_connected);
                }
                ok
            });
        if !ok {
            shared.rejected_frames.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if inbound.send(env).is_err() {
            return;
        }
    }
}

struct PeerTarget {
    target: Arc<Mutex<String>>,
    tls: Option<(Arc<ClientConfig>, ServerName<'static>)>,
    handshake_failures: Arc<AtomicU64>,
}

impl PeerTarget {
    fn connect(&self) -> io::Result<Conn> {
        let target = self
            .target
            .lock()
            .map_err(|_| io::Error::other("peer target lock poisoned"))?
            .clone();
        let s = TcpStream::connect_timeout(&tls::resolve(&target)?, CONNECT_TIMEOUT)?;
        s.set_nodelay(true)?;
        s.set_write_timeout(Some(WRITE_TIMEOUT))?;
        s.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
        let conn = Conn::connect(s, self.tls.as_ref().map(|(c, n)| (c, n.clone())));
        if conn.is_err() && self.tls.is_some() {
            self.handshake_failures.fetch_add(1, Ordering::Relaxed);
        }
        conn
    }
}

fn peer_sender(
    target: &PeerTarget,
    rx: Receiver<Envelope>,
    stop: &AtomicBool,
    stats: &PeerStats,
    live: &PeerLiveness,
) {
    let mut conn: Option<Conn> = None;
    let mut connected_ms = 0;
    let mut last_failure: Option<Instant> = None;
    loop {
        let env = match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(env) => env,
            Err(RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if stop.load(Ordering::SeqCst) {
            return;
        }
        if conn.is_some() && live.is_stale(connected_ms, &env.msg) {
            stats.stale_reconnects.fetch_add(1, Ordering::Relaxed);
            conn = None;
        }
        if conn.is_none() && last_failure.is_none_or(|t| t.elapsed() >= RECONNECT_BACKOFF) {
            conn = target.connect().ok();
            if conn.is_none() {
                stats.connect_failures.fetch_add(1, Ordering::Relaxed);
                last_failure = Some(Instant::now());
            } else {
                connected_ms = live.now_ms();
            }
        }
        match conn.as_mut() {
            Some(s) => {
                if write_frame(s, &env).is_ok() {
                    stats.sent.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.write_failures.fetch_add(1, Ordering::Relaxed);
                    conn = None;
                    last_failure = Some(Instant::now());
                }
            }
            None => {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn write_frame(w: &mut impl Write, env: &Envelope) -> io::Result<()> {
    let body = serde_json::to_vec(env).map_err(io::Error::other)?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raft frame too large",
        ));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    w.write_all(&frame)?;
    w.flush()
}

fn read_frame(r: &mut impl Read) -> io::Result<Envelope> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Message;

    #[test]
    fn frame_round_trip_and_size_guard() {
        let env = Envelope {
            from: "a".into(),
            to: "b".into(),
            msg: Message::RequestVoteResponse {
                term: 3,
                granted: true,
            },
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &env).unwrap();
        let back = read_frame(&mut buf.as_slice()).unwrap();
        assert_eq!(back.from, "a");
        assert_eq!(back.msg.term(), 3);

        let mut catalog = crate::metadata::Catalog::default();
        catalog.volumes.insert(
            "v".into(),
            crate::metadata::VolumeMeta {
                id: "v".into(),
                name: "v".into(),
                size_bytes: 8192,
                extents: [(4096, "e1".to_string())].into(),
            },
        );
        let snap = Envelope {
            from: "a".into(),
            to: "b".into(),
            msg: Message::InstallSnapshot {
                term: 2,
                snapshot: Box::new(catalog),
            },
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &snap).unwrap();
        let Message::InstallSnapshot { snapshot, .. } =
            read_frame(&mut buf.as_slice()).unwrap().msg
        else {
            panic!("not a snapshot");
        };
        assert_eq!(snapshot.volumes["v"].extents[&4096], "e1");

        let mut huge = ((MAX_FRAME + 1) as u32).to_be_bytes().to_vec();
        huge.extend_from_slice(b"{}");
        assert!(read_frame(&mut huge.as_slice()).is_err());
    }
}
