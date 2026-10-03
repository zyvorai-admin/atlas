// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Runs a [`RaftNode`] over TCP: a driver thread owns the clock and inbound queue, one sender
//! thread per peer keeps a connection open and drops messages while the peer is unreachable
//! (Raft tolerates loss), and the listener accepts peer connections.
//!
//! Wire format: a 4-byte big-endian length followed by a JSON [`Envelope`]. There is no
//! authentication or encryption; bind to a private metadata network only. Inbound envelopes are
//! dropped unless they come from a configured peer and are addressed to this node.

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

use crate::{
    metadata::{Catalog, MetaCommand},
    raft::{Envelope, NodeId, RaftConfig, RaftError, RaftNode, Role},
};

const MAX_FRAME: usize = 256 << 20;
const PEER_QUEUE: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStatus {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
}

struct Shared {
    node: Mutex<RaftNode>,
    changed: Condvar,
    stop: AtomicBool,
    fatal: Mutex<Option<String>>,
    outbound: BTreeMap<NodeId, SyncSender<Envelope>>,
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

    fn flush(&self, node: &mut RaftNode) {
        for env in node.take_messages() {
            if let Some(tx) = self.outbound.get(&env.to) {
                // A full queue means the peer is unreachable; Raft retransmits.
                let _ = tx.try_send(env);
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
    /// Starts serving `cfg` on an already-bound `listener`. `peers` maps every id in
    /// `cfg.peers` to its address.
    pub fn start(
        cfg: RaftConfig,
        listener: TcpListener,
        peers: BTreeMap<NodeId, SocketAddr>,
        tick: Duration,
    ) -> Result<Self, RaftError> {
        for p in &cfg.peers {
            if !peers.contains_key(p) {
                return Err(RaftError::Config(format!("no address for peer {p}")));
            }
        }
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let node = RaftNode::open(cfg.clone())?;

        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        let mut outbound = BTreeMap::new();
        for p in &cfg.peers {
            let (tx, rx) = mpsc::sync_channel(PEER_QUEUE);
            outbound.insert(p.clone(), tx);
            let peer_addr = peers[p];
            let stop = stop.clone();
            threads.push(thread::spawn(move || peer_sender(peer_addr, rx, &stop)));
        }

        let shared = Arc::new(Shared {
            node: Mutex::new(node),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
            fatal: Mutex::new(None),
            outbound,
            conns: Mutex::new(BTreeMap::new()),
            next_conn: AtomicU64::new(0),
            readers: Mutex::new(Vec::new()),
        });

        let (in_tx, in_rx) = mpsc::channel();
        {
            let shared = shared.clone();
            let peer_ids: Vec<NodeId> = cfg.peers.clone();
            let id = cfg.id.clone();
            threads.push(thread::spawn(move || {
                accept_loop(listener, &shared, in_tx, &id, &peer_ids)
            }));
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
        })
    }

    pub fn catalog(&self) -> Result<Catalog, RaftError> {
        Ok(self.shared.lock()?.catalog().clone())
    }

    /// The first fatal error (e.g. a failed fsync) that stopped this server, if any.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.fatal.lock().ok().and_then(|f| f.clone())
    }

    /// Proposes `command` and blocks until it is applied locally. Returns `LeadershipLost` if
    /// this node stops being leader (or changes term) first: the entry may or may not commit.
    pub fn propose(&self, command: MetaCommand, timeout: Duration) -> Result<u64, RaftError> {
        let deadline = Instant::now() + timeout;
        let mut node = self.shared.lock()?;
        let term = node.term();
        let index = node.propose(command)?;
        self.shared.flush(&mut node);
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
    }
}

impl Drop for RaftServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn drive(shared: &Shared, inbound: Receiver<Envelope>, tick: Duration) {
    let mut next_tick = Instant::now() + tick;
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
            result = result.and_then(|_| node.step(env));
        }
        if Instant::now() >= next_tick {
            result = result.and_then(|_| node.tick());
            next_tick += tick;
            if next_tick < Instant::now() {
                next_tick = Instant::now() + tick;
            }
        }
        shared.flush(&mut node);
        drop(node);
        if let Err(e) = result {
            shared.fail(e);
            return;
        }
        shared.changed.notify_all();
    }
}

fn accept_loop(
    listener: TcpListener,
    shared: &Arc<Shared>,
    inbound: Sender<Envelope>,
    id: &str,
    peers: &[NodeId],
) {
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
                let id = id.to_string();
                let peers = peers.to_vec();
                let reader_shared = shared.clone();
                let handle = thread::spawn(move || {
                    read_loop(stream, &inbound, &id, &peers);
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

fn read_loop(mut stream: TcpStream, inbound: &Sender<Envelope>, id: &str, peers: &[NodeId]) {
    while let Ok(env) = read_frame(&mut stream) {
        if env.to != id || !peers.contains(&env.from) {
            continue;
        }
        if inbound.send(env).is_err() {
            return;
        }
    }
}

fn peer_sender(addr: SocketAddr, rx: Receiver<Envelope>, stop: &AtomicBool) {
    let mut conn: Option<TcpStream> = None;
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
        if conn.is_none() && last_failure.is_none_or(|t| t.elapsed() >= RECONNECT_BACKOFF) {
            conn = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
                .and_then(|s| {
                    s.set_nodelay(true)?;
                    s.set_write_timeout(Some(WRITE_TIMEOUT))?;
                    Ok(s)
                })
                .ok();
            if conn.is_none() {
                last_failure = Some(Instant::now());
            }
        }
        if let Some(s) = conn.as_mut() {
            if write_frame(s, &env).is_err() {
                conn = None;
                last_failure = Some(Instant::now());
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
    w.write_all(&frame)
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

        let mut huge = ((MAX_FRAME + 1) as u32).to_be_bytes().to_vec();
        huge.extend_from_slice(b"{}");
        assert!(read_frame(&mut huge.as_slice()).is_err());
    }
}
