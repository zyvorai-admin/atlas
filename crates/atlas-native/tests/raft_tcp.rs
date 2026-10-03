// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    io::Read,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use atlas_native::{MetaCommand, RaftConfig, RaftError, RaftServer, Role};

const TICK: Duration = Duration::from_millis(10);
const WAIT: Duration = Duration::from_secs(15);

struct TcpCluster {
    _td: tempfile::TempDir,
    roots: BTreeMap<String, PathBuf>,
    addrs: BTreeMap<String, SocketAddr>,
    servers: BTreeMap<String, Option<RaftServer>>,
}

impl TcpCluster {
    fn new(n: usize) -> Self {
        let td = tempfile::tempdir().unwrap();
        let ids: Vec<String> = (1..=n).map(|i| format!("m{i}")).collect();
        let mut listeners = BTreeMap::new();
        let mut addrs = BTreeMap::new();
        for id in &ids {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            addrs.insert(id.clone(), l.local_addr().unwrap());
            listeners.insert(id.clone(), l);
        }
        let roots = ids
            .iter()
            .map(|id| (id.clone(), td.path().join(id)))
            .collect();
        let mut c = Self {
            _td: td,
            roots,
            addrs,
            servers: BTreeMap::new(),
        };
        for (id, l) in listeners {
            let s = c.start_with(&id, l);
            c.servers.insert(id, Some(s));
        }
        c
    }

    fn start_with(&self, id: &str, listener: TcpListener) -> RaftServer {
        let peers: BTreeMap<String, SocketAddr> = self
            .addrs
            .iter()
            .filter(|(p, _)| *p != id)
            .map(|(p, a)| (p.clone(), *a))
            .collect();
        let cfg = RaftConfig::new(id, peers.keys().cloned().collect(), self.roots[id].clone());
        RaftServer::start(cfg, listener, peers, TICK).unwrap()
    }

    fn stop(&mut self, id: &str) {
        if let Some(Some(mut s)) = self.servers.insert(id.to_string(), None) {
            s.shutdown();
        }
    }

    fn restart(&mut self, id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let listener = loop {
            match TcpListener::bind(self.addrs[id]) {
                Ok(l) => break l,
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("rebind {id}: {e}"),
            }
        };
        let s = self.start_with(id, listener);
        self.servers.insert(id.to_string(), Some(s));
    }

    fn live(&self) -> impl Iterator<Item = (&String, &RaftServer)> {
        self.servers
            .iter()
            .filter_map(|(id, s)| s.as_ref().map(|s| (id, s)))
    }

    fn wait_leader(&self) -> String {
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            let leaders: Vec<String> = self
                .live()
                .filter(|(_, s)| s.status().unwrap().role == Role::Leader)
                .map(|(id, _)| id.clone())
                .collect();
            if leaders.len() == 1 {
                return leaders[0].clone();
            }
            thread::sleep(TICK);
        }
        panic!("no single leader within {WAIT:?}");
    }

    fn propose_on_leader(&self, cmd: MetaCommand) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            let l = self.wait_leader();
            match self.servers[&l]
                .as_ref()
                .unwrap()
                .propose(cmd.clone(), Duration::from_secs(5))
            {
                Ok(idx) => return idx,
                Err(RaftError::NotLeader { .. } | RaftError::LeadershipLost { .. })
                    if Instant::now() < deadline =>
                {
                    thread::sleep(TICK)
                }
                Err(e) => panic!("propose failed: {e}"),
            }
        }
    }

    fn wait_converged(&self, names: &[&str]) {
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            let ok = self.live().all(|(_, s)| {
                let c = s.catalog().unwrap();
                names
                    .iter()
                    .all(|n| c.volumes.contains_key(&format!("vol-{n}")))
            });
            if ok {
                return;
            }
            thread::sleep(TICK);
        }
        panic!("replicas did not converge on {names:?}");
    }
}

fn create(name: &str) -> MetaCommand {
    MetaCommand::CreateVolume {
        id: format!("vol-{name}"),
        name: name.into(),
        size_bytes: 4096,
    }
}

#[test]
fn tcp_cluster_elects_and_replicates() {
    let c = TcpCluster::new(3);
    c.propose_on_leader(create("a"));
    c.propose_on_leader(create("b"));
    c.wait_converged(&["a", "b"]);

    // Healthy peers answer, so no connection is ever replaced as stale.
    thread::sleep(Duration::from_secs(3));
    for (id, s) in c.live() {
        let m = s.render_metrics().unwrap();
        for line in m
            .lines()
            .filter(|l| l.starts_with("atlas_native_transport_stale_reconnects_total{"))
        {
            assert!(line.ends_with(" 0"), "{id}: {line}");
        }
    }
}

#[test]
fn tcp_follower_rejects_with_leader_hint() {
    let c = TcpCluster::new(3);
    let l = c.wait_leader();
    c.propose_on_leader(create("warmup"));
    let (fid, f) = c.live().find(|(id, _)| **id != l).unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        match f.propose(create("x"), Duration::from_secs(1)) {
            Err(RaftError::NotLeader { leader: Some(hint) }) => {
                assert_eq!(hint, l, "{fid} pointed at the wrong leader");
                return;
            }
            Err(RaftError::NotLeader { leader: None }) if Instant::now() < deadline => {
                thread::sleep(TICK)
            }
            other => panic!("expected NotLeader from follower, got {other:?}"),
        }
    }
}

#[test]
fn tcp_leader_failover_and_rejoin() {
    let mut c = TcpCluster::new(3);
    c.propose_on_leader(create("a"));
    c.wait_converged(&["a"]);

    let old = c.wait_leader();
    c.stop(&old);
    let new = c.wait_leader();
    assert_ne!(old, new);
    c.propose_on_leader(create("b"));

    c.restart(&old);
    c.wait_converged(&["a", "b"]);
}

/// Accepts connections on `addr` and drains them without ever answering or closing: a peer that
/// vanished without resetting its sockets (deleted pod, powered-off host), from the sender's side.
/// Stops listening when `stop` is set; already-accepted sockets stay open until their peer
/// closes them.
fn zombie(addr: SocketAddr, stop: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let l = loop {
        match TcpListener::bind(addr) {
            Ok(l) => break l,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("bind zombie on {addr}: {e}"),
        }
    };
    l.set_nonblocking(true).unwrap();
    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match l.accept() {
                Ok((mut s, _)) => {
                    s.set_nonblocking(false).unwrap();
                    thread::spawn(move || {
                        let mut sink = [0u8; 8192];
                        while matches!(s.read(&mut sink), Ok(n) if n > 0) {}
                    });
                }
                Err(_) => thread::sleep(Duration::from_millis(5)),
            }
        }
    })
}

#[test]
fn tcp_sender_replaces_a_connection_to_a_silent_peer() {
    let mut c = TcpCluster::new(3);
    c.propose_on_leader(create("a"));
    c.wait_converged(&["a"]);
    let l = c.wait_leader();
    let gone = c
        .live()
        .map(|(id, _)| id.clone())
        .find(|id| *id != l)
        .unwrap();

    // The follower disappears and something that never answers takes its address; the others
    // reconnect to it and keep writing successfully.
    c.stop(&gone);
    let stop = Arc::new(AtomicBool::new(false));
    let z = zombie(c.addrs[&gone], stop.clone());
    c.propose_on_leader(create("b"));
    thread::sleep(Duration::from_millis(300));

    // The real node comes back on the same address: it only catches up if the others notice
    // their connections lead nowhere.
    stop.store(true, Ordering::SeqCst);
    z.join().unwrap();
    c.restart(&gone);
    c.wait_converged(&["a", "b"]);

    let l = c.wait_leader();
    let m = c.servers[&l].as_ref().unwrap().render_metrics().unwrap();
    let stale: u64 = m
        .lines()
        .filter(|line| line.starts_with("atlas_native_transport_stale_reconnects_total{"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum();
    assert!(stale > 0, "no stale reconnects recorded:\n{m}");
}

#[test]
fn tcp_metrics_expose_role_replication_and_transport_failures() {
    let mut c = TcpCluster::new(3);
    c.propose_on_leader(create("a"));
    c.wait_converged(&["a"]);
    let l = c.wait_leader();

    let m = c.servers[&l].as_ref().unwrap().render_metrics().unwrap();
    assert!(m.contains(&format!(
        "atlas_native_raft_role{{node=\"{l}\",role=\"leader\"}} 1"
    )));
    assert!(m.contains("# TYPE atlas_native_raft_peer_match_index gauge"));
    assert_eq!(m.matches("atlas_native_raft_peer_match_index{").count(), 2);
    assert!(m.contains("atlas_native_raft_leader_terms_total{node="));

    let (fid, f) = c.live().find(|(id, _)| **id != l).unwrap();
    let fm = f.render_metrics().unwrap();
    assert!(fm.contains(&format!(
        "atlas_native_raft_role{{node=\"{fid}\",role=\"follower\"}} 1"
    )));
    let fid = fid.clone();

    c.stop(&fid);
    let deadline = Instant::now() + WAIT;
    let leader = c.servers[&l].as_ref().unwrap();
    let pattern =
        format!("atlas_native_transport_connect_failures_total{{node=\"{l}\",peer=\"{fid}\"}} ");
    loop {
        let m = leader.render_metrics().unwrap();
        let failures: u64 = m
            .lines()
            .find_map(|line| line.strip_prefix(&pattern))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        if failures > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no connect failures recorded for {fid}:\n{m}"
        );
        thread::sleep(TICK);
    }
}
