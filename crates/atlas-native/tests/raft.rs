// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use atlas_native::{MetaCommand, RaftConfig, RaftError, RaftNode, Role};

struct Cluster {
    _td: tempfile::TempDir,
    cfgs: BTreeMap<String, RaftConfig>,
    nodes: BTreeMap<String, Option<RaftNode>>,
    isolated: BTreeSet<String>,
}

impl Cluster {
    fn new(n: usize, compact_after: u64) -> Self {
        let td = tempfile::tempdir().unwrap();
        let ids: Vec<String> = (1..=n).map(|i| format!("m{i}")).collect();
        let mut cfgs = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        for id in &ids {
            let peers = ids.iter().filter(|p| *p != id).cloned().collect();
            let mut cfg = RaftConfig::new(id.clone(), peers, td.path().join(id));
            cfg.compact_after = compact_after;
            nodes.insert(id.clone(), Some(RaftNode::open(cfg.clone()).unwrap()));
            cfgs.insert(id.clone(), cfg);
        }
        Self {
            _td: td,
            cfgs,
            nodes,
            isolated: BTreeSet::new(),
        }
    }

    fn node(&self, id: &str) -> &RaftNode {
        self.nodes[id].as_ref().expect("node is down")
    }

    fn node_mut(&mut self, id: &str) -> &mut RaftNode {
        self.nodes
            .get_mut(id)
            .unwrap()
            .as_mut()
            .expect("node is down")
    }

    fn crash(&mut self, id: &str) {
        self.nodes.insert(id.to_string(), None);
    }

    fn restart(&mut self, id: &str) {
        let n = RaftNode::open(self.cfgs[id].clone()).unwrap();
        self.nodes.insert(id.to_string(), Some(n));
    }

    fn tick(&mut self) {
        for n in self.nodes.values_mut().flatten() {
            n.tick().unwrap();
        }
        self.deliver();
    }

    fn deliver(&mut self) {
        for _ in 0..10_000 {
            let mut batch = Vec::new();
            for n in self.nodes.values_mut().flatten() {
                batch.extend(n.take_messages());
            }
            if batch.is_empty() {
                return;
            }
            for env in batch {
                if self.isolated.contains(&env.from) || self.isolated.contains(&env.to) {
                    continue;
                }
                if let Some(Some(n)) = self.nodes.get_mut(&env.to) {
                    n.step(env).unwrap();
                }
            }
        }
        panic!("message storm: cluster never went quiet");
    }

    fn run(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    fn run_until(&mut self, max_ticks: usize, pred: impl Fn(&Self) -> bool) -> bool {
        for _ in 0..max_ticks {
            if pred(self) {
                return true;
            }
            self.tick();
        }
        pred(self)
    }

    /// The leader among live, connected nodes (asserting there is at most one per term).
    fn leader(&self) -> Option<String> {
        let leaders: Vec<&RaftNode> = self
            .nodes
            .values()
            .flatten()
            .filter(|n| n.is_leader() && !self.isolated.contains(n.id()))
            .collect();
        let terms: BTreeSet<u64> = leaders.iter().map(|n| n.term()).collect();
        assert_eq!(terms.len(), leaders.len(), "two leaders in one term");
        leaders
            .iter()
            .max_by_key(|n| n.term())
            .map(|n| n.id().to_string())
    }

    fn elect(&mut self) -> String {
        assert!(
            self.run_until(500, |c| c.leader().is_some()),
            "no leader elected"
        );
        self.leader().unwrap()
    }

    fn volumes(&self, id: &str) -> BTreeSet<String> {
        self.node(id)
            .catalog()
            .volumes
            .values()
            .map(|v| v.name.clone())
            .collect()
    }

    fn live_ids(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.is_some())
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn converged(&self, names: &[&str]) -> bool {
        let want: BTreeSet<String> = names.iter().map(|s| s.to_string()).collect();
        self.live_ids().iter().all(|id| self.volumes(id) == want)
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
fn single_node_commits_immediately() {
    let td = tempfile::tempdir().unwrap();
    let mut n = RaftNode::open(RaftConfig::new("solo", vec![], td.path())).unwrap();
    for _ in 0..50 {
        n.tick().unwrap();
    }
    assert!(n.is_leader());
    let idx = n.propose(create("a")).unwrap();
    assert_eq!(n.applied_index(), idx);
    assert!(n.catalog().volumes.contains_key("vol-a"));
}

#[test]
fn elects_one_leader_and_replicates() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    c.node_mut(&l).propose(create("a")).unwrap();
    c.node_mut(&l).propose(create("b")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a", "b"])));
    let applied: BTreeSet<u64> = c
        .live_ids()
        .iter()
        .map(|id| c.node(id).applied_index())
        .collect();
    assert_eq!(applied.len(), 1, "replicas applied different prefixes");
}

#[test]
fn followers_reject_proposals_with_leader_hint() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    c.run(10);
    let f = c.live_ids().into_iter().find(|id| *id != l).unwrap();
    match c.node_mut(&f).propose(create("x")) {
        Err(RaftError::NotLeader { leader }) => assert_eq!(leader.as_deref(), Some(l.as_str())),
        other => panic!("expected NotLeader, got {other:?}"),
    }
}

#[test]
fn invalid_command_is_rejected_before_logging() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    let before = c.node(&l).last_index();
    let err = c
        .node_mut(&l)
        .propose(MetaCommand::DeleteVolume {
            volume_id: "missing".into(),
        })
        .unwrap_err();
    assert!(matches!(err, RaftError::Rejected(_)));
    assert_eq!(c.node(&l).last_index(), before);
}

#[test]
fn leader_crash_preserves_committed_entries() {
    let mut c = Cluster::new(3, 0);
    let l1 = c.elect();
    c.node_mut(&l1).propose(create("a")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a"])));

    c.crash(&l1);
    let l2 = c.elect();
    assert_ne!(l1, l2);
    c.node_mut(&l2).propose(create("b")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a", "b"])));

    c.restart(&l1);
    assert!(c.run_until(100, |c| c.converged(&["a", "b"])));
    assert_eq!(c.node(&l1).role(), Role::Follower);
}

#[test]
fn minority_leader_cannot_commit_and_its_entries_are_discarded() {
    let mut c = Cluster::new(3, 0);
    let old = c.elect();
    c.node_mut(&old).propose(create("a")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a"])));

    c.isolated.insert(old.clone());
    let commit_before = c.node(&old).commit_index();
    c.node_mut(&old).propose(create("lost")).unwrap();
    c.run(100);
    assert_eq!(c.node(&old).commit_index(), commit_before);
    assert!(!c.volumes(&old).contains("lost"));

    let new = c.leader().expect("majority elected a new leader");
    assert_ne!(new, old);
    c.node_mut(&new).propose(create("kept")).unwrap();
    c.run(10);

    c.isolated.clear();
    assert!(c.run_until(200, |c| c.converged(&["a", "kept"])));
    assert!(!c.node(&old).is_leader());
}

#[test]
fn full_cluster_restart_recovers_from_disk() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    c.node_mut(&l).propose(create("a")).unwrap();
    c.node_mut(&l).propose(create("b")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a", "b"])));
    let term = c.node(&l).term();

    for id in c.live_ids() {
        c.crash(&id);
    }
    for id in c.cfgs.keys().cloned().collect::<Vec<_>>() {
        c.restart(&id);
    }
    assert!(c.converged(&["a", "b"]));
    let l = c.elect();
    assert!(c.node(&l).term() > term);
    c.node_mut(&l).propose(create("c")).unwrap();
    assert!(c.run_until(50, |c| c.converged(&["a", "b", "c"])));
}

#[test]
fn rejoining_node_does_not_disrupt_stable_leader() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    c.run(10);
    let term = c.node(&l).term();
    let f = c.live_ids().into_iter().find(|id| *id != l).unwrap();

    c.isolated.insert(f.clone());
    c.run(300);
    assert_eq!(
        c.node(&f).term(),
        term,
        "pre-vote must stop an isolated node from inflating its term"
    );
    assert_eq!(c.node(&l).role(), Role::Leader);

    c.isolated.clear();
    c.run(50);
    assert_eq!(c.leader().as_deref(), Some(l.as_str()));
    assert_eq!(c.node(&l).term(), term);
}

#[test]
fn isolated_leader_steps_down() {
    let mut c = Cluster::new(3, 0);
    let l = c.elect();
    c.isolated.insert(l.clone());
    c.run(50);
    assert!(
        !c.node(&l).is_leader(),
        "check-quorum must demote a leader cut off from the majority"
    );
}

#[test]
fn randomized_faults_never_lose_acknowledged_commits() {
    for seed in [7u64, 42, 1337] {
        run_fault_schedule(seed);
    }
}

fn run_fault_schedule(seed: u64) {
    let mut rng = seed;
    let mut next = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut c = Cluster::new(3, 8);
    let ids: Vec<String> = c.cfgs.keys().cloned().collect();
    let mut pending: Vec<(String, u64, u64, String)> = Vec::new();
    let mut acked: BTreeSet<String> = BTreeSet::new();
    let mut proposed = 0usize;

    for _ in 0..2000 {
        match next(100) {
            0..=1 => {
                c.isolated.insert(ids[next(3) as usize].clone());
            }
            2..=3 => c.isolated.clear(),
            4 => {
                let id = &ids[next(3) as usize];
                if c.live_ids().len() == 3 {
                    c.crash(id);
                }
            }
            5..=7 => {
                for id in &ids {
                    if c.nodes[id].is_none() {
                        c.restart(id);
                    }
                }
            }
            _ => {}
        }
        if next(3) == 0 {
            if let Some(l) = c.leader() {
                let name = format!("p{proposed}");
                proposed += 1;
                let term = c.node(&l).term();
                if let Ok(idx) = c.node_mut(&l).propose(create(&name)) {
                    pending.push((l, term, idx, name));
                }
            }
        }
        c.tick();
        pending.retain(|(l, term, idx, name)| {
            let Some(Some(n)) = c.nodes.get(l) else {
                return false;
            };
            if n.term() != *term {
                return false;
            }
            if n.applied_index() >= *idx && n.catalog().volumes.contains_key(&format!("vol-{name}"))
            {
                acked.insert(name.clone());
                return false;
            }
            true
        });
    }

    c.isolated.clear();
    for id in &ids {
        if c.nodes[id].is_none() {
            c.restart(id);
        }
    }
    let settled = c.run_until(1000, |c| {
        let Some(l) = c.leader() else { return false };
        let want = c.volumes(&l);
        let applied = c.node(&l).applied_index();
        c.node(&l).commit_index() == c.node(&l).last_index()
            && ids
                .iter()
                .all(|id| c.volumes(id) == want && c.node(id).applied_index() == applied)
    });
    assert!(settled, "seed {seed}: cluster did not converge");
    let final_names = c.volumes(&c.leader().unwrap());
    assert!(
        !acked.is_empty(),
        "seed {seed}: schedule acknowledged nothing"
    );
    for name in &acked {
        assert!(
            final_names.contains(name),
            "seed {seed}: acknowledged commit {name} was lost"
        );
    }
}

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let mut c = Cluster::new(3, 4);
    let l = c.elect();
    let lagger = c.live_ids().into_iter().find(|id| *id != l).unwrap();
    c.isolated.insert(lagger.clone());

    let names: Vec<String> = (0..12).map(|i| format!("v{i}")).collect();
    for n in &names {
        c.node_mut(&l).propose(create(n)).unwrap();
        c.run(2);
    }
    assert!(
        c.node(&l).snapshot_index() > c.node(&lagger).last_index(),
        "leader should have compacted past the lagging follower"
    );

    c.isolated.clear();
    let want: Vec<&str> = names.iter().map(String::as_str).collect();
    assert!(c.run_until(200, |c| c.converged(&want)));
    assert_eq!(
        c.node(&lagger).applied_index(),
        c.node(&c.leader().unwrap()).applied_index()
    );

    c.crash(&lagger);
    c.restart(&lagger);
    assert_eq!(c.volumes(&lagger).len(), names.len());
}
