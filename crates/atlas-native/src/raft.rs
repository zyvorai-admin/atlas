// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Minimal Raft for the Atlas Native metadata log: leader election, log replication, quorum
//! commit, log compaction and snapshot installation. It replicates `MetaCommand` records and
//! applies them through the same `Catalog` state machine the single-node engine uses.
//!
//! The node is sans-IO: the caller drives [`RaftNode::tick`], feeds inbound messages to
//! [`RaftNode::step`] and delivers whatever [`RaftNode::take_messages`] returns. Transport is the
//! caller's concern. Durability is not: the vote is fsynced before any reply, log entries are
//! fsynced before they are acknowledged or counted toward the leader's own quorum vote, and
//! `catalog.json` is persisted before the log is compacted past it.
//!
//! Not implemented: pre-vote / check-quorum (a partitioned node that rejoins with a higher term
//! forces one extra election) and membership changes (the voter set is fixed at open).

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::{
    durable,
    metadata::{Catalog, MetaCommand, MetaError},
    wal::{Wal, WalError, WalRecord},
};

pub type NodeId = String;
pub type Entry = WalRecord<MetaCommand>;

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("not the leader (leader hint: {leader:?})")]
    NotLeader { leader: Option<NodeId> },
    #[error("rejected by the metadata state machine: {0}")]
    Rejected(#[from] MetaError),
    #[error("invalid raft config: {0}")]
    Config(String),
    #[error("raft log inconsistency: {0}")]
    Inconsistent(String),
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub id: NodeId,
    /// The other voters (excluding `id`).
    pub peers: Vec<NodeId>,
    pub root: PathBuf,
    /// Election timeout is drawn uniformly from `[min, max)` ticks.
    pub election_ticks: (u64, u64),
    pub heartbeat_ticks: u64,
    pub max_batch: usize,
    /// Compact the log once this many applied entries sit above the last compaction point
    /// (0 disables compaction).
    pub compact_after: u64,
}

impl RaftConfig {
    pub fn new(id: impl Into<NodeId>, peers: Vec<NodeId>, root: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            peers,
            root: root.into(),
            election_ticks: (10, 20),
            heartbeat_ticks: 3,
            max_batch: 64,
            compact_after: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    RequestVote {
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    /// On success `match_index` is the last index known to match the leader; on failure it is a
    /// hint for where the leader should retry.
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
    /// Carries the leader's applied catalog; its `applied_index`/`current_term` are the
    /// snapshot's last included index/term.
    InstallSnapshot {
        term: u64,
        snapshot: Box<Catalog>,
    },
    /// `match_index` is the follower's commit index: committed entries are the only ones
    /// guaranteed to match the leader (a kept log suffix may still hold stale entries).
    InstallSnapshotResponse {
        term: u64,
        match_index: u64,
    },
}

impl Message {
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResponse { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotResponse { term, .. } => *term,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct HardState {
    term: u64,
    voted_for: Option<NodeId>,
}

#[derive(Debug)]
pub struct RaftNode {
    cfg: RaftConfig,
    role: Role,
    hard: HardState,
    leader: Option<NodeId>,
    /// Contiguous entries with `index > snapshot_index`.
    log: Vec<Entry>,
    snapshot_index: u64,
    snapshot_term: u64,
    commit_index: u64,
    /// Applied state machine; `catalog.applied_index` is the applied index.
    catalog: Catalog,
    wal: Wal,
    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    rng: u64,
    outbox: Vec<Envelope>,
}

impl RaftNode {
    pub fn open(cfg: RaftConfig) -> Result<Self, RaftError> {
        let peers: BTreeSet<_> = cfg.peers.iter().collect();
        if peers.len() != cfg.peers.len() || peers.contains(&cfg.id) {
            return Err(RaftError::Config(
                "peers must be unique and must not include the node itself".into(),
            ));
        }
        let (lo, hi) = cfg.election_ticks;
        if lo == 0 || hi <= lo || cfg.heartbeat_ticks == 0 || cfg.heartbeat_ticks >= lo {
            return Err(RaftError::Config(
                "need 0 < heartbeat_ticks < election_ticks.0 < election_ticks.1".into(),
            ));
        }
        fs::create_dir_all(&cfg.root)?;

        let hard_path = cfg.root.join("raft_state.json");
        let mut hard: HardState = if hard_path.exists() {
            serde_json::from_slice(&fs::read(&hard_path)?)?
        } else {
            HardState::default()
        };
        let catalog_path = cfg.root.join("catalog.json");
        let catalog: Catalog = if catalog_path.exists() {
            serde_json::from_slice(&fs::read(&catalog_path)?)?
        } else {
            Catalog::default()
        };
        let applied = catalog.applied_index;

        let mut wal = Wal::open(cfg.root.join("wal"))?;
        let log: Vec<Entry> = wal
            .replay::<MetaCommand>()?
            .into_iter()
            .filter(|e| e.index > applied)
            .collect();
        for (i, e) in log.iter().enumerate() {
            if e.index != applied + 1 + i as u64 {
                return Err(RaftError::Inconsistent(format!(
                    "log gap: expected index {}, found {}",
                    applied + 1 + i as u64,
                    e.index
                )));
            }
        }
        wal.compact_through(applied)?;
        wal.raise_floor(applied);
        hard.term = hard.term.max(catalog.current_term);

        let mut rng = 0xcbf2_9ce4_8422_2325u64;
        for b in cfg.id.bytes() {
            rng = (rng ^ b as u64).wrapping_mul(0x0100_0000_01b3);
        }
        let mut node = Self {
            role: Role::Follower,
            hard,
            leader: None,
            log,
            snapshot_index: applied,
            snapshot_term: catalog.current_term,
            commit_index: applied,
            catalog,
            wal,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng: rng | 1,
            outbox: Vec::new(),
            cfg,
        };
        node.reset_election_timer();
        Ok(node)
    }

    pub fn id(&self) -> &str {
        &self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn leader(&self) -> Option<&str> {
        self.leader.as_deref()
    }
    pub fn term(&self) -> u64 {
        self.hard.term
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn applied_index(&self) -> u64 {
        self.catalog.applied_index
    }
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn last_index(&self) -> u64 {
        self.log
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }
    fn last_term(&self) -> u64 {
        self.log
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    pub fn take_messages(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }

    pub fn tick(&mut self) -> Result<(), RaftError> {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                self.campaign()?;
            }
        }
        Ok(())
    }

    /// Appends `command` to the replicated log. Returns its index; the command takes effect once
    /// `applied_index() >= index`. Validated against the leader's applied state plus every
    /// uncommitted entry, so a command that cannot apply is rejected here instead of being logged.
    pub fn propose(&mut self, command: MetaCommand) -> Result<u64, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader {
                leader: self.leader.clone(),
            });
        }
        let mut spec = self.catalog.clone();
        let applied = spec.applied_index;
        for e in self.log.iter().filter(|e| e.index > applied) {
            let _ = spec.apply_committed(e.term, e.index, &e.command);
        }
        let index = self.last_index() + 1;
        spec.apply(self.hard.term, index, &command)?;
        self.append_local(command)?;
        self.broadcast_append();
        self.advance_commit()?;
        Ok(index)
    }

    pub fn step(&mut self, env: Envelope) -> Result<(), RaftError> {
        let from = env.from;
        let term = env.msg.term();
        if term > self.hard.term {
            let hint = matches!(
                env.msg,
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. }
            )
            .then(|| from.clone());
            self.become_follower(term, hint)?;
        }
        match env.msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                let up_to_date = last_log_term > self.last_term()
                    || (last_log_term == self.last_term() && last_log_index >= self.last_index());
                let can_vote = self.hard.voted_for.as_ref().is_none_or(|v| *v == from);
                let granted = term == self.hard.term && can_vote && up_to_date;
                if granted {
                    self.hard.voted_for = Some(from.clone());
                    self.persist_hard()?;
                    self.reset_election_timer();
                }
                self.send(
                    from,
                    Message::RequestVoteResponse {
                        term: self.hard.term,
                        granted,
                    },
                );
            }
            Message::RequestVoteResponse { term, granted } => {
                if self.role == Role::Candidate && term == self.hard.term && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader()?;
                    }
                }
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < self.hard.term {
                    self.send(
                        from,
                        Message::AppendEntriesResponse {
                            term: self.hard.term,
                            success: false,
                            match_index: 0,
                        },
                    );
                    return Ok(());
                }
                if self.role != Role::Follower {
                    self.become_follower(term, Some(from.clone()))?;
                }
                self.leader = Some(from.clone());
                self.reset_election_timer();
                let (success, match_index) =
                    self.handle_append(prev_log_index, prev_log_term, entries, leader_commit)?;
                self.send(
                    from,
                    Message::AppendEntriesResponse {
                        term: self.hard.term,
                        success,
                        match_index,
                    },
                );
            }
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
            } => {
                if self.role != Role::Leader || term != self.hard.term {
                    return Ok(());
                }
                if success {
                    let m = self.match_index.entry(from.clone()).or_insert(0);
                    *m = (*m).max(match_index);
                    let next = *m + 1;
                    self.next_index.insert(from.clone(), next);
                    self.advance_commit()?;
                    if next <= self.last_index() {
                        self.send_append(&from);
                    }
                } else {
                    let cur = self.next_index.get(&from).copied().unwrap_or(1);
                    let next = cur.saturating_sub(1).min(match_index + 1).max(1);
                    self.next_index.insert(from.clone(), next);
                    self.send_append(&from);
                }
            }
            Message::InstallSnapshot { term, snapshot } => {
                if term >= self.hard.term {
                    if self.role != Role::Follower {
                        self.become_follower(term, Some(from.clone()))?;
                    }
                    self.leader = Some(from.clone());
                    self.reset_election_timer();
                    self.handle_snapshot(*snapshot)?;
                }
                self.send(
                    from,
                    Message::InstallSnapshotResponse {
                        term: self.hard.term,
                        match_index: self.commit_index,
                    },
                );
            }
            Message::InstallSnapshotResponse { term, match_index } => {
                if self.role != Role::Leader || term != self.hard.term {
                    return Ok(());
                }
                let last = self.last_index();
                let m = self.match_index.entry(from.clone()).or_insert(0);
                *m = (*m).max(match_index.min(last));
                let next = *m + 1;
                self.next_index.insert(from.clone(), next);
                self.advance_commit()?;
                if next <= self.last_index() {
                    self.send_append(&from);
                }
            }
        }
        Ok(())
    }

    fn handle_append(
        &mut self,
        mut prev_index: u64,
        mut prev_term: u64,
        mut entries: Vec<Entry>,
        leader_commit: u64,
    ) -> Result<(bool, u64), RaftError> {
        if prev_index < self.snapshot_index {
            // Everything up to snapshot_index is committed and therefore identical to the leader.
            let skip = (self.snapshot_index - prev_index) as usize;
            if skip >= entries.len() {
                return Ok((true, self.snapshot_index));
            }
            entries.drain(..skip);
            prev_index = self.snapshot_index;
            prev_term = self.snapshot_term;
        }
        match self.term_at(prev_index) {
            None => return Ok((false, self.last_index())),
            Some(t) if t != prev_term => {
                let mut hint = prev_index - 1;
                while hint > self.snapshot_index && self.term_at(hint) == Some(t) {
                    hint -= 1;
                }
                return Ok((false, hint));
            }
            Some(_) => {}
        }
        let match_index = prev_index + entries.len() as u64;
        for e in entries {
            match self.term_at(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    if e.index <= self.commit_index {
                        return Err(RaftError::Inconsistent(format!(
                            "leader overwrote committed index {}",
                            e.index
                        )));
                    }
                    self.wal.truncate_after(e.index - 1)?;
                    self.log
                        .truncate((e.index - self.snapshot_index - 1) as usize);
                }
                None => {}
            }
            self.wal.append(&e)?;
            self.log.push(e);
        }
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(match_index).max(self.commit_index);
            self.apply_committed()?;
        }
        Ok((true, match_index))
    }

    fn handle_snapshot(&mut self, snap: Catalog) -> Result<(), RaftError> {
        let si = snap.applied_index;
        let st = snap.current_term;
        if si <= self.commit_index {
            return Ok(());
        }
        if self.term_at(si) == Some(st) {
            self.log.drain(..(si - self.snapshot_index) as usize);
            self.wal.compact_through(si)?;
        } else {
            self.log.clear();
            self.wal.reset(si)?;
        }
        self.wal.raise_floor(si);
        self.catalog = snap;
        self.persist_catalog()?;
        self.snapshot_index = si;
        self.snapshot_term = st;
        self.commit_index = si;
        Ok(())
    }

    fn campaign(&mut self) -> Result<(), RaftError> {
        self.role = Role::Candidate;
        self.hard.term += 1;
        self.hard.voted_for = Some(self.cfg.id.clone());
        self.persist_hard()?;
        self.leader = None;
        self.votes = BTreeSet::from([self.cfg.id.clone()]);
        self.reset_election_timer();
        if self.votes.len() >= self.quorum() {
            return self.become_leader();
        }
        let msg = Message::RequestVote {
            term: self.hard.term,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        for p in self.cfg.peers.clone() {
            self.send(p, msg.clone());
        }
        Ok(())
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) -> Result<(), RaftError> {
        if term > self.hard.term {
            self.hard.term = term;
            self.hard.voted_for = None;
            self.persist_hard()?;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_election_timer();
        Ok(())
    }

    fn become_leader(&mut self) -> Result<(), RaftError> {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id.clone());
        self.heartbeat_elapsed = 0;
        let next = self.last_index() + 1;
        self.next_index = self.cfg.peers.iter().map(|p| (p.clone(), next)).collect();
        self.match_index = self.cfg.peers.iter().map(|p| (p.clone(), 0)).collect();
        // Entries from earlier terms only commit once an entry of the current term does.
        self.append_local(MetaCommand::Noop)?;
        self.broadcast_append();
        self.advance_commit()
    }

    fn append_local(&mut self, command: MetaCommand) -> Result<u64, RaftError> {
        let index = self.last_index() + 1;
        let e = Entry {
            term: self.hard.term,
            index,
            command,
        };
        self.wal.append(&e)?;
        self.log.push(e);
        Ok(index)
    }

    fn broadcast_append(&mut self) {
        for p in self.cfg.peers.clone() {
            self.send_append(&p);
        }
    }

    fn send_append(&mut self, peer: &NodeId) {
        let next = self
            .next_index
            .get(peer)
            .copied()
            .unwrap_or(self.last_index() + 1)
            .min(self.last_index() + 1);
        let prev = next - 1;
        if prev < self.snapshot_index {
            let snapshot = Box::new(self.catalog.clone());
            self.send(
                peer.clone(),
                Message::InstallSnapshot {
                    term: self.hard.term,
                    snapshot,
                },
            );
            return;
        }
        let prev_term = self.term_at(prev).unwrap_or(0);
        let start = (next - self.snapshot_index - 1) as usize;
        let end = (start + self.cfg.max_batch).min(self.log.len());
        let entries = self.log[start..end].to_vec();
        self.send(
            peer.clone(),
            Message::AppendEntries {
                term: self.hard.term,
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    fn advance_commit(&mut self) -> Result<(), RaftError> {
        let quorum = self.quorum();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1..=self.last_index()).rev() {
            if self.term_at(n) != Some(self.hard.term) {
                break;
            }
            let acks = 1 + self.match_index.values().filter(|m| **m >= n).count();
            if acks >= quorum {
                new_commit = n;
                break;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed()?;
            self.broadcast_append();
        }
        Ok(())
    }

    fn apply_committed(&mut self) -> Result<(), RaftError> {
        let start = self.catalog.applied_index;
        while self.catalog.applied_index < self.commit_index {
            let idx = self.catalog.applied_index + 1;
            let e = self
                .entry(idx)
                .ok_or_else(|| RaftError::Inconsistent(format!("missing entry {idx}")))?
                .clone();
            // A rejected command is a committed no-op on every replica; see apply_committed.
            let _ = self.catalog.apply_committed(e.term, e.index, &e.command);
        }
        if self.catalog.applied_index > start {
            self.persist_catalog()?;
            self.maybe_compact()?;
        }
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<(), RaftError> {
        let applied = self.catalog.applied_index;
        if self.cfg.compact_after == 0 || applied - self.snapshot_index < self.cfg.compact_after {
            return Ok(());
        }
        let term = self
            .term_at(applied)
            .ok_or_else(|| RaftError::Inconsistent(format!("no term for {applied}")))?;
        self.wal.compact_through(applied)?;
        self.log.drain(..(applied - self.snapshot_index) as usize);
        self.snapshot_index = applied;
        self.snapshot_term = term;
        Ok(())
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry(index).map(|e| e.term)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        if index <= self.snapshot_index {
            return None;
        }
        self.log.get((index - self.snapshot_index - 1) as usize)
    }

    fn quorum(&self) -> usize {
        let voters = self.cfg.peers.len() + 1;
        voters / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let (lo, hi) = self.cfg.election_ticks;
        self.election_elapsed = 0;
        self.election_timeout = lo + self.rng % (hi - lo);
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Envelope {
            from: self.cfg.id.clone(),
            to,
            msg,
        });
    }

    fn persist_hard(&self) -> Result<(), RaftError> {
        let bytes = serde_json::to_vec(&self.hard)?;
        durable::write_atomic(&self.cfg.root.join("raft_state.json"), &bytes)?;
        Ok(())
    }

    fn persist_catalog(&self) -> Result<(), RaftError> {
        let bytes = serde_json::to_vec_pretty(&self.catalog)?;
        durable::write_atomic(&self.cfg.root.join("catalog.json"), &bytes)?;
        Ok(())
    }
}
