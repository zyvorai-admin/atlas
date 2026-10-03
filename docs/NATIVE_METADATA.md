<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas Native metadata durability — Phases 2 and 3

Phase 2 introduced a deterministic metadata state machine and a write-ahead log (WAL). Phase 3 adds
WAL checkpoint/compaction, device-space free lists, and a Raft core that replicates the same log.

## Commit invariant (single-node engine)

For every metadata mutation:

1. build a `MetaCommand`;
2. apply it to a copy of the catalog; a command that fails validation is rejected here and never logged;
3. append `{term,index,command}` to `metadata.wal`;
4. `fsync` the WAL;
5. publish the new catalog in memory;
6. atomically replace `catalog.json` and fsync the directory.

On restart, Atlas loads `catalog.json` and replays every WAL record whose index is greater than
`catalog.applied_index`. A torn final WAL line (a record that never finished its fsync, so was never
acknowledged) is discarded on open; corruption anywhere else is a hard error.

## Checkpoint and WAL compaction

`catalog.json` already durably covers every applied index, so WAL records at or below it are
redundant. The engine compacts the WAL once it holds `EngineConfig::wal_compact_after` records
(default 1024, `0` disables) and on an explicit `NativeEngine::checkpoint()`. Compaction rewrites the
log through a temp file + fsync + rename + directory fsync. After compaction the WAL may be empty, so
its index floor is raised to `catalog.applied_index` on open to keep indexes monotonic.

## Extent lifetime and space reuse

Each physical extent has a metadata refcount. Active volumes and snapshots both own references.
Overwrites decrement the previous active extent and install a new immutable extent. Snapshot delete
and volume delete decrement references. Zero-reference extents become GC candidates.

`gc_once` commits `MarkExtentReclaimed` for each candidate. Applying it removes the extent and returns
every replica's `(node, device, offset, len)` to the catalog's free list (`alloc::FreeList`: sorted,
non-overlapping, adjacent ranges coalesced; a double free is rejected).

Writes allocate first-fit from the free list before appending to the device. The data is written to
the free range *before* the `InstallExtent` commit; applying `InstallExtent` is what removes the range
from the free list. So:

- a crash after the data write but before the commit leaves the range free and unreferenced;
- WAL replay and Raft followers rebuild exactly the same free list, because allocation is a
  deterministic consequence of applied commands;
- a single engine-wide write lock spans allocate → write → commit, so two writers can never be handed
  the same range.

Space held by a snapshot is never reused until the snapshot is deleted and GC runs.

## Raft metadata replication (`raft` module)

`RaftNode` replicates `MetaCommand` records across a fixed set of metadata voters and applies them
through `Catalog::apply_committed`. It is sans-IO: the caller drives `tick()`, delivers inbound
messages with `step()` and sends whatever `take_messages()` returns. Implemented:

- randomized election timeouts, `RequestVote` with the up-to-date-log check, one vote per term;
- `AppendEntries` with prev-index/term consistency, conflict truncation (never below the commit
  index) and a conflict hint so the leader backs off a whole term at a time;
- leader commit only for entries of its own term (a new leader appends a `Noop` to commit earlier
  ones), quorum = majority of voters including itself;
- log compaction after `compact_after` applied entries and `InstallSnapshot` (the leader's applied
  catalog) for followers behind the compaction point.

Each replica keeps its own `raft_state.json` (term + vote), WAL and `catalog.json`. Durability order:
the vote is fsynced before any reply; entries are fsynced before they are acknowledged or counted
toward the leader's own vote; `catalog.json` is persisted before the log is compacted past it.

Proposals are validated on the leader against its applied catalog plus all uncommitted entries, so
an invalid command is rejected instead of logged. If a committed command still fails to apply, it is
a no-op that consumes its index on every replica, keeping replicas identical.

Tests (`tests/raft.rs`) run 3-node clusters over a simulated network: election, replication,
follower redirect, leader crash, a partitioned minority leader whose uncommitted entry is discarded,
full-cluster restart from disk, snapshot catch-up, and a randomized partition/crash/restart schedule
that checks no acknowledged commit is lost and all replicas converge.

Not implemented yet:

- a network transport, and wiring `NativeEngine` to commit through a `RaftNode` instead of its local
  WAL (the engine is still single-node);
- pre-vote / check-quorum: a partitioned node that rejoins with a higher term forces one extra election;
- membership changes: the voter set is fixed at open;
- linearizable reads (read index / leases).

## Failure model covered

- process crash after WAL fsync but before catalog persistence (tested by restoring a stale
  `catalog.json`);
- torn final WAL record;
- restart/replay without double-applying committed commands;
- snapshot copy-on-write isolation and space protection;
- refcount underflow and free-list double-free protection;
- monotonic WAL indexes across compaction;
- metadata leader crash, minority partition and full restart (Raft).

## Next phase

- network transport for `RaftNode` and engine integration (commit through Raft);
- pre-vote, check-quorum and joint-consensus membership changes;
- background scrub and replica repair;
- hole punching for freed ranges at the device tail;
- io_uring/raw-NVMe data path.
