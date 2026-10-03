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
  catalog) for followers behind the compaction point;
- pre-vote: a node whose election timer fires first asks for pre-votes for `term + 1` without
  changing anyone's term, and only campaigns once a majority would vote for it, so a partitioned
  node cannot inflate its term and depose a healthy leader when it rejoins;
- check-quorum: a leader that has not heard from a majority within the minimum election timeout
  steps down, and a node that has heard from a live leader within that window ignores
  higher-term vote requests.

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

### TCP transport (`raft_server` module)

`RaftServer::start(cfg, listener, peers, tick)` runs a `RaftNode` across processes using only the
standard library:

- frames are a 4-byte big-endian length plus a JSON `Envelope`, capped at 256 MiB;
- a driver thread owns the node, ticks it every `tick` and steps inbound messages in batches;
- one sender thread per peer keeps a connection open, reconnects with a short backoff and drops
  messages while the peer is down (Raft retransmits);
- inbound envelopes are dropped unless `from` is a configured peer and `to` is this node;
- `propose(cmd, timeout)` blocks until the entry is applied locally, or returns `NotLeader` (with a
  leader hint), `LeadershipLost` (outcome unknown), `Timeout` or `Shutdown`;
- a storage error inside the node (e.g. a failed fsync) stops the server and is reported by
  `fatal_error()`; it never keeps serving on a state it could not persist.

Started without a `TlsIdentity` the transport has **no authentication or encryption**; bind it to a
private metadata network only, or enable mutual TLS (below).

`tests/raft_tcp.rs` runs a real 3-server cluster over localhost: election and replication, follower
redirect, and leader shutdown, failover and rejoin from disk.

### Quorum configuration (`membership` module)

Every Raft quorum decision (pre-votes, votes, commit acknowledgements, check-quorum) goes through
`Membership::has_quorum`. Today the configuration is always `Stable { voters }` (the node plus its
peers). `Joint { old, new }` requires a majority of both sets, which is the rule a joint-consensus
membership change needs; the change protocol itself (committing the joint and then the new
configuration) is not implemented yet.

### Metrics

`RaftServer::render_metrics()` and `NativeEngine::render_metrics()` return Prometheus text; the
process hosting a node serves them on its `/metrics` endpoint (there is no standalone native node
binary yet). All Raft series carry a `node` label:

- Raft: `atlas_native_raft_{term,commit_index,applied_index,last_index,snapshot_index}`,
  `atlas_native_raft_role{role}` (one-hot), `atlas_native_raft_peer_match_index{peer}` (leader only),
  `atlas_native_raft_{elections,leader_terms,append_rejections}_total`;
- transport, per peer: `atlas_native_transport_{sent,connect_failures,write_failures,dropped}_total`,
  plus `atlas_native_transport_rejected_frames_total`;
- engine: `atlas_native_{reads,writes,read_bytes,write_bytes,checksum_failures,replica_fallbacks}_total`,
  `atlas_native_gc_reclaimed_extents_total`, `atlas_native_metadata_applied_index`,
  `atlas_native_wal_records`, `atlas_native_{volumes,snapshots,extents}`,
  `atlas_native_allocator_free_{bytes,ranges}`, `atlas_native_device_bytes{node}`;
- data node: `atlas_native_data_requests_total{op}`, `atlas_native_data_{fenced_writes,errors,
  written_bytes,read_bytes}_total`, `atlas_native_data_fence`, `atlas_native_data_device_bytes`.

### Data nodes (`data_node` module)

The engine writes replicas through the `BlockStore` trait: `FileDevice` for local files, or
`RemoteDevice` for a `DataNodeServer` on another host. A data node serves one local device file:

- each message is a 4-byte big-endian header length, a JSON header, then the raw payload the
  header announces (write data in requests, read data in responses); headers are capped at 64 KiB
  and payloads at 256 MiB;
- one thread per connection; `RemoteDevice` keeps one connection open, bounds every connect, read
  and write with a timeout, and retries once on a fresh connection if a reused one fails (a retried
  append can leave an unreferenced copy, the same leak as a crash between data write and commit);
- without a `TlsIdentity` the transport has **no authentication or encryption**, same as Raft.

**Write fencing.** Every `append`/`write_at` carries a fence: the writer's Raft term. The node
durably records (`root/fence`, atomic rewrite) the highest fence it has accepted and rejects lower
ones with `Fenced { current }`, holding the fence lock across the write. This is what makes free-list
reuse safe across leader changes: a deposed leader that has not stepped down yet can only allocate
ranges that are free in its own state, and the new leader always writes a range to its data nodes
before committing it, so the stale write either lands first and is overwritten, or arrives later and
is fenced.

### Engine over Raft (`MetaBackend::Raft`)

`NativeEngine::open_with(cfg, nodes, MetaBackend::Raft { server, timeout })` commits every
`MetaCommand` through a `RaftServer` instead of the local WAL. One engine runs per metadata replica,
all configured with the same node ids and data-node addresses:

- mutations only succeed on the leader's engine; others return `Raft(NotLeader { leader })`;
- `write` first calls `RaftServer::leader_ready`, which waits until the node is leader and has
  applied its whole log (including the no-op that commits earlier terms), so allocation reads a
  free list that reflects every committed reservation. The returned term fences the data writes,
  and `InstallExtent` is proposed with `propose_in_term`, which refuses if the term moved on;
- `gc_once` waits for the same barrier so it never proposes a reclaim that is already in the log;
- reads come from the local replica's applied catalog, so a follower can lag the leader (no
  linearizable reads yet); extent metadata is copied out before the network read, so a slow data
  node never holds the Raft node lock;
- `checkpoint` is refused: the Raft node compacts its own log. `wal_records` reports the Raft log
  length above its compaction point.

Node health and repair are described in the next section.

`tests/data_node.rs` covers the remote device and its bounds, fencing across a data-node restart, an
engine on remote data nodes losing one, and three Raft-backed engines sharing three data nodes
through a leader failover, followed by an overwrite and GC on the new leader that reuse space freed
under the old one. A write with the old leader's term is then fenced.

### Node health, write failover and repair

Each engine keeps a per-node circuit breaker. Any data-node I/O failure (a fenced write excepted:
that means this engine was deposed and is returned to the caller) counts a failure and backs the
node off for `EngineConfig::node_retry_after` (default 5 s); any success clears the back-off. After
the window the node is eligible again, so a recovered node rejoins on its next successful I/O.
Health is local to each engine and never replicated; a node's configured `healthy: false` still
excludes it permanently.

- **Writes** walk every eligible node in placement preference order (rack spread first, distinct
  hosts if required) and stop once `placement.replicas` writes succeed. A node that fails is backed
  off and the replica goes to the next node, so with a spare node a write survives a data node
  dying mid-write. With fewer eligible nodes than replicas the write fails with
  `InsufficientReplicas` before any I/O. Data already written to other replicas of a failed attempt
  is not referenced (the same leak as a crash between data write and commit).
- **Reads** try replicas on eligible nodes first and backed-off nodes last (they may hold the only
  good copy).
- **`repair_once`** scrubs every extent: it reads each replica and verifies its checksum. A replica
  that is unreachable or corrupt is rewritten from a verified copy onto an eligible node that holds
  none of the extent's other replicas (and, when distinct hosts are required, none of their hosts);
  a corrupt replica on a reachable node can move to a fresh range on the same node. The move commits
  as `MetaCommand::ReplaceReplica { extent_id, old, new }`, which reserves the new range and returns
  the old one to the free list, so it replicates through Raft and replays from the WAL like any
  other command. Under Raft it runs on the leader behind the same `leader_ready` barrier and fence
  as writes. It reports `extents_checked`, `replicas_repaired`, `unrecoverable` (no good copy left)
  and `deferred` (no eligible target, or the extent changed underneath; retried on the next pass).
  The engine does not schedule it; the hosting process decides how often to call it.

Metrics: `atlas_native_node_up{node}`, `atlas_native_node_failures_total{node}`,
`atlas_native_replica_write_failures_total`, `atlas_native_replicas_repaired_total`;
`atlas_native_device_bytes{node}` omits unreachable nodes instead of failing the scrape.

`tests/health.rs` covers failing over a write to a spare node, skipping a backed-off node and
reusing it after recovery, refusing writes with too few nodes, re-replicating after losing a node
(then surviving the loss of a second original), rewriting a corrupt replica in place and freeing
its old range (across reopen), reporting an extent with no good copy, and `ReplaceReplica`
validation. `tests/data_node.rs` also repairs a lost replica on a Raft leader and checks every
replica's engine reads through the new copy.

### Mutual TLS (`tls` module)

Both transports take an optional `TlsIdentity` (`RaftServer::start_with`,
`DataNodeServer::start_with`, `RemoteDevice::with_tls`), built from PEM: the cluster CA bundle,
the node's certificate chain and its private key. With it every connection is mutual TLS (rustls,
explicit `ring` provider, TLS 1.2/1.3) and both sides verify the other against the cluster CA:

- each node's certificate carries its node id as a DNS SAN, so node ids must be valid DNS names;
- a Raft peer is dialled as its node id, and an inbound envelope is accepted only if its `from` is a
  configured peer **and** a name the connection's client certificate is valid for, so a node can
  only speak for the identity its certificate names;
- a `RemoteDevice` verifies the data node's certificate against the expected node id; a data node
  accepts any client certificate signed by the cluster CA (it does not yet restrict which clients);
- handshakes are bounded by a 5 s timeout and counted in
  `atlas_native_transport_tls_handshake_failures_total` / `atlas_native_data_tls_handshake_failures_total`.

Without an identity both transports stay plaintext, as before. There is no certificate rotation
without restarting the server, and no revocation (CRL/OCSP) checking.

`tests/tls.rs` runs an mTLS Raft cluster, isolates a peer holding a CA-signed certificate for another
node's name (frames rejected, never dialled successfully) and a peer from a different CA (handshake
failures), checks the data node refuses plaintext, wrong-name and rogue-CA clients, and runs
Raft-backed engines writing to TLS data nodes.

Not implemented yet:

- per-client authorization on data nodes, certificate hot reload and revocation;
- a background repair schedule and repair rate limiting (`repair_once` is a full scan per call);
- membership changes: the voter set is fixed at open (quorum math already supports joint
  configurations);
- linearizable reads (read index / leases).

## Failure model covered

- process crash after WAL fsync but before catalog persistence (tested by restoring a stale
  `catalog.json`);
- torn final WAL record;
- restart/replay without double-applying committed commands;
- snapshot copy-on-write isolation and space protection;
- refcount underflow and free-list double-free protection;
- monotonic WAL indexes across compaction;
- metadata leader crash, minority partition, rejoin without disruption, isolated-leader step-down
  and full restart (Raft);
- data-node loss (reads fall back, writes move to a spare node, repair re-replicates), corrupt
  replicas (repair rewrites them), stale-leader data writes (fenced, including across a data-node
  restart) and engine continuity through a metadata leader failover.

## Next phase

- a container image, Helm chart and gateway integration for `atlas-native-node`
  (`docs/NATIVE_NODE.md`);
- joint-consensus membership changes;
- incremental, rate-limited background scrub (today `repair_once` scans everything);
- hole punching for freed ranges at the device tail;
- io_uring/raw-NVMe data path.
