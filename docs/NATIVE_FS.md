<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native filesystems

An atlas-native cluster (`docs/NATIVE_NODE.md`) serves POSIX filesystems next to its block volumes.
Files and directories live in the same replicated catalog as volumes, and file data uses the same
copy-on-write extents (3 replicas) on a per-filesystem grid. Clients mount a filesystem through FUSE
with `atlas-native-mount`; the Atlas gateway creates, snapshots and clones filesystems as
`filesystem` volumes on backend `bkd_native`.

## Model

- A **filesystem** (`/v1/fs`) is an inode table inside the catalog: `id`, `name`, root inode 1,
  and an optional `source_snapshot` for clones.
- An **inode** is a directory (name → inode map), a regular file (offset → extent map plus size),
  a symlink (target, at most 4096 bytes) or a special file (FIFO, socket, character or block
  device, with `rdev`). Each has mode, uid, gid, nlink and atime/mtime/ctime in nanoseconds.
  Names are 1–255 bytes, without `/` or NUL, and never `.` or `..`.
- Every change is one Raft command (`MetaCommand::Fs`) applied identically on each replica.
  Timestamps are chosen by the leader and travel in the command, so replicas stay deterministic.
  Creates carry a client-chosen `op_id`: a retried create returns the inode it already made instead
  of failing with "exists".
- Hard links, `rename` (including over an existing entry, across directories, and refusing to move a
  directory into itself), `setattr` (mode, owner, size, times) and `truncate` follow POSIX. Shrinking
  a file drops the extents past the new end and trims the extent that straddles it.
- File data is written through the extent path the volumes use, on the filesystem's own grid
  (`extent_bytes`, chosen at create time): a write covering part of an extent rewrites that extent
  with the old bytes merged in; never-written ranges are holes and read as
  zeros; reads stop at end of file.
- **Snapshots** copy a filesystem's inode table under a snapshot id and take a reference on every
  extent it points to; no data is copied. A snapshot is read-only and addressed as `<fs>@<snapshot>`.
  **Clones** start a new filesystem from a snapshot. Writes on either side allocate new extents, so a
  filesystem, its snapshots and its clones never see each other's changes. Each survives deleting the
  others; extents are freed by GC once nothing references them.

## Node API

All routes need the API token (and client certificate where configured), like the rest of `/v1/*`.

| Method and path | Description |
| --- | --- |
| `GET /v1/fs` | `{"filesystems": [{id, name, inodes, bytes, source_snapshot, extent_bytes}]}`. |
| `POST /v1/fs` | `{"name", "id"?, "extent_bytes"?}` → 201 `{"id"}`. Idempotent with the same `id` and name. `extent_bytes` (default 1 MiB, at most the cluster's `extent_bytes`) is the file extent grid; snapshots and clones keep it. |
| `DELETE /v1/fs/{fs}` | 204. Snapshots and clones of it are unaffected. |
| `GET /v1/fs/{fs}/statfs` | `{inodes, used_bytes, free_list_bytes}`. |
| `POST /v1/fs/{fs}/rename` | `{parent, name, new_parent, new_name}` → 204. |
| `POST /v1/fs/{fs}/snapshots` | `{"name", "id"?}` → 201 `{"id"}`. |
| `GET /v1/fs-snapshots` | `{"snapshots": [{id, fs_id, name, created_ns, inodes}]}`. |
| `DELETE /v1/fs-snapshots/{id}` | 204. |
| `POST /v1/fs-snapshots/{id}/clone` | `{"name", "id"?}` → 201 `{"id"}` of the new filesystem. |
| `GET /v1/fs/{fs}/inodes/{ino}` | Attributes: `{ino, kind, mode, uid, gid, nlink, size, blocks, rdev, atime_ns, mtime_ns, ctime_ns}`. |
| `POST /v1/fs/{fs}/inodes/{ino}/attr` | Any of `{mode, uid, gid, size, atime_ns, mtime_ns}` → attributes. |
| `GET /v1/fs/{fs}/inodes/{dir}/lookup?name=` | Attributes of the entry (`name` percent-encoded). |
| `GET /v1/fs/{fs}/inodes/{dir}/entries` | `{"entries": [{name, ino, kind}]}` in name order. `?limit=N` returns at most N; `?after=<name>` (percent-encoded) starts after that name. Without `limit`, the whole directory. |
| `POST /v1/fs/{fs}/inodes/{dir}/entries` | `{name, op_id, kind, mode, uid, gid, target?, rdev?}` (`kind`: `file`, `dir`, `symlink`, `fifo`, `socket`, `char_device`, `block_device`) → 201 attributes. |
| `POST /v1/fs/{fs}/inodes/{dir}/unlink`, `/rmdir` | `{name}` → 204. |
| `POST /v1/fs/{fs}/inodes/{ino}/links` | `{parent, name}`: a hard link → attributes. |
| `GET /v1/fs/{fs}/inodes/{ino}/target` | `{"target"}` of a symlink. |
| `GET /v1/fs/{fs}/inodes/{ino}/xattrs` | `{"names": [...]}` of the extended attributes. |
| `GET /v1/fs/{fs}/inodes/{ino}/xattrs/{name}` | Raw value (`name` percent-encoded); 404 `no_attr` if unset. |
| `PUT /v1/fs/{fs}/inodes/{ino}/xattrs/{name}?mode=` | Raw body as the value → 204. `mode`: `set` (default), `create` (409 `exists` if set), `replace` (404 `no_attr` if unset). |
| `DELETE /v1/fs/{fs}/inodes/{ino}/xattrs/{name}` | 204; 404 `no_attr` if unset. |
| `GET /v1/fs/{fs}/inodes/{ino}/data?offset=N&len=M` | Raw bytes; short at end of file. |
| `PUT /v1/fs/{fs}/inodes/{ino}/data?offset=N` | Raw body written at `offset` (extends the file) → attributes. |
| `POST /v1/fs/{fs}/sessions` | `{"session", "ttl_ms", "cache"?}` (TTL 1000–300000): opens a client session, or renews it with a new TTL → `{session, ttl_ms, expires_ms}`. Session ids are client-chosen (the mount uses a UUID). `cache: true` if the session will hold cache leases. |
| `GET /v1/fs/{fs}/sessions/{id}/recalls?wait_ms=` | `{"inos": [...]}`: cache leases the session must give back, waiting up to `wait_ms` (at most 25 s) for one. Leader only. |
| `POST /v1/fs/{fs}/sessions/{id}/recalls/done` | `{"inos": [...]}`: the session dropped what it cached under these leases → 204. |
| `POST /v1/fs/{fs}/sessions/{id}/renew` | → `{session, ttl_ms, expires_ms}`; 410 `no_session` once it expired or was closed. |
| `DELETE /v1/fs/{fs}/sessions/{id}` | 204; releases every lock the session holds. |
| `POST /v1/fs/{fs}/inodes/{ino}/locks` | `{session, owner, kind, start?, end?, pid?}` (`kind`: `read`, `write`, `unlock`; `end` inclusive, default end of file) → 204, or 409 `locked` on another owner's conflicting lock. POSIX `F_SETLK` semantics per `(session, owner)`; renews the session. Regular files only. |
| `GET /v1/fs/{fs}/inodes/{ino}/locks?session=&owner=&kind=&start=&end=` | `{"conflict": {ino, session, owner, kind, start, end, pid} \| null}`: the first lock that would block the request (`F_GETLK`). |
| `POST /v1/fs/{fs}/inodes/{ino}/locks/release` | `{session, owner}`: drops the owner's locks on the inode (a close) → 204. |
| `GET /v1/fs/{fs}/locks` | `{"sessions": N, "locks": [...]}`: every lock held on the filesystem and the open sessions in its metadata group. |

`{fs}` may be `<fs>@<snapshot>` for reads; any change to a snapshot gets 409 `read_only`.

`?session=<id>` names the client session a request comes from. On `GET .../inodes/{ino}` and
`GET .../lookup`, `&lease=1` also asks for a cache lease: the leader confirms its leadership (a
read barrier), grants the session a lease on the inode (on a lookup, on the directory and the
child) and adds `"lease_ms"` to the attributes; no `lease_ms` means none was granted (a change to
the inode is in flight, a follower answered, or the tree is a snapshot).

Errors are `{"error", "code", "leader"}`. `code` is stable and maps to an errno in the client:
`not_found` (404, ENOENT), `exists` (EEXIST), `not_empty` (ENOTEMPTY), `not_dir` (ENOTDIR),
`is_dir` (EISDIR), `invalid` (EINVAL), `read_only` (EROFS), `busy` (EBUSY), `no_attr` (404,
ENODATA), `too_big` (E2BIG), `unsupported` (EOPNOTSUPP), `locked` (EAGAIN), `no_session` (410,
ENOLCK) — all 409 except where noted — plus
`not_leader` (421), `unavailable` (503) and `internal` (500).

Reads are served by the leader once it has applied its log as of the request, so a client always
sees its own writes. `?barrier=1` on a `GET` lets any replica answer after a read barrier (still
linearizable, at the cost of one round trip to the leader), and `?stale=1` reads whatever the
receiving replica has applied.
Mutations go to the leader; followers answer 421 with the leader's id.

## atlas-native-mount

`crates/atlas-native-fuse` (bin `atlas-native-mount`, `fuse` cargo feature, Linux only):

```
atlas-native-mount --endpoint https://n1:7480,https://n2:7480,https://n3:7480 \
  --token-file /etc/atlas-native/token --ca-file ca.pem --fs <id> /mnt/data
atlas-native-mount ... --fs <id> --snapshot <snapshot-id> /mnt/data-at-snap   # read-only
```

Options: `--identity-file` (client certificate + key PEM), `--ttl-ms` (attribute and name cache,
default 1000), `--writeback-bytes` (default 4 MiB), `--readahead-bytes` (default 4 MiB, 0 disables),
`--max-io-bytes` (largest request, default 8 MiB; keep at or below the nodes' `max_request_bytes`),
`--retry-secs` (default 30), `--read-only`, `--allow-other`, `--fuse-threads` (kernel request
workers, each with its own `/dev/fuse` fd, default 4), `--session-ttl-ms` (lease of the mount's
session, default 15000), `--cache-leases` (see Consistency) and `--direct-reads` (below). It runs in the
foreground until `fusermount3 -u`; the mount uses `default_permissions`, so the kernel checks
modes and ownership.

- **Failover**: every call goes to the last endpoint that answered and moves on through 421s,
  503s and unreachable nodes until `--retry-secs` runs out. Creates reuse their `op_id` and writes
  are absolute, so retrying after an ambiguous failure cannot apply twice; a remove that finds the
  entry already gone after such a failure counts as done.
- **Write-back**: sequential writes to an open file are buffered per inode and sent when the buffer
  fills, on `fsync`/`flush`/close, and before a read, truncate or unlink of that file; `getattr`
  already reports the buffered size. A failed send surfaces as an error from `fsync`/close.
- **Read-ahead**: a read that continues where the previous one ended fetches `--readahead-bytes`
  and serves following reads from it; random reads fetch only what was asked.
- **Unlink while open**: removing a file another handle in the same mount still has open renames it
  to a hidden `.atlas_hidden_<ino>_<n>` entry (not listed by `readdir`) and removes it on the last
  close, or at unmount.
- **Directory listings**: each open directory reads 1024 entries per request (`?after=&limit=`)
  and keeps its place between the kernel's `readdir` calls, so listing a directory of any size
  costs one request per page. Seeking back restarts the listing.
- **Direct reads** (`--direct-reads`): the leader only answers
  `GET /v1/fs/<fs>/inodes/<ino>/layout?offset=&len=` (each extent's offset, length, SHA-256 and
  replicas with their data-node addresses, in the order a read should try them); the client then
  fetches the extents from the data nodes itself, in parallel, over the data-node protocol, and
  verifies every checksum. An unreachable node, a checksum mismatch or a layout made stale by a
  concurrent write falls back to reading through the leader. The client needs network access to
  the data nodes; when they require mTLS, `--identity-file` must hold a certificate signed by the
  cluster CA in `--ca-file`, and that identity is also accepted for writes by the data nodes, so
  give it only to trusted clients. Direct reads take read traffic off the leader; on a single host
  they cost an extra round trip (see the table below).
  An erasure-coded extent's layout lists its shards in shard order with each shard's checksum; the
  client reads and verifies the k data shards and joins them, and leaves decoding around a missing
  shard to the leader.

## Data path

- A read fetches all the extents it covers in parallel (up to 8 at once), each from a replica
  chosen from the extent id so the load spreads over every replica; any replica that fails or
  doesn't verify falls back to the next.
- A write that spans several extents writes their replicas in parallel (up to 4 extents at once,
  each to 3 nodes) and installs them all with one metadata commit (`install_extents` /
  `install_file_extents`), so a 4 MiB write costs one Raft round trip, not four. All placements of
  one write draw from one copy of the free list, so concurrent placements never reuse the same
  free range.
- Each data-node client keeps up to 8 idle connections, so concurrent requests to one node don't
  queue behind a single socket; payloads over 16 KiB are written straight from the caller's
  buffer.
- The node HTTP server sets `TCP_NODELAY` and sends small responses in one write. Before it did,
  every small reply waited for the client's delayed ACK (40 ms on Linux), which capped random
  reads at about 24/s and sequential writes at 18 MiB/s regardless of hardware.
- Upgrades: the two batched commands are new log entries, so upgrade every metadata node before
  clients write through an upgraded leader; an older node cannot apply them.

## Consistency

- Within one mount, operations are linearizable: every call goes through the leader and returns
  only after its command is committed and applied.
- Across mounts it is **close-to-open**: data written on one client is visible to another after the
  writer closes or fsyncs the file and the reader opens it after that; attributes and names may be
  cached for up to `--ttl-ms` (set 0 for no caching). Unlocked concurrent writers to the same
  file range see last-writer-wins at the extent level.
- **Cache leases** (`--cache-leases`): attributes and names are cached for as long as the
  cluster's lease on them lasts (5 s, re-taken on the next miss) instead of for `--ttl-ms`, and
  never go stale: before any change to an inode commits, the leader recalls every other mount's
  lease on it and waits until that mount has dropped its cache (its recall thread answers within
  a round trip) or the lease ran out. A mount's own changes do not wait on its own leases. The
  kernel gets a zero TTL, so every `stat` and lookup reaches the mount's cache. A new metadata
  leader knows none of its predecessor's leases: while a caching session opened under an earlier
  leader is open, it holds changes back for one lease period (5 s) after taking over. A mount
  that stops answering recalls delays other mounts' changes to what it cached by up to 5 s. File
  data is still close-to-open.
- Unlink-while-open only protects handles in the same mount; a file removed by another client
  disappears for everyone.
- **Extended attributes** in the `user.`, `trusted.` and `security.` namespaces are stored on the
  inode (replicated, copied by snapshots and clones): at most 64 KiB per value and 256 KiB per
  inode. `system.*` (POSIX ACLs) is refused with EOPNOTSUPP rather than stored unenforced.
- **Locks**: `fcntl` byte-range locks and `flock` locks are held in the cluster, so every mount
  of the filesystem sees them. A mount opens a session on its first lock and renews it every
  third of `--session-ttl-ms`; the session holds its locks. Closing a file drops its process's
  locks on it, unmounting closes the session, and a mount that dies (or loses the cluster for a
  whole TTL) has its session expired by the metadata leader and its locks released. A mount whose
  session expired under it logs the loss and starts a new session on its next lock. `flock`
  reaches the cluster as a whole-file lock, as on Linux NFS, so it conflicts with `fcntl` locks
  on the same file. `F_SETLKW` polls the leader (10–250 ms backoff) while it waits; at most
  `--fuse-threads` − 1 waits run at once, more fail with ENOLCK, so waiters never take the
  kernel worker an unlock needs. A snapshot mount grants every lock locally (nothing writes
  there). Locks need every metadata node upgraded first: an older node cannot apply them.
- Not implemented: POSIX ACLs, quotas, `O_DIRECT`.

## Atlas gateway

With the native backend enabled (`docs/NATIVE_NODE.md`, "Atlas gateway"):

```
atlasctl create-volume team-share --kind filesystem --backend bkd_native --tenant acme
atlasctl snapshot-volume vol_native_fs_<id> --name before-upgrade
atlasctl clone-snapshot snap_native_fs_<id> --name team-share-copy
atlasctl restore-snapshot snap_native_fs_<id>
```

Filesystems appear as volumes `vol_native_fs_<id>` (kind `filesystem`, backend id `fs:<id>`) and
their snapshots as `snap_native_fs_<id>`. A filesystem has no size limit: its `size_bytes` is the sum
of its file sizes (0 when empty), expanding it is refused, and the requested size at create time is
only used for tenant quota admission. Block data routes (`/volumes/{id}/data`) refuse filesystems;
mount them with `atlas-native-mount` instead.

## Limits and performance

Each filesystem lives in one Raft group's catalog (`metadata.groups` spreads filesystems across
groups; `docs/NATIVE_NODE.md`, "Metadata groups"). Commands apply in place: every
command checks its inputs before it changes anything, so a rejected one leaves the catalog as it
was (debug builds assert this on every apply), and the leader validates proposals against one
running copy of the catalog plus its uncommitted entries. A create therefore costs the same at
100k inodes as at 10k. Log compaction (every 1024 entries) checkpoints only the records changed
since the last one to `catalog.redb` (`docs/NATIVE_METADATA.md`, "Catalog store"). Inodes and
directory entries are paged from it, so memory holds what changed since the last checkpoint plus
a bounded inode cache, not the namespace. The leader's applied and speculative catalogs share
everything paged.

Engine-level create rates without FUSE or HTTP (`crates/atlas-native/tests/metadata_bench.rs`, one
host, tmpfs) are ~9k/s from one proposer and ~12k/s from eight on a 3-voter group at 20k files.
The table below predates pipelined replication and the catalog store.

Measured 2026-10-04, file creates through the node API with 16 concurrent clients on a 3-node
cluster on one laptop (Apple SSD, release build, other load on the machine), all filesystems in
one cluster counted:

| Inodes | Creates/s | p50 ms | p99 ms | Catalog MiB | Leader RSS MiB |
| --- | --- | --- | --- | --- | --- |
| 10k | 126 | 123 | 296 | 2.2 | 168 |
| 50k | 156 | 69 | 338 | 11.9 | 356 |
| 80k | 263 | 55 | 166 | 19.0 | 367 |
| 100k | 271 | 54 | 174 | 23.6 | 628 |

No requests were retried or failed; the spread between rows is load from other processes on the
laptop, not the inode count. Before commands applied in place, each create copied the catalog and
throughput fell from 200/s at 10k inodes to 36/s at 60k. On the shared lab host, where fsync
takes tens of milliseconds and other tenants keep I/O pressure around 15%, creates are bound by
disk latency but stay flat with the inode count (same run shape, no retries):

| Inodes | Creates/s | p50 ms | p99 ms | Catalog MiB | Leader RSS MiB |
| --- | --- | --- | --- | --- | --- |
| 5k | 40 | 362 | 1103 | 1.2 | 41 |
| 15k | 47 | 321 | 811 | 3.4 | 99 |
| 25k | 42 | 346 | 1214 | 5.8 | 112 |

These runs predate the paged catalog, when leader memory grew about 6 KiB per inode. Memory now
follows the inode cache rather than the inode count: `store::bench::memory_of_a_paged_catalog`
(4096-inode cache, second lab host, 2026-10-05) peaks at ~76 MB for 400k files in one directory
and ~78 MB spread over 1000, flat from 300k files on as redb's page cache fills (112–114 MB before
directory entries were paged).

Data path, FUSE mount, measured 2026-10-04: 3 nodes on one 12-core host (other tenants' load
average around 25), node data on tmpfs so the shared HDDs don't mask software overhead, fio with
one job and `psync` on a 256 MiB file, page cache dropped before each read test, release builds,
two rounds each (ranges shown):

| | Before | After | After, `--direct-reads` |
| --- | --- | --- | --- |
| 1 MiB sequential write | 18 MiB/s | 379 MiB/s | 348–350 MiB/s |
| 1 MiB sequential read | 328–374 MiB/s | 369–405 MiB/s | 483–541 MiB/s |
| 4 KiB random read | 23 IOPS | 588–606 IOPS | 510–519 IOPS |

Most of the write and random-read gain is the `TCP_NODELAY` fix (see Data path); the rest is
group commit and parallel extent I/O. A 4 KiB random read still fetches and checksums its whole
extent (1 MiB by default). On the lab hosts' shared disks the same runs are bound by fsync latency
and other tenants' I/O (pressure 50–80%), so they are not a fair measure of either build.
`crates/atlas-native/tests/datapath_bench.rs` (`--ignored`) measures the engine alone over
localhost data nodes.

Known limits:

- One filesystem never spans groups: its inodes all live in one group, behind one leader.
- Filesystem snapshot trees are held in memory in full, and each is one record in the store.
- A write that covers part of an extent reads, merges and rewrites the whole extent: up to the
  filesystem's `extent_bytes` (1 MiB by default). With the cluster on its default 4 MiB grid,
  random 4 KiB writes measured 24/s at a 4 MiB file grid, 28/s at 1 MiB, 35/s at 256 KiB and
  42/s at 64 KiB (p50 40 → 22 ms); the remaining ~20 ms is the replicated commit. A smaller grid
  means more extents in the in-memory catalog, so use 64–256 KiB only for random-write-heavy
  filesystems.
- The client reuses connections (one per concurrent request) but does not pipeline or batch
  requests, so each metadata operation is one round trip to the leader.
- A read verifies whole extents, so a small random read transfers and checksums a full extent.
- Writes always go through the leader; only reads can go direct.
- Unlink-while-open works only within one mount (see Consistency).

## Verification

- `crates/atlas-native/src/namespace.rs` unit tests (every command, idempotent replay, rename edge
  cases, truncate refcounts, snapshot and clone isolation) and `crates/atlas-native/tests/fs.rs`
  (engine: holes, EOF, truncate, GC, reopen, clones).
- `crates/atlas-native/tests/node.rs`: the file API on a 3-node in-process cluster.
- `crates/atlas-native-fuse/tests/ops.rs`: the FUSE operations layer against a live in-process
  cluster, including a leader failure mid-workload, unlink-while-open, read-only snapshot mounts,
  clone isolation, concurrent creates, direct reads (across extents, after a rewrite) and their
  fallback to the leader; `crates/atlas-native-fuse/src/dirs.rs` unit tests page a 2500-entry
  listing through small kernel buffers with one request per page, and rewind on a seek back.
- `crates/atlas-native/tests/allocator.rs`: one write's concurrent extent placements reuse
  distinct free ranges (verified by reading back and scrubbing every replica).
- `crates/atlas-driver-native/tests/nodes.rs` and `crates/atlas-gateway/tests/native_backend.rs`:
  filesystem create, snapshot, clone, restore and delete through the driver and the gateway.
- Lab (2026-10-04), a 3-node cluster on one host with a FUSE mount: pjdfstest `unlink`, `mkdir`,
  `rmdir`, `rename`, `link`, `symlink`, `mknod`, `mkfifo`, `truncate`, `chmod`, `chown` and `open`
  all pass (8565 tests); cp/mv/rm round trips; `git clone` + `git fsck --full --strict`; a snapshot
  mount stayed identical (sha256) while the source was overwritten, truncated and extended, refused
  writes with EROFS, and a clone of it took writes that neither the source nor the snapshot saw.
  `atlasctl` created, snapshotted, cloned, restored and deleted a filesystem through a gateway in
  real mode against a live cluster.
