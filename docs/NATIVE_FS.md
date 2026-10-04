<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native filesystems

An atlas-native cluster (`docs/NATIVE_NODE.md`) serves POSIX filesystems next to its block volumes.
Files and directories live in the same replicated catalog as volumes, and file data uses the same
copy-on-write extents (3 replicas, `extent_bytes` grid). Clients mount a filesystem through FUSE
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
- File data is written through the extent path the volumes use: a write covering part of an extent
  rewrites that extent with the old bytes merged in; never-written ranges are holes and read as
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
| `GET /v1/fs` | `{"filesystems": [{id, name, inodes, bytes, source_snapshot}]}`. |
| `POST /v1/fs` | `{"name", "id"?}` → 201 `{"id"}`. Idempotent with the same `id` and name. |
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
| `GET /v1/fs/{fs}/inodes/{dir}/entries` | `{"entries": [{name, ino, kind}]}`. |
| `POST /v1/fs/{fs}/inodes/{dir}/entries` | `{name, op_id, kind, mode, uid, gid, target?, rdev?}` (`kind`: `file`, `dir`, `symlink`, `fifo`, `socket`, `char_device`, `block_device`) → 201 attributes. |
| `POST /v1/fs/{fs}/inodes/{dir}/unlink`, `/rmdir` | `{name}` → 204. |
| `POST /v1/fs/{fs}/inodes/{ino}/links` | `{parent, name}`: a hard link → attributes. |
| `GET /v1/fs/{fs}/inodes/{ino}/target` | `{"target"}` of a symlink. |
| `GET /v1/fs/{fs}/inodes/{ino}/data?offset=N&len=M` | Raw bytes; short at end of file. |
| `PUT /v1/fs/{fs}/inodes/{ino}/data?offset=N` | Raw body written at `offset` (extends the file) → attributes. |

`{fs}` may be `<fs>@<snapshot>` for reads; any change to a snapshot gets 409 `read_only`.

Errors are `{"error", "code", "leader"}`. `code` is stable and maps to an errno in the client:
`not_found` (404, ENOENT), `exists` (EEXIST), `not_empty` (ENOTEMPTY), `not_dir` (ENOTDIR),
`is_dir` (EISDIR), `invalid` (EINVAL), `read_only` (EROFS), `busy` (EBUSY) — all 409 except where
noted — plus `not_leader` (421), `unavailable` (503) and `internal` (500).

Reads are served by the leader once it has applied its log as of the request, so a client always
sees its own writes; `?stale=1` on a `GET` reads whatever the receiving replica has applied.
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
`--retry-secs` (default 30), `--read-only`, `--allow-other`. It runs in the foreground until
`fusermount3 -u`; the mount uses `default_permissions`, so the kernel checks modes and ownership.

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

## Consistency

- Within one mount, operations are linearizable: every call goes through the leader and returns
  only after its command is committed and applied.
- Across mounts it is **close-to-open**: data written on one client is visible to another after the
  writer closes or fsyncs the file and the reader opens it after that; attributes and names may be
  cached for up to `--ttl-ms` (set 0 for no caching). There are no leases or byte-range locks, so
  concurrent writers to the same file range see last-writer-wins at the extent level.
- Unlink-while-open only protects handles in the same mount; a file removed by another client
  disappears for everyone.
- Not implemented: POSIX ACLs, extended attributes, quotas, `flock`/`fcntl` locks, `O_DIRECT`.

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

The whole namespace lives in one Raft group's in-memory catalog. Commands apply in place: every
command checks its inputs before it changes anything, so a rejected one leaves the catalog as it
was (debug builds assert this on every apply), and the leader validates proposals against one
running copy of the catalog plus its uncommitted entries. A create therefore costs the same at
100k inodes as at 10k; the catalog is still written whole at each log compaction (every 1024
entries, amortised) and the leader holds two copies of it in memory.

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

The practical limit is now memory: about 6 KiB
of leader RSS per inode, so plan on roughly 1M inodes per 8 GiB node.

Data path, lab FUSE mount (3 nodes on one host, fio, one job, `psync`): 1 MiB sequential write
12.5 MiB/s, sequential read 72.5 MiB/s cold, 4 KiB random read about 94 IOPS.

Known limits:

- One Raft group and one leader serve all metadata; there is no namespace sharding.
- The catalog lives in memory (twice on the leader) and is written whole to disk at each log
  compaction.
- A write that covers part of an extent reads, merges and rewrites the whole extent
  (`extent_bytes`, 4 MiB by default), so small random writes are expensive.
- The client opens a new connection per request; there is no pipelining or request batching.
- Unlink-while-open works only within one mount (see Consistency).

## Verification

- `crates/atlas-native/src/namespace.rs` unit tests (every command, idempotent replay, rename edge
  cases, truncate refcounts, snapshot and clone isolation) and `crates/atlas-native/tests/fs.rs`
  (engine: holes, EOF, truncate, GC, reopen, clones).
- `crates/atlas-native/tests/node.rs`: the file API on a 3-node in-process cluster.
- `crates/atlas-native-fuse/tests/ops.rs`: the FUSE operations layer against a live in-process
  cluster, including a leader failure mid-workload, unlink-while-open, read-only snapshot mounts,
  clone isolation and concurrent creates.
- `crates/atlas-driver-native/tests/nodes.rs` and `crates/atlas-gateway/tests/native_backend.rs`:
  filesystem create, snapshot, clone, restore and delete through the driver and the gateway.
- Lab (2026-10-04), a 3-node cluster on one host with a FUSE mount: pjdfstest `unlink`, `mkdir`,
  `rmdir`, `rename`, `link`, `symlink`, `mknod`, `mkfifo`, `truncate`, `chmod`, `chown` and `open`
  all pass (8565 tests); cp/mv/rm round trips; `git clone` + `git fsck --full --strict`; a snapshot
  mount stayed identical (sha256) while the source was overwritten, truncated and extended, refused
  writes with EROFS, and a clone of it took writes that neither the source nor the snapshot saw.
  `atlasctl` created, snapshotted, cloned, restored and deleted a filesystem through a gateway in
  real mode against a live cluster.
