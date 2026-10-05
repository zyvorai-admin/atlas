<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native-node

`atlas-native-node --config <file.json>` runs one native storage node (`crates/atlas-native`,
`node` module). A node runs a **data node** (serves one or more local devices to the cluster), a
**metadata replica** (Raft + `NativeEngine`), or both, and always serves an HTTP endpoint for
health, metrics, status, block volumes and POSIX filesystems. See `docs/NATIVE_METADATA.md` for the
storage design behind it, `docs/NATIVE_FS.md` for filesystems and the `atlas-native-mount` FUSE
client, and "Atlas gateway" below for how the gateway drives it.

## Roles and topology

- **Data nodes** serve `data_dir/data/nvme0.data` by default, or the devices listed in
  `data_node.devices` (see "Devices" below). Their `node_id` is the id the metadata replicas use
  for them (and their TLS server name).
- **Metadata replicas** form one Raft group (3 or 5 voters), or several sharing the same voters
  (`metadata.groups`), and all list the same `data_nodes`.
  Only the leader accepts mutations; any replica serves reads from its applied state.

A typical small cluster runs metadata and data roles on the same three hosts, or three metadata
nodes plus N data nodes. `replicas` must not exceed the number of data nodes; with one spare data
node, writes keep working and repair can re-replicate when a data node dies.

## Config

Unknown fields are rejected. A combined node (`m1` running both roles):

```json
{
  "node_id": "m1",
  "data_dir": "/var/lib/atlas-native",
  "http_listen": "127.0.0.1:7480",
  "api_token_file": "/etc/atlas-native/token",
  "tls": {
    "ca": "/etc/atlas-native/ca.pem",
    "cert": "/etc/atlas-native/m1.pem",
    "key": "/etc/atlas-native/m1-key.pem"
  },
  "data_node": { "listen": "10.0.0.1:7481" },
  "metadata": {
    "listen": "10.0.0.1:7482",
    "peers": { "m1": "10.0.0.1:7482", "m2": "10.0.0.2:7482", "m3": "10.0.0.3:7482" },
    "data_nodes": [
      { "id": "m1", "addr": "10.0.0.1:7481", "rack": "r1" },
      { "id": "m2", "addr": "10.0.0.2:7481", "rack": "r2" },
      { "id": "m3", "addr": "10.0.0.3:7481", "rack": "r3" }
    ],
    "replicas": 3,
    "extent_bytes": 4194304,
    "repair_interval_secs": 300,
    "gc_interval_secs": 60
  }
}
```

`${NAME}` anywhere in the file is replaced with the environment variable `NAME` before parsing; an
unset variable or an unterminated `${` is a config error. `metadata.peers` may list every voter
including this node (its own entry is ignored), so one file can be shared by all members. Peer and
data-node addresses are `host:port` and resolved on every connect, so DNS names that move to a new IP
(a rescheduled pod) keep working.

| Field | Default | Notes |
| --- | --- | --- |
| `api_token_file` | none | Bearer token required on every `/v1/*` request. Without it `/v1/*` is open: keep `http_listen` on localhost or a private network. |
| `http_tls` | none | `{"cert", "key", "client_ca"?}`: serve the HTTP API over TLS. With `client_ca`, every `/v1/*` request must also present a client certificate signed by it (401 otherwise; a certificate from another CA fails the handshake), while `/healthz`, `/readyz` and `/metrics` stay open to probes and scrapers. |
| `tls` | none | Mutual TLS for the Raft and data-node transports (not the HTTP endpoint). Node ids must then be DNS names present as SANs on each node's certificate. |
| `max_request_bytes` | 64 MiB | Larger HTTP bodies and read lengths get 413. |
| `metadata.bootstrap` | every `peers` entry | Initial Raft voters, used until the first membership change commits. A node not listed starts as a non-voter (it never campaigns) and waits to be added through `POST /v1/members`. Every id needs a `peers` entry. |
| `metadata.replicas` | 3 | Between 1 and the number of `data_nodes`. |
| `metadata.erasure` | unset | Reed-Solomon scheme `k+m` (e.g. `4+2`, `8+3`; k up to 32, m up to 8) for new extents; unset keeps replicating. Needs at least k+m data nodes; see [Erasure coding](#erasure-coding). |
| `metadata.erasure_min_bytes` | 64 KiB | Smaller extents (file tails, small files) are still replicated. |
| `metadata.extent_bytes` | 4 MiB | Writes are split into extents of this size. |
| `metadata.tick_ms` | 50 | Raft tick; elections take 10–20 ticks. |
| `metadata.proposal_timeout_ms` | 5000 | Bounds leader readiness, each proposal and each data-node I/O. |
| `metadata.repair_interval_secs` | 300 | Leader-only scrub/repair loop; 0 disables. |
| `metadata.gc_interval_secs` | 60 | Leader-only GC loop; 0 disables. |
| `metadata.groups` | 1 | Raft groups the namespace is sharded across (1–64), all on `metadata.listen`; see [Metadata groups](#metadata-groups). Every metadata node must use the same value. It can be raised later but never lowered. |
| `data_nodes[].host` / `rack` / `zone` | `id` / `id` / empty | Failure domains for placement; replicas always land on distinct hosts. |
| `data_nodes[].free_bytes` | 1 TiB | Placement capacity hint. |
| `data_nodes[].devices` | 1 | How many devices that data node serves; new replicas are striped across them round-robin. |
| `data_node.devices` | `[]` | `[{"path", "backend"?}]`, served in index order. Empty: one `file` device at `data_dir/data/nvme0.data`. |

### Devices

A data node can serve several devices, for example one per NVMe drive:

```json
"data_node": {
  "listen": "10.0.0.1:7481",
  "devices": [
    { "path": "/dev/nvme0n1", "backend": "io_uring" },
    { "path": "/dev/nvme1n1", "backend": "io_uring" }
  ]
}
```

The metadata replicas must then list `"devices": 2` for that data node. A replica records which
device it is on, so device order must not change once data is written.

| `backend` | Access | Notes |
| --- | --- | --- |
| `file` (default) | Buffered I/O on a regular file, `fsync` per write | What every data node used before devices were configurable. |
| `aligned` | `pread`/`pwrite` with `O_DSYNC` on 4 KiB-aligned buffers | Regular file or block device. Appends start on a 4 KiB boundary; unaligned overwrites read-modify-write the edge blocks. |
| `io_uring` | `O_DIRECT` + `O_DSYNC` through `io_uring`, bypassing the page cache | Linux only, and only in builds with the `atlas-native/io-uring` feature (otherwise the node refuses to start). Same layout as `aligned`. |

A block device has no file length to say how much of it is in use, so `aligned` and `io_uring`
persist a high-water mark in `data_dir/data/dev<index>/<name>.hwm`, raised 256 MiB at a time
before space past it is handed out. Losing that file on a block device loses track of the data on
it; on a regular file the file length is used when the mark is missing. Switching an existing
`file` device to `aligned` or `io_uring` keeps its data: its extents need not be aligned, and new
appends start at the next 4 KiB boundary.

Upgrade order: upgrade the data nodes first, then raise `data_nodes[].devices` on the metadata
replicas. A client asking for device 1 or above first asks the data node how many devices it
serves; an older data node doesn't understand the question, so the request fails instead of
silently landing on device 0. Requests for device 0 are unchanged on the wire.

### Erasure coding

With `"erasure": "4+2"` in the `metadata` section, each extent of at least `erasure_min_bytes` is
split into 4 data shards and 2 Reed-Solomon parity shards, each on a different data node (and so a
different host), so a 4 MiB extent occupies 6 MiB instead of the 12 MiB three replicas take. Any
2 shards can be lost. Each shard carries its own SHA-256 next to the extent's.

- **Reads** fetch the data shards, preferring nodes that are up. A shard that fails its checksum or
  can't be read is replaced by a parity shard and the missing data is decoded.
- **Repair** (the scrub loop) reads every shard, rebuilds the missing or corrupt ones and writes
  each to a node holding no other shard of that extent. With more than m shards gone the extent is
  reported unrecoverable, as with replicas.
- **Writes** need a node per shard; a node that fails is swapped for a spare. With fewer than k+m
  nodes up, the write fails rather than storing a weaker layout.
- **Existing data** stays as written: changing or removing `erasure` affects new extents only, and
  coded and replicated extents can be mixed in one file. Clients that read data nodes directly
  (`docs/NATIVE_FS.md`) read the data shards and fall back to the leader when any is missing.

Measured codec speed on one core (4 MiB extents, release build): about 1.5 GiB/s to encode
(checksums included) and 2 GiB/s or more to rebuild two lost data shards, for both 4+2 and 8+3.

## HTTP API

| Method and path | Auth | Description |
| --- | --- | --- |
| `GET /healthz` | no | Process is up. |
| `GET /readyz` | no | 200 once every metadata group has a known leader (metadata role; a non-voter waiting to be added counts as ready) or the data node is serving; 503 otherwise, including after a fatal storage error. |
| `GET /metrics` | no | Prometheus text: Raft/transport, engine (including `atlas_native_client_sessions` and `atlas_native_file_locks`), data node, `atlas_native_{repair,gc}_{runs,errors}_total`, `atlas_native_client_sessions_expired_total` and `atlas_native_cache_leases{group}` (cache leases this node holds out as a group leader). |
| `GET /v1/status` | yes | Raft role/term/leader/indexes and voter flag (`metadata`, group 0; `metadata_groups` lists `{group, leader, ...}` for every group), `layout` (`extent_bytes`, `replicas`), per-data-node health, last repair result, data-node fence. |
| `GET /v1/volumes` | yes | Volumes in the applied catalog. |
| `POST /v1/volumes` | yes | `{"name": "...", "size_bytes": N, "id"?: "..."}` → 201 `{"id": "..."}`. An optional client-chosen `id` (1–64 of `[A-Za-z0-9-]`) makes the create idempotent: repeating it with the same parameters is a no-op, different parameters get 409. |
| `DELETE /v1/volumes/{id}` | yes | 204. |
| `POST /v1/volumes/{id}/resize` | yes | `{"size_bytes": N}` → 200. Grow only; a smaller size gets 409 (it would drop written data). |
| `PUT /v1/volumes/{id}/data?offset=N` | yes | Raw body written at any `offset` → 204. Extents sit on a fixed `extent_bytes` grid; a write covering part of an extent rewrites that extent with the old bytes merged in. |
| `GET /v1/volumes/{id}/data?offset=N&len=M` | yes | Raw bytes from any range within the volume, across extents; never-written bytes read as zeros. `len` above `max_request_bytes` gets 413. |
| `POST /v1/volumes/{id}/snapshots` | yes | `{"name": "...", "id"?: "..."}` → 201 `{"id": "..."}` (`id` as for volumes). |
| `POST /v1/snapshots/{id}/clone` | yes | `{"name": "...", "size_bytes"?: N, "id"?: "..."}` → 201 `{"id": "..."}`: a new volume sharing the snapshot's extents (copy-on-write; it survives deleting the snapshot and its source). `size_bytes` defaults to the snapshot's size and may only be larger. |
| `DELETE /v1/snapshots/{id}` | yes | 204. |
| `GET /v1/snapshots/{id}/data?offset=N&len=M` | yes | Same as the volume read, against the snapshot. |
| `GET /v1/members` | yes | `{"membership": {"type": "stable", "voters": [...]}, "addrs": {id: "host:port"}}` (`type` is `joint` with `old`/`new` mid-change); `addrs` are the Raft addresses learned from membership changes; `groups` lists `{group, membership}` for every metadata group. |
| `POST /v1/members` | yes | `{"voters": {"<id>": "<host:port>", ...}}`: move to exactly this voter set (leader only) and return once the final configuration has committed. 409 while another change is in flight. With several groups each node changes the groups it leads and answers 421 until every group has the new set, so repeat it across the metadata nodes until it returns 200. |
| `POST /v1/repair`, `POST /v1/gc` | yes | Run one pass now over the groups this node leads (421 if it leads none) and return the summed stats. |
| `/v1/fs/...`, `/v1/fs-snapshots/...` | yes | Filesystems, inodes, file data and filesystem snapshots/clones: see `docs/NATIVE_FS.md`. |

Errors are JSON `{"error": "...", "code": "...", "leader": ...}`; `code` is a stable machine-readable
reason (`not_found`, `exists`, `invalid`, `read_only`, `not_leader`, `unavailable`, …). A mutation sent
to a follower returns **421** with the leader's node id in `leader` (null while unknown). Status codes: 400 bad request, 401 missing or
invalid token, 404 unknown volume/snapshot/route (or no metadata role), 409 rejected by the state
machine, 413 body too large, 503 retryable (not enough data nodes, leadership changed, timeout).

The HTTP server is deliberately small: HTTP/1.1 persistent connections (closed on
`Connection: close`, HTTP/1.0, a malformed request or 30 s idle — keep client pools below that),
`Content-Length` bodies only (chunked requests get 411), 16 KiB of headers, 30 s socket timeouts,
optional TLS (`http_tls`). A small request on a reused connection costs about 60 µs locally; a new
connection per request costs about 7 ms.

## Operations

- **Maintenance**: the leader runs `repair_once` (scrub every replica, re-replicate missing or
  corrupt ones) and `gc_once` (reclaim unreferenced extents) on their intervals. Each pass is a full
  scan; size the repair interval to the data volume.
- **Dead connections**: a peer that vanishes without closing its sockets (a deleted pod, a
  powered-off host) is detected by its Raft senders: a connection is replaced when the peer has
  answered none of our requests for `max(40 ticks, 1 s)` or has reconnected to us since (it
  restarted). Counted in `atlas_native_transport_stale_reconnects_total`.
- **Failure**: a data node that fails I/O is backed off for 5 s and writes move to the next eligible
  node; reads fall back to other replicas. Losing the metadata leader triggers an election
  (typically well under a second with the default tick) and clients retry against the new leader.
- **Durability**: a follower fsyncs its log once per batch of AppendEntries, before acknowledging
  any of it; the leader writes proposals in parallel with replicating them and counts itself towards
  a quorum only for entries it has fsynced, so concurrent proposals share fsyncs (group commit). The
  catalog is written to disk only when the log is compacted: once 2048 entries have been applied
  since the last snapshot, everything but the newest 1024 is folded into it, so a briefly lagging
  follower still catches up from the log rather than a full catalog transfer. A restart replays
  the log since the last snapshot. A follower too far behind gets one InstallSnapshot per election
  timeout until it answers.
- **Slow disks**: the Raft core holds its lock while it fsyncs, so a disk whose fsyncs take hundreds
  of milliseconds can delay heartbeats past the election timeout (10–20 ticks) under write load and
  cause needless elections (clients retry through them). On such disks raise `metadata.tick_ms`
  (e.g. 200 for 2–4 s elections).
- **Restart**: all Raft state and data are crash-safe on disk, so stopping the process (any signal)
  needs no graceful path. A fatal storage error inside the Raft replica (e.g. a failed fsync) makes
  the process exit non-zero so its supervisor restarts it from disk.
- **Logging**: startup prints the bound addresses to stderr; everything else is in `/metrics` and
  `/v1/status`.

## Membership changes

The voter set lives in the replicated log. `POST /v1/members` on the leader moves it to a new set by
joint consensus: the leader appends a `joint` configuration (old + new; every election and commit
needs a majority of **both**), and once that commits it appends the `stable` new set. Each node
switches configuration as soon as the entry reaches its log, so there is no window with two
independent majorities. Only one change runs at a time; the request returns after the final entry
commits (bounded by `6 × proposal_timeout_ms`).

- **Adding** a node: start it with `metadata.bootstrap` set to the current voters (so it does not
  count itself in) and `peers` listing them; it waits as a non-voter. Then post the full new voter set
  including its address. The leader replicates the log (or a snapshot) to it as part of the change.
- **Removing** a node, including the leader: post the set without it. A leader that removes itself
  steps down once the final entry commits and the remaining voters elect a new leader. A removed
  follower may never receive the final entry; it cannot disrupt the new group (its pre-votes need a
  majority of the new set too) and should simply be shut down.
- Change one voter at a time where possible and keep the voter count odd. Data placement
  (`data_nodes`) is separate from Raft membership: removing a data node from the config makes repair
  re-replicate its extents onto the remaining nodes (`POST /v1/repair` to run it now).

## Metadata groups

`metadata.groups: N` shards the namespace across N Raft groups. Every metadata node runs a replica
of every group over its one `metadata.listen` port (frames carry their group), so the groups share
voters but elect leaders independently and spread leadership, commits and catalog memory across
the nodes. Each group keeps its own log, catalog and free list (`raft`/`engine` for group 0,
`raft-g<N>`/`engine-g<N>` for the rest) and its own fence on every data node.

- **Placement.** A new volume or filesystem goes to its id's home group (FNV-1a of the id, modulo
  N). Snapshots and clones stay in their source's group, because they share its extents. Data nodes
  are shared: each append is allocated by the data node, so groups never coordinate space.
- **Routing.** Any node accepts any request and finds the group holding the id. It checks its local
  catalogs first, and on a miss runs a read barrier on every group (`docs/NATIVE_METADATA.md`, "Read
  barriers") before answering 404, so a lagging replica never misses a committed object. A create
  or clone whose id another group already holds gets 409. Two concurrent creates of the same id
  that land in different groups can both succeed; give every object a unique id.
- **Lists** (`GET /v1/volumes`, `/v1/fs`) merge every group's catalog after a barrier on each.
- **Health.** `/readyz` waits for a leader in every group, and the driver reports a group without
  one as critical. `/metrics` labels engine metrics with `group`.
- **Changing N.** Raising it is safe: existing objects stay where they are and are found by lookup.
  Lowering it would orphan the objects in the removed groups, so a node whose data directory holds
  a group at or above the configured count refuses to start. Every metadata node must use the same
  value.

## Kubernetes

`Dockerfile.native` builds a slim image (`atlas-native-node`, uid 10001). `deploy/k8s/atlas-native.yaml`
runs it as a 3-replica StatefulSet in namespace `atlas-native`, each pod a combined metadata + data
node:

- one shared ConfigMap with `node_id: "${POD_NAME}"` (downward API) and peers/data nodes addressed by
  stable DNS `atlas-native-N.atlas-native.atlas-native.svc.cluster.local` through a headless Service
  with `publishNotReadyAddresses` (members must find each other before any is ready);
- per-pod state on a `volumeClaimTemplate` (5 Gi, default StorageClass), so Raft log, catalog and
  extents survive rescheduling;
- the API token from Secret `atlas-native-api`; ClusterIP Service `atlas-native-api:7480` for clients
  (mutations sent to a follower get 421 with the leader's pod name);
- probes on `/healthz` (startup, liveness) and `/readyz` (readiness), a PDB of `maxUnavailable: 1`,
  non-root, read-only root filesystem, all capabilities dropped.

`scripts/deploy-native-remote.sh <host> [user] [--verify-failover]` builds the image with podman on a
k3s host, tags it by content id, imports it into containerd, creates the token Secret if missing and
applies the manifest with the image pinned to that tag and the pod template stamped with a manifest
hash. Pods therefore roll only when the image or manifest changed; re-running it is a no-op. It then
writes and reads a block through the leader, and with `--verify-failover` deletes the leader pod and
reads the block back from the newly elected one.

### Helm

`deploy/helm/atlas-native` is the production form of the same layout: configurable voter count,
replication factor and engine tuning, an API token Secret (generated and kept across upgrades, or
your own), optional Raft/data mutual TLS (an existing Secret or a cert-manager `Certificate` with
every pod name as a SAN), optional HTTPS with client-certificate auth, and a `ServiceMonitor`. See
its [README](../deploy/helm/atlas-native/README.md). `deploy/native/helm-live-check.sh <repo> <tag>`
installs it with both TLS layers on a throwaway PKI and verifies auth refusals, unaligned I/O,
leader-pod failover and that an unchanged `helm upgrade` restarts nothing.

## Smoke test

`deploy/native/smoke.sh [path/to/atlas-native-node]` starts three metadata and three data node
processes on localhost, writes and reads a block through the leader with `curl`, kills the leader,
and checks the new leader still serves the block and accepts writes.

`crates/atlas-native/tests/node.rs` runs the same topology in-process: auth, readiness, follower
421 with the leader hint, volume/snapshot round trips, request validation, metrics, background
repair after losing a data node, and config validation.

## Atlas gateway

`atlas-driver-native` is the gateway's `StorageDriver` for a native cluster. With
`ATLAS_NATIVE_ENABLE=1` (Helm: `native.enabled` in `deploy/helm/atlas`) the gateway registers
backend `bkd_native`, discovers it at startup and every `ATLAS_MONITOR_INTERVAL_SECS`, and serves:

- inventory: one cluster `cls_native_bkd_native`, one replicated pool `native` (replica size from
  `/v1/status` `layout`), block volumes `vol_native_<native id>` and filesystems
  `vol_native_fs_<native id>` (kind `filesystem`, see `docs/NATIVE_FS.md`); health is critical without a
  metadata leader and warn while a data node is down. Capacity is not reported (the nodes do not
  know their disks' size), so it stays empty instead of being invented;
- `POST /volumes` with `"kubernetes": {"backend_id": "bkd_native"}`: created synchronously through
  the leader (201, no job; `"kind": "filesystem"` makes a filesystem, `atlasctl create-volume
  --backend bkd_native [--kind filesystem]`), recorded under the request's tenant (quota admission and product
  bindings as for any volume); `DELETE /volumes/{id}`, `POST /volumes/{id}/expand`,
  `POST /volumes/{id}/snapshots`, `DELETE /snapshots/{id}` and `POST /snapshots/{id}/clone` /
  `restore` (a new volume from the snapshot, recorded as its dependent so the snapshot can't be
  deleted without `force` while it exists) likewise go straight to the cluster;
- block data: `PUT /volumes/{id}/data?offset=N` (raw body) and
  `GET /volumes/{id}/data?offset=N&len=M` proxy to the node data API for atlas-native volumes
  (operator role and the volume's tenant; at most 4 MiB per request, larger bodies get 413;
  ranges outside the volume get 400; volumes on other backends get 400).

Real mode (`ATLAS_NATIVE_DRIVER_MODE=real`) needs `ATLAS_NATIVE_ENDPOINTS` (comma-separated
`https://pod:7480` URLs of metadata nodes). Mutations are retried across the endpoints until the
leader accepts them (for up to 5 s while every node answers "not the leader"); reads use any
node. The driver picks the ids of new volumes and snapshots, so a create retried after an
ambiguous failure (a 503 or timeout after the proposal) cannot create a second object, and a
retried delete that finds the object gone counts as done. `ATLAS_NATIVE_TOKEN_FILE` is the API token,
`ATLAS_NATIVE_CA_CERT` a private CA for `http_tls`, and `ATLAS_NATIVE_CLIENT_CERT`/`_KEY` a client
certificate for clusters with `client_ca`. Fake mode keeps volumes in memory for demos and tests.

Verified live (2026-10-03) on the k3s lab: a gateway in real mode against a 3-pod Helm release
discovered `bkd_native` (pool `native`, 3 replicas, ok), created a 16 MiB volume (201, visible on
the nodes), showed its used extent after a write, took a snapshot, expanded the volume to 32 MiB,
cloned and restored the snapshot (the clone's bytes matched the snapshot; snapshot delete got 409
while they existed), then deleted everything on the cluster.
A 1 MiB write through the gateway's data route read back identically through the gateway and
straight from the nodes, and a clone of the snapshot served the same bytes through the gateway.
