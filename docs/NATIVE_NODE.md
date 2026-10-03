<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native-node

`atlas-native-node --config <file.json>` runs one native storage node (`crates/atlas-native`,
`node` module). A node runs a **data node** (serves one local device file to the cluster), a
**metadata replica** (Raft + `NativeEngine`), or both, and always serves an HTTP endpoint for
health, metrics, status and a small volume API. See `docs/NATIVE_METADATA.md` for the storage
design behind it.

It is not wired into the Atlas gateway yet; this is the standalone process for running and testing
the native data plane.

## Roles and topology

- **Data nodes** each serve `data_dir/data/nvme0.data`. Their `node_id` is the id the metadata
  replicas use for them (and their TLS server name).
- **Metadata replicas** form one Raft group (3 or 5 voters) and all list the same `data_nodes`.
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
| `metadata.extent_bytes` | 4 MiB | Writes are split into extents of this size. |
| `metadata.tick_ms` | 50 | Raft tick; elections take 10–20 ticks. |
| `metadata.proposal_timeout_ms` | 5000 | Bounds leader readiness, each proposal and each data-node I/O. |
| `metadata.repair_interval_secs` | 300 | Leader-only scrub/repair loop; 0 disables. |
| `metadata.gc_interval_secs` | 60 | Leader-only GC loop; 0 disables. |
| `data_nodes[].host` / `rack` / `zone` | `id` / `id` / empty | Failure domains for placement; replicas always land on distinct hosts. |
| `data_nodes[].free_bytes` | 1 TiB | Placement capacity hint. |

## HTTP API

| Method and path | Auth | Description |
| --- | --- | --- |
| `GET /healthz` | no | Process is up. |
| `GET /readyz` | no | 200 once a metadata leader is known (metadata role; a non-voter waiting to be added counts as ready) or the data node is serving; 503 otherwise, including after a fatal storage error. |
| `GET /metrics` | no | Prometheus text: Raft/transport, engine, data node, and `atlas_native_{repair,gc}_{runs,errors}_total`. |
| `GET /v1/status` | yes | Raft role/term/leader/indexes and voter flag, `layout` (`extent_bytes`, `replicas`), per-data-node health, last repair result, data-node fence. |
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
| `GET /v1/members` | yes | `{"membership": {"type": "stable", "voters": [...]}, "addrs": {id: "host:port"}}` (`type` is `joint` with `old`/`new` mid-change); `addrs` are the Raft addresses learned from membership changes. |
| `POST /v1/members` | yes | `{"voters": {"<id>": "<host:port>", ...}}`: move to exactly this voter set (leader only) and return once the final configuration has committed. 409 while another change is in flight. |
| `POST /v1/repair`, `POST /v1/gc` | yes | Run one pass now (leader only) and return its stats. |

Errors are JSON `{"error": "...", "leader": ...}`. A mutation sent to a follower returns **421** with
the leader's node id in `leader` (null while unknown). Status codes: 400 bad request, 401 missing or
invalid token, 404 unknown volume/snapshot/route (or no metadata role), 409 rejected by the state
machine, 413 body too large, 503 retryable (not enough data nodes, leadership changed, timeout).

The HTTP server is deliberately small: one request per connection, `Content-Length` bodies only
(chunked requests get 411), 16 KiB of headers, 30 s socket timeouts, optional TLS (`http_tls`).

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
  `/v1/status` `layout`), block volumes `vol_native_<native id>`; health is critical without a
  metadata leader and warn while a data node is down. Capacity is not reported (the nodes do not
  know their disks' size), so it stays empty instead of being invented;
- `POST /volumes` with `"kubernetes": {"backend_id": "bkd_native"}`: created synchronously through
  the leader (201, no job), recorded under the request's tenant (quota admission and product
  bindings as for any volume); `DELETE /volumes/{id}`, `POST /volumes/{id}/expand`,
  `POST /volumes/{id}/snapshots`, `DELETE /snapshots/{id}` and `POST /snapshots/{id}/clone` /
  `restore` (a new volume from the snapshot, recorded as its dependent so the snapshot can't be
  deleted without `force` while it exists) likewise go straight to the cluster.

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

## Not implemented yet

- the data path through the gateway (volumes are created and listed via Atlas; block I/O goes to
  the node API directly).
