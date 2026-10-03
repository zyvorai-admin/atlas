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
| `tls` | none | Mutual TLS for the Raft and data-node transports (not the HTTP endpoint). Node ids must then be DNS names present as SANs on each node's certificate. |
| `max_request_bytes` | 64 MiB | Larger HTTP bodies get 413. |
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
| `GET /readyz` | no | 200 once a metadata leader is known (metadata role) or the data node is serving; 503 otherwise, including after a fatal storage error. |
| `GET /metrics` | no | Prometheus text: Raft/transport, engine, data node, and `atlas_native_{repair,gc}_{runs,errors}_total`. |
| `GET /v1/status` | yes | Raft role/term/leader/indexes, per-data-node health, last repair result, data-node fence. |
| `GET /v1/volumes` | yes | Volumes in the applied catalog. |
| `POST /v1/volumes` | yes | `{"name": "...", "size_bytes": N}` → 201 `{"id": "..."}`. |
| `DELETE /v1/volumes/{id}` | yes | 204. |
| `PUT /v1/volumes/{id}/data?offset=N` | yes | Raw body written at `offset` (split into extents) → 204. |
| `GET /v1/volumes/{id}/data?offset=N&len=M` | yes | Raw bytes. `offset` must be an extent start and `len` at most one extent. |
| `POST /v1/volumes/{id}/snapshots` | yes | `{"name": "..."}` → 201 `{"id": "..."}`. |
| `DELETE /v1/snapshots/{id}` | yes | 204. |
| `GET /v1/snapshots/{id}/data?offset=N&len=M` | yes | Raw bytes from the snapshot. |
| `POST /v1/repair`, `POST /v1/gc` | yes | Run one pass now (leader only) and return its stats. |

Errors are JSON `{"error": "...", "leader": ...}`. A mutation sent to a follower returns **421** with
the leader's node id in `leader` (null while unknown). Status codes: 400 bad request, 401 missing or
invalid token, 404 unknown volume/snapshot/route (or no metadata role), 409 rejected by the state
machine, 413 body too large, 503 retryable (not enough data nodes, leadership changed, timeout).

The HTTP server is deliberately small: one request per connection, `Content-Length` bodies only
(chunked requests get 411), 16 KiB of headers, 30 s socket timeouts, no TLS (terminate TLS in front
of it if it leaves the host).

## Operations

- **Maintenance**: the leader runs `repair_once` (scrub every replica, re-replicate missing or
  corrupt ones) and `gc_once` (reclaim unreferenced extents) on their intervals. Each pass is a full
  scan; size the repair interval to the data volume.
- **Failure**: a data node that fails I/O is backed off for 5 s and writes move to the next eligible
  node; reads fall back to other replicas. Losing the metadata leader triggers an election
  (typically well under a second with the default tick) and clients retry against the new leader.
- **Restart**: all Raft state and data are crash-safe on disk, so stopping the process (any signal)
  needs no graceful path. A fatal storage error inside the Raft replica (e.g. a failed fsync) makes
  the process exit non-zero so its supervisor restarts it from disk.
- **Logging**: startup prints the bound addresses to stderr; everything else is in `/metrics` and
  `/v1/status`.

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

## Smoke test

`deploy/native/smoke.sh [path/to/atlas-native-node]` starts three metadata and three data node
processes on localhost, writes and reads a block through the leader with `curl`, kills the leader,
and checks the new leader still serves the block and accepts writes.

`crates/atlas-native/tests/node.rs` runs the same topology in-process: auth, readiness, follower
421 with the leader hint, volume/snapshot round trips, request validation, metrics, background
repair after losing a data node, and config validation.

## Not implemented yet

- TLS or client-certificate auth on the HTTP endpoint (use a token plus a private network, or a
  TLS-terminating proxy);
- cross-extent reads and unaligned I/O in the volume API;
- Raft membership changes (the voter set is fixed by config);
- a Helm chart and gateway integration (the raw manifest above is the only deployment).
