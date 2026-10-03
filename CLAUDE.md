<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas — Zyvor Storage Control Plane

Atlas is the central storage control plane for the Zyvor product suite. Products (Zeus OS/v9s,
Veyron, Transiva, GuestKit, PacketWolf, Aether, Ragnarok, Machina, HyperSDK) call **Atlas APIs**;
Atlas talks to storage backends through **pluggable drivers**. Ceph is the first driver.

Design authority: `Zyvor_Ceph_Integration_Developer_Implementation_Plan.pdf` (v1.0 engineering draft).

## Current state (slices 1–5 done; verified on real Rook Ceph)
Implemented:
- `atlas-*` cargo workspace, query layer on `sqlx::Any` (`AnyPool`) — SQLite (default, zero-dependency)
  or Postgres (`ATLAS_DATABASE_URL=postgres://...`, HA/multi-replica) selected by URL scheme at
  startup, same SQL/migrations tree logic against both (`migrations/` SQLite,
  `migrations-postgres/` Postgres dialect) — axum 0.8 REST + `tonic` gRPC; async **job engine**.
- **Pluggable backends** behind `StorageDriver`, each with a `fake`/`real` `DriverMode` (fixture-only
  by default, zero external dependency): Ceph (`ceph`/`rbd` CLI), **NFS** (`showmount`/`df`),
  **ZFS** (local `zpool`/`zfs list`) and read-only **Longhorn** — plus a live K8s driver. Real drivers
  never fabricate data; they propagate a real error when the target is unreachable. Discovery worker
  → inventory. **Object storage defaults to Ceph RGW** (`bkd_ceph_lab`, Rook `ObjectBucketClaim`);
  any S3-compatible endpoint (MinIO, Garage, AWS, ...) is usable as a
  bring-your-own backend via the generic `atlas-driver-rgw::S3Target` client — see `docs/RUSTFS.md`
  for the history of Atlas's now-removed first-party RustFS product integration (Helm chart, admin
  proxy, console pages, drive/instance install jobs) and what replaced it. CI gates run on the lab
  host via `scripts/ci-remote.sh` (never locally).
- **Raw disk provisioning** (`docs/DISKS.md`): Disks console page + `GET /zfs/devices`,
  `GET /ceph/nodes/{node}/devices` pickers, `POST /zfs/pools/from-device` (with explicit
  `wipe_existing`) and `POST /ceph/devices`. **ZFS verified live on a real disk** (wiped a stale Ceph
  OSD signature, created a pool). The gateway pod needs `privileged` + hostPath `/dev`, `/run/udev`
  and `/proc/1/mountinfo` for this (`deploy/k8s/atlas-gateway.yaml`); active-pool members, the
  root/boot disk and mounted devices are hard refusals that `wipe_existing` can never override.
  `POST /zfs/pools/{name}/destroy` (Disks page **Destroy…**, typed-name confirm) tears a pool down
  again (verified live: destroyed `tank0`, re-provisioned `sdb` as `tank1`).
- Write path: volumes (PVC + direct RBD), snapshots/clone/restore, CephFS RWX, buckets (Ceph RGW default, BYO S3) + backups
  (`export-diff`→S3, retention, presigned), scheduled snapshots/backups, per-tenant quotas + policies.
- Observability: monitor/alerts + webhook, `/metrics` (Prometheus self), `/metrics/{history,forecast,ceph}`,
  Ceph-native `/ceph/{status,osd-tree,osd-df,df}`, unified `/events`, `/readyz`, OpenTelemetry
  tracing (`docs/TRACING.md`); `deploy/observability/`.
- **AI Ops Advisor** (`docs/AI_ADVISOR.md`): explainable, read-only (`can_execute: false`) risk
  scoring and runbook (`/api/atlas/v1/ai/advisor`, local/auto/LLM modes), correlated incidents
  (`/ai/incidents`, optional LLM-narrated root-cause summary), what-if capacity planning
  (`/ai/what-if`), and median/MAD-based anomaly detection (`/ai/anomalies`) that pauses itself
  rather than scoring on stale/missing telemetry (>15 min old). All four are also exposed as MCP
  tools (`crates/atlas-gateway/src/mcp.rs`, `mcp` feature) for agent-driven ops.
- **React console** embedded in the gateway (HTTPS, login, Observatory, Ceph page, Ops Advisor).
  UI identity: **Soundings** bathymetric system — see `docs/ATLAS_UI_CONTRACT.md`
  and `crates/atlas-gateway/ui/src/atlas-soundings.css`.
- `deploy/rook-ceph-lab/` (single-node overlay + day-2 ops scripts), `deploy/k8s/` ceph deployment,
  `deploy/postgres-lab/`, and a Helm chart (`deploy/helm/atlas/`, `database.kind: sqlite|postgres`)
  for production rollout.
- **DataBridge** (`atlas-databridge`): cloud-to-edge DB migration control plane. **Six source engines**
  — Postgres, MySQL, MariaDB (homogeneous → CNPG/Percona) + Oracle, SQL Server (heterogeneous → Postgres
  edge, seeded by Debezium's initial snapshot) + MongoDB (homogeneous document → Percona Server for
  MongoDB, Mongo Kafka sink) on Ceph. Real connectors: Postgres/MySQL/MariaDB default, SQL Server
  (`tiberius`)/Oracle (OCI)/MongoDB behind cargo features; precise CDC lag behind `kafka-lag`. Full
  pipeline (discover→assess→provision→full-load→CDC→validate→cutover→rollback); **Postgres verified
  end-to-end on two live Rook Ceph clusters incl. real Debezium CDC**; **all six source engines'
  real-connector discovery verified live** — MySQL 8.4, MariaDB 11.x, MongoDB 7.0 (replica set),
  SQL Server 2022, Postgres, and Oracle 23ai/26ai Free — on real servers through the deployed k3s gateway
  (binlog ROW / replica-set change streams / `is_cdc_enabled` / supplemental-log-min → `cdc_capable`,
  real db + table/collection names, PKs, counts). The gateway image now bundles the OCI Instant Client
  and builds the `oracle` feature. **Deeper stages: Postgres, MariaDB, and MongoDB are verified through
  real CDC + cutover** on the Rook Ceph lab (Kafka/Debezium + Connect image); MySQL is verified through
  CDC for DATETIME columns (cutover still pending). Oracle / SQL Server remain discover-live (heterogeneous
  CDC path not yet run end-to-end on the shared lab). The Ceph gateway image (`Dockerfile.ceph`) now
  builds with `mongodb`/`sqlserver`/`oracle`/`kafka-lag` features to match the fake/k8s image.
  See `docs/DATABRIDGE.md` + `deploy/databridge/`.

Day-2 operations added (slices, all fake-first tested): control-plane durability (job recovery, graceful
shutdown, self-state backup, deep readyz/livez), alerting maturity (ack/silence + job/CDC/quota rules,
native alerting sinks), cluster-ops & maintenance (OSD ops, backend cordon, worker pause, job
cancellation), governance (token revocation, rate limiting — DB-backed and cross-replica-aware once
on Postgres, still per-pod on SQLite, `docs/HA.md` — optional OIDC/SSO login alongside local
username/password, verified live against a throwaway Dex instance, `deploy/dex-lab/`; optional
Vault-backed secrets resolution at startup, `docs/SECRETS.md`), volume lifecycle (orphan GC,
QoS), DataBridge CDC self-heal, upgrade pre-flight + rollback, k6 load/performance testing
(read-path + write-path), and cross-cluster DR **scaffolding** (RBD-mirroring
peers/mirrors/failover API + jobs).

**Observe-first storage I/O sensor** (`docs/IO_EBPF.md`): `atlas-io` is a separate, optional node
agent (`atlas-io-agent`, own Dockerfile.io/DaemonSet, `deploy/k8s/atlas-io-agent.yaml`) — kept out of
`atlas-gateway` specifically so the main gateway never needs `CAP_BPF`/`CAP_PERFMON`. Fake bio source
by default (zero external dependency, same fake-first pattern as the storage drivers); device-map
attribution, log2-µs histograms, per-cgroup/pid workload accounting, RCA verdicts, and fail-open
leased write-freezes are all live-verified over HTTP/`/metrics` and `atlasctl io`. Real CO-RE
attach (`crates/atlas-io/bpf/*.bpf.c` C contracts) is not yet wired up — live mode reports the
programs as missing rather than fabricating data.

**Licensed** under the [Apache License 2.0](LICENSE) (`Apache-2.0`; relicensed from the Zyvor
Production License v1.0 at the maintainer's explicit request — history in
[`docs/LICENSING.md`](docs/LICENSING.md)). Don't weaken or remove license notices,
[`CLA.md`](CLA.md), [`DCO.md`](DCO.md), or [`NOTICE`](NOTICE) without an explicit human
request. There is **no** runtime license-key or trial/JWT gate.

Deferred: **real** RBD-mirroring/DR verification (needs a 2nd cluster; the API/jobs are scaffolded but
the `rbd mirror` paths are unverified), per-product integrations beyond the gRPC surface.

## Layout
- `crates/atlas-common` — config, error, tracing, id helpers.
- `crates/atlas-api-types` — shared serde DTOs.
- `crates/atlas-driver-core` — `StorageDriver` trait + `DriverError` + `DriverRegistry`.
- `crates/atlas-driver-ceph` — `ceph`/`rbd` CLI wrappers + `FakeCephDriver`.
- `crates/atlas-driver-nfs` — `FakeNfsDriver`/`RealNfsDriver` (second backend; exports→pools,
  shares→filesystem volumes).
- `crates/atlas-driver-zfs` — `FakeZfsDriver`/`RealZfsDriver` (third backend; zpools→pools,
  datasets→filesystem volumes) + raw-disk inspection/wipe/`zpool create` (`cmd.rs`).
- `crates/atlas-driver-rgw` — `S3Target`, the generic SigV4 S3 client for the bucket/object write path
  (Ceph RGW, the default, and any bring-your-own S3-compatible endpoint). The first-party
  `atlas-driver-rustfs` crate (RustFS-product-specific discovery, admin proxy, drive/instance install
  jobs) was removed — see `docs/RUSTFS.md`.
- `crates/atlas-databridge` — DataBridge: source connectors, assessment, CNPG/Percona CR builders,
  pipeline stages, reconciler (cloud-to-edge DB migration; `migrations/0011`, `/api/atlas/v1/databridge/*`).
- `crates/atlas-io` — optional node sensor (bin `atlas-io-agent`): fake/live bio source, device-map
  attribution, histograms, workload accounting, RCA, fail-open leases; own HTTP API + `/metrics`,
  not embedded in `atlas-gateway` (see `docs/IO_EBPF.md`).
- `crates/atlas-native` — native data plane (library only, not yet wired into the gateway): replicated
  extent engine, metadata WAL + checkpoint/compaction, refcounted extents with free-list space reuse,
  and a sans-IO Raft core for the metadata log (no network transport yet). See
  `docs/NATIVE_STORAGE.md`, `docs/NATIVE_METADATA.md`.
- `crates/atlas-driver-k8s` — `kube-rs` read-only StorageClass/PVC/PV listing.
- `crates/atlas-inventory` — read/upsert model against `sqlx::AnyPool` (SQLite or Postgres); also
  DB-backed rate-limit counters (`rate_limit.rs`).
- `crates/atlas-discovery` — discovery worker.
- `crates/atlas-gateway` — axum server (bin `atlas-gateway`); `routes/ai.rs` (Ops Advisor/incidents/
  anomalies/what-if), `mcp.rs` (MCP tool exposure, `mcp` feature).
- `crates/atlasctl` — `atlasctl` REST client.
- `migrations/` — SQLite schema; `migrations-postgres/` — the Postgres-dialect equivalent (CI fails
  if the two drift to different highest migration numbers).
- `deploy/rook-ceph-lab/` — lab manifests + `up.sh` (`--single-node`; Rook v1.20.2 + Squid + CSI drivers).
- `deploy/postgres-lab/` — throwaway Postgres for HA query-layer testing/dev.
- `deploy/helm/atlas/` — production Helm chart (`database.kind: sqlite|postgres`, driver modes,
  rate limiting, tracing, secrets).
- `scripts/deploy-remote.sh <host> <user>` — build (`Dockerfile`) + deploy the fake/k8s gateway to `zyvor-system`.
- `scripts/deploy-ceph-gateway-remote.sh <host> [user]` — build (`Dockerfile.ceph`, Squid client) + roll out
  the **real-Ceph** gateway `atlas-gateway-ceph` in `rook-ceph` (NodePort 30511). See `docs/DEPLOYMENT.md`.

## Conventions
- Every source file begins with a Zyvor copyright line plus
  `SPDX-License-Identifier: Apache-2.0`
  (enforced by `make headers` / CI). Don't weaken license notices, [`CLA.md`](CLA.md),
  [`DCO.md`](DCO.md), or [`NOTICE`](NOTICE) without an explicit human request.
- Match the monorepo Rust stack: axum 0.8, `sqlx::Any` (SQLite/Postgres, `$N` placeholders — no
  dialect-specific SQL in app code), `thiserror` 2.0 + `anyhow`, `tracing`.
- Ceph/rbd command wrappers use **arg-arrays only, never string concatenation**.
- **sqlx 0.9 only accepts `&'static str` SQL.** A dynamically built query string must be wrapped in
  `sqlx::AssertSqlSafe(..)` with a one-line comment saying why nothing user-controlled is in it
  (values always go through `$N` binds; the interpolated part is a const, a column list or a generated
  `$1,$2,…` placeholder list). Prefer fixing it in the one helper that builds the string.
- **rustls crypto provider**: `main()` installs `ring` before anything builds a TLS client. reqwest
  0.13's default rustls backend enables `aws-lc-rs` too; with both features on and no explicit install
  rustls panics at startup ("Could not automatically determine the process-level CryptoProvider").
- Workspace dependencies were upgraded to latest majors on 2026-09-28 (kube 4.x, sqlx 0.9,
  tonic/prost 0.14 — codegen is `tonic-prost-build` now — reqwest 0.13, argon2 0.6, …). Not bumped:
  `azure_*` (no compatible release yet) and TypeScript 7 (typescript-eslint caps `<6.1.0`).

## Run locally (no Ceph, no cluster needed)
```
make run            # gateway with fake Ceph driver on 127.0.0.1:5110
cargo run -p atlasctl -- --base-url http://127.0.0.1:5110 health
```
Live K8s path (needs KUBECONFIG): `atlasctl storage-classes`.

## Deploy script gotchas

- **`scripts/deploy-ceph-gateway-remote.sh` no longer restarts `atlas-gateway-ceph`
  unconditionally.** It used to run `kubectl rollout restart` after every `kubectl apply`,
  including no-op re-runs — on Ceph-RBD-backed storage (this Deployment's own PVC) that
  repeatedly interrupts an in-flight CSI mount and corrupts the volume (found live, 2026-08-31,
  chasing a stuck Atlas gateway on a second `../hypercluster`-provisioned host: `../hypercluster`'s
  own `storage apply` calls this script on every re-run, so simply re-running it — even just to
  validate an unrelated fix — kept bouncing a healthy pod and produced a multi-hour Ceph
  `HEALTH_ERR`/`recovery_unfound` cascade). Now it only restarts when `kubectl apply`'s output
  does **not** match `^deployment\.apps/${DEPLOY} unchanged$`. If you touch step 7/7 again, keep
  that gate — see `../zeus-os/CLAUDE.md`'s "unconditional restart on every re-apply" entry for
  the full writeup and two sibling-repo instances of the same bug (`../hypercluster`,
  `../packetwolf`).
