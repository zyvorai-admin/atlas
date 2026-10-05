<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas compared with WEKA

WEKA (WekaFS) is a commercial parallel filesystem built for AI and HPC. Atlas is a storage control
plane: one API over several backends (Ceph, NFS, ZFS, Longhorn, S3) plus its own data plane,
atlas-native. This page says where each one is the better choice today. The WEKA column describes
WEKA's publicly documented feature set; it does not quote WEKA benchmark numbers. The Atlas
columns state only what has been measured or verified in this repository.

## Summary

- **Choose WEKA** when the job is the fastest possible shared filesystem for GPU training or HPC
  scratch on NVMe and RDMA, and a commercial licence is acceptable.
- **Choose Atlas** when you need one open control plane across mixed storage (including edge
  sites), database migration to the edge, cross-site DR on Ceph, and operations tooling, without a
  licence key.
- **They combine.** Atlas can register a WEKA cluster as one more backend (`atlas-driver-weka`,
  read-only discovery), so a WEKA estate shows up next to Ceph and ZFS in the same API and console.

## Matrix

| | **Atlas over Ceph** | **Atlas native** | **WEKA** |
|---|---|---|---|
| What it is | Control plane driving Ceph (RBD, CephFS, RGW) through Rook | Atlas's own replicated block and POSIX store | Parallel filesystem with its own client and data path |
| Data path | Ceph's (librbd, kernel RBD, CephFS) | FUSE client; writes through the Raft leader, reads direct from data nodes over a binary protocol (optional); data nodes on files or raw devices (`O_DIRECT` via `io_uring`), striped across several devices | Kernel-bypass NVMe and network stack |
| Measured performance | Ceph's; Atlas adds no data-path hop | Metadata engine ~9–12k creates/s on a 3-voter group; through FUSE ~270 creates/s, ~380 MiB/s write, up to ~540 MiB/s read, ~600 4 KiB random-read IOPS (one host, tmpfs, `docs/NATIVE_FS.md`); no NVMe numbers yet | Designed for very high IOPS and throughput at low latency |
| Metadata | Ceph MDS (CephFS) | Raft groups sharded by volume/filesystem; inodes and directory entries paged from an embedded KV store | Distributed across the cluster |
| Data protection | Ceph replication or erasure coding | 3 replicas or Reed-Solomon erasure coding (e.g. 4+2, 8+3), SHA-256 checksums per extent and per shard | Distributed erasure coding |
| Tiering to object storage | Not managed by Atlas | Not yet | Yes |
| GPUDirect Storage | No | No | Yes |
| Protocols | Block, CephFS, S3 (RGW) | Block over HTTP, POSIX via FUSE | POSIX, NFS, SMB, S3 |
| Snapshots and clones | Yes (RBD, CephFS) | Yes, copy-on-write | Yes |
| Cross-site DR | RBD mirroring, verified live (`docs/DR.md`) | Not yet | Snapshot-to-object and replication features |
| Mixed backends under one API | Ceph, NFS, ZFS, Longhorn, S3, native | (same gateway) | WEKA only |
| Cloud-to-edge DB migration | DataBridge: six engines, CDC, cutover | (same gateway) | Not in scope |
| Operations | AI Ops Advisor, anomaly detection, MCP tools, eBPF I/O RCA (`atlas-io`) | (same gateway) | Own management GUI and monitoring |
| Edge footprint | Single-node Rook lab profile | Single node or three | Multi-node clusters on qualified hardware |
| Licence | Apache-2.0, no key | Apache-2.0, no key | Commercial |

## WEKA as an Atlas backend

`atlas-driver-weka` registers a WEKA cluster as backend `bkd_weka`. It is read-only: WEKA
filesystems appear as `filesystem` volumes (native id `weka-fs:<uid>`) in one `weka` pool, with
cluster capacity, health and `weka_*` metrics. Atlas never creates or changes anything on WEKA.

| Variable | Meaning |
|---|---|
| `ATLAS_WEKA_ENABLE=1` | Register the backend |
| `ATLAS_WEKA_DRIVER_MODE` | `fake` (default, two fixture filesystems) or `real` |
| `ATLAS_WEKA_ENDPOINT` | API base, e.g. `https://weka01:14000/api/v2` |
| `ATLAS_WEKA_USERNAME`, `ATLAS_WEKA_PASSWORD_FILE`, `ATLAS_WEKA_ORG` | Login (`POST /login`); a rejected token triggers one re-login |
| `ATLAS_WEKA_CA_CERT`, `ATLAS_WEKA_TIMEOUT_SECS` | TLS trust and request timeout (10 s) |

Real mode reads `GET /cluster` (`capacity.total_bytes`, `capacity.unprovisioned_bytes`, `status`)
and `GET /fileSystems` (`name`, `uid`, `status`, `total_budget`, `used_total`). It is tested
against a mock of those endpoints, not yet against a live WEKA cluster; any field the cluster
doesn't return is reported as unknown.

## Closing the data-plane gap

The atlas-native roadmap, in order, each phase gated on a published benchmark:

1. **Data path:** clients read and write data nodes directly (striped across replicas) over a
   binary streaming protocol; an `io_uring` device backend with `O_DIRECT` on raw NVMe. Parallel
   extent I/O, group commit, direct client reads, the `io_uring`/`O_DIRECT` backend and
   multi-device striping are done; direct client writes and an NVMe benchmark are not.
2. **Metadata scale:** namespace sharded across Raft groups, an on-disk catalog, client leases.
   Commits cost one fsync, Raft replication is pipelined (a 3-voter group sustains ~12–15k
   creates/s on one host), and checkpoints write only changed records to an embedded KV store.
   Inode tables and directory entries are paged from the store, volumes and filesystems shard
   across Raft groups, client sessions hold cross-mount file locks, and cache leases let mounts
   cache metadata without serving it stale. Filesystem snapshot trees are still held in memory.
3. **Efficiency:** erasure coding with a rebuild controller; cold extents tiered to S3.
   Reed-Solomon k+m extents are done (4+2 stores 1.5× the data instead of 3×); the rebuild
   controller and S3 tiering are not.
4. **AI:** RDMA transport, a GPUDirect Storage path, a checkpoint fast path, a CSI driver;
   MLPerf Storage results.
5. **Protocols:** NFS, SMB and S3 front ends over the same namespace; ACLs, quotas, `O_DIRECT`;
   an IO500 submission.

Status of each step is tracked in `docs/NATIVE_STORAGE.md`. Until a phase's benchmark is
published, treat atlas-native as behind WEKA on that axis.

WEKA and WekaFS are trademarks of WekaIO, Inc. Ceph, Rook and Longhorn are trademarks of their
respective owners.
