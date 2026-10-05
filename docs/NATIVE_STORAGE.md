<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas Native storage engine — Phase 1

This PR introduces the first native Atlas data-plane primitive: an append-only replicated extent engine.
It intentionally does **not** claim WEKA-equivalent maturity. The purpose of Phase 1 is to establish durable
extent, checksum, placement, snapshot and telemetry contracts that later distributed metadata, RDMA, SPDK,
GDS and erasure-coding work can build on.

## Invariants

- Writes allocate new extents; existing extents are never overwritten in place.
- Every extent is checksummed with SHA-256.
- Placement is failure-domain aware and defaults to 3 replicas on distinct hosts/racks.
- Metadata is checkpointed incrementally into `catalog.redb` (one durable redb transaction per checkpoint).
- Snapshot creation copies only extent references, so subsequent writes are copy-on-write.
- Reads validate checksums and may fall back to another healthy replica; a read starts at a
  replica chosen from the extent id, so reads of many extents spread over all replicas.
- Runtime counters record IOPS/bytes/checksum failures/replica fallbacks.
- `atlas_native_io.bpf.c` defines the bounded eBPF ABI for block latency attribution.

## Status against the roadmap

The roadmap and the comparison it serves are in [`COMPARISON.md`](COMPARISON.md).

| Step | Status |
|---|---|
| Metadata WAL, one Raft group, extent refcounts and GC, repair | Done (`docs/NATIVE_NODE.md`) |
| POSIX namespace and FUSE client | Done (`docs/NATIVE_FS.md`) |
| Data path: parallel extent I/O, group commit, pooled data-node connections, direct client reads | Done (`docs/NATIVE_FS.md`, "Data path") |
| `io_uring` device backend with `O_DIRECT` on raw NVMe, multi-device striping | Done (`docs/NATIVE_NODE.md`, "Devices"); verified on ext4, not yet benchmarked on NVMe |
| Namespace sharded across Raft groups, on-disk catalog, client leases | Started: commits cost one fsync, Raft replication is pipelined, and checkpoints write only changed records to an embedded KV store (`catalog.redb`; `metadata_bench`: ~9k creates/s from one proposer and ~12k/s from eight on a 3-voter group at 20k files). Inode tables of filesystems and their snapshots are paged from the store through a bounded cache, so memory no longer holds the whole namespace (a directory's entries still load as a whole); sharding and leases are not started |
| Erasure coding, rebuild controller, S3 tiering of cold extents | Not started; 3 replicas |
| RDMA transport, GPUDirect Storage, checkpoint fast path, CSI driver | Not started |
| NFS, SMB and S3 front ends, POSIX ACLs, quotas, `O_DIRECT` | Not started |
| libbpf/aya loader for the native eBPF maps | Not started (`atlas-io` covers block-layer attribution) |
