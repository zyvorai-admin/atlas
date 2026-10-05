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
- Placement is failure-domain aware and defaults to 3 replicas on distinct hosts/racks. With
  `metadata.erasure` set (`4+2`, `8+3`, any k+m up to 32+8), extents of at least
  `erasure_min_bytes` are Reed-Solomon coded into k data and m parity shards on k+m distinct
  nodes instead (`docs/NATIVE_NODE.md`, "Erasure coding").
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
| Namespace sharded across Raft groups, on-disk catalog, client leases | Started: commits cost one fsync, Raft replication is pipelined, and checkpoints write only changed records to an embedded KV store (`catalog.redb`; `metadata_bench`: ~9k creates/s from one proposer and ~12k/s from eight on a 3-voter group at 20k files). Inode tables are paged from the store through a bounded cache, and so are directory entries, one name or one page at a time, so memory no longer holds the whole namespace (filesystem snapshot trees still do); volumes and filesystems shard across Raft groups that share voters and data nodes (`metadata.groups`), with ReadIndex barriers so any replica routes correctly; client sessions (leases) hold cross-mount `fcntl`/`flock` locks that survive leader failover and are released when a client stops renewing; cache leases recalled before every change let mounts cache attributes and names without ever serving them stale (`--cache-leases`) |
| Erasure coding, rebuild controller, S3 tiering of cold extents | Done: Reed-Solomon k+m extents (`metadata.erasure`), read around lost shards and rebuilt onto spare nodes by repair; codec ~1.5 GiB/s encode and ~2 GiB/s two-shard rebuild per core (4 MiB extents). A rebuild controller on each group leader probes nodes and rebuilds the extents of nodes lost for `rebuild_delay_secs` (least spare redundancy first, paced), then scrubs incrementally. Cold extents tier to S3 (Ceph RGW by default, or any S3-compatible bucket) and are read back from their objects; verified against Ceph RGW on the Rook lab. Snapshots export to the bucket as content-addressed blobs (later exports upload only changed extents) and import into a new volume on any cluster with the bucket |
| RDMA transport, GPUDirect Storage, checkpoint fast path, CSI driver | Started: checkpoint fast path (whole-extent writes place data without the engine write lock through a shared in-flight allocator, concurrent appends on file devices, background parallel write-back in the FUSE client; `docs/NATIVE_FS.md`) and the CSI driver (PVCs as filesystems, VolumeSnapshots, snapshot and PVC clones, every access mode through per-pod FUSE mounts; verified on a single-node k3s, `docs/NATIVE_CSI.md`) and the dataset locality API (per-node and per-host bytes of a tree, pinning a full copy of a dataset onto chosen hosts, host-preferring direct reads; `docs/NATIVE_FS.md`, "Dataset locality"). RDMA and GPUDirect Storage need the NVMe/RDMA lab |
| NFS, SMB and S3 front ends, POSIX ACLs, quotas, `O_DIRECT` | Not started |
| libbpf/aya loader for the native eBPF maps | Not started (`atlas-io` covers block-layer attribution) |
