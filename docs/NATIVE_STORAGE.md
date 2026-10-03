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
- Metadata is atomically checkpointed via `catalog.json.tmp -> catalog.json` rename.
- Snapshot creation copies only extent references, so subsequent writes are copy-on-write.
- Reads validate checksums and may fall back to another healthy replica.
- Runtime counters record IOPS/bytes/checksum failures/replica fallbacks.
- `atlas_native_io.bpf.c` defines the bounded eBPF ABI for block latency attribution.

## Deliberate Phase-1 limits

- single metadata authority; no Raft yet
- file-backed devices standing in for raw NVMe
- no garbage collection/refcount reclaim yet
- no cross-extent read API yet
- no RDMA/SPDK/GDS transport yet
- no erasure coding yet

## Next PRs

1. Metadata WAL + Raft shards and extent refcounts/GC.
2. Async io_uring device backend and multi-device striping.
3. libbpf/aya loader for native eBPF maps + Prometheus export.
4. RDMA transport and NUMA-aware placement.
5. Erasure coding + rebuild controller.
6. GPUDirect Storage path and Gryvia dataset locality API.
