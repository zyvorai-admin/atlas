<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas Native metadata durability — Phase 2

Phase 2 introduces a deterministic metadata state machine and a write-ahead log (WAL).

## Commit invariant

For every metadata mutation:

1. build a `MetaCommand`;
2. apply it to a copy of the catalog; a command that fails validation is rejected here and never logged;
3. append `{term,index,command}` to `metadata.wal`;
4. `fsync` the WAL;
5. publish the new catalog in memory;
6. atomically replace `catalog.json` and fsync the directory.

On restart, Atlas loads `catalog.json` and replays every WAL record whose index is greater than
`catalog.applied_index`.

This is deliberately the same boundary a future Raft implementation needs. Raft replicates
`MetaCommand` records; `Catalog::apply` remains the deterministic state machine.

## Extent lifetime

Each physical extent has a metadata refcount. Active volumes and snapshots both own references.
Overwrites decrement the previous active extent and install a new immutable extent. Snapshot delete
and volume delete decrement references. Zero-reference extents become GC candidates.

Phase 2 marks zero-reference extents reclaimed in metadata. Physical free-space reuse/hole punching
is deferred to the allocator PR to keep the append-only crash model simple.

## Failure model covered

- process crash after WAL fsync but before catalog persistence;
- restart/replay without double-applying committed commands;
- snapshot copy-on-write isolation;
- refcount underflow protection;
- monotonic WAL indexes.

## Next phase

- real Raft transport/election and quorum commit;
- allocator free lists + physical extent reuse;
- checkpoint/WAL truncation;
- background scrub and replica repair;
- io_uring/raw-NVMe data path.
