<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas I/O eBPF programs

`atlas_bio.bpf.c` is compiled by `build.rs` (clang `-target bpf`) when the crate is built with the
`bpf` feature, embedded in the binary, and attached by `src/bpf.rs` (aya) in `ATLAS_IO_MODE=live`.
The tracepoint records are declared with `preserve_access_index`, so field offsets are relocated
against the running kernel's BTF (CO-RE; needs `/sys/kernel/btf/vmlinux`). The default build does
not compile it: CI tests the userspace pipeline with `FakeSource`.

| File | Attach | Purpose |
|---|---|---|
| `atlas_bio.bpf.c` | `block/block_rq_issue`, `block/block_rq_complete` | latency, size, op, and the issuing task's pid / comm / cgroup id |
| `atlas_cgroup.bpf.c` | — | contract for binding a cgroup id to an Atlas volume (not loaded) |
| `atlas_native_io.bpf.c` | — | contract for atlas-native I/O (not loaded) |

Observe-only: the programs never change or drop I/O. Write-freeze is a userspace lease that fails
open. Only non-GPL helpers are used, so the object loads under its `Apache-2.0` license string.
