<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas I/O eBPF programs

These files are the **on-disk contract** for a future CO-RE loader (`aya` or
libbpf). CI does **not** compile them: the userspace pipeline is tested with
`FakeSource`, and `ATLAS_IO_MODE=live` reports the programs as missing until a
loader lands.

| File | Attach | Purpose |
|---|---|---|
| `atlas_bio.bpf.c` | `block/block_rq_issue`, `block/block_rq_complete` | latency + size |
| `atlas_cgroup.bpf.c` | same + `bpf_get_current_cgroup_id` | tenant / PVC attribution |

Pin directory: `/sys/fs/bpf/atlas`. Observe-first. Write-freeze is a userspace
lease that fails open; these programs must never drop I/O by default.
