<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Atlas I/O sensor (eBPF)

Atlas stays a **storage control plane**. Block/FS/object observability is an
optional node agent so the gateway never holds `CAP_BPF`.

## Why a separate agent

The gateway runs as uid `10001` in-cluster and talks to drivers over CLI/API.
eBPF needs `CAP_BPF`/`CAP_PERFMON` and `/sys/fs/bpf`. Mixing those in
`atlas-gateway` would punch a hole in the secret-isolation story. Netra already
owns sockets; this agent owns **bio / NFS / ZFS / io_uring**.

## What ships in this slice

| Surface | Status |
|---|---|
| Fake event source + collector | implemented, unit-tested |
| Device map `major:minor` → volume | implemented |
| Log2-µs histograms, p50/p99 | implemented |
| cgroup/pid workloads | implemented |
| Read-only RCA | implemented |
| Write-freeze leases, fail-open, 15m cap | implemented |
| HTTP + Prometheus text | implemented (`:5111`) |
| `atlasctl io …` | implemented |
| DaemonSet manifest | implemented, fake mode (live settings in its header) |
| Live block-layer attach (`aya`, CO-RE) | implemented behind the `bpf` feature; verified live (below) |
| NFS / ZFS / io_uring programs, queue time, volume binding | not yet — reported in `programs_missing`, `queued_ns` is 0 |

## Run

```bash
cargo run -p atlas-io --bin atlas-io-agent
# http://127.0.0.1:5111/io/histograms
cargo run -p atlasctl -- io rca --volume vol_vm_web01
```

## Live mode

`cargo build -p atlas-io --features bpf` compiles `crates/atlas-io/bpf/atlas_bio.bpf.c` with clang
(plus libbpf headers) and embeds it; `Dockerfile.io` builds that way. With `--mode live` the agent
attaches it to `block/block_rq_issue` and `block/block_rq_complete`: issue records time, size, op and
the issuing task's pid / comm / cgroup id under `(dev, sector)`; completion joins and pushes one event
to a ring buffer, which the agent drains every second. Devices are named from `/sys/class/block`
at startup (later ones show as `major:minor`); kernel-side drops (inflight map full, ring buffer
full) are added to `events_dropped`.

Needs a kernel with BTF (`/sys/kernel/btf/vmlinux`) and ring buffers (5.8+), and in a container:
uid 0 with `CAP_BPF` + `CAP_PERFMON`, a seccomp profile that allows `perf_event_open` (podman's /
containerd's default only allows it with `CAP_SYS_ADMIN`, so `Unconfined`), and the host tracefs at
`/sys/kernel/tracing`. If attaching fails the agent logs why and keeps serving with the programs
reported missing — it never fabricates events.

Attribution is the task that *issued* the request: plugged or writeback I/O shows as a `kworker`
in cgroup 1, direct I/O as the application (`dd`, `qemu`, …) in its own cgroup.

**Verified (2026-10-03, `212.8.248.187`, Ubuntu 26.04, kernel 7.0):** both programs pass the
verifier and attach (CO-RE relocated `trace_event_raw_block_rq{,_completion}` against the kernel
BTF); a 64 MiB `oflag=direct` write showed up as `sda` write histograms (`/io/histograms`,
`/metrics`) and as a `dd` workload with its pid and cgroup id, alongside the host's own `llama-server`
and kworker traffic; ~5,000 events in a few seconds with no drops; the programs detach when the
agent stops. The `Dockerfile.io` image does the same with only `CAP_BPF` + `CAP_PERFMON`,
`seccomp=unconfined` and the tracefs mount; with the default seccomp profile it reports the
programs missing (`perf_event_open … Operation not permitted`) and keeps serving.

## Non-goals

- TC/XDP, DNS, L7 (Netra / PacketWolf)
- KVM exits (Shukra / FluxVM)
- Replacing CSI / `ceph` CLI
- Standing default-deny on writes
