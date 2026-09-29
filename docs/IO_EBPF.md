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
| DaemonSet manifest | implemented, fake mode |
| Live CO-RE loader (`aya`) | **not** compiled; `live` mode reports missing programs |

## Run

```bash
cargo run -p atlas-io --bin atlas-io-agent
# http://127.0.0.1:5111/io/histograms
cargo run -p atlasctl -- io rca --volume vol_vm_web01
```

## Non-goals

- TC/XDP, DNS, L7 (Netra / PacketWolf)
- KVM exits (Shukra / FluxVM)
- Replacing CSI / `ceph` CLI
- Standing default-deny on writes
