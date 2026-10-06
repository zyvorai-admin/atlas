---
sidebar_position: 1
title: Architecture
---

{/* Copyright (c) 2026 ZyvorAI Labs Private Limited. */}
{/* SPDX-License-Identifier: Apache-2.0 */}

# Architecture

Atlas is the **central storage control plane** for the Zyvor suite. Products call
stable REST / gRPC APIs; Atlas talks to backends through pluggable
`StorageDriver`s. Ceph (RBD / CephFS / RGW) is first; NFS and ZFS are also wired.

```
 Products (Zeus OS · Veyron · Transiva · …)
                    │  REST / gRPC
                    ▼
             Atlas Gateway
         auth · audit · jobs · UI
                    │
     ┌──────────────┼──────────────┐
     ▼              ▼              ▼
 discovery      inventory       drivers
                (SQLite)     Ceph · NFS · ZFS · K8s
```

Design authority:
[`Zyvor_Ceph_Integration_Developer_Implementation_Plan.pdf`](https://github.com/zyvorai/zyvor-atlas)
(v1.0 engineering draft). Full operator docs live in the repository under
`docs/ARCHITECTURE.md`.
