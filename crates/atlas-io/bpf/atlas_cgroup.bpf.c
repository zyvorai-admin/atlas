// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//
// cgroup-id helper contract. Live attach would use bpf_get_current_cgroup_id()
// at issue time so a PVC / QEMU cgroup can be bound to an Atlas volume.

struct atlas_cgroup_bind {
    __u64 cgroup_id;
    char volume_id[64];
};
