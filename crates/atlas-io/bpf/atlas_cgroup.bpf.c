// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//
// Contract for binding a cgroup to an Atlas volume. atlas_bio.bpf.c already records
// bpf_get_current_cgroup_id() at issue time; this map would let userspace name the volume.

struct atlas_cgroup_bind {
    __u64 cgroup_id;
    char volume_id[64];
};
