// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//
// Atlas Native eBPF contract.
//
// This follows atlas-io's current convention: CO-RE map/event ABI first, loader wiring later.
// The loader should attach block_rq_issue/block_rq_complete and attribute events to the
// atlas-native cgroup or PID set, then pin maps under /sys/fs/bpf/atlas/native.
//
// Bounded cardinality is mandatory. Userspace must evict stale inflight entries.

#ifndef ATLAS_NATIVE_IO_BPF_H
#define ATLAS_NATIVE_IO_BPF_H

struct atlas_native_io_key {
    __u32 major;
    __u32 minor;
    __u32 op;
    __u32 pad;
    __u64 cgroup_id;
};

struct atlas_native_io_value {
    __u64 ios;
    __u64 bytes;
    __u64 latency_ns;
    __u64 max_latency_ns;
    __u64 errors;
};

struct atlas_native_extent_event {
    __u64 ts_ns;
    __u64 cgroup_id;
    __u64 sector;
    __u64 latency_ns;
    __u32 bytes;
    __u32 major;
    __u32 minor;
    __u32 op;
    __u32 pid;
    __u32 error;
    char comm[16];
};

#define ATLAS_NATIVE_IO_STATS_MAX 4096
#define ATLAS_NATIVE_INFLIGHT_MAX 16384
#define ATLAS_NATIVE_SLOW_IO_NS 5000000ULL

#endif
