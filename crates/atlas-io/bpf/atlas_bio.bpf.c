// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//
// CO-RE skeleton for block I/O issue/complete. Not compiled in CI.
// Intended attach:
//   tracepoint/block/block_rq_issue
//   tracepoint/block/block_rq_complete
// Maps pin under /sys/fs/bpf/atlas.
//
// Keep this file a documented contract for a future aya/libbpf loader.
// Cardinality of hist and inflight maps MUST stay capped in userspace.

#ifndef ATLAS_BIO_BPF_H
#define ATLAS_BIO_BPF_H

// Event layout must match atlas_api_types::BioEvent field order in the loader.
struct atlas_bio_event {
    __u64 issued_ns;
    __u64 completed_ns;
    __u64 queued_ns;
    __u32 bytes;
    __u32 op;
    __u32 major;
    __u32 minor;
    __u32 pid;
    __u32 pad;
    __u64 cgroup_id;
    char comm[16];
};

// Inflight keyed by (dev, sector) — userspace joins if the kernel
// tracepoint does not carry both timestamps.
struct atlas_bio_inflight {
    __u64 issued_ns;
    __u32 pid;
    __u32 bytes;
    __u32 op;
    __u32 major;
    __u32 minor;
    __u64 cgroup_id;
};

#define ATLAS_BIO_INFLIGHT_MAX 8192
#define ATLAS_BIO_HIST_MAX 4096

#endif
