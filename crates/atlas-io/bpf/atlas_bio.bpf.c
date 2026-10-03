// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//
// Block I/O latency, size and attribution. Observe-only: never changes or drops I/O.
//
//   tracepoint/block/block_rq_issue     remember (dev, sector) -> issue time, size, op, task
//   tracepoint/block/block_rq_complete  join, emit one atlas_bio_event on the ring buffer
//
// Built with clang -target bpf (crates/atlas-io/build.rs, `bpf` feature) and loaded by
// src/bpf.rs. The tracepoint records are declared with preserve_access_index, so field
// offsets are relocated against the running kernel's BTF (CO-RE); no vmlinux.h is needed.

#include <linux/types.h>
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// Only the fields read here; names must match the kernel's trace_event_raw_* types.
struct trace_event_raw_block_rq {
    __u32 dev;
    __u64 sector;
    __u32 bytes;
    char rwbs[10];
} __attribute__((preserve_access_index));

struct trace_event_raw_block_rq_completion {
    __u32 dev;
    __u64 sector;
} __attribute__((preserve_access_index));

// Must match RawEvent in src/bpf.rs.
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

struct atlas_bio_key {
    __u32 dev;
    __u32 pad;
    __u64 sector;
};

struct atlas_bio_inflight {
    __u64 issued_ns;
    __u64 cgroup_id;
    __u32 pid;
    __u32 bytes;
    __u32 op;
    __u32 pad;
    char comm[16];
};

#define ATLAS_BIO_INFLIGHT_MAX 8192

// Linux REQ_OP_* values, as parsed by source::parse_op.
#define ATLAS_OP_READ 0
#define ATLAS_OP_WRITE 1
#define ATLAS_OP_FLUSH 2
#define ATLAS_OP_DISCARD 3
#define ATLAS_OP_OTHER 255

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, ATLAS_BIO_INFLIGHT_MAX);
    __type(key, struct atlas_bio_key);
    __type(value, struct atlas_bio_inflight);
} atlas_inflight SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 22);
} atlas_events SEC(".maps");

// [0] issues not tracked (inflight full), [1] completions not delivered (ring buffer full).
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 2);
    __type(key, __u32);
    __type(value, __u64);
} atlas_dropped SEC(".maps");

static __always_inline void count_drop(__u32 slot)
{
    __u64 *n = bpf_map_lookup_elem(&atlas_dropped, &slot);
    if (n)
        __sync_fetch_and_add(n, 1);
}

// rwbs is blktrace's op string: an optional 'F' (preflush), then the op letter — 'R', 'W', 'D'
// (discard), 'F' (flush) or 'N' — then flag letters. So the op letter is rwbs[0] or rwbs[1].
static __always_inline __u32 op_letter(char c)
{
    switch (c) {
    case 'R':
        return ATLAS_OP_READ;
    case 'W':
        return ATLAS_OP_WRITE;
    case 'D':
        return ATLAS_OP_DISCARD;
    case 'F':
        return ATLAS_OP_FLUSH;
    default:
        return ATLAS_OP_OTHER;
    }
}

SEC("tracepoint/block/block_rq_issue")
int atlas_rq_issue(struct trace_event_raw_block_rq *ctx)
{
    struct atlas_bio_key key = {};
    struct atlas_bio_inflight v = {};
    // Read byte by byte: the verifier refuses loads through a ctx pointer moved by a memcpy.
    char c0 = ctx->rwbs[0];
    char c1 = ctx->rwbs[1];

    key.dev = ctx->dev;
    key.sector = ctx->sector;

    v.issued_ns = bpf_ktime_get_ns();
    v.cgroup_id = bpf_get_current_cgroup_id();
    v.pid = bpf_get_current_pid_tgid() >> 32;
    v.bytes = ctx->bytes;
    v.op = op_letter(c0 == 'F' && c1 != 0 ? c1 : c0);
    bpf_get_current_comm(v.comm, sizeof(v.comm));

    if (bpf_map_update_elem(&atlas_inflight, &key, &v, BPF_ANY))
        count_drop(0);
    return 0;
}

SEC("tracepoint/block/block_rq_complete")
int atlas_rq_complete(struct trace_event_raw_block_rq_completion *ctx)
{
    struct atlas_bio_key key = {};
    struct atlas_bio_inflight *v;
    struct atlas_bio_event *e;

    key.dev = ctx->dev;
    key.sector = ctx->sector;
    v = bpf_map_lookup_elem(&atlas_inflight, &key);
    if (!v)
        return 0;

    e = bpf_ringbuf_reserve(&atlas_events, sizeof(*e), 0);
    if (!e) {
        count_drop(1);
        bpf_map_delete_elem(&atlas_inflight, &key);
        return 0;
    }
    e->issued_ns = v->issued_ns;
    e->completed_ns = bpf_ktime_get_ns();
    e->queued_ns = 0;
    e->bytes = v->bytes;
    e->op = v->op;
    // dev_t inside the kernel is MAJOR << 20 | MINOR.
    e->major = key.dev >> 20;
    e->minor = key.dev & ((1U << 20) - 1);
    e->pid = v->pid;
    e->pad = 0;
    e->cgroup_id = v->cgroup_id;
    __builtin_memcpy(e->comm, v->comm, sizeof(e->comm));
    bpf_ringbuf_submit(e, 0);
    bpf_map_delete_elem(&atlas_inflight, &key);
    return 0;
}

// Only non-GPL helpers are used, so the program loads under the project's own license.
char LICENSE[] SEC("license") = "Apache-2.0";
