// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Live block-layer source: attaches `bpf/atlas_bio.bpf.c` (compiled by build.rs) to the
//! `block_rq_issue` / `block_rq_complete` tracepoints and drains its ring buffer.

use std::sync::Mutex;

use anyhow::{Context, Result};
use atlas_api_types::{BioEvent, IoSensorHealth};
use aya::maps::{Array, MapData, RingBuf};
use aya::programs::TracePoint;
use aya::Ebpf;

use crate::source::{parse_op, IoSource};
use crate::{CARDINALITY_CAP, PIN_DIR};

static OBJECT: &[u8] = aya::include_bytes_aligned!(concat!(env!("OUT_DIR"), "/atlas_bio.bpf.o"));

const PROGRAMS: [(&str, &str); 2] = [
    ("atlas_rq_issue", "block_rq_issue"),
    ("atlas_rq_complete", "block_rq_complete"),
];

/// Events taken per poll, so a burst can't hold the collector lock for long; the rest wait in
/// the ring buffer for the next poll.
const MAX_DRAIN: usize = 65_536;

/// `struct atlas_bio_event`.
#[repr(C)]
#[derive(Clone, Copy)]
struct RawEvent {
    issued_ns: u64,
    completed_ns: u64,
    queued_ns: u64,
    bytes: u32,
    op: u32,
    major: u32,
    minor: u32,
    pid: u32,
    pad: u32,
    cgroup_id: u64,
    comm: [u8; 16],
}

struct Attached {
    events: RingBuf<MapData>,
    dropped: Array<MapData, u64>,
    // Owns the programs and their tracepoint links; dropping it detaches them.
    _ebpf: Ebpf,
}

pub struct BpfSource {
    inner: Mutex<Attached>,
}

impl BpfSource {
    /// Load and attach both programs. Needs CAP_BPF + CAP_PERFMON (or root) and tracefs.
    pub fn attach() -> Result<Self> {
        let mut ebpf = Ebpf::load(OBJECT).context("load the atlas_bio BPF object")?;
        for (name, tracepoint) in PROGRAMS {
            let prog: &mut TracePoint = ebpf
                .program_mut(name)
                .with_context(|| format!("{name} missing from the BPF object"))?
                .try_into()?;
            prog.load().with_context(|| format!("load {name}"))?;
            prog.attach("block", tracepoint)
                .with_context(|| format!("attach {name} to block/{tracepoint}"))?;
        }
        let events = RingBuf::try_from(ebpf.take_map("atlas_events").context("atlas_events map")?)?;
        let dropped = Array::try_from(
            ebpf.take_map("atlas_dropped")
                .context("atlas_dropped map")?,
        )?;
        Ok(Self {
            inner: Mutex::new(Attached {
                events,
                dropped,
                _ebpf: ebpf,
            }),
        })
    }
}

impl IoSource for BpfSource {
    fn mode(&self) -> &'static str {
        "live"
    }

    fn poll_events(&mut self) -> Vec<BioEvent> {
        let g = self.inner.get_mut().expect("bpf source lock");
        let mut out = Vec::new();
        while out.len() < MAX_DRAIN {
            let Some(item) = g.events.next() else { break };
            out.extend(decode(&item));
        }
        out
    }

    fn health(&self, seen: u64, dropped: u64, cardinality: u64) -> IoSensorHealth {
        let kernel_dropped: u64 = {
            let g = self.inner.lock().expect("bpf source lock");
            (0..2).filter_map(|slot| g.dropped.get(&slot, 0).ok()).sum()
        };
        IoSensorHealth {
            mode: self.mode().to_string(),
            programs_loaded: vec!["atlas_bio".into(), "atlas_cgroup".into()],
            programs_missing: vec!["atlas_nfs".into(), "atlas_zfs".into(), "atlas_uring".into()],
            pin_dir: PIN_DIR.to_string(),
            events_seen: seen,
            events_dropped: dropped + kernel_dropped,
            map_cardinality: cardinality,
            cardinality_cap: CARDINALITY_CAP,
        }
    }
}

fn decode(buf: &[u8]) -> Option<BioEvent> {
    if buf.len() < std::mem::size_of::<RawEvent>() {
        return None;
    }
    // SAFETY: the length is checked above and RawEvent is plain old data, valid for any bytes.
    let raw: RawEvent = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };
    let comm_len = raw
        .comm
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(raw.comm.len());
    Some(BioEvent {
        issued_ns: raw.issued_ns,
        completed_ns: raw.completed_ns,
        queued_ns: raw.queued_ns,
        bytes: raw.bytes,
        op: parse_op(raw.op),
        major: raw.major,
        minor: raw.minor,
        pid: raw.pid,
        cgroup_id: raw.cgroup_id,
        comm: String::from_utf8_lossy(&raw.comm[..comm_len]).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_api_types::IoOp;

    #[test]
    fn decodes_the_ring_buffer_record() {
        let mut comm = [0u8; 16];
        comm[..3].copy_from_slice(b"fio");
        let raw = RawEvent {
            issued_ns: 1_000,
            completed_ns: 251_000,
            queued_ns: 0,
            bytes: 4096,
            op: 1,
            major: 259,
            minor: 0,
            pid: 42,
            pad: 0,
            cgroup_id: 7,
            comm,
        };
        // SAFETY: RawEvent is repr(C) plain old data.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&raw as *const RawEvent).cast::<u8>(),
                std::mem::size_of::<RawEvent>(),
            )
        };
        let ev = decode(bytes).expect("decodes");
        assert_eq!(ev.latency_us(), 250);
        assert_eq!(ev.op, IoOp::Write);
        assert_eq!((ev.major, ev.minor, ev.pid, ev.cgroup_id), (259, 0, 42, 7));
        assert_eq!(ev.comm, "fio");
        assert!(decode(&bytes[..10]).is_none());
    }

    #[test]
    fn record_matches_the_c_layout() {
        assert_eq!(std::mem::size_of::<RawEvent>(), 72);
    }
}
