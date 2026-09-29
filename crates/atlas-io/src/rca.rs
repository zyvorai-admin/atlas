// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Deterministic, read-only RCA from histograms + workloads.

use atlas_api_types::{IoHistogram, IoOp, IoRca, IoWorkload};

const P99_WARN_US: u64 = 2_000;
const P99_CRIT_US: u64 = 10_000;
const QUEUE_HEAVY_RATIO: u64 = 4; // unused placeholder kept for future queue-split

pub fn explain(hists: &[IoHistogram], workloads: &[IoWorkload], volume: Option<&str>) -> Vec<IoRca> {
    let _ = QUEUE_HEAVY_RATIO;
    let mut out = Vec::new();
    for h in hists {
        if h.op != IoOp::Read && h.op != IoOp::Write {
            continue;
        }
        if let Some(v) = volume {
            if h.volume_id.as_deref() != Some(v) && h.device != v {
                continue;
            }
        }
        let p99 = h.p99_us();
        let p50 = h.p50_us();
        let verdict = if h.count == 0 {
            "unknown"
        } else if p99 >= P99_CRIT_US {
            "critical_latency"
        } else if p99 >= P99_WARN_US {
            "elevated_latency"
        } else {
            "healthy"
        };
        let hottest = workloads
            .iter()
            .filter(|w| w.device == h.device)
            .max_by_key(|w| w.bytes)
            .map(|w| w.comm.clone());
        let mut notes = vec![format!(
            "{} {} IOs, p50={}µs p99={}µs, {} bytes",
            h.count,
            h.op.as_str(),
            p50,
            p99,
            h.bytes
        )];
        if let Some(comm) = &hottest {
            notes.push(format!("hottest issuer on {} is {comm}", h.device));
        }
        if p99 >= P99_WARN_US && p99 > p50.saturating_mul(8) {
            notes.push("long tail vs median — check OSD/device saturation or a clone/discard job".into());
        }
        out.push(IoRca {
            volume_id: h.volume_id.clone(),
            device: h.device.clone(),
            verdict: verdict.into(),
            p50_us: p50,
            p99_us: p99,
            hottest_comm: hottest,
            notes,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_api_types::IoHistogram;

    #[test]
    fn flags_slow_writes() {
        let mut buckets = vec![0u64; 26];
        buckets[14] = 20; // 16ms
        let h = IoHistogram {
            device: "rbd0".into(),
            volume_id: Some("vol_vm_web01".into()),
            op: IoOp::Write,
            buckets,
            count: 20,
            sum_us: 20 * 16_000,
            bytes: 20 * 128 * 1024,
        };
        let rca = explain(&[h], &[], None);
        assert_eq!(rca[0].verdict, "critical_latency");
    }
}
