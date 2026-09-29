// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use atlas_io::devmap::DeviceMap;
use atlas_io::http::router;
use atlas_io::source::FakeSource;
use atlas_io::Collector;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn http_histograms_and_rca() {
    let c = Collector::new(Box::new(FakeSource::demo()), DeviceMap::lab());
    let app = router(c);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/io/histograms")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v.as_array().unwrap().len() >= 2);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/io/rca?volume=vol_vm_web01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let found = v
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["verdict"] == "critical_latency");
    assert!(found, "expected critical write tail on vol_vm_web01, got {v}");

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("atlas_io_events_seen"));
    assert!(text.contains("atlas_io_hist_p99_us"));
}

#[tokio::test]
async fn lease_grant_and_list() {
    let c = Collector::new(Box::new(FakeSource::demo()), DeviceMap::lab());
    let app = router(c);
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/io/leases")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"device":"rbd0","ttl_secs":30,"reason":"incident-42"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    let res = app
        .oneshot(
            Request::builder()
                .uri("/io/leases")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1);
    assert_eq!(v[0]["device"], "rbd0");
    assert_eq!(v[0]["action"], "write_freeze");
}

#[tokio::test]
async fn live_mode_reports_missing_programs() {
    use atlas_io::source::LiveSource;
    let c = Collector::new(Box::new(LiveSource), DeviceMap::new());
    let h = c.health();
    assert_eq!(h.mode, "live");
    assert!(h.programs_loaded.is_empty());
    assert!(h.programs_missing.contains(&"atlas_bio".into()));
}
