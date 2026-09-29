// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Agent HTTP surface (REST + Prometheus text). Independent of atlas-gateway.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::Collector;

#[derive(Clone)]
pub struct AppState {
    pub collector: Arc<Collector>,
}

pub fn router(collector: Arc<Collector>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/io/health", get(health))
        .route("/io/summary", get(summary))
        .route("/io/histograms", get(histograms))
        .route("/io/workloads", get(workloads))
        .route("/io/rca", get(rca))
        .route("/io/leases", get(leases).post(grant_lease))
        .route("/io/coverage", get(coverage))
        .route("/metrics", get(prom))
        .route("/io/poll", post(poll))
        .with_state(AppState { collector })
}

async fn health(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.collector.health())
}

async fn summary(State(st): State<AppState>) -> impl IntoResponse {
    st.collector.poll();
    let h = st.collector.health();
    let hists = st.collector.histograms();
    Json(serde_json::json!({
        "health": h,
        "histograms": hists.len(),
        "workloads": st.collector.workloads().len(),
        "leases": st.collector.leases().len(),
    }))
}

async fn histograms(State(st): State<AppState>) -> impl IntoResponse {
    st.collector.poll();
    Json(st.collector.histograms())
}

async fn workloads(State(st): State<AppState>) -> impl IntoResponse {
    st.collector.poll();
    Json(st.collector.workloads())
}

#[derive(Deserialize)]
struct RcaQuery {
    volume: Option<String>,
}

async fn rca(State(st): State<AppState>, Query(q): Query<RcaQuery>) -> impl IntoResponse {
    st.collector.poll();
    Json(st.collector.rca(q.volume.as_deref()))
}

async fn leases(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.collector.leases())
}

#[derive(Deserialize)]
struct GrantBody {
    device: String,
    volume_id: Option<String>,
    #[serde(default = "default_ttl")]
    ttl_secs: u64,
    #[serde(default = "default_reason")]
    reason: String,
}

fn default_ttl() -> u64 {
    60
}
fn default_reason() -> String {
    "operator".into()
}

async fn grant_lease(
    State(st): State<AppState>,
    Json(body): Json<GrantBody>,
) -> impl IntoResponse {
    match st
        .collector
        .grant_lease(body.device, body.volume_id, body.ttl_secs, body.reason)
    {
        Ok(l) => (
            StatusCode::CREATED,
            Json(serde_json::to_value(l).expect("IoLease is serializable")),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

async fn coverage(State(st): State<AppState>) -> impl IntoResponse {
    let h = st.collector.health();
    Json(serde_json::json!({
        "pin_dir": h.pin_dir,
        "loaded": h.programs_loaded,
        "missing": h.programs_missing,
        "mode": h.mode,
    }))
}

async fn poll(State(st): State<AppState>) -> impl IntoResponse {
    st.collector.poll();
    Json(serde_json::json!({ "seen": st.collector.seen() }))
}

async fn prom(State(st): State<AppState>) -> impl IntoResponse {
    st.collector.poll();
    let mut out = String::from("# HELP atlas_io_events_seen Total bio events ingested\n# TYPE atlas_io_events_seen counter\n");
    out.push_str(&format!("atlas_io_events_seen {}\n", st.collector.seen()));
    out.push_str("# HELP atlas_io_hist_count Events in a (device,op) histogram\n# TYPE atlas_io_hist_count gauge\n");
    for h in st.collector.histograms() {
        out.push_str(&format!(
            "atlas_io_hist_count{{device=\"{}\",op=\"{}\"}} {}\n",
            h.device,
            h.op.as_str(),
            h.count
        ));
        out.push_str(&format!(
            "atlas_io_hist_p99_us{{device=\"{}\",op=\"{}\"}} {}\n",
            h.device,
            h.op.as_str(),
            h.p99_us()
        ));
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
}
