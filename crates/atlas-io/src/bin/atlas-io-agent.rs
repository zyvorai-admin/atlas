// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Node agent. Fake mode is the default so `make run` / CI never need CAP_BPF.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use atlas_io::devmap::DeviceMap;
use atlas_io::source::{self, FakeSource, LiveSource};
use atlas_io::Collector;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "atlas-io-agent", about = "Atlas observe-first storage I/O sensor")]
struct Cli {
    /// Bind address.
    #[arg(long, env = "ATLAS_IO_BIND", default_value = "127.0.0.1:5111")]
    bind: SocketAddr,
    /// `fake` (scripted events) or `live` (block-layer BPF; needs the `bpf` build and
    /// CAP_BPF + CAP_PERFMON, else it reports the programs missing).
    #[arg(long, env = "ATLAS_IO_MODE", default_value = "fake")]
    mode: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    let collector = match cli.mode.as_str() {
        "live" => {
            let src = source::live().unwrap_or_else(|e| {
                tracing::warn!(error = %format!("{e:#}"), "BPF attach failed; reporting programs missing");
                Box::new(LiveSource)
            });
            Collector::new(src, DeviceMap::from_sysfs())
        }
        _ => Collector::new(Box::new(FakeSource::demo()), DeviceMap::lab()),
    };
    collector.poll();
    let poller = collector.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            poller.poll();
        }
    });
    tracing::info!(%cli.bind, mode = %cli.mode, seen = collector.seen(), "atlas-io-agent listening");
    let app = atlas_io::http::router(collector);
    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
