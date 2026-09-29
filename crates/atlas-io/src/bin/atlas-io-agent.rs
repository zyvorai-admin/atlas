// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Node agent. Fake mode is the default so `make run` / CI never need CAP_BPF.

use std::net::SocketAddr;

use anyhow::Result;
use atlas_io::devmap::DeviceMap;
use atlas_io::source::{FakeSource, LiveSource};
use atlas_io::Collector;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "atlas-io-agent", about = "Atlas observe-first storage I/O sensor")]
struct Cli {
    /// Bind address.
    #[arg(long, env = "ATLAS_IO_BIND", default_value = "127.0.0.1:5111")]
    bind: SocketAddr,
    /// `fake` (scripted events) or `live` (honest empty attach until a BPF loader ships).
    #[arg(long, env = "ATLAS_IO_MODE", default_value = "fake")]
    mode: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    let map = DeviceMap::lab();
    let collector = match cli.mode.as_str() {
        "live" => Collector::new(Box::new(LiveSource), map),
        _ => Collector::new(Box::new(FakeSource::demo()), map),
    };
    collector.poll();
    tracing::info!(%cli.bind, mode = %cli.mode, seen = collector.seen(), "atlas-io-agent listening");
    let app = atlas_io::http::router(collector);
    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
