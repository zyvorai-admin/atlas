// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! `atlas-native-mount --endpoint https://node:7400 --fs <id> /mnt/point`: mounts an atlas-native
//! filesystem (or, with `--snapshot`, one of its snapshots read-only) through FUSE. Runs in the
//! foreground until unmounted (`fusermount3 -u /mnt/point`).

use std::{path::PathBuf, process::ExitCode, time::Duration};

use atlas_native_fuse::{
    client::{Client, ClientConfig},
    ops::{Ops, OpsConfig},
};
use clap::Parser;

#[derive(Parser)]
#[command(version, about = "Mount an atlas-native filesystem through FUSE")]
struct Args {
    /// Metadata node base URL; repeat (or comma-separate) for every node.
    #[arg(
        long,
        env = "ATLAS_NATIVE_ENDPOINTS",
        value_delimiter = ',',
        required = true
    )]
    endpoint: Vec<String>,
    /// Filesystem id.
    #[arg(long)]
    fs: String,
    /// Mount this snapshot of the filesystem, read-only.
    #[arg(long)]
    snapshot: Option<String>,
    mountpoint: PathBuf,
    /// File holding the API bearer token.
    #[arg(long, env = "ATLAS_NATIVE_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// PEM CA bundle for HTTPS endpoints.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// PEM client certificate and key (one file, certificate first) for mTLS.
    #[arg(long)]
    identity_file: Option<PathBuf>,
    /// Attribute and name cache lifetime (also given to the kernel).
    #[arg(long, default_value_t = 1000)]
    ttl_ms: u64,
    /// Buffer up to this many bytes of sequential writes per file before sending them.
    #[arg(long, default_value_t = 4 << 20)]
    writeback_bytes: usize,
    /// Fetch at least this much per read and serve following reads from it (0 disables).
    #[arg(long, default_value_t = 4 << 20)]
    readahead_bytes: usize,
    /// Largest single request; keep at or below the nodes' `max_request_bytes`.
    #[arg(long, default_value_t = 8 << 20)]
    max_io_bytes: usize,
    /// Keep retrying through elections and unreachable nodes for this long before failing a call.
    #[arg(long, default_value_t = 30)]
    retry_secs: u64,
    /// Mount read-only.
    #[arg(long)]
    read_only: bool,
    /// Let other users access the mount (needs `user_allow_other` in /etc/fuse.conf unless root).
    #[arg(long)]
    allow_other: bool,
}

fn read(path: &Option<PathBuf>, what: &str) -> Result<Option<Vec<u8>>, String> {
    path.as_ref()
        .map(|p| std::fs::read(p).map_err(|e| format!("{what} {}: {e}", p.display())))
        .transpose()
}

fn ops(args: &Args) -> Result<Ops, String> {
    let mut cfg = ClientConfig::new(args.endpoint.clone());
    cfg.token = read(&args.token_file, "token file")?
        .map(|t| String::from_utf8_lossy(&t).trim().to_string());
    cfg.ca_pem = read(&args.ca_file, "CA file")?;
    cfg.identity_pem = read(&args.identity_file, "identity file")?;
    cfg.retry_for = Duration::from_secs(args.retry_secs);
    let client = Client::new(cfg).map_err(|e| e.to_string())?;
    let fs = match &args.snapshot {
        Some(s) => format!("{}@{s}", args.fs),
        None => args.fs.clone(),
    };
    let ops = Ops::new(
        client,
        fs,
        OpsConfig {
            ttl: Duration::from_millis(args.ttl_ms),
            writeback_bytes: args.writeback_bytes,
            readahead_bytes: args.readahead_bytes,
            max_io_bytes: args.max_io_bytes,
        },
    );
    // Fail fast on a wrong endpoint, token or filesystem id instead of at first access.
    ops.getattr(atlas_native::ROOT_INO)
        .map_err(|e| format!("filesystem {} is not reachable (errno {e})", args.fs))?;
    Ok(ops)
}

#[cfg(all(feature = "fuse", target_os = "linux"))]
fn mount(args: &Args, ops: Ops) -> Result<(), String> {
    use fuser::{Config, MountOption, SessionACL};
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName(format!("atlas-native:{}", args.fs)),
        MountOption::Subtype("atlas".into()),
        MountOption::DefaultPermissions,
    ];
    if args.read_only || ops.read_only() {
        cfg.mount_options.push(MountOption::RO);
    }
    if args.allow_other {
        cfg.acl = SessionACL::All;
    }
    let fs = atlas_native_fuse::fuse::AtlasFs { ops };
    fuser::mount(fs, &args.mountpoint, &cfg).map_err(|e| format!("mount: {e}"))
}

#[cfg(not(all(feature = "fuse", target_os = "linux")))]
fn mount(_args: &Args, _ops: Ops) -> Result<(), String> {
    Err("this build has no FUSE support; rebuild on Linux with `--features fuse`".into())
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "atlas_native_fuse=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    // reqwest's rustls backend can't pick between ring and aws-lc-rs when both are compiled in.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    let result = ops(&args).and_then(|ops| mount(&args, ops));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("atlas-native-mount: {e}");
            ExitCode::FAILURE
        }
    }
}
