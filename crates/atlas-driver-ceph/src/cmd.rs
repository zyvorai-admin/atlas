// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Safe `ceph`/`rbd` command wrappers.
//!
//! Security rule (PDF §17.3): pass arguments as an **array only**; never build a shell string.
//! Modeled on `machina/agent/src/provision_ops.rs`, but async and JSON-parsing.

use std::time::Duration;

use atlas_driver_core::DriverError;

/// `radosgw-admin` talks to RGW over its admin ops socket/REST API rather than directly to the
/// mons, so it can hang far longer than an `rbd`/`ceph` CLI call when RGW is slow or unresponsive
/// — and unlike those, nothing here waits on a multi-stage operation the caller explicitly wants
/// to block on. Bounding every `radosgw-admin` invocation at this shared layer means no caller
/// (present or future) can wedge the single-threaded job engine's worker indefinitely — a hung
/// `bucket.create` quota-set call was observed blocking every other tenant's jobs for the full
/// 2h `ATLAS_JOB_TIMEOUT_SECS` ceiling until manually cancelled via `POST /jobs/{id}/cancel`.
const RADOSGW_ADMIN_TIMEOUT: Duration = Duration::from_secs(12);

/// Build a `Command` for a ceph/rbd/radosgw-admin binary with `kill_on_drop(true)` — if the
/// enclosing future is dropped (job cancellation, `tokio::select!` racing a timeout), the child
/// process is killed rather than left running as an orphan holding cluster-side locks/watchers.
fn cmd(bin: &str) -> tokio::process::Command {
    let mut c = tokio::process::Command::new(bin);
    c.kill_on_drop(true);
    c
}

/// Run `ceph <args...> --format json` and parse stdout as JSON.
pub async fn ceph_cmd(args: &[&str]) -> Result<serde_json::Value, DriverError> {
    run_json("ceph", args).await
}

/// Run `rbd <args...> --format json` and parse stdout as JSON.
pub async fn rbd_cmd(args: &[&str]) -> Result<serde_json::Value, DriverError> {
    run_json("rbd", args).await
}

/// Create an RBD snapshot `pool/image@snap` (idempotent-ish; errors if it already exists).
pub async fn rbd_snap_create(pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let output = cmd("rbd")
        .args(["snap", "create", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd snap create {spec}: {stderr}"
        )));
    }
    Ok(())
}

/// Day-2 cross-cluster DR: per-image RBD mirroring op — `rbd mirror image enable <pool>/<image>
/// <mode>` / `disable` / `promote` [`--force`] / `demote` / `resync`. Real ops need a live second Ceph cluster
/// (see `docs/DR.md`). `force` only applies to `promote` (split-brain / non-clean failover).
pub async fn rbd_mirror_op(
    op: &str,
    pool: &str,
    image: &str,
    mode: &str,
    force: bool,
) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    let args: Vec<String> = match op {
        "enable" => vec![
            "mirror".into(),
            "image".into(),
            "enable".into(),
            spec.clone(),
            mode.into(),
        ],
        "disable" => vec![
            "mirror".into(),
            "image".into(),
            "disable".into(),
            spec.clone(),
        ],
        "promote" if force => vec![
            "mirror".into(),
            "image".into(),
            "promote".into(),
            spec.clone(),
            "--force".into(),
        ],
        "promote" => vec![
            "mirror".into(),
            "image".into(),
            "promote".into(),
            spec.clone(),
        ],
        "demote" => vec![
            "mirror".into(),
            "image".into(),
            "demote".into(),
            spec.clone(),
        ],
        "resync" => vec![
            "mirror".into(),
            "image".into(),
            "resync".into(),
            spec.clone(),
        ],
        other => return Err(DriverError::Backend(format!("unknown mirror op: {other}"))),
    };
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = cmd("rbd")
        .args(&refs)
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd {}: {stderr}",
            refs.join(" ")
        )));
    }
    Ok(())
}

/// Day-2 per-image QoS: `rbd config image set <pool>/<image> rbd_qos_{iops,bps}_limit <n>` to cap a
/// noisy volume's IOPS / bandwidth. A limit of `0` removes that cap. Only the provided limits are set.
pub async fn rbd_qos_set(
    pool: &str,
    image: &str,
    iops_limit: Option<i64>,
    bps_limit: Option<i64>,
) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    for (key, val) in [
        ("rbd_qos_iops_limit", iops_limit),
        ("rbd_qos_bps_limit", bps_limit),
    ] {
        let Some(v) = val else { continue };
        let vs = v.to_string();
        let output = cmd("rbd")
            .args(["config", "image", "set", &spec, key, &vs])
            .output()
            .await
            .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(DriverError::Backend(format!(
                "rbd config image set {spec} {key} {vs}: {stderr}"
            )));
        }
    }
    Ok(())
}

/// Day-2 OSD maintenance op: `ceph osd out|in <id>` (drain / return an OSD) or
/// `ceph osd reweight <id> <weight>` (rebalance data off/onto an OSD; weight in [0,1]).
pub async fn ceph_osd_op(op: &str, osd_id: i64, weight: Option<f64>) -> Result<(), DriverError> {
    let id = osd_id.to_string();
    let args: Vec<String> = match op {
        "out" => vec!["osd".into(), "out".into(), id],
        "in" => vec!["osd".into(), "in".into(), id],
        "reweight" => {
            let w =
                weight.ok_or_else(|| DriverError::Backend("reweight requires a weight".into()))?;
            if !(0.0..=1.0).contains(&w) {
                return Err(DriverError::Backend(
                    "reweight weight must be in [0.0, 1.0]".into(),
                ));
            }
            vec!["osd".into(), "reweight".into(), id, format!("{w}")]
        }
        other => return Err(DriverError::Backend(format!("unknown osd op: {other}"))),
    };
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = cmd("ceph")
        .args(&refs)
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `ceph`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "ceph {}: {stderr}",
            refs.join(" ")
        )));
    }
    Ok(())
}

/// Set + enable an RGW per-bucket quota via `radosgw-admin` (Rook doesn't always apply the OBC
/// additionalConfig quota, so Atlas enforces it directly with the admin keyring in the pod).
pub async fn radosgw_bucket_quota(
    bucket: &str,
    max_objects: Option<i64>,
    max_size: Option<&str>,
) -> Result<(), DriverError> {
    let mo = max_objects.map(|n| n.to_string());
    let mut set_args: Vec<&str> = vec!["quota", "set", "--bucket", bucket, "--quota-scope=bucket"];
    if let Some(ref n) = mo {
        set_args.push("--max-objects");
        set_args.push(n);
    }
    if let Some(sz) = max_size {
        set_args.push("--max-size");
        set_args.push(sz);
    }
    radosgw_admin(&set_args).await?;
    radosgw_admin(&[
        "quota",
        "enable",
        "--bucket",
        bucket,
        "--quota-scope=bucket",
    ])
    .await?;
    Ok(())
}

/// Run `radosgw-admin <args...> --format json` and parse stdout (e.g. `bucket stats`).
pub async fn radosgw_admin_json(args: &[&str]) -> Result<serde_json::Value, DriverError> {
    let output = tokio::time::timeout(
        RADOSGW_ADMIN_TIMEOUT,
        cmd("radosgw-admin")
            .args(args)
            .arg("--format")
            .arg("json")
            .output(),
    )
    .await
    .map_err(|_| DriverError::Unreachable("radosgw-admin timed out".into()))?
    .map_err(|e| DriverError::Unreachable(format!("failed to spawn `radosgw-admin`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "radosgw-admin {}: {stderr}",
            args.join(" ")
        )));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|e| DriverError::Parse(format!("radosgw-admin json: {e}")))
}

/// List an RBD image's snapshot names (`rbd snap ls pool/image --format json`).
pub async fn rbd_snap_list(pool: &str, image: &str) -> Result<Vec<String>, DriverError> {
    let spec = format!("{pool}/{image}");
    let v = rbd_cmd(&["snap", "ls", &spec]).await?;
    Ok(v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default())
}

/// Roll an RBD image back to a snapshot (`rbd snap rollback pool/image@snap`). Destructive.
pub async fn rbd_snap_rollback(pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let output = cmd("rbd")
        .args(["snap", "rollback", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd snap rollback {spec}: {stderr}"
        )));
    }
    Ok(())
}

async fn radosgw_admin(args: &[&str]) -> Result<(), DriverError> {
    let output = tokio::time::timeout(
        RADOSGW_ADMIN_TIMEOUT,
        cmd("radosgw-admin").args(args).output(),
    )
    .await
    .map_err(|_| DriverError::Unreachable("radosgw-admin timed out".into()))?
    .map_err(|e| DriverError::Unreachable(format!("failed to spawn `radosgw-admin`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "radosgw-admin {}: {stderr}",
            args.join(" ")
        )));
    }
    Ok(())
}

/// Create an RBD image directly (`rbd create pool/image --size <MiB>`) for non-CSI consumers.
pub async fn rbd_create(pool: &str, image: &str, size_bytes: i64) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    // Round up so the image is never smaller than requested when size_bytes isn't MiB-aligned.
    // NOTE: `i64::div_ceil` is unstable (int_roundings); do not "simplify" to it.
    // `saturating_add` guards a caller-supplied size near `i64::MAX`: a plain `+` would overflow
    // (panics in debug, silently wraps to garbage in release — e.g. creating a 1 MiB image when
    // an enormous size was requested instead of erroring).
    #[allow(clippy::manual_div_ceil)]
    let mib_val = size_bytes.saturating_add(1024 * 1024 - 1) / (1024 * 1024);
    let mib = std::cmp::max(1, mib_val).to_string();
    let output = cmd("rbd")
        .args(["create", &spec, "--size", &mib])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!("rbd create {spec}: {stderr}")));
    }
    Ok(())
}

/// Remove an RBD image directly (`rbd rm pool/image`; idempotent on "not found").
pub async fn rbd_remove(pool: &str, image: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    let output = cmd("rbd")
        .args(["rm", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("No such file") || stderr.contains("does not exist") {
            return Ok(());
        }
        return Err(DriverError::Backend(format!(
            "rbd rm {spec}: {}",
            stderr.trim()
        )));
    }
    Ok(())
}

/// Resize an RBD image (`rbd resize pool/image --size <MiB>`). Shrinking is refused by `rbd` unless
/// `allow_shrink` is set (day-2, guarded — a shrink can lose data past the new size).
pub async fn rbd_resize(
    pool: &str,
    image: &str,
    size_bytes: i64,
    allow_shrink: bool,
) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    // Round up so a grow never under-shoots and a shrink never removes more than requested.
    // NOTE: `i64::div_ceil` is unstable (int_roundings); do not "simplify" to it.
    // `saturating_add` guards a caller-supplied size near `i64::MAX` (see `rbd_create` above).
    #[allow(clippy::manual_div_ceil)]
    let mib_val = size_bytes.saturating_add(1024 * 1024 - 1) / (1024 * 1024);
    let mib = std::cmp::max(1, mib_val).to_string();
    let mut args = vec!["resize", &spec, "--size", &mib];
    if allow_shrink {
        args.push("--allow-shrink");
    }
    let output = cmd("rbd")
        .args(&args)
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!("rbd resize {spec}: {stderr}")));
    }
    Ok(())
}

/// Day-2: migrate an RBD image to another pool (`rbd migration prepare`→`execute`→`commit`). Live,
/// online migration. UNVERIFIED against real Ceph (needs the source + dest pools present).
pub async fn rbd_migrate(pool: &str, image: &str, dest_pool: &str) -> Result<(), DriverError> {
    let src = format!("{pool}/{image}");
    let dst = format!("{dest_pool}/{image}");
    for stage in [
        vec!["migration", "prepare", &src, &dst],
        vec!["migration", "execute", &dst],
        vec!["migration", "commit", &dst],
    ] {
        let output = cmd("rbd")
            .args(&stage)
            .output()
            .await
            .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(DriverError::Backend(format!(
                "rbd {}: {stderr}",
                stage.join(" ")
            )));
        }
    }
    Ok(())
}

/// Flatten a cloned image so it no longer depends on its parent snapshot (`rbd flatten`).
pub async fn rbd_flatten(pool: &str, image: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}");
    let output = cmd("rbd")
        .args(["flatten", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd flatten {spec}: {stderr}"
        )));
    }
    Ok(())
}

/// Total provisioned size of an RBD image in bytes (`rbd info pool/image --format json`).
pub async fn rbd_info_size(pool: &str, image: &str) -> Result<i64, DriverError> {
    let spec = format!("{pool}/{image}");
    let v = rbd_cmd(&["info", &spec]).await?;
    v.get("size")
        .and_then(|s| s.as_i64())
        .ok_or_else(|| DriverError::Parse(format!("rbd info {spec}: no size")))
}

/// Mirroring role of an RBD image from `rbd info pool/image --format json`: `Some(true)` primary,
/// `Some(false)` non-primary, `None` when mirroring is not enabled on the image.
pub async fn rbd_mirror_primary(pool: &str, image: &str) -> Result<Option<bool>, DriverError> {
    let spec = format!("{pool}/{image}");
    let v = rbd_cmd(&["info", &spec]).await?;
    Ok(v.get("mirroring")
        .filter(|m| m.get("state").and_then(|s| s.as_str()) == Some("enabled"))
        .and_then(|m| m.get("primary"))
        .and_then(|p| p.as_bool()))
}

/// This site's mirroring status for an image (`rbd mirror image status pool/image --format json`),
/// as written by the local `rbd-mirror` daemon, plus what each peer site reports.
pub async fn rbd_mirror_image_status(
    pool: &str,
    image: &str,
) -> Result<serde_json::Value, DriverError> {
    let spec = format!("{pool}/{image}");
    rbd_cmd(&["mirror", "image", "status", &spec]).await
}

/// Why a non-forced promote of this site's non-primary copy would not be a clean failback, given
/// its `rbd mirror image status` JSON; `None` when it is safe.
///
/// Ceph accepts a non-forced promote whenever the newest local mirror snapshot is a demotion,
/// including when this site never ran `rbd-mirror` and so never replayed the peer's later writes
/// (one-way topologies). Only the local daemon reporting `up` and "remote image demoted" shows
/// that it has caught up with the peer's demotion.
pub fn mirror_promote_blocker(status: &serde_json::Value) -> Option<String> {
    let state = status.get("state").and_then(|s| s.as_str()).unwrap_or("");
    let description = status
        .get("description")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    if !state.starts_with("up+") {
        return Some(format!(
            "no rbd-mirror daemon is replaying this image here (state '{state}'); this site may \
             be missing the peer's writes"
        ));
    }
    if !description.contains("remote image demoted") {
        return Some(format!(
            "the peer has not demoted its copy, or this site has not replayed it yet \
             (state '{state}', '{description}')"
        ));
    }
    None
}

/// Actual used (allocated) bytes of every image in a pool (`rbd du pool --format json`), as
/// `(image_name, used_size)` pairs. Used to enrich `rbd ls -l` (which has no `used_size` field).
pub async fn rbd_du_pool(pool: &str) -> Result<Vec<(String, i64)>, DriverError> {
    let v = rbd_cmd(&["du", pool]).await?;
    Ok(v.get("images")
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|img| {
                    let name = img.get("image").and_then(|n| n.as_str())?.to_string();
                    let used = img.get("used_size").and_then(|u| u.as_i64())?;
                    Some((name, used))
                })
                .collect()
        })
        .unwrap_or_default())
}

/// Actual used (allocated) bytes of an RBD image (`rbd du pool/image --format json`).
pub async fn rbd_du_image(pool: &str, image: &str) -> Result<i64, DriverError> {
    let spec = format!("{pool}/{image}");
    let v = rbd_cmd(&["du", &spec]).await?;
    v.get("images")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|img| img.get("used_size"))
        .and_then(|u| u.as_i64())
        .ok_or_else(|| DriverError::Parse(format!("rbd du {spec}: no used_size")))
}

/// Protect a snapshot so it can be used as a clone parent (`rbd snap protect`).
pub async fn rbd_snap_protect(pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    snap_op(&["snap", "protect"], pool, image, snap).await
}

/// Unprotect a snapshot (`rbd snap unprotect`); best-effort on "not protected".
pub async fn rbd_snap_unprotect(pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let output = cmd("rbd")
        .args(["snap", "unprotect", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("not protected")
            || stderr.contains("already unprotected")
            || stderr.contains("does not exist")
        {
            return Ok(());
        }
        return Err(DriverError::Backend(format!(
            "rbd snap unprotect {spec}: {}",
            stderr.trim()
        )));
    }
    Ok(())
}

/// Clone a protected snapshot into a new COW image (`rbd clone parent@snap clone`).
pub async fn rbd_clone(
    parent_pool: &str,
    parent_image: &str,
    snap: &str,
    clone_pool: &str,
    clone_image: &str,
) -> Result<(), DriverError> {
    let src = format!("{parent_pool}/{parent_image}@{snap}");
    let dst = format!("{clone_pool}/{clone_image}");
    let output = cmd("rbd")
        .args(["clone", &src, &dst])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd clone {src} {dst}: {stderr}"
        )));
    }
    Ok(())
}

async fn snap_op(op: &[&str], pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let mut args: Vec<&str> = op.to_vec();
    args.push(&spec);
    let output = cmd("rbd")
        .args(&args)
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd {} {spec}: {stderr}",
            op.join(" ")
        )));
    }
    Ok(())
}

/// List RBD image names in a pool (`rbd ls pool --format json`).
pub async fn rbd_list(pool: &str) -> Result<Vec<String>, DriverError> {
    let v = rbd_cmd(&["ls", pool]).await?;
    Ok(v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default())
}

/// Remove an RBD snapshot `pool/image@snap` (best-effort; ignores "not found").
pub async fn rbd_snap_rm(pool: &str, image: &str, snap: &str) -> Result<(), DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let output = cmd("rbd")
        .args(["snap", "rm", &spec])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("No such file") || stderr.contains("does not exist") {
            return Ok(());
        }
        return Err(DriverError::Backend(format!(
            "rbd snap rm {spec}: {}",
            stderr.trim()
        )));
    }
    Ok(())
}

/// Export an RBD snapshot as an incremental diff stream (`rbd export-diff pool/image@snap -`).
/// For a fresh/sparse image this is small; the caller must cap the size it buffers.
pub async fn rbd_export_diff(pool: &str, image: &str, snap: &str) -> Result<Vec<u8>, DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    let output = cmd("rbd")
        .args(["export-diff", &spec, "-"])
        .output()
        .await
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd export-diff {spec}: {stderr}"
        )));
    }
    Ok(output.stdout)
}

/// Spawn `rbd export-diff pool/image@snap -` with stdout piped, for streaming the diff elsewhere
/// (e.g. straight into an S3 multipart upload) without buffering it in memory. The caller takes
/// `child.stdout`, streams it, then `wait()`s and checks the exit status.
pub fn rbd_export_diff_child(
    pool: &str,
    image: &str,
    snap: &str,
) -> Result<tokio::process::Child, DriverError> {
    let spec = format!("{pool}/{image}@{snap}");
    cmd("rbd")
        .args(["export-diff", &spec, "-"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))
}

/// Spawn `rbd import-diff - pool/image` with stdin piped, for streaming a diff into it (e.g. from
/// an S3 download) without buffering. The caller writes to `child.stdin`, drops it, then `wait()`s.
pub fn rbd_import_diff_child(
    pool: &str,
    image: &str,
) -> Result<tokio::process::Child, DriverError> {
    let spec = format!("{pool}/{image}");
    cmd("rbd")
        .args(["import-diff", "-", &spec])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))
}

/// Apply an RBD diff stream (from `rbd export-diff`) into an existing image via stdin
/// (`rbd import-diff - pool/image`). Used by restore-from-data.
pub async fn rbd_import_diff(pool: &str, image: &str, data: Vec<u8>) -> Result<(), DriverError> {
    use tokio::io::AsyncWriteExt;
    let spec = format!("{pool}/{image}");
    let mut child = cmd("rbd")
        .args(["import-diff", "-", &spec])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| DriverError::Unreachable(format!("failed to spawn `rbd`: {e}")))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| DriverError::Backend("rbd import-diff: no stdin".into()))?;
        stdin
            .write_all(&data)
            .await
            .map_err(|e| DriverError::Backend(format!("rbd import-diff write: {e}")))?;
        let _ = stdin.shutdown().await;
    }
    let output = child
        .wait_with_output()
        .await
        .map_err(|e| DriverError::Backend(format!("rbd import-diff wait: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "rbd import-diff {spec}: {stderr}"
        )));
    }
    Ok(())
}

async fn run_json(bin: &str, args: &[&str]) -> Result<serde_json::Value, DriverError> {
    let output = cmd(bin)
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .await
        .map_err(|e| {
            // e.g. binary missing, or no permission — treat as backend unreachable.
            DriverError::Unreachable(format!("failed to spawn `{bin}`: {e}"))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(DriverError::Backend(format!(
            "`{bin} {}` failed: {stderr}",
            args.join(" ")
        )));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|e| DriverError::Parse(format!("`{bin}` json: {e}")))
}

#[cfg(test)]
mod tests {
    use super::mirror_promote_blocker;
    use serde_json::json;

    #[test]
    fn promote_is_clean_only_after_replaying_the_peer_demotion() {
        let ready = json!({ "state": "up+unknown", "description": "remote image demoted" });
        assert_eq!(mirror_promote_blocker(&ready), None);

        let replaying = json!({ "state": "up+replaying", "description": "replaying, {}" });
        assert!(mirror_promote_blocker(&replaying)
            .unwrap()
            .contains("not demoted"));

        let no_daemon = json!({ "state": "down+unknown", "description": "remote image demoted" });
        assert!(mirror_promote_blocker(&no_daemon)
            .unwrap()
            .contains("no rbd-mirror daemon"));

        assert!(mirror_promote_blocker(&json!({})).is_some());
    }
}
