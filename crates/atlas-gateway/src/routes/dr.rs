// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use atlas_common::{ids, AppError, AppResult};
use atlas_jobs::JobSpec;

use super::util::accepted;
use crate::auth::Actor;
use crate::state::AppState;

// ---- cross-cluster DR (RBD mirroring; real ops drilled two-way on a lab, see docs/DR.md) ----

#[derive(Debug, Deserialize)]
pub(crate) struct PeerBody {
    name: String,
    cluster_fsid: Option<String>,
    direction: Option<String>,
    /// k8s Secret holding the peer bootstrap token (never the token itself).
    secret_ref: Option<String>,
}

/// `POST /dr/peers` — register a mirroring peer cluster (admin).
/// `DELETE /dr/peers/{id}` — remove a mirroring peer (and any mirrors that referenced it).
pub(crate) async fn delete_dr_peer(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_ADMIN)?;
    atlas_inventory::dr::delete_peer(&s.pool, &id).await?;
    Ok(Json(json!({ "peer_id": id, "deleted": true })))
}

pub(crate) async fn register_dr_peer(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(body): Json<PeerBody>,
) -> AppResult<(StatusCode, Json<Value>)> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_ADMIN)?;
    if body.name.trim().is_empty() {
        return Err(AppError::Validation("name is required".into()));
    }
    let id = ids::stable_id("drp", &body.name);
    let direction = body.direction.as_deref().unwrap_or("rx-tx");
    atlas_inventory::dr::register_peer(
        &s.pool,
        &id,
        &body.name,
        body.cluster_fsid.as_deref(),
        direction,
        body.secret_ref.as_deref(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": id, "name": body.name, "direction": direction })),
    ))
}

pub(crate) async fn list_dr_peers(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    Ok(Json(json!(atlas_inventory::dr::list_peers(&s.pool).await?)))
}

pub(crate) async fn list_dr_mirrors(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    Ok(Json(json!(
        atlas_inventory::dr::list_mirrors(&s.pool).await?
    )))
}

/// `GET /dr/status` — DR posture: mirror counts by role/state + worst RPO.
pub(crate) async fn dr_status(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    let mirrors = atlas_inventory::dr::list_mirrors(&s.pool).await?;
    let primaries = mirrors.iter().filter(|m| m["role"] == "primary").count();
    let secondaries = mirrors.iter().filter(|m| m["role"] == "secondary").count();
    let errored = mirrors.iter().filter(|m| m["state"] == "error").count();
    let worst_rpo = mirrors
        .iter()
        .filter_map(|m| m["rpo_seconds"].as_i64())
        .max();
    let peers = atlas_inventory::dr::list_peers(&s.pool).await?.len();
    let control_plane_ready = peers > 0 && errored == 0;
    Ok(Json(json!({
        "peers": peers, "mirrors": mirrors.len(),
        "primary": primaries, "secondary": secondaries,
        "error": errored, "worst_rpo_seconds": worst_rpo,
        "control_plane_ready": control_plane_ready,
        "dataplane_verified": s.config.dr_dataplane_verified,
        "verified": s.config.dr_dataplane_verified,
        "note": if s.config.dr_dataplane_verified {
            "control-plane catalog ready; this deployment has completed the live two-site rbd mirror drill — see docs/DR.md"
        } else {
            "control-plane catalog ready; live rbd mirror needs a second Ceph cluster — see docs/DR.md"
        },
    })))
}

/// `GET /dr/preflight` — control-plane checklist before a failover drill (operator).
pub(crate) async fn dr_preflight(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    Ok(Json(
        atlas_inventory::dr::preflight(&s.pool, s.config.dr_dataplane_verified).await?,
    ))
}

/// Resolve a volume id to its `(pool, image)` (direct-RBD `rbd:<pool>/<image>` native id).
pub(crate) async fn rbd_of_volume(s: &AppState, volume_id: &str) -> AppResult<(String, String)> {
    let vols = atlas_inventory::list_volumes(&s.pool).await?;
    let v = vols
        .iter()
        .find(|v| v.id == volume_id)
        .ok_or_else(|| AppError::NotFound(format!("volume {volume_id}")))?;
    let native = v.backend_native_id.as_deref().unwrap_or("");
    let rest = native
        .strip_prefix("rbd:")
        .ok_or_else(|| AppError::Validation("volume is not a direct RBD image".into()))?;
    let (pool, image) = rest
        .split_once('/')
        .ok_or_else(|| AppError::Validation("malformed rbd native id".into()))?;
    Ok((pool.to_string(), image.to_string()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct MirrorQuery {
    mode: Option<String>,
    peer: Option<String>,
    /// `secondary`: record this site's copy of an image the peer mirrors to us instead of enabling
    /// mirroring here. Defaults to `primary`.
    role: Option<String>,
}

/// `POST /volumes/{id}/mirror?mode=snapshot&peer=<id>[&role=secondary]` — enable RBD mirroring for
/// a volume, or register the peer site's non-primary copy so it can later be promoted (admin).
pub(crate) async fn enable_mirror(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(volume_id): Path<String>,
    Query(q): Query<MirrorQuery>,
) -> AppResult<(StatusCode, Json<Value>)> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_ADMIN)?;
    let (rbd_pool, image) = rbd_of_volume(&s, &volume_id).await?;
    let mode = q.mode.as_deref().unwrap_or("snapshot");
    if !matches!(mode, "snapshot" | "journal") {
        return Err(AppError::Validation(
            "mode must be snapshot or journal".into(),
        ));
    }
    let peer = if let Some(peer) = q.peer.as_deref() {
        if !atlas_inventory::dr::peer_exists(&s.pool, peer).await? {
            return Err(AppError::Validation(format!(
                "unknown DR peer '{peer}' — register it via POST /dr/peers first"
            )));
        }
        Some(peer.to_string())
    } else {
        let peers = atlas_inventory::dr::list_peers(&s.pool).await?;
        if peers.is_empty() {
            return Err(AppError::Validation(
                "no DR peers registered — POST /dr/peers before enabling mirroring".into(),
            ));
        }
        None
    };
    let role = q.role.as_deref().unwrap_or("primary");
    if !matches!(role, "primary" | "secondary") {
        return Err(AppError::Validation(
            "role must be primary or secondary".into(),
        ));
    }
    let mirror_id = ids::stable_id("drm", &format!("{rbd_pool}/{image}"));
    let real = matches!(
        s.config.ceph_driver_mode,
        atlas_common::config::CephDriverMode::Real
    );
    let primary_here = if real {
        atlas_driver_ceph::rbd_mirror_primary(&rbd_pool, &image)
            .await
            .map_err(|e| AppError::Unavailable(format!("rbd info {rbd_pool}/{image}: {e}")))?
    } else {
        None
    };
    // `rbd mirror image enable` on the peer's replicated copy is a successful no-op, so without
    // this check that copy would be catalogued as an enabled primary.
    if role == "primary" && primary_here == Some(false) {
        return Err(AppError::Conflict(format!(
            "{rbd_pool}/{image} is the peer's non-primary copy — register it with role=secondary"
        )));
    }
    if role == "secondary" {
        if real {
            match primary_here {
                Some(false) => {}
                Some(true) => {
                    return Err(AppError::Conflict(format!(
                        "{rbd_pool}/{image} is primary on this cluster — enable it as primary instead"
                    )))
                }
                None => {
                    return Err(AppError::Conflict(format!(
                        "{rbd_pool}/{image} is not mirrored here yet — enable it on the peer and wait for rbd-mirror to create it"
                    )))
                }
            }
        }
        atlas_inventory::dr::upsert_mirror(
            &s.pool,
            &mirror_id,
            "global",
            Some(&volume_id),
            &rbd_pool,
            &image,
            peer.as_deref(),
            mode,
            "secondary",
            "enabled",
        )
        .await?;
        return Ok((
            StatusCode::CREATED,
            Json(json!({
                "mirror_id": mirror_id, "rbd": format!("{rbd_pool}/{image}"),
                "role": "secondary", "state": "enabled",
            })),
        ));
    }
    let state = if real { "enabling" } else { "enabled" };
    atlas_inventory::dr::upsert_mirror(
        &s.pool,
        &mirror_id,
        "global",
        Some(&volume_id),
        &rbd_pool,
        &image,
        peer.as_deref(),
        mode,
        "primary",
        state,
    )
    .await?;
    let job_id = ids::job_id();
    let spec = JobSpec::RbdMirror {
        mirror_id: mirror_id.clone(),
        pool: rbd_pool.clone(),
        image: image.clone(),
        action: "enable".into(),
        mode: mode.to_string(),
        force: false,
    };
    let job = s
        .jobs
        .enqueue(&job_id, "global", &actor.id, spec, None)
        .await?;
    Ok(accepted(
        &job,
        json!({ "mirror_id": mirror_id, "rbd": format!("{rbd_pool}/{image}"), "state": state }),
    ))
}

/// `DELETE /volumes/{id}/mirror` — disable RBD mirroring for a volume (admin).
pub(crate) async fn disable_mirror(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(volume_id): Path<String>,
) -> AppResult<(StatusCode, Json<Value>)> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_ADMIN)?;
    let (rbd_pool, image) = rbd_of_volume(&s, &volume_id).await?;
    let mirror_id = ids::stable_id("drm", &format!("{rbd_pool}/{image}"));
    let real = matches!(
        s.config.ceph_driver_mode,
        atlas_common::config::CephDriverMode::Real
    );
    atlas_inventory::dr::set_mirror(
        &s.pool,
        &mirror_id,
        "primary",
        if real { "disabling" } else { "disabled" },
    )
    .await?;
    let job_id = ids::job_id();
    let spec = JobSpec::RbdMirror {
        mirror_id: mirror_id.clone(),
        pool: rbd_pool.clone(),
        image: image.clone(),
        action: "disable".into(),
        mode: "snapshot".into(),
        force: false,
    };
    let job = s
        .jobs
        .enqueue(&job_id, "global", &actor.id, spec, None)
        .await?;
    Ok(accepted(
        &job,
        json!({ "mirror_id": mirror_id, "state": "disabling" }),
    ))
}

#[derive(Debug, Deserialize)]
pub(crate) struct PromoteQuery {
    /// Split-brain / non-clean failover: pass to `rbd mirror image promote --force`.
    force: Option<bool>,
}

/// `POST /dr/mirrors/{id}/promote?force=0|1` — failover: promote this cluster's copy to primary.
pub(crate) async fn promote_mirror(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Query(q): Query<PromoteQuery>,
) -> AppResult<(StatusCode, Json<Value>)> {
    mirror_role_op(&s, &actor, &id, "promote", q.force.unwrap_or(false)).await
}

/// `GET /dr/mirrors/{id}/status` — live `rbd mirror image status` of this site's copy and whether a
/// non-forced promote would be a clean failback (operator).
pub(crate) async fn mirror_status(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    let (rbd_pool, image, role, state) = atlas_inventory::dr::mirror_detail(&s.pool, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("mirror {id}")))?;
    let real = matches!(
        s.config.ceph_driver_mode,
        atlas_common::config::CephDriverMode::Real
    );
    let (live, blocker) = if real {
        let status = atlas_driver_ceph::rbd_mirror_image_status(&rbd_pool, &image)
            .await
            .map_err(|e| {
                AppError::Unavailable(format!("rbd mirror image status {rbd_pool}/{image}: {e}"))
            })?;
        let blocker = atlas_driver_ceph::mirror_promote_blocker(&status);
        let live = json!({
            "state": status.get("state"),
            "description": status.get("description"),
            "last_update": status.get("last_update"),
            "peer_sites": status.get("peer_sites"),
        });
        (Some(live), blocker)
    } else {
        (None, None)
    };
    let blocker = if role != "secondary" {
        Some(format!("this site's copy is {role}"))
    } else {
        blocker
    };
    Ok(Json(json!({
        "mirror_id": id, "rbd": format!("{rbd_pool}/{image}"),
        "role": role, "state": state, "live": live,
        "promote_ready": blocker.is_none(), "promote_blocker": blocker,
    })))
}

/// `POST /dr/mirrors/{id}/demote` — demote this cluster's copy to secondary (admin).
pub(crate) async fn demote_mirror(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> AppResult<(StatusCode, Json<Value>)> {
    mirror_role_op(&s, &actor, &id, "demote", false).await
}

/// `POST /dr/mirrors/{id}/resync` — discard this cluster's non-primary copy and pull a full copy
/// from the peer's primary; the way out of split-brain after a forced or one-way failback (admin).
pub(crate) async fn resync_mirror(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> AppResult<(StatusCode, Json<Value>)> {
    mirror_role_op(&s, &actor, &id, "resync", false).await
}

pub(crate) async fn mirror_role_op(
    s: &AppState,
    actor: &Actor,
    id: &str,
    action: &str,
    force: bool,
) -> AppResult<(StatusCode, Json<Value>)> {
    crate::auth::require_role(s.config.auth_required, actor, crate::auth::ROLE_ADMIN)?;
    let (rbd_pool, image, role, state) = atlas_inventory::dr::mirror_detail(&s.pool, id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("mirror {id}")))?;
    if matches!(state.as_str(), "disabled" | "disabling") {
        return Err(AppError::Conflict(format!(
            "mirror {id} is {state} — re-enable mirroring before {action}"
        )));
    }
    match action {
        "promote" if role == "primary" && !force => {
            return Err(AppError::Conflict(format!(
                "mirror {id} is already primary — pass ?force=1 only for split-brain recovery"
            )));
        }
        "demote" if role == "secondary" => {
            return Err(AppError::Conflict(format!(
                "mirror {id} is already secondary"
            )));
        }
        "resync" if role != "secondary" => {
            return Err(AppError::Conflict(format!(
                "resync discards this copy and needs role=secondary (have {role}); demote first"
            )));
        }
        "promote" if role != "secondary" && !force => {
            return Err(AppError::Conflict(format!(
                "promote requires role=secondary (have {role}); use ?force=1 for unclean failover"
            )));
        }
        _ => {}
    }
    let real = matches!(
        s.config.ceph_driver_mode,
        atlas_common::config::CephDriverMode::Real
    );
    if real && action == "promote" && !force {
        let status = atlas_driver_ceph::rbd_mirror_image_status(&rbd_pool, &image)
            .await
            .map_err(|e| {
                AppError::Unavailable(format!("rbd mirror image status {rbd_pool}/{image}: {e}"))
            })?;
        if let Some(blocker) = atlas_driver_ceph::mirror_promote_blocker(&status) {
            return Err(AppError::Conflict(format!(
                "promoting {rbd_pool}/{image} would not be a clean failback: {blocker}; demote \
                 the peer and wait for GET /dr/mirrors/{id}/status to report promote_ready, or \
                 pass ?force=1 and resync the peer afterwards"
            )));
        }
    }
    if !real {
        let new_role = if action == "promote" {
            "primary"
        } else {
            "secondary"
        };
        atlas_inventory::dr::set_mirror(&s.pool, id, new_role, "enabled").await?;
        if action == "promote" {
            let _ = atlas_inventory::dr::record_failover(&s.pool, id, force).await;
        }
    } else {
        let pending = match action {
            "promote" => "promoting",
            "resync" => "resyncing",
            _ => "demoting",
        };
        atlas_inventory::dr::set_mirror(&s.pool, id, &role, pending).await?;
    }
    let job_id = ids::job_id();
    let spec = JobSpec::RbdMirror {
        mirror_id: id.to_string(),
        pool: rbd_pool,
        image,
        action: action.to_string(),
        mode: "snapshot".into(),
        force,
    };
    let job = s
        .jobs
        .enqueue(&job_id, "global", &actor.id, spec, None)
        .await?;
    let _ = atlas_inventory::audit::record(
        &s.pool,
        None,
        &actor.id,
        &format!("dr.mirror.{action}"),
        "dr_mirror",
        id,
        "ok",
        Some(json!({ "force": force })),
        None,
    )
    .await;
    Ok(accepted(
        &job,
        json!({ "mirror_id": id, "action": action, "force": force }),
    ))
}

#[derive(Debug, Deserialize)]
pub(crate) struct FailoverBody {
    mirror_id: String,
    confirm: bool,
    #[serde(default)]
    force: bool,
}

/// `POST /dr/failover` — one-click failover runbook: promote a secondary (admin, confirm required).
pub(crate) async fn dr_failover(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(body): Json<FailoverBody>,
) -> AppResult<(StatusCode, Json<Value>)> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_ADMIN)?;
    if !body.confirm {
        return Err(AppError::Validation(
            "confirm=true is required for failover (destructive)".into(),
        ));
    }
    let pre = atlas_inventory::dr::preflight(&s.pool, s.config.dr_dataplane_verified).await?;
    if pre["ready"] == false && !body.force {
        return Err(AppError::Conflict(format!(
            "DR preflight not ready: {}; pass force=true to override",
            pre["blockers"]
        )));
    }
    let result = mirror_role_op(&s, &actor, &body.mirror_id, "promote", body.force).await?;
    let _ = atlas_inventory::audit::record(
        &s.pool,
        None,
        &actor.id,
        "dr.failover",
        "dr_mirror",
        &body.mirror_id,
        "ok",
        Some(json!({ "force": body.force, "preflight": pre })),
        None,
    )
    .await;
    Ok(result)
}

#[derive(Debug, Deserialize)]
pub(crate) struct RpoBody {
    rpo_seconds: Option<i64>,
}

/// `POST /dr/mirrors/{id}/rpo` — record observed RPO seconds (operator).
pub(crate) async fn set_mirror_rpo(
    State(s): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Json(body): Json<RpoBody>,
) -> AppResult<Json<Value>> {
    crate::auth::require_role(s.config.auth_required, &actor, crate::auth::ROLE_OPERATOR)?;
    if !atlas_inventory::dr::set_rpo(&s.pool, &id, body.rpo_seconds).await? {
        return Err(AppError::NotFound(format!("mirror {id}")));
    }
    Ok(Json(
        json!({ "mirror_id": id, "rpo_seconds": body.rpo_seconds }),
    ))
}
