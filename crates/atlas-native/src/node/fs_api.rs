// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! `/v1/fs/...`: the inode-based file API. Paths address inodes by number; names travel in JSON
//! bodies (or a percent-encoded `name` query on lookup). A snapshot tree is read as `<fs>@<id>`.
//! Only the metadata leader answers, unless a read passes `?stale=1`.

use serde_json::json;

use super::{body_json, client_id, query_u64, read_range, NodeShared};
use crate::{
    engine::{NativeEngine, NativeError, NewNode},
    http::{Request, Response},
    namespace::{SetAttr, XattrMode},
};

type Routed = Result<Response, NativeError>;

pub(super) fn route(sh: &NodeShared, e: &NativeEngine, req: &Request, segs: &[&str]) -> Routed {
    // A follower's catalog can lag the leader, so a client could miss its own writes there.
    // `?stale=1` opts into reading whatever this replica has applied.
    if req.method != "GET" || !req.query.contains_key("stale") {
        e.ensure_leader()?;
    }
    match (req.method.as_str(), segs) {
        ("GET", ["v1", "fs"]) => Ok(Response::json(
            200,
            &json!({ "filesystems": e.filesystems()? }),
        )),
        ("POST", ["v1", "fs"]) => {
            let body = parse(req)?;
            let name = str_field(&body, "name")?;
            let id = client_id(&body).map_err(bad)?;
            let extent_bytes = match body.get("extent_bytes") {
                None | Some(serde_json::Value::Null) => None,
                Some(v) => Some(v.as_u64().ok_or_else(|| {
                    NativeError::Invalid("extent_bytes must be an integer".into())
                })?),
            };
            Ok(Response::json(
                201,
                &json!({ "id": e.create_fs_with(id, name, extent_bytes)? }),
            ))
        }
        ("DELETE", ["v1", "fs", fs]) => {
            e.delete_fs(fs)?;
            Ok(Response::text(204, ""))
        }
        ("GET", ["v1", "fs", fs, "statfs"]) => Ok(Response::json(200, &json!(e.fs_statfs(fs)?))),
        ("POST", ["v1", "fs", fs, "rename"]) => {
            let body = parse(req)?;
            e.fs_rename(
                fs,
                u64_field(&body, "parent")?,
                str_field(&body, "name")?,
                u64_field(&body, "new_parent")?,
                str_field(&body, "new_name")?,
            )?;
            Ok(Response::text(204, ""))
        }
        ("POST", ["v1", "fs", fs, "snapshots"]) => {
            let body = parse(req)?;
            let name = str_field(&body, "name")?;
            let id = client_id(&body).map_err(bad)?;
            Ok(Response::json(
                201,
                &json!({ "id": e.snapshot_fs_as(id, fs, name)? }),
            ))
        }
        ("GET", ["v1", "fs-snapshots"]) => Ok(Response::json(
            200,
            &json!({ "snapshots": e.fs_snapshots()? }),
        )),
        ("DELETE", ["v1", "fs-snapshots", id]) => {
            e.delete_fs_snapshot(id)?;
            Ok(Response::text(204, ""))
        }
        ("POST", ["v1", "fs-snapshots", id, "clone"]) => {
            let body = parse(req)?;
            let name = str_field(&body, "name")?;
            let fid = client_id(&body).map_err(bad)?;
            Ok(Response::json(
                201,
                &json!({ "id": e.clone_fs_as(fid, id, name)? }),
            ))
        }
        (method, ["v1", "fs", fs, "inodes", ino, rest @ ..]) => {
            let ino: u64 = ino
                .parse()
                .map_err(|_| NativeError::Invalid(format!("inode {ino:?} is not a number")))?;
            inode_route(sh, e, req, method, fs, ino, rest)
        }
        _ => Ok(Response::text(404, "no such route")),
    }
}

fn inode_route(
    sh: &NodeShared,
    e: &NativeEngine,
    req: &Request,
    method: &str,
    fs: &str,
    ino: u64,
    rest: &[&str],
) -> Routed {
    let attr = |a| Ok(Response::json(200, &json!(a)));
    match (method, rest) {
        ("GET", []) => attr(e.fs_getattr(fs, ino)?),
        ("POST", ["attr"]) => {
            let a: SetAttr = serde_json::from_slice(&req.body)
                .map_err(|err| NativeError::Invalid(format!("invalid attributes: {err}")))?;
            attr(e.fs_setattr(fs, ino, a)?)
        }
        ("GET", ["lookup"]) => {
            let name = req
                .query
                .get("name")
                .ok_or_else(|| NativeError::Invalid("query parameter name is required".into()))
                .and_then(|n| pct_decode(n))?;
            attr(e.fs_lookup(fs, ino, &name)?)
        }
        ("GET", ["entries"]) => Ok(Response::json(
            200,
            &json!({ "entries": e.fs_readdir(fs, ino)? }),
        )),
        ("POST", ["entries"]) => {
            let node: NewNode = serde_json::from_slice(&req.body)
                .map_err(|err| NativeError::Invalid(format!("invalid node: {err}")))?;
            Ok(Response::json(201, &json!(e.fs_mknode(fs, ino, node)?)))
        }
        ("POST", ["unlink"]) => {
            e.fs_unlink(fs, ino, str_field(&parse(req)?, "name")?)?;
            Ok(Response::text(204, ""))
        }
        ("POST", ["rmdir"]) => {
            e.fs_rmdir(fs, ino, str_field(&parse(req)?, "name")?)?;
            Ok(Response::text(204, ""))
        }
        ("POST", ["links"]) => {
            let body = parse(req)?;
            attr(e.fs_link(
                fs,
                ino,
                u64_field(&body, "parent")?,
                str_field(&body, "name")?,
            )?)
        }
        ("GET", ["xattrs"]) => Ok(Response::json(
            200,
            &json!({ "names": e.fs_listxattr(fs, ino)? }),
        )),
        ("GET", ["xattrs", name]) => Ok(Response::bytes(
            200,
            e.fs_getxattr(fs, ino, &pct_decode(name)?)?,
        )),
        ("PUT", ["xattrs", name]) => {
            let mode = match req.query.get("mode").map(String::as_str) {
                None | Some("set") => XattrMode::Set,
                Some("create") => XattrMode::Create,
                Some("replace") => XattrMode::Replace,
                Some(m) => return Err(NativeError::Invalid(format!("unknown xattr mode {m:?}"))),
            };
            e.fs_setxattr(fs, ino, &pct_decode(name)?, &req.body, mode)?;
            Ok(Response::text(204, ""))
        }
        ("DELETE", ["xattrs", name]) => {
            e.fs_removexattr(fs, ino, &pct_decode(name)?)?;
            Ok(Response::text(204, ""))
        }
        ("GET", ["target"]) => Ok(Response::json(
            200,
            &json!({ "target": e.fs_readlink(fs, ino)? }),
        )),
        ("GET", ["data"]) => {
            let (offset, len) = match read_range(sh, req) {
                Ok(r) => r,
                Err(r) => return Ok(r),
            };
            Ok(Response::bytes(200, e.read_file(fs, ino, offset, len)?))
        }
        ("PUT", ["data"]) => {
            let offset = match query_u64(req, "offset") {
                Ok(o) => o,
                Err(r) => return Ok(r),
            };
            attr(e.write_file(fs, ino, offset, &req.body)?)
        }
        _ => Ok(Response::text(404, "no such route")),
    }
}

fn bad(r: Response) -> NativeError {
    NativeError::Invalid(String::from_utf8_lossy(&r.body).into_owned())
}

fn parse(req: &Request) -> Result<serde_json::Value, NativeError> {
    body_json(req).map_err(bad)
}

fn str_field<'a>(body: &'a serde_json::Value, key: &str) -> Result<&'a str, NativeError> {
    body[key]
        .as_str()
        .ok_or_else(|| NativeError::Invalid(format!("body field {key:?} (string) is required")))
}

fn u64_field(body: &serde_json::Value, key: &str) -> Result<u64, NativeError> {
    body[key]
        .as_u64()
        .ok_or_else(|| NativeError::Invalid(format!("body field {key:?} (integer) is required")))
}

/// Decodes `%XX` escapes (and `+` as a space) into a UTF-8 string.
fn pct_decode(s: &str) -> Result<String, NativeError> {
    let bad = || NativeError::Invalid(format!("malformed percent-encoding in {s:?}"));
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'%' => {
                let hex = [bytes.next().ok_or_else(bad)?, bytes.next().ok_or_else(bad)?];
                let hex = std::str::from_utf8(&hex).map_err(|_| bad())?;
                out.push(u8::from_str_radix(hex, 16).map_err(|_| bad())?);
            }
            b'+' => out.push(b' '),
            b => out.push(b),
        }
    }
    String::from_utf8(out).map_err(|_| NativeError::Invalid("name is not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::pct_decode;

    #[test]
    fn decodes_percent_escapes() {
        assert_eq!(pct_decode("a%20b+c%2Fd%C3%A9").unwrap(), "a b c/dé");
        assert!(pct_decode("%4").is_err());
        assert!(pct_decode("%zz").is_err());
        assert!(pct_decode("%FF").is_err());
    }
}
