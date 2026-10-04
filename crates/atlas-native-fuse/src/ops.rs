// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Filesystem operations against one native filesystem, independent of the kernel interface:
//! every call returns a value or an errno. Attributes and name lookups are cached for a short
//! TTL; writes are buffered per inode and sent on fsync, close, a non-sequential write, or once
//! a run reaches the flush size.

use std::{sync::Mutex, time::Duration};

use atlas_native::{
    engine::{Attr, DirEntry, FsStat, NewNode},
    NodeType, SetAttr,
};
use reqwest::Method;
use serde_json::json;

use crate::{
    cache::TtlCache,
    client::{encode, Body, Client, Error, Retry},
    writeback::{Dirty, WriteBack},
};

pub type Errno = i32;

#[derive(Debug, Clone)]
pub struct OpsConfig {
    /// How long attributes and name lookups may be served from cache (also the kernel TTL).
    pub ttl: Duration,
    /// Write-back run size; 0 sends every write immediately.
    pub writeback_bytes: usize,
    /// Largest single read or write request sent to the cluster.
    pub max_io_bytes: usize,
}

impl Default for OpsConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(1),
            writeback_bytes: 4 << 20,
            max_io_bytes: 8 << 20,
        }
    }
}

pub struct Ops {
    client: Client,
    /// `<fs>` or `<fs>@<snapshot>` (read-only).
    fs: String,
    pub cfg: OpsConfig,
    attrs: Mutex<TtlCache<u64, Attr>>,
    names: Mutex<TtlCache<(u64, String), u64>>,
    dirty: Mutex<WriteBack>,
}

fn errno(e: Error) -> Errno {
    tracing::debug!(error = %e, "native fs call failed");
    e.errno()
}

fn decode<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> Result<T, Errno> {
    serde_json::from_value(v).map_err(|e| {
        tracing::warn!(error = %e, "undecodable response");
        libc::EIO
    })
}

fn check_name(name: &str) -> Result<(), Errno> {
    if name.len() > atlas_native::namespace::MAX_NAME_BYTES {
        return Err(libc::ENAMETOOLONG);
    }
    Ok(())
}

impl Ops {
    pub fn new(client: Client, fs: impl Into<String>, cfg: OpsConfig) -> Self {
        Self {
            client,
            fs: fs.into(),
            attrs: Mutex::new(TtlCache::new(cfg.ttl)),
            names: Mutex::new(TtlCache::new(cfg.ttl)),
            dirty: Mutex::new(WriteBack::new(cfg.writeback_bytes)),
            cfg,
        }
    }

    pub fn read_only(&self) -> bool {
        self.fs.contains('@')
    }

    fn path(&self, rest: &str) -> String {
        format!("/v1/fs/{}{rest}", self.fs)
    }

    fn call(
        &self,
        method: Method,
        rest: &str,
        body: Body<'_>,
        retry: Retry,
    ) -> Result<serde_json::Value, Errno> {
        self.client
            .json(method, &self.path(rest), body, retry)
            .map_err(errno)
    }

    fn void(&self, method: Method, rest: &str, body: Body<'_>, retry: Retry) -> Result<(), Errno> {
        self.client
            .request(method, &self.path(rest), body, retry)
            .map(drop)
            .map_err(errno)
    }

    /// Caches `a` and returns it with any buffered bytes counted in its size.
    fn remember(&self, mut a: Attr) -> Attr {
        if let Ok(mut c) = self.attrs.lock() {
            c.put(a.ino, a.clone());
        }
        if let Some(end) = self.dirty.lock().ok().and_then(|d| d.end(a.ino)) {
            a.size = a.size.max(end);
        }
        a
    }

    fn forget_attr(&self, ino: u64) {
        if let Ok(mut c) = self.attrs.lock() {
            c.remove(&ino);
        }
    }

    pub fn getattr(&self, ino: u64) -> Result<Attr, Errno> {
        let cached = self.attrs.lock().ok().and_then(|mut c| c.get(&ino));
        let a = match cached {
            Some(a) => a,
            None => decode(self.call(
                Method::GET,
                &format!("/inodes/{ino}"),
                Body::Empty,
                Retry::Idempotent,
            )?)?,
        };
        Ok(self.remember(a))
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Result<Attr, Errno> {
        check_name(name)?;
        let key = (parent, name.to_string());
        if let Some(ino) = self.names.lock().ok().and_then(|mut n| n.get(&key)) {
            if let Ok(a) = self.getattr(ino) {
                return Ok(a);
            }
        }
        let a: Attr = decode(self.call(
            Method::GET,
            &format!("/inodes/{parent}/lookup?name={}", encode(name)),
            Body::Empty,
            Retry::Idempotent,
        )?)?;
        if let Ok(mut n) = self.names.lock() {
            n.put(key, a.ino);
        }
        Ok(self.remember(a))
    }

    pub fn readdir(&self, ino: u64) -> Result<Vec<DirEntry>, Errno> {
        let v = self.call(
            Method::GET,
            &format!("/inodes/{ino}/entries"),
            Body::Empty,
            Retry::Idempotent,
        )?;
        decode(v["entries"].clone())
    }

    pub fn readlink(&self, ino: u64) -> Result<String, Errno> {
        let v = self.call(
            Method::GET,
            &format!("/inodes/{ino}/target"),
            Body::Empty,
            Retry::Idempotent,
        )?;
        v["target"].as_str().map(str::to_string).ok_or(libc::EIO)
    }

    pub fn statfs(&self) -> Result<FsStat, Errno> {
        decode(self.call(Method::GET, "/statfs", Body::Empty, Retry::Idempotent)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mknode(
        &self,
        parent: u64,
        name: &str,
        kind: NodeType,
        target: Option<String>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Attr, Errno> {
        check_name(name)?;
        let node = NewNode {
            name: name.into(),
            // Fresh per call: a retry of this request is recognised, a later create is EEXIST.
            op_id: uuid::Uuid::new_v4().to_string(),
            kind,
            target,
            mode,
            uid,
            gid,
        };
        let body = serde_json::to_value(&node).map_err(|_| libc::EINVAL)?;
        let a: Attr = decode(self.call(
            Method::POST,
            &format!("/inodes/{parent}/entries"),
            Body::Json(body),
            Retry::Idempotent,
        )?)?;
        self.forget_attr(parent);
        if let Ok(mut n) = self.names.lock() {
            n.put((parent, name.to_string()), a.ino);
        }
        Ok(self.remember(a))
    }

    pub fn link(&self, ino: u64, parent: u64, name: &str) -> Result<Attr, Errno> {
        check_name(name)?;
        let a: Attr = decode(self.call(
            Method::POST,
            &format!("/inodes/{ino}/links"),
            Body::Json(json!({ "parent": parent, "name": name })),
            // Linking the same inode under the same name again is a no-op on the server.
            Retry::Idempotent,
        )?)?;
        self.forget_attr(parent);
        Ok(self.remember(a))
    }

    fn drop_name(&self, parent: u64, name: &str) {
        let ino = self
            .names
            .lock()
            .ok()
            .and_then(|mut n| n.remove(&(parent, name.to_string())));
        self.forget_attr(parent);
        if let Some(ino) = ino {
            self.forget_attr(ino);
        }
    }

    pub fn unlink(&self, parent: u64, name: &str) -> Result<(), Errno> {
        if let Ok(a) = self.lookup(parent, name) {
            // Buffered bytes must not land on an inode that is about to disappear.
            self.flush(a.ino)?;
        }
        self.drop_name(parent, name);
        self.void(
            Method::POST,
            &format!("/inodes/{parent}/unlink"),
            Body::Json(json!({ "name": name })),
            Retry::Remove,
        )
    }

    pub fn rmdir(&self, parent: u64, name: &str) -> Result<(), Errno> {
        self.drop_name(parent, name);
        self.void(
            Method::POST,
            &format!("/inodes/{parent}/rmdir"),
            Body::Json(json!({ "name": name })),
            Retry::Remove,
        )
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(), Errno> {
        check_name(new_name)?;
        self.drop_name(parent, name);
        self.drop_name(new_parent, new_name);
        // Both inodes' ctime and link counts may change; re-fetch every attribute.
        if let Ok(mut c) = self.attrs.lock() {
            c.clear();
        }
        self.void(
            Method::POST,
            "/rename",
            Body::Json(json!({
                "parent": parent,
                "name": name,
                "new_parent": new_parent,
                "new_name": new_name,
            })),
            Retry::Remove,
        )
    }

    pub fn setattr(&self, ino: u64, attr: SetAttr) -> Result<Attr, Errno> {
        if let Some(size) = attr.size {
            // Bytes buffered below the new size must reach the file before it is cut.
            self.flush(ino)?;
            if let Ok(mut d) = self.dirty.lock() {
                d.truncate(ino, size);
            }
        }
        let body = serde_json::to_value(&attr).map_err(|_| libc::EINVAL)?;
        let a = decode(self.call(
            Method::POST,
            &format!("/inodes/{ino}/attr"),
            Body::Json(body),
            Retry::Idempotent,
        )?)?;
        Ok(self.remember(a))
    }

    pub fn read(&self, ino: u64, offset: u64, len: usize) -> Result<Vec<u8>, Errno> {
        self.flush(ino)?;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let want = (len - out.len()).min(self.cfg.max_io_bytes);
            let at = offset + out.len() as u64;
            let chunk = self
                .client
                .request(
                    Method::GET,
                    &self.path(&format!("/inodes/{ino}/data?offset={at}&len={want}")),
                    Body::Empty,
                    Retry::Idempotent,
                )
                .map_err(errno)?;
            let short = chunk.len() < want;
            out.extend_from_slice(&chunk);
            if short {
                break;
            }
        }
        Ok(out)
    }

    pub fn write(&self, ino: u64, offset: u64, data: &[u8]) -> Result<usize, Errno> {
        if self.read_only() {
            return Err(libc::EROFS);
        }
        let mut d = self.dirty.lock().map_err(|_| libc::EIO)?;
        for run in d.write(ino, offset, data) {
            self.send(&run)?;
        }
        Ok(data.len())
    }

    /// Sends any buffered bytes of `ino`.
    pub fn flush(&self, ino: u64) -> Result<(), Errno> {
        let mut d = self.dirty.lock().map_err(|_| libc::EIO)?;
        match d.take(ino) {
            Some(run) => self.send(&run),
            None => Ok(()),
        }
    }

    pub fn flush_all(&self) -> Result<(), Errno> {
        let mut d = self.dirty.lock().map_err(|_| libc::EIO)?;
        let mut result = Ok(());
        for run in d.take_all() {
            if let Err(e) = self.send(&run) {
                result = Err(e);
            }
        }
        result
    }

    /// Writes a run in requests of at most `max_io_bytes` (each is idempotent: same bytes, same
    /// offset), then caches the attributes the last one returned.
    fn send(&self, run: &Dirty) -> Result<(), Errno> {
        let mut last = None;
        for (i, chunk) in run.data.chunks(self.cfg.max_io_bytes.max(1)).enumerate() {
            let at = run.offset + (i * self.cfg.max_io_bytes.max(1)) as u64;
            let v = self.call(
                Method::PUT,
                &format!("/inodes/{}/data?offset={at}", run.ino),
                Body::Bytes(chunk),
                Retry::Idempotent,
            )?;
            last = Some(v);
        }
        if let Some(v) = last {
            let a: Attr = decode(v)?;
            if let Ok(mut c) = self.attrs.lock() {
                c.put(a.ino, a);
            }
        }
        Ok(())
    }
}
