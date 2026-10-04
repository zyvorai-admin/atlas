// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Filesystem operations against one native filesystem, independent of the kernel interface:
//! every call returns a value or an errno. Attributes and name lookups are cached for a short
//! TTL; writes are buffered per inode and sent on fsync, close, a non-sequential write, or once
//! a run reaches the flush size.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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

/// A read-ahead window: its file offset and the bytes read from there.
type Window = (u64, Arc<Vec<u8>>);

#[derive(Debug, Clone)]
pub struct OpsConfig {
    /// How long attributes and name lookups may be served from cache (also the kernel TTL).
    pub ttl: Duration,
    /// Write-back run size; 0 sends every write immediately.
    pub writeback_bytes: usize,
    /// Largest single read or write request sent to the cluster.
    pub max_io_bytes: usize,
    /// A read fetches at least this much (from the read offset) and serves following reads from
    /// it until the TTL expires or the file is written through this mount; 0 disables it.
    pub readahead_bytes: usize,
}

impl Default for OpsConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(1),
            writeback_bytes: 4 << 20,
            max_io_bytes: 8 << 20,
            readahead_bytes: 4 << 20,
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
    readahead: Mutex<TtlCache<u64, Window>>,
    /// Where each inode's last read ended: only a read that continues there reads ahead.
    last_read_end: Mutex<TtlCache<u64, u64>>,
    /// Open handles per inode in this mount.
    opens: Mutex<HashMap<u64, u32>>,
    /// Files unlinked while open here: renamed to a hidden name in their directory and removed
    /// on last close, so open handles keep working (as libfuse does without `hard_remove`).
    hidden: Mutex<HashMap<u64, (u64, String)>>,
}

/// Prefix of the names open-but-unlinked files are parked under; hidden from listings.
pub const HIDDEN_PREFIX: &str = ".atlas_hidden_";

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
            readahead: Mutex::new(TtlCache::new(if cfg.readahead_bytes > 0 {
                cfg.ttl
            } else {
                Duration::ZERO
            })),
            last_read_end: Mutex::new(TtlCache::new(Duration::from_secs(10))),
            opens: Mutex::new(HashMap::new()),
            hidden: Mutex::new(HashMap::new()),
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
        if self.hidden.lock().is_ok_and(|h| h.contains_key(&a.ino)) {
            a.nlink = a.nlink.saturating_sub(1);
        }
        a
    }

    pub fn opened(&self, ino: u64) {
        if let Ok(mut o) = self.opens.lock() {
            *o.entry(ino).or_default() += 1;
        }
    }

    /// Drops one open handle; the last one sends buffered writes and removes the file if it
    /// was unlinked while open.
    pub fn released(&self, ino: u64) -> Result<(), Errno> {
        let flushed = self.flush(ino);
        let last = self.opens.lock().is_ok_and(|mut o| match o.get_mut(&ino) {
            Some(n) if *n > 1 => {
                *n -= 1;
                false
            }
            _ => {
                o.remove(&ino);
                true
            }
        });
        if last {
            let parked = self.hidden.lock().ok().and_then(|mut h| h.remove(&ino));
            if let Some((parent, name)) = parked {
                self.forget_attr(ino);
                self.remove_entry(parent, &name)?;
            }
        }
        flushed
    }

    fn remove_entry(&self, parent: u64, name: &str) -> Result<(), Errno> {
        self.void(
            Method::POST,
            &format!("/inodes/{parent}/unlink"),
            Body::Json(json!({ "name": name })),
            Retry::Remove,
        )
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
        let mut entries: Vec<DirEntry> = decode(v["entries"].clone())?;
        entries.retain(|e| !e.name.starts_with(HIDDEN_PREFIX));
        Ok(entries)
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
        self.create(
            parent,
            NewNode {
                name: name.into(),
                op_id: String::new(),
                kind,
                target,
                rdev: 0,
                mode,
                uid,
                gid,
            },
        )
    }

    /// Creates `node` in `parent` under a fresh op id: a retry of this request is recognised,
    /// a later create of the same name is EEXIST.
    pub fn create(&self, parent: u64, mut node: NewNode) -> Result<Attr, Errno> {
        check_name(&node.name)?;
        node.op_id = uuid::Uuid::new_v4().to_string();
        let name = node.name.clone();
        let body = serde_json::to_value(&node).map_err(|_| libc::EINVAL)?;
        let a: Attr = decode(self.call(
            Method::POST,
            &format!("/inodes/{parent}/entries"),
            Body::Json(body),
            Retry::Idempotent,
        )?)?;
        self.forget_attr(parent);
        if let Ok(mut n) = self.names.lock() {
            n.put((parent, name), a.ino);
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
            let open = self.opens.lock().is_ok_and(|o| o.contains_key(&a.ino));
            if open && a.nlink == 1 && a.kind != NodeType::Dir {
                let parked = format!(
                    "{HIDDEN_PREFIX}{}_{}",
                    a.ino,
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                );
                self.rename(parent, name, parent, &parked)?;
                if let Ok(mut h) = self.hidden.lock() {
                    h.insert(a.ino, (parent, parked));
                }
                return Ok(());
            }
            // Buffered bytes must not land on an inode that is about to disappear.
            self.flush(a.ino)?;
        }
        self.drop_name(parent, name);
        self.remove_entry(parent, name)
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
        self.drop_readahead(ino);
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
        let cached = self.readahead.lock().ok().and_then(|mut r| r.get(&ino));
        if let Some((at, buf)) = cached {
            let end = at + buf.len() as u64;
            // A window shorter than requested ended at EOF, so it still answers fully.
            if offset >= at && (offset + len as u64 <= end || buf.len() < self.cfg.readahead_bytes)
            {
                let from = ((offset - at) as usize).min(buf.len());
                let to = (from + len).min(buf.len());
                if let Ok(mut l) = self.last_read_end.lock() {
                    l.put(ino, at + to as u64);
                }
                return Ok(buf[from..to].to_vec());
            }
        }
        let sequential = self
            .last_read_end
            .lock()
            .ok()
            .and_then(|mut l| {
                let prev = l.get(&ino);
                l.put(ino, offset + len as u64);
                prev
            })
            .map_or(offset == 0, |end| end == offset);
        let want = if sequential {
            len.max(self.cfg.readahead_bytes)
        } else {
            len
        };
        let data = self.fetch(ino, offset, want)?;
        let out = data[..len.min(data.len())].to_vec();
        if want > len {
            if let Ok(mut r) = self.readahead.lock() {
                r.put(ino, (offset, Arc::new(data)));
            }
        }
        Ok(out)
    }

    fn fetch(&self, ino: u64, offset: u64, len: usize) -> Result<Vec<u8>, Errno> {
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

    fn drop_readahead(&self, ino: u64) {
        if let Ok(mut r) = self.readahead.lock() {
            r.remove(&ino);
        }
    }

    pub fn write(&self, ino: u64, offset: u64, data: &[u8]) -> Result<usize, Errno> {
        if self.read_only() {
            return Err(libc::EROFS);
        }
        self.drop_readahead(ino);
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

    /// Sends every buffered write and removes files parked by unlink-while-open (at unmount).
    pub fn flush_all(&self) -> Result<(), Errno> {
        let mut result = Ok(());
        let runs = self.dirty.lock().map_err(|_| libc::EIO)?.take_all();
        for run in runs {
            if let Err(e) = self.send(&run) {
                result = Err(e);
            }
        }
        let parked: Vec<(u64, String)> = self
            .hidden
            .lock()
            .map(|mut h| h.drain().map(|(_, v)| v).collect())
            .unwrap_or_default();
        for (parent, name) in parked {
            if let Err(e) = self.remove_entry(parent, &name) {
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
