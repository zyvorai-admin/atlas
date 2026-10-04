// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! The kernel side: a `fuser` filesystem that forwards every request to [`Ops`].

use std::{
    ffi::OsStr,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use atlas_native::{engine::Attr, NodeType, SetAttr};
use fuser::{
    AccessFlags, BsdFileFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, LockOwner, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow, WriteFlags,
};

use crate::ops::Ops;

pub struct AtlasFs {
    pub ops: Ops,
}

fn time(ns: i64) -> SystemTime {
    if ns >= 0 {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    } else {
        UNIX_EPOCH
    }
}

fn ns(t: TimeOrNow) -> i64 {
    let t = match t {
        TimeOrNow::SpecificTime(t) => t,
        TimeOrNow::Now => SystemTime::now(),
    };
    t.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn file_attr(a: &Attr) -> FileAttr {
    FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.blocks,
        atime: time(a.atime_ns),
        mtime: time(a.mtime_ns),
        ctime: time(a.ctime_ns),
        crtime: time(a.ctime_ns),
        kind: kind(a.kind),
        perm: (a.mode & 0o7777) as u16,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn kind(k: NodeType) -> FileType {
    match k {
        NodeType::File => FileType::RegularFile,
        NodeType::Dir => FileType::Directory,
        NodeType::Symlink => FileType::Symlink,
    }
}

fn err(e: i32) -> Errno {
    Errno::from_i32(e)
}

fn name(n: &OsStr) -> Result<&str, Errno> {
    n.to_str().ok_or(Errno::EINVAL)
}

impl AtlasFs {
    fn ttl(&self) -> Duration {
        self.ops.cfg.ttl
    }

    fn entry(&self, r: Result<Attr, i32>, reply: ReplyEntry) {
        match r {
            Ok(a) => reply.entry(&self.ttl(), &file_attr(&a), Generation(0)),
            Err(e) => reply.error(err(e)),
        }
    }

    fn mutate(&self) -> Result<(), Errno> {
        if self.ops.read_only() {
            return Err(Errno::EROFS);
        }
        Ok(())
    }
}

macro_rules! tri {
    ($reply:ident, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return $reply.error(e),
        }
    };
}

impl Filesystem for AtlasFs {
    fn destroy(&mut self) {
        if let Err(e) = self.ops.flush_all() {
            tracing::error!(errno = e, "buffered writes lost at unmount");
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEntry) {
        let n = tri!(reply, name(n));
        self.entry(self.ops.lookup(parent.0, n), reply);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.ops.getattr(ino.0) {
            Ok(a) => reply.attr(&self.ttl(), &file_attr(&a)),
            Err(e) => reply.error(err(e)),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        tri!(reply, self.mutate());
        let attr = SetAttr {
            mode,
            uid,
            gid,
            size,
            atime_ns: atime.map(ns),
            mtime_ns: mtime.map(ns),
        };
        match self.ops.setattr(ino.0, attr) {
            Ok(a) => reply.attr(&self.ttl(), &file_attr(&a)),
            Err(e) => reply.error(err(e)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.ops.readlink(ino.0) {
            Ok(t) => reply.data(t.as_bytes()),
            Err(e) => reply.error(err(e)),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        // Only regular files: there is no device, FIFO or socket inode type.
        if mode & libc::S_IFMT != libc::S_IFREG && mode & libc::S_IFMT != 0 {
            return reply.error(Errno::EPERM);
        }
        let r = self.ops.mknode(
            parent.0,
            n,
            NodeType::File,
            None,
            mode & !umask,
            req.uid(),
            req.gid(),
        );
        self.entry(r, reply);
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        let r = self.ops.mknode(
            parent.0,
            n,
            NodeType::Dir,
            None,
            mode & !umask,
            req.uid(),
            req.gid(),
        );
        self.entry(r, reply);
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        match self.ops.unlink(parent.0, n) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        match self.ops.rmdir(parent.0, n) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn symlink(&self, req: &Request, parent: INodeNo, n: &OsStr, target: &Path, reply: ReplyEntry) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        let target = tri!(reply, target.to_str().ok_or(Errno::EINVAL));
        let r = self.ops.mknode(
            parent.0,
            n,
            NodeType::Symlink,
            Some(target.to_string()),
            0o777,
            req.uid(),
            req.gid(),
        );
        self.entry(r, reply);
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        n: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        tri!(reply, self.mutate());
        // RENAME_NOREPLACE / RENAME_EXCHANGE are not supported.
        if !flags.is_empty() {
            return reply.error(Errno::EINVAL);
        }
        let (n, nn) = (tri!(reply, name(n)), tri!(reply, name(new_name)));
        match self.ops.rename(parent.0, n, new_parent.0, nn) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        new_parent: INodeNo,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        tri!(reply, self.mutate());
        let nn = tri!(reply, name(new_name));
        self.entry(self.ops.link(ino.0, new_parent.0, nn), reply);
    }

    fn open(&self, _req: &Request, _ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if self.ops.read_only() && flags.acc_mode() != OpenAccMode::O_RDONLY {
            return reply.error(Errno::EROFS);
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        tri!(reply, self.mutate());
        let n = tri!(reply, name(n));
        match self.ops.mknode(
            parent.0,
            n,
            NodeType::File,
            None,
            mode & !umask,
            req.uid(),
            req.gid(),
        ) {
            Ok(a) => reply.created(
                &self.ttl(),
                &file_attr(&a),
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            ),
            Err(e) => reply.error(err(e)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.ops.read(ino.0, offset, size as usize) {
            Ok(b) => reply.data(&b),
            Err(e) => reply.error(err(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.ops.write(ino.0, offset, data) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(err(e)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.ops.flush(ino.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.ops.flush(ino.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.ops.flush(ino.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err(e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.ops.readdir(ino.0) {
            Ok(e) => e,
            Err(e) => return reply.error(err(e)),
        };
        let dots = [
            (ino.0, FileType::Directory, "."),
            (ino.0, FileType::Directory, ".."),
        ];
        let all = dots
            .into_iter()
            .map(|(i, k, n)| (i, k, n.to_string()))
            .chain(entries.into_iter().map(|e| (e.ino, kind(e.kind), e.name)));
        for (i, (ino, k, n)) in all.enumerate().skip(offset as usize) {
            if reply.add(INodeNo(ino), (i + 1) as u64, k, n) {
                break;
            }
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Capacity is the cluster's, not this filesystem's; report what is used plus headroom.
        const HEADROOM_BLOCKS: u64 = 1 << 28;
        const HEADROOM_INODES: u64 = 1 << 24;
        match self.ops.statfs() {
            Ok(s) => reply.statfs(
                s.used_bytes.div_ceil(4096) + HEADROOM_BLOCKS,
                HEADROOM_BLOCKS,
                HEADROOM_BLOCKS,
                s.inodes + HEADROOM_INODES,
                HEADROOM_INODES,
                4096,
                atlas_native::namespace::MAX_NAME_BYTES as u32,
                4096,
            ),
            Err(e) => reply.error(err(e)),
        }
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        // Permission checks are the kernel's (mounted with default_permissions).
        reply.ok();
    }
}
