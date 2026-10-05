// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! A POSIX-style file and directory namespace in the replicated catalog. A filesystem is a
//! table of inodes; a directory inode holds its entries, a file inode holds an extent map on the
//! same fixed grid as a volume (and shares the extent refcounts, so snapshots and clones are
//! metadata-only), and a symlink holds its target. Every timestamp arrives in the command, so
//! every replica applying the same log reaches byte-identical state.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    inodes::{DirEntries, InodeTable},
    metadata::{Catalog, ExtentId, ExtentRef, MetaError, SnapshotId},
};

pub type FsId = String;

pub const ROOT_INO: u64 = 1;
pub const MAX_NAME_BYTES: usize = 255;
pub const MAX_SYMLINK_BYTES: usize = 4096;
pub const MAX_XATTR_VALUE_BYTES: usize = 64 << 10;
/// Names plus values of every extended attribute on one inode.
pub const MAX_XATTR_TOTAL_BYTES: usize = 256 << 10;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsMeta {
    pub id: FsId,
    pub name: String,
    pub next_ino: u64,
    pub inodes: InodeTable,
    /// The snapshot this filesystem was cloned from, if any.
    #[serde(default)]
    pub source_snapshot: Option<SnapshotId>,
    /// File extent grid; `None` (filesystems created before it was recorded) uses the cluster's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extent_bytes: Option<u64>,
    /// Kept by every change, so stat needn't walk the inodes. `None` in a catalog written before
    /// it was kept, until [`Catalog::fill_usage`] counts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<FsUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<FsQuota>,
}

/// Limits a filesystem's growth: a change that would take `used_bytes` above `max_bytes`, or
/// the inode count above `max_inodes`, fails with [`MetaError::Quota`]. Changes that don't grow
/// usage (rewrites, truncates, removes) always pass, even over a limit lowered below usage.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsQuota {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_inodes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsUsage {
    /// Sum of file sizes.
    pub file_bytes: u64,
    /// Sum of the lengths of the extents files reference.
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsSnapshotMeta {
    pub id: SnapshotId,
    pub fs_id: FsId,
    pub name: String,
    pub created_ns: i64,
    /// The filesystem as it was when the snapshot was taken.
    pub tree: FsMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Inode {
    pub ino: u64,
    pub kind: InodeKind,
    /// Permission bits (`0o7777`); the file type comes from `kind`.
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    /// Client-chosen id of the create that made this inode, so a retried create is a no-op.
    #[serde(default)]
    pub op_id: String,
    /// Extended attributes (`user.*`, `trusted.*`, `security.*`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub xattrs: BTreeMap<String, Vec<u8>>,
}

/// How `setxattr(2)` treats an existing attribute.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum XattrMode {
    /// Create or replace.
    #[default]
    Set,
    /// Fail with `exists` if the attribute is already set (`XATTR_CREATE`).
    Create,
    /// Fail with `no_attr` if the attribute is not set (`XATTR_REPLACE`).
    Replace,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    Dir {
        parent: u64,
        entries: DirEntries,
    },
    File {
        size: u64,
        extents: BTreeMap<u64, ExtentId>,
    },
    Symlink {
        target: String,
    },
    /// A FIFO, socket or device node: metadata only, the kernel implements its behaviour.
    Special {
        node_type: NodeType,
        rdev: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

/// Attribute changes; `None` leaves a field alone. A smaller `size` truncates (dropping whole
/// extents past it), a larger one extends with a hole.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetAttr {
    #[serde(default)]
    pub mode: Option<u32>,
    #[serde(default)]
    pub uid: Option<u32>,
    #[serde(default)]
    pub gid: Option<u32>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub atime_ns: Option<i64>,
    #[serde(default)]
    pub mtime_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FsOp {
    CreateFs {
        fs: FsId,
        name: String,
        now_ns: i64,
        /// File extent grid in bytes; `None` uses the cluster's.
        #[serde(default)]
        extent_bytes: Option<u64>,
    },
    DeleteFs {
        fs: FsId,
    },
    Mknode {
        fs: FsId,
        parent: u64,
        name: String,
        op_id: String,
        node_type: NodeType,
        #[serde(default)]
        target: Option<String>,
        /// Device number of a character or block device node.
        #[serde(default)]
        rdev: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        now_ns: i64,
    },
    /// A hard link to an existing non-directory inode.
    Link {
        fs: FsId,
        ino: u64,
        parent: u64,
        name: String,
        now_ns: i64,
    },
    Unlink {
        fs: FsId,
        parent: u64,
        name: String,
        now_ns: i64,
    },
    Rmdir {
        fs: FsId,
        parent: u64,
        name: String,
        now_ns: i64,
    },
    /// POSIX `rename(2)`: replaces an existing target of a compatible type.
    Rename {
        fs: FsId,
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
        now_ns: i64,
    },
    SetAttr {
        fs: FsId,
        ino: u64,
        attr: SetAttr,
        now_ns: i64,
    },
    SetXattr {
        fs: FsId,
        ino: u64,
        name: String,
        value: Vec<u8>,
        #[serde(default)]
        mode: XattrMode,
        now_ns: i64,
    },
    RemoveXattr {
        fs: FsId,
        ino: u64,
        name: String,
        now_ns: i64,
    },
    /// Installs an already-replicated extent at `logical_offset` of a file and grows the file to
    /// at least `size`.
    InstallFileExtent {
        fs: FsId,
        ino: u64,
        logical_offset: u64,
        extent: ExtentRef,
        size: u64,
        now_ns: i64,
    },
    /// [`FsOp::InstallFileExtent`] for several extents of one write (each at its own
    /// `logical_offset`), committed as one entry.
    InstallFileExtents {
        fs: FsId,
        ino: u64,
        extents: Vec<ExtentRef>,
        size: u64,
        now_ns: i64,
    },
    /// Freezes the whole inode table; shares every file extent.
    SnapshotFs {
        id: SnapshotId,
        fs: FsId,
        name: String,
        now_ns: i64,
    },
    DeleteFsSnapshot {
        id: SnapshotId,
    },
    /// A new writable filesystem from a snapshot (copy-on-write).
    CloneFs {
        id: FsId,
        name: String,
        snapshot_id: SnapshotId,
    },
    /// Replaces the filesystem's quota; both limits `None` removes it.
    SetQuota {
        fs: FsId,
        #[serde(default)]
        max_bytes: Option<u64>,
        #[serde(default)]
        max_inodes: Option<u64>,
    },
}

impl Inode {
    fn new(ino: u64, kind: InodeKind, mode: u32, uid: u32, gid: u32, now_ns: i64) -> Self {
        let nlink = if matches!(kind, InodeKind::Dir { .. }) {
            2
        } else {
            1
        };
        Self {
            ino,
            kind,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink,
            atime_ns: now_ns,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
            op_id: String::new(),
            xattrs: BTreeMap::new(),
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind, InodeKind::Dir { .. })
    }

    /// File size; a directory reports its entry count and a symlink its target length.
    pub fn size(&self) -> u64 {
        match &self.kind {
            InodeKind::Dir { entries, .. } => entries.len(),
            InodeKind::File { size, .. } => *size,
            InodeKind::Symlink { target } => target.len() as u64,
            InodeKind::Special { .. } => 0,
        }
    }

    fn touch(&mut self, now_ns: i64) {
        self.mtime_ns = now_ns;
        self.ctime_ns = now_ns;
    }
}

impl FsMeta {
    pub fn inode(&self, ino: u64) -> Result<Arc<Inode>, MetaError> {
        self.inodes
            .get(ino)?
            .ok_or_else(|| MetaError::NotFound(format!("inode {ino} in filesystem {}", self.id)))
    }

    fn inode_mut(&mut self, ino: u64) -> Result<&mut Inode, MetaError> {
        let id = &self.id;
        self.inodes
            .get_mut(ino)?
            .ok_or_else(|| MetaError::NotFound(format!("inode {ino} in filesystem {id}")))
    }

    /// A directory inode.
    pub fn dir(&self, dir: u64) -> Result<Arc<Inode>, MetaError> {
        let i = self.inode(dir)?;
        if !i.is_dir() {
            return Err(MetaError::NotDir(format!("inode {dir}")));
        }
        Ok(i)
    }

    /// The inode `name` in directory `dir` names, if any.
    pub fn entry(&self, dir: u64, name: &str) -> Result<Option<u64>, MetaError> {
        self.inodes.entry(&*self.dir(dir)?, name)
    }

    /// Up to `limit` entries of directory `dir` named after `after`, in name order.
    pub fn entries(
        &self,
        dir: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MetaError> {
        self.inodes.entries(&*self.dir(dir)?, after, limit)
    }

    fn set_entry(&mut self, dir: u64, name: &str, ino: Option<u64>) -> Result<(), MetaError> {
        self.dir(dir)?;
        self.inodes.set_entry(dir, name, ino)?;
        Ok(())
    }

    pub fn lookup(&self, dir: u64, name: &str) -> Result<u64, MetaError> {
        self.entry(dir, name)?
            .ok_or_else(|| MetaError::NotFound(format!("{name:?} in directory {dir}")))
    }

    /// Adjusts a directory's link count (`+1` per subdirectory) and stamps it modified.
    /// Fails if one more inode would take the filesystem over its inode quota (the root counts).
    fn check_inode_quota(&self) -> Result<(), MetaError> {
        match self.quota.and_then(|q| q.max_inodes) {
            Some(max) if self.inodes.len() >= max => Err(MetaError::Quota(format!(
                "filesystem {} has reached its inode quota of {max}",
                self.id
            ))),
            _ => Ok(()),
        }
    }

    fn dir_changed(&mut self, dir: u64, subdirs: i64, now_ns: i64) -> Result<(), MetaError> {
        let d = self.inode_mut(dir)?;
        d.nlink = (i64::from(d.nlink) + subdirs).max(0) as u32;
        d.touch(now_ns);
        Ok(())
    }

    /// Drops one link to a non-directory; returns the inode once its last link is gone.
    fn drop_link(&mut self, ino: u64, now_ns: i64) -> Result<Option<Inode>, MetaError> {
        let i = self.inode_mut(ino)?;
        i.nlink = i.nlink.saturating_sub(1);
        i.ctime_ns = now_ns;
        if i.nlink == 0 {
            return self.inodes.remove(ino);
        }
        Ok(None)
    }

    /// Whether `ino` is `ancestor` or lies below it.
    fn is_within(&self, mut ino: u64, ancestor: u64) -> Result<bool, MetaError> {
        loop {
            if ino == ancestor {
                return Ok(true);
            }
            match &self.inode(ino)?.kind {
                InodeKind::Dir { parent, .. } if ino != ROOT_INO => ino = *parent,
                _ => return Ok(false),
            }
        }
    }
}

/// Every extent referenced by a table of inodes (hard links count once: they share an inode).
pub fn file_extents<'a>(
    inodes: impl Iterator<Item = &'a Inode>,
) -> impl Iterator<Item = &'a ExtentId> {
    inodes
        .flat_map(|i| match &i.kind {
            InodeKind::File { extents, .. } => Some(extents.values()),
            _ => None,
        })
        .flatten()
}

/// An extended attribute name in a namespace we store. `system.*` (POSIX ACLs and the like)
/// is refused: the kernel would not enforce what we stored.
pub fn check_xattr_name(name: &str) -> Result<(), MetaError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.contains('\0') {
        return Err(MetaError::Invalid(format!(
            "invalid extended attribute name {name:?}"
        )));
    }
    match name.split_once('.') {
        Some(("user" | "trusted" | "security", rest)) if !rest.is_empty() => Ok(()),
        _ => Err(MetaError::Unsupported(format!(
            "extended attribute namespace of {name:?}"
        ))),
    }
}

fn check_name(name: &str) -> Result<(), MetaError> {
    if name.is_empty()
        || name.len() > MAX_NAME_BYTES
        || name == "."
        || name == ".."
        || name.contains(['/', '\0'])
    {
        return Err(MetaError::Invalid(format!("invalid file name {name:?}")));
    }
    Ok(())
}

impl Catalog {
    pub fn filesystem(&self, fs: &str) -> Result<&FsMeta, MetaError> {
        self.filesystems
            .get(fs)
            .ok_or_else(|| MetaError::NotFound(format!("filesystem {fs}")))
    }

    /// A live filesystem, or `"<fs>@<snapshot>"` for the frozen tree of one of its snapshots.
    pub fn fs_view(&self, fs: &str) -> Result<&FsMeta, MetaError> {
        let Some((fs, snap)) = fs.split_once('@') else {
            return self.filesystem(fs);
        };
        self.fs_snapshots
            .get(snap)
            .filter(|s| s.fs_id == fs)
            .map(|s| &s.tree)
            .ok_or_else(|| MetaError::NotFound(format!("snapshot {snap} of filesystem {fs}")))
    }

    fn fs_mut(&mut self, fs: &str) -> Result<&mut FsMeta, MetaError> {
        self.filesystems
            .get_mut(fs)
            .ok_or_else(|| MetaError::NotFound(format!("filesystem {fs}")))
    }

    fn drop_inode(&mut self, inode: &Inode, gc: &mut Vec<ExtentId>) -> Result<(), MetaError> {
        if let InodeKind::File { extents, .. } = &inode.kind {
            for eid in extents.values() {
                self.dec_ref(eid, gc)?;
            }
        }
        Ok(())
    }

    fn share_extents(&mut self, inodes: &InodeTable) -> Result<(), MetaError> {
        let all = inodes.scan()?;
        for eid in file_extents(all.iter().map(|i| &**i)) {
            self.extents
                .get_mut(eid)
                .ok_or_else(|| MetaError::NotFound(eid.clone()))?
                .refs += 1;
        }
        Ok(())
    }

    fn extent_len(&self, eid: &ExtentId) -> u64 {
        self.extents.get(eid).map_or(0, |m| m.extent.len as u64)
    }

    /// Fails if installing `new` (`(logical_offset, len)` pairs) in file `ino` would grow `fs`'s
    /// `used_bytes` past its byte quota. Extents the install replaces are credited back, so a
    /// same-size rewrite always passes.
    fn check_byte_quota(&self, fs: &str, ino: u64, new: &[(u64, u64)]) -> Result<(), MetaError> {
        let f = self.filesystem(fs)?;
        let (Some(max), Some(usage)) = (f.quota.and_then(|q| q.max_bytes), f.usage) else {
            return Ok(());
        };
        let inode = f.inode(ino)?;
        let InodeKind::File { extents, .. } = &inode.kind else {
            return Ok(());
        };
        let freed: u64 = new
            .iter()
            .filter_map(|(off, _)| extents.get(off))
            .map(|o| self.extent_len(o))
            .sum();
        let added: u64 = new.iter().map(|(_, len)| len).sum();
        if added <= freed {
            return Ok(());
        }
        let after = (usage.used_bytes + added).saturating_sub(freed);
        if after > max {
            return Err(MetaError::Quota(format!(
                "filesystem {fs} would use {after} bytes, over its quota of {max}"
            )));
        }
        Ok(())
    }

    /// A filesystem's usage counters, if it keeps them.
    fn usage_mut(&mut self, fs: &str) -> Option<&mut FsUsage> {
        self.filesystems.get_mut(fs)?.usage.as_mut()
    }

    /// Takes a file about to be dropped out of its filesystem's usage.
    fn forget_usage(&mut self, fs: &str, inode: &Inode) {
        let InodeKind::File { size, extents } = &inode.kind else {
            return;
        };
        let used: u64 = extents.values().map(|e| self.extent_len(e)).sum();
        if let Some(u) = self.usage_mut(fs) {
            u.file_bytes = u.file_bytes.saturating_sub(*size);
            u.used_bytes = u.used_bytes.saturating_sub(used);
        }
    }

    /// Counts the usage of every filesystem and snapshot tree that doesn't keep it yet.
    pub fn fill_usage(&mut self) -> Result<(), MetaError> {
        let count = |c: &Catalog, f: &FsMeta| -> Result<FsUsage, MetaError> {
            let all = f.inodes.scan()?;
            Ok(FsUsage {
                file_bytes: all
                    .iter()
                    .filter(|i| matches!(i.kind, InodeKind::File { .. }))
                    .map(|i| i.size())
                    .sum(),
                used_bytes: file_extents(all.iter().map(|i| &**i))
                    .map(|e| c.extent_len(e))
                    .sum(),
            })
        };
        let fs: Vec<(FsId, FsUsage)> = self
            .filesystems
            .values()
            .filter(|f| f.usage.is_none())
            .map(|f| Ok((f.id.clone(), count(self, f)?)))
            .collect::<Result<_, MetaError>>()?;
        for (id, u) in fs {
            if let Some(f) = self.filesystems.get_mut(&id) {
                f.usage = Some(u);
            }
        }
        let snaps: Vec<(SnapshotId, FsUsage)> = self
            .fs_snapshots
            .values()
            .filter(|s| s.tree.usage.is_none())
            .map(|s| Ok((s.id.clone(), count(self, &s.tree)?)))
            .collect::<Result<_, MetaError>>()?;
        for (id, u) in snaps {
            if let Some(s) = self.fs_snapshots.get_mut(&id) {
                s.tree.usage = Some(u);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_fs(&mut self, op: &FsOp, gc: &mut Vec<ExtentId>) -> Result<(), MetaError> {
        match op {
            FsOp::CreateFs {
                fs,
                name,
                now_ns,
                extent_bytes,
            } => {
                if *extent_bytes == Some(0) {
                    return Err(MetaError::Invalid("extent_bytes must be positive".into()));
                }
                if let Some(f) = self.filesystems.get(fs) {
                    if f.name == *name {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("filesystem {fs}")));
                }
                let root = Inode::new(
                    ROOT_INO,
                    InodeKind::Dir {
                        parent: ROOT_INO,
                        entries: DirEntries::default(),
                    },
                    0o755,
                    0,
                    0,
                    *now_ns,
                );
                self.filesystems.insert(
                    fs.clone(),
                    FsMeta {
                        id: fs.clone(),
                        name: name.clone(),
                        next_ino: ROOT_INO + 1,
                        inodes: [root].into_iter().collect(),
                        source_snapshot: None,
                        extent_bytes: *extent_bytes,
                        usage: Some(FsUsage::default()),
                        quota: None,
                    },
                );
            }
            FsOp::DeleteFs { fs } => {
                let all = self.filesystem(fs)?.inodes.scan()?;
                self.filesystems.remove(fs);
                self.leases.forget_fs(fs);
                for inode in &all {
                    self.drop_inode(inode, gc)?;
                }
            }
            FsOp::Mknode {
                fs,
                parent,
                name,
                op_id,
                node_type,
                target,
                rdev,
                mode,
                uid,
                gid,
                now_ns,
            } => {
                check_name(name)?;
                let f = self.fs_mut(fs)?;
                if let Some(existing) = f.entry(*parent, name)? {
                    if !op_id.is_empty() && f.inode(existing)?.op_id == *op_id {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("{name:?} in directory {parent}")));
                }
                f.check_inode_quota()?;
                let kind = match node_type {
                    NodeType::File => InodeKind::File {
                        size: 0,
                        extents: BTreeMap::new(),
                    },
                    NodeType::Dir => InodeKind::Dir {
                        parent: *parent,
                        entries: DirEntries::default(),
                    },
                    NodeType::Symlink => {
                        let t = target
                            .as_ref()
                            .filter(|t| !t.is_empty() && t.len() <= MAX_SYMLINK_BYTES)
                            .ok_or_else(|| {
                                MetaError::Invalid(format!(
                                    "a symlink needs a 1-{MAX_SYMLINK_BYTES} byte target"
                                ))
                            })?;
                        InodeKind::Symlink { target: t.clone() }
                    }
                    t => InodeKind::Special {
                        node_type: *t,
                        rdev: *rdev,
                    },
                };
                let ino = f.next_ino;
                f.next_ino += 1;
                let mut inode = Inode::new(ino, kind, *mode, *uid, *gid, *now_ns);
                inode.op_id = op_id.clone();
                let is_dir = inode.is_dir();
                f.inodes.insert(inode);
                f.set_entry(*parent, name, Some(ino))?;
                f.dir_changed(*parent, i64::from(is_dir), *now_ns)?;
            }
            FsOp::Link {
                fs,
                ino,
                parent,
                name,
                now_ns,
            } => {
                check_name(name)?;
                let f = self.fs_mut(fs)?;
                if f.inode(*ino)?.is_dir() {
                    return Err(MetaError::IsDir(format!(
                        "inode {ino}: directories cannot be hard-linked"
                    )));
                }
                if let Some(existing) = f.entry(*parent, name)? {
                    if existing == *ino {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("{name:?} in directory {parent}")));
                }
                f.set_entry(*parent, name, Some(*ino))?;
                f.dir_changed(*parent, 0, *now_ns)?;
                let i = f.inode_mut(*ino)?;
                i.nlink += 1;
                i.ctime_ns = *now_ns;
            }
            FsOp::Unlink {
                fs,
                parent,
                name,
                now_ns,
            } => {
                let f = self.fs_mut(fs)?;
                let ino = f.lookup(*parent, name)?;
                if f.inode(ino)?.is_dir() {
                    return Err(MetaError::IsDir(format!("{name:?} in directory {parent}")));
                }
                f.set_entry(*parent, name, None)?;
                f.dir_changed(*parent, 0, *now_ns)?;
                if let Some(gone) = f.drop_link(ino, *now_ns)? {
                    self.forget_usage(fs, &gone);
                    self.drop_inode(&gone, gc)?;
                }
            }
            FsOp::Rmdir {
                fs,
                parent,
                name,
                now_ns,
            } => {
                let f = self.fs_mut(fs)?;
                let ino = f.lookup(*parent, name)?;
                if f.dir(ino)?.size() != 0 {
                    return Err(MetaError::NotEmpty(format!(
                        "{name:?} in directory {parent}"
                    )));
                }
                f.set_entry(*parent, name, None)?;
                f.dir_changed(*parent, -1, *now_ns)?;
                f.inodes.remove(ino)?;
            }
            FsOp::Rename {
                fs,
                parent,
                name,
                new_parent,
                new_name,
                now_ns,
            } => {
                check_name(new_name)?;
                let f = self.fs_mut(fs)?;
                let src = f.lookup(*parent, name)?;
                let dst = f.entry(*new_parent, new_name)?;
                if dst == Some(src) {
                    return Ok(());
                }
                let src_dir = f.inode(src)?.is_dir();
                if src_dir && f.is_within(*new_parent, src)? {
                    return Err(MetaError::Invalid(format!(
                        "cannot move directory {src} inside itself"
                    )));
                }
                let mut dropped = None;
                if let Some(dst) = dst {
                    let dst_dir = f.inode(dst)?.is_dir();
                    match (src_dir, dst_dir) {
                        (true, false) => {
                            return Err(MetaError::NotDir(format!(
                                "{new_name:?} in directory {new_parent}"
                            )))
                        }
                        (false, true) => {
                            return Err(MetaError::IsDir(format!(
                                "{new_name:?} in directory {new_parent}"
                            )))
                        }
                        (true, true) if f.dir(dst)?.size() != 0 => {
                            return Err(MetaError::NotEmpty(format!(
                                "{new_name:?} in directory {new_parent}"
                            )))
                        }
                        _ => {}
                    }
                    f.set_entry(*new_parent, new_name, None)?;
                    if dst_dir {
                        f.inodes.remove(dst)?;
                        f.dir_changed(*new_parent, -1, *now_ns)?;
                    } else {
                        dropped = f.drop_link(dst, *now_ns)?;
                    }
                }
                f.set_entry(*parent, name, None)?;
                f.set_entry(*new_parent, new_name, Some(src))?;
                let moved = i64::from(src_dir && parent != new_parent);
                f.dir_changed(*parent, -moved, *now_ns)?;
                f.dir_changed(*new_parent, moved, *now_ns)?;
                let s = f.inode_mut(src)?;
                s.ctime_ns = *now_ns;
                if let InodeKind::Dir { parent: p, .. } = &mut s.kind {
                    *p = *new_parent;
                }
                if let Some(gone) = dropped {
                    self.forget_usage(fs, &gone);
                    self.drop_inode(&gone, gc)?;
                }
            }
            FsOp::SetAttr {
                fs,
                ino,
                attr,
                now_ns,
            } => {
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                let mut cut = BTreeMap::new();
                let mut resized = None;
                if let Some(new_size) = attr.size {
                    let InodeKind::File { size, extents } = &mut i.kind else {
                        return Err(MetaError::IsDir(format!(
                            "inode {ino} is not a regular file"
                        )));
                    };
                    if new_size < *size {
                        cut = extents.split_off(&new_size);
                    }
                    if new_size != *size {
                        resized = Some((*size, new_size));
                        *size = new_size;
                        i.mtime_ns = *now_ns;
                    }
                }
                if let Some(m) = attr.mode {
                    i.mode = m & 0o7777;
                }
                if let Some(u) = attr.uid {
                    i.uid = u;
                }
                if let Some(g) = attr.gid {
                    i.gid = g;
                }
                if let Some(t) = attr.atime_ns {
                    i.atime_ns = t;
                }
                if let Some(t) = attr.mtime_ns {
                    i.mtime_ns = t;
                }
                i.ctime_ns = *now_ns;
                let cut_bytes: u64 = cut.values().map(|e| self.extent_len(e)).sum();
                if let Some(u) = self.usage_mut(fs) {
                    if let Some((old, new)) = resized {
                        u.file_bytes = u.file_bytes.saturating_sub(old).saturating_add(new);
                    }
                    u.used_bytes = u.used_bytes.saturating_sub(cut_bytes);
                }
                for eid in cut.values() {
                    self.dec_ref(eid, gc)?;
                }
            }
            FsOp::SetXattr {
                fs,
                ino,
                name,
                value,
                mode,
                now_ns,
            } => {
                check_xattr_name(name)?;
                if value.len() > MAX_XATTR_VALUE_BYTES {
                    return Err(MetaError::TooBig(format!(
                        "extended attribute values are at most {MAX_XATTR_VALUE_BYTES} bytes"
                    )));
                }
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                let old = i.xattrs.get(name);
                match (mode, old) {
                    (XattrMode::Create, Some(_)) => {
                        return Err(MetaError::Exists(format!("extended attribute {name:?}")))
                    }
                    (XattrMode::Replace, None) => return Err(MetaError::NoAttr(name.clone())),
                    _ => {}
                }
                let total: usize = i
                    .xattrs
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>()
                    - old.map_or(0, |v| name.len() + v.len())
                    + name.len()
                    + value.len();
                if total > MAX_XATTR_TOTAL_BYTES {
                    return Err(MetaError::TooBig(format!(
                        "extended attributes of inode {ino} would exceed {MAX_XATTR_TOTAL_BYTES} bytes"
                    )));
                }
                i.xattrs.insert(name.clone(), value.clone());
                i.ctime_ns = *now_ns;
            }
            FsOp::RemoveXattr {
                fs,
                ino,
                name,
                now_ns,
            } => {
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                if i.xattrs.remove(name).is_none() {
                    return Err(MetaError::NoAttr(name.clone()));
                }
                i.ctime_ns = *now_ns;
            }
            FsOp::InstallFileExtent {
                fs,
                ino,
                logical_offset,
                extent,
                size: at_least,
                now_ns,
            } => {
                extent.check()?;
                self.check_byte_quota(fs, *ino, &[(*logical_offset, extent.len as u64)])?;
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                let InodeKind::File { size, extents } = &mut i.kind else {
                    return Err(MetaError::IsDir(format!(
                        "inode {ino} is not a regular file"
                    )));
                };
                let old = extents.insert(*logical_offset, extent.id.clone());
                let old_size = *size;
                *size = (*size).max(*at_least);
                let grown = *size - old_size;
                i.touch(*now_ns);
                self.add_extent_ref(extent);
                let old_len = old.as_ref().map_or(0, |o| self.extent_len(o));
                if let Some(u) = self.usage_mut(fs) {
                    u.file_bytes = u.file_bytes.saturating_add(grown);
                    u.used_bytes = (u.used_bytes + extent.len as u64).saturating_sub(old_len);
                }
                if let Some(old) = old {
                    self.dec_ref(&old, gc)?;
                }
            }
            FsOp::InstallFileExtents {
                fs,
                ino,
                extents: new,
                size: at_least,
                now_ns,
            } => {
                for e in new {
                    e.check()?;
                }
                let lens: Vec<(u64, u64)> = new
                    .iter()
                    .map(|e| (e.logical_offset, e.len as u64))
                    .collect();
                self.check_byte_quota(fs, *ino, &lens)?;
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                let InodeKind::File { size, extents } = &mut i.kind else {
                    return Err(MetaError::IsDir(format!(
                        "inode {ino} is not a regular file"
                    )));
                };
                let old: Vec<ExtentId> = new
                    .iter()
                    .filter_map(|e| extents.insert(e.logical_offset, e.id.clone()))
                    .collect();
                let old_size = *size;
                *size = (*size).max(*at_least);
                let grown = *size - old_size;
                i.touch(*now_ns);
                for e in new {
                    self.add_extent_ref(e);
                }
                let old_len: u64 = old.iter().map(|o| self.extent_len(o)).sum();
                let new_len: u64 = new.iter().map(|e| e.len as u64).sum();
                if let Some(u) = self.usage_mut(fs) {
                    u.file_bytes = u.file_bytes.saturating_add(grown);
                    u.used_bytes = (u.used_bytes + new_len).saturating_sub(old_len);
                }
                for o in old {
                    self.dec_ref(&o, gc)?;
                }
            }
            FsOp::SnapshotFs {
                id,
                fs,
                name,
                now_ns,
            } => {
                if let Some(s) = self.fs_snapshots.get(id) {
                    if s.fs_id == *fs && s.name == *name {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("filesystem snapshot {id}")));
                }
                let f = self.filesystem(fs)?;
                let tree = FsMeta {
                    inodes: f.inodes.detached()?,
                    ..f.clone()
                };
                self.share_extents(&tree.inodes)?;
                self.fs_snapshots.insert(
                    id.clone(),
                    FsSnapshotMeta {
                        id: id.clone(),
                        fs_id: fs.clone(),
                        name: name.clone(),
                        created_ns: *now_ns,
                        tree,
                    },
                );
            }
            FsOp::DeleteFsSnapshot { id } => {
                let s = self
                    .fs_snapshots
                    .remove(id)
                    .ok_or_else(|| MetaError::NotFound(format!("filesystem snapshot {id}")))?;
                for inode in s.tree.inodes.scan()? {
                    self.drop_inode(&inode, gc)?;
                }
            }
            FsOp::CloneFs {
                id,
                name,
                snapshot_id,
            } => {
                if let Some(f) = self.filesystems.get(id) {
                    if f.name == *name && f.source_snapshot.as_ref() == Some(snapshot_id) {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("filesystem {id}")));
                }
                let s = self.fs_snapshots.get(snapshot_id).ok_or_else(|| {
                    MetaError::NotFound(format!("filesystem snapshot {snapshot_id}"))
                })?;
                let mut tree = s.tree.clone();
                self.share_extents(&tree.inodes)?;
                tree.id = id.clone();
                tree.name = name.clone();
                tree.source_snapshot = Some(snapshot_id.clone());
                self.filesystems.insert(id.clone(), tree);
            }
            FsOp::SetQuota {
                fs,
                max_bytes,
                max_inodes,
            } => {
                let quota = FsQuota {
                    max_bytes: *max_bytes,
                    max_inodes: *max_inodes,
                };
                self.fs_mut(fs)?.quota = (quota != FsQuota::default()).then_some(quota);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetaCommand, ReplicaRef};

    struct T {
        c: Catalog,
        index: u64,
    }

    impl T {
        fn new() -> Self {
            let mut t = Self {
                c: Catalog::default(),
                index: 0,
            };
            t.ok(FsOp::CreateFs {
                fs: "f".into(),
                name: "fs".into(),
                now_ns: 1,
                extent_bytes: None,
            });
            t
        }

        fn run(&mut self, op: FsOp) -> Result<Vec<ExtentId>, MetaError> {
            self.index += 1;
            let r = self
                .c
                .apply_committed(1, self.index, &MetaCommand::Fs { op });
            // The usage every op keeps matches a fresh count.
            let mut counted = self.c.clone();
            let ids: Vec<FsId> = counted.filesystems.keys().cloned().collect();
            for id in ids {
                counted.filesystems.get_mut(&id).unwrap().usage = None;
            }
            let ids: Vec<SnapshotId> = counted.fs_snapshots.keys().cloned().collect();
            for id in ids {
                counted.fs_snapshots.get_mut(&id).unwrap().tree.usage = None;
            }
            counted.fill_usage().unwrap();
            assert_eq!(
                serde_json::to_value(&counted).unwrap(),
                serde_json::to_value(&self.c).unwrap(),
                "usage drifted from a fresh count"
            );
            r
        }

        fn ok(&mut self, op: FsOp) -> Vec<ExtentId> {
            self.run(op).expect("op applies")
        }

        fn fs(&self) -> &FsMeta {
            self.c.filesystem("f").unwrap()
        }

        fn mk(&mut self, parent: u64, name: &str, t: NodeType) -> u64 {
            self.ok(mknode(parent, name, t, name));
            self.fs().lookup(parent, name).unwrap()
        }

        fn extent(&mut self, ino: u64, off: u64, id: &str, len: usize) -> Vec<ExtentId> {
            self.ok(install(ino, off, id, len))
        }
    }

    fn install(ino: u64, off: u64, id: &str, len: usize) -> FsOp {
        FsOp::InstallFileExtent {
            fs: "f".into(),
            ino,
            logical_offset: off,
            extent: ExtentRef {
                id: id.into(),
                logical_offset: off,
                len,
                checksum: [0; 32],
                replicas: vec![ReplicaRef {
                    node_id: "n".into(),
                    device_index: 0,
                    offset: off,
                }],
                ec: None,
                created_ms: 0,
                object: None,
            },
            size: off + len as u64,
            now_ns: 5,
        }
    }

    fn mknode(parent: u64, name: &str, t: NodeType, op_id: &str) -> FsOp {
        FsOp::Mknode {
            fs: "f".into(),
            parent,
            name: name.into(),
            op_id: op_id.into(),
            node_type: t,
            target: (t == NodeType::Symlink).then(|| "/tmp/x".to_string()),
            rdev: 0,
            mode: 0o100644,
            uid: 1000,
            gid: 1000,
            now_ns: 2,
        }
    }

    fn unlink(parent: u64, name: &str) -> FsOp {
        FsOp::Unlink {
            fs: "f".into(),
            parent,
            name: name.into(),
            now_ns: 3,
        }
    }

    fn rename(parent: u64, name: &str, new_parent: u64, new_name: &str) -> FsOp {
        FsOp::Rename {
            fs: "f".into(),
            parent,
            name: name.into(),
            new_parent,
            new_name: new_name.into(),
            now_ns: 4,
        }
    }

    #[test]
    fn xattrs_respect_the_per_inode_total_and_stamp_ctime() {
        let mut t = T::new();
        let f = t.mk(ROOT_INO, "f", NodeType::File);
        let set = |name: &str, len: usize, now_ns| FsOp::SetXattr {
            fs: "f".into(),
            ino: f,
            name: name.into(),
            value: vec![1; len],
            mode: XattrMode::Set,
            now_ns,
        };
        for i in 0..3 {
            t.ok(set(&format!("user.{i}"), MAX_XATTR_VALUE_BYTES, 10 + i));
        }
        assert_eq!(t.fs().inode(f).unwrap().ctime_ns, 12);
        let err = t.run(set("user.3", MAX_XATTR_VALUE_BYTES, 20)).unwrap_err();
        assert!(matches!(err, MetaError::TooBig(_)));
        // Replacing an attribute only counts its new size.
        t.ok(set("user.0", MAX_XATTR_VALUE_BYTES, 21));
        let err = t.run(set("bad", 1, 22)).unwrap_err();
        assert!(matches!(err, MetaError::Unsupported(_)));
        t.ok(FsOp::RemoveXattr {
            fs: "f".into(),
            ino: f,
            name: "user.1".into(),
            now_ns: 23,
        });
        t.ok(set("user.3", MAX_XATTR_VALUE_BYTES, 24));
        assert_eq!(t.fs().inode(f).unwrap().xattrs.len(), 3);
    }

    #[test]
    fn create_fs_records_its_grid_and_refuses_zero() {
        let mut t = T::new();
        assert_eq!(t.fs().extent_bytes, None, "created without a grid");
        let err = t
            .run(FsOp::CreateFs {
                fs: "g".into(),
                name: "g".into(),
                now_ns: 1,
                extent_bytes: Some(0),
            })
            .unwrap_err();
        assert!(matches!(err, MetaError::Invalid(_)));
        t.ok(FsOp::CreateFs {
            fs: "g".into(),
            name: "g".into(),
            now_ns: 1,
            extent_bytes: Some(65536),
        });
        assert_eq!(t.c.filesystem("g").unwrap().extent_bytes, Some(65536));
    }

    #[test]
    fn creates_are_idempotent_by_op_id() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        assert_eq!(t.fs().inode(a).unwrap().mode, 0o644);
        let fifo = t.mk(ROOT_INO, "p", NodeType::Fifo);
        assert!(matches!(
            t.fs().inode(fifo).unwrap().kind,
            InodeKind::Special {
                node_type: NodeType::Fifo,
                rdev: 0
            }
        ));
        t.ok(unlink(ROOT_INO, "p"));
        // The retried create (same op id) is a no-op; a different create of the name is EEXIST.
        t.ok(mknode(ROOT_INO, "a", NodeType::File, "a"));
        assert_eq!(t.fs().inodes.len(), 2);
        assert!(matches!(
            t.run(mknode(ROOT_INO, "a", NodeType::File, "other")),
            Err(MetaError::Exists(_))
        ));
        assert!(matches!(
            t.run(mknode(ROOT_INO, "a/b", NodeType::File, "x")),
            Err(MetaError::Invalid(_))
        ));
        assert!(matches!(
            t.run(mknode(a, "x", NodeType::File, "x")),
            Err(MetaError::NotDir(_))
        ));
    }

    #[test]
    fn directories_track_link_counts_and_refuse_bad_removal() {
        let mut t = T::new();
        let d = t.mk(ROOT_INO, "d", NodeType::Dir);
        t.mk(d, "f", NodeType::File);
        assert_eq!(t.fs().inode(ROOT_INO).unwrap().nlink, 3);
        let rmdir = |name: &str| FsOp::Rmdir {
            fs: "f".into(),
            parent: ROOT_INO,
            name: name.into(),
            now_ns: 3,
        };
        assert!(matches!(t.run(rmdir("d")), Err(MetaError::NotEmpty(_))));
        assert!(matches!(
            t.run(unlink(ROOT_INO, "d")),
            Err(MetaError::IsDir(_))
        ));
        t.ok(unlink(d, "f"));
        t.ok(rmdir("d"));
        assert_eq!(t.fs().inode(ROOT_INO).unwrap().nlink, 2);
        assert_eq!(t.fs().inodes.len(), 1);
    }

    #[test]
    fn hard_links_keep_data_until_the_last_name_goes() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        t.extent(a, 0, "e1", 10);
        t.ok(FsOp::Link {
            fs: "f".into(),
            ino: a,
            parent: ROOT_INO,
            name: "b".into(),
            now_ns: 3,
        });
        assert_eq!(t.fs().inode(a).unwrap().nlink, 2);
        assert!(t.ok(unlink(ROOT_INO, "a")).is_empty());
        assert_eq!(t.ok(unlink(ROOT_INO, "b")), vec!["e1".to_string()]);
        assert!(t.fs().inode(a).is_err());
    }

    #[test]
    fn rename_follows_posix_replacement_rules() {
        let mut t = T::new();
        let d1 = t.mk(ROOT_INO, "d1", NodeType::Dir);
        let d2 = t.mk(ROOT_INO, "d2", NodeType::Dir);
        let a = t.mk(d1, "a", NodeType::File);
        let b = t.mk(d2, "b", NodeType::File);
        t.extent(b, 0, "eb", 4);

        // File over file drops the target (and its data); the moved inode keeps its number.
        assert_eq!(t.ok(rename(d1, "a", d2, "b")), vec!["eb".to_string()]);
        assert_eq!(t.fs().lookup(d2, "b").unwrap(), a);
        assert!(t.fs().lookup(d1, "a").is_err());

        // Moving a directory updates both parents' link counts and its `..`.
        let sub = t.mk(d1, "sub", NodeType::Dir);
        t.ok(rename(d1, "sub", d2, "sub"));
        assert_eq!(t.fs().inode(d1).unwrap().nlink, 2);
        assert_eq!(t.fs().inode(d2).unwrap().nlink, 3);
        assert!(matches!(
            t.fs().inode(sub).unwrap().kind,
            InodeKind::Dir { parent, .. } if parent == d2
        ));

        assert!(matches!(
            t.run(rename(ROOT_INO, "d2", sub, "loop")),
            Err(MetaError::Invalid(_))
        ));
        assert!(matches!(
            t.run(rename(d2, "b", d2, "sub")),
            Err(MetaError::IsDir(_))
        ));
        assert!(matches!(
            t.run(rename(d2, "sub", d2, "b")),
            Err(MetaError::NotDir(_))
        ));
        t.mk(sub, "x", NodeType::File);
        let empty = t.mk(ROOT_INO, "empty", NodeType::Dir);
        assert!(matches!(
            t.run(rename(ROOT_INO, "empty", d2, "sub")),
            Err(MetaError::NotEmpty(_))
        ));
        // A rename onto itself is a no-op; a retried rename finds its source gone.
        t.ok(rename(ROOT_INO, "empty", ROOT_INO, "empty"));
        t.ok(rename(ROOT_INO, "empty", ROOT_INO, "e2"));
        assert_eq!(t.fs().lookup(ROOT_INO, "e2").unwrap(), empty);
        assert!(matches!(
            t.run(rename(ROOT_INO, "empty", ROOT_INO, "e2")),
            Err(MetaError::NotFound(_))
        ));
    }

    #[test]
    fn truncate_drops_whole_extents_past_the_new_size() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        t.extent(a, 0, "e0", 8);
        t.extent(a, 8, "e1", 8);
        assert_eq!(t.fs().inode(a).unwrap().size(), 16);
        // Rewriting an extent releases the old one.
        assert_eq!(t.extent(a, 8, "e1b", 8), vec!["e1".to_string()]);
        let setsize = |size| FsOp::SetAttr {
            fs: "f".into(),
            ino: a,
            attr: SetAttr {
                size: Some(size),
                ..Default::default()
            },
            now_ns: 9,
        };
        assert_eq!(t.ok(setsize(4)), vec!["e1b".to_string()]);
        let i = t.fs().inode(a).unwrap();
        assert_eq!((i.size(), i.mtime_ns, i.ctime_ns), (4, 9, 9));
        assert!(t.ok(setsize(100)).is_empty());
        assert_eq!(t.fs().inode(a).unwrap().size(), 100);
        assert_eq!(t.c.extents["e0"].refs, 1);
    }

    #[test]
    fn snapshots_and_clones_share_extents_copy_on_write() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        t.mk(ROOT_INO, "l", NodeType::Symlink);
        t.extent(a, 0, "e0", 8);
        t.ok(FsOp::SnapshotFs {
            id: "s".into(),
            fs: "f".into(),
            name: "snap".into(),
            now_ns: 10,
        });
        let clone = FsOp::CloneFs {
            id: "c".into(),
            name: "clone".into(),
            snapshot_id: "s".into(),
        };
        t.ok(clone.clone());
        t.ok(clone);
        assert_eq!(t.c.extents["e0"].refs, 3);
        // Rewriting the source leaves the snapshot and clone on the old extent.
        assert!(t.extent(a, 0, "e0b", 8).is_empty());
        assert_eq!(t.c.extents["e0"].refs, 2);
        let c = t.c.filesystem("c").unwrap();
        assert_eq!(c.inodes.len(), 3);
        assert!(matches!(
            &c.inode(a).unwrap().kind,
            InodeKind::File { extents, .. } if extents[&0] == "e0"
        ));
        t.ok(FsOp::DeleteFsSnapshot { id: "s".into() });
        assert_eq!(
            t.ok(FsOp::DeleteFs { fs: "c".into() }),
            vec!["e0".to_string()]
        );
    }

    #[test]
    fn a_failed_op_leaves_the_catalog_untouched() {
        let mut t = T::new();
        let d = t.mk(ROOT_INO, "d", NodeType::Dir);
        t.mk(d, "x", NodeType::File);
        t.mk(ROOT_INO, "x", NodeType::File);
        let before = t.c.filesystems.clone();
        // Fails after looking up the source: nothing may have moved.
        assert!(t.run(rename(ROOT_INO, "x", ROOT_INO, "d")).is_err());
        assert_eq!(t.c.filesystems, before);
        assert_eq!(t.c.applied_index, t.index);
    }

    #[test]
    fn catalog_round_trips_through_json() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        t.extent(a, 4096, "e", 1);
        let json = serde_json::to_string(&t.c).unwrap();
        let back: Catalog = serde_json::from_str(&json).unwrap();
        assert_eq!(back.filesystems, t.c.filesystems);
        let cmd = serde_json::to_string(&MetaCommand::Fs {
            op: mknode(ROOT_INO, "z", NodeType::Symlink, "z"),
        })
        .unwrap();
        assert!(matches!(
            serde_json::from_str::<MetaCommand>(&cmd).unwrap(),
            MetaCommand::Fs {
                op: FsOp::Mknode { .. }
            }
        ));
        // Catalogs written before the namespace existed still load.
        let old: Catalog = serde_json::from_str(
            r#"{"volumes":{},"snapshots":{},"extents":{},"applied_index":3,"current_term":1}"#,
        )
        .unwrap();
        assert!(old.filesystems.is_empty());
    }

    fn quota(max_bytes: Option<u64>, max_inodes: Option<u64>) -> FsOp {
        FsOp::SetQuota {
            fs: "f".into(),
            max_bytes,
            max_inodes,
        }
    }

    #[test]
    fn quotas_stop_growth_but_not_rewrites_or_removes() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        t.extent(a, 0, "e1", 600);
        t.ok(quota(Some(1000), Some(3)));
        assert_eq!(
            t.fs().quota,
            Some(FsQuota {
                max_bytes: Some(1000),
                max_inodes: Some(3)
            })
        );

        // Growth past the byte limit fails without touching anything.
        let before = t.c.filesystems.clone();
        let err = t.run(install(a, 4096, "e2", 600)).unwrap_err();
        assert!(matches!(err, MetaError::Quota(_)), "{err:?}");
        assert_eq!(t.c.filesystems, before);
        // Up to the limit is fine; a same-size rewrite at the limit is too.
        t.extent(a, 4096, "e2", 400);
        assert_eq!(t.fs().usage.unwrap().used_bytes, 1000);
        t.extent(a, 0, "e3", 600);
        assert!(matches!(
            t.run(install(a, 0, "e4", 601)),
            Err(MetaError::Quota(_))
        ));

        // Root + "a" + "b" fills the inode quota.
        t.mk(ROOT_INO, "b", NodeType::Dir);
        let err = t
            .run(mknode(ROOT_INO, "c", NodeType::File, "c"))
            .unwrap_err();
        assert!(matches!(err, MetaError::Quota(_)), "{err:?}");
        // A retried create of an existing entry still answers idempotently.
        t.ok(mknode(ROOT_INO, "b", NodeType::Dir, "b"));

        // Lowering below usage is allowed; removing still works, then growth resumes.
        t.ok(quota(Some(100), Some(3)));
        t.ok(unlink(ROOT_INO, "a"));
        t.mk(ROOT_INO, "c", NodeType::File);
        let c = t.fs().lookup(ROOT_INO, "c").unwrap();
        t.extent(c, 0, "e5", 100);

        // Snapshots and clones carry the quota; clearing both limits removes it.
        t.ok(FsOp::SnapshotFs {
            id: "s".into(),
            fs: "f".into(),
            name: "snap".into(),
            now_ns: 6,
        });
        t.ok(FsOp::CloneFs {
            id: "g".into(),
            name: "clone".into(),
            snapshot_id: "s".into(),
        });
        assert_eq!(t.c.filesystem("g").unwrap().quota, t.fs().quota);
        t.ok(quota(None, None));
        assert_eq!(t.fs().quota, None);
        t.mk(ROOT_INO, "d", NodeType::File);
    }
}
