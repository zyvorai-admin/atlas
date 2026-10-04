// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! A POSIX-style file and directory namespace in the replicated catalog. A filesystem is a
//! table of inodes; a directory inode holds its entries, a file inode holds an extent map on the
//! same fixed grid as a volume (and shares the extent refcounts, so snapshots and clones are
//! metadata-only), and a symlink holds its target. Every timestamp arrives in the command, so
//! every replica applying the same log reaches byte-identical state.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::metadata::{Catalog, ExtentId, ExtentRef, MetaError, SnapshotId};

pub type FsId = String;

pub const ROOT_INO: u64 = 1;
pub const MAX_NAME_BYTES: usize = 255;
pub const MAX_SYMLINK_BYTES: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsMeta {
    pub id: FsId,
    pub name: String,
    pub next_ino: u64,
    pub inodes: BTreeMap<u64, Inode>,
    /// The snapshot this filesystem was cloned from, if any.
    #[serde(default)]
    pub source_snapshot: Option<SnapshotId>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    Dir {
        parent: u64,
        entries: BTreeMap<String, u64>,
    },
    File {
        size: u64,
        extents: BTreeMap<u64, ExtentId>,
    },
    Symlink {
        target: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    File,
    Dir,
    Symlink,
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
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind, InodeKind::Dir { .. })
    }

    /// File size; a directory reports its entry count and a symlink its target length.
    pub fn size(&self) -> u64 {
        match &self.kind {
            InodeKind::Dir { entries, .. } => entries.len() as u64,
            InodeKind::File { size, .. } => *size,
            InodeKind::Symlink { target } => target.len() as u64,
        }
    }

    fn touch(&mut self, now_ns: i64) {
        self.mtime_ns = now_ns;
        self.ctime_ns = now_ns;
    }
}

impl FsMeta {
    pub fn inode(&self, ino: u64) -> Result<&Inode, MetaError> {
        self.inodes
            .get(&ino)
            .ok_or_else(|| MetaError::NotFound(format!("inode {ino} in filesystem {}", self.id)))
    }

    fn inode_mut(&mut self, ino: u64) -> Result<&mut Inode, MetaError> {
        let id = &self.id;
        self.inodes
            .get_mut(&ino)
            .ok_or_else(|| MetaError::NotFound(format!("inode {ino} in filesystem {id}")))
    }

    pub fn entries(&self, dir: u64) -> Result<&BTreeMap<String, u64>, MetaError> {
        match &self.inode(dir)?.kind {
            InodeKind::Dir { entries, .. } => Ok(entries),
            _ => Err(MetaError::NotDir(format!("inode {dir}"))),
        }
    }

    fn entries_mut(&mut self, dir: u64) -> Result<&mut BTreeMap<String, u64>, MetaError> {
        match &mut self.inode_mut(dir)?.kind {
            InodeKind::Dir { entries, .. } => Ok(entries),
            _ => Err(MetaError::NotDir(format!("inode {dir}"))),
        }
    }

    pub fn lookup(&self, dir: u64, name: &str) -> Result<u64, MetaError> {
        self.entries(dir)?
            .get(name)
            .copied()
            .ok_or_else(|| MetaError::NotFound(format!("{name:?} in directory {dir}")))
    }

    /// Adjusts a directory's link count (`+1` per subdirectory) and stamps it modified.
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
            return Ok(self.inodes.remove(&ino));
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
pub fn file_extents(inodes: &BTreeMap<u64, Inode>) -> impl Iterator<Item = &ExtentId> {
    inodes
        .values()
        .flat_map(|i| match &i.kind {
            InodeKind::File { extents, .. } => Some(extents.values()),
            _ => None,
        })
        .flatten()
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

    fn drop_inode(&mut self, inode: Inode, gc: &mut Vec<ExtentId>) -> Result<(), MetaError> {
        if let InodeKind::File { extents, .. } = inode.kind {
            for eid in extents.values() {
                self.dec_ref(eid, gc)?;
            }
        }
        Ok(())
    }

    fn share_extents(&mut self, inodes: &BTreeMap<u64, Inode>) -> Result<(), MetaError> {
        for eid in file_extents(inodes) {
            self.extents
                .get_mut(eid)
                .ok_or_else(|| MetaError::NotFound(eid.clone()))?
                .refs += 1;
        }
        Ok(())
    }

    pub(crate) fn apply_fs(&mut self, op: &FsOp, gc: &mut Vec<ExtentId>) -> Result<(), MetaError> {
        match op {
            FsOp::CreateFs { fs, name, now_ns } => {
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
                        entries: BTreeMap::new(),
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
                        inodes: BTreeMap::from([(ROOT_INO, root)]),
                        source_snapshot: None,
                    },
                );
            }
            FsOp::DeleteFs { fs } => {
                let f = self
                    .filesystems
                    .remove(fs)
                    .ok_or_else(|| MetaError::NotFound(format!("filesystem {fs}")))?;
                for inode in f.inodes.into_values() {
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
                mode,
                uid,
                gid,
                now_ns,
            } => {
                check_name(name)?;
                let f = self.fs_mut(fs)?;
                if let Some(&existing) = f.entries(*parent)?.get(name) {
                    if !op_id.is_empty() && f.inode(existing)?.op_id == *op_id {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("{name:?} in directory {parent}")));
                }
                let kind = match node_type {
                    NodeType::File => InodeKind::File {
                        size: 0,
                        extents: BTreeMap::new(),
                    },
                    NodeType::Dir => InodeKind::Dir {
                        parent: *parent,
                        entries: BTreeMap::new(),
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
                };
                let ino = f.next_ino;
                f.next_ino += 1;
                let mut inode = Inode::new(ino, kind, *mode, *uid, *gid, *now_ns);
                inode.op_id = op_id.clone();
                let is_dir = inode.is_dir();
                f.inodes.insert(ino, inode);
                f.entries_mut(*parent)?.insert(name.clone(), ino);
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
                if let Some(&existing) = f.entries(*parent)?.get(name) {
                    if existing == *ino {
                        return Ok(());
                    }
                    return Err(MetaError::Exists(format!("{name:?} in directory {parent}")));
                }
                f.entries_mut(*parent)?.insert(name.clone(), *ino);
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
                f.entries_mut(*parent)?.remove(name);
                f.dir_changed(*parent, 0, *now_ns)?;
                if let Some(gone) = f.drop_link(ino, *now_ns)? {
                    self.drop_inode(gone, gc)?;
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
                if !f.entries(ino)?.is_empty() {
                    return Err(MetaError::NotEmpty(format!(
                        "{name:?} in directory {parent}"
                    )));
                }
                f.entries_mut(*parent)?.remove(name);
                f.dir_changed(*parent, -1, *now_ns)?;
                f.inodes.remove(&ino);
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
                let dst = f.entries(*new_parent)?.get(new_name).copied();
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
                        (true, true) if !f.entries(dst)?.is_empty() => {
                            return Err(MetaError::NotEmpty(format!(
                                "{new_name:?} in directory {new_parent}"
                            )))
                        }
                        _ => {}
                    }
                    f.entries_mut(*new_parent)?.remove(new_name);
                    if dst_dir {
                        f.inodes.remove(&dst);
                        f.dir_changed(*new_parent, -1, *now_ns)?;
                    } else {
                        dropped = f.drop_link(dst, *now_ns)?;
                    }
                }
                f.entries_mut(*parent)?.remove(name);
                f.entries_mut(*new_parent)?.insert(new_name.clone(), src);
                let moved = i64::from(src_dir && parent != new_parent);
                f.dir_changed(*parent, -moved, *now_ns)?;
                f.dir_changed(*new_parent, moved, *now_ns)?;
                let s = f.inode_mut(src)?;
                s.ctime_ns = *now_ns;
                if let InodeKind::Dir { parent: p, .. } = &mut s.kind {
                    *p = *new_parent;
                }
                if let Some(gone) = dropped {
                    self.drop_inode(gone, gc)?;
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
                for eid in cut.values() {
                    self.dec_ref(eid, gc)?;
                }
            }
            FsOp::InstallFileExtent {
                fs,
                ino,
                logical_offset,
                extent,
                size: at_least,
                now_ns,
            } => {
                let i = self.fs_mut(fs)?.inode_mut(*ino)?;
                let InodeKind::File { size, extents } = &mut i.kind else {
                    return Err(MetaError::IsDir(format!(
                        "inode {ino} is not a regular file"
                    )));
                };
                let old = extents.insert(*logical_offset, extent.id.clone());
                *size = (*size).max(*at_least);
                i.touch(*now_ns);
                self.add_extent_ref(extent);
                if let Some(old) = old {
                    self.dec_ref(&old, gc)?;
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
                let tree = self.filesystem(fs)?.clone();
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
                for inode in s.tree.inodes.into_values() {
                    self.drop_inode(inode, gc)?;
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
            });
            t
        }

        fn run(&mut self, op: FsOp) -> Result<Vec<ExtentId>, MetaError> {
            self.index += 1;
            self.c
                .apply_committed(1, self.index, &MetaCommand::Fs { op })
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
            self.ok(FsOp::InstallFileExtent {
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
                },
                size: off + len as u64,
                now_ns: 5,
            })
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
    fn creates_are_idempotent_by_op_id() {
        let mut t = T::new();
        let a = t.mk(ROOT_INO, "a", NodeType::File);
        assert_eq!(t.fs().inode(a).unwrap().mode, 0o644);
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
}
