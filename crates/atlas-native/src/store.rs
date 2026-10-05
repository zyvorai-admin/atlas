// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! The catalog on disk (`catalog.redb`): one record per volume, snapshot, extent, filesystem,
//! inode and filesystem snapshot, plus one for the rest of the catalog's state. A checkpoint
//! writes only the records the catalog's [`Tracked`](crate::tracked::Tracked) maps report as
//! changed, in one transaction, so its cost follows the changes since the last checkpoint rather
//! than the size of the catalog. A catalog the store doesn't hold yet (new, read from
//! `catalog.json`, installed from a Raft snapshot) is written in full.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use redb::{
    Database, ReadableDatabase, ReadableTable, TableDefinition, TableError, WriteTransaction,
};
use serde::{Deserialize, Serialize};

use crate::{
    alloc::FreeList,
    inodes::InodeTable,
    membership::Membership,
    metadata::{Catalog, SnapshotId},
    namespace::{FsId, FsMeta, FsSnapshotMeta, FsUsage, Inode, InodeKind},
    tracked::Tracked,
};

const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("state");
const VOLUMES: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
const EXTENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("extents");
const FILESYSTEMS: TableDefinition<&str, &[u8]> = TableDefinition::new("filesystems");
const INODES: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("inodes");
/// Directory entries, one record each, so a create in a large directory writes one small record.
const DIR_ENTRIES: TableDefinition<(&str, u64, &str), u64> = TableDefinition::new("dir_entries");
const FS_SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("fs_snapshots");
const STATE_KEY: &str = "catalog";

/// Everything in [`Catalog`] outside its tracked maps; small, rewritten at every checkpoint.
#[derive(Serialize)]
struct StateRef<'a> {
    applied_index: u64,
    current_term: u64,
    free: &'a FreeList,
    membership: &'a Option<Membership>,
    raft_addrs: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct State {
    applied_index: u64,
    current_term: u64,
    free: FreeList,
    membership: Option<Membership>,
    raft_addrs: BTreeMap<String, String>,
}

/// A filesystem without its inodes, which have records of their own.
#[derive(Serialize, Deserialize)]
struct FsHeader {
    id: FsId,
    name: String,
    next_ino: u64,
    source_snapshot: Option<SnapshotId>,
    extent_bytes: Option<u64>,
    #[serde(default)]
    usage: Option<FsUsage>,
    /// Inode count (absent before tables were paged: counted on load).
    #[serde(default)]
    inodes: Option<u64>,
}

impl FsHeader {
    fn of(f: &FsMeta) -> Self {
        Self {
            id: f.id.clone(),
            name: f.name.clone(),
            next_ino: f.next_ino,
            source_snapshot: f.source_snapshot.clone(),
            extent_bytes: f.extent_bytes,
            usage: f.usage,
            inodes: Some(f.inodes.len()),
        }
    }

    /// The filesystem, its inodes paged from the store under `key`.
    fn paged(self, key: FsId, snap: &Arc<StoreSnapshot>) -> io::Result<FsMeta> {
        let len = match self.inodes {
            Some(n) => n,
            None => snap.inos(&key)?.len() as u64,
        };
        Ok(FsMeta {
            inodes: InodeTable::paged(key, snap.clone(), len),
            id: self.id,
            name: self.name,
            next_ino: self.next_ino,
            source_snapshot: self.source_snapshot,
            extent_bytes: self.extent_bytes,
            usage: self.usage,
        })
    }
}

/// A filesystem snapshot without its tree's inodes, which have records of their own under
/// `@<snapshot id>`. (Before that, the record held the whole tree; such records are read and
/// rewritten in this form at the next checkpoint.)
#[derive(Serialize, Deserialize)]
struct SnapHeader {
    id: SnapshotId,
    fs_id: FsId,
    name: String,
    created_ns: i64,
    tree: FsHeader,
}

fn snapshot_key(id: &str) -> String {
    format!("@{id}")
}

/// redb's own page cache; the operating system's page cache sits behind it.
const STORE_CACHE_BYTES: usize = 64 << 20;

/// Inodes the paged tables kept in memory after a checkpoint, unless configured otherwise.
pub const DEFAULT_CACHE_INODES: usize = 256 * 1024;

fn err(e: impl Into<redb::Error>) -> io::Error {
    io::Error::other(e.into())
}

fn json<T: Serialize + ?Sized>(v: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(io::Error::other)
}

fn parse<'a, T: Deserialize<'a>>(b: &'a [u8]) -> io::Result<T> {
    serde_json::from_slice(b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// The store's file under an engine or Raft root.
pub const CATALOG_STORE: &str = "catalog.redb";
/// Where catalogs were checkpointed before the store; read once, then removed.
const LEGACY_CATALOG: &str = "catalog.json";

/// The checkpointed catalog: from the store, else from a legacy `catalog.json`, else empty.
pub fn load_checkpoint(root: &Path, store: &CatalogStore) -> io::Result<Catalog> {
    let legacy = root.join(LEGACY_CATALOG);
    let mut c = match store.load()? {
        Some(c) => c,
        None if legacy.exists() => parse(&fs::read(&legacy)?)?,
        None => Catalog::default(),
    };
    c.fill_usage().map_err(io::Error::other)?;
    Ok(c)
}

/// Drops a legacy `catalog.json` once the store holds a checkpoint, so it can never be read
/// over newer state.
pub fn remove_legacy_catalog(root: &Path) -> io::Result<()> {
    match fs::remove_file(root.join(LEGACY_CATALOG)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

pub struct CatalogStore {
    db: Database,
    path: PathBuf,
    cache_inodes: usize,
    /// The snapshot the last load or checkpoint paged the catalog on.
    last: Mutex<Option<Arc<StoreSnapshot>>>,
}

impl std::fmt::Debug for CatalogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogStore")
            .field("path", &self.path)
            .finish()
    }
}

impl CatalogStore {
    /// Opens or creates the store. Only one process may hold it open.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        Ok(Self {
            db: redb::Builder::new()
                .set_cache_size(STORE_CACHE_BYTES)
                .create(&path)
                .map_err(err)?,
            path,
            cache_inodes: DEFAULT_CACHE_INODES,
            last: Mutex::new(None),
        })
    }

    /// How many unchanged inodes paged tables keep in memory (at least 64).
    pub fn with_cache_inodes(mut self, n: usize) -> Self {
        self.cache_inodes = n.max(64);
        self
    }

    /// A snapshot of the store as it is now, with the still-valid part of the last one's cache:
    /// everything but the inodes in `stale` and the filesystems in `stale_fs`.
    fn snapshot(
        &self,
        stale: &BTreeSet<(FsId, u64)>,
        stale_fs: &BTreeSet<FsId>,
    ) -> io::Result<Arc<StoreSnapshot>> {
        let mut cache = Lru::new(self.cache_inodes);
        let mut last = lock(&self.last);
        if let Some(old) = last.take() {
            cache = std::mem::replace(&mut *lock(&old.cache), Lru::new(0));
            for (fs, ino) in stale {
                cache.take(fs, *ino);
            }
            if !stale_fs.is_empty() {
                cache.retain(|k| !stale_fs.contains(&k.0));
            }
            cache.cap = self.cache_inodes;
        }
        let snap = Arc::new(StoreSnapshot {
            tx: self.db.begin_read().map_err(err)?,
            cache: Mutex::new(cache),
        });
        *last = Some(snap.clone());
        Ok(snap)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The last checkpointed catalog, or `None` if nothing was checkpointed yet.
    pub fn load(&self) -> io::Result<Option<Catalog>> {
        let tx = self.db.begin_read().map_err(err)?;
        let state: State = match tx.open_table(STATE) {
            Ok(t) => match t.get(STATE_KEY).map_err(err)? {
                Some(v) => parse(v.value())?,
                None => return Ok(None),
            },
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err(e)),
        };
        let mut c = Catalog {
            applied_index: state.applied_index,
            current_term: state.current_term,
            free: state.free,
            membership: state.membership,
            raft_addrs: state.raft_addrs,
            ..Catalog::default()
        };
        c.volumes = read_table(&tx, VOLUMES)?;
        c.snapshots = read_table(&tx, SNAPSHOTS)?;
        c.extents = read_table(&tx, EXTENTS)?;
        let snapshots: Vec<Vec<u8>> = read_table::<serde_json::Value>(&tx, FS_SNAPSHOTS)?
            .into_values()
            .map(|v| json(&v))
            .collect::<io::Result<_>>()?;
        let headers: Vec<FsHeader> = read_table::<FsHeader>(&tx, FILESYSTEMS)?
            .into_values()
            .collect();
        drop(tx);
        let snap = self.snapshot(&BTreeSet::new(), &BTreeSet::new())?;
        let mut filesystems = BTreeMap::new();
        for h in headers {
            let id = h.id.clone();
            filesystems.insert(id.clone(), h.paged(id, &snap)?);
        }
        c.filesystems = filesystems.into();
        let mut legacy = Vec::new();
        let mut fs_snapshots = BTreeMap::new();
        for raw in snapshots {
            match serde_json::from_slice::<SnapHeader>(&raw) {
                Ok(h) => {
                    let tree = h.tree.paged(snapshot_key(&h.id), &snap)?;
                    fs_snapshots.insert(
                        h.id.clone(),
                        FsSnapshotMeta {
                            id: h.id,
                            fs_id: h.fs_id,
                            name: h.name,
                            created_ns: h.created_ns,
                            tree,
                        },
                    );
                }
                Err(_) => legacy.push(parse::<FsSnapshotMeta>(&raw)?),
            }
        }
        c.fs_snapshots = fs_snapshots.into();
        for s in legacy {
            // Recorded as replaced, so the next checkpoint writes it in the paged form.
            c.fs_snapshots.insert(s.id.clone(), s);
        }
        c.in_store = true;
        Ok(Some(c))
    }

    /// Writes `c`'s changes (all of it unless `c.in_store`) durably, then forgets them. Returns
    /// how many records were written or removed.
    pub fn checkpoint(&self, c: &mut Catalog) -> io::Result<u64> {
        let tx = self.db.begin_write().map_err(err)?;
        let full = !c.in_store;
        if full {
            for t in [
                STATE,
                VOLUMES,
                SNAPSHOTS,
                EXTENTS,
                FILESYSTEMS,
                FS_SNAPSHOTS,
            ] {
                tx.delete_table(t).map_err(err)?;
            }
            tx.delete_table(INODES).map_err(err)?;
            tx.delete_table(DIR_ENTRIES).map_err(err)?;
        }
        write_state(&tx, c)?;
        let records = 1
            + write_map(&tx, VOLUMES, &c.volumes, full)?
            + write_map(&tx, SNAPSHOTS, &c.snapshots, full)?
            + write_map(&tx, EXTENTS, &c.extents, full)?
            + write_filesystems(&tx, &c.filesystems, full)?
            + write_fs_snapshots(&tx, &c.fs_snapshots, full)?;
        tx.commit().map_err(err)?;

        // Cache entries the next snapshot can't keep: tables replaced or removed, and inodes
        // changed in the others.
        let mut stale_fs: BTreeSet<FsId> = c.filesystems.replaced().iter().cloned().collect();
        stale_fs.extend(c.fs_snapshots.replaced().iter().map(|id| snapshot_key(id)));
        let mut stale: BTreeSet<(FsId, u64)> = BTreeSet::new();
        for f in c
            .filesystems
            .touched()
            .iter()
            .filter_map(|id| c.filesystems.get(id))
        {
            stale.extend(f.inodes.touched().map(|ino| (f.id.clone(), ino)));
        }
        for s in c
            .fs_snapshots
            .touched()
            .iter()
            .filter_map(|id| c.fs_snapshots.get(id))
        {
            stale.extend(
                s.tree
                    .inodes
                    .touched()
                    .map(|ino| (snapshot_key(&s.id), ino)),
            );
        }
        c.volumes.clear_changes();
        c.snapshots.clear_changes();
        c.extents.clear_changes();
        c.fs_snapshots
            .clear_changes_with(|s| s.tree.inodes.clear_changes());
        c.filesystems
            .clear_changes_with(|f| f.inodes.clear_changes());
        let snap = self.snapshot(&stale, &stale_fs)?;
        for f in c.filesystems.values_mut_quietly() {
            f.inodes.attach(f.id.clone(), snap.clone());
        }
        for s in c.fs_snapshots.values_mut_quietly() {
            s.tree.inodes.attach(snapshot_key(&s.id), snap.clone());
        }
        c.in_store = true;
        Ok(records)
    }
}

/// An inode's record: a directory's entries are stored separately.
fn inode_record(i: &Inode) -> io::Result<Vec<u8>> {
    match &i.kind {
        InodeKind::Dir { parent, .. } => json(&Inode {
            kind: InodeKind::Dir {
                parent: *parent,
                entries: Tracked::default(),
            },
            op_id: i.op_id.clone(),
            xattrs: i.xattrs.clone(),
            ..*i
        }),
        _ => json(i),
    }
}

fn read_table<V: for<'a> Deserialize<'a>>(
    tx: &redb::ReadTransaction,
    def: TableDefinition<&str, &[u8]>,
) -> io::Result<Tracked<String, V>> {
    let t = match tx.open_table(def) {
        Ok(t) => t,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Tracked::default()),
        Err(e) => return Err(err(e)),
    };
    let mut out = BTreeMap::new();
    for row in t.iter().map_err(err)? {
        let (k, v) = row.map_err(err)?;
        out.insert(k.value().to_string(), parse(v.value())?);
    }
    Ok(out.into())
}

fn write_state(tx: &WriteTransaction, c: &Catalog) -> io::Result<()> {
    let state = json(&StateRef {
        applied_index: c.applied_index,
        current_term: c.current_term,
        free: &c.free,
        membership: &c.membership,
        raft_addrs: &c.raft_addrs,
    })?;
    tx.open_table(STATE)
        .map_err(err)?
        .insert(STATE_KEY, state.as_slice())
        .map_err(err)?;
    Ok(())
}

fn write_map<V: Serialize>(
    tx: &WriteTransaction,
    def: TableDefinition<&str, &[u8]>,
    map: &Tracked<String, V>,
    full: bool,
) -> io::Result<u64> {
    let mut t = tx.open_table(def).map_err(err)?;
    let keys: Box<dyn Iterator<Item = &String>> = if full {
        Box::new(map.keys())
    } else {
        Box::new(map.touched().iter())
    };
    let mut n = 0;
    for k in keys {
        n += 1;
        match map.get(k) {
            Some(v) => {
                t.insert(k.as_str(), json(v)?.as_slice()).map_err(err)?;
            }
            None => {
                t.remove(k.as_str()).map_err(err)?;
            }
        }
    }
    Ok(n)
}

type InodeRows<'t> = redb::Table<'t, (&'static str, u64), &'static [u8]>;
type EntryRows<'t> = redb::Table<'t, (&'static str, u64, &'static str), u64>;

/// Drops every inode and entry record under `key`.
fn delete_rows(inodes: &mut InodeRows, dirents: &mut EntryRows, key: &str) -> io::Result<()> {
    inodes
        .retain_in((key, 0)..=(key, u64::MAX), |_, _| false)
        .map_err(err)?;
    dirents
        .retain_in((key, 0, "")..(key, u64::MAX, ""), |_, _| false)
        .map_err(err)
}

/// Writes inode `i` under `key` with its directory entries: all of them if `fresh`, else the
/// changed ones. Returns the records written.
fn put_inode(
    inodes: &mut InodeRows,
    dirents: &mut EntryRows,
    key: &str,
    i: &Inode,
    fresh: bool,
) -> io::Result<u64> {
    inodes
        .insert((key, i.ino), inode_record(i)?.as_slice())
        .map_err(err)?;
    let mut n = 1;
    if let InodeKind::Dir { entries, .. } = &i.kind {
        let names: Box<dyn Iterator<Item = &String>> = if fresh {
            Box::new(entries.keys())
        } else {
            Box::new(entries.touched().iter())
        };
        for name in names {
            n += 1;
            match entries.get(name) {
                Some(child) => {
                    dirents
                        .insert((key, i.ino, name.as_str()), *child)
                        .map_err(err)?;
                }
                None => {
                    dirents.remove((key, i.ino, name.as_str())).map_err(err)?;
                }
            }
        }
    }
    Ok(n)
}

/// Writes an inode table under `key`: all of it if `rewrite` (replacing whatever the store held
/// there unless the whole store is being rewritten), else its changes.
fn write_inodes(
    inodes: &mut InodeRows,
    dirents: &mut EntryRows,
    key: &str,
    table: &InodeTable,
    rewrite: bool,
    full: bool,
) -> io::Result<u64> {
    let mut n = 0;
    if rewrite {
        if !full {
            delete_rows(inodes, dirents, key)?;
        }
        let mut written = Ok(());
        table
            .for_each(|i| {
                if written.is_ok() {
                    written = put_inode(inodes, dirents, key, i, true).map(|k| n += k);
                }
            })
            .map_err(io::Error::other)?;
        written?;
        return Ok(n);
    }
    let changed: Vec<u64> = table.touched().collect();
    for ino in changed {
        // An inode inserted or removed (not just edited) keeps none of its old entries.
        let fresh = table.replaced(ino);
        if fresh {
            dirents
                .retain_in((key, ino, "")..(key, ino + 1, ""), |_, _| false)
                .map_err(err)?;
        }
        match table.get(ino).map_err(io::Error::other)? {
            Some(i) => n += put_inode(inodes, dirents, key, &i, fresh)?,
            None => {
                n += 1;
                inodes.remove((key, ino)).map_err(err)?;
            }
        }
    }
    Ok(n)
}

fn write_filesystems(
    tx: &WriteTransaction,
    filesystems: &Tracked<FsId, FsMeta>,
    full: bool,
) -> io::Result<u64> {
    let mut headers = tx.open_table(FILESYSTEMS).map_err(err)?;
    let mut inodes = tx.open_table(INODES).map_err(err)?;
    let mut dirents = tx.open_table(DIR_ENTRIES).map_err(err)?;
    let ids: Box<dyn Iterator<Item = &FsId>> = if full {
        Box::new(filesystems.keys())
    } else {
        Box::new(filesystems.touched().iter())
    };
    let mut n = 0;
    for id in ids {
        n += 1;
        // A filesystem replaced or removed since the last checkpoint keeps none of its records.
        let rewrite = full || filesystems.replaced().contains(id);
        let Some(f) = filesystems.get(id) else {
            delete_rows(&mut inodes, &mut dirents, id)?;
            headers.remove(id.as_str()).map_err(err)?;
            continue;
        };
        headers
            .insert(id.as_str(), json(&FsHeader::of(f))?.as_slice())
            .map_err(err)?;
        n += write_inodes(&mut inodes, &mut dirents, id, &f.inodes, rewrite, full)?;
    }
    Ok(n)
}

fn write_fs_snapshots(
    tx: &WriteTransaction,
    snapshots: &Tracked<SnapshotId, FsSnapshotMeta>,
    full: bool,
) -> io::Result<u64> {
    let mut headers = tx.open_table(FS_SNAPSHOTS).map_err(err)?;
    let mut inodes = tx.open_table(INODES).map_err(err)?;
    let mut dirents = tx.open_table(DIR_ENTRIES).map_err(err)?;
    let ids: Box<dyn Iterator<Item = &SnapshotId>> = if full {
        Box::new(snapshots.keys())
    } else {
        Box::new(snapshots.touched().iter())
    };
    let mut n = 0;
    for id in ids {
        n += 1;
        let key = snapshot_key(id);
        let rewrite = full || snapshots.replaced().contains(id);
        let Some(s) = snapshots.get(id) else {
            delete_rows(&mut inodes, &mut dirents, &key)?;
            headers.remove(id.as_str()).map_err(err)?;
            continue;
        };
        let header = json(&SnapHeader {
            id: s.id.clone(),
            fs_id: s.fs_id.clone(),
            name: s.name.clone(),
            created_ns: s.created_ns,
            tree: FsHeader::of(&s.tree),
        })?;
        headers
            .insert(id.as_str(), header.as_slice())
            .map_err(err)?;
        n += write_inodes(
            &mut inodes,
            &mut dirents,
            &key,
            &s.tree.inodes,
            rewrite,
            full,
        )?;
    }
    Ok(n)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The store as of one checkpoint, which paged inode tables read, with a cache of the inodes
/// read so far. A catalog cloned before a later checkpoint keeps reading this one.
pub struct StoreSnapshot {
    tx: redb::ReadTransaction,
    cache: Mutex<Lru>,
}

impl StoreSnapshot {
    pub(crate) fn get(&self, fs: &str, ino: u64) -> io::Result<Option<Arc<Inode>>> {
        if let Some(i) = lock(&self.cache).get(fs, ino) {
            return Ok(Some(i));
        }
        let Some(i) = self.read(fs, ino)? else {
            return Ok(None);
        };
        let i = Arc::new(i);
        self.put(fs, ino, i.clone());
        Ok(Some(i))
    }

    /// As [`Self::get`] without filling the cache (a scan of a whole filesystem).
    pub(crate) fn peek(&self, fs: &str, ino: u64) -> io::Result<Option<Arc<Inode>>> {
        if let Some(i) = lock(&self.cache).get(fs, ino) {
            return Ok(Some(i));
        }
        Ok(self.read(fs, ino)?.map(Arc::new))
    }

    /// The inode for an edit: out of the cache, so the caller holds the only reference.
    pub(crate) fn take(&self, fs: &str, ino: u64) -> io::Result<Option<Arc<Inode>>> {
        if let Some(i) = lock(&self.cache).take(fs, ino) {
            return Ok(Some(i));
        }
        Ok(self.read(fs, ino)?.map(Arc::new))
    }

    pub(crate) fn put(&self, fs: &str, ino: u64, i: Arc<Inode>) {
        lock(&self.cache).put((fs.to_string(), ino), i);
    }

    /// Every inode number of `fs`, in order.
    pub(crate) fn inos(&self, fs: &str) -> io::Result<Vec<u64>> {
        self.inos_from(fs, 0, usize::MAX, |_| false)
    }

    /// Up to `limit` inode numbers of `fs` from `from` on, in order, leaving out `skip`.
    pub(crate) fn inos_from(
        &self,
        fs: &str,
        from: u64,
        limit: usize,
        skip: impl Fn(u64) -> bool,
    ) -> io::Result<Vec<u64>> {
        let t = match self.tx.open_table(INODES) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(err(e)),
        };
        let mut out = Vec::new();
        for row in t.range((fs, from)..=(fs, u64::MAX)).map_err(err)? {
            if out.len() >= limit {
                break;
            }
            let ino = row.map_err(err)?.0.value().1;
            if !skip(ino) {
                out.push(ino);
            }
        }
        Ok(out)
    }

    fn read(&self, fs: &str, ino: u64) -> io::Result<Option<Inode>> {
        let t = match self.tx.open_table(INODES) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err(e)),
        };
        let Some(v) = t.get((fs, ino)).map_err(err)? else {
            return Ok(None);
        };
        let mut inode: Inode = parse(v.value())?;
        if let InodeKind::Dir { entries, .. } = &mut inode.kind {
            let mut names = BTreeMap::new();
            match self.tx.open_table(DIR_ENTRIES) {
                Ok(d) => {
                    for row in d.range((fs, ino, "")..(fs, ino + 1, "")).map_err(err)? {
                        let (k, v) = row.map_err(err)?;
                        names.insert(k.value().2.to_string(), v.value());
                    }
                }
                Err(TableError::TableDoesNotExist(_)) => {}
                Err(e) => return Err(err(e)),
            }
            *entries = names.into();
        }
        Ok(Some(inode))
    }
}

/// A least-recently-used map from `(filesystem, inode)` to inode.
pub(crate) struct Lru {
    cap: usize,
    tick: u64,
    map: HashMap<(FsId, u64), (Arc<Inode>, u64)>,
    order: BTreeMap<u64, (FsId, u64)>,
}

impl Lru {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            tick: 0,
            map: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn get(&mut self, fs: &str, ino: u64) -> Option<Arc<Inode>> {
        let k = (fs.to_string(), ino);
        let (v, t) = self.map.get_mut(&k)?;
        self.order.remove(t);
        self.tick += 1;
        *t = self.tick;
        self.order.insert(self.tick, k);
        Some(v.clone())
    }

    fn take(&mut self, fs: &str, ino: u64) -> Option<Arc<Inode>> {
        let (v, t) = self.map.remove(&(fs.to_string(), ino))?;
        self.order.remove(&t);
        Some(v)
    }

    fn put(&mut self, k: (FsId, u64), v: Arc<Inode>) {
        self.tick += 1;
        if let Some((_, t)) = self.map.insert(k.clone(), (v, self.tick)) {
            self.order.remove(&t);
        }
        self.order.insert(self.tick, k);
        while self.map.len() > self.cap {
            let Some((_, k)) = self.order.pop_first() else {
                break;
            };
            self.map.remove(&k);
        }
    }

    fn retain(&mut self, keep: impl Fn(&(FsId, u64)) -> bool) {
        self.map.retain(|k, _| keep(k));
        self.order.retain(|_, k| keep(k));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metadata::{ExtentRef, MetaCommand, ReplicaRef},
        namespace::{FsOp, NodeType, ROOT_INO},
    };

    struct Harness {
        _td: tempfile::TempDir,
        store: CatalogStore,
        catalog: Catalog,
        index: u64,
    }

    impl Harness {
        fn new() -> Self {
            let td = tempfile::tempdir().unwrap();
            let store = CatalogStore::open(td.path().join(CATALOG_STORE)).unwrap();
            Self {
                _td: td,
                store,
                catalog: Catalog::default(),
                index: 0,
            }
        }

        fn apply(&mut self, cmd: MetaCommand) {
            self.index += 1;
            self.catalog.apply(1, self.index, &cmd).unwrap();
        }

        fn fs(&mut self, op: FsOp) {
            self.apply(MetaCommand::Fs { op });
        }

        fn mknode(&mut self, fs: &str, parent: u64, name: &str, node_type: NodeType) {
            self.fs(FsOp::Mknode {
                fs: fs.into(),
                parent,
                name: name.into(),
                op_id: format!("{fs}-{name}"),
                node_type,
                target: None,
                rdev: 0,
                mode: 0o644,
                uid: 0,
                gid: 0,
                now_ns: self.index as i64,
            });
        }

        fn ino(&self, fs: &str, name: &str) -> u64 {
            self.catalog.filesystems[fs].lookup(ROOT_INO, name).unwrap()
        }

        /// Checkpoints, then reads the store back and compares it with memory.
        fn check(&mut self) {
            self.store.checkpoint(&mut self.catalog).unwrap();
            assert!(self.catalog.in_store);
            let loaded = self.store.load().unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(&loaded).unwrap(),
                serde_json::to_value(&self.catalog).unwrap()
            );
        }
    }

    fn extent(id: &str, offset: u64) -> ExtentRef {
        ExtentRef {
            id: id.into(),
            logical_offset: 0,
            len: 4096,
            checksum: [7; 32],
            replicas: vec![ReplicaRef {
                node_id: "n1".into(),
                device_index: 0,
                offset,
            }],
        }
    }

    #[test]
    fn incremental_checkpoints_round_trip() {
        let mut h = Harness::new();
        assert!(h.store.load().unwrap().is_none());
        h.apply(MetaCommand::CreateVolume {
            id: "vol".into(),
            name: "vol".into(),
            size_bytes: 1 << 20,
        });
        h.fs(FsOp::CreateFs {
            fs: "f".into(),
            name: "f".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        h.mknode("f", ROOT_INO, "a", NodeType::File);
        h.mknode("f", ROOT_INO, "b", NodeType::File);
        h.mknode("f", ROOT_INO, "d", NodeType::Dir);
        h.check();

        // In-place edits, a removal and a new extent.
        let (a, b, d) = (h.ino("f", "a"), h.ino("f", "b"), h.ino("f", "d"));
        h.fs(FsOp::Unlink {
            fs: "f".into(),
            parent: ROOT_INO,
            name: "a".into(),
            now_ns: 5,
        });
        h.mknode("f", d, "inner", NodeType::File);
        h.fs(FsOp::InstallFileExtent {
            fs: "f".into(),
            ino: b,
            logical_offset: 0,
            extent: extent("e1", 0),
            size: 4096,
            now_ns: 6,
        });
        h.check();
        assert!(h.catalog.filesystems["f"].inodes.get(a).unwrap().is_none());

        // Renames across directories and an rmdir move and drop entry records.
        h.mknode("f", ROOT_INO, "gone", NodeType::Dir);
        h.fs(FsOp::Rename {
            fs: "f".into(),
            parent: d,
            name: "inner".into(),
            new_parent: ROOT_INO,
            new_name: "moved".into(),
            now_ns: 6,
        });
        h.fs(FsOp::Rmdir {
            fs: "f".into(),
            parent: ROOT_INO,
            name: "gone".into(),
            now_ns: 6,
        });
        h.check();

        // A snapshot, a clone of it, and an edit in the clone.
        h.fs(FsOp::SnapshotFs {
            id: "s1".into(),
            fs: "f".into(),
            name: "s1".into(),
            now_ns: 7,
        });
        h.fs(FsOp::CloneFs {
            id: "g".into(),
            name: "g".into(),
            snapshot_id: "s1".into(),
        });
        h.mknode("g", ROOT_INO, "only-in-g", NodeType::File);
        h.check();

        // A filesystem deleted and recreated under the same id keeps none of its old inodes.
        h.fs(FsOp::DeleteFs { fs: "g".into() });
        h.fs(FsOp::CreateFs {
            fs: "g".into(),
            name: "g2".into(),
            now_ns: 8,
            extent_bytes: None,
        });
        h.mknode("g", ROOT_INO, "fresh", NodeType::File);
        h.check();
        assert_eq!(h.catalog.filesystems["g"].inodes.len(), 2);

        h.fs(FsOp::DeleteFs { fs: "g".into() });
        h.apply(MetaCommand::DeleteVolume {
            volume_id: "vol".into(),
        });
        h.check();
    }

    #[test]
    fn a_create_in_a_large_directory_writes_a_few_records() {
        let mut h = Harness::new();
        h.fs(FsOp::CreateFs {
            fs: "f".into(),
            name: "f".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        for i in 0..1000 {
            h.mknode("f", ROOT_INO, &format!("file-{i}"), NodeType::File);
        }
        h.check();
        h.mknode("f", ROOT_INO, "one-more", NodeType::File);
        // State, filesystem header, the new inode, the directory inode and its new entry.
        let written = h.store.checkpoint(&mut h.catalog).unwrap();
        assert_eq!(written, 5);
        h.check();
    }

    #[test]
    fn a_paged_catalog_matches_one_kept_in_memory() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join(CATALOG_STORE);
        let store = CatalogStore::open(&path).unwrap().with_cache_inodes(64);
        let mut paged = Catalog::default();
        let mut mem = Catalog::default();
        let mut index = 0;
        let mut run = |paged: &mut Catalog, mem: &mut Catalog, op: FsOp| {
            index += 1;
            let cmd = MetaCommand::Fs { op };
            let a = paged.apply(1, index, &cmd).map_err(|e| e.to_string());
            let b = mem.apply(1, index, &cmd).map_err(|e| e.to_string());
            assert_eq!(a.is_ok(), b.is_ok(), "{cmd:?}: {a:?} vs {b:?}");
        };
        let mk = |parent: u64, name: String, node_type: NodeType| FsOp::Mknode {
            fs: "f".into(),
            parent,
            op_id: name.clone(),
            name,
            node_type,
            target: None,
            rdev: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            now_ns: 2,
        };
        run(
            &mut paged,
            &mut mem,
            FsOp::CreateFs {
                fs: "f".into(),
                name: "f".into(),
                now_ns: 1,
                extent_bytes: None,
            },
        );
        for d in 0..8u64 {
            run(
                &mut paged,
                &mut mem,
                mk(ROOT_INO, format!("d{d}"), NodeType::Dir),
            );
        }
        for round in 0..6u64 {
            for i in 0..100u64 {
                let dir = 2 + (i * 7 + round) % 8;
                run(
                    &mut paged,
                    &mut mem,
                    mk(dir, format!("f{round}-{i}"), NodeType::File),
                );
                if i % 3 == 0 && round > 0 {
                    run(
                        &mut paged,
                        &mut mem,
                        FsOp::Unlink {
                            fs: "f".into(),
                            parent: dir,
                            name: format!("f{}-{i}", round - 1),
                            now_ns: 3,
                        },
                    );
                }
                if i % 5 == 0 {
                    run(
                        &mut paged,
                        &mut mem,
                        FsOp::Rename {
                            fs: "f".into(),
                            parent: dir,
                            name: format!("f{round}-{i}"),
                            new_parent: 2 + (dir + 1) % 8,
                            new_name: format!("moved-{round}-{i}"),
                            now_ns: 4,
                        },
                    );
                }
            }
            match round {
                2 => run(
                    &mut paged,
                    &mut mem,
                    FsOp::SnapshotFs {
                        id: "s".into(),
                        fs: "f".into(),
                        name: "s".into(),
                        now_ns: 5,
                    },
                ),
                3 => {
                    run(
                        &mut paged,
                        &mut mem,
                        FsOp::CloneFs {
                            id: "g".into(),
                            name: "g".into(),
                            snapshot_id: "s".into(),
                        },
                    );
                    run(
                        &mut paged,
                        &mut mem,
                        FsOp::SnapshotFs {
                            id: "t".into(),
                            fs: "f".into(),
                            name: "t".into(),
                            now_ns: 6,
                        },
                    );
                }
                4 => run(
                    &mut paged,
                    &mut mem,
                    FsOp::DeleteFsSnapshot { id: "t".into() },
                ),
                _ => {}
            }
            store.checkpoint(&mut paged).unwrap();
            assert!(lock(&lock(&store.last).as_ref().unwrap().cache).len() <= 64);
            if round >= 2 {
                // A snapshot's record is a header; its tree's inodes are records of their own.
                let tx = store.db.begin_read().unwrap();
                let t = tx.open_table(FS_SNAPSHOTS).unwrap();
                assert!(t.get("s").unwrap().unwrap().value().len() < 1024);
            }
            assert!(paged.filesystems["f"].inodes.len() > 64);
            assert_eq!(
                serde_json::to_value(&paged).unwrap(),
                serde_json::to_value(&mem).unwrap()
            );
        }
        drop(paged);
        drop(store);
        let store = CatalogStore::open(&path).unwrap();
        let loaded = load_checkpoint(td.path(), &store).unwrap();
        assert_eq!(
            serde_json::to_value(&loaded).unwrap(),
            serde_json::to_value(&mem).unwrap()
        );
    }

    #[test]
    fn a_snapshot_recorded_with_its_whole_tree_is_rewritten_paged() {
        let mut h = Harness::new();
        h.fs(FsOp::CreateFs {
            fs: "f".into(),
            name: "f".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        h.mknode("f", ROOT_INO, "a", NodeType::File);
        h.fs(FsOp::SnapshotFs {
            id: "s".into(),
            fs: "f".into(),
            name: "s".into(),
            now_ns: 2,
        });
        h.check();
        // What an older version wrote: the whole tree in the snapshot's record, no tree rows.
        {
            let tx = h.store.db.begin_write().unwrap();
            {
                let s = &h.catalog.fs_snapshots["s"];
                let whole = serde_json::to_vec(s).unwrap();
                tx.open_table(FS_SNAPSHOTS)
                    .unwrap()
                    .insert("s", whole.as_slice())
                    .unwrap();
                let mut inodes = tx.open_table(INODES).unwrap();
                let mut dirents = tx.open_table(DIR_ENTRIES).unwrap();
                delete_rows(&mut inodes, &mut dirents, "@s").unwrap();
            }
            tx.commit().unwrap();
        }
        let want = serde_json::to_value(&h.catalog).unwrap();
        let mut loaded = h.store.load().unwrap().unwrap();
        assert_eq!(serde_json::to_value(&loaded).unwrap(), want);
        h.store.checkpoint(&mut loaded).unwrap();
        let reloaded = h.store.load().unwrap().unwrap();
        assert_eq!(serde_json::to_value(&reloaded).unwrap(), want);
        let tx = h.store.db.begin_read().unwrap();
        let t = tx.open_table(FS_SNAPSHOTS).unwrap();
        assert!(serde_json::from_slice::<SnapHeader>(t.get("s").unwrap().unwrap().value()).is_ok());
    }

    #[test]
    fn usage_missing_from_an_older_checkpoint_is_counted_on_load() {
        let mut h = Harness::new();
        h.fs(FsOp::CreateFs {
            fs: "f".into(),
            name: "f".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        h.mknode("f", ROOT_INO, "a", NodeType::File);
        let a = h.ino("f", "a");
        h.fs(FsOp::InstallFileExtent {
            fs: "f".into(),
            ino: a,
            logical_offset: 0,
            extent: extent("e1", 0),
            size: 4000,
            now_ns: 3,
        });
        let kept = h.catalog.filesystems["f"].usage;
        assert_eq!(
            kept,
            Some(FsUsage {
                file_bytes: 4000,
                used_bytes: 4096
            })
        );
        h.catalog.filesystems.get_mut("f").unwrap().usage = None;
        h.store.checkpoint(&mut h.catalog).unwrap();
        let loaded = load_checkpoint(h._td.path(), &h.store).unwrap();
        assert_eq!(loaded.filesystems["f"].usage, kept);
    }

    #[test]
    fn a_catalog_not_from_the_store_is_written_in_full() {
        let mut h = Harness::new();
        h.fs(FsOp::CreateFs {
            fs: "f".into(),
            name: "f".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        h.mknode("f", ROOT_INO, "a", NodeType::File);
        h.check();
        // A Raft snapshot (or catalog.json) arrives as plain JSON with no change records.
        let mut other = Harness::new();
        other.fs(FsOp::CreateFs {
            fs: "x".into(),
            name: "x".into(),
            now_ns: 1,
            extent_bytes: None,
        });
        h.catalog = serde_json::from_value(serde_json::to_value(&other.catalog).unwrap()).unwrap();
        assert!(!h.catalog.in_store);
        h.check();
        assert!(!h
            .store
            .load()
            .unwrap()
            .unwrap()
            .filesystems
            .contains_key("f"));
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use crate::{
        metadata::MetaCommand,
        namespace::{FsOp, NodeType, ROOT_INO},
    };

    fn peak_rss() -> String {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .map(str::to_owned)
            })
            .unwrap_or_default()
    }

    /// Peak memory of a paged catalog with a small cache, files in one directory or spread
    /// over `BENCH_DIRS` of them.
    #[test]
    #[ignore]
    fn memory_of_a_paged_catalog() {
        let td = tempfile::tempdir().unwrap();
        let store = CatalogStore::open(td.path().join(CATALOG_STORE))
            .unwrap()
            .with_cache_inodes(4096);
        let dirs: u64 = std::env::var("BENCH_DIRS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let mut c = Catalog::default();
        let mut index = 0;
        let mut apply = |c: &mut Catalog, op: FsOp| {
            index += 1;
            c.apply(1, index, &MetaCommand::Fs { op }).unwrap();
        };
        let mk = |parent: u64, name: String, node_type: NodeType| FsOp::Mknode {
            fs: "f".into(),
            parent,
            op_id: name.clone(),
            name,
            node_type,
            target: None,
            rdev: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            now_ns: 2,
        };
        apply(
            &mut c,
            FsOp::CreateFs {
                fs: "f".into(),
                name: "f".into(),
                now_ns: 1,
                extent_bytes: None,
            },
        );
        for d in 0..dirs.saturating_sub(1) {
            apply(&mut c, mk(ROOT_INO, format!("d{d}"), NodeType::Dir));
        }
        for i in 0..400_000u64 {
            let parent = if dirs == 1 {
                ROOT_INO
            } else {
                2 + i % (dirs - 1)
            };
            apply(&mut c, mk(parent, format!("file-{i}"), NodeType::File));
            if i % 1024 == 1023 {
                store.checkpoint(&mut c).unwrap();
            }
            if i % 100_000 == 99_999 {
                println!("{} files: {}", i + 1, peak_rss());
            }
        }
    }

    /// `cargo test --release -p atlas-native --lib store::bench -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn checkpoint_time_as_the_store_grows() {
        let td = tempfile::tempdir().unwrap();
        let store = CatalogStore::open(td.path().join(CATALOG_STORE)).unwrap();
        let mut c = Catalog::default();
        let mut index = 1;
        c.apply(
            1,
            index,
            &MetaCommand::Fs {
                op: FsOp::CreateFs {
                    fs: "f".into(),
                    name: "f".into(),
                    now_ns: 1,
                    extent_bytes: None,
                },
            },
        )
        .unwrap();
        for round in 0..64 {
            let t = std::time::Instant::now();
            for i in 0..1024 {
                index += 1;
                let name = format!("file-{round}-{i}");
                c.apply(
                    1,
                    index,
                    &MetaCommand::Fs {
                        op: FsOp::Mknode {
                            fs: "f".into(),
                            parent: ROOT_INO,
                            op_id: name.clone(),
                            name,
                            node_type: NodeType::File,
                            target: None,
                            rdev: 0,
                            mode: 0o644,
                            uid: 0,
                            gid: 0,
                            now_ns: 2,
                        },
                    },
                )
                .unwrap();
            }
            let applied = t.elapsed();
            let t = std::time::Instant::now();
            let n = store.checkpoint(&mut c).unwrap();
            if round % 8 == 7 {
                println!(
                    "{} inodes: apply 1024 in {applied:?}, checkpoint {n} records in {:?}, file {} KiB",
                    c.filesystems["f"].inodes.len(),
                    t.elapsed(),
                    std::fs::metadata(store.path()).unwrap().len() / 1024
                );
            }
        }
    }
}
