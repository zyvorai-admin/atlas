// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! A filesystem's inode table. Inodes are shared (`Arc`): a read hands out a reference without
//! holding the table, and a snapshot or clone shares every inode until one side changes it.
//! Changes are tracked for the catalog store like the catalog's other maps.
//!
//! Once the catalog store holds a filesystem, its table is paged: memory keeps only the inodes
//! changed since the last checkpoint, and every other read goes to the store as of that
//! checkpoint, through a bounded cache ([`StoreSnapshot`]).

use std::{collections::BTreeMap, fmt, io, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    metadata::MetaError,
    namespace::{FsId, Inode, InodeKind},
    store::StoreSnapshot,
    tracked::Tracked,
};

#[derive(Debug, Clone, Default)]
pub struct InodeTable {
    /// Every inode of an unpaged table; the inodes changed since the last checkpoint of a
    /// paged one (an inode removed since is absent here but in the change record).
    map: Tracked<u64, Arc<Inode>>,
    len: u64,
    paged: Option<Paged>,
}

/// Where a paged table's inodes are in the store: under `key`, a filesystem's id or
/// `@<snapshot id>` for a snapshot's frozen tree.
#[derive(Clone)]
struct Paged {
    key: FsId,
    store: Arc<StoreSnapshot>,
}

impl fmt::Debug for Paged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Paged").field("key", &self.key).finish()
    }
}

/// Inodes read at a time by full walks of a table.
const PAGE: usize = 1024;

fn store_err(e: io::Error) -> MetaError {
    MetaError::Store(e.to_string())
}

impl InodeTable {
    /// A table of `len` inodes that the store holds under `key`.
    pub(crate) fn paged(key: FsId, store: Arc<StoreSnapshot>, len: u64) -> Self {
        Self {
            map: Tracked::default(),
            len,
            paged: Some(Paged { key, store }),
        }
    }

    /// The unchanged store behind `ino`, unless the table changed it since the checkpoint.
    fn behind(&self, ino: u64) -> Option<&Paged> {
        self.paged
            .as_ref()
            .filter(|_| !self.map.touched().contains(&ino))
    }

    pub fn get(&self, ino: u64) -> Result<Option<Arc<Inode>>, MetaError> {
        if let Some(i) = self.map.get(&ino) {
            return Ok(Some(i.clone()));
        }
        match self.behind(ino) {
            Some(p) => p.store.get(&p.key, ino).map_err(store_err),
            None => Ok(None),
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Brings `ino` into `map` to be changed; false if there is no such inode.
    fn page_in(&mut self, ino: u64) -> Result<bool, MetaError> {
        if self.map.contains_key(&ino) {
            return Ok(true);
        }
        let Some(p) = self.behind(ino) else {
            return Ok(false);
        };
        match p.store.take(&p.key, ino).map_err(store_err)? {
            Some(i) => {
                self.map.insert_quietly(ino, i);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The inode to edit in place; copied first if a snapshot or a reader still shares it.
    pub fn get_mut(&mut self, ino: u64) -> Result<Option<&mut Inode>, MetaError> {
        if !self.page_in(ino)? {
            return Ok(None);
        }
        Ok(self.map.get_mut(&ino).map(Arc::make_mut))
    }

    /// Adds a new inode (inode numbers are never reused).
    pub fn insert(&mut self, inode: Inode) {
        if self.map.insert(inode.ino, Arc::new(inode)).is_none() {
            self.len += 1;
        }
    }

    pub fn remove(&mut self, ino: u64) -> Result<Option<Inode>, MetaError> {
        if !self.page_in(ino)? {
            return Ok(None);
        }
        let i = self.map.remove(&ino).map(Arc::unwrap_or_clone);
        if i.is_some() {
            self.len -= 1;
        }
        Ok(i)
    }

    /// Every inode, in inode order (read from the store for a paged table, bypassing its cache).
    pub fn scan(&self) -> Result<Vec<Arc<Inode>>, MetaError> {
        let Some(p) = &self.paged else {
            return Ok(self.map.values().cloned().collect());
        };
        let mut all = BTreeMap::new();
        for ino in p.store.inos(&p.key).map_err(store_err)? {
            if self.map.touched().contains(&ino) {
                continue;
            }
            if let Some(i) = p.store.peek(&p.key, ino).map_err(store_err)? {
                all.insert(ino, i);
            }
        }
        all.extend(self.map.iter().map(|(k, v)| (*k, v.clone())));
        Ok(all.into_values().collect())
    }

    /// Up to `limit` inodes numbered `from` or more, in inode order (bypassing the cache).
    pub fn page(&self, from: u64, limit: usize) -> Result<Vec<Arc<Inode>>, MetaError> {
        let mut out: BTreeMap<u64, Arc<Inode>> = self
            .map
            .range(from..)
            .take(limit)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        if let Some(p) = &self.paged {
            let touched = self.map.touched();
            for ino in p
                .store
                .inos_from(&p.key, from, limit, |i| touched.contains(&i))
                .map_err(store_err)?
            {
                if let Some(i) = p.store.peek(&p.key, ino).map_err(store_err)? {
                    out.insert(ino, i);
                }
            }
        }
        Ok(out.into_values().take(limit).collect())
    }

    /// Calls `f` on every inode in inode order, a page at a time.
    pub fn for_each(&self, mut f: impl FnMut(&Inode)) -> Result<(), MetaError> {
        let mut from = 0;
        loop {
            let page = self.page(from, PAGE)?;
            for i in &page {
                f(i);
            }
            match page.last() {
                Some(last) if page.len() == PAGE => from = last.ino + 1,
                _ => return Ok(()),
            }
        }
    }

    /// Inodes changed since [`Self::clear_changes`].
    pub(crate) fn touched(&self) -> impl Iterator<Item = u64> + '_ {
        self.map.touched().iter().copied()
    }

    /// Whether `ino` was inserted or removed (rather than edited in place) since the last
    /// [`Self::clear_changes`].
    pub(crate) fn replaced(&self, ino: u64) -> bool {
        self.map.replaced().contains(&ino)
    }

    /// Forgets the changes, including those of the directory entries of changed inodes.
    pub(crate) fn clear_changes(&mut self) {
        self.map.clear_changes_with(|i| {
            if matches!(&i.kind, InodeKind::Dir { entries, .. } if !entries.touched().is_empty()) {
                if let InodeKind::Dir { entries, .. } = &mut Arc::make_mut(i).kind {
                    entries.clear_changes();
                }
            }
        });
    }

    /// Pages the table on `store`, which holds all of it under `key`, handing the inodes in
    /// memory to its cache. Only after [`Self::clear_changes`].
    pub(crate) fn attach(&mut self, key: FsId, store: Arc<StoreSnapshot>) {
        debug_assert!(self.map.touched().is_empty());
        for i in std::mem::take(&mut self.map).into_values() {
            store.put(&key, i.ino, i);
        }
        self.paged = Some(Paged { key, store });
    }
}

impl FromIterator<Inode> for InodeTable {
    fn from_iter<I: IntoIterator<Item = Inode>>(iter: I) -> Self {
        let map: Tracked<u64, Arc<Inode>> =
            iter.into_iter().map(|i| (i.ino, Arc::new(i))).collect();
        Self {
            len: map.len() as u64,
            map,
            paged: None,
        }
    }
}

/// Equality is over contents (read from the store where paged; a store error is inequality).
impl PartialEq for InodeTable {
    fn eq(&self, other: &Self) -> bool {
        matches!((self.scan(), other.scan()), (Ok(a), Ok(b)) if a == b)
    }
}
impl Eq for InodeTable {}

impl Serialize for InodeTable {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let all = self.scan().map_err(serde::ser::Error::custom)?;
        s.collect_map(all.iter().map(|i| (i.ino, &**i)))
    }
}

impl<'de> Deserialize<'de> for InodeTable {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        BTreeMap::<u64, Inode>::deserialize(d).map(|m| m.into_values().collect())
    }
}
