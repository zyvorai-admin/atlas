// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

/// A map whose entries expire `ttl` after they were stored. A zero `ttl` caches nothing.
pub struct TtlCache<K, V> {
    ttl: Duration,
    map: HashMap<K, (V, Instant)>,
}

impl<K: Hash + Eq, V: Clone> TtlCache<K, V> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            map: HashMap::new(),
        }
    }

    pub fn get(&mut self, k: &K) -> Option<V> {
        match self.map.get(k) {
            Some((v, at)) if at.elapsed() < self.ttl => Some(v.clone()),
            Some(_) => {
                self.map.remove(k);
                None
            }
            None => None,
        }
    }

    pub fn put(&mut self, k: K, v: V) {
        if self.ttl.is_zero() {
            return;
        }
        // Expired entries are only dropped on access; sweep before the map grows unbounded.
        if self.map.len() >= 65_536 {
            let ttl = self.ttl;
            self.map.retain(|_, (_, at)| at.elapsed() < ttl);
        }
        self.map.insert(k, (v, Instant::now()));
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.map.remove(k).map(|(v, _)| v)
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_expire() {
        let mut c = TtlCache::new(Duration::from_millis(30));
        c.put(1, "a");
        assert_eq!(c.get(&1), Some("a"));
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(c.get(&1), None);
        let mut off = TtlCache::new(Duration::ZERO);
        off.put(1, "a");
        assert_eq!(off.get(&1), None);
    }
}
