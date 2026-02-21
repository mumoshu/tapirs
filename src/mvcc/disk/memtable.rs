use super::vlog::ValuePointer;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// Trait for types that have a maximum value.
/// Used to construct seek keys for BTreeMap lookups.
pub trait MaxValue {
    fn max_value() -> Self;
}

impl MaxValue for u64 {
    fn max_value() -> Self {
        u64::MAX
    }
}

/// Composite key: (user key ASC, timestamp DESC).
///
/// Descending timestamp within each key means the latest version
/// appears first when iterating forward through the BTreeMap.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CompositeKey<K, TS> {
    pub key: K,
    pub timestamp: Reverse<TS>,
}

impl<K, TS> CompositeKey<K, TS> {
    pub fn new(key: K, timestamp: TS) -> Self {
        Self {
            key,
            timestamp: Reverse(timestamp),
        }
    }
}

/// Entry stored in the LSM index (memtable and SSTables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmEntry {
    /// Pointer to value in vlog. `None` means the value is inlined
    /// or this is a metadata-only entry (e.g., `commit_get`).
    pub value_ptr: Option<ValuePointer>,
    /// Last-read timestamp for OCC conflict detection.
    pub last_read_ts: Option<u64>,
}

/// In-memory write buffer backed by a sorted BTreeMap.
pub struct Memtable<K, TS> {
    map: BTreeMap<CompositeKey<K, TS>, LsmEntry>,
    /// Approximate size in bytes (for flush threshold).
    approx_bytes: usize,
}

impl<K: Ord, TS: Ord> Default for Memtable<K, TS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, TS: Ord> Memtable<K, TS> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_bytes: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Insert or update an entry.
    pub fn insert(&mut self, key: CompositeKey<K, TS>, entry: LsmEntry) {
        // Rough size estimate: 64 bytes per entry overhead.
        self.approx_bytes += 64;
        self.map.insert(key, entry);
    }

    /// Get the entry for an exact composite key.
    pub fn get(&self, key: &CompositeKey<K, TS>) -> Option<&LsmEntry> {
        self.map.get(key)
    }

    /// Find the latest version of `key` with timestamp <= `ts`.
    ///
    /// Scans forward from `CompositeKey(key, Reverse(ts))` — since
    /// timestamps are reversed, this finds the first entry whose
    /// original timestamp is <= `ts`.
    pub fn get_at(&self, key: &K, ts: &TS) -> Option<(&CompositeKey<K, TS>, &LsmEntry)>
    where
        K: Clone,
        TS: Clone,
    {
        let search = CompositeKey::new(key.clone(), ts.clone());
        // Range from search..= end of this key's versions.
        // Because Reverse<TS>, searching from (key, Reverse(ts)) forward
        // gives versions with ts <= requested ts.
        self.map.range(search..).next().filter(|(k, _)| k.key == *key)
    }

    /// Find the latest version of `key` (any timestamp).
    ///
    /// Uses `TS::MAX` so that `Reverse(MAX)` is the smallest Reverse
    /// value, placing us at the very start of this key's entries.
    pub fn get_latest(&self, key: &K) -> Option<(&CompositeKey<K, TS>, &LsmEntry)>
    where
        K: Clone,
        TS: Clone + MaxValue,
    {
        let search = CompositeKey::new(key.clone(), TS::max_value());
        self.map.range(search..).next().filter(|(k, _)| k.key == *key)
    }

    /// Update the `last_read_ts` for the version at or before `ts`.
    pub fn update_last_read(&mut self, key: &K, ts: &TS, read_ts: u64)
    where
        K: Clone,
        TS: Clone,
    {
        let search = CompositeKey::new(key.clone(), ts.clone());
        if let Some((found_key, _)) = self.map.range(search..).next().filter(|(k, _)| k.key == *key)
        {
            let found_key = found_key.clone();
            if let Some(entry) = self.map.get_mut(&found_key) {
                entry.last_read_ts = Some(match entry.last_read_ts {
                    Some(existing) => existing.max(read_ts),
                    None => read_ts,
                });
            }
        }
    }

    /// Drain all entries for SSTable flush.
    pub fn drain(&mut self) -> BTreeMap<CompositeKey<K, TS>, LsmEntry> {
        self.approx_bytes = 0;
        std::mem::take(&mut self.map)
    }

    /// Borrow the entries map (for SSTable write before clearing).
    pub fn entries(&self) -> &BTreeMap<CompositeKey<K, TS>, LsmEntry> {
        &self.map
    }

    /// Clear all entries and reset byte counter.
    pub fn clear(&mut self) {
        self.map.clear();
        self.approx_bytes = 0;
    }

    /// Iterate all entries (for flush or scan).
    pub fn iter(&self) -> impl Iterator<Item = (&CompositeKey<K, TS>, &LsmEntry)> {
        self.map.iter()
    }

    /// Range scan: all versions of keys in `[start, end]` at `ts`.
    pub fn scan(
        &self,
        start: &K,
        end: &K,
        ts: &TS,
    ) -> Vec<(&CompositeKey<K, TS>, &LsmEntry)>
    where
        K: Clone,
        TS: Clone + MaxValue,
    {
        let from = CompositeKey::new(start.clone(), TS::max_value());
        let mut results = Vec::new();
        let mut last_key: Option<&K> = None;

        for (ck, entry) in self.map.range(from..) {
            if ck.key > *end {
                break;
            }
            // Skip if timestamp is too new (Reverse means > in Ord = smaller TS).
            if ck.timestamp.0 > *ts {
                continue;
            }
            // Only take the first (latest) version per key.
            if last_key == Some(&ck.key) {
                continue;
            }
            last_key = Some(&ck.key);
            results.push((ck, entry));
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_latest() {
        let mut mt = Memtable::<String, u64>::new();

        mt.insert(
            CompositeKey::new("a".into(), 10),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );
        mt.insert(
            CompositeKey::new("a".into(), 20),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );

        let (ck, _) = mt.get_latest(&"a".into()).unwrap();
        assert_eq!(ck.timestamp.0, 20);
    }

    #[test]
    fn get_at_specific_timestamp() {
        let mut mt = Memtable::<String, u64>::new();

        mt.insert(
            CompositeKey::new("a".into(), 10),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );
        mt.insert(
            CompositeKey::new("a".into(), 20),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );

        // At ts=15, should get the version at ts=10.
        let (ck, _) = mt.get_at(&"a".into(), &15).unwrap();
        assert_eq!(ck.timestamp.0, 10);

        // At ts=20, should get the version at ts=20.
        let (ck, _) = mt.get_at(&"a".into(), &20).unwrap();
        assert_eq!(ck.timestamp.0, 20);
    }

    #[test]
    fn update_last_read() {
        let mut mt = Memtable::<String, u64>::new();
        mt.insert(
            CompositeKey::new("a".into(), 10),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );

        mt.update_last_read(&"a".into(), &10, 50);
        let (_, entry) = mt.get_at(&"a".into(), &10).unwrap();
        assert_eq!(entry.last_read_ts, Some(50));

        // Update with a smaller value — should keep the max.
        mt.update_last_read(&"a".into(), &10, 30);
        let (_, entry) = mt.get_at(&"a".into(), &10).unwrap();
        assert_eq!(entry.last_read_ts, Some(50));
    }

    #[test]
    fn drain_clears_memtable() {
        let mut mt = Memtable::<String, u64>::new();
        mt.insert(
            CompositeKey::new("a".into(), 10),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );
        let drained = mt.drain();
        assert_eq!(drained.len(), 1);
        assert!(mt.is_empty());
    }
}
