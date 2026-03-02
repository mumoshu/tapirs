use super::types::{UnifiedLsmEntry, UnifiedVlogPrepareValuePtr, UnifiedVlogPtr, ValueLocation};
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use std::collections::BTreeMap;

/// In-memory MVCC memtable using UnifiedLsmEntry as values.
/// Wraps a BTreeMap<CompositeKey<K, Timestamp>, UnifiedLsmEntry>.
pub(crate) struct UnifiedMemtable<K: Ord> {
    map: std::collections::BTreeMap<CompositeKey<K, Timestamp>, UnifiedLsmEntry>,
}

impl<K: Ord> Default for UnifiedMemtable<K> {
    fn default() -> Self {
        Self {
            map: std::collections::BTreeMap::new(),
        }
    }
}

impl<K: Ord + Clone> UnifiedMemtable<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, ts: Timestamp, entry: UnifiedLsmEntry) {
        self.map.insert(CompositeKey::new(key, ts), entry);
    }

    pub fn get_at(&self, key: &K, ts: Timestamp) -> Option<(&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)> {
        let search = CompositeKey::new(key.clone(), ts);
        self.map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
    }

    pub fn get_latest(&self, key: &K) -> Option<(&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)>
    where
        Timestamp: MaxValue,
    {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        self.map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
    }

    pub fn update_last_read(&mut self, key: &K, ts: Timestamp, read_ts: u64) {
        let search = CompositeKey::new(key.clone(), ts);
        if let Some((found_key, _)) = self
            .map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
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

    pub fn scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Vec<(&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)>
    where
        Timestamp: MaxValue,
    {
        let from = CompositeKey::new(start.clone(), Timestamp::max_value());
        let mut results = Vec::new();
        let mut last_key: Option<&K> = None;

        for (ck, entry) in self.map.range(from..) {
            if ck.key > *end {
                break;
            }
            if ck.timestamp.0 > ts {
                continue;
            }
            if last_key == Some(&ck.key) {
                continue;
            }
            last_key = Some(&ck.key);
            results.push((ck, entry));
        }
        results
    }

    pub fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> bool
    where
        Timestamp: MaxValue,
    {
        let from = CompositeKey::new(start.clone(), Timestamp::max_value());
        for (ck, entry) in self.map.range(from..) {
            if ck.key > *end {
                break;
            }
            let ts = ck.timestamp.0;
            if ts > after_ts && ts < before_ts && entry.value_ref.is_some() {
                return true;
            }
        }
        false
    }

    /// Find the next version of a key after the given timestamp.
    /// Returns the timestamp of the first version with ts > after_ts.
    pub fn find_next_version(&self, key: &K, after_ts: Timestamp) -> Option<Timestamp>
    where
        Timestamp: MaxValue,
    {
        // Scan all versions of this key from newest to oldest (BTreeMap order
        // with Reverse<Timestamp>). Collect versions with ts > after_ts,
        // then return the smallest (closest successor).
        let from = CompositeKey::new(key.clone(), Timestamp::max_value());
        let mut best: Option<Timestamp> = None;

        for (ck, _) in self.map.range(from..) {
            if ck.key != *key {
                break;
            }
            let ts = ck.timestamp.0;
            if ts > after_ts {
                // This version is newer — track the closest successor
                best = Some(match best {
                    Some(b) if ts < b => ts,
                    Some(b) => b,
                    None => ts,
                });
            }
        }
        best
    }

    /// Iterate over all entries in the memtable.
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)> {
        self.map.iter()
    }

    /// Convert all InMemory value locations to OnDisk using the given
    /// txn_id -> VLog pointer mapping (populated at seal time).
    pub fn convert_in_memory_to_on_disk(
        &mut self,
        txn_vlog_index: &BTreeMap<OccTransactionId, UnifiedVlogPtr>,
    ) {
        for entry in self.map.values_mut() {
            if let Some(ValueLocation::InMemory { txn_id, write_index }) = &entry.value_ref
                && let Some(vlog_ptr) = txn_vlog_index.get(txn_id)
            {
                entry.value_ref = Some(ValueLocation::OnDisk(UnifiedVlogPrepareValuePtr {
                    txn_ptr: *vlog_ptr,
                    write_index: *write_index,
                }));
            }
        }
    }
}
