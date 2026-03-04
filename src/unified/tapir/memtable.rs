use super::storage_types::{LsmEntry, ValueLocation, VlogTransactionPtr};
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::wisckeylsm::types::VlogPtr;
use std::collections::BTreeMap;

pub(crate) struct Memtable<K: Ord> {
    map: std::collections::BTreeMap<CompositeKey<K, Timestamp>, LsmEntry>,
}

impl<K: Ord> Default for Memtable<K> {
    fn default() -> Self {
        Self {
            map: std::collections::BTreeMap::new(),
        }
    }
}

impl<K: Ord + Clone> Memtable<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: K, ts: Timestamp, entry: LsmEntry) {
        self.map.insert(CompositeKey::new(key, ts), entry);
    }

    pub fn get_at(&self, key: &K, ts: Timestamp) -> Option<(&CompositeKey<K, Timestamp>, &LsmEntry)> {
        let search = CompositeKey::new(key.clone(), ts);
        self.map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
    }

    pub fn get_latest(&self, key: &K) -> Option<(&CompositeKey<K, Timestamp>, &LsmEntry)>
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
    ) -> Vec<(&CompositeKey<K, Timestamp>, &LsmEntry)>
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

    pub fn convert_in_memory_to_on_disk(
        &mut self,
        txn_vlog_index: &BTreeMap<OccTransactionId, VlogPtr>,
    ) {
        for entry in self.map.values_mut() {
            if let Some(ValueLocation::InMemory { txn_id, write_index }) = &entry.value_ref
                && let Some(vlog_ptr) = txn_vlog_index.get(txn_id)
            {
                entry.value_ref = Some(ValueLocation::OnDisk(VlogTransactionPtr {
                    txn_ptr: *vlog_ptr,
                    write_index: *write_index,
                }));
            }
        }
    }
}
