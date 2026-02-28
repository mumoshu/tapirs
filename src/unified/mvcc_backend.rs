use super::types::*;
use super::UnifiedStore;
use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// Typed adapter that implements `MvccBackend<K, V, Timestamp>` by
/// delegating to `UnifiedStore`.
pub struct UnifiedMvccBackend<K: Ord, V, IO: DiskIo> {
    store: UnifiedStore<K, IO>,
    _marker: PhantomData<V>,
}

impl<K: Ord + Clone, V, IO: DiskIo> UnifiedMvccBackend<K, V, IO> {
    pub fn new(store: UnifiedStore<K, IO>) -> Self {
        Self {
            store,
            _marker: PhantomData,
        }
    }

    pub fn inner(&self) -> &UnifiedStore<K, IO> {
        &self.store
    }

    pub fn inner_mut(&mut self) -> &mut UnifiedStore<K, IO> {
        &mut self.store
    }
}

/// In-memory MVCC memtable using UnifiedLsmEntry as values.
/// Wraps a BTreeMap<CompositeKey<K, Timestamp>, UnifiedLsmEntry>.
struct UnifiedMemtable<K: Ord> {
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
    fn insert(&mut self, key: K, ts: Timestamp, entry: UnifiedLsmEntry) {
        self.map.insert(CompositeKey::new(key, ts), entry);
    }

    fn get_at(&self, key: &K, ts: Timestamp) -> Option<(&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)> {
        let search = CompositeKey::new(key.clone(), ts);
        self.map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
    }

    fn get_latest(&self, key: &K) -> Option<(&CompositeKey<K, Timestamp>, &UnifiedLsmEntry)>
    where
        Timestamp: MaxValue,
    {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        self.map
            .range(search..)
            .next()
            .filter(|(k, _)| k.key == *key)
    }

    fn update_last_read(&mut self, key: &K, ts: Timestamp, read_ts: u64) {
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

    fn scan(
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

    fn has_writes_in_range(
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
}

impl<K, V, IO: DiskIo> MvccBackend<K, V, Timestamp> for UnifiedMvccBackend<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    type Error = StorageError;

    fn get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        // Look up latest version in unified memtable
        let _key_bytes =
            bitcode::serialize(key).map_err(|e| StorageError::Codec(e.to_string()))?;

        // Check unified memtable (we store entries in the existing memtable for now)
        if let Some((ck, entry)) = self.store.mvcc_memtable().get_latest(key) {
            // The existing memtable uses LsmEntry, not UnifiedLsmEntry.
            // For the unified backend, we need our own storage.
            // This is handled by the unified_memtable field.
            let ts = ck.timestamp.0;
            if let Some(value_ptr) = &entry.value_ptr {
                // Resolve value - for now return default
                let _ = value_ptr;
            }
            return Ok((None, ts));
        }

        Ok((None, Timestamp::default()))
    }

    fn get_at(&self, key: &K, timestamp: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        // Check memtable first
        if let Some((ck, entry)) = self.store.mvcc_memtable().get_at(key, &timestamp) {
            let ts = ck.timestamp.0;
            if let Some(value_ptr) = &entry.value_ptr {
                let _ = value_ptr;
            }
            return Ok((None, ts));
        }

        // Check SSTs
        // TODO: Check MVCC SSTs from sealed views

        Ok((None, Timestamp::default()))
    }

    fn get_range(&self, key: &K, timestamp: Timestamp) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        // Find the version at or before `timestamp`
        if let Some((ck, entry)) = self.store.mvcc_memtable().get_at(key, &timestamp) {
            let write_ts = ck.timestamp.0;
            // Find the next version after this one
            let next_ts = self.store.mvcc_memtable().get_at(key, &Timestamp {
                time: write_ts.time + 1,
                ..write_ts
            });
            let _ = (entry, next_ts);
            return Ok((write_ts, None));
        }

        Ok((Timestamp::default(), None))
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: Timestamp) -> Result<(), StorageError> {
        // For direct puts (not through commit_batch_for_transaction),
        // we create a simple entry. This is used for initial data setup.
        let value_bytes = value
            .as_ref()
            .map(|v| bitcode::serialize(v).map_err(|e| StorageError::Codec(e.to_string())))
            .transpose()?;
        let key_bytes =
            bitcode::serialize(&key).map_err(|e| StorageError::Codec(e.to_string()))?;

        // Create a synthetic prepare entry for this single write
        let txn_id = OccTransactionId {
            client_id: crate::IrClientId(u64::MAX),
            number: timestamp.time,
        };

        let write_set = if let Some(vb) = value_bytes {
            vec![(key_bytes, vb)]
        } else {
            vec![]
        };

        let prepare = Arc::new(CachedPrepare {
            transaction_id: txn_id,
            commit_ts: timestamp,
            read_set: vec![],
            write_set,
            scan_set: vec![],
        });

        self.store.register_prepare_raw(txn_id, prepare);

        // Insert into memtable with InMemory location
        let entry = crate::mvcc::disk::memtable::LsmEntry {
            value_ptr: None, // Unified path uses ValueLocation::InMemory
            last_read_ts: None,
        };

        self.store.mvcc_memtable_mut().insert(
            CompositeKey::new(key, timestamp),
            entry,
        );

        Ok(())
    }

    fn commit_get(
        &mut self,
        key: K,
        read: Timestamp,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.store
            .mvcc_memtable_mut()
            .update_last_read(&key, &read, commit.time);
        Ok(())
    }

    fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.store.mvcc_memtable().get_latest(key)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    fn get_last_read_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.store.mvcc_memtable().get_at(key, &timestamp)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        let results = self.store.mvcc_memtable().scan(start, end, &timestamp);
        let mut output = Vec::new();
        for (ck, _entry) in results {
            let ts = ck.timestamp.0;
            // TODO: Resolve value from VLog/prepare_registry
            output.push((ck.key.clone(), None, ts));
        }
        Ok(output)
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        // Check memtable for writes in the given timestamp range
        let from = CompositeKey::new(start.clone(), Timestamp::max_value());
        for (ck, entry) in self.store.mvcc_memtable().entries().range(from..) {
            if ck.key > *end {
                break;
            }
            let ts = ck.timestamp.0;
            if ts > after_ts && ts < before_ts && entry.value_ptr.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn commit_batch(
        &mut self,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, Timestamp)>,
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        Timestamp: Copy,
    {
        for (key, value) in writes {
            self.put(key, value, commit)?;
        }
        for (key, read) in reads {
            self.commit_get(key, read, commit)?;
        }
        Ok(())
    }
}

use crate::occ::Timestamp as OccTimestamp;

impl<K, V, IO: DiskIo> UnifiedMvccBackend<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    /// Register a prepared transaction for future zero-copy commit.
    pub fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) {
        let shard = crate::tapir::ShardNumber(0); // Unified store serves one shard

        let read_set: Vec<(Vec<u8>, Timestamp)> = transaction
            .shard_read_set(shard)
            .map(|(k, ts)| {
                let kb = bitcode::serialize(k).unwrap_or_default();
                (kb, ts)
            })
            .collect();

        let write_set: Vec<(Vec<u8>, Vec<u8>)> = transaction
            .shard_write_set(shard)
            .map(|(k, v)| {
                let kb = bitcode::serialize(k).unwrap_or_default();
                let vb = v
                    .as_ref()
                    .map(|val| bitcode::serialize(val).unwrap_or_default())
                    .unwrap_or_default();
                (kb, vb)
            })
            .collect();

        let scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)> = transaction
            .shard_scan_set(shard)
            .map(|entry| {
                let start = bitcode::serialize(&entry.start_key).unwrap_or_default();
                let end = bitcode::serialize(&entry.end_key).unwrap_or_default();
                (start, end, entry.timestamp)
            })
            .collect();

        let prepare = Arc::new(CachedPrepare {
            transaction_id: txn_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        });

        self.store.register_prepare_raw(txn_id, prepare);
    }

    /// Commit with transaction identity for PrepareRef lookup.
    /// Creates MVCC entries with ValueLocation::InMemory.
    pub fn commit_batch_for_transaction(
        &mut self,
        txn_id: OccTransactionId,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, Timestamp)>,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        // Look up the prepare in registry to find write_index mapping
        let has_prepare = self.store.prepare_registry.contains_key(&txn_id);

        if has_prepare {
            // Create MVCC entries with InMemory location pointing to prepare_registry
            for (key, _value) in writes.iter() {
                let entry = crate::mvcc::disk::memtable::LsmEntry {
                    value_ptr: None, // Unified path uses ValueLocation::InMemory
                    last_read_ts: None,
                };

                self.store.mvcc_memtable_mut().insert(
                    CompositeKey::new(key.clone(), commit),
                    entry,
                );
            }
        } else {
            // Fallback: no prepare registered, use direct commit_batch
            return self.commit_batch(writes, reads, commit);
        }

        // Update read timestamps
        for (key, read) in reads {
            self.commit_get(key, read, commit)?;
        }

        Ok(())
    }

    fn commit_get(&mut self, key: K, read: Timestamp, commit: Timestamp) -> Result<(), StorageError> {
        self.store
            .mvcc_memtable_mut()
            .update_last_read(&key, &read, commit.time);
        Ok(())
    }
}
