use super::types::*;
use super::UnifiedStore;
use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

/// Typed adapter that implements `MvccBackend<K, V, Timestamp>` by
/// delegating to `UnifiedStore`.
pub struct UnifiedMvccBackend<K: Ord, V, IO: DiskIo> {
    store: UnifiedStore<K, V, IO>,
}

impl<K: Ord + Clone, V, IO: DiskIo> UnifiedMvccBackend<K, V, IO> {
    pub fn new(store: UnifiedStore<K, V, IO>) -> Self {
        Self { store }
    }

    pub fn inner(&self) -> &UnifiedStore<K, V, IO> {
        &self.store
    }

    pub fn inner_mut(&mut self) -> &mut UnifiedStore<K, V, IO> {
        &mut self.store
    }
}

impl<K, V, IO: DiskIo> UnifiedMvccBackend<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Debug + Send + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    /// Resolve a value from a UnifiedLsmEntry's ValueLocation.
    ///
    /// - `InMemory { txn_id, write_index }` → look up in prepare_registry
    /// - `OnDisk(ptr)` → read from VLog via prepare_cache
    /// - `None` → tombstone / metadata-only entry
    fn resolve_value(&self, entry: &UnifiedLsmEntry) -> Result<Option<V>, StorageError> {
        match &entry.value_ref {
            None => Ok(None),
            Some(ValueLocation::InMemory { txn_id, write_index }) => {
                match self.store.resolve_in_memory(txn_id, *write_index) {
                    Some((_key_bytes, value_bytes)) => {
                        let value: V = bitcode::deserialize(value_bytes)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        Ok(Some(value))
                    }
                    None => Ok(None),
                }
            }
            Some(ValueLocation::OnDisk(ptr)) => {
                let cached = self.store.resolve_on_disk(ptr)?;
                if let Some((_key_bytes, value_bytes)) =
                    cached.write_set.get(ptr.write_index as usize)
                {
                    let value: V = bitcode::deserialize(value_bytes)
                        .map_err(|e| StorageError::Codec(e.to_string()))?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
        }
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
        if let Some((ck, entry)) = self.store.unified_memtable().get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    fn get_at(&self, key: &K, timestamp: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.store.unified_memtable().get_at(key, timestamp) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    fn get_range(&self, key: &K, timestamp: Timestamp) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        if let Some((ck, _entry)) = self.store.unified_memtable().get_at(key, timestamp) {
            let write_ts = ck.timestamp.0;
            let next = self.store.unified_memtable().find_next_version(key, write_ts);
            return Ok((write_ts, next));
        }
        Ok((Timestamp::default(), None))
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: Timestamp) -> Result<(), StorageError> {
        // For direct puts (not through commit_batch_for_transaction),
        // create a synthetic prepare entry for this single write.
        let value_bytes = value
            .as_ref()
            .map(|v| bitcode::serialize(v).map_err(|e| StorageError::Codec(e.to_string())))
            .transpose()?;
        let key_bytes =
            bitcode::serialize(&key).map_err(|e| StorageError::Codec(e.to_string()))?;

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

        let value_ref = if value.is_some() {
            Some(ValueLocation::InMemory {
                txn_id,
                write_index: 0,
            })
        } else {
            None
        };

        self.store.unified_memtable_mut().insert(
            key,
            timestamp,
            UnifiedLsmEntry {
                value_ref,
                last_read_ts: None,
            },
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
            .unified_memtable_mut()
            .update_last_read(&key, read, commit.time);
        Ok(())
    }

    fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.store.unified_memtable().get_latest(key)
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
        if let Some((_, entry)) = self.store.unified_memtable().get_at(key, timestamp)
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
        let results = self.store.unified_memtable().scan(start, end, timestamp);
        let mut output = Vec::new();
        for (ck, entry) in results {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            output.push((ck.key.clone(), value, ts));
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
        Ok(self
            .store
            .unified_memtable()
            .has_writes_in_range(start, end, after_ts, before_ts))
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

// Bring OccTimestamp trait into scope for Timestamp::from_time().
use crate::occ::Timestamp as _;

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
    /// Creates MVCC entries with ValueLocation::InMemory pointing to
    /// the prepare_registry, avoiding value duplication in the VLog.
    pub fn commit_batch_for_transaction(
        &mut self,
        txn_id: OccTransactionId,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, Timestamp)>,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        let has_prepare = self.store.prepare_registry.contains_key(&txn_id);
        let cross_view_ptr = self.store.prepare_vlog_index.get(&txn_id).copied();

        if has_prepare {
            // InMemory path: prepare is in current view's prepare_registry
            for (i, (key, value)) in writes.iter().enumerate() {
                let value_ref = if value.is_some() {
                    Some(ValueLocation::InMemory {
                        txn_id,
                        write_index: i as u16,
                    })
                } else {
                    None
                };

                self.store.unified_memtable_mut().insert(
                    key.clone(),
                    commit,
                    UnifiedLsmEntry {
                        value_ref,
                        last_read_ts: None,
                    },
                );
            }
        } else if let Some(vlog_ptr) = cross_view_ptr {
            // OnDisk path: prepare is in a sealed VLog (cross-view commit)
            for (i, (key, value)) in writes.iter().enumerate() {
                let value_ref = if value.is_some() {
                    Some(ValueLocation::OnDisk(UnifiedVlogPrepareValuePtr {
                        prepare_ptr: vlog_ptr,
                        write_index: i as u16,
                    }))
                } else {
                    None
                };

                self.store.unified_memtable_mut().insert(
                    key.clone(),
                    commit,
                    UnifiedLsmEntry {
                        value_ref,
                        last_read_ts: None,
                    },
                );
            }
        } else {
            // Fallback: no prepare registered and not in VLog index
            return self.commit_batch(writes, reads, commit);
        }

        // Update read timestamps
        for (key, read) in reads {
            MvccBackend::commit_get(self, key, read, commit)?;
        }

        Ok(())
    }
}
