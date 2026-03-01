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
///
/// Provides two commit paths:
///
/// - **`commit_batch`** (from `MvccBackend`) — creates a synthetic prepare
///   per write.  Used when no prepare was registered beforehand.
///
/// - **`commit_batch_for_transaction`** — the normal TAPIR path.  Expects
///   `register_prepare` to have been called first.  Creates MVCC entries
///   with `ValueLocation::InMemory` pointing into the prepare_registry,
///   avoiding value duplication.  Falls back to `commit_batch` if the
///   prepare is not found (e.g., the prepare arrived in a different view
///   and was not recorded in the VLog index).
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

impl<K, V, IO: DiskIo> MvccBackend<K, V, Timestamp> for UnifiedMvccBackend<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    type Error = StorageError;

    fn get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.store.unified_memtable().get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.store.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    fn get_at(&self, key: &K, timestamp: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.store.unified_memtable().get_at(key, timestamp) {
            let ts = ck.timestamp.0;
            let value = self.store.resolve_value(entry)?;
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
        let txn_id = OccTransactionId {
            client_id: crate::IrClientId(u64::MAX),
            number: timestamp.time,
        };

        let write_set = if value.is_some() {
            vec![(key.clone(), value)]
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

        let value_ref = if self.store.resolve_in_memory(&txn_id, 0).is_some() {
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
            let value = self.store.resolve_value(entry)?;
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
    K: Clone + Ord + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    /// Register a prepared transaction for future zero-copy commit.
    ///
    /// Delegates to `UnifiedStore::register_prepare()`.
    pub fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) {
        self.store.register_prepare(txn_id, transaction, commit_ts);
    }

    /// Commit a prepared transaction by creating MVCC index entries.
    ///
    /// Three paths, tried in order:
    ///
    /// 1. **InMemory** — prepare is in the current view's `prepare_registry`.
    ///    MVCC entries get `ValueLocation::InMemory { txn_id, write_index }`.
    ///    This is the common case and avoids any VLog I/O.
    ///
    /// 2. **OnDisk** — prepare was sealed into the VLog (cross-view commit).
    ///    MVCC entries get `ValueLocation::OnDisk(ptr)` using the
    ///    `prepare_vlog_index`.  Value reads later go through the LRU cache.
    ///
    /// 3. **Fallback** — neither registry nor VLog index has the prepare.
    ///    Falls back to `commit_batch` which creates a synthetic prepare
    ///    per write (less efficient, but correct).
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
