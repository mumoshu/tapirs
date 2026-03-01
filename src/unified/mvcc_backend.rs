use super::UnifiedStore;
use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

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
    V: Clone + Eq + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    type Error = StorageError;

    fn get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        self.store.get(key)
    }

    fn get_at(&self, key: &K, timestamp: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        self.store.get_at(key, timestamp)
    }

    fn get_range(&self, key: &K, timestamp: Timestamp) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        self.store.get_range(key, timestamp)
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: Timestamp) -> Result<(), StorageError> {
        self.store.put(key, value, timestamp)
    }

    fn commit_get(
        &mut self,
        key: K,
        read: Timestamp,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.store.commit_get(key, read, commit)
    }

    fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        self.store.get_last_read(key)
    }

    fn get_last_read_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        self.store.get_last_read_at(key, timestamp)
    }

    fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        self.store.scan(start, end, timestamp)
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
        self.store.commit_batch(writes, reads, commit)
    }
}

impl<K, V, IO: DiskIo> UnifiedMvccBackend<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Eq + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
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
    /// `commit` is the final commit timestamp (may differ from the
    /// prepare-time proposal stored in `CachedPrepare::commit_ts`).
    /// See `UnifiedStore::commit_prepared()` for details.
    pub fn commit_batch_for_transaction(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.store.commit_prepared(txn_id, commit)
    }
}
