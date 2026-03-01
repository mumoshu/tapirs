use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use super::types::{UnifiedLsmEntry, ValueLocation};
use super::UnifiedStore;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

// Bring OccTimestamp trait into scope for Timestamp::from_time().
#[cfg(test)]
use crate::occ::Timestamp as _;

impl<K, V, IO: DiskIo> UnifiedStore<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Eq + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
    pub(crate) fn get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn get_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_at(key, timestamp) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn get_range(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        if let Some((ck, _entry)) = self.unified_memtable().get_at(key, timestamp) {
            let write_ts = ck.timestamp.0;
            let next = self.unified_memtable().find_next_version(key, write_ts);
            return Ok((write_ts, next));
        }
        Ok((Timestamp::default(), None))
    }

    pub(crate) fn put(
        &mut self,
        key: K,
        value: Option<V>,
        timestamp: Timestamp,
    ) -> Result<(), StorageError> {
        // For direct puts (not through commit_batch_for_transaction),
        // create a synthetic prepare entry for this single write.
        let txn_id = OccTransactionId {
            client_id: crate::IrClientId(u64::MAX),
            number: timestamp.time,
        };

        let value_ref = if value.is_some() {
            let mut txn = crate::occ::Transaction::default();
            txn.add_write(crate::tapir::Sharded::from(key.clone()), value);
            let txn = Arc::new(txn);
            self.register_prepare(txn_id, &txn, timestamp);

            if self.resolve_in_memory(&txn_id, 0).is_some() {
                Some(ValueLocation::InMemory {
                    txn_id,
                    write_index: 0,
                })
            } else {
                None
            }
        } else {
            None
        };

        self.unified_memtable_mut().insert(
            key,
            timestamp,
            UnifiedLsmEntry {
                value_ref,
                last_read_ts: None,
            },
        );

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn commit_get(
        &mut self,
        key: K,
        read: Timestamp,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.unified_memtable_mut()
            .update_last_read(&key, read, commit.time);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.unified_memtable().get_latest(key)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn get_last_read_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.unified_memtable().get_at(key, timestamp)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    pub(crate) fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        let results = self.unified_memtable().scan(start, end, timestamp);
        let mut output = Vec::new();
        for (ck, entry) in results {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            output.push((ck.key.clone(), value, ts));
        }
        Ok(output)
    }

    pub(crate) fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        Ok(self
            .unified_memtable()
            .has_writes_in_range(start, end, after_ts, before_ts))
    }

}
