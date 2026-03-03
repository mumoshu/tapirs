use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::tapir::Timestamp;
use super::UnifiedStore;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

// Bring OccTimestamp trait into scope for Timestamp::from_time().
#[cfg(test)]
use crate::occ::Timestamp as _;

impl<K, V, IO: DiskIo> UnifiedStore<K, V, IO>
where
    K: Clone + Ord + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    V: Clone + Eq + Hash + Debug + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    IO: DiskIo,
{
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
