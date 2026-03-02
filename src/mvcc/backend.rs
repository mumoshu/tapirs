use std::fmt::Debug;

/// Abstract MVCC storage backend.
///
/// `DiskStore` implements this with `Error = StorageError`.
pub trait MvccBackend<K, V, TS>: Send {
    type Error: Debug + Send;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), Self::Error>;
    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), Self::Error>;
    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), Self::Error>;
    fn put(&mut self, key: K, value: Option<V>, timestamp: TS) -> Result<(), Self::Error>;
    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), Self::Error>;
    fn get_last_read(&self, key: &K) -> Result<Option<TS>, Self::Error>;
    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, Self::Error>;
    /// Return all key-value pairs in `[start..=end]` at the given timestamp.
    fn scan(&self, start: &K, end: &K, timestamp: TS) -> Result<Vec<(K, Option<V>, TS)>, Self::Error>;
    /// Check if any writes exist for keys in `[start..=end]` with timestamps in `(after_ts, before_ts)`.
    fn has_writes_in_range(&self, start: &K, end: &K, after_ts: TS, before_ts: TS) -> Result<bool, Self::Error>;

    /// Apply a batch of writes and read-timestamp updates for a committed transaction.
    ///
    /// Accepts the full write set and read set at once. The default implementation
    /// delegates to individual `put()` and `commit_get()` calls. `DiskStore`
    /// overrides this to batch all vlog appends into a single write, reducing
    /// per-transaction I/O from O(keys) to O(1) syscalls.
    fn commit_batch(
        &mut self,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, TS)>,
        commit: TS,
    ) -> Result<(), Self::Error>
    where
        TS: Copy,
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
