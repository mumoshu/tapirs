use std::fmt::Debug;

/// Abstract MVCC storage backend.
///
/// `DiskStore` implements this with `Error = StorageError`.
pub trait MvccBackend<K, V, TS>: Send {
    type Error: Debug + Send;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), Self::Error>;
    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), Self::Error>;
    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), Self::Error>;
    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), Self::Error>;
    fn get_last_read(&self, key: &K) -> Result<Option<TS>, Self::Error>;
    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, Self::Error>;
    /// Return all key-value pairs in `[start..=end]` at the given timestamp.
    fn scan(&self, start: &K, end: &K, timestamp: TS) -> Result<Vec<(K, Option<V>, TS)>, Self::Error>;
    /// Check if any writes exist for keys in `[start..=end]` with timestamps in `(after_ts, before_ts)`.
    fn has_writes_in_range(&self, start: &K, end: &K, after_ts: TS, before_ts: TS) -> Result<bool, Self::Error>;

    /// Apply a batch of writes and read-timestamp updates for a committed transaction.
    ///
    /// This is the sole write path for committed data on the `MvccBackend` trait.
    /// The OCC layer calls this once per committed transaction with the full
    /// write set and read set.
    ///
    /// # Parameters
    ///
    /// - `writes`: The transaction's write set. Each `(key, Option<value>)` pair
    ///   stores a new version at `commit`. `Some(value)` creates a live entry;
    ///   `None` creates a tombstone (logical delete).
    ///
    /// - `reads`: The transaction's read set. Each `(key, read_timestamp)` pair
    ///   records which version the transaction observed, enabling future OCC
    ///   conflict detection.
    ///
    /// - `commit`: The commit timestamp. All writes are stored at this timestamp
    ///   and all read-tracking entries reference it.
    ///
    /// # Implementation Requirements
    ///
    /// 1. For each `(key, value)` in `writes`, store the value at `commit` so that
    ///    `get_at(key, commit)` returns it afterward.
    ///
    /// 2. For each `(key, read_ts)` in `reads`, call `commit_get(key, read_ts, commit)`
    ///    (or equivalent) to update the last-read timestamp for OCC conflict detection.
    ///
    /// 3. Batch I/O where possible. A naive loop of individual writes incurs O(keys)
    ///    syscalls per transaction. Implementations should batch writes into fewer I/O
    ///    operations (e.g., a single vlog append or a native database transaction).
    fn commit_batch(
        &mut self,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, TS)>,
        commit: TS,
    ) -> Result<(), Self::Error>
    where
        TS: Copy;
}
