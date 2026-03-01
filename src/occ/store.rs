use super::{SharedTransaction, Timestamp, Transaction, TransactionId};
use crate::{
    mvcc::backend::MvccBackend,
    tapir::{Key, ShardNumber, Value},
    util::vectorize_btree,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map, BTreeMap},
    fmt::Debug,
    ops::{Bound, Deref, DerefMut},
};
use tracing::trace;

#[derive(Serialize, Deserialize)]
pub struct Store<K, V, TS, M> {
    shard: ShardNumber,
    linearizable: bool,
    #[serde(bound(
        serialize = "M: Serialize",
        deserialize = "M: Deserialize<'de>"
    ))]
    inner: M,
    // BTreeMap for deterministic iteration during view change merge. Entries are
    // transient (cleared on commit/abort), so O(log n) vs O(1) is negligible for
    // the bounded number of concurrent transactions.
    /// Transactions which may commit in the future (and whether the prepare was
    /// finalized in the IR sense).
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "V: Serialize, TS: Serialize",
            deserialize = "V: Deserialize<'de>, TS: Deserialize<'de> + Ord"
        )
    )]
    pub prepared: BTreeMap<TransactionId, (TS, SharedTransaction<K, V, TS>, bool)>,
    // Cache.
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "K: Serialize + Ord, TS: Serialize",
            deserialize = "K: Deserialize<'de> + Ord, TS: Deserialize<'de> + Ord"
        )
    )]
    prepared_reads: BTreeMap<K, TimestampSet<TS>>,
    // Cache. BTreeMap for efficient range queries during scan-set validation.
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "K: Serialize + Ord, TS: Serialize",
            deserialize = "K: Deserialize<'de> + Ord, TS: Deserialize<'de> + Ord"
        )
    )]
    prepared_writes: BTreeMap<K, TimestampSet<TS>>,
    /// Range-level read timestamps from read-only QuorumScan operations.
    /// Each entry `(start_key, end_key, read_ts)` protects the entire range
    /// from future writes at commit_ts < read_ts (both overwrites and insertions).
    #[serde(bound(
        serialize = "K: Serialize + Ord, TS: Serialize",
        deserialize = "K: Deserialize<'de> + Ord, TS: Deserialize<'de>"
    ))]
    range_reads: Vec<(K, K, TS)>,
    /// Highest `commit` timestamp passed to `commit_get()` (from `quorum_read()`
    /// and committed read-write transactions). Used by resharding to compute
    /// the min_prepare_time baseline for new shards.
    #[serde(skip, bound(deserialize = ""))]
    max_read_commit_time: Option<TS>,
}

#[derive(Serialize, Deserialize)]
struct TimestampSet<TS> {
    /// Use a map in order to use APIs sets don't have.
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "TS: Serialize",
            deserialize = "TS: Deserialize<'de> + Ord"
        )
    )]
    inner: BTreeMap<TS, ()>,
}

impl<TS> Default for TimestampSet<TS> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<TS> Deref for TimestampSet<TS> {
    type Target = BTreeMap<TS, ()>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<TS> DerefMut for TimestampSet<TS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum PrepareResult<TS: Timestamp> {
    /// The transaction is possible.
    Ok,
    /// There was a conflict that might be resolved by retrying prepare at a different timestamp.
    Retry { proposed: TS::Time },
    /// There was a conflict with a prepared transaction (which may later abort).
    Abstain,
    /// There was a conflict with a committed transaction.
    Fail,
    /// It is too late to prepare with this commit timestamp.
    TooLate,
    /// The commit time is too old (would be or was already garbage collected).
    ///
    /// It isn't known whether such transactions were prepared, committed, or aborted.
    /// - Clients can safely hang i.e. while polling more replicas, or return an
    ///   indeterminate result.
    /// - Backup coordinators can safely give up (transaction guaranteed to have
    ///   committed or aborted already).
    /// - Merging replicas can safely self-destruct (TODO: is there a better option?)
    TooOld,
    /// The transaction touches keys outside this shard's current key range.
    OutOfRange,
}

/// Returned by [`Store::quorum_read`] and [`Store::quorum_scan`] when the
/// read conflicts with a prepared-but-uncommitted write at `commit_ts <=
/// snapshot_ts`.
///
/// This is a tapirs extension to TAPIR — the original paper uses
/// piggybacking to ensure Finalize ordering, but piggybacking is
/// fundamentally unreliable because IR Finalize is fire-and-forget.
/// Instead, tapirs detects the conflict via the OCC `prepared_writes`
/// list and the ShardClient retries with backoff until the prepare
/// resolves (committed to MVCC or aborted).
///
/// Without this check, a QuorumRead starting immediately after an RW
/// commit could return the stale pre-commit value — a linearizability
/// violation — because the IO::Commit Finalize may not have reached
/// the overlapping replica yet.
#[derive(Debug)]
pub struct PrepareConflict;

impl<TS: Timestamp> PrepareResult<TS> {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Fail)
    }

    pub fn is_abstain(&self) -> bool {
        matches!(self, Self::Abstain)
    }

    pub fn is_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    pub fn is_too_late(&self) -> bool {
        matches!(self, Self::TooLate)
    }

    pub fn is_too_old(&self) -> bool {
        matches!(self, Self::TooOld)
    }

    pub fn is_out_of_range(&self) -> bool {
        matches!(self, Self::OutOfRange)
    }
}

impl<K: Key, V: Value, TS, M> Store<K, V, TS, M> {
    pub fn shard(&self) -> ShardNumber {
        self.shard
    }

    pub fn new(shard: ShardNumber, linearizable: bool) -> Self
    where
        M: Default,
    {
        Self {
            shard,
            linearizable,
            inner: Default::default(),
            prepared: Default::default(),
            prepared_reads: Default::default(),
            prepared_writes: Default::default(),
            range_reads: Vec::new(),
            max_read_commit_time: None,
        }
    }

    pub fn new_with_backend(shard: ShardNumber, linearizable: bool, backend: M) -> Self {
        Self {
            shard,
            linearizable,
            inner: backend,
            prepared: Default::default(),
            prepared_reads: Default::default(),
            prepared_writes: Default::default(),
            range_reads: Vec::new(),
            max_read_commit_time: None,
        }
    }
}

impl<K: Key, V: Value, TS: Timestamp + Send, M: MvccBackend<K, V, TS>> Store<K, V, TS, M> {
    pub fn get(&self, key: &K) -> (Option<V>, TS) {
        MvccBackend::get(&self.inner, key).unwrap()
    }

    pub fn get_at(&self, key: &K, timestamp: TS) -> (Option<V>, TS) {
        MvccBackend::get_at(&self.inner, key, timestamp).unwrap()
    }

    pub fn scan(&self, start: &K, end: &K, timestamp: TS) -> Vec<(K, Option<V>, TS)> {
        MvccBackend::scan(&self.inner, start, end, timestamp).unwrap()
    }

    /// Check if the version at `snapshot_ts` is "validated": has `read_ts >= snapshot_ts`.
    /// Returns `Some((value, write_ts))` if validated, `None` otherwise.
    pub fn get_validated(&self, key: &K, snapshot_ts: TS) -> Option<(Option<V>, TS)> {
        let read_ts = MvccBackend::get_last_read_at(&self.inner, key, snapshot_ts).unwrap()?;
        if read_ts >= snapshot_ts {
            let (value, write_ts) = MvccBackend::get_at(&self.inner, key, snapshot_ts).unwrap();
            Some((value, write_ts))
        } else {
            None
        }
    }

    /// Get value at `snapshot_ts` and update the read timestamp to block future
    /// writes from overwriting the version.
    ///
    /// Returns `Err(PrepareConflict)` when a prepared-but-uncommitted write
    /// exists on `key` at `commit_ts <= snapshot_ts`. Without this check, a
    /// QuorumRead starting right after an RW commit could miss the write if
    /// the IO::Commit Finalize hasn't reached this replica yet — returning
    /// the stale pre-commit value (a linearizability violation). This is the
    /// RW→RO direction, symmetric with `commit_get()` which protects the
    /// RO→RW direction.
    pub fn quorum_read(&mut self, key: K, snapshot_ts: TS) -> Result<(Option<V>, TS), PrepareConflict> {
        if let Some(timestamps) = self.prepared_writes.get(&key)
            && timestamps.range(..=&snapshot_ts).next().is_some()
        {
            return Err(PrepareConflict);
        }
        let (value, write_ts) = MvccBackend::get_at(&self.inner, &key, snapshot_ts).unwrap();
        MvccBackend::commit_get(&mut self.inner, key, snapshot_ts, snapshot_ts).unwrap();
        self.max_read_commit_time = Some(match self.max_read_commit_time {
            Some(prev) => prev.max(snapshot_ts),
            None => snapshot_ts,
        });
        Ok((value, write_ts))
    }

    /// Read-only scan fast path: check if a covering range_read exists.
    /// Returns `Some(results)` if the range is covered (read_ts >= snapshot_ts),
    /// `None` otherwise (fall back to QuorumScan).
    pub fn scan_validated(&self, start: &K, end: &K, snapshot_ts: TS) -> Option<Vec<(K, Option<V>, TS)>> {
        let covered = self.range_reads.iter().any(|(rs, re, rts)| {
            rs <= start && re >= end && *rts >= snapshot_ts
        });
        if !covered {
            return None;
        }
        Some(MvccBackend::scan(&self.inner, start, end, snapshot_ts).unwrap())
    }

    /// Record a range-level read timestamp protecting `[start, end]` from
    /// future writes at commit_ts < snapshot_ts.
    pub fn commit_scan(&mut self, start: K, end: K, snapshot_ts: TS) {
        self.range_reads.push((start, end, snapshot_ts));
    }

    /// Read-only scan slow path: scan the MVCC store and record range protection.
    ///
    /// Returns `Err(PrepareConflict)` when any key in `[start, end]` has a
    /// prepared-but-uncommitted write at `commit_ts <= snapshot_ts`. Same
    /// rationale as [`Self::quorum_read`]: prevents returning stale values
    /// when the IO::Commit Finalize for a recently committed RW transaction
    /// hasn't arrived yet.
    pub fn quorum_scan(&mut self, start: K, end: K, snapshot_ts: TS) -> Result<Vec<(K, Option<V>, TS)>, PrepareConflict> {
        for (_key, timestamps) in self.prepared_writes.range(&start..=&end) {
            if timestamps.range(..=&snapshot_ts).next().is_some() {
                return Err(PrepareConflict);
            }
        }
        let results = MvccBackend::scan(&self.inner, &start, &end, snapshot_ts).unwrap();
        self.commit_scan(start, end, snapshot_ts);
        Ok(results)
    }

    /// Return the max timestamps from the two read-protection mechanisms.
    ///
    /// - `max_range_read_time`: highest `scan_ts.time()` across all `range_reads`.
    /// - `max_read_commit_time`: highest `commit.time()` passed to `commit_get()`.
    ///
    /// Used by resharding to set `raise_min_prepare_time(max(both) + 1)` on
    /// the new shard, subsuming all historical read protections from the source.
    pub(crate) fn min_prepare_baseline(&self) -> (Option<TS>, Option<TS>) {
        let max_rr = self.range_reads.iter().map(|(_, _, ts)| *ts).max();
        (max_rr, self.max_read_commit_time)
    }

    pub fn try_prepare_txn(
        &mut self,
        id: TransactionId,
        transaction: SharedTransaction<K, V, TS>,
        commit: TS,
        dry_run: bool,
    ) -> PrepareResult<TS> {
        if let btree_map::Entry::Occupied(occupied) = self.prepared.entry(id) {
            if occupied.get().0 == commit {
                // Already prepared at this timestamp.
                return PrepareResult::Ok;
            } else if dry_run {
                // Don't remove from prepared set.
                let transaction = occupied.get().1.clone();
                let commit = occupied.get().0;
                self.remove_prepared_inner(&transaction, commit);
            } else {
                // Run the checks again for a new timestamp.
                self.remove_prepared(id);
            }
        }

        let result = self.occ_check(&transaction, commit);

        // Avoid logical mutation in dry run.
        if dry_run
            && let Some((commit, transaction, _)) = self.prepared.get(&id) {
                let transaction = transaction.clone();
                let commit = *commit;
                self.add_prepared_inner(&transaction, commit);
            }

        if result.is_ok() {
            if dry_run {
                return PrepareResult::Retry {
                    proposed: commit.time(),
                };
            } else {
                self.add_prepared(id, transaction, commit, false);
            }
        }

        result
    }

    fn occ_check(&self, transaction: &Transaction<K, V, TS>, commit: TS) -> PrepareResult<TS> {
        // Check for conflicts with the read set.
        for (key, read) in transaction.shard_read_set(self.shard) {
            if read > commit {
                debug_assert!(false, "client picked too low commit timestamp for read");
                return PrepareResult::Retry {
                    proposed: read.time(),
                };
            }

            // If we don't have this key then no conflicts for read.
            let (beginning, end) = MvccBackend::get_range(&self.inner, key, read).unwrap();

            if beginning == read {
                if let Some(end) = end && (self.linearizable || commit > end) {
                    // Read value is now invalid (not the latest version), so
                    // the prepare isn't linearizable and may not be serializable.
                    //
                    // In other words, the read conflicts with a later committed write.
                    return PrepareResult::Fail;
                }
            } else {
                // If we don't have this version then no conflicts for read.
            }

            // There may be a pending write that would invalidate the read version.
            if let Some(writes) = self.prepared_writes.get(key)
                && writes
                    .range((
                        if self.linearizable {
                            Bound::Unbounded
                        } else {
                            Bound::Excluded(read)
                        },
                        Bound::Excluded(commit),
                    ))
                    .next()
                    .is_some()
                {
                    // Read conflicts with later prepared write.
                    return PrepareResult::Abstain;
                }
        }

        // Check for conflicts with the write set.
        for (key, _) in transaction.shard_write_set(self.shard) {
            {
                let (_, timestamp) = MvccBackend::get(&self.inner, key).unwrap();
                // If the last commited write is after the write...
                if self.linearizable && timestamp > commit {
                    // ...then the write isn't linearizable.
                    return PrepareResult::Retry {
                        proposed: timestamp.time(),
                    };
                }

                // if last committed read is after the write...
                let last_read = if self.linearizable {
                    // Cannot write an old version.
                    MvccBackend::get_last_read(&self.inner, key).unwrap()
                } else {
                    // Might be able to write an old version.
                    MvccBackend::get_last_read_at(&self.inner, key, commit).unwrap()
                };

                if let Some(last_read) = last_read && last_read > commit {
                    // Write conflicts with a later committed read.
                    return PrepareResult::Retry{proposed: last_read.time()};
                }
            }

            if self.linearizable && let Some(writes) = self.prepared_writes.get(key)
                && let Some((write, _)) = writes.range((Bound::Excluded(&commit), Bound::Unbounded)).next() {
                    // Write conflicts with later prepared write.
                    return PrepareResult::Retry { proposed: write.time() };
                }

            if let Some(reads) = self.prepared_reads.get(key)
                && reads.range((Bound::Excluded(&commit), Bound::Unbounded)).next().is_some() {
                    // Write conflicts with later prepared read.
                    return PrepareResult::Abstain;
                }

            // Check for conflicts with range-level reads from read-only QuorumScan.
            for (start, end, read_ts) in &self.range_reads {
                if key >= start && key <= end && *read_ts > commit {
                    return PrepareResult::Retry { proposed: read_ts.time() };
                }
            }
        }

        // Check for conflicts with the scan set (phantom prevention).
        for entry in transaction.shard_scan_set(self.shard) {
            // Check if any committed writes appeared in the scanned range after the scan.
            if MvccBackend::has_writes_in_range(
                &self.inner,
                &entry.start_key,
                &entry.end_key,
                entry.timestamp,
                commit,
            ).unwrap() {
                return PrepareResult::Fail;
            }
            // Check if any prepared writes exist in the scanned range.
            for (_key, timestamps) in self
                .prepared_writes
                .range(&entry.start_key..=&entry.end_key)
            {
                if timestamps
                    .range((Bound::Excluded(entry.timestamp), Bound::Excluded(commit)))
                    .next()
                    .is_some()
                {
                    return PrepareResult::Abstain;
                }
            }
        }

        PrepareResult::Ok
    }

    pub fn commit(&mut self, id: TransactionId, transaction: &Transaction<K, V, TS>, commit: TS) {
        let reads: Vec<(K, TS)> = transaction
            .shard_read_set(self.shard)
            .map(|(key, read)| (key.clone(), read))
            .collect();

        if !reads.is_empty() {
            self.max_read_commit_time = Some(match self.max_read_commit_time {
                Some(prev) => prev.max(commit),
                None => commit,
            });
        }

        let writes: Vec<(K, Option<V>)> = transaction
            .shard_write_set(self.shard)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        MvccBackend::commit_batch_for_transaction(&mut self.inner, id, writes, reads, commit).unwrap();

        // Note: Transaction may not be in the prepared list of this particular replica, and that's okay.
        self.remove_prepared(id);
    }

    pub fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        MvccBackend::put(&mut self.inner, key, value, timestamp).unwrap();
    }

    pub fn add_prepared(
        &mut self,
        id: TransactionId,
        transaction: SharedTransaction<K, V, TS>,
        commit: TS,
        finalized: bool,
    ) {
        trace!("preparing {id:?} at {commit:?} (fin = {finalized})");
        match self.prepared.entry(id) {
            btree_map::Entry::Vacant(vacant) => {
                vacant.insert((commit, transaction.clone(), finalized));
                self.add_prepared_inner(&transaction, commit);
            }
            btree_map::Entry::Occupied(mut occupied) => {
                if occupied.get().0 == commit {
                    debug_assert_eq!(*occupied.get().1, *transaction);
                    occupied.get_mut().2 = finalized;
                } else {
                    let old_commit = occupied.get().0;
                    occupied.insert((commit, transaction.clone(), finalized));
                    self.remove_prepared_inner(&transaction, old_commit);
                    self.add_prepared_inner(&transaction, commit);
                }
            }
        }
    }

    fn add_prepared_inner(&mut self, transaction: &Transaction<K, V, TS>, commit: TS) {
        for (key, _) in transaction.shard_read_set(self.shard) {
            self.prepared_reads
                .entry(key.clone())
                .or_default()
                .insert(commit, ());
        }
        for (key, _) in transaction.shard_write_set(self.shard) {
            self.prepared_writes
                .entry(key.clone())
                .or_default()
                .insert(commit, ());
        }
    }

    pub fn remove_prepared(&mut self, id: TransactionId) -> bool {
        if let Some((commit, transaction, finalized)) = self.prepared.remove(&id) {
            trace!("removing prepared {id:?} at {commit:?} (fin = {finalized})");
            self.remove_prepared_inner(&transaction, commit);
            true
        } else {
            false
        }
    }

    fn remove_prepared_inner(&mut self, transaction: &Transaction<K, V, TS>, commit: TS) {
        for (key, _) in transaction.shard_read_set(self.shard) {
            if let btree_map::Entry::Occupied(mut occupied) = self.prepared_reads.entry(key.clone()) {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
        for (key, _) in transaction.shard_write_set(self.shard) {
            if let btree_map::Entry::Occupied(mut occupied) = self.prepared_writes.entry(key.clone()) {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::disk::{DiskStore, disk_io::BufferedIo};
    use crate::occ::ScanEntry;
    use crate::tapir::{ShardNumber, Sharded, Timestamp as TapirTimestamp};
    use crate::IrClientId;
    use std::sync::Arc;
    use tempfile::TempDir;

    type TS = TapirTimestamp;
    type TestStore = Store<String, String, TS, DiskStore<String, String, TS, BufferedIo>>;

    fn ts(time: u64, client_id: u64) -> TS {
        TapirTimestamp {
            time,
            client_id: IrClientId(client_id),
        }
    }

    fn txn_id(client: u64, num: u64) -> TransactionId {
        TransactionId {
            client_id: IrClientId(client),
            number: num,
        }
    }

    fn sharded(key: &str) -> Sharded<String> {
        Sharded {
            shard: ShardNumber(0),
            key: key.to_string(),
        }
    }

    fn new_store(linearizable: bool) -> (TempDir, TestStore) {
        let dir = TempDir::new().unwrap();
        let backend = DiskStore::open(dir.path().to_path_buf()).unwrap();
        (dir, Store::new_with_backend(ShardNumber(0), linearizable, backend))
    }

    fn make_txn(
        reads: Vec<(&str, TS)>,
        writes: Vec<(&str, Option<&str>)>,
        scans: Vec<ScanEntry<String, TS>>,
    ) -> SharedTransaction<String, String, TS> {
        let mut txn = Transaction::<String, String, TS>::default();
        for (key, timestamp) in reads {
            txn.add_read(sharded(key), timestamp);
        }
        for (key, value) in writes {
            txn.add_write(sharded(key), value.map(|v| v.to_string()));
        }
        txn.scan_set = scans;
        Arc::new(txn)
    }

    // ── prepare() conflict detection ──

    #[test]
    fn prepare_ok_no_conflicts() {
        let (_dir, mut store) = new_store(true);
        // T1 writes "x", T2 writes "y" — no overlap.
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let t2 = make_txn(vec![], vec![("y", Some("v2"))], vec![]);

        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(10, 1), false), PrepareResult::Ok);
        assert_eq!(store.try_prepare_txn(txn_id(2, 1), t2, ts(11, 2), false), PrepareResult::Ok);
    }

    #[test]
    fn prepare_ok_already_prepared_same_ts() {
        let (_dir, mut store) = new_store(true);
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);

        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1.clone(), ts(10, 1), false), PrepareResult::Ok);
        // Re-prepare at exact same timestamp → Ok (idempotent).
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(10, 1), false), PrepareResult::Ok);
    }

    #[test]
    fn prepare_read_write_conflict_committed() {
        let (_dir, mut store) = new_store(false);

        // Commit a write of "x" at ts(5,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id1 = txn_id(1, 1);
        store.commit(id1, &t1, ts(5, 1));

        // Now commit a newer write of "x" at ts(10,1), creating a version range end.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        let id2 = txn_id(2, 1);
        store.commit(id2, &t2, ts(10, 1));

        // T3 read "x" at ts(5,1) (the old version). The version range for ts(5,1) ends
        // at ts(10,1). Since commit ts(15,1) > end ts(10,1), this is a Fail.
        let t3 = make_txn(vec![("x", ts(5, 1))], vec![], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(3, 1), t3, ts(15, 1), false),
            PrepareResult::Fail
        );
    }

    #[test]
    fn prepare_read_write_conflict_prepared() {
        let (_dir, mut store) = new_store(false);

        // Commit initial write of "x" at ts(5,1) so reads can reference it.
        let t_init = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_init, ts(5, 1));

        // T1 prepares a write to "x" at ts(8,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(8, 1), false), PrepareResult::Ok);

        // T2 read "x" at ts(5,1), prepares at ts(12,2).
        // There's a prepared write at ts(8,1) in range (read=5, commit=12) → Abstain.
        let t2 = make_txn(vec![("x", ts(5, 1))], vec![], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Abstain
        );
    }

    #[test]
    fn prepare_write_write_conflict_linearizable() {
        let (_dir, mut store) = new_store(true);

        // Commit a write of "x" at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(10, 1));

        // T2 tries to write "x" at ts(5,2). In linearizable mode,
        // last committed write ts(10) > commit ts(5) → Retry.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    #[test]
    fn prepare_write_read_conflict() {
        let (_dir, mut store) = new_store(true);

        // Commit initial value so reads and commit_get track properly.
        let t_init = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_init, ts(3, 1));

        // T1 reads "x" at ts(3,1) and commits at ts(10,1).
        // This calls commit_get("x", read=ts(3,1), commit=ts(10,1)) which records
        // a last-read timestamp of ts(10,1) on the ts(3,1) version.
        let t1 = make_txn(vec![("x", ts(3, 1))], vec![], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(10, 1));

        // T2 writes "x" at ts(5,2). get_last_read("x") returns ts(10,1).
        // last_read ts(10,1) > commit ts(5,2) → Retry.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    #[test]
    fn prepare_write_prepared_read_conflict() {
        let (_dir, mut store) = new_store(false);

        // Commit initial value.
        let t_init = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_init, ts(3, 1));

        // T1 prepares a read of "x" at ts(3,1) with commit ts(10,1).
        let t1 = make_txn(vec![("x", ts(3, 1))], vec![], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(10, 1), false), PrepareResult::Ok);

        // T2 writes "x" at ts(5,2). prepared_reads has ts(10,1) for "x".
        // prepared read ts(10,1) > commit ts(5,2) → Abstain.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Abstain
        );
    }

    #[test]
    fn prepare_write_prepared_write_conflict_linearizable() {
        let (_dir, mut store) = new_store(true);

        // T1 prepares a write of "x" at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(10, 1), false), PrepareResult::Ok);

        // T2 writes "x" at ts(5,2). In linearizable mode, prepared_writes has ts(10,1)
        // for "x" which is > commit ts(5,2) → Retry.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    // ── Scan-set phantom detection ──

    #[test]
    fn prepare_scan_committed_phantom() {
        let (_dir, mut store) = new_store(false);

        // Commit a write of "b" at ts(8,1) — this is the phantom.
        let t1 = make_txn(vec![], vec![("b", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(8, 1));

        // T2 scanned range ["a","c"] at ts(5,1) and saw nothing. Now prepares at ts(12,2).
        // The committed write "b"@ts(8,1) is in range (after_ts=5, before_ts=12) → Fail.
        let t2 = make_txn(
            vec![],
            vec![],
            vec![ScanEntry {
                shard: ShardNumber(0),
                start_key: "a".to_string(),
                end_key: "c".to_string(),
                timestamp: ts(5, 1),
            }],
        );
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Fail
        );
    }

    #[test]
    fn prepare_scan_prepared_phantom() {
        let (_dir, mut store) = new_store(false);

        // T1 prepares a write of "b" at ts(8,1).
        let t1 = make_txn(vec![], vec![("b", Some("v1"))], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(8, 1), false), PrepareResult::Ok);

        // T2 scanned range ["a","c"] at ts(5,1), prepares at ts(12,2).
        // Prepared write "b"@ts(8,1) in range (5,12) → Abstain.
        let t2 = make_txn(
            vec![],
            vec![],
            vec![ScanEntry {
                shard: ShardNumber(0),
                start_key: "a".to_string(),
                end_key: "c".to_string(),
                timestamp: ts(5, 1),
            }],
        );
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Abstain
        );
    }

    #[test]
    fn prepare_scan_no_phantom() {
        let (_dir, mut store) = new_store(false);

        // Commit a write of "z" (outside scan range) at ts(8,1).
        let t1 = make_txn(vec![], vec![("z", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(8, 1));

        // T2 scanned range ["a","c"] at ts(5,1), prepares at ts(12,2).
        // "z" is outside the scan range → Ok.
        let t2 = make_txn(
            vec![],
            vec![],
            vec![ScanEntry {
                shard: ShardNumber(0),
                start_key: "a".to_string(),
                end_key: "c".to_string(),
                timestamp: ts(5, 1),
            }],
        );
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Ok
        );
    }

    // ── commit() and abort() lifecycle ──

    #[test]
    fn commit_applies_writes_and_reads() {
        let (_dir, mut store) = new_store(true);

        let txn = make_txn(vec![], vec![("x", Some("hello"))], vec![]);
        let id = txn_id(1, 1);
        store.commit(id, &txn, ts(5, 1));

        let (val, version) = store.get(&"x".to_string());
        assert_eq!(val, Some("hello".to_string()));
        assert_eq!(version, ts(5, 1));
    }

    #[test]
    fn abort_removes_prepared() {
        let (_dir, mut store) = new_store(true);

        let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id = txn_id(1, 1);
        assert_eq!(store.try_prepare_txn(id, txn, ts(10, 1), false), PrepareResult::Ok);
        assert!(store.prepared.contains_key(&id));

        // Abort by removing prepared.
        assert!(store.remove_prepared(id));
        assert!(!store.prepared.contains_key(&id));
        // Prepared caches should also be cleaned up.
        assert!(store.prepared_writes.is_empty());
    }

    #[test]
    fn commit_unprepared_transaction() {
        let (_dir, mut store) = new_store(true);

        // Commit without prior prepare — should work (per code comment).
        let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id = txn_id(1, 1);
        store.commit(id, &txn, ts(5, 1));

        let (val, _) = store.get(&"x".to_string());
        assert_eq!(val, Some("v1".to_string()));
    }

    // ── Linearizable vs eventual mode ──

    #[test]
    fn eventual_allows_old_write() {
        let (_dir, mut store) = new_store(false);

        // Commit a write of "x" at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(10, 1));

        // T2 writes "x" at ts(5,2). In eventual mode, older writes are allowed
        // as long as no committed read conflicts.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Ok
        );
    }

    #[test]
    fn linearizable_rejects_old_write() {
        let (_dir, mut store) = new_store(true);

        // Commit a write of "x" at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(10, 1));

        // T2 writes "x" at ts(5,2). In linearizable mode → Retry.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    // ── Dry-run prepare ──

    #[test]
    fn prepare_dry_run_no_side_effects() {
        let (_dir, mut store) = new_store(true);

        let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id = txn_id(1, 1);

        // Dry run returns Retry (even if occ_check passes) and does not add to prepared.
        let result = store.try_prepare_txn(id, txn, ts(10, 1), true);
        assert_eq!(result, PrepareResult::Retry { proposed: 10 });
        assert!(!store.prepared.contains_key(&id));
        assert!(store.prepared_writes.is_empty());
    }

    // ── A.3.1 Linearizable read-write conflict ──

    #[test]
    fn prepare_read_write_conflict_committed_linearizable() {
        let (_dir, mut store) = new_store(true);

        // Commit "x" at ts(5,1), then overwrite at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(5, 1));
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        store.commit(txn_id(2, 1), &t2, ts(10, 1));

        // T3 read "x" at ts(5,1), prepares at ts(8,1).
        // Serializable: commit(8) < end(10) → Ok. Linearizable: any end → Fail.
        let t3 = make_txn(vec![("x", ts(5, 1))], vec![], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(3, 1), t3, ts(8, 1), false),
            PrepareResult::Fail
        );
    }

    #[test]
    fn prepare_read_write_conflict_prepared_linearizable() {
        let (_dir, mut store) = new_store(true);

        // Commit initial version at ts(2,1).
        let t_init = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_init, ts(2, 1));

        // T1 prepares write to "x" at ts(3,1). Passes linearizable write-set check
        // because latest committed ts(2,1) < ts(3,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(3, 1), false), PrepareResult::Ok);

        // T3 commits a newer version at ts(5,1). Now T1's prepared write at ts(3,1)
        // is BEFORE this version.
        let t3 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        store.commit(txn_id(3, 1), &t3, ts(5, 1));

        // T2 reads "x" at ts(5,1) (the newer version), prepares at ts(12,2).
        // T1's prepared write at ts(3,1) < read ts(5,1).
        // Serializable: Excluded(read=5,1) excludes ts(3,1) → no conflict → Ok.
        // Linearizable: Unbounded includes ts(3,1) → conflict → Abstain.
        let t2 = make_txn(vec![("x", ts(5, 1))], vec![], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Abstain
        );
    }

    // ── A.3.2 Serializable: write between versions allowed if earlier version unread ──

    #[test]
    fn serializable_write_between_versions_no_read_conflict() {
        let (_dir, mut store) = new_store(false);

        // Commit "x" at ts(3,1) — version A (never read).
        let t_a = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_a, ts(3, 1));

        // Commit "x" at ts(10,1) — version B.
        let t_b = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t_b, ts(10, 1));

        // T1 reads version B at ts(10,1), commits at ts(20,1).
        // This records last_read on version B = ts(20,1).
        let t1 = make_txn(vec![("x", ts(10, 1))], vec![], vec![]);
        store.commit(txn_id(2, 1), &t1, ts(20, 1));

        // T2 writes "x" at ts(7,2) — between versions A@3 and B@10.
        // Serializable: get_last_read_at("x", ts(7,2)) finds version A which has
        // no last_read → None → no conflict → Ok.
        // (In linearizable mode, the write-write check would reject this since
        // latest committed ts(10,1) > ts(7,2) → Retry.)
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(3, 1), t2, ts(7, 2), false),
            PrepareResult::Ok
        );
    }

    #[test]
    fn serializable_write_between_versions_read_conflict() {
        let (_dir, mut store) = new_store(false);

        // Commit "x" at ts(3,1) — version A.
        let t_a = make_txn(vec![], vec![("x", Some("v0"))], vec![]);
        store.commit(txn_id(0, 1), &t_a, ts(3, 1));

        // T0 reads version A at ts(3,1), commits at ts(20,1).
        // This records last_read on version A = ts(20,1).
        let t0 = make_txn(vec![("x", ts(3, 1))], vec![], vec![]);
        store.commit(txn_id(1, 1), &t0, ts(20, 1));

        // Commit "x" at ts(10,1) — version B.
        let t_b = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(2, 1), &t_b, ts(10, 1));

        // T2 writes "x" at ts(7,2) — between versions A@3 and B@10.
        // Serializable: get_last_read_at("x", ts(7,2)) finds version A which has
        // last_read = ts(20,1). Since ts(20,1) > ts(7,2) → Retry.
        // (Paired with serializable_write_between_versions_no_read_conflict to show
        // get_last_read_at is both permissive and restrictive — the core SSI property.)
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(3, 1), t2, ts(7, 2), false),
            PrepareResult::Retry { proposed: 20 }
        );
    }

    #[test]
    fn serializable_no_prepared_write_write_conflict() {
        let (_dir, mut store) = new_store(false);

        // T1 prepares a write of "x" at ts(10,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        assert_eq!(store.try_prepare_txn(txn_id(1, 1), t1, ts(10, 1), false), PrepareResult::Ok);

        // T2 writes "x" at ts(5,2). In serializable mode, prepared write-write
        // conflicts are NOT checked → Ok.
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(5, 2), false),
            PrepareResult::Ok
        );
    }

    // ── A.3.3 Multiple scan entries ──

    #[test]
    fn prepare_scan_multiple_entries_one_conflict() {
        let (_dir, mut store) = new_store(false);

        // Commit a write of "m" at ts(8,1) — inside second scan range only.
        let t1 = make_txn(vec![], vec![("m", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(8, 1));

        // T2 has two scan entries: ["a","c"]@5 and ["k","p"]@5.
        // Only the second range contains the phantom "m"@8.
        let t2 = make_txn(
            vec![],
            vec![],
            vec![
                ScanEntry {
                    shard: ShardNumber(0),
                    start_key: "a".to_string(),
                    end_key: "c".to_string(),
                    timestamp: ts(5, 1),
                },
                ScanEntry {
                    shard: ShardNumber(0),
                    start_key: "k".to_string(),
                    end_key: "p".to_string(),
                    timestamp: ts(5, 1),
                },
            ],
        );
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(12, 2), false),
            PrepareResult::Fail
        );
    }

    // ── A.3.4 Prepared cache: timestamp change ──

    #[test]
    fn add_prepared_replaces_at_new_timestamp() {
        let (_dir, mut store) = new_store(true);

        let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id = txn_id(1, 1);

        // First prepare at ts(10,1).
        store.add_prepared(id, txn.clone(), ts(10, 1), false);
        assert!(store.prepared_writes.get("x").unwrap().contains_key(&ts(10, 1)));

        // Re-prepare same txn at ts(20,1).
        store.add_prepared(id, txn, ts(20, 1), false);

        // Old cache entry removed, new one present.
        let writes = store.prepared_writes.get("x").unwrap();
        assert!(!writes.contains_key(&ts(10, 1)));
        assert!(writes.contains_key(&ts(20, 1)));
        assert_eq!(store.prepared.get(&id).unwrap().0, ts(20, 1));
    }

    #[test]
    fn dry_run_preserves_existing_prepare() {
        let (_dir, mut store) = new_store(true);

        let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        let id = txn_id(1, 1);

        // Actually prepare at ts(10,1).
        assert_eq!(store.try_prepare_txn(id, txn.clone(), ts(10, 1), false), PrepareResult::Ok);
        assert!(store.prepared_writes.get("x").unwrap().contains_key(&ts(10, 1)));

        // Dry-run re-prepare at ts(20,1). Should not disturb existing prepare.
        let result = store.try_prepare_txn(id, txn, ts(20, 1), true);
        assert_eq!(result, PrepareResult::Retry { proposed: 20 });

        // Original prepare still intact.
        assert!(store.prepared_writes.get("x").unwrap().contains_key(&ts(10, 1)));
        assert_eq!(store.prepared.get(&id).unwrap().0, ts(10, 1));
    }

    // ── get_validated / quorum_read (read-only transactions) ──

    #[test]
    fn get_validated_returns_none_when_no_read_ts() {
        let (_dir, store) = new_store(false);
        // No data and no reads → get_validated returns None.
        assert_eq!(store.get_validated(&"x".to_string(), ts(5, 1)), None);
    }

    #[test]
    fn get_validated_returns_none_when_unread() {
        let (_dir, mut store) = new_store(false);
        // Write a value but never read it.
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // Version exists at ts(3,1) but has no read_ts → None.
        assert_eq!(store.get_validated(&"x".to_string(), ts(5, 1)), None);
    }

    #[test]
    fn get_validated_returns_some_when_read_ts_sufficient() {
        let (_dir, mut store) = new_store(false);

        // Write "x" at ts(3,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // Read "x" with a read-write txn that commits at ts(10,1).
        // This sets read_ts on the version at ts(3,1) to ts(10,1).
        let t2 = make_txn(vec![("x", ts(3, 1))], vec![], vec![]);
        store.commit(txn_id(2, 1), &t2, ts(10, 1));

        // get_validated with snapshot_ts(5,1): read_ts=ts(10,1) >= ts(5,1) → Some.
        let result = store.get_validated(&"x".to_string(), ts(5, 1));
        assert!(result.is_some());
        let (value, write_ts) = result.unwrap();
        assert_eq!(value, Some("v1".to_string()));
        assert_eq!(write_ts, ts(3, 1));

        // get_validated with snapshot_ts(15,1): read_ts=ts(10,1) < ts(15,1) → None.
        assert_eq!(store.get_validated(&"x".to_string(), ts(15, 1)), None);
    }

    // ── scan_validated / commit_scan / quorum_scan (read-only range scans) ──

    #[test]
    fn scan_validated_no_range_coverage() {
        let (_dir, mut store) = new_store(false);
        // Write a key.
        let t1 = make_txn(vec![], vec![("b", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // scan_validated without prior commit_scan → returns None.
        assert_eq!(
            store.scan_validated(&"a".to_string(), &"c".to_string(), ts(5, 1)),
            None
        );
    }

    #[test]
    fn scan_validated_with_coverage() {
        let (_dir, mut store) = new_store(false);
        // Write a key.
        let t1 = make_txn(vec![], vec![("b", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // commit_scan records range_read at snapshot_ts(10,1).
        store.commit_scan("a".to_string(), "c".to_string(), ts(10, 1));

        // scan_validated with snapshot_ts(5,1) — covered (10 >= 5).
        let result = store.scan_validated(&"a".to_string(), &"c".to_string(), ts(5, 1));
        assert!(result.is_some());
        let entries = result.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "b".to_string());
        assert_eq!(entries[0].1, Some("v1".to_string()));

        // scan_validated with snapshot_ts(15,1) — NOT covered (10 < 15).
        assert_eq!(
            store.scan_validated(&"a".to_string(), &"c".to_string(), ts(15, 1)),
            None
        );
    }

    #[test]
    fn commit_scan_blocks_overwrite() {
        let (_dir, mut store) = new_store(false);
        // Write a key.
        let t1 = make_txn(vec![], vec![("b", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // commit_scan at snapshot_ts(10,1) — protects range [a,c].
        store.commit_scan("a".to_string(), "c".to_string(), ts(10, 1));

        // Prepare a write to existing key "b" at commit_ts(7,2) — read_ts(10) > commit(7) → Retry.
        let t2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(7, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    #[test]
    fn quorum_scan_range_read_blocks_phantom() {
        let (_dir, mut store) = new_store(false);
        // quorum_scan [a,z] at snapshot_ts(10,1).
        let _results = store.quorum_scan("a".to_string(), "z".to_string(), ts(10, 1)).unwrap();

        // Prepare a write of NEW key "m" at commit_ts(7,2) — range_read(10) > commit(7) → Retry.
        let t2 = make_txn(vec![], vec![("m", Some("v1"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(7, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }

    #[test]
    fn quorum_scan_range_read_allows_later_write() {
        let (_dir, mut store) = new_store(false);
        // quorum_scan [a,z] at snapshot_ts(10,1).
        let _results = store.quorum_scan("a".to_string(), "z".to_string(), ts(10, 1)).unwrap();

        // Prepare a write of "m" at commit_ts(15,2) — range_read(10) < commit(15) → Ok.
        let t2 = make_txn(vec![], vec![("m", Some("v1"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(15, 2), false),
            PrepareResult::Ok
        );
    }

    #[test]
    fn quorum_scan_range_read_outside_range() {
        let (_dir, mut store) = new_store(false);
        // quorum_scan [a,c] at snapshot_ts(10,1).
        let _results = store.quorum_scan("a".to_string(), "c".to_string(), ts(10, 1)).unwrap();

        // Prepare a write of "z" at commit_ts(7,2) — "z" outside range [a,c] → Ok.
        let t2 = make_txn(vec![], vec![("z", Some("v1"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(2, 1), t2, ts(7, 2), false),
            PrepareResult::Ok
        );
    }

    #[test]
    fn quorum_read_updates_read_ts_and_blocks_writes() {
        let (_dir, mut store) = new_store(false);

        // Write "x" at ts(3,1).
        let t1 = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
        store.commit(txn_id(1, 1), &t1, ts(3, 1));

        // quorum_read at snapshot_ts(10,1) should return the value and set read_ts.
        let (value, write_ts) = store.quorum_read("x".to_string(), ts(10, 1)).unwrap();
        assert_eq!(value, Some("v1".to_string()));
        assert_eq!(write_ts, ts(3, 1));

        // Now get_validated should succeed at ts(10,1) since we just set read_ts.
        let result = store.get_validated(&"x".to_string(), ts(10, 1));
        assert!(result.is_some());

        // A subsequent write at ts(7,2) should be rejected because
        // read_ts(ts(10,1)) > commit_ts(ts(7,2)).
        let t2 = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
        assert_eq!(
            store.try_prepare_txn(txn_id(3, 1), t2, ts(7, 2), false),
            PrepareResult::Retry { proposed: 10 }
        );
    }
}
