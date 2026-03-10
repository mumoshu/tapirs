use super::{SharedTransaction, Timestamp, Transaction, TransactionId};
use crate::{
    tapir::{Key, ShardNumber, Value, store::CheckPrepareStatus},
    util::vectorize_btree,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map, BTreeMap},
    ops::{Deref, DerefMut},
};
use tracing::trace;

#[derive(Serialize, Deserialize)]
pub struct TimestampSet<TS> {
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

/// MVCC-independent prepared transaction state management.
///
/// Tracks the registry of prepared transactions, read/write caches for
/// OCC conflict detection, and range-level read protections.
///
/// This struct is designed to be reusable by any TapirStore implementation
/// regardless of the underlying MVCC backend.
#[derive(Serialize, Deserialize)]
pub struct PreparedTransactions<K, V, TS> {
    shard: ShardNumber,
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
    prepared: BTreeMap<TransactionId, (TS, SharedTransaction<K, V, TS>, bool)>,
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
}

impl<K: Key, V: Value, TS: Timestamp + Send> PreparedTransactions<K, V, TS> {
    pub fn new(shard: ShardNumber) -> Self {
        Self {
            shard,
            prepared: Default::default(),
            prepared_reads: Default::default(),
            prepared_writes: Default::default(),
            range_reads: Vec::new(),
        }
    }

    pub fn shard(&self) -> ShardNumber {
        self.shard
    }

    // === Prepared Transaction Queries ===

    /// Look up a prepared transaction by ID.
    pub fn get(
        &self,
        id: &TransactionId,
    ) -> Option<(&TS, &SharedTransaction<K, V, TS>, bool)> {
        self.prepared
            .get(id)
            .map(|(ts, txn, fin)| (ts, txn, *fin))
    }

    /// Check if a transaction is in the prepared set.
    pub fn contains(&self, id: &TransactionId) -> bool {
        self.prepared.contains_key(id)
    }

    /// Number of prepared transactions.
    pub fn len(&self) -> usize {
        self.prepared.len()
    }

    /// Check if prepared at a specific commit timestamp. Returns the
    /// finalized flag if found at that exact timestamp, `None` otherwise.
    pub fn prepared_at_timestamp(
        &self,
        id: &TransactionId,
        commit: &TS,
    ) -> Option<bool> {
        self.prepared
            .get(id)
            .filter(|(ts, _, _)| ts == commit)
            .map(|(_, _, fin)| *fin)
    }

    /// Return the minimum commit timestamp (`.time()`) among all prepared
    /// transactions, or `None` if none are prepared.
    pub fn min_prepared_timestamp(&self) -> Option<TS::Time> {
        self.prepared
            .values()
            .map(|(ts, _, _)| ts.time())
            .min()
    }

    /// Return the oldest prepared transaction (minimum commit timestamp).
    pub fn oldest(
        &self,
    ) -> Option<(TransactionId, TS, SharedTransaction<K, V, TS>)> {
        self.prepared
            .iter()
            .min_by_key(|(_, (c, _, _))| *c)
            .map(|(id, (ts, txn, _))| (*id, *ts, txn.clone()))
    }

    /// Check if the transaction is already prepared at exactly `commit`.
    /// Used by `try_prepare_txn` for idempotency.
    pub fn is_prepared_at(&self, id: &TransactionId, commit: &TS) -> bool {
        self.prepared
            .get(id)
            .is_some_and(|(ts, _, _)| ts == commit)
    }

    // === Prepared Transaction Mutations ===

    /// Upsert a prepared transaction with three distinct behaviors:
    ///
    /// - **Insert** (vacant): adds to prepared, populates
    ///   `prepared_reads`/`prepared_writes` caches.
    ///
    /// - **Finalize** (same commit timestamp): only updates the `finalized`
    ///   flag. Caches are untouched.
    ///
    /// - **Replace** (different commit timestamp): removes old cache entries
    ///   and adds new ones under the new timestamp.
    pub fn add_or_replace_or_finalize(
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
                self.add_caches(&transaction, commit);
            }
            btree_map::Entry::Occupied(mut occupied) => {
                if occupied.get().0 == commit {
                    debug_assert_eq!(*occupied.get().1, *transaction);
                    occupied.get_mut().2 = finalized;
                } else {
                    let old_commit = occupied.get().0;
                    occupied.insert((commit, transaction.clone(), finalized));
                    self.remove_caches(&transaction, old_commit);
                    self.add_caches(&transaction, commit);
                }
            }
        }
    }

    /// Remove a prepared transaction and clean up its cache entries.
    pub fn remove(&mut self, id: TransactionId) -> bool {
        if let Some((commit, transaction, finalized)) = self.prepared.remove(&id) {
            trace!("removing prepared {id:?} at {commit:?} (fin = {finalized})");
            self.remove_caches(&transaction, commit);
            true
        } else {
            false
        }
    }

    /// Finalize a tentatively prepared transaction after IR quorum
    /// confirmation. Returns true if the entry existed at the given
    /// commit timestamp.
    pub fn finalize(&mut self, id: &TransactionId, commit: &TS) -> bool {
        if let Some((ts, _, finalized)) = self.prepared.get_mut(id)
            && ts == commit
        {
            *finalized = true;
            return true;
        }
        false
    }

    /// Remove all prepared transactions that are NOT finalized.
    pub fn remove_all_unfinalized(&mut self) {
        let ids: Vec<_> = self
            .prepared
            .iter()
            .filter(|(_, (_, _, f))| !*f)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.remove(id);
        }
    }

    // === Cache Management ===

    fn add_caches(&mut self, transaction: &Transaction<K, V, TS>, commit: TS) {
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

    fn remove_caches(&mut self, transaction: &Transaction<K, V, TS>, commit: TS) {
        for (key, _) in transaction.shard_read_set(self.shard) {
            if let btree_map::Entry::Occupied(mut occupied) =
                self.prepared_reads.entry(key.clone())
            {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
        for (key, _) in transaction.shard_write_set(self.shard) {
            if let btree_map::Entry::Occupied(mut occupied) =
                self.prepared_writes.entry(key.clone())
            {
                occupied.get_mut().remove(&commit);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
    }

    // === Read-Only Accessors for OCC Conflict Detection ===

    pub fn prepared_writes(&self) -> &BTreeMap<K, TimestampSet<TS>> {
        &self.prepared_writes
    }

    pub fn prepared_reads(&self) -> &BTreeMap<K, TimestampSet<TS>> {
        &self.prepared_reads
    }

    pub fn range_reads(&self) -> &[(K, K, TS)] {
        &self.range_reads
    }

    // === Range Read Protection ===

    /// Record a range-level read timestamp protecting `[start, end]` from
    /// future writes at commit_ts < snapshot_ts.
    pub fn commit_scan(&mut self, start: K, end: K, snapshot_ts: TS) {
        self.range_reads.push((start, end, snapshot_ts));
    }

    /// Cascading lookup to determine the prepare status of a transaction.
    ///
    /// Takes external state as parameters to keep `PreparedTransactions`
    /// decoupled from `TransactionLog` and `MinPrepareTimes`:
    /// - `txn_log_entry`: result of `TransactionLog::txn_log_get(id)`
    /// - `min_prepare_time`: current min_prepare_time from `MinPrepareTimes`
    pub fn check_prepare_status(
        &self,
        txn_log_entry: Option<(TS, bool)>,
        min_prepare_time: u64,
        id: &TransactionId,
        commit: &TS,
    ) -> CheckPrepareStatus
    where
        TS: Timestamp<Time = u64>,
    {
        if let Some((ts, committed)) = txn_log_entry {
            if committed {
                if ts == *commit {
                    CheckPrepareStatus::CommittedAtTimestamp
                } else {
                    CheckPrepareStatus::CommittedDifferent {
                        proposed: ts.time(),
                    }
                }
            } else {
                CheckPrepareStatus::Aborted
            }
        } else if let Some(finalized) = self.prepared_at_timestamp(id, commit) {
            CheckPrepareStatus::PreparedAtTimestamp { finalized }
        } else if commit.time() < min_prepare_time
            || self
                .get(id)
                .map(|(c, _, _)| c.time() < min_prepare_time)
                .unwrap_or(false)
        {
            CheckPrepareStatus::TooLate
        } else {
            CheckPrepareStatus::Unknown
        }
    }

}
