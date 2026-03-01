use crate::mvcc::disk::error::StorageError;
use crate::occ::{PrepareConflict, PrepareResult, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use serde::{de::DeserializeOwned, Serialize};

/// Result of checking the current status of a transaction for prepare decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckPrepareStatus {
    /// Already committed at the exact requested timestamp.
    CommittedAtTimestamp,
    /// Already committed, but at a different timestamp.
    CommittedDifferent { proposed: u64 },
    /// Already aborted.
    Aborted,
    /// Already prepared at the requested timestamp.
    PreparedAtTimestamp { finalized: bool },
    /// Commit time is before min_prepare_time (too late).
    TooLate,
    /// No decision can be made from local state.
    Unknown,
}

/// Abstracts all stateful operations the TAPIR replica needs.
///
/// Implementations must provide:
/// - **Versioned reads**: point gets, snapshot reads, and range scans over
///   committed data, with optional read-protection tracking for
///   linearizability.
/// - **Transaction lifecycle**: prepare (with conflict detection), commit,
///   and abort of transactions, including a registry of currently-prepared
///   transactions.
/// - **Transaction log**: durable record of committed and aborted outcomes,
///   queryable by transaction ID.
/// - **Min-prepare-time thresholds**: tentative and finalized lower bounds
///   on acceptable prepare timestamps, used to reject stale transactions.
/// - **CDC deltas**: per-view change-data-capture history for resharding.
/// - **Resharding baselines**: read-protection watermarks for safe
///   cross-shard data transfer.
///
/// The TAPIR replica keeps only transient protocol state (key range,
/// phase, counters) and delegates all persistent state through this trait.
pub trait TapirStore<K: Key, V: Value>: Send + Serialize + DeserializeOwned + 'static {
    // === Identity ===

    fn shard(&self) -> ShardNumber;

    // === Uncommitted Reads ===

    /// Read the latest committed version of `key` (any timestamp).
    ///
    /// **Pure read** -- takes `&self`, no side effects. Does not record a
    /// read timestamp, so this read cannot cause future `try_prepare_txn`
    /// calls to fail OCC checks. Used by the replica for `UO::Get`
    /// (unlogged single-key reads that skip consensus).
    ///
    /// Returns `(value, write_ts)` where `write_ts` is the commit
    /// timestamp of the version returned. If no version exists, returns
    /// `(None, Timestamp::default())`.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("x","v1"), ts(1,1)):
    /// do_uncommitted_get("x") => (Some("v1"), ts(1,1))
    ///
    /// // After commit_prepared_txn(_, write("x","v2"), ts(5,1)):
    /// do_uncommitted_get("x") => (Some("v2"), ts(5,1))
    ///
    /// // Key never written:
    /// do_uncommitted_get("y") => (None, Timestamp::default())
    /// ```
    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError>;

    /// Read the version of `key` visible at snapshot timestamp `ts`.
    ///
    /// **Pure read** -- takes `&self`, no side effects. Returns the latest
    /// committed version with `write_ts <= ts`. Does not record a read
    /// timestamp, so this read cannot block future OCC prepares. Used by
    /// the replica for `UO::GetAt` (unlogged point-in-time reads that skip
    /// consensus).
    ///
    /// Returns `(value, write_ts)`. If no version exists at or before `ts`,
    /// returns `(None, Timestamp::default())`.
    ///
    /// # Valid inputs
    ///
    /// `ts` acts as an upper bound on which versions are visible. Any
    /// `Timestamp` is valid, including `Timestamp::default()` (returns only
    /// versions written at time 0).
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("x","v1"), ts(1,1))
    /// //   and commit_prepared_txn(_, write("x","v2"), ts(5,1)):
    /// do_uncommitted_get_at("x", ts(3,1))  => (Some("v1"), ts(1,1))
    /// do_uncommitted_get_at("x", ts(10,1)) => (Some("v2"), ts(5,1))
    ///
    /// // After commit_prepared_txn(_, write("x","v3"), ts(7,1)):
    /// do_uncommitted_get_at("x", ts(10,1)) => (Some("v3"), ts(7,1))
    /// do_uncommitted_get_at("x", ts(3,1))  => (Some("v1"), ts(1,1))  // unchanged
    /// ```
    fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError>;

    /// Scan all keys in `[start, end]` (inclusive) at snapshot timestamp `ts`.
    ///
    /// **Pure read** -- takes `&self`, no side effects. For each key in
    /// the range, returns the latest committed version with `write_ts <= ts`.
    /// Does not record read timestamps or range protections, so this scan
    /// cannot cause future OCC prepare failures. Used by the replica for
    /// `UO::Scan` (with `ts = Timestamp::MAX`) and `UO::ScanAt`.
    ///
    /// Returns `Vec<(key, value, write_ts)>` sorted by key. Keys with no
    /// version at or before `ts` are omitted. Returns empty `Vec` if no
    /// keys exist in the range.
    ///
    /// # Valid inputs
    ///
    /// `start <= end` in key ordering. Pass `ts` with `time = u64::MAX`
    /// to scan the latest committed versions of all keys.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("a","v1"), ts(1,1))
    /// //   and commit_prepared_txn(_, write("b","v2"), ts(1,1)):
    /// do_uncommitted_scan("a", "c", ts(10,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))]
    ///
    /// // After commit_prepared_txn(_, write("b","v3"), ts(5,1)):
    /// do_uncommitted_scan("a", "c", ts(10,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v3"), ts(5,1))]
    /// do_uncommitted_scan("a", "c", ts(3,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))]
    /// ```
    fn do_uncommitted_scan(&self, start: &K, end: &K, ts: Timestamp) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError>;

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        dry_run: bool,
    ) -> PrepareResult<Timestamp>;

    /// Record a commit in the transaction log and apply the write set.
    /// Panics (debug only) if the transaction was previously logged as aborted
    /// or committed at a different timestamp.
    fn commit_prepared_txn(
        &mut self,
        id: TransactionId,
        txn: &Transaction<K, V, Timestamp>,
        commit: Timestamp,
    );

    fn remove_prepared_txn(&mut self, id: TransactionId) -> bool;

    /// Upsert a prepared transaction with three distinct behaviors:
    ///
    /// - **Insert** (vacant): If the transaction ID doesn't exist in `prepared`, inserts
    ///   the new entry and populates `prepared_reads`/`prepared_writes` caches.
    ///
    /// - **Finalize** (same commit timestamp): If the ID already exists with the same
    ///   commit timestamp, only updates the `finalized` flag. Debug-asserts that the
    ///   transaction content is identical. Caches are untouched.
    ///
    /// - **Replace** (different commit timestamp): If the ID exists but with a different
    ///   commit timestamp, replaces the entry entirely — removes old cache entries and
    ///   adds new ones under the new timestamp.
    ///
    /// Calling this twice with identical arguments is idempotent (hits the finalize path).
    /// Calling with a different commit timestamp for the same ID is not idempotent — it
    /// mutates the caches differently each time.
    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    );

    // === Prepared Queries ===

    /// Look up a prepared transaction by ID.
    fn get_prepared_txn(
        &self,
        id: &TransactionId,
    ) -> Option<(&Timestamp, &SharedTransaction<K, V, Timestamp>, bool)>;

    /// Check the current status of a transaction for prepare/check-prepare decisions.
    ///
    /// Performs a cascading lookup: txn_log → prepared_at_timestamp → min_prepare_time,
    /// returning a typed status that callers map to their own result type.
    fn check_prepare_status(&self, id: &TransactionId, commit: &Timestamp) -> CheckPrepareStatus;

    /// Finalize a tentatively prepared transaction after IR quorum confirmation.
    ///
    /// The normal commit path (exec_inconsistent → commit_prepared_txn) never
    /// checks the finalized flag. This method is used in TAPIR merge (view
    /// change), where it finalizes tentative prepared transactions that the
    /// quorum agreed on. Unfinalized entries are discarded by
    /// remove_all_unfinalized_prepared_txns().
    ///
    /// Returns true if the entry existed at the given commit timestamp.
    fn finalize_prepared_txn(&mut self, id: &TransactionId, commit: &Timestamp) -> bool;

    fn prepared_count(&self) -> usize;

    /// Returns the oldest prepared transaction (minimum commit timestamp).
    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)>;

    /// Remove all prepared transactions that are NOT finalized.
    fn remove_all_unfinalized_prepared_txns(&mut self);

    // === Committed Read/Scan ===

    /// Read `key` at snapshot `ts` with conflict detection and read
    /// protection.
    ///
    /// **Mutating** -- takes `&mut self`. Used by the replica for
    /// `IO::QuorumRead` (read-only transaction reads that go through IR
    /// inconsistent consensus for linearizability).
    ///
    /// # Side effects
    ///
    /// 1. **Conflict check**: If any prepared-but-uncommitted transaction
    ///    has a write on `key` with `commit_ts <= ts`, returns
    ///    `Err(PrepareConflict)` immediately with no state change. This
    ///    prevents reading stale data when a concurrent RW transaction's
    ///    finalize has not yet reached this replica.
    ///
    /// 2. **Read protection**: On success, records `commit_get(key, ts)`
    ///    in the MVCC store, updating `last_read_ts` for the version at
    ///    `ts`. Future `try_prepare_txn` calls writing to this key will
    ///    fail OCC checks if their `commit_ts <= ts`.
    ///
    /// 3. **Read-commit tracking**: Updates `max_read_commit_time =
    ///    max(prev, ts)`, observable via `min_prepare_baseline()` and
    ///    used by resharding to carry forward read protections.
    ///
    /// 4. **Enables fast-path validation**: After this call,
    ///    `do_uncommitted_get_validated(key, ts)` returns `Some` instead
    ///    of `None` for the same key and timestamp.
    ///
    /// # Valid inputs
    ///
    /// `ts` is the client's snapshot timestamp. Any `Timestamp` is valid.
    /// `key` is moved (not borrowed) because it is stored in the MVCC
    /// read log.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("x","v1"), ts(1,1)):
    /// do_committed_get("x", ts(5,1)) => Ok((Some("v1"), ts(1,1)))
    ///
    /// // After try_prepare_txn(_, write("x","v2"), ts(5,1)):
    /// do_committed_get("x", ts(10,1)) => Err(PrepareConflict)
    ///
    /// // After remove_prepared_txn(_):
    /// do_committed_get("x", ts(10,1)) => Ok((Some("v1"), ts(1,1)))
    ///
    /// // After commit_prepared_txn(_, write("x","v2"), ts(5,1)):
    /// do_committed_get("x", ts(10,1)) => Ok((Some("v2"), ts(5,1)))
    /// ```
    fn do_committed_get(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict>;

    /// Scan `[start, end]` (inclusive) at snapshot `ts` with conflict
    /// detection and range-level read protection.
    ///
    /// **Mutating** -- takes `&mut self`. Used by the replica for
    /// `IO::QuorumScan` (read-only transaction range scans that go through
    /// IR inconsistent consensus for linearizability).
    ///
    /// # Side effects
    ///
    /// 1. **Conflict check**: If *any* key in `[start, end]` has a
    ///    prepared-but-uncommitted write with `commit_ts <= ts`, returns
    ///    `Err(PrepareConflict)` immediately with no state change. This
    ///    prevents phantom reads when a concurrent RW transaction's
    ///    finalize has not yet reached this replica.
    ///
    /// 2. **Range read protection**: On success, records
    ///    `commit_scan(start, end, ts)` which appends to `range_reads`.
    ///    Future `try_prepare_txn` calls writing to any key in
    ///    `[start, end]` will fail OCC checks if their `commit_ts <= ts`.
    ///
    /// 3. **Read-commit tracking**: Updates `max_read_commit_time =
    ///    max(prev, ts)`, observable via `min_prepare_baseline()`.
    ///
    /// 4. **Enables fast-path validation**: After this call,
    ///    `do_uncommitted_scan_validated(start, end, ts)` returns `Some`
    ///    instead of `None` for the same range and timestamp.
    ///
    /// # Valid inputs
    ///
    /// `start <= end` in key ordering. `ts` is the client's snapshot
    /// timestamp. `start` and `end` are moved (not borrowed) because they
    /// are stored in the range-read protection list.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("a","v1"), ts(1,1))
    /// //   and commit_prepared_txn(_, write("b","v2"), ts(1,1)):
    /// do_committed_scan("a", "b", ts(5,1))
    ///   => Ok([("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))])
    ///
    /// // After try_prepare_txn(_, write("a","v3"), ts(3,1)):
    /// do_committed_scan("a", "b", ts(5,1)) => Err(PrepareConflict)
    ///
    /// // After remove_prepared_txn(_):
    /// do_committed_scan("a", "b", ts(5,1))
    ///   => Ok([("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))])
    /// ```
    fn do_committed_scan(
        &mut self,
        start: K,
        end: K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict>;

    // === Uncommitted Validated Reads ===

    /// Read `key` at snapshot `ts` using a previously established read
    /// protection — the "fast path" for read-only transactions.
    ///
    /// **Non-mutating** — takes `&self`. Used by the replica for
    /// `UO::ReadValidated` (unlogged single-replica reads that skip IR
    /// consensus when a prior quorum read has already set read protection).
    ///
    /// # Side effects
    ///
    /// **None.** This is a pure read. It does not update `last_read_ts`,
    /// does not modify `max_read_commit_time`, and does not check for
    /// prepared-but-uncommitted writes (that conflict detection already
    /// happened during the prior `do_committed_get` call that established
    /// the read protection).
    ///
    /// # How it works
    ///
    /// Looks up `last_read_ts` for the version of `key` at `ts`. If
    /// `last_read_ts >= ts`, the version is considered "validated" and the
    /// value is returned. Otherwise returns `None`, signalling the caller
    /// to fall back to a full quorum read.
    ///
    /// # Valid inputs
    ///
    /// `key`: any key (borrowed). `ts`: any `Timestamp` — acts as both
    /// the snapshot time and the minimum required read-protection level.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After seed_value("x", "v1", ts(1,1)):
    /// do_uncommitted_get_validated("x", ts(5,1)) => None
    ///   // Value exists but no read protection has been set.
    ///
    /// // After do_committed_get("x", ts(5,1)):
    /// do_uncommitted_get_validated("x", ts(5,1)) => Some((Some("v1"), ts(1,1)))
    ///   // read_ts is now ts(5,1) >= ts(5,1), so validated.
    /// do_uncommitted_get_validated("x", ts(10,1)) => None
    ///   // read_ts is ts(5,1) < ts(10,1), protection insufficient.
    ///
    /// // After do_committed_get("x", ts(10,1)):
    /// do_uncommitted_get_validated("x", ts(10,1)) => Some((Some("v1"), ts(1,1)))
    ///   // read_ts is now ts(10,1) >= ts(10,1).
    ///
    /// // After commit_prepared_txn(_, write("x","v2"), ts(7,1)):
    /// do_uncommitted_get_validated("x", ts(10,1)) => Some((Some("v2"), ts(7,1)))
    ///   // New committed version is visible at snapshot ts(10,1).
    /// ```
    fn do_uncommitted_get_validated(&self, key: &K, ts: Timestamp) -> Option<(Option<V>, Timestamp)>;

    /// Scan `[start, end]` (inclusive) at snapshot `ts` using a previously
    /// established range-read protection — the "fast path" for read-only
    /// transaction scans.
    ///
    /// **Non-mutating** — takes `&self`. Used by the replica for
    /// `UO::ScanValidated` (unlogged single-replica scans that skip IR
    /// consensus when a prior quorum scan has already set range protection).
    ///
    /// # Side effects
    ///
    /// **None.** This is a pure read. It does not append to `range_reads`,
    /// does not modify `max_read_commit_time`, and does not check for
    /// prepared-but-uncommitted writes (that conflict detection already
    /// happened during the prior `do_committed_scan` call that established
    /// the range protection).
    ///
    /// # How it works
    ///
    /// Searches recorded range-read entries for any `(rs, re, rts)` where
    /// `rs <= start`, `re >= end`, and `rts >= ts`. If such a covering
    /// entry exists, performs the MVCC scan and returns the results.
    /// Otherwise returns `None`, signalling the caller to fall back to
    /// a full quorum scan.
    ///
    /// # Valid inputs
    ///
    /// `start <= end` in key ordering (inclusive range). `ts`: any
    /// `Timestamp` — acts as both the snapshot time and the minimum
    /// required range-protection level. Keys are borrowed (`&K`).
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After seed_value("a", "v1", ts(1,1))
    /// //   and seed_value("b", "v2", ts(1,1)):
    /// do_uncommitted_scan_validated("a", "b", ts(5,1)) => None
    ///   // Values exist but no range protection has been set.
    ///
    /// // After do_committed_scan("a", "b", ts(5,1)):
    /// do_uncommitted_scan_validated("a", "b", ts(5,1))
    ///   => Some([("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))])
    ///   // range_reads has (a, b, ts(5,1)); ts(5,1) >= ts(5,1), covered.
    /// do_uncommitted_scan_validated("a", "b", ts(10,1)) => None
    ///   // ts(10,1) > ts(5,1), protection insufficient.
    ///
    /// // After do_committed_scan("a", "c", ts(10,1)):
    /// do_uncommitted_scan_validated("a", "b", ts(10,1))
    ///   => Some([("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))])
    ///   // range_reads has (a, c, ts(10,1)); a<=a, c>=b, ts(10,1)>=ts(10,1).
    /// do_uncommitted_scan_validated("b", "c", ts(10,1))
    ///   => Some([("b", Some("v2"), ts(1,1))])
    ///   // Also covered by (a, c, ts(10,1)); a<=b, c>=c.
    /// ```
    fn do_uncommitted_scan_validated(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>>;

    // === Transaction Log ===

    /// Look up a transaction's final outcome in the transaction log.
    ///
    /// Returns `Some((timestamp, committed))` if the transaction has been
    /// recorded, where `committed = true` means committed at `timestamp`,
    /// and `committed = false` means aborted (`timestamp` is
    /// `Timestamp::default()` for aborts).
    ///
    /// Returns `None` if the transaction has never been logged.
    ///
    /// # Callers
    ///
    /// - `exec_inconsistent(Abort)`: checks whether the transaction was
    ///   already committed before recording an abort.
    /// - `check_prepare_status`: first lookup in the cascading chain
    ///   (txn_log → prepared → min_prepare_time).
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:
    ///   txn_log_get(&id) → None
    ///
    /// After txn_log_insert(id, ts(10,1), true):
    ///   txn_log_get(&id) → Some((ts(10,1), true))
    ///
    /// After txn_log_insert(id, default(), false):
    ///   txn_log_get(&id) → Some((ts(0,0), false))
    /// ```
    fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)>;

    /// Record a transaction outcome (commit or abort) in the transaction log.
    ///
    /// Inserts `(ts, committed)` for the given transaction ID. If the ID
    /// already exists, the entry is **replaced** and the previous value is
    /// returned.
    ///
    /// - `committed = true`: the transaction committed at `ts`.
    /// - `committed = false`: the transaction was aborted. By convention,
    ///   `ts` is `Timestamp::default()` for aborts.
    ///
    /// # Return value
    ///
    /// `Some((old_ts, old_committed))` if the ID was already present (the
    /// old entry is replaced), or `None` for a new insertion.
    ///
    /// # Side effects
    ///
    /// - `txn_log_get` and `txn_log_contains` immediately reflect the new
    ///   entry. `txn_log_len` increments by 1 for new IDs (not replacements).
    /// - `check_prepare_status` consults the log first: a committed entry
    ///   yields `CommittedAtTimestamp` or `CommittedDifferent`; an aborted
    ///   entry yields `Aborted`.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store (txn_log_len = 0):
    ///   txn_log_insert(id, ts(10,1), true)  → None       // len = 1
    ///   txn_log_insert(id, ts(20,1), false) → Some((ts(10,1), true))  // replaced
    ///   txn_log_get(&id) → Some((ts(20,1), false))       // current value
    /// ```
    fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)>;

    /// Check whether a transaction has any outcome recorded in the log.
    ///
    /// Returns `true` if the ID has been inserted (committed or aborted).
    /// Equivalent to `txn_log_get(id).is_some()` but avoids copying.
    ///
    /// # Callers
    ///
    /// - `sync()`: during leader-record replay, skips re-finalizing a
    ///   prepare if `txn_log_contains` returns `true`, since the
    ///   transaction already has a final outcome.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:                          txn_log_contains(&id) → false
    /// After txn_log_insert(id, ..., true):  txn_log_contains(&id) → true
    /// ```
    fn txn_log_contains(&self, id: &TransactionId) -> bool;

    /// Return the number of entries in the transaction log.
    ///
    /// Counts both committed and aborted entries. Only grows (there is no
    /// method to remove individual log entries). Replacing an existing
    /// entry does not change the count.
    ///
    /// Used by `metrics()` to expose `tapirs_transaction_log_size`.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:                            txn_log_len() → 0
    /// After txn_log_insert(id1, ..., true):   txn_log_len() → 1
    /// After txn_log_insert(id2, ..., false):  txn_log_len() → 2
    /// After txn_log_insert(id1, ..., false):  txn_log_len() → 2  // replace
    /// ```
    fn txn_log_len(&self) -> usize;

    // === Min Prepare Time ===

    /// Speculatively raise the tentative min_prepare_time.
    ///
    /// Computes `max(current_tentative, min(time, min_prepared_timestamp))`
    /// where `min_prepared_timestamp` is the smallest commit timestamp among
    /// all currently prepared transactions (`u64::MAX` if none exist).
    ///
    /// The `min(time, min_prepared_timestamp)` cap prevents raising the
    /// threshold above any existing prepared transaction, which would
    /// retroactively invalidate it.
    ///
    /// # Return value
    ///
    /// The tentative `min_prepare_time` after the update. Monotonically
    /// non-decreasing under `raise` alone, but can be rolled back by
    /// `sync_min_prepare_time` or `reset_min_prepare_time_to_finalized`.
    ///
    /// # Side effects
    ///
    /// - `check_prepare_status` returns `TooLate` for any transaction whose
    ///   `commit.time < min_prepare_time` (the tentative value).
    /// - Does NOT change `finalized_min_prepare_time`.
    ///
    /// # Callers
    ///
    /// - `exec_consensus(RaiseMinPrepareTime)`: explicit raise from client
    ///   (resharding sets `commit.time + 1` to subsume read protections).
    ///
    /// # Min Prepare Time State Model
    ///
    /// Two fields track min_prepare_time:
    /// - **tentative** (`min_prepare_time`): the working threshold used by
    ///   `check_prepare_status`. Can be raised speculatively and rolled back.
    /// - **finalized** (`finalized_min_prepare_time`): quorum-confirmed floor.
    ///   Monotonically increasing; survives view changes.
    ///
    /// The four functions and their effects:
    ///
    /// | Function    | tentative                            | finalized      |
    /// |-------------|--------------------------------------|----------------|
    /// | `raise`     | `max(t, min(time, min_prepared_ts))` | unchanged      |
    /// | `finalize`  | `max(t, finalized')`                 | `max(f, time)` |
    /// | `sync`      | `min(t, finalized')`                 | `max(f, time)` |
    /// | `reset`     | `= finalized`                        | unchanged      |
    ///
    /// **Invariant**: `tentative >= finalized` after `raise`, `finalize`, and
    /// `reset`. After `sync`, tentative can be **less than** finalized (this
    /// is intentional — sync rolls back speculative raises the leader didn't
    /// confirm).
    ///
    /// **Observation idiom**: `raise(0)` returns the current tentative value
    /// without mutation, because `max(current, min(0, any)) = current` for
    /// any non-negative current. Used in conformance tests to observe state.
    ///
    /// **Valid inputs**: All four functions accept any `u64`. Values below the
    /// current effective threshold are no-ops for monotonic operations (`raise`
    /// on tentative, `finalize`/`sync` on finalized). `reset` takes no
    /// argument.
    ///
    /// # Interactions
    ///
    /// - After `raise(X)`, `finalize(Y)` leaves tentative at `max(X, Y)` (does
    ///   not lower it). But `sync(Y)` pulls tentative down to
    ///   `min(X, max(old_finalized, Y))`.
    /// - After `raise(X)`, `reset()` sets tentative to finalized (discarding X
    ///   if X > finalized).
    /// - `raise` is the only function that returns a value. Calling `raise(0)`
    ///   after any other function reveals the new tentative.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store (tentative=0, no prepared txns):
    ///   raise(100) → 100    // max(0, min(100, MAX)) = 100
    ///   raise(50)  → 100    // max(100, min(50, MAX)) = 100 (monotonic)
    ///   raise(200) → 200    // max(100, min(200, MAX)) = 200
    ///
    /// With a prepared txn at ts.time=50, tentative=100:
    ///   raise(200) → 100    // max(100, min(200, 50)) = 100  (capped)
    ///   raise(30)  → 100    // max(100, min(30, 50))  = 100
    /// ```
    ///
    /// # Cross-function example
    ///
    /// ```text
    /// Fresh store:       tentative=0,   finalized=0
    ///
    /// raise(100):        tentative=100, finalized=0     // speculative
    ///   raise(0) → 100
    ///
    /// finalize(80):      tentative=100, finalized=80    // quorum confirms 80
    ///   raise(0) → 100                                  // tentative stays (>= 80)
    ///
    /// raise(200):        tentative=200, finalized=80    // another speculative
    ///   raise(0) → 200
    ///
    /// sync(50):          tentative=50,  finalized=80    // rollback! t < f
    ///   raise(0) → 50
    ///
    /// reset():           tentative=80,  finalized=80    // snap to finalized
    ///   raise(0) → 80
    /// ```
    fn raise_min_prepare_time(&mut self, time: u64) -> u64;

    /// Finalize the min_prepare_time after IR quorum agreement.
    ///
    /// Two-step update:
    /// 1. `finalized = max(finalized, time)` — finalized only increases.
    /// 2. `tentative = max(tentative, finalized)` — tentative is raised to
    ///    at least match finalized, but never lowered.
    ///
    /// Unlike `raise_min_prepare_time`, this does NOT cap at
    /// `min_prepared_timestamp` — the quorum has agreed on this value, so
    /// it is authoritative even if it exceeds a prepared transaction's
    /// timestamp.
    ///
    /// See [`raise_min_prepare_time`] for the two-field state model overview.
    ///
    /// # Interactions
    ///
    /// - After `finalize(X)`, `raise(0)` returns at least `X` (tentative was
    ///   raised to `max(tentative, X)`).
    /// - After `finalize(X)`, `reset()` will set tentative to at least `X`
    ///   (finalized is now at least `X`).
    /// - `finalize` does NOT lower tentative. If `raise(200)` was called
    ///   before `finalize(100)`, tentative stays at 200.
    ///
    /// # Side effects
    ///
    /// - May raise tentative, causing `check_prepare_status` to return
    ///   `TooLate` for transactions below the new threshold.
    /// - The finalized value persists through `sync_min_prepare_time` and
    ///   `reset_min_prepare_time_to_finalized` (they use it as a floor).
    ///
    /// # Callers
    ///
    /// - `finalize_consensus(RaiseMinPrepareTime)`: after IR confirms the
    ///   quorum agreed on the value.
    ///
    /// # Examples
    ///
    /// ```text
    /// State: tentative=50, finalized=0
    ///   finalize(100) → finalized=100, tentative=max(50,100)=100
    ///
    /// State: tentative=200, finalized=100
    ///   finalize(150) → finalized=150, tentative=max(200,150)=200
    ///
    /// State: tentative=100, finalized=100
    ///   finalize(80)  → finalized=100, tentative=100  // no change
    /// ```
    fn finalize_min_prepare_time(&mut self, time: u64);

    /// Sync the min_prepare_time from the leader's authoritative value.
    ///
    /// Two-step update:
    /// 1. `finalized = max(finalized, time)` — finalized only increases.
    /// 2. `tentative = min(tentative, finalized)` — tentative is pulled
    ///    DOWN to match finalized if it was speculatively raised above it.
    ///
    /// This is the key difference from `finalize_min_prepare_time`: step 2
    /// uses `min` instead of `max`, allowing rollback of speculative raises
    /// that the leader did not confirm.
    ///
    /// See [`raise_min_prepare_time`] for the two-field state model overview.
    ///
    /// # Interactions
    ///
    /// - After `sync(X)`, `raise(0)` may return a **lower** value than before
    ///   (tentative was pulled down to `min(old_tentative, max(old_finalized,
    ///   X))`).
    /// - After `sync`, tentative can be **less than** finalized. This is the
    ///   only function that breaks the `tentative >= finalized` invariant.
    /// - After `sync(X)`, `reset()` sets tentative to `max(old_finalized, X)`
    ///   (the new finalized), which may be higher than the post-sync tentative.
    ///
    /// # Side effects
    ///
    /// - May lower tentative, causing `check_prepare_status` to stop
    ///   returning `TooLate` for transactions that were previously rejected.
    /// - Finalized value only increases (monotonic).
    ///
    /// # Callers
    ///
    /// - `sync(RaiseMinPrepareTime)`: applies the leader's finalized value
    ///   during leader-record synchronization.
    ///
    /// # Examples
    ///
    /// ```text
    /// State: tentative=100, finalized=30
    ///   sync(50) → finalized=50, tentative=min(100,50)=50
    ///
    /// State: tentative=50, finalized=50
    ///   sync(40) → finalized=50, tentative=min(50,50)=50
    ///
    /// State: tentative=50, finalized=50
    ///   sync(80) → finalized=80, tentative=min(50,80)=50
    /// ```
    fn sync_min_prepare_time(&mut self, time: u64);

    /// Reset tentative min_prepare_time to the finalized value.
    ///
    /// Sets `tentative = finalized`, discarding any speculative raises not
    /// confirmed by quorum. No-op when `tentative == finalized`.
    ///
    /// See [`raise_min_prepare_time`] for the two-field state model overview.
    ///
    /// # Interactions
    ///
    /// - After `reset()`, `raise(0)` returns the finalized value (all
    ///   speculative raises are discarded).
    /// - `reset` does not affect finalized. A subsequent `finalize(X)` or
    ///   `sync(X)` behaves identically to calling it before the reset.
    /// - If `sync` left tentative < finalized, `reset()` restores the
    ///   invariant `tentative = finalized`.
    ///
    /// # Side effects
    ///
    /// - Typically lowers tentative (since speculative raises push it above
    ///   finalized), which may un-reject transactions that were `TooLate`.
    /// - Always followed by `remove_all_unfinalized_prepared_txns()` in the
    ///   merge flow, so the prepared set is also cleaned up.
    ///
    /// # Callers
    ///
    /// - `merge()`: first step of merge, before replaying quorum-agreed
    ///   operations. Clears speculative state from the previous view.
    ///
    /// # Examples
    ///
    /// ```text
    /// State: tentative=100, finalized=42
    ///   reset() → tentative=42, finalized=42
    ///
    /// State: tentative=50, finalized=50
    ///   reset() → no change
    /// ```
    fn reset_min_prepare_time_to_finalized(&mut self);

    // === CDC Deltas ===

    /// Record a CDC delta for a completed view change.
    ///
    /// Stores the delta keyed by `base_view` (the view the replica was in
    /// before the transition). If a delta for `base_view` already exists,
    /// it is replaced.
    ///
    /// Each delta captures the committed KV changes that were finalized
    /// during the view transition from `base_view` to some `to_view`.
    ///
    /// # Side effects
    ///
    /// - `cdc_max_view()` may increase to `base_view`.
    /// - `cdc_deltas_from(v)` for `v <= base_view` will include this delta.
    ///
    /// # Callers
    ///
    /// - View-change handler in the replica: called once per view change
    ///   after finalizing operations, with the changes committed during
    ///   that transition.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:
    ///   cdc_max_view() → None
    ///
    /// After record_cdc_delta(0, delta_0):
    ///   cdc_max_view() → Some(0)
    ///   cdc_deltas_from(0) → [delta_0]
    ///
    /// After record_cdc_delta(3, delta_3):
    ///   cdc_max_view() → Some(3)
    ///   cdc_deltas_from(0) → [delta_0, delta_3]
    /// ```
    fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>);

    /// Retrieve all CDC deltas with `base_view >= from_view`.
    ///
    /// Returns deltas in ascending `base_view` order. The range is
    /// inclusive on the lower bound (`from_view..`). Returns an empty
    /// `Vec` if no deltas exist at or above `from_view`.
    ///
    /// # Callers
    ///
    /// - `exec_unlogged(ScanChanges)`: called by the resharding catch-up
    ///   client. The caller advances its cursor as
    ///   `next_from = cdc_max_view + 1` to get only new deltas.
    ///
    /// # Examples
    ///
    /// ```text
    /// After record_cdc_delta(0, d0), (1, d1), (3, d3):
    ///   cdc_deltas_from(0) → [d0, d1, d3]
    ///   cdc_deltas_from(1) → [d1, d3]
    ///   cdc_deltas_from(2) → [d3]      // no delta at 2, next is 3
    ///   cdc_deltas_from(4) → []         // nothing at or above 4
    /// ```
    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>>;

    /// Return the highest `base_view` among recorded CDC deltas.
    ///
    /// Returns `None` if no deltas have been recorded. Returns `Some(0)`
    /// if the only delta was recorded at `base_view = 0`. This distinction
    /// matters: `None` means "no CDC history" while `Some(0)` means
    /// "changes from view 0 exist, scan from view 1 next".
    ///
    /// # Callers
    ///
    /// - `exec_unlogged(ScanChanges)`: returned as `effective_end_view`,
    ///   used by the resharding client as a cursor to track consumption.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:                         cdc_max_view() → None
    /// After record_cdc_delta(0, d0):       cdc_max_view() → Some(0)
    /// After record_cdc_delta(3, d3):       cdc_max_view() → Some(3)
    /// ```
    fn cdc_max_view(&self) -> Option<u64>;

    // === Resharding ===

    /// Return the OCC read-protection high-water marks for resharding.
    ///
    /// Returns `(max_range_read_time, max_read_commit_time)`:
    ///
    /// - `max_range_read_time`: the highest `scan_ts` passed to
    ///   `do_committed_scan`. Protects against phantom writes. `None` if
    ///   no committed scans have been performed.
    ///
    /// - `max_read_commit_time`: the highest commit timestamp passed to
    ///   `do_committed_get`. Protects against write-after-read conflicts.
    ///   `None` if no committed gets have been performed.
    ///
    /// The two values are tracked independently — a `do_committed_get`
    /// does not affect `max_range_read_time` and vice versa. Each value
    /// is either `None` (no reads/scans yet) or `Some(ts)` where `ts`
    /// is monotonically non-decreasing: once set, it can only increase
    /// as higher-timestamped reads or scans arrive.
    ///
    /// # Side effects
    ///
    /// None. This is a read-only query — no state mutation occurs.
    ///
    /// # Usage
    ///
    /// Only meaningful when the shard is in `Decommissioning` phase (new
    /// reads are blocked). The caller computes
    /// `barrier = max(both_values) + 1` and calls
    /// `raise_min_prepare_time(barrier)` on the target shard to subsume
    /// all historical read protections from the source.
    ///
    /// # Examples
    ///
    /// ```text
    /// Fresh store:
    ///   min_prepare_baseline() → (None, None)
    ///
    /// After do_committed_get("x", ts(5,1)):
    ///   min_prepare_baseline() → (None, Some(ts(5,1)))
    ///
    /// After do_committed_scan("a", "b", ts(7,1)):
    ///   min_prepare_baseline() → (Some(ts(7,1)), Some(ts(5,1)))
    ///
    /// After do_committed_get("y", ts(3,1)):  // 3 < 5, max unchanged
    ///   min_prepare_baseline() → (Some(ts(7,1)), Some(ts(5,1)))
    ///
    /// After do_committed_get("z", ts(9,1)):  // 9 > 5, max updated
    ///   min_prepare_baseline() → (Some(ts(7,1)), Some(ts(9,1)))
    /// ```
    fn min_prepare_baseline(&self) -> (Option<Timestamp>, Option<Timestamp>);
}
