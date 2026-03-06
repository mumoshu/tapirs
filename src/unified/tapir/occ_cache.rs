use crate::mvcc::disk::error::StorageError;
use crate::occ::PrepareResult;
use crate::tapir::Timestamp;
use std::collections::BTreeMap;
use std::ops::Bound;

/// Trait for MVCC committed-state queries needed by OCC validation.
/// Implemented by TapirState to supply committed data without OccCache
/// depending on VlogLsm internals.
pub(crate) trait MvccQueries<K> {
    /// Returns (version_ts, next_version_ts) for the version of key at read_ts.
    /// If no version exists at read_ts, returns (Timestamp::default(), None).
    fn get_version_range(
        &self,
        key: &K,
        read_ts: Timestamp,
    ) -> Result<(Timestamp, Option<Timestamp>), StorageError>;

    /// Returns last_read_ts for the latest version of key.
    fn get_last_read_ts(&self, key: &K) -> Result<Option<Timestamp>, StorageError>;

    /// Returns last_read_ts for the version of key at or before at_ts.
    fn get_last_read_ts_at(
        &self,
        key: &K,
        at_ts: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError>;

    /// Returns true if any committed write exists in [start,end] with
    /// timestamp in (after_ts, before_ts).
    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError>;

    /// Returns the latest committed version timestamp for key.
    fn get_latest_version_ts(&self, key: &K) -> Result<Timestamp, StorageError>;
}

/// In-memory OCC (optimistic concurrency control) state for conflict detection
/// in TapirState. Tracks prepared transaction read/write sets and read-only
/// transaction protections to achieve strict serializability.
///
/// # Anomaly Prevention
///
/// | Anomaly | Mechanism | Data Used |
/// |---------|-----------|-----------|
/// | **Non-repeatable reads** (RW txn read invalidated by committed write) | `occ_check` read-set validation: `MvccQueries::get_version_range()` detects overwritten version → `Fail` | MVCC VlogLsm |
/// | **Write-after-committed-read** (write at ts < committed read's ts) | `occ_check` write-set validation: `MvccQueries::get_last_read_ts[_at]()` detects read protection → `Retry` | MVCC VlogLsm `last_read_ts` |
/// | **Write-after-prepared-read** (write conflicts with in-flight read) | `occ_check` write-set validation: checks `self.reads` for prepared reads > commit_ts → `Abstain` | `reads` cache |
/// | **Scan phantom (RW txn)** (new key in scanned range) | `occ_check` scan-set validation: `MvccQueries::has_writes_in_range()` for committed → `Fail`; checks `self.writes` range for prepared → `Abstain` | MVCC VlogLsm + `writes` cache |
/// | **Write into RO-scanned range** (future write violates RO scan) | `occ_check` write-set validation: checks `self.range_reads` for key in protected range → `Retry` | `range_reads` |
/// | **Point phantom (RO read of non-existent key)** | `snapshot_get_protected` records degenerate `range_reads` entry `(key, key, ts)` when key doesn't exist → same mechanism as range protection | `range_reads` |
/// | **RW→RO stale read** (prepared write not yet finalized) | `check_get_conflict` / `check_scan_conflict`: checks `self.writes` for prepared writes at ts ≤ snapshot_ts → `PrepareConflict` | `writes` cache |
///
/// # Recovery
///
/// After crash, `reads` and `writes` are rebuilt from the prepared VlogLsm
/// via `OccCacheState::transactions`. `range_reads` are NOT rebuilt — they
/// are subsumed by `max_read_time` which acts as a conservative global
/// watermark: any prepare with `commit_ts < max_read_time` → `Retry`.
pub(crate) struct OccCache<K: Ord> {
    linearizable: bool,
    /// Read keys from in-flight prepared RW transactions: key → {commit_ts}.
    reads: BTreeMap<K, BTreeMap<Timestamp, ()>>,
    /// Write keys from in-flight prepared RW transactions: key → {commit_ts}.
    writes: BTreeMap<K, BTreeMap<Timestamp, ()>>,
    /// Range-level read protections: (start, end, snapshot_ts).
    ///
    /// Sources: snapshot_scan_protected (full range scans),
    /// snapshot_get_protected of non-existent key (degenerate (key, key, ts)).
    /// Used by write-set validation to block writes at commit_ts < snapshot_ts.
    range_reads: Vec<(K, K, Timestamp)>,
}

/// Recovered state for OccCache initialization.
pub(crate) struct OccCacheState<K> {
    /// Active prepared txns recovered from prepared VlogLsm.
    pub transactions: Vec<OccCachedTransaction<K>>,
}

pub(crate) struct OccCachedTransaction<K> {
    pub read_set: Vec<(K, Timestamp)>,
    pub write_set: Vec<K>,
    pub commit_ts: Timestamp,
}

impl<K: Ord + Clone> OccCache<K> {
    pub(crate) fn new(linearizable: bool, state: Option<OccCacheState<K>>) -> Self {
        let mut cache = Self {
            linearizable,
            reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            range_reads: Vec::new(),
        };

        if let Some(state) = state {
            for txn in state.transactions {
                let write_keys: Vec<(K, Option<()>)> =
                    txn.write_set.into_iter().map(|k| (k, None)).collect();
                cache.register_prepare(&txn.read_set, &write_keys, txn.commit_ts);
            }
        }

        cache
    }

    /// Populate reads/writes caches when a transaction is prepared.
    pub(crate) fn register_prepare<V>(
        &mut self,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, V)],
        commit_ts: Timestamp,
    ) {
        for (key, _) in read_set {
            self.reads
                .entry(key.clone())
                .or_default()
                .insert(commit_ts, ());
        }
        for (key, _) in write_set {
            self.writes
                .entry(key.clone())
                .or_default()
                .insert(commit_ts, ());
        }
    }

    /// Remove reads/writes caches when a transaction is committed or aborted.
    pub(crate) fn unregister_prepare<V>(
        &mut self,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, V)],
        commit_ts: Timestamp,
    ) {
        for (key, _) in read_set {
            if let std::collections::btree_map::Entry::Occupied(mut occupied) =
                self.reads.entry(key.clone())
            {
                occupied.get_mut().remove(&commit_ts);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
        for (key, _) in write_set {
            if let std::collections::btree_map::Entry::Occupied(mut occupied) =
                self.writes.entry(key.clone())
            {
                occupied.get_mut().remove(&commit_ts);
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
        }
    }

    /// Full OCC validation covering read-set, write-set, and scan-set checks.
    /// Mirrors `occ::Store::occ_check()` but adapted for OccCache + MvccQueries.
    pub(crate) fn occ_check(
        &self,
        mvcc: &impl MvccQueries<K>,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<impl Clone>)],
        scan_set: &[(K, K, Timestamp)],
        commit: Timestamp,
    ) -> Result<PrepareResult<Timestamp>, StorageError> {
        // Check for conflicts with the read set.
        for (key, read) in read_set {
            if *read > commit {
                return Ok(PrepareResult::Retry {
                    proposed: read.time,
                });
            }

            let (beginning, end) = mvcc.get_version_range(key, *read)?;

            if beginning == *read
                && let Some(end) = end
                && (self.linearizable || commit > end)
            {
                return Ok(PrepareResult::Fail);
            }

            if let Some(writes) = self.writes.get(key) {
                let lower = if self.linearizable {
                    Bound::Unbounded
                } else {
                    Bound::Excluded(*read)
                };
                if writes
                    .range((lower, Bound::Excluded(commit)))
                    .next()
                    .is_some()
                {
                    return Ok(PrepareResult::Abstain);
                }
            }
        }

        // Check for conflicts with the write set.
        for (key, _) in write_set {
            if self.linearizable {
                let latest_ts = mvcc.get_latest_version_ts(key)?;
                if latest_ts > commit {
                    return Ok(PrepareResult::Retry {
                        proposed: latest_ts.time,
                    });
                }
            }

            let last_read = if self.linearizable {
                mvcc.get_last_read_ts(key)?
            } else {
                mvcc.get_last_read_ts_at(key, commit)?
            };

            if let Some(last_read) = last_read
                && last_read > commit
            {
                return Ok(PrepareResult::Retry {
                    proposed: last_read.time,
                });
            }

            // Two prepared transactions writing the same key always conflict.
            // In linearizable mode, a prepared write at ts > commit → Retry.
            // Otherwise → Abstain (the other prepare may abort).
            if let Some(writes) = self.writes.get(key)
                && !writes.is_empty()
            {
                if self.linearizable
                    && let Some((write, _)) = writes
                        .range((Bound::Excluded(commit), Bound::Unbounded))
                        .next()
                {
                    return Ok(PrepareResult::Retry {
                        proposed: write.time,
                    });
                }
                return Ok(PrepareResult::Abstain);
            }

            if let Some(reads) = self.reads.get(key)
                && reads
                    .range((Bound::Excluded(commit), Bound::Unbounded))
                    .next()
                    .is_some()
            {
                return Ok(PrepareResult::Abstain);
            }

            for (start, end, read_ts) in &self.range_reads {
                if key >= start && key <= end && *read_ts > commit {
                    return Ok(PrepareResult::Retry {
                        proposed: read_ts.time,
                    });
                }
            }
        }

        // Check for conflicts with the scan set (phantom prevention).
        for (start, end, scan_ts) in scan_set {
            if mvcc.has_writes_in_range(start, end, *scan_ts, commit)? {
                return Ok(PrepareResult::Fail);
            }

            for (_key, timestamps) in self.writes.range(start..=end) {
                if timestamps
                    .range((Bound::Excluded(*scan_ts), Bound::Excluded(commit)))
                    .next()
                    .is_some()
                {
                    return Ok(PrepareResult::Abstain);
                }
            }
        }

        Ok(PrepareResult::Ok)
    }

}

#[cfg(test)]
impl<K: Ord + Clone> OccCache<K> {
    /// Check if a point read conflicts with in-flight prepared writes.
    /// Used by snapshot_get_protected for RO transaction protection.
    pub(crate) fn check_get_conflict(&self, key: &K, snapshot_ts: Timestamp) -> bool {
        if let Some(writes) = self.writes.get(key) {
            for ts in writes.keys() {
                if *ts <= snapshot_ts {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a range scan conflicts with in-flight prepared writes.
    /// Used by snapshot_scan_protected for RO transaction protection.
    pub(crate) fn check_scan_conflict(
        &self,
        start: &K,
        end: &K,
        snapshot_ts: Timestamp,
    ) -> bool {
        for (_key, timestamps) in self.writes.range(start..=end) {
            for ts in timestamps.keys() {
                if *ts <= snapshot_ts {
                    return true;
                }
            }
        }
        false
    }

    /// Record a range-level read protection for RO transactions.
    /// Prevents future prepares from writing into this range at ts < snapshot_ts.
    pub(crate) fn record_range_read(&mut self, start: K, end: K, snapshot_ts: Timestamp) {
        self.range_reads.push((start, end, snapshot_ts));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IrClientId;

    const NO_WRITES: &[(String, Option<String>)] = &[];

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            time,
            client_id: IrClientId(1),
        }
    }

    #[test]
    fn register_unregister_lifecycle() {
        let mut cache: OccCache<String> = OccCache::new(true, None);

        let read_set = vec![("a".to_string(), ts(10))];
        let write_set: Vec<(String, Option<String>)> =
            vec![("b".to_string(), Some("v".to_string()))];

        cache.register_prepare(&read_set, &write_set, ts(15));
        assert!(cache.reads.contains_key("a"));
        assert!(cache.reads["a"].contains_key(&ts(15)));
        assert!(cache.writes.contains_key("b"));
        assert!(cache.writes["b"].contains_key(&ts(15)));

        cache.unregister_prepare(&read_set, &write_set, ts(15));
        assert!(cache.reads.is_empty());
        assert!(cache.writes.is_empty());
    }

    #[test]
    fn register_multiple_txns_same_key() {
        let mut cache: OccCache<String> = OccCache::new(true, None);

        let write_set1: Vec<(String, Option<String>)> =
            vec![("x".to_string(), Some("v1".to_string()))];
        let write_set2: Vec<(String, Option<String>)> =
            vec![("x".to_string(), Some("v2".to_string()))];

        cache.register_prepare::<Option<String>>(&[], &write_set1, ts(10));
        cache.register_prepare::<Option<String>>(&[], &write_set2, ts(20));

        assert_eq!(cache.writes["x"].len(), 2);

        cache.unregister_prepare::<Option<String>>(&[], &write_set1, ts(10));
        assert_eq!(cache.writes["x"].len(), 1);
        assert!(cache.writes["x"].contains_key(&ts(20)));

        cache.unregister_prepare::<Option<String>>(&[], &write_set2, ts(20));
        assert!(cache.writes.is_empty());
    }

    #[test]
    fn new_with_recovered_state() {
        let state = OccCacheState {
            transactions: vec![OccCachedTransaction {
                read_set: vec![("r".to_string(), ts(5))],
                write_set: vec!["w".to_string()],
                commit_ts: ts(10),
            }],
        };

        let cache: OccCache<String> = OccCache::new(true, Some(state));
        assert!(cache.reads.contains_key("r"));
        assert!(cache.writes.contains_key("w"));
    }

    /// Mock MvccQueries for unit testing OCC checks without a real VlogLsm.
    struct MockMvcc {
        /// (key, version_ts, next_version_ts, last_read_ts)
        versions: Vec<(String, Timestamp, Option<Timestamp>, Option<Timestamp>)>,
        /// (start, end, after_ts, before_ts) → has_writes result
        range_writes: Vec<(String, String, Timestamp, Timestamp)>,
    }

    impl MockMvcc {
        fn new() -> Self {
            Self {
                versions: Vec::new(),
                range_writes: Vec::new(),
            }
        }

        fn add_version(
            &mut self,
            key: &str,
            version_ts: Timestamp,
            next_ts: Option<Timestamp>,
            last_read: Option<Timestamp>,
        ) {
            self.versions
                .push((key.to_string(), version_ts, next_ts, last_read));
        }
    }

    impl MvccQueries<String> for MockMvcc {
        fn get_version_range(
            &self,
            key: &String,
            read_ts: Timestamp,
        ) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
            for (k, ver_ts, next_ts, _) in &self.versions {
                if k == key && *ver_ts <= read_ts {
                    return Ok((*ver_ts, *next_ts));
                }
            }
            Ok((Timestamp::default(), None))
        }

        fn get_last_read_ts(&self, key: &String) -> Result<Option<Timestamp>, StorageError> {
            // Return last_read_ts from the latest version
            for (k, _, _, last_read) in self.versions.iter().rev() {
                if k == key {
                    return Ok(*last_read);
                }
            }
            Ok(None)
        }

        fn get_last_read_ts_at(
            &self,
            key: &String,
            at_ts: Timestamp,
        ) -> Result<Option<Timestamp>, StorageError> {
            for (k, ver_ts, _, last_read) in &self.versions {
                if k == key && *ver_ts <= at_ts {
                    return Ok(*last_read);
                }
            }
            Ok(None)
        }

        fn has_writes_in_range(
            &self,
            start: &String,
            end: &String,
            after_ts: Timestamp,
            before_ts: Timestamp,
        ) -> Result<bool, StorageError> {
            for (s, e, a, b) in &self.range_writes {
                if s == start && e == end && *a == after_ts && *b == before_ts {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        fn get_latest_version_ts(&self, key: &String) -> Result<Timestamp, StorageError> {
            for (k, ver_ts, _, _) in self.versions.iter().rev() {
                if k == key {
                    return Ok(*ver_ts);
                }
            }
            Ok(Timestamp::default())
        }
    }

    #[test]
    fn occ_check_ok_no_conflicts() {
        let cache: OccCache<String> = OccCache::new(true, None);
        let mvcc = MockMvcc::new();

        let result = cache
            .occ_check(
                &mvcc,
                &[("a".to_string(), ts(5))],
                &[("b".to_string(), Some("v".to_string()))],
                &[],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Ok);
    }

    #[test]
    fn occ_check_read_set_fail_overwritten() {
        let cache: OccCache<String> = OccCache::new(true, None);
        let mut mvcc = MockMvcc::new();
        // Key "a" was written at ts=5, then overwritten at ts=8
        mvcc.add_version("a", ts(5), Some(ts(8)), None);

        let result = cache
            .occ_check(
                &mvcc,
                &[("a".to_string(), ts(5))],
                NO_WRITES,
                &[],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Fail);
    }

    #[test]
    fn occ_check_read_set_abstain_prepared_write() {
        let mut cache: OccCache<String> = OccCache::new(true, None);
        let mut mvcc = MockMvcc::new();
        mvcc.add_version("a", ts(5), None, None);

        // Another txn has prepared a write to "a" at ts=7
        cache
            .writes
            .entry("a".to_string())
            .or_default()
            .insert(ts(7), ());

        let result = cache
            .occ_check(
                &mvcc,
                &[("a".to_string(), ts(5))],
                NO_WRITES,
                &[],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Abstain);
    }

    #[test]
    fn occ_check_write_set_retry_last_read() {
        let cache: OccCache<String> = OccCache::new(true, None);
        let mut mvcc = MockMvcc::new();
        // Key "b" was read at ts=15, so writing at ts=10 should Retry
        mvcc.add_version("b", ts(3), None, Some(ts(15)));

        let result = cache
            .occ_check(
                &mvcc,
                &[],
                &[("b".to_string(), Some("v".to_string()))],
                &[],
                ts(10),
            )
            .unwrap();
        assert!(result.is_retry());
    }

    #[test]
    fn occ_check_write_set_abstain_prepared_read() {
        let mut cache: OccCache<String> = OccCache::new(true, None);
        let mvcc = MockMvcc::new();

        // Another txn has prepared a read on "b" at commit_ts=15
        cache
            .reads
            .entry("b".to_string())
            .or_default()
            .insert(ts(15), ());

        let result = cache
            .occ_check(
                &mvcc,
                &[],
                &[("b".to_string(), Some("v".to_string()))],
                &[],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Abstain);
    }

    #[test]
    fn occ_check_scan_set_fail_committed_write() {
        let cache: OccCache<String> = OccCache::new(true, None);
        let mut mvcc = MockMvcc::new();
        // There's a committed write in range ["a","z"] between ts=5 and ts=10
        mvcc.range_writes
            .push(("a".to_string(), "z".to_string(), ts(5), ts(10)));

        let result = cache
            .occ_check(
                &mvcc,
                &[],
                NO_WRITES,
                &[("a".to_string(), "z".to_string(), ts(5))],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Fail);
    }

    #[test]
    fn occ_check_scan_set_abstain_prepared_write() {
        let mut cache: OccCache<String> = OccCache::new(true, None);
        let mvcc = MockMvcc::new();

        // Another txn has prepared a write to "m" at ts=7
        cache
            .writes
            .entry("m".to_string())
            .or_default()
            .insert(ts(7), ());

        let result = cache
            .occ_check(
                &mvcc,
                &[],
                NO_WRITES,
                &[("a".to_string(), "z".to_string(), ts(5))],
                ts(10),
            )
            .unwrap();
        assert_eq!(result, PrepareResult::Abstain);
    }

    #[test]
    fn occ_check_write_set_retry_range_reads() {
        let mut cache: OccCache<String> = OccCache::new(true, None);
        let mvcc = MockMvcc::new();

        // RO scan protected range ["a","z"] at ts=15
        cache
            .range_reads
            .push(("a".to_string(), "z".to_string(), ts(15)));

        let result = cache
            .occ_check(
                &mvcc,
                &[],
                &[("m".to_string(), Some("v".to_string()))],
                &[],
                ts(10),
            )
            .unwrap();
        assert!(result.is_retry());
    }

    /// Shared helper for linearizable vs serializable mode divergence tests.
    /// Runs the same scenario in both modes and asserts mode-specific outcomes.
    fn occ_mode_divergence_test(
        linearizable: bool,
        // Expected results at each divergence point:
        expect_read_fail_vs_ok: PrepareResult<Timestamp>,
        expect_write_latest_retry: PrepareResult<Timestamp>,
        expect_write_last_read_retry: PrepareResult<Timestamp>,
        expect_write_prepared_write_retry: PrepareResult<Timestamp>,
        expect_read_prepared_write_abstain: PrepareResult<Timestamp>,
    ) {
        // Divergence 1: Read-set — overwritten version with commit NOT past end
        // Linearizable: Fail (any overwrite invalidates). Serializable: Ok (commit <= end).
        {
            let cache: OccCache<String> = OccCache::new(linearizable, None);
            let mut mvcc = MockMvcc::new();
            mvcc.add_version("a", ts(5), Some(ts(12)), None);

            let result = cache
                .occ_check(
                    &mvcc,
                    &[("a".to_string(), ts(5))],
                    NO_WRITES,
                    &[],
                    ts(10), // commit=10, end=12, so commit <= end
                )
                .unwrap();
            assert_eq!(result, expect_read_fail_vs_ok);
        }

        // Divergence 2: Write-set — latest version ts > commit_ts
        // Linearizable: Retry. Serializable: Ok (doesn't check latest version).
        {
            let cache: OccCache<String> = OccCache::new(linearizable, None);
            let mut mvcc = MockMvcc::new();
            mvcc.add_version("b", ts(15), None, None);

            let result = cache
                .occ_check(
                    &mvcc,
                    &[],
                    &[("b".to_string(), Some("v".to_string()))],
                    &[],
                    ts(10),
                )
                .unwrap();
            assert_eq!(result, expect_write_latest_retry);
        }

        // Divergence 3: Write-set — last_read_ts check scope
        // Linearizable: uses get_last_read_ts (global). Serializable: uses get_last_read_ts_at.
        // Set up: version at ts=3, last_read_ts=15. Commit at ts=10.
        // Linearizable: get_last_read_ts returns 15 > 10 → Retry.
        // Serializable: get_last_read_ts_at(ts=10) — version at ts=3 has last_read=15 > 10 → Retry.
        // Both should Retry here, but with different internal paths.
        {
            let cache: OccCache<String> = OccCache::new(linearizable, None);
            let mut mvcc = MockMvcc::new();
            mvcc.add_version("c", ts(3), None, Some(ts(15)));

            let result = cache
                .occ_check(
                    &mvcc,
                    &[],
                    &[("c".to_string(), Some("v".to_string()))],
                    &[],
                    ts(10),
                )
                .unwrap();
            assert_eq!(result, expect_write_last_read_retry);
        }

        // Divergence 4: Write-set — prepared write at higher ts
        // Linearizable: Retry (write conflicts with later prepared write).
        // Serializable: Ok (doesn't check prepared writes for write-set).
        {
            let mut cache: OccCache<String> = OccCache::new(linearizable, None);
            let mvcc = MockMvcc::new();
            cache
                .writes
                .entry("d".to_string())
                .or_default()
                .insert(ts(20), ());

            let result = cache
                .occ_check(
                    &mvcc,
                    &[],
                    &[("d".to_string(), Some("v".to_string()))],
                    &[],
                    ts(10),
                )
                .unwrap();
            assert_eq!(result, expect_write_prepared_write_retry);
        }

        // Divergence 5: Read-set — prepared write in range
        // Linearizable uses Unbounded lower bound: any prepared write < commit → Abstain.
        // Serializable uses Excluded(read) lower bound: only writes in (read, commit).
        {
            let mut cache: OccCache<String> = OccCache::new(linearizable, None);
            let mut mvcc = MockMvcc::new();
            mvcc.add_version("e", ts(5), None, None);
            // Prepared write at ts=3 (before read_ts=5)
            cache
                .writes
                .entry("e".to_string())
                .or_default()
                .insert(ts(3), ());

            let result = cache
                .occ_check(
                    &mvcc,
                    &[("e".to_string(), ts(5))],
                    NO_WRITES,
                    &[],
                    ts(10),
                )
                .unwrap();
            assert_eq!(result, expect_read_prepared_write_abstain);
        }
    }

    #[test]
    fn occ_linearizable() {
        occ_mode_divergence_test(
            true,
            PrepareResult::Fail,                     // overwritten → Fail
            PrepareResult::Retry { proposed: 15 },    // latest > commit → Retry
            PrepareResult::Retry { proposed: 15 },    // last_read > commit → Retry
            PrepareResult::Retry { proposed: 20 },    // prepared write > commit → Retry (linearizable)
            PrepareResult::Abstain,                   // prepared write before read → Abstain (Unbounded lower)
        );
    }

    #[test]
    fn occ_serializable() {
        occ_mode_divergence_test(
            false,
            PrepareResult::Ok,                        // commit <= end → Ok
            PrepareResult::Ok,                        // no latest check → Ok
            PrepareResult::Retry { proposed: 15 },    // last_read > commit → Retry (same)
            PrepareResult::Abstain,                   // prepared write → Abstain (write-write conflict)
            PrepareResult::Ok,                        // prepared write at ts=3 < read_ts=5, Excluded(5) skips it → Ok
        );
    }
}
