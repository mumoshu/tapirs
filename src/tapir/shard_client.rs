use super::{Key, LeaderRecordDelta, Replica, ShardNumber, Timestamp, TransactionError, Value, CO, CR, IO, IR, UO, UR};
use tracing::debug;
use super::message::MinPrepareBaselineResult;
use crate::{
    transport::Transport, IrClient, IrClientId, IrMembership, IrRecord, IrSharedView,
    OccPrepareResult, OccSharedTransaction, OccTransaction, OccTransactionId,
};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub struct ShardClient<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    shard: ShardNumber,
    pub(crate) inner: IrClient<Replica<K, V>, T>,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> Clone for ShardClient<K, V, T> {
    fn clone(&self) -> Self {
        Self {
            shard: self.shard,
            inner: self.inner.clone(),
        }
    }
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> ShardClient<K, V, T> {
    pub fn new(
        rng: crate::Rng,
        id: IrClientId,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        transport: T,
    ) -> Self {
        let mut inner = IrClient::new(rng, membership, transport);

        // Id of all shard clients must match for the timestamps to match during recovery.
        inner.set_id(id);

        Self { shard, inner }
    }

    pub fn get(
        &self,
        key: K,
        timestamp: Option<Timestamp>,
    ) -> impl Future<Output = Result<(Option<V>, Timestamp), TransactionError>> {
        let future = self.inner.invoke_unlogged(UO::Get { key, timestamp });

        async move {
            let reply = future.await;

            match reply {
                UR::Get(value, timestamp) => Ok((value, timestamp)),
                UR::OutOfRange => Err(TransactionError::OutOfRange),
                other => {
                    debug_assert!(false, "unexpected UR variant for get: {other:?}");
                    Ok((None, Default::default()))
                }
            }
        }
    }

    pub fn scan(
        &self,
        start_key: K,
        end_key: K,
        timestamp: Option<Timestamp>,
    ) -> impl Future<Output = Result<(Vec<(K, Option<V>)>, Timestamp), TransactionError>> + use<'_, K, V, T> {
        let future = self
            .inner
            .invoke_unlogged(UO::Scan { start_key, end_key, timestamp });

        async move {
            match future.await {
                UR::Scan(results, ts) => Ok((results, ts)),
                UR::OutOfRange => Err(TransactionError::OutOfRange),
                other => {
                    debug_assert!(false, "unexpected UR variant for scan: {other:?}");
                    Ok((Vec::new(), Default::default()))
                }
            }
        }
    }

    pub fn prepare(
        &self,
        transaction_id: OccTransactionId,
        transaction: &OccSharedTransaction<K, V, Timestamp>,
        timestamp: Timestamp,
    ) -> impl Future<Output = OccPrepareResult<Timestamp>> + Send + use<K, V, T> {
        let future = self.inner.invoke_consensus(
            CO::Prepare {
                transaction_id,
                transaction: Arc::clone(transaction),
                commit: timestamp,
            },
            |results, membership_size| {
                let mut ok_count = 0;
                let mut abstain_count = 0;
                let mut timestamp = 0u64;

                for (reply, count) in results {
                    let CR::Prepare(reply) = reply else {
                        debug_assert!(false);
                        continue;
                    };

                    match reply {
                        OccPrepareResult::Ok => {
                            ok_count += count;
                        }
                        OccPrepareResult::Retry { proposed } => {
                            timestamp = timestamp.max(proposed);
                        }
                        OccPrepareResult::Abstain => {
                            abstain_count += count;
                        }
                        OccPrepareResult::Fail => {
                            return CR::Prepare(OccPrepareResult::Fail);
                        }
                        OccPrepareResult::TooLate => {
                            return CR::Prepare(OccPrepareResult::TooLate);
                        }
                        OccPrepareResult::TooOld => {
                            return CR::Prepare(OccPrepareResult::TooOld);
                        }
                        OccPrepareResult::OutOfRange => {
                            return CR::Prepare(OccPrepareResult::OutOfRange);
                        }
                    }
                }

                CR::Prepare(if ok_count >= membership_size.f_plus_one() {
                    OccPrepareResult::Ok
                } else if abstain_count >= membership_size.f_plus_one() {
                    OccPrepareResult::Fail
                } else if timestamp > 0 {
                    OccPrepareResult::Retry {
                        proposed: timestamp,
                    }
                } else {
                    OccPrepareResult::Fail
                })
            },
        );

        async move {
            let reply = future.await;
            if let CR::Prepare(result) = reply {
                result
            } else {
                debug_assert!(false);
                OccPrepareResult::Fail
            }
        }
    }

    pub fn end(
        &self,
        transaction_id: OccTransactionId,
        transaction: &OccSharedTransaction<K, V, Timestamp>,
        prepared_timestamp: Timestamp,
        commit: bool,
    ) -> impl Future<Output = ()> + Send + use<K, V, T> {
        self.inner.invoke_inconsistent(if commit {
            IO::Commit {
                transaction_id,
                transaction: Arc::clone(transaction),
                commit: prepared_timestamp,
            }
        } else {
            IO::Abort {
                transaction_id,
                commit: Some(prepared_timestamp),
            }
        })
    }

    /// Fast-path read: check if one replica has a validated version.
    ///
    /// Returns `Ok(result)` when the replica responds with a valid
    /// `ReadValidated` reply within 2s, `Err(OutOfRange)` if the key
    /// is outside this shard's range, or `Err(Unavailable)` on timeout
    /// or unexpected response type.
    pub fn read_validated(
        &self,
        key: K,
        timestamp: Timestamp,
    ) -> impl Future<Output = Result<Option<(Option<V>, Timestamp)>, TransactionError>> {
        let future = self.inner.invoke_unlogged(UO::ReadValidated { key, timestamp });
        async move {
            let timeout = T::sleep(Duration::from_secs(2));
            let timeout = std::pin::pin!(timeout);
            let future = std::pin::pin!(future);
            let result = futures::future::select(timeout, future).await;
            match &result {
                futures::future::Either::Right((UR::ReadValidated(_), _)) => {
                    debug!("read_validated: got response");
                }
                futures::future::Either::Right((UR::OutOfRange, _)) => {
                    debug!("read_validated: OutOfRange");
                }
                futures::future::Either::Left(_) => {
                    debug!("read_validated: 2s timeout");
                }
                _ => {
                    debug!("read_validated: unexpected UR variant");
                }
            }
            match result {
                futures::future::Either::Right((UR::ReadValidated(r), _)) => Ok(r),
                futures::future::Either::Right((UR::OutOfRange, _)) => {
                    Err(TransactionError::OutOfRange)
                }
                _ => Err(TransactionError::Unavailable),
            }
        }
    }

    /// Quorum read via IR inconsistent op. Sends to all replicas,
    /// waits for f+1 finalize replies, picks highest write_ts.
    pub fn quorum_read(
        &self,
        key: K,
        timestamp: Timestamp,
    ) -> impl Future<Output = Result<(Option<V>, Timestamp), TransactionError>> + Send + use<K, V, T> {
        let future = self
            .inner
            .invoke_inconsistent_with_result(IO::QuorumRead { key, timestamp });
        async move {
            let results = future.await;
            let mut best: Option<(Option<V>, Timestamp)> = None;
            let mut all_out_of_range = true;

            for ir in results {
                match ir {
                    IR::QuorumRead(value, ts) => {
                        all_out_of_range = false;
                        if best.as_ref().is_none_or(|(_, best_ts)| ts > *best_ts) {
                            best = Some((value, ts));
                        }
                    }
                    IR::OutOfRange => {}
                    _ => {
                        all_out_of_range = false;
                    }
                }
            }

            if all_out_of_range {
                return Err(TransactionError::OutOfRange);
            }
            Ok(best.unwrap_or((None, Timestamp::default())))
        }
    }

    /// Read-only scan fast path: send ScanValidated to all replicas,
    /// wait for f+1 replies. If any replica returned `None` (no covering
    /// range_read), returns `None` (fall through to QuorumScan). Otherwise,
    /// merges results: for each key, pick highest `write_ts`.
    pub fn scan_validated(
        &self,
        start_key: K,
        end_key: K,
        snapshot_ts: Timestamp,
    ) -> impl Future<Output = Option<Vec<(K, Option<V>, Timestamp)>>> + Send + use<K, V, T> {
        let future = self.inner.invoke_unlogged_quorum(UO::ScanValidated {
            start_key,
            end_key,
            snapshot_ts,
        });
        async move {
            let results = future.await;
            let mut merged = std::collections::BTreeMap::<K, (Option<V>, Timestamp)>::new();
            for ur in results {
                match ur {
                    UR::ScanValidated(Some(entries)) => {
                        for (key, value, write_ts) in entries {
                            merged
                                .entry(key)
                                .and_modify(|(v, ts)| {
                                    if write_ts > *ts {
                                        *v = value.clone();
                                        *ts = write_ts;
                                    }
                                })
                                .or_insert((value, write_ts));
                        }
                    }
                    UR::ScanValidated(None) => {
                        // No covering range_read at this replica — fast path fails.
                        return None;
                    }
                    _ => {
                        // OutOfRange or unexpected — fast path fails.
                        return None;
                    }
                }
            }
            Some(
                merged
                    .into_iter()
                    .map(|(k, (v, ts))| (k, v, ts))
                    .collect(),
            )
        }
    }

    /// Read-only scan slow path: QuorumScan via IR inconsistent op.
    /// Sends IO::QuorumScan, waits for f+1 finalize replies, merges
    /// results by picking highest `write_ts` per key.
    pub fn quorum_scan(
        &self,
        start_key: K,
        end_key: K,
        snapshot_ts: Timestamp,
    ) -> impl Future<Output = Result<Vec<(K, Option<V>, Timestamp)>, TransactionError>> + Send + use<K, V, T> {
        let future = self
            .inner
            .invoke_inconsistent_with_result(IO::QuorumScan {
                start_key,
                end_key,
                snapshot_ts,
            });
        async move {
            let results = future.await;
            let mut merged = std::collections::BTreeMap::<K, (Option<V>, Timestamp)>::new();
            let mut all_out_of_range = true;

            for ir in results {
                match ir {
                    IR::QuorumScan(entries) => {
                        all_out_of_range = false;
                        for (key, value, write_ts) in entries {
                            merged
                                .entry(key)
                                .and_modify(|(v, ts)| {
                                    if write_ts > *ts {
                                        *v = value.clone();
                                        *ts = write_ts;
                                    }
                                })
                                .or_insert((value, write_ts));
                        }
                    }
                    IR::OutOfRange => {}
                    _ => {
                        all_out_of_range = false;
                    }
                }
            }

            if all_out_of_range {
                return Err(TransactionError::OutOfRange);
            }
            Ok(merged
                .into_iter()
                .map(|(k, (v, ts))| (k, v, ts))
                .collect())
        }
    }

    /// Request committed changes by view number for CDC-based resharding.
    /// Queries f+1 replicas from the same view, merges their CDC deltas,
    /// and returns the combined result.
    ///
    /// # What scan_changes returns
    ///
    /// CDC deltas recorded during past **completed** view changes. Each delta
    /// is the diff between two consecutive view-change-merged states.
    ///
    /// The current view's overlay is **not** captured — all operations since
    /// the last view change started (new Prepares, new Commits, in-flight
    /// transactions) are invisible until the next view change seals them.
    /// In other words, this provides a snapshot of data known to have
    /// committed as of the current view's start.
    ///
    /// # Committed-only guarantee
    ///
    /// CDC deltas contain only **committed** changes — the KV write sets from
    /// transactions whose `IO::Commit` was executed. Prepared-but-not-yet-
    /// committed transactions (`CO::Prepare`) are **not** included.
    ///
    /// This distinction is critical for correctness: TAPIR transactions are
    /// multi-shard. `CO::Prepare` carries the write set; `IO::Commit` only
    /// carries ID+timestamp. If a shard loses a prepared transaction (e.g.,
    /// via backup/restore or resharding that only ships committed data), and
    /// the coordinator later commits it, the write set is lost — violating
    /// the atomicity guarantee.
    ///
    /// # Completeness for migration (split/merge/compact)
    ///
    /// For complete data migration, callers must freeze the shard (reject new
    /// Prepares via `phase=ReadOnly`) and drain all existing prepared
    /// transactions via `recover_coordination()`. Only after `pending_prepares`
    /// reaches 0 and the final view change seals the resolutions does
    /// `scan_changes` capture the complete state. This is Phase 3
    /// freeze+drain in `sharding/shardmanager/cdc.rs`.
    ///
    /// # Backup use case
    ///
    /// For backup (where freezing the shard is not acceptable), `scan_changes`
    /// provides a point-in-time snapshot of data committed as of the current
    /// view start. Changes during the current view are not captured. This is
    /// acceptable because the cluster continues running; these changes will be
    /// sealed by a subsequent view change and appear in the next incremental
    /// backup.
    ///
    /// # No force_view_change() required
    ///
    /// `force_view_change()` is not required before calling `scan_changes()`.
    /// IR replicas trigger natural view changes every ~2s via periodic
    /// `tick()`, so past operations are already in sealed deltas. The shard
    /// manager split/merge/compact operations call `scan_changes(0)` without
    /// `force_view_change()` first, using catch-up loops + freeze+drain for
    /// convergence and completeness.
    pub async fn scan_changes(&self, from_view: u64) -> ScanChangesResult<K, V> {
        let responses = self
            .inner
            .invoke_unlogged_quorum(UO::ScanChanges { from_view })
            .await;

        // Always merge responses from multiple replicas. Not all replicas
        // have CDC deltas for every view transition (replicas that received
        // Full payloads during view change skip delta recording), so merging
        // ensures we combine fine-grained deltas from whichever replica has
        // them. By quorum intersection, at least one replica in any f+1
        // response set has the delta for each view transition.
        let mut delta_lists = Vec::new();
        let mut effective_end_view: Option<u64> = None;
        let mut pending_prepares = usize::MAX;

        for r in responses {
            if let UR::ScanChanges {
                deltas,
                effective_end_view: eev,
                pending_prepares: pp,
            } = r
            {
                delta_lists.push((deltas, eev));
                // Merge: take the max across replicas. If any replica has
                // Some(v), the merged result is Some(max of all v values).
                // None means "no deltas" — only stays None if ALL replicas
                // report None.
                effective_end_view = match (effective_end_view, eev) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
                pending_prepares = pending_prepares.min(pp);
            }
        }

        // If no ScanChanges responses at all, reset pending_prepares to 0.
        if pending_prepares == usize::MAX {
            pending_prepares = 0;
        }

        let deltas = merge_responses(delta_lists);
        ScanChangesResult {
            deltas,
            effective_end_view,
            pending_prepares,
        }
    }

    /// Send a commit for a transaction (used by ShardManager to replicate changes).
    pub fn commit(
        &self,
        transaction_id: OccTransactionId,
        transaction: OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) -> impl Future<Output = ()> + Send + use<K, V, T> {
        self.inner.invoke_inconsistent(IO::Commit {
            transaction_id,
            transaction: Arc::new(transaction),
            commit,
        })
    }

    /// Broadcast a `Reconfigure` to all replicas, triggering a view change
    /// that atomically updates the shard's app_config (e.g. key_range).
    pub fn reconfigure(&self, config: Vec<u8>) {
        self.inner.reconfigure(config);
    }

    pub fn fetch_leader_record(
        &self,
    ) -> impl Future<Output = Option<(IrSharedView<T::Address>, Arc<IrRecord<Replica<K, V>>>)>>
           + Send
           + use<K, V, T>
    {
        self.inner.fetch_leader_record()
    }

    pub fn bootstrap_record(
        &self,
        record: IrRecord<Replica<K, V>>,
        view: IrSharedView<T::Address>,
    ) {
        self.inner.bootstrap_record(record, view);
    }

    pub fn add_member(&self, address: T::Address) {
        self.inner.add_member(address);
    }

    pub fn remove_member(&self, address: T::Address) {
        self.inner.remove_member(address);
    }

    /// Query the min_prepare_time baseline from f+1 replicas.
    /// Returns the element-wise max of `(max_range_read_time, max_read_commit_time)`.
    /// Only counts `Ok` responses toward the f+1 quorum (replicas that haven't
    /// applied the Decommissioning config yet return `NotDecommissioning`).
    pub async fn min_prepare_baseline(&self) -> (u64, u64) {
        let responses = self
            .inner
            .invoke_unlogged_quorum(UO::MinPrepareBaseline)
            .await;

        let mut max_rr = 0u64;
        let mut max_rc = 0u64;
        for r in responses {
            if let UR::MinPrepareBaseline(MinPrepareBaselineResult::Ok {
                max_range_read_time,
                max_read_commit_time,
            }) = r
            {
                max_rr = max_rr.max(max_range_read_time);
                max_rc = max_rc.max(max_read_commit_time);
            }
        }
        (max_rr, max_rc)
    }

    pub fn raise_min_prepare_time(&self, time: u64) -> impl Future<Output = u64> + Send {
        let future =
            self.inner
                .invoke_consensus(CO::RaiseMinPrepareTime { time }, |results, size| {
                    #[allow(clippy::disallowed_methods)] // .iter().filter_map().max() is order-independent
                    let times = results.iter().filter_map(|(r, c)| {
                        if let CR::RaiseMinPrepareTime { time } = r {
                            Some((*time, *c))
                        } else {
                            debug_assert!(false);
                            None
                        }
                    });

                    // Find a time that a quorum of replicas agree on.
                    CR::RaiseMinPrepareTime {
                        time: times
                            .clone()
                            .filter(|&(time, _)| {
                                times
                                    .clone()
                                    .filter(|&(t, _)| t >= time)
                                    .map(|(_, c)| c)
                                    .sum::<usize>()
                                    >= size.f_plus_one()
                            })
                            .map(|(t, _)| t)
                            .max()
                            .unwrap_or_else(|| {
                                debug_assert!(false);
                                0
                            }),
                    }
                });
        async move {
            match future.await {
                CR::RaiseMinPrepareTime { time } => time,
                _ => {
                    debug_assert!(false);
                    0
                }
            }
        }
    }
}

pub struct ScanChangesResult<K, V> {
    pub deltas: Vec<LeaderRecordDelta<K, V>>,
    /// The highest base_view for which any replica returned a delta, or
    /// `None` if no replicas have any deltas (no view changes yet).
    ///
    /// - `None` → no CDC history exists; nothing to consume.
    /// - `Some(N)` → deltas up through base_view=N are available.
    ///   Advance cursor with `from_view = N + 1`.
    pub effective_end_view: Option<u64>,
    pub pending_prepares: usize,
}

/// Merge CDC delta lists from multiple replicas into a single covering sequence.
///
/// Each replica may have a different set of deltas depending on which view changes
/// it participated in. The leader at each view always records a fine-grained delta,
/// while non-leaders that received `RecordPayload::Full` (because their base view
/// didn't match the leader's) record **spanning deltas** — a single delta that covers
/// multiple view transitions (e.g., delta(1→3) when the replica's base was at view 1
/// and it received the full record at view 3, skipping view 2).
///
/// # Spanning delta example
///
/// With 3 replicas and views 1→2→3 where each view change uses a different quorum:
///
/// ```text
/// view 1: r1' (leader), r2 in quorum
/// view 2: r2' (leader), r3 in quorum  (r3 receives Full, base was view 0)
/// view 3: r3' (leader), r1 in quorum  (r1 receives Full, base was view 1)
///
/// r1: delta(0→1), delta(1→3)      ← spanning delta from Full
/// r2: delta(0→1), delta(1→2)
/// r3: delta(0→2), delta(2→3)      ← spanning delta from Full
/// ```
///
/// The algorithm prefers fine-grained deltas when available, but falls back to
/// spanning deltas to fill gaps. Spanning deltas may include duplicate changes
/// (e.g., delta(1→3) includes view 1→2 changes already covered by delta(1→2)),
/// but duplicates are benign — `ship_changes` creates fresh transaction IDs and
/// the MVCC store handles idempotent writes at the same timestamp.
///
/// See `ir/replica.rs` StartView handler for where spanning deltas are created.
fn merge_responses<K, V>(
    response_lists: Vec<(Vec<LeaderRecordDelta<K, V>>, Option<u64>)>,
) -> Vec<LeaderRecordDelta<K, V>> {
    let mut cursors: Vec<(VecDeque<LeaderRecordDelta<K, V>>, u64)> = response_lists
        .into_iter()
        .map(|(v, eev)| (v.into_iter().collect(), eev.unwrap_or(0)))
        .collect();

    let mut result = Vec::new();
    let mut pos = 0u64;

    loop {
        // Try fine-grained deltas first: find the minimum from_view >= pos.
        let min_view = cursors
            .iter()
            .filter_map(|(q, _)| q.front().map(|d| d.from_view))
            .filter(|&v| v >= pos)
            .min();

        let picked_idx = if let Some(view) = min_view {
            // Among cursors with a delta starting at `view`, pick the smallest span.
            let mut best_idx = None;
            let mut best_span = u64::MAX;

            for (i, (q, eev)) in cursors.iter().enumerate() {
                if q.front().is_some_and(|d| d.from_view == view) {
                    let inferred_to = q.get(1).map(|d| d.from_view).unwrap_or(*eev + 1);
                    let span = inferred_to.saturating_sub(view);
                    if span < best_span {
                        best_span = span;
                        best_idx = Some(i);
                    }
                }
            }
            best_idx
        } else {
            // No fine-grained delta starts at or after pos. Fall back to spanning
            // deltas that bridge the gap: from_view < pos but to_view > pos.
            cursors
                .iter()
                .enumerate()
                .filter_map(|(i, (q, _))| {
                    q.front()
                        .filter(|d| d.from_view < pos && d.to_view > pos)
                        .map(|d| (i, d.to_view))
                })
                .max_by_key(|&(_, to_view)| to_view)
                .map(|(i, _)| i)
        };

        let Some(idx) = picked_idx else { break };

        let delta = cursors[idx].0.pop_front().unwrap();
        pos = delta.to_view;

        // Skip deltas on other cursors whose entire range is already covered.
        // Use to_view (not from_view) so that spanning deltas extending past pos
        // are preserved as potential fallback candidates for later iterations.
        for (i, (q, _)) in cursors.iter_mut().enumerate() {
            if i != idx {
                while q.front().is_some_and(|d| d.to_view <= pos) {
                    q.pop_front();
                }
            }
        }

        result.push(delta);
    }

    result
}
