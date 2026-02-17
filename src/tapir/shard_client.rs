use super::{Key, LeaderRecordDelta, Replica, ShardNumber, Timestamp, Value, CO, CR, IO, IR, UO, UR};
use crate::{
    transport::Transport, IrClient, IrClientId, IrMembership, IrRecord, IrSharedView,
    OccPrepareResult, OccSharedTransaction, OccTransaction, OccTransactionId,
};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

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
        id: IrClientId,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        transport: T,
    ) -> Self {
        let mut inner = IrClient::new(membership, transport);

        // Id of all shard clients must match for the timestamps to match during recovery.
        inner.set_id(id);

        Self { shard, inner }
    }

    pub fn get(
        &self,
        key: K,
        timestamp: Option<Timestamp>,
    ) -> impl Future<Output = (Option<V>, Timestamp)> {
        let future = self.inner.invoke_unlogged(UO::Get { key, timestamp });

        async move {
            let reply = future.await;

            if let UR::Get(value, timestamp) = reply {
                (value, timestamp)
            } else {
                debug_assert!(false);

                // Was valid at the beginning of time (the transaction will
                // abort if that's too old).
                (None, Default::default())
            }
        }
    }

    pub fn scan(
        &self,
        start_key: K,
        end_key: K,
        timestamp: Option<Timestamp>,
    ) -> impl Future<Output = (Vec<(K, Option<V>)>, Timestamp)> + use<'_, K, V, T> {
        let future = self
            .inner
            .invoke_unlogged(UO::Scan { start_key, end_key, timestamp });

        async move {
            match future.await {
                UR::Scan(results, ts) => (results, ts),
                _ => {
                    debug_assert!(false);
                    (Vec::new(), Default::default())
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
    pub fn read_validated(
        &self,
        key: K,
        timestamp: Timestamp,
    ) -> impl Future<Output = Option<(Option<V>, Timestamp)>> {
        let future = self.inner.invoke_unlogged(UO::ReadValidated { key, timestamp });
        async move {
            match future.await {
                UR::ReadValidated(result) => result,
                _ => None,
            }
        }
    }

    /// Quorum read via IR inconsistent op. Sends to all replicas,
    /// waits for f+1 finalize replies, picks highest write_ts.
    pub fn quorum_read(
        &self,
        key: K,
        timestamp: Timestamp,
    ) -> impl Future<Output = (Option<V>, Timestamp)> + Send + use<K, V, T> {
        let future = self
            .inner
            .invoke_inconsistent_with_result(IO::QuorumRead { key, timestamp });
        async move {
            let results = future.await;
            results
                .into_iter()
                .filter_map(|ir| match ir {
                    IR::QuorumRead(value, ts) => Some((value, ts)),
                    _ => None,
                })
                .max_by_key(|(_, ts)| *ts)
                .unwrap_or((None, Timestamp::default()))
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
    ) -> impl Future<Output = Vec<(K, Option<V>, Timestamp)>> + Send + use<K, V, T> {
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
            for ir in results {
                if let IR::QuorumScan(entries) = ir {
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
            }
            merged
                .into_iter()
                .map(|(k, (v, ts))| (k, v, ts))
                .collect()
        }
    }

    /// Request committed changes by view number for CDC-based resharding.
    /// Queries f+1 replicas and merges their leader record deltas.
    pub async fn scan_changes(&self, from_view: u64) -> ScanChangesResult<K, V> {
        let responses = self
            .inner
            .invoke_unlogged_quorum(UO::ScanChanges { from_view })
            .await;

        let mut delta_lists = Vec::new();
        let mut effective_end_view = 0u64;
        let mut pending_prepares = 0usize;

        for r in responses {
            if let UR::ScanChanges {
                deltas,
                effective_end_view: eev,
                pending_prepares: pp,
            } = r
            {
                delta_lists.push((deltas, eev));
                effective_end_view = effective_end_view.max(eev);
                pending_prepares = pending_prepares.max(pp);
            }
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

    pub fn raise_min_prepare_time(&self, time: u64) -> impl Future<Output = u64> + Send {
        let future =
            self.inner
                .invoke_consensus(CO::RaiseMinPrepareTime { time }, |results, size| {
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
    pub effective_end_view: u64,
    pub pending_prepares: usize,
}

/// Merge leader record deltas from multiple replica responses.
/// Picks the most granular (smallest span) delta at each view position,
/// consuming via VecDeque pop_front (moves, no clones).
fn merge_responses<K, V>(
    response_lists: Vec<(Vec<LeaderRecordDelta<K, V>>, u64)>,
) -> Vec<LeaderRecordDelta<K, V>> {
    let mut cursors: Vec<(VecDeque<LeaderRecordDelta<K, V>>, u64)> = response_lists
        .into_iter()
        .map(|(v, eev)| (v.into_iter().collect(), eev))
        .collect();

    let mut result = Vec::new();
    let mut pos = 0u64;

    loop {
        let min_view = cursors
            .iter()
            .filter_map(|(q, _)| q.front().map(|d| d.from_view))
            .filter(|&v| v >= pos)
            .min();
        let Some(view) = min_view else { break };

        let mut best_idx = None;
        let mut best_span = u64::MAX;

        for (i, (q, eev)) in cursors.iter().enumerate() {
            if q.front().map_or(false, |d| d.from_view == view) {
                let inferred_to = q.get(1).map(|d| d.from_view).unwrap_or(*eev + 1);
                let span = inferred_to.saturating_sub(view);
                if span < best_span {
                    best_span = span;
                    best_idx = Some(i);
                }
            }
        }

        let idx = best_idx.unwrap();
        let delta = cursors[idx].0.pop_front().unwrap();
        pos = delta.to_view;

        for (i, (q, _)) in cursors.iter_mut().enumerate() {
            if i != idx {
                while q.front().map_or(false, |d| d.from_view < pos) {
                    q.pop_front();
                }
            }
        }

        result.push(delta);
    }

    result
}
