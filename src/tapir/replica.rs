use super::{Change, Key, KeyRange, ShardNumber, Timestamp, Value, CO, CR, IO, UO, UR};
use crate::ir::ReplyUnlogged;
use crate::tapir::ShardClient;
use crate::util::vectorize;
use crate::{
    IrClientId, IrMembership, IrMembershipSize, IrOpId, IrRecord, IrReplicaUpcalls,
    OccPrepareResult, OccSharedTransaction, OccStore, OccTransactionId, TapirTransport,
};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::task::Context;
use std::time::Duration;
use std::{collections::HashMap, future::Future, hash::Hash};
use tracing::{trace, warn};

fn none<T>() -> Option<T> {
    None
}

/// Diverge from TAPIR and don't maintain a no-vote list. Instead, wait for a
/// view change to syncronize each participant shard's prepare result and then
/// let one or more of many possible backup coordinators take them at face-value.
#[derive(Serialize, Deserialize)]
pub struct Replica<K, V> {
    #[serde(bound(
        serialize = "K: Serialize + Ord + Hash, V: Serialize",
        deserialize = "K: Deserialize<'de> + Ord + Hash + Eq, V: Deserialize<'de>"
    ))]
    inner: OccStore<K, V, Timestamp>,
    /// Stores the commit timestamp, read/write sets, and commit status (true if committed) for
    /// all known committed and aborted transactions.
    #[serde(
        with = "vectorize",
        bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de> + Hash + Eq, V: Deserialize<'de>"
        )
    )]
    transaction_log: HashMap<OccTransactionId, (Timestamp, bool)>,
    /// Extension to TAPIR: Garbage collection watermark time.
    /// - All transactions before this are committed/aborted.
    /// - Must not prepare transactions before this.
    /// - May (at any time) garbage collect MVCC versions
    ///   that are invalid at and after this.
    /// - May (at any time) garbage collect keys with
    ///   a tombstone valid at and after this.
    gc_watermark: u64,
    /// Minimum acceptable prepare time (tentative).
    min_prepare_time: u64,
    /// Minimum acceptable prepare time (finalized).
    finalized_min_prepare_time: u64,
    /// Highest timestamp at which all transactions are guaranteed committed or aborted.
    validated_timestamp: u64,
    /// If set, reject operations for keys outside this range.
    /// Not persisted: re-applied from view.app_config via apply_config after each view change.
    #[serde(skip_serializing, skip_deserializing, default = "none", bound(deserialize = ""))]
    key_range: Option<KeyRange<K>>,
}

impl<K: Key, V: Value> Replica<K, V> {
    pub fn new(shard: ShardNumber, linearizable: bool) -> Self {
        Self {
            inner: OccStore::new(shard, linearizable),
            transaction_log: HashMap::new(),
            gc_watermark: 0,
            min_prepare_time: 0,
            finalized_min_prepare_time: 0,
            validated_timestamp: 0,
            key_range: None,
        }
    }

    fn recover_coordination<T: TapirTransport<K, V>>(
        transaction_id: OccTransactionId,
        transaction: OccSharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        // TODO: Optimize.
        _membership: IrMembership<T::Address>,
        transport: T,
    ) -> impl Future<Output = ()> {
        warn!("trying to recover {transaction_id:?}");

        async move {
            let mut participants = HashMap::new();
            let client_id = IrClientId::new();
            for shard in transaction.participants() {
                let membership = transport.shard_addresses(shard).await;
                participants.insert(
                    shard,
                    ShardClient::new(client_id, shard, membership, transport.clone()),
                );
            }

            let min_prepares = join_all(
                participants
                    .values()
                    .map(|client| client.raise_min_prepare_time(commit.time + 1)),
            )
            .await;

            if min_prepares.into_iter().any(|min_prepare_time| {
                if commit.time >= min_prepare_time {
                    // Not ready.
                    return true;
                }

                false
            }) {
                return;
            }

            fn decide<K, V, A>(
                results: &HashMap<A, ReplyUnlogged<UR<K, V>, A>>,
                membership: IrMembershipSize,
            ) -> Option<OccPrepareResult<Timestamp>> {
                let highest_view = results.values().map(|r| r.view.number).max()?;
                Some(
                    if results
                        .values()
                        .any(|r| matches!(r.result, UR::CheckPrepare(OccPrepareResult::Fail)))
                    {
                        OccPrepareResult::Fail
                    } else if results
                        .values()
                        .filter(|r| {
                            r.view.number == highest_view
                                && matches!(r.result, UR::CheckPrepare(OccPrepareResult::Ok))
                        })
                        .count()
                        >= membership.f_plus_one()
                    {
                        OccPrepareResult::Ok
                    } else if results
                        .values()
                        .filter(|r| {
                            r.view.number == highest_view
                                && matches!(r.result, UR::CheckPrepare(OccPrepareResult::TooLate))
                        })
                        .count()
                        >= membership.f_plus_one()
                    {
                        OccPrepareResult::TooLate
                    } else {
                        return None;
                    },
                )
            }

            let results = join_all(participants.values().map(|client| {
                let (future, membership) = client.inner.invoke_unlogged_joined(UO::CheckPrepare {
                    transaction_id,
                    commit,
                });

                async move {
                    let mut timeout = std::pin::pin!(T::sleep(Duration::from_millis(1000)));

                    let results = future
                        .until(
                            |results: &HashMap<T::Address, ReplyUnlogged<UR<K, V>, T::Address>>,
                             cx: &mut Context<'_>| {
                                decide(results, membership).is_some()
                                    || timeout.as_mut().poll(cx).is_ready()
                            },
                        )
                        .await;
                    decide(&results, membership)
                }
            }))
            .await;

            if results.iter().any(|r| r.is_none()) {
                // Try again later.
                return;
            }

            let ok = results
                .iter()
                .all(|r| matches!(r, Some(OccPrepareResult::Ok)));

            trace!("BACKUP COORD got ok={ok} for {transaction_id:?} @ {commit:?}");

            join_all(participants.values().map(|client| {
                let transaction = transaction.clone();
                async move {
                    if ok {
                        client
                            .inner
                            .invoke_inconsistent(IO::Commit {
                                transaction_id,
                                transaction,
                                commit,
                            })
                            .await
                    } else {
                        client
                            .inner
                            .invoke_inconsistent(IO::Abort {
                                transaction_id,
                                commit: Some(commit),
                            })
                            .await
                    }
                }
            }))
            .await;
        }
    }
}

impl<K: Key, V: Value> IrReplicaUpcalls for Replica<K, V> {
    type UO = UO<K>;
    type UR = UR<K, V>;
    type IO = IO<K, V>;
    type CO = CO<K, V>;
    type CR = CR;

    fn exec_unlogged(&self, op: Self::UO) -> Self::UR {
        match op {
            UO::Get { key, timestamp } => {
                if let Some(range) = &self.key_range
                    && !range.contains(&key) {
                        return UR::OutOfRange;
                    }
                let (v, ts) = if let Some(timestamp) = timestamp {
                    self.inner.get_at(&key, timestamp)
                } else {
                    self.inner.get(&key)
                };
                UR::Get(v.cloned(), ts)
            }
            UO::Scan {
                start_key,
                end_key,
                timestamp,
            } => {
                if let Some(range) = &self.key_range
                    && (!range.contains(&start_key) || !range.contains(&end_key)) {
                        return UR::OutOfRange;
                    }
                // When no timestamp is specified, use latest (same semantics as Get).
                let ts = timestamp.unwrap_or({
                    // Use maximum possible timestamp to get latest versions.
                    Timestamp {
                        time: u64::MAX,
                        client_id: IrClientId(u64::MAX),
                    }
                });
                let results = self.inner.scan(&start_key, &end_key, ts);
                let max_ts = results
                    .iter()
                    .map(|(_, _, t)| *t)
                    .max()
                    .unwrap_or_default();
                let pairs = results
                    .into_iter()
                    .map(|(k, v, _)| (k, v))
                    .collect();
                UR::Scan(pairs, max_ts)
            }
            UO::CheckPrepare {
                transaction_id,
                commit,
            } => {
                UR::CheckPrepare(if commit.time < self.gc_watermark {
                    // In theory, could check the other conditions first, but
                    // that might hide bugs.
                    OccPrepareResult::TooOld
                } else if let Some((ts, c)) = self.transaction_log.get(&transaction_id) {
                    if *c && *ts == commit {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else {
                        // Didn't (and will never) commit at this timestamp.
                        OccPrepareResult::Fail
                    }
                } else if let Some(f) = self
                    .inner
                    .prepared
                    .get(&transaction_id)
                    .filter(|(ts, _, _)| *ts == commit)
                    .map(|(_, _, f)| *f)
                {
                    // Already prepared at this timestamp.
                    if f {
                        // Prepare was finalized.
                        OccPrepareResult::Ok
                    } else {
                        // Prepare wasn't finalized, can't be sure yet.
                        OccPrepareResult::Abstain
                    }
                } else if commit.time < self.min_prepare_time
                    || self
                        .inner
                        .prepared
                        .get(&transaction_id)
                        .map(|(c, _, _)| c.time < self.min_prepare_time)
                        .unwrap_or(false)
                {
                    // Too late for the client to prepare.
                    OccPrepareResult::TooLate
                } else {
                    // Not sure.
                    OccPrepareResult::Abstain
                })
            }
            UO::ScanChanges {
                start_ts,
                end_ts_inclusive,
            } => {
                let effective_end = Timestamp {
                    time: end_ts_inclusive.min(self.validated_timestamp),
                    client_id: IrClientId(u64::MAX),
                };
                let start = Timestamp {
                    time: start_ts,
                    client_id: IrClientId(0),
                };
                let raw = self.inner.scan_committed_since(start, effective_end);
                let changes = raw
                    .into_iter()
                    .map(|(key, value, ts)| Change {
                        key,
                        value,
                        timestamp: ts,
                    })
                    .collect();
                UR::ScanChanges {
                    changes,
                    effective_end: effective_end.time,
                }
            }
        }
    }

    fn exec_inconsistent(&mut self, op: &Self::IO) {
        match op {
            IO::Commit {
                transaction_id,
                transaction,
                commit,
            } => {
                let old = self
                    .transaction_log
                    .insert(*transaction_id, (*commit, true));
                if let Some((ts, committed)) = old {
                    debug_assert!(committed, "{transaction_id:?} aborted");
                    debug_assert_eq!(
                        ts, *commit,
                        "{transaction_id:?} committed at (different) {ts:?}"
                    );
                }
                self.inner.commit(*transaction_id, transaction, *commit);
            }
            IO::Abort {
                transaction_id,
                commit,
            } => {
                #[allow(clippy::blocks_in_conditions)]
                if commit
                    .map(|commit| {
                        debug_assert!(
                            !self
                                .transaction_log
                                .get(transaction_id)
                                .map(|(ts, c)| *c && *ts == commit)
                                .unwrap_or(false),
                            "{transaction_id:?} committed at {commit:?}"
                        );
                        self.inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| *ts == commit)
                            .unwrap_or(true)
                    })
                    .unwrap_or_else(|| {
                        // Guard: never overwrite a committed transaction.
                        if self
                            .transaction_log
                            .get(transaction_id)
                            .map(|(_, c)| *c)
                            .unwrap_or(false)
                        {
                            return false;
                        }
                        debug_assert!(
                            !self
                                .transaction_log
                                .get(transaction_id)
                                .map(|(_, c)| *c)
                                .unwrap_or(false),
                            "{transaction_id:?} committed"
                        );
                        self.transaction_log
                            .insert(*transaction_id, (Default::default(), false));
                        true
                    })
                {
                    self.inner.remove_prepared(*transaction_id);
                }
            }
        }
    }

    fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR {
        match op {
            CO::Prepare {
                transaction_id,
                transaction,
                commit,
            } => {
                if let Some(range) = &self.key_range {
                    let reads_in_range = transaction
                        .shard_read_set(self.inner.shard())
                        .all(|(key, _)| range.contains(key));
                    let writes_in_range = transaction
                        .shard_write_set(self.inner.shard())
                        .all(|(key, _)| range.contains(key));
                    if !reads_in_range || !writes_in_range {
                        return CR::Prepare(OccPrepareResult::OutOfRange);
                    }
                }
                CR::Prepare(if commit.time < self.gc_watermark {
                // In theory, could check the other conditions first, but
                // that might hide bugs.
                OccPrepareResult::TooOld
            } else if let Some((ts, c)) = self.transaction_log.get(transaction_id) {
                if *c {
                    if ts == commit {
                        // Already committed at this timestamp.
                        OccPrepareResult::Ok
                    } else {
                        // Committed at a different timestamp.
                        OccPrepareResult::Retry { proposed: ts.time }
                    }
                } else {
                    // Already aborted by client.
                    OccPrepareResult::Fail
                }
            } else if self
                .inner
                .prepared
                .get(transaction_id)
                .map(|(ts, _, _)| *ts == *commit)
                .unwrap_or(false)
            {
                // Already prepared at this timestamp.
                OccPrepareResult::Ok
            } else if commit.time < self.min_prepare_time
                || self
                    .inner
                    .prepared
                    .get(transaction_id)
                    .map(|(c, _, _)| c.time < self.min_prepare_time)
                    .unwrap_or(false)
            {
                // Too late to prepare or reprepare.
                OccPrepareResult::TooLate
            } else {
                self.inner
                    .prepare(*transaction_id, transaction.clone(), *commit, false)
            })
            }
            CO::RaiseMinPrepareTime { time } => {
                // Want to avoid tentative prepare operations materializing later on...
                self.min_prepare_time = self.min_prepare_time.max(
                    (*time).min(
                        self.inner
                            .prepared
                            .values()
                            //.filter(|(_, _, f)| !*f)
                            .map(|(ts, _, _)| ts.time)
                            .min()
                            .unwrap_or(u64::MAX),
                    ),
                );
                CR::RaiseMinPrepareTime {
                    time: self.min_prepare_time,
                }
            }
        }
    }

    fn finalize_consensus(&mut self, op: &Self::CO, res: &Self::CR) {
        match op {
            CO::Prepare {
                transaction_id,
                commit,
                ..
            } => {
                if matches!(res, CR::Prepare(OccPrepareResult::Ok)) && let Some((ts, _, finalized)) = self.inner.prepared.get_mut(transaction_id) && *commit == *ts {
                    trace!("confirming prepare {transaction_id:?} at {commit:?}");
                    *finalized = true;
                }
            }
            CO::RaiseMinPrepareTime { time } => {
                self.finalized_min_prepare_time = self.finalized_min_prepare_time.max(*time);
                self.min_prepare_time = self.min_prepare_time.max(self.finalized_min_prepare_time);
            }
        }
    }

    fn sync(&mut self, local: &IrRecord<Self>, leader: &IrRecord<Self>) {
        for (op_id, entry) in &leader.consensus {
            if local
                .consensus
                .get(op_id)
                .map(|local| local.state.is_finalized() && local.result == entry.result)
                .unwrap_or(false)
            {
                // Record already finalized in local state.
                continue;
            }

            match &entry.op {
                CO::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                } => {
                    // Backup coordinator prepares don't change state.
                    if matches!(entry.result, CR::Prepare(OccPrepareResult::Ok)) {
                        if self
                            .inner
                            .prepared
                            .get(transaction_id)
                            .map(|(ts, _, _)| ts == commit)
                            .unwrap_or(true)
                            && !self.transaction_log.contains_key(transaction_id)
                        {
                            // Enough other replicas agreed to prepare
                            // the transaction so it must be okay.
                            //
                            // Finalize it immediately since we are syncing
                            // from the leader's record.
                            trace!("syncing successful {op_id:?} prepare for {transaction_id:?} at {commit:?} (had {:?})", self.inner.prepared.get(transaction_id));
                            self.inner.add_prepared(
                                *transaction_id,
                                transaction.clone(),
                                *commit,
                                true,
                            );
                        }
                    } else if self
                        .inner
                        .prepared
                        .get(transaction_id)
                        .map(|(ts, _, _)| ts == commit)
                        .unwrap_or(false)
                    {
                        trace!(
                            "syncing {:?} {op_id:?} prepare for {transaction_id:?} at {commit:?}",
                            entry.result
                        );
                        self.inner.remove_prepared(*transaction_id);
                    }
                }
                CO::RaiseMinPrepareTime { .. } => {
                    if let CR::RaiseMinPrepareTime { time } = &entry.result {
                        // Finalized min prepare time is monotonically non-decreasing.
                        self.finalized_min_prepare_time =
                            self.finalized_min_prepare_time.max(*time);
                        // Can rollback tentative prepared time.
                        self.min_prepare_time =
                            self.min_prepare_time.min(self.finalized_min_prepare_time);
                    } else {
                        debug_assert!(false);
                    }
                }
            }
        }
        for (op_id, entry) in &leader.inconsistent {
            if local
                .inconsistent
                .get(op_id)
                .map(|e| e.state.is_finalized())
                .unwrap_or(false)
            {
                // Record already finalized in local state.
                continue;
            }

            trace!("syncing inconsistent {op_id:?} {:?}", entry.op);

            self.exec_inconsistent(&entry.op);
        }
        self.recompute_validated_timestamp();
    }

    fn merge(
        &mut self,
        d: HashMap<IrOpId, (Self::CO, Self::CR)>,
        u: Vec<(IrOpId, Self::CO, Self::CR)>,
    ) -> HashMap<IrOpId, Self::CR> {
        let mut ret: HashMap<IrOpId, Self::CR> = HashMap::new();

        // Remove inconsistencies caused by out-of-order execution at the leader.
        self.min_prepare_time = self.finalized_min_prepare_time;
        for transaction_id in self
            .inner
            .prepared
            .iter()
            .filter(|(_, (_, _, f))| !*f)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
        {
            self.inner.remove_prepared(transaction_id);
        }

        // Preserve any potentially valid fast-path consensus operations.
        for (op_id, (request, reply)) in &d {
            let result = match request {
                CO::Prepare {
                    transaction_id,
                    commit,
                    ..
                } => {
                    let result = if matches!(reply, CR::Prepare(OccPrepareResult::Ok)) {
                        // Possibly successful fast quorum.
                        self.exec_consensus(request)
                    } else {
                        reply.clone()
                    };

                    if &result == reply {
                        trace!("merge preserving {op_id:?} {transaction_id:?} result {result:?} at {commit:?}");
                    } else {
                        trace!("merge changed {op_id:?} {transaction_id:?} at {commit:?} from {reply:?} to {result:?}");
                    }

                    self.finalize_consensus(request, &result);
                    result
                }
                CO::RaiseMinPrepareTime { time } => {
                    let received = if let CR::RaiseMinPrepareTime { time } = reply {
                        *time
                    } else {
                        debug_assert!(false);
                        0
                    };

                    if received >= *time {
                        // Possibly successful fast quorum.
                        let mut result = self.exec_consensus(request);
                        if let CR::RaiseMinPrepareTime { time: new_time } = &mut result {
                            // Don't grant time in excess of the requested time,
                            // which should preserve semantics better if reordered.
                            *new_time = (*new_time).min(*time);
                        }
                        result
                    } else {
                        // Preserve unsuccessful result.
                        reply.clone()
                    }
                }
            };
            ret.insert(*op_id, result);
        }

        // Leader is consistent with a quorum so can decide consensus
        // results.
        for (op_id, request, _) in &u {
            let result = self.exec_consensus(request);
            trace!("merge choosing {result:?} for {op_id:?}");
            ret.insert(*op_id, result);
        }

        ret
    }

    fn apply_config(&mut self, config: &[u8]) {
        if let Ok(range) = serde_json::from_slice::<KeyRange<K>>(config) {
            self.key_range = Some(range);
        }
    }
}

impl<K: Key, V: Value> Replica<K, V> {
    fn recompute_validated_timestamp(&mut self) {
        // GC orphaned prepares below gc_watermark — these can never be
        // resolved (CheckPrepare returns TooOld) and would block
        // validated_timestamp / CDC progress indefinitely.
        if self.gc_watermark > 0 {
            let orphans: Vec<_> = self
                .inner
                .prepared
                .iter()
                .filter(|(_, (ts, _, _))| ts.time < self.gc_watermark)
                .map(|(id, _)| *id)
                .collect();
            for id in orphans {
                self.inner.remove_prepared(id);
            }
        }

        let max_committed = self
            .transaction_log
            .values()
            .filter(|(_, committed)| *committed)
            .map(|(ts, _)| ts.time)
            .max()
            .unwrap_or(0);

        let earliest_pending_prepare = self
            .inner
            .prepared
            .values()
            .map(|(ts, _, _)| ts.time)
            .min();

        self.validated_timestamp = if let Some(earliest) = earliest_pending_prepare {
            max_committed.min(earliest.saturating_sub(1))
        } else {
            max_committed
        };
    }

    pub fn tick<T: TapirTransport<K, V>>(
        &self,
        transport: &T,
        membership: &IrMembership<T::Address>,
    ) {
        if !self.inner.prepared.is_empty() {
            trace!(
                "there are {} prepared transactions",
                self.inner.prepared.len()
            );
        }
        let threshold: u64 = transport.time_offset(-500);
        if let Some((transaction_id, (commit, transaction, _))) =
            self.inner.prepared.iter().min_by_key(|(_, (c, _, _))| *c)
        {
            if commit.time > threshold {
                // Allow the client to finish on its own.
                return;
            }
            let future = Self::recover_coordination(
                *transaction_id,
                transaction.clone(),
                *commit,
                membership.clone(),
                transport.clone(),
            );
            T::spawn(async move {
                futures::pin_mut!(future);
                let timeout = T::sleep(Duration::from_secs(5));
                futures::pin_mut!(timeout);
                let _ = futures::future::select(future, timeout).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ReplicaUpcalls;
    use std::sync::Arc;

    fn make_txn_id(client: u64, num: u64) -> OccTransactionId {
        OccTransactionId {
            client_id: IrClientId(client),
            number: num,
        }
    }

    fn make_ts(time: u64, client: u64) -> Timestamp {
        Timestamp {
            time,
            client_id: IrClientId(client),
        }
    }

    fn empty_txn() -> OccSharedTransaction<i64, i64, Timestamp> {
        Arc::new(crate::OccTransaction {
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            scan_set: Vec::new(),
        })
    }

    #[test]
    fn abort_none_does_not_overwrite_committed() {
        let mut replica = Replica::<i64, i64>::new(ShardNumber(0), false);
        let txn_id = make_txn_id(1, 1);
        let ts = make_ts(10, 1);
        let txn = empty_txn();

        // Commit the transaction.
        replica.exec_inconsistent(&IO::Commit {
            transaction_id: txn_id,
            transaction: txn,
            commit: ts,
        });
        assert_eq!(replica.transaction_log.get(&txn_id), Some(&(ts, true)));

        // Abort with commit: None should NOT overwrite the committed entry.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: None,
        });
        assert_eq!(
            replica.transaction_log.get(&txn_id),
            Some(&(ts, true)),
            "abort with commit: None must not overwrite a committed transaction"
        );
    }

    #[test]
    fn abort_with_timestamp_skips_different_prepare() {
        let mut replica = Replica::<i64, i64>::new(ShardNumber(0), false);
        let txn_id = make_txn_id(1, 1);
        let ts1 = make_ts(10, 1);
        let ts2 = make_ts(20, 1);
        let txn = empty_txn();

        // Prepare at ts1.
        let result = replica.exec_consensus(&CO::Prepare {
            transaction_id: txn_id,
            transaction: txn,
            commit: ts1,
        });
        assert!(
            matches!(result, CR::Prepare(OccPrepareResult::Ok)),
            "prepare should succeed"
        );
        assert!(replica.inner.prepared.contains_key(&txn_id));

        // Abort at ts2 (different timestamp) should NOT remove the prepare at ts1.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: Some(ts2),
        });
        assert!(
            replica.inner.prepared.contains_key(&txn_id),
            "abort at different timestamp must not remove the prepared entry"
        );
    }

    #[test]
    fn abort_with_timestamp_removes_matching_prepare() {
        let mut replica = Replica::<i64, i64>::new(ShardNumber(0), false);
        let txn_id = make_txn_id(1, 1);
        let ts1 = make_ts(10, 1);
        let txn = empty_txn();

        // Prepare at ts1.
        let result = replica.exec_consensus(&CO::Prepare {
            transaction_id: txn_id,
            transaction: txn,
            commit: ts1,
        });
        assert!(
            matches!(result, CR::Prepare(OccPrepareResult::Ok)),
            "prepare should succeed"
        );
        assert!(replica.inner.prepared.contains_key(&txn_id));

        // Abort at ts1 (matching timestamp) should remove the prepare.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: Some(ts1),
        });
        assert!(
            !replica.inner.prepared.contains_key(&txn_id),
            "abort at matching timestamp must remove the prepared entry"
        );
    }

    #[test]
    fn orphaned_prepares_below_gc_watermark_are_cleaned() {
        let mut replica = Replica::<i64, i64>::new(ShardNumber(0), false);
        let txn_id = make_txn_id(1, 1);
        let ts = make_ts(5, 1);
        let txn = empty_txn();

        // Prepare at time=5.
        let result = replica.exec_consensus(&CO::Prepare {
            transaction_id: txn_id,
            transaction: txn,
            commit: ts,
        });
        assert!(
            matches!(result, CR::Prepare(OccPrepareResult::Ok)),
            "prepare should succeed"
        );
        assert!(replica.inner.prepared.contains_key(&txn_id));

        // Advance gc_watermark past the prepare timestamp.
        replica.gc_watermark = 10;

        // recompute_validated_timestamp should GC the orphaned prepare.
        replica.recompute_validated_timestamp();
        assert!(
            !replica.inner.prepared.contains_key(&txn_id),
            "orphaned prepare below gc_watermark should be removed"
        );
    }
}
