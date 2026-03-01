use super::{Change, Key, KeyRange, LeaderRecordDelta, ShardNumber, Timestamp, Value, CO, CR, IO, IR, UO, UR};
use super::message::MinPrepareBaselineResult;
use crate::ir::ReplyUnlogged;
use crate::tapir::ShardClient;
use crate::tapirstore::TapirStore;
use crate::{
    DefaultDiskIo, IrClientId, IrMembership, IrMembershipSize, IrOpId, IrRecord, IrReplicaUpcalls,
    MvccBackend, MvccDiskStore,
    OccPrepareResult, OccSharedTransaction, OccTransactionId, TapirTransport,
};
use crate::tapirstore::InMemTapirStore;
use futures::future::join_all;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::marker::PhantomData;
use std::task::Context;
use std::time::Duration;
use std::{future::Future, hash::Hash};
use tracing::{trace, warn};

fn none<T>() -> Option<T> {
    None
}

/// Shard phase controlling which operations are accepted.
///
/// - `ReadWrite`: normal operation, all operations accepted.
/// - `ReadOnly`: blocks `CO::Prepare` (no new writes). Used during
///   resharding drain to let existing prepared transactions resolve.
/// - `Decommissioning`: blocks `CO::Prepare` AND `IO::QuorumRead`/
///   `IO::QuorumScan` (which mutate OCC state via `commit_get`/
///   `commit_scan`). Used to freeze read-protection state before
///   querying `MinPrepareBaseline`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ShardPhase {
    #[default]
    ReadWrite,
    ReadOnly,
    Decommissioning,
}

/// Shard configuration applied via reconfigure→view change.
#[derive(Serialize, Deserialize)]
pub(crate) struct ShardConfig<K> {
    #[serde(default = "none")]
    pub key_range: Option<KeyRange<K>>,
    #[serde(default)]
    pub phase: ShardPhase,
}

/// Diverge from TAPIR and don't maintain a no-vote list. Instead, wait for a
/// view change to syncronize each participant shard's prepare result and then
/// let one or more of many possible backup coordinators take them at face-value.
#[derive(Serialize, Deserialize)]
pub struct Replica<K, V, S = InMemTapirStore<K, V, MvccDiskStore<K, V, Timestamp, DefaultDiskIo>>> {
    #[serde(bound(
        serialize = "K: Serialize + Ord + Hash, V: Serialize, S: Serialize",
        deserialize = "K: Deserialize<'de> + Ord + Hash + Eq, V: Deserialize<'de>, S: Deserialize<'de>"
    ))]
    store: S,
    #[serde(skip, default)]
    _phantom: PhantomData<V>,
    /// If set, reject operations for keys outside this range.
    /// Not persisted: re-applied from view.app_config via apply_config after each view change.
    #[serde(skip_serializing, skip_deserializing, default = "none", bound(deserialize = ""))]
    key_range: Option<KeyRange<K>>,
    #[serde(skip_serializing, skip_deserializing, default, bound(deserialize = ""))]
    phase: ShardPhase,
    /// Runtime metrics counters (not persisted).
    #[serde(skip_serializing, skip_deserializing, default, bound(deserialize = ""))]
    counters: ReplicaCounters,
}

/// Atomic counters for TAPIR transaction metrics, tracked at runtime.
#[derive(Default)]
pub(crate) struct ReplicaCounters {
    pub commit_count: AtomicU64,
    pub abort_count: AtomicU64,
    pub prepare_ok_count: AtomicU64,
    pub prepare_fail_count: AtomicU64,
    pub prepare_retry_count: AtomicU64,
    pub prepare_too_late_count: AtomicU64,
}

impl<K: Key, V: Value, S: TapirStore<K, V>> Replica<K, V, S> {
    pub fn new_with_store(store: S) -> Self {
        Self {
            store,
            _phantom: PhantomData,
            key_range: None,
            phase: ShardPhase::default(),
            counters: ReplicaCounters::default(),
        }
    }
}

impl<K: Key, V: Value, M> Replica<K, V, InMemTapirStore<K, V, M>>
where
    M: MvccBackend<K, V, Timestamp> + Serialize + DeserializeOwned + 'static,
{
    pub fn new(shard: ShardNumber, linearizable: bool) -> Self
    where
        M: Default,
    {
        Self {
            store: InMemTapirStore::new(shard, linearizable),
            _phantom: PhantomData,
            key_range: None,
            phase: ShardPhase::default(),
            counters: ReplicaCounters::default(),
        }
    }

    pub fn new_with_backend(shard: ShardNumber, linearizable: bool, backend: M) -> Self {
        Self {
            store: InMemTapirStore::new_with_backend(shard, linearizable, backend),
            _phantom: PhantomData,
            key_range: None,
            phase: ShardPhase::default(),
            counters: ReplicaCounters::default(),
        }
    }
}

impl<K: Key, V: Value, S: TapirStore<K, V>> Replica<K, V, S> {

    fn recover_coordination<T: TapirTransport<K, V>>(
        transaction_id: OccTransactionId,
        transaction: OccSharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        // TODO: Optimize.
        _membership: IrMembership<T::Address>,
        transport: T,
        mut rng: crate::Rng,
    ) -> impl Future<Output = ()> {
        warn!("trying to recover {transaction_id:?}");

        async move {
            let mut participants = BTreeMap::new();
            let client_id = IrClientId::new(&mut rng);
            for shard in transaction.participants() {
                let membership = transport.shard_addresses(shard).await;
                participants.insert(
                    shard,
                    ShardClient::new(rng.fork(), client_id, shard, membership, transport.clone()),
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
                results: &BTreeMap<A, ReplyUnlogged<UR<K, V>, A>>,
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
                            |results: &BTreeMap<T::Address, ReplyUnlogged<UR<K, V>, T::Address>>,
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

impl<K: Key, V: Value, S: TapirStore<K, V>> IrReplicaUpcalls for Replica<K, V, S> {
    type UO = UO<K>;
    type UR = UR<K, V>;
    type IO = IO<K, V>;
    type IR = IR<K, V>;
    type CO = CO<K, V>;
    type CR = CR;

    fn exec_unlogged(&self, op: Self::UO) -> Self::UR {
        match op {
            UO::Get { key } => {
                if let Some(range) = &self.key_range
                    && !range.contains(&key) {
                        return UR::OutOfRange;
                    }
                let (v, ts) = self.store.do_uncommitted_get(&key);
                UR::Get(v, ts)
            }
            UO::GetAt { key, timestamp } => {
                if let Some(range) = &self.key_range
                    && !range.contains(&key) {
                        return UR::OutOfRange;
                    }
                let (v, ts) = self.store.do_uncommitted_get_at(&key, timestamp);
                UR::GetAt(v, ts)
            }
            UO::Scan {
                start_key,
                end_key,
            } => {
                if let Some(range) = &self.key_range {
                    // start_key must be within [range.start, range.end).
                    if !range.contains(&start_key) {
                        return UR::OutOfRange;
                    }
                    // end_key is the scan's exclusive upper bound — allow
                    // end_key == range.end since scan [start, end) is fully
                    // within [range.start, range.end).
                    if let Some(range_end) = &range.end
                        && &end_key > range_end
                    {
                        return UR::OutOfRange;
                    }
                }
                let ts = Timestamp {
                    time: u64::MAX,
                    client_id: IrClientId(u64::MAX),
                };
                let results = self.store.do_uncommitted_scan(&start_key, &end_key, ts);
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
            UO::ScanAt {
                start_key,
                end_key,
                timestamp,
            } => {
                if let Some(range) = &self.key_range {
                    if !range.contains(&start_key) {
                        return UR::OutOfRange;
                    }
                    if let Some(range_end) = &range.end
                        && &end_key > range_end
                    {
                        return UR::OutOfRange;
                    }
                }
                let results = self.store.do_uncommitted_scan(&start_key, &end_key, timestamp);
                let max_ts = results
                    .iter()
                    .map(|(_, _, t)| *t)
                    .max()
                    .unwrap_or_default();
                let pairs = results
                    .into_iter()
                    .map(|(k, v, _)| (k, v))
                    .collect();
                UR::ScanAt(pairs, max_ts)
            }
            UO::CheckPrepare {
                transaction_id,
                commit,
            } => {
                use crate::tapirstore::CheckPrepareStatus;
                UR::CheckPrepare(match self.store.check_prepare_status(&transaction_id, &commit) {
                    CheckPrepareStatus::CommittedAtTimestamp => OccPrepareResult::Ok,
                    CheckPrepareStatus::CommittedDifferent { .. }
                    | CheckPrepareStatus::Aborted => OccPrepareResult::Fail,
                    CheckPrepareStatus::PreparedAtTimestamp { finalized: true } => OccPrepareResult::Ok,
                    CheckPrepareStatus::PreparedAtTimestamp { finalized: false } => OccPrepareResult::Abstain,
                    CheckPrepareStatus::TooLate => OccPrepareResult::TooLate,
                    CheckPrepareStatus::Unknown => OccPrepareResult::Abstain,
                })
            }
            UO::ReadValidated { key, timestamp } => {
                if let Some(range) = &self.key_range
                    && !range.contains(&key) {
                        return UR::OutOfRange;
                    }
                UR::ReadValidated(self.store.do_uncommitted_get_validated(&key, timestamp))
            }
            UO::ScanValidated {
                start_key,
                end_key,
                snapshot_ts,
            } => {
                if let Some(range) = &self.key_range {
                    if !range.contains(&start_key) {
                        return UR::OutOfRange;
                    }
                    if let Some(range_end) = &range.end
                        && &end_key > range_end
                    {
                        return UR::OutOfRange;
                    }
                }
                UR::ScanValidated(self.store.scan_validated(&start_key, &end_key, snapshot_ts))
            }
            UO::MinPrepareBaseline => {
                if self.phase != ShardPhase::Decommissioning {
                    return UR::MinPrepareBaseline(MinPrepareBaselineResult::NotDecommissioning);
                }
                let (max_rr, max_rc) = self.store.min_prepare_baseline();
                UR::MinPrepareBaseline(MinPrepareBaselineResult::Ok {
                    max_range_read_time: max_rr.map(|ts| ts.time).unwrap_or(0),
                    max_read_commit_time: max_rc.map(|ts| ts.time).unwrap_or(0),
                })
            }
            UO::ScanChanges { from_view } => {
                // effective_end_view = the highest base_view for which this
                // replica has a delta, or None if no deltas exist.
                //
                // A delta keyed by base_view=N contains changes that
                // accumulated DURING view N. Caller advances cursor with
                // `from_view = effective_end_view.unwrap() + 1`.
                //
                // None vs Some(0) distinction is critical: None means "no
                // view changes have happened, no CDC history exists", while
                // Some(0) means "changes committed during view 0 are
                // available". Without Option, both map to 0 and the caller
                // cannot tell whether to scan from 0 (no history) or from
                // 1 (view 0 already consumed).
                let effective_end_view = self.store.cdc_max_view();
                let deltas = self.store.cdc_deltas_from(from_view);
                UR::ScanChanges {
                    deltas,
                    effective_end_view,
                    pending_prepares: self.store.prepared_count(),
                }
            }
        }
    }

    fn exec_inconsistent(&mut self, op: &Self::IO) -> Option<Self::IR> {
        match op {
            IO::Commit {
                transaction_id,
                transaction,
                commit,
            } => {
                self.store.commit_and_log(*transaction_id, transaction, *commit);
                self.counters.commit_count.fetch_add(1, Ordering::Relaxed);
                None
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
                                .store
                                .txn_log_get(transaction_id)
                                .map(|(ts, c)| c && ts == commit)
                                .unwrap_or(false),
                            "{transaction_id:?} committed at {commit:?}"
                        );
                        self.store
                            .prepared_get(transaction_id)
                            .map(|(ts, _, _)| *ts == commit)
                            .unwrap_or(true)
                    })
                    .unwrap_or_else(|| {
                        // Guard: never overwrite a committed transaction.
                        if self
                            .store
                            .txn_log_get(transaction_id)
                            .map(|(_, c)| c)
                            .unwrap_or(false)
                        {
                            return false;
                        }
                        debug_assert!(
                            !self
                                .store
                                .txn_log_get(transaction_id)
                                .map(|(_, c)| c)
                                .unwrap_or(false),
                            "{transaction_id:?} committed"
                        );
                        self.store
                            .txn_log_insert(*transaction_id, Default::default(), false);
                        true
                    })
                {
                    self.store.remove_prepared(*transaction_id);
                }
                self.counters.abort_count.fetch_add(1, Ordering::Relaxed);
                None
            }
            IO::QuorumRead { .. } | IO::QuorumScan { .. }
                if self.phase == ShardPhase::Decommissioning =>
            {
                // Shard is decommissioning — block IO reads that mutate OCC
                // state (commit_get / commit_scan). Returning None causes the
                // IR layer to not send a FinalizeInconsistentReply, so the
                // client waits/retries until redirected after directory swap.
                None
            }
            IO::QuorumRead { key, timestamp } => {
                if let Some(range) = &self.key_range
                    && !range.contains(key) {
                        return Some(IR::OutOfRange);
                    }
                match self.store.do_committed_get(key.clone(), *timestamp) {
                    Ok((value, write_ts)) => Some(IR::QuorumRead(value, write_ts)),
                    Err(_) => Some(IR::PrepareConflict),
                }
            }
            IO::QuorumScan {
                start_key,
                end_key,
                snapshot_ts,
            } => {
                if let Some(range) = &self.key_range {
                    if !range.contains(start_key) {
                        return Some(IR::OutOfRange);
                    }
                    if let Some(range_end) = &range.end
                        && end_key > range_end
                    {
                        return Some(IR::OutOfRange);
                    }
                }
                match self.store.do_committed_scan(start_key.clone(), end_key.clone(), *snapshot_ts) {
                    Ok(results) => Some(IR::QuorumScan(results)),
                    Err(_) => Some(IR::PrepareConflict),
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
                        .shard_read_set(self.store.shard())
                        .all(|(key, _)| range.contains(key));
                    let writes_in_range = transaction
                        .shard_write_set(self.store.shard())
                        .all(|(key, _)| range.contains(key));
                    if !reads_in_range || !writes_in_range {
                        return CR::Prepare(OccPrepareResult::OutOfRange);
                    }
                }
                if self.phase != ShardPhase::ReadWrite {
                    tracing::debug!(
                        shard = ?self.store.shard(),
                        phase = ?self.phase,
                        txn_id = ?transaction_id,
                        "Prepare rejected: shard not in ReadWrite phase"
                    );
                    self.counters.prepare_fail_count.fetch_add(1, Ordering::Relaxed);
                    return CR::Prepare(OccPrepareResult::Fail);
                }
                use crate::tapirstore::CheckPrepareStatus;
                let result = match self.store.check_prepare_status(transaction_id, commit) {
                    CheckPrepareStatus::CommittedAtTimestamp => OccPrepareResult::Ok,
                    CheckPrepareStatus::CommittedDifferent { proposed } => {
                        OccPrepareResult::Retry { proposed }
                    }
                    CheckPrepareStatus::Aborted => OccPrepareResult::Fail,
                    CheckPrepareStatus::PreparedAtTimestamp { .. } => OccPrepareResult::Ok,
                    CheckPrepareStatus::TooLate => {
                        tracing::debug!(
                            shard = ?self.store.shard(),
                            txn_id = ?transaction_id,
                            commit_time = commit.time,
                            "Prepare rejected: TooLate"
                        );
                        OccPrepareResult::TooLate
                    }
                    CheckPrepareStatus::Unknown => {
                        self.store.prepare(*transaction_id, transaction.clone(), *commit, false)
                    }
                };
                match &result {
                    OccPrepareResult::Ok => { self.counters.prepare_ok_count.fetch_add(1, Ordering::Relaxed); }
                    OccPrepareResult::Fail => { self.counters.prepare_fail_count.fetch_add(1, Ordering::Relaxed); }
                    OccPrepareResult::Retry { .. } => { self.counters.prepare_retry_count.fetch_add(1, Ordering::Relaxed); }
                    OccPrepareResult::TooLate => { self.counters.prepare_too_late_count.fetch_add(1, Ordering::Relaxed); }
                    _ => {}
                }
                CR::Prepare(result)
            }
            CO::RaiseMinPrepareTime { time } => {
                CR::RaiseMinPrepareTime {
                    time: self.store.raise_min_prepare_time(*time),
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
                if matches!(res, CR::Prepare(OccPrepareResult::Ok))
                    && self.store.set_prepared_finalized(transaction_id, commit)
                {
                    trace!("confirming prepare {transaction_id:?} at {commit:?}");
                }
            }
            CO::RaiseMinPrepareTime { time } => {
                self.store.finalize_min_prepare_time(*time);
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
                            .store
                            .prepared_get(transaction_id)
                            .map(|(ts, _, _)| ts == commit)
                            .unwrap_or(true)
                            && !self.store.txn_log_contains(transaction_id)
                        {
                            // Enough other replicas agreed to prepare
                            // the transaction so it must be okay.
                            //
                            // Finalize it immediately since we are syncing
                            // from the leader's record.
                            trace!("syncing successful {op_id:?} prepare for {transaction_id:?} at {commit:?} (had {:?})", self.store.prepared_get(transaction_id));
                            self.store.add_prepared(
                                *transaction_id,
                                transaction.clone(),
                                *commit,
                                true,
                            );
                        }
                    } else if self
                        .store
                        .prepared_get(transaction_id)
                        .map(|(ts, _, _)| ts == commit)
                        .unwrap_or(false)
                    {
                        trace!(
                            "syncing {:?} {op_id:?} prepare for {transaction_id:?} at {commit:?}",
                            entry.result
                        );
                        self.store.remove_prepared(*transaction_id);
                    }
                }
                CO::RaiseMinPrepareTime { .. } => {
                    if let CR::RaiseMinPrepareTime { time } = &entry.result {
                        self.store.sync_min_prepare_time(*time);
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
        self.gc_stale_state();
    }

    fn merge(
        &mut self,
        d: BTreeMap<IrOpId, (Self::CO, Self::CR)>,
        u: Vec<(IrOpId, Self::CO, Self::CR)>,
    ) -> BTreeMap<IrOpId, Self::CR> {
        let mut ret: BTreeMap<IrOpId, Self::CR> = BTreeMap::new();

        // Remove inconsistencies caused by out-of-order execution at the leader.
        self.store.reset_min_prepare_time_to_finalized();
        self.store.remove_unfinalized_prepared();

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
        if let Ok(cfg) = serde_json::from_slice::<ShardConfig<K>>(config) {
            if let Some(range) = cfg.key_range {
                self.key_range = Some(range);
            }
            self.phase = cfg.phase.clone();
        } else if let Ok(range) = serde_json::from_slice::<KeyRange<K>>(config) {
            self.key_range = Some(range);
        }
    }

    fn on_install_leader_record_delta(
        &mut self,
        base_view: u64,
        new_view: u64,
        delta: &IrRecord<Self>,
    ) {
        let shard = self.store.shard();
        let mut changes = Vec::new();

        for entry in delta.inconsistent.values() {
            if let IO::Commit {
                transaction_id,
                transaction,
                commit,
            } = &entry.op
            {
                changes.extend(transaction.shard_write_set(shard).map(|(key, value)| Change {
                    transaction_id: *transaction_id,
                    key: key.clone(),
                    value: value.clone(),
                    timestamp: *commit,
                }));
            }
        }

        if !changes.is_empty() {
            self.store.record_cdc_delta(
                base_view,
                LeaderRecordDelta {
                    from_view: base_view,
                    to_view: new_view,
                    changes,
                },
            );
        }
    }

    fn metrics(&self) -> Vec<(&'static str, f64)> {
        vec![
            ("tapirs_prepared_transactions", self.store.prepared_count() as f64),
            ("tapirs_transaction_log_size", self.store.txn_log_len() as f64),
            ("tapirs_commits_total", self.counters.commit_count.load(Ordering::Relaxed) as f64),
            ("tapirs_aborts_total", self.counters.abort_count.load(Ordering::Relaxed) as f64),
            ("tapirs_prepare_ok_total", self.counters.prepare_ok_count.load(Ordering::Relaxed) as f64),
            ("tapirs_prepare_fail_total", self.counters.prepare_fail_count.load(Ordering::Relaxed) as f64),
            ("tapirs_prepare_retry_total", self.counters.prepare_retry_count.load(Ordering::Relaxed) as f64),
            ("tapirs_prepare_too_late_total", self.counters.prepare_too_late_count.load(Ordering::Relaxed) as f64),
        ]
    }
}

impl<K: Key, V: Value, S: TapirStore<K, V>> Replica<K, V, S> {
    fn gc_stale_state(&mut self) {
        // Placeholder for future GC logic (e.g. range_reads cleanup,
        // transaction_log trimming) using finalized_min_prepare_time.
    }

    pub fn tick<T: TapirTransport<K, V>>(
        &self,
        transport: &T,
        membership: &IrMembership<T::Address>,
        rng: &mut crate::Rng,
    ) {
        let prepared_count = self.store.prepared_count();
        if prepared_count > 0 {
            trace!(
                "there are {} prepared transactions",
                prepared_count
            );
        }
        let threshold: u64 = transport.time_offset(-500);
        if let Some((transaction_id, commit, transaction)) =
            self.store.oldest_prepared()
        {
            if commit.time > threshold {
                // Allow the client to finish on its own.
                return;
            }
            let future = Self::recover_coordination(
                transaction_id,
                transaction,
                commit,
                membership.clone(),
                transport.clone(),
                rng.fork(),
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
    use crate::mvcc::disk::{DiskStore, memory_io::MemoryIo};
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
            read_set: BTreeMap::new(),
            write_set: BTreeMap::new(),
            scan_set: Vec::new(),
        })
    }

    fn new_replica(shard: ShardNumber, linearizable: bool) -> Replica<i64, i64> {
        let backend = DiskStore::<i64, i64, Timestamp, MemoryIo>::open(MemoryIo::temp_path()).unwrap();
        Replica::new_with_backend(shard, linearizable, backend)
    }

    #[test]
    fn abort_none_does_not_overwrite_committed() {
        let mut replica = new_replica(ShardNumber(0), false);
        let txn_id = make_txn_id(1, 1);
        let ts = make_ts(10, 1);
        let txn = empty_txn();

        // Commit the transaction.
        replica.exec_inconsistent(&IO::Commit {
            transaction_id: txn_id,
            transaction: txn,
            commit: ts,
        });
        assert_eq!(replica.store.txn_log_get(&txn_id), Some((ts, true)));

        // Abort with commit: None should NOT overwrite the committed entry.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: None,
        });
        assert_eq!(
            replica.store.txn_log_get(&txn_id),
            Some((ts, true)),
            "abort with commit: None must not overwrite a committed transaction"
        );
    }

    #[test]
    fn abort_with_timestamp_skips_different_prepare() {
        let mut replica = new_replica(ShardNumber(0), false);
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
        assert!(replica.store.prepared_get(&txn_id).is_some());

        // Abort at ts2 (different timestamp) should NOT remove the prepare at ts1.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: Some(ts2),
        });
        assert!(
            replica.store.prepared_get(&txn_id).is_some(),
            "abort at different timestamp must not remove the prepared entry"
        );
    }

    #[test]
    fn abort_with_timestamp_removes_matching_prepare() {
        let mut replica = new_replica(ShardNumber(0), false);
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
        assert!(replica.store.prepared_get(&txn_id).is_some());

        // Abort at ts1 (matching timestamp) should remove the prepare.
        replica.exec_inconsistent(&IO::Abort {
            transaction_id: txn_id,
            commit: Some(ts1),
        });
        assert!(
            replica.store.prepared_get(&txn_id).is_none(),
            "abort at matching timestamp must remove the prepared entry"
        );
    }

    #[test]
    fn gc_stale_state_does_not_remove_prepared_entries() {
        let mut replica = new_replica(ShardNumber(0), false);
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
        assert!(replica.store.prepared_get(&txn_id).is_some());

        // gc_stale_state must NOT remove prepared entries — they are resolved
        // exclusively by the coordinator system (IO::Commit / IO::Abort).
        replica.gc_stale_state();
        assert!(
            replica.store.prepared_get(&txn_id).is_some(),
            "prepared entries must not be removed by gc_stale_state"
        );
    }
}
