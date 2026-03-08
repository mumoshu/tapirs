
use super::{
    inmem::record_payload::RecordPayload,
    message::{BootstrapRecord, FinalizeInconsistentReply, LeaderRecordReply, Reconfigure, StatusBroadcast, ViewChangeAddendum},
    payload::IrPayload,
    shared_view::SharedView, AddMember, Confirm, DoViewChange,
    FinalizeConsensus, FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus,
    ProposeInconsistent, Record, RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry,
    RemoveMember, ReplyConsensus, ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView,
    IrRecordStore, RecordView, VersionedEntry, View, ViewNumber,
};
use crate::{Transport, TransportMessage};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{info, trace, trace_span, warn};

#[derive(Debug)]
pub enum Status {
    Normal,
    ViewChanging,
}

impl Status {
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    pub fn is_view_changing(&self) -> bool {
        matches!(self, Self::ViewChanging)
    }
}

pub trait Upcalls: Sized + Send + 'static {
    /// Unlogged operation.
    type UO: TransportMessage;
    /// Unlogged result.
    type UR: TransportMessage;
    /// Inconsistent operation.
    type IO: TransportMessage + Eq;
    /// Inconsistent result (returned from exec_inconsistent at FINALIZE time).
    type IR: TransportMessage;
    /// Consensus operation.
    type CO: TransportMessage + Eq;
    /// Consensus result.
    type CR: TransportMessage + Eq + Ord + Hash;
    /// The record snapshot type used by sync and view-change callbacks.
    type Record: RecordView<IO = Self::IO, CO = Self::CO, CR = Self::CR>;

    fn exec_unlogged(&self, op: Self::UO) -> Self::UR;
    fn exec_inconsistent(&mut self, op_id: &OpId, op: &Self::IO) -> Option<Self::IR>;
    fn exec_consensus(&mut self, op_id: &OpId, op: &Self::CO) -> Self::CR;
    /// Extension to TAPIR: Called when an entry becomes finalized. This
    /// addresses a potential issue with `merge` rolling back finalized
    /// operations. The application assumes responsibility for calling
    /// this during `sync` and, if necessary, `merge`.
    fn finalize_consensus(&mut self, _op_id: &OpId, op: &Self::CO, res: &Self::CR) {
        // No-op.
        let _ = (op, res);
    }
    /// In addition to the IR spec, this must not rely on the existence
    /// of any ancient records (from before the last view change) in the
    /// leader's record.
    fn sync(&mut self, local: &Self::Record, leader: &Self::Record);
    fn merge(
        &mut self,
        d: BTreeMap<OpId, (Self::CO, Self::CR)>,
        u: Vec<(OpId, Self::CO, Self::CR)>,
    ) -> BTreeMap<OpId, Self::CR>;

    /// Called after a view change completes with the current `app_config`.
    /// Default: no-op (static setups ignore config).
    fn apply_config(&mut self, _config: &[u8]) {}

    /// Called during a view change with the delta record — the IR ops that
    /// changed during `base_view` (the view preceding the new view).
    /// `new_view` is the view the shard is transitioning to.
    /// Default: no-op.
    fn on_install_leader_record_delta(
        &mut self,
        _base_view: u64,
        _new_view: u64,
        _delta: &Self::Record,
    ) {
    }

    /// Returns application-level metrics as key-value pairs for Prometheus
    /// exposition. Default: empty (no app metrics).
    fn metrics(&self) -> Vec<(&'static str, f64)> {
        Vec::new()
    }
}

pub struct Replica<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR>> {
    inner: Arc<Inner<U, T, R>>,
}

impl<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR>> Debug for Replica<U, T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Replica");
        if let Ok(sync) = self.inner.sync.try_lock() {
            s.field("stat", &sync.status);
            s.field("view", &sync.view.number);
            s.field("norm", &sync.latest_normal_view);
            s.field("inc", &sync.record.inconsistent_len());
            s.field("con", &sync.record.consensus_len());
        }
        s.finish_non_exhaustive()
    }
}

struct Inner<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR>> {
    transport: T,
    app_tick: Option<fn(&U, &T, &Membership<T::Address>, &mut crate::Rng)>,
    view_change_interval: Duration,
    rng: Mutex<crate::Rng>,
    /// Number of completed view changes (status transitioned to Normal).
    view_change_count: AtomicU64,
    // Single Mutex for all state — faithfully implements the TLA+ single-threaded
    // state machine model. A RwLock<Upcalls> + Mutex<ProtocolState> split was
    // considered to make RequestUnlogged (exec_unlogged(&self)) concurrently
    // runnable via read locks. However, exec_consensus(&mut self) mutates state
    // (prepared list, min_prepare_time), so ProposeConsensus still needs a write
    // lock and blocks Gets. The only gain would be concurrent Gets with each
    // other — Gets are fast (BTreeMap lookup) and with thread-per-core the Mutex
    // is uncontended. Deemed unnecessary complexity; reconsider only if profiling
    // shows contention.
    sync: Mutex<SyncInner<U, T, R>>,
}

struct SyncInner<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR>> {
    status: Status,
    view: SharedView<T::Address>,
    latest_normal_view: SharedView<T::Address>,
    /// View at which the record's base was sealed, for StartView replies.
    record_base_view: Option<SharedView<T::Address>>,
    changed_view_recently: bool,
    upcalls: U,
    record: R,
    outstanding_do_view_changes: BTreeMap<T::Address, DoViewChange<U::IO, U::CO, U::CR, T::Address>>,
    /// Last time received message from each peer replica.
    peer_liveness: BTreeMap<T::Address, Instant>,
    /// Latest normal-view number confirmed by each peer via periodic status broadcasts.
    peer_normal_views: BTreeMap<T::Address, ViewNumber>,
}

impl<U: Upcalls<Record = Record<U>>, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR>> Replica<U, T, R> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(
        rng: crate::Rng,
        membership: Membership<T::Address>,
        upcalls: U,
        transport: T,
        app_tick: Option<fn(&U, &T, &Membership<T::Address>, &mut crate::Rng)>,
    ) -> Self {
        Self::with_view_change_interval(rng, membership, upcalls, transport, app_tick, None)
    }

    pub fn with_view_change_interval(
        rng: crate::Rng,
        membership: Membership<T::Address>,
        upcalls: U,
        transport: T,
        app_tick: Option<fn(&U, &T, &Membership<T::Address>, &mut crate::Rng)>,
        view_change_interval: Option<Duration>,
    ) -> Self {
        let interval = view_change_interval.unwrap_or(Self::VIEW_CHANGE_INTERVAL);
        let view = SharedView::new(View {
            membership,
            number: ViewNumber(0),
            app_config: None,
        });
        let ret = Self {
            inner: Arc::new(Inner {
                transport,
                app_tick,
                view_change_interval: interval,
                rng: Mutex::new(rng),
                view_change_count: AtomicU64::new(0),
                sync: Mutex::new(SyncInner {
                    status: Status::Normal,
                    latest_normal_view: view.clone(),
                    view,
                    changed_view_recently: true,
                    upcalls,
                    record: R::default(),
                    record_base_view: None,
                    outstanding_do_view_changes: BTreeMap::new(),
                    peer_liveness: BTreeMap::new(),
                    peer_normal_views: BTreeMap::new(),
                }),
            }),
        };
        ret.tick();
        ret.tick_app();
        ret
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    pub fn address(&self) -> T::Address {
        self.inner.transport.address()
    }

    fn tick(&self) {
        let view_change_interval = self.inner.view_change_interval;
        let inner = Arc::downgrade(&self.inner);
        T::spawn(async move {
            loop {
                T::sleep(view_change_interval).await;

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let mut sync = inner.sync.lock().unwrap();
                let sync = &mut *sync;

                sync.peer_liveness
                    .retain(|a, _| sync.view.membership.contains(*a));

                sync.peer_normal_views
                    .retain(|a, _| sync.view.membership.contains(*a));

                if sync.status.is_normal() {
                    for address in sync.view.membership.iter() {
                        if address != inner.transport.address() {
                            inner.transport.do_send(
                                address,
                                Message::<U, T>::StatusBroadcast(StatusBroadcast {
                                    latest_normal_view: sync.latest_normal_view.number,
                                }),
                            );
                        }
                    }
                }

                if sync.changed_view_recently {
                    trace!("{:?} skipping view change", inner.transport.address());
                    sync.changed_view_recently = false;
                } else if sync.view.membership.len() == 1 {
                    // Single-replica: skip periodic view change. No peers exist to
                    // exchange DoViewChange messages with, so the view change would
                    // never complete, leaving the replica stuck in ViewChanging state.
                } else {
                    if sync.status.is_normal() {
                        sync.status = Status::ViewChanging;
                    }
                    sync.view.make_mut().number.0 += 1;

                    info!(
                        "{:?} timeout sending do view change {}",
                        inner.transport.address(),
                        sync.view.number.0
                    );

                    Self::broadcast_do_view_change(&inner.transport, &mut *sync);
                }
            }
        });
    }

    fn tick_app(&self) {
        let inner = Arc::downgrade(&self.inner);
        let transport = self.inner.transport.clone();
        T::spawn(async move {
            loop {
                T::sleep(Duration::from_secs(1)).await;

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let mut sync = inner.sync.lock().unwrap();
                let sync = &mut *sync;
                if let Some(tick) = inner.app_tick.as_ref() {
                    let mut rng = inner.rng.lock().unwrap();
                    tick(&sync.upcalls, &transport, &sync.view.membership, &mut rng);
                } else {
                    break;
                }
            }
        });
    }

    fn broadcast_do_view_change(transport: &T, sync: &mut SyncInner<U, T, R>) {
        sync.changed_view_recently = true;
        let destinations = sync
            .view
            .membership
            .iter()
            .chain(sync.latest_normal_view.membership.iter())
            .collect::<BTreeSet<_>>();

        for address in destinations {
            if address == transport.address() {
                continue;
            }
            transport.do_send(
                address,
                Message::<U, T>::DoViewChange(DoViewChange {
                    view: sync.view.clone(),
                    from_client: false,
                    addendum: (address == sync.view.leader()).then(|| {
                        let can_delta = sync.record.has_sealed_view()
                            && sync.latest_normal_view.number.0 + 1 == sync.view.number.0
                            && sync.latest_normal_view.membership.iter()
                                .filter(|a| *a != transport.address())
                                .all(|a| {
                                    sync.peer_normal_views.get(&a)
                                        .is_some_and(|v| *v >= sync.latest_normal_view.number)
                                });
                        let payload = if can_delta {
                            RecordPayload::Delta {
                                base_view: ViewNumber(sync.record.sealed_view_number()),
                                entries: sync.record.current_view_delta(),
                            }
                        } else {
                            RecordPayload::Full(sync.record.full_record())
                        };
                        ViewChangeAddendum {
                            payload,
                            latest_normal_view: sync.latest_normal_view.clone(),
                        }
                    }),
                }),
            )
        }
    }

    pub fn receive(&self, address: T::Address, message: Message<U, T>) -> Option<Message<U, T>> {
        let _span = trace_span!("recv", address = ?self.address()).entered();

        let mut sync = self.inner.sync.lock().unwrap();
        let sync = &mut *sync;

        if sync.view.membership.get_index(address).is_some() {
            sync.peer_liveness.insert(address, Instant::now());
        }

        match message {
            Message::<U, T>::RequestUnlogged(RequestUnlogged { op }) => {
                if sync.status.is_normal() {
                    let result = sync.upcalls.exec_unlogged(op);
                    return Some(Message::<U, T>::ReplyUnlogged(ReplyUnlogged {
                        result,
                        view: sync.view.clone(),
                    }));
                }
            }
            Message::<U, T>::ProposeInconsistent(ProposeInconsistent { op_id, op, recent }) => {
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        warn!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U, T>::ReplyInconsistent(ReplyInconsistent {
                            op_id,
                            view: sync.view.clone(),
                            state: None,
                        }));
                    }

                    let state = match sync.record.entry_inconsistent(op_id) {
                        VersionedEntry::Vacant(vacant) => {
                            vacant.insert(RecordInconsistentEntry {
                                op,
                                state: RecordEntryState::Tentative,
                                modified_view: sync.view.number.0,
                            }).state
                        }
                        VersionedEntry::Occupied(occupied) => {
                            debug_assert_eq!(occupied.op, op);
                            occupied.state
                        }
                    };

                    return Some(Message::<U, T>::ReplyInconsistent(ReplyInconsistent {
                        op_id,
                        view: sync.view.clone(),
                        state: Some(state),
                    }));
                }
            }
            Message::<U, T>::ProposeConsensus(ProposeConsensus { op_id, op, recent }) => {
                if sync.status.is_normal() {
                    if !recent.is_recent_relative_to(sync.view.number) {
                        warn!("ancient relative to {:?}", sync.view.number);
                        return Some(Message::<U, T>::ReplyConsensus(ReplyConsensus {
                            op_id,
                            view: sync.view.clone(),
                            result_state: None,
                        }));
                    }
                    let (result, state) = match sync.record.entry_consensus(op_id) {
                        VersionedEntry::Occupied(entry) => {
                            debug_assert_eq!(entry.op, op);
                            (entry.result.clone(), entry.state)
                        }
                        VersionedEntry::Vacant(vacant) => {
                            let entry = vacant.insert(RecordConsensusEntry {
                                result: sync.upcalls.exec_consensus(&op_id, &op),
                                op,
                                state: RecordEntryState::Tentative,
                                modified_view: sync.view.number.0,
                            });
                            (entry.result.clone(), entry.state)
                        }
                    };

                    return Some(Message::<U, T>::ReplyConsensus(ReplyConsensus {
                        op_id,
                        view: sync.view.clone(),
                        result_state: Some((result, state)),
                    }));
                }
            }
            Message::<U, T>::FinalizeInconsistent(FinalizeInconsistent { op_id }) => {
                if sync.status.is_normal()
                    && let Some(entry) = sync.record.get_mut_inconsistent(&op_id)
                {
                    if entry.state.is_tentative() {
                        entry.state = RecordEntryState::Finalized(sync.view.number);
                        entry.modified_view = sync.view.number.0;
                    }
                    // Execute and reply regardless of whether the entry was
                    // just finalized or was already finalized (e.g. from a
                    // view-change merge). Re-execution is safe: QuorumRead's
                    // commit_get is idempotent, and Commit/Abort return None
                    // (no reply sent either way).
                    let result = sync.upcalls.exec_inconsistent(&op_id, &entry.op);
                    if let Some(result) = result {
                        return Some(Message::<U, T>::FinalizeInconsistentReply(FinalizeInconsistentReply {
                            op_id,
                            result,
                            view: sync.view.clone(),
                        }));
                    }
                }
            }
            Message::<U, T>::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                if sync.status.is_normal()
                    && let Some(entry) = sync.record.get_mut_consensus(&op_id) {
                        // Don't allow a late `FinalizeConsensus` to overwrite
                        // a view change decision.
                        if entry.state.is_tentative() {
                            entry.state = RecordEntryState::Finalized(sync.view.number);
                            entry.result = result;
                            entry.modified_view = sync.view.number.0;
                            sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                        } else if cfg!(debug_assertions) && entry.result != result {
                            // For diagnostic purposes.
                            warn!("tried to finalize consensus with {result:?} when {:?} was already finalized", entry.result);
                        }

                        // Send `Confirm` regardless; the view number gives the
                        // client enough information to retry if needed.
                        return Some(Message::<U, T>::Confirm(Confirm {
                            op_id,
                            view: sync.view.clone(),
                        }));
                    }
            }
            Message::<U, T>::DoViewChange(msg) => {
                if msg.view.number > sync.view.number
                    || (msg.view.number == sync.view.number && sync.status.is_view_changing())
                {
                    if msg.view.number > sync.view.number {
                        sync.view = msg.view.clone();
                        if sync.status.is_normal() {
                            sync.status = Status::ViewChanging;
                        }

                        info!(
                            "{:?} received DoViewChange for view {}, broadcasting to peers",
                            self.inner.transport.address(),
                            sync.view.number.0,
                        );
                        Self::broadcast_do_view_change(
                            &self.inner.transport,
                            &mut *sync,
                        );

                        // Single-replica: no peers to exchange DoViewChange messages with,
                        // so the view change can never complete via quorum. Snap directly
                        // back to Normal with the new view number.
                        if sync.view.membership.len() == 1 {
                            info!(
                                "{:?} single-replica fast-completing view change to view {}",
                                self.inner.transport.address(),
                                sync.view.number.0,
                            );
                            sync.status = Status::Normal;
                            sync.latest_normal_view = sync.view.clone();
                            sync.changed_view_recently = true;
                            return None;
                        }
                    }

                    if self.inner.transport.address() == sync.view.leader() && msg.addendum.is_some() {
                        debug_assert!(!msg.from_client);
                        let msg_view_number = msg.view.number;
                        match sync.outstanding_do_view_changes.entry(address) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(msg);
                            }
                            Entry::Occupied(mut occupied) => {
                                if msg.view.number < occupied.get().view.number {
                                    return None;
                                }
                                occupied.insert(msg);
                            }
                        }

                        let my_address = self.inner.transport.address();
                        let synthetic = DoViewChange {
                            view: sync.view.clone(),
                            from_client: false,
                            addendum: Some(ViewChangeAddendum {
                                payload: if sync.record.has_sealed_view() {
                                    RecordPayload::Delta {
                                        base_view: ViewNumber(sync.record.sealed_view_number()),
                                        entries: sync.record.current_view_delta(),
                                    }
                                } else {
                                    RecordPayload::Full(sync.record.full_record())
                                },
                                latest_normal_view: sync.latest_normal_view.clone(),
                            }),
                        };
                        let matching = sync
                            .outstanding_do_view_changes
                            .iter()
                            .chain(std::iter::once((&my_address, &synthetic)))
                            .filter(|(address, other)|
                                sync.latest_normal_view.membership.contains(**address)
                                && other.view.number == sync.view.number
                            );

                        if matching.clone().count() >= sync.latest_normal_view.membership.size().f_plus_one() {
                            info!("changing to {:?}", msg_view_number);
                            let (old_base_view, delta_entries) = {
                                let latest_normal_view =
                                    matching
                                        .clone()
                                        .map(|(_, r)| {
                                            &r.addendum.as_ref().unwrap().latest_normal_view
                                        })
                                        .chain(std::iter::once(&sync.latest_normal_view))
                                        .max_by_key(|v| v.number)
                                        .unwrap()
                                ;
                                let latest_records = matching
                                    .clone()
                                    .filter(|(_, r)| {
                                        r.addendum.as_ref().unwrap().latest_normal_view.number
                                            == latest_normal_view.number
                                    })
                                    .map(|(_, r)| {
                                        let addendum = r.addendum.as_ref().unwrap();
                                        match &addendum.payload {
                                            RecordPayload::Delta { base_view, .. } => {
                                                let base_ok = sync.record.has_sealed_view()
                                                    && ViewNumber(sync.record.sealed_view_number()) == *base_view;
                                                assert!(
                                                    base_ok,
                                                    "Delta addendum base_view={base_view:?} does not match \
                                                     coordinator record base={:?}; gossip confirmed all \
                                                     members but coordinator lacks base",
                                                    sync.record.has_sealed_view().then(|| ViewNumber(sync.record.sealed_view_number())),
                                                );
                                                let base = if sync.record.has_sealed_view() {
                                                    Some(sync.record.sealed_record())
                                                } else {
                                                    None
                                                };
                                                addendum.payload.clone().resolve(base)
                                            }
                                            RecordPayload::Full(_) => {
                                                addendum.payload.clone().resolve(None)
                                            }
                                        }
                                    })
                                    .collect::<Vec<_>>();

                                if tracing::enabled!(tracing::Level::TRACE) {
                                    let dvc_debug: Vec<_> = sync
                                        .outstanding_do_view_changes
                                        .iter()
                                        .map(|(a, dvt)| (*a, dvt.view.number, dvt.addendum.as_ref().unwrap().latest_normal_view.number))
                                        .chain(
                                            std::iter::once(
                                                (self.inner.transport.address(), sync.view.number, sync.latest_normal_view.number)
                                            )
                                        )
                                        .collect();
                                    trace!(
                                        "have {} latest records ({:?})",
                                        latest_records.len(),
                                        dvc_debug
                                    );
                                }

                                #[allow(non_snake_case)]
                                let mut R = Record::<U>::default();
                                let mut entries_by_opid =
                                    BTreeMap::<OpId, Vec<RecordConsensusEntry<U::CO, U::CR>>>::new();
                                let mut finalized = HashSet::new();
                                for r in latest_records {
                                    for (op_id, entry) in r.inconsistent.clone() {
                                        match R.inconsistent.entry(op_id) {
                                            Entry::Vacant(vacant) => {
                                                // Mark as finalized as `sync` will execute it.
                                                let e = vacant.insert(entry);
                                                e.state = RecordEntryState::Finalized(sync.view.number);
                                                e.modified_view = sync.view.number.0;
                                            }
                                            Entry::Occupied(mut occupied) => {
                                                if let RecordEntryState::Finalized(view) = entry.state {
                                                    let e = occupied.get_mut();
                                                    e.state = RecordEntryState::Finalized(view);
                                                    e.modified_view = view.0;
                                                }
                                            }
                                        }
                                    }
                                    for (op_id, entry) in r.consensus.clone() {
                                        match entry.state {
                                            RecordEntryState::Finalized(_) => {
                                                match R.consensus.entry(op_id) {
                                                    Entry::Vacant(vacant) => {
                                                        sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                                                        vacant.insert(entry);
                                                    }
                                                    Entry::Occupied(mut occupied) => {
                                                        if occupied.get().state.is_tentative() {
                                                            sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                                                            occupied.insert(entry);
                                                        } else {
                                                            debug_assert_eq!(occupied.get().result, entry.result);
                                                        }
                                                    }
                                                }
                                                finalized.insert(op_id);
                                                entries_by_opid.remove(&op_id);
                                            }
                                            RecordEntryState::Tentative => {
                                                if !finalized.contains(&op_id) {
                                                    entries_by_opid
                                                        .entry(op_id)
                                                        .or_default()
                                                        .push(entry);
                                                }
                                            }
                                        }
                                    }
                                }

                                // build d and u
                                let mut d =
                                    BTreeMap::<OpId, (U::CO, U::CR)>::new();
                                let mut u =
                                    Vec::<(OpId, U::CO, U::CR)>::new();

                                for (op_id, entries) in entries_by_opid.clone() {
                                    debug_assert!(!finalized.contains(&op_id));

                                    let mut majority_result_in_d = None;

                                    for entry in &entries {
                                        debug_assert!(entry.state.is_tentative());

                                        let matches = entries
                                            .iter()
                                            .filter(|other| other.result == entry.result)
                                            .count();

                                        if matches
                                            >= sync.latest_normal_view.membership.size().f_over_two_plus_one()
                                        {
                                            majority_result_in_d = Some(entry.result.clone());
                                            break;
                                        }
                                    }

                                    if let Some(majority_result_in_d) = majority_result_in_d {
                                        trace!("merge majority replied {:?} to {op_id:?}", majority_result_in_d);
                                        d.insert(op_id, (entries[0].op.clone(), majority_result_in_d));
                                    } else {
                                        trace!("merge no majority for {op_id:?}; deciding among {:?}", entries.iter().map(|entry| (entry.result.clone(), entry.state)).collect::<Vec<_>>());
                                        u.extend(entries.into_iter().map(|e| (op_id, e.op, e.result)));
                                    }
                                }

                                {
                                    let old_record = sync.record.full_record();
                                    let sync = &mut *sync;
                                    sync.upcalls.sync(&old_record, &R);
                                }

                                let results_by_opid =
                                    sync.upcalls.merge(d, u);

                                debug_assert_eq!(results_by_opid.len(), entries_by_opid.len());

                                for (op_id, result) in results_by_opid {
                                    let entries = entries_by_opid.get(&op_id).unwrap();
                                    let entry = &entries[0];
                                    sync.upcalls.finalize_consensus(&op_id, &entry.op, &result);
                                    R.consensus.insert(
                                        op_id,
                                        RecordConsensusEntry {
                                            op: entry.op.clone(),
                                            result: result.clone(),
                                            state: RecordEntryState::Finalized(sync.view.number),
                                            modified_view: sync.view.number.0,
                                        },
                                    );
                                }

                                // Compute CDC delta and base_view BEFORE storing R.
                                let old_base_view = if sync.record.has_sealed_view() {
                                    Some(ViewNumber(sync.record.sealed_view_number()))
                                } else {
                                    None
                                };
                                let delta_entries = if sync.record.has_sealed_view() {
                                    Some(R.delta_from(sync.record.sealed_record()))
                                } else {
                                    None
                                };

                                // CDC: notify upcalls of the delta record for this view transition.
                                {
                                    let base_view_num = old_base_view.map(|bv| bv.0).unwrap_or(0);
                                    if let Some(ref delta) = delta_entries {
                                        sync.upcalls.on_install_leader_record_delta(
                                            base_view_num, msg_view_number.0, delta,
                                        );
                                    } else {
                                        // First view: entire record is the delta.
                                        sync.upcalls.on_install_leader_record_delta(
                                            0, msg_view_number.0, &R,
                                        );
                                    }
                                }

                                // Store R as new base with empty overlay.
                                sync.record = R::install(R, msg_view_number.0);
                                (old_base_view, delta_entries)
                            };
                            sync.record_base_view = Some(sync.view.clone());
                            sync.changed_view_recently = true;
                            sync.status = Status::Normal;
                            self.inner.view_change_count.fetch_add(1, Ordering::Relaxed);
                            sync.view.make_mut().number = msg_view_number;
                            if let Some(config) = sync.view.app_config.as_ref() {
                                sync.upcalls.apply_config(config);
                            }

                            let destinations = sync
                                .view
                                .membership
                                .iter()
                                .chain(sync.latest_normal_view.membership.iter())
                                .collect::<BTreeSet<_>>();

                            sync.latest_normal_view.make_mut().number = msg_view_number;
                            sync.latest_normal_view.make_mut().membership = sync.view.membership.clone();
    

                            let full_payload = RecordPayload::Full(sync.record.sealed_record().clone());

                            for address in destinations {
                                if address == self.inner.transport.address() {
                                    continue;
                                }
                                let recipient_same_base = old_base_view
                                    .map(|bv| {
                                        sync.outstanding_do_view_changes
                                            .get(&address)
                                            .filter(|dvc| dvc.view.number == sync.view.number)
                                            .and_then(|dvc| dvc.addendum.as_ref())
                                            .map(|a| a.latest_normal_view.number == bv)
                                            .unwrap_or(false)
                                    })
                                    .unwrap_or(false);

                                let payload = if recipient_same_base {
                                    if let Some(delta) = delta_entries.as_ref() {
                                        RecordPayload::Delta {
                                            base_view: old_base_view.unwrap(),
                                            entries: delta.clone(),
                                        }
                                    } else {
                                        full_payload.clone()
                                    }
                                } else {
                                    full_payload.clone()
                                };
                                let sv = StartView {
                                    payload,
                                    view: sync.view.clone(),
                                };
                                self.inner.transport.do_send(address, Message::<U, T>::StartView(sv));
                            }
                            self.inner.transport.on_membership_changed(&sync.view.membership, sync.view.number.0);
                        }
                    }
                } else if !msg.from_client
                    && let Some(ref base_view) = sync.record_base_view
                    && base_view.number >= msg.view.number
                {
                    warn!("{:?} sending leader record to help catch up {address:?}", self.address());
                    let sv = StartView {
                        payload: RecordPayload::Full(sync.record.sealed_record().clone()),
                        view: base_view.clone(),
                    };
                    self.inner.transport.do_send(address, Message::<U, T>::StartView(sv));
                }
            }
            Message::<U, T>::StartView(StartView { payload, view }) => {
                if view.number > sync.view.number
                    || (view.number == sync.view.number && !sync.status.is_normal())
                {
                    info!("starting view {:?} (was {:?} in {:?})", view.number, sync.status, sync.view.number);
                    let base = if sync.record.has_sealed_view() {
                        Some(sync.record.sealed_record())
                    } else {
                        None
                    };
                    // Validate Delta base before resolving — the leader-side
                    // view filter should prevent this, but defend against it.
                    if let RecordPayload::Delta { base_view, .. } = &payload {
                        let base_ok = sync.record.has_sealed_view()
                            && ViewNumber(sync.record.sealed_view_number()) == *base_view;
                        // Should never fire after the leader-side fix. If it does,
                        // it indicates a new bug in the Delta optimization.
                        debug_assert!(base_ok,
                            "Delta StartView with mismatched base: expected base_view={base_view:?}, \
                             record base={:?}",
                            sync.record.has_sealed_view().then(|| ViewNumber(sync.record.sealed_view_number())));
                        if !base_ok {
                            warn!(
                                ?base_view,
                                record_base_view = ?sync.record.has_sealed_view().then(|| ViewNumber(sync.record.sealed_view_number())),
                                "ignoring Delta StartView: base mismatch or missing"
                            );
                            // TODO: Send a NAK back to the leader so it can retry with
                            // Full payload, rather than silently dropping the StartView.
                            // For now, the replica will stay in ViewChanging and the next
                            // tick-driven view change will recover.
                            return None;
                        }
                    }
                    // CDC: record delta so scan_changes quorum reads can find it
                    // regardless of which f+1 replicas respond.
                    //
                    // When this replica receives a Full payload (because its base view
                    // didn't match the leader's Delta base), it computes a **spanning
                    // delta** from its own stale base to the full record. For example,
                    // if this replica was at view 1 and receives the full record at
                    // view 3, it records delta(1→3) covering both view transitions.
                    //
                    // Spanning deltas may overlap with fine-grained deltas on other
                    // replicas (e.g., the view-2 leader has delta(1→2)). The
                    // merge_responses() algorithm in shard_client.rs handles this by
                    // preferring fine-grained deltas when available and falling back to
                    // spanning deltas to fill gaps. Duplicate changes from overlap are
                    // benign — ship_changes creates fresh transaction IDs and the MVCC
                    // store handles idempotent writes at the same timestamp.
                    match &payload {
                        RecordPayload::Delta { base_view, entries } => {
                            sync.upcalls.on_install_leader_record_delta(
                                base_view.0, view.number.0, entries,
                            );
                        }
                        RecordPayload::Full(record) if !sync.record.has_sealed_view() => {
                            // First view: entire record is the delta.
                            sync.upcalls.on_install_leader_record_delta(
                                0, view.number.0, record,
                            );
                        }
                        RecordPayload::Full(record) => {
                            // Spanning delta: base is stale, compute diff to full record.
                            let delta = record.delta_from(sync.record.sealed_record());
                            sync.upcalls.on_install_leader_record_delta(
                                sync.record.sealed_view_number(), view.number.0, &delta,
                            );
                        }
                    }
                    let new_record = payload.resolve(base);
                    {
                        let old_record = sync.record.full_record();
                        sync.upcalls.sync(&old_record, &new_record);
                    }
                    sync.record = R::install(new_record, view.number.0);
                    sync.record_base_view = Some(view.clone());
                    sync.status = Status::Normal;
                    self.inner.view_change_count.fetch_add(1, Ordering::Relaxed);
                    sync.view = view.clone();
                    sync.latest_normal_view = view;
                    if let Some(config) = sync.view.app_config.as_ref() {
                        sync.upcalls.apply_config(config);
                    }
                    sync.changed_view_recently = true;

                    self.inner.transport.on_membership_changed(&sync.view.membership, sync.view.number.0);
                }
            }
            Message::<U, T>::AddMember(AddMember{address}) => {
                if sync.status.is_normal() && sync.view.membership.get_index(address).is_none() {
                    if !sync.view.membership.contains(self.inner.transport.address()) {
                        // TODO: Expand coverage.
                        return None;
                    }
                    info!("adding member {address:?}");

                    sync.status = Status::ViewChanging;
                    sync.view.make_mut().number.0 += 3;

                    // Add the node.
                    let new_members = sync.view.membership
                        .iter()
                        .chain(std::iter::once(address))
                        .collect();
                    sync.view.make_mut().membership = Membership::new(new_members);


                    // Election.
                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            Message::<U, T>::RemoveMember(RemoveMember{address}) => {
                let dominated_by_self = address == self.inner.transport.address();
                let is_normal = sync.status.is_normal();
                let in_membership = sync.view.membership.get_index(address).is_some();
                eprintln!("[ir.remove_member] me={:?} removing={address:?} is_normal={is_normal} in_membership={in_membership} self_skip={dominated_by_self} view={}", self.inner.transport.address(), sync.view.number.0);
                if is_normal && in_membership && sync.view.membership.len() > 1 && !dominated_by_self {
                    if !sync.view.membership.contains(self.inner.transport.address()) {
                        return None;
                    }
                    info!("removing member {address:?}");
                    sync.status = Status::ViewChanging;
                    sync.view.make_mut().number.0 += 3;

                    // Remove the node.
                    let new_members = sync.view.membership
                        .iter()
                        .filter(|a| *a != address)
                        .collect();
                    sync.view.make_mut().membership = Membership::new(new_members);


                    // Election.
                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            Message::<U, T>::Reconfigure(Reconfigure { config }) => {
                if sync.status.is_normal() {
                    info!("reconfiguring with {} bytes", config.len());
                    sync.view.make_mut().app_config = Some(config);
                    if sync.view.membership.len() == 1 {
                        // Single-replica: apply config directly without view change.
                        // No peers to synchronize with, so the DoViewChange exchange
                        // would hang. Bump view number and apply immediately.
                        sync.view.make_mut().number.0 += 3;
                        if let Some(config) = sync.view.app_config.as_ref() {
                            sync.upcalls.apply_config(config);
                        }
                        sync.latest_normal_view = sync.view.clone();
                        sync.changed_view_recently = true;
                    } else {
                        sync.status = Status::ViewChanging;
                        sync.view.make_mut().number.0 += 3;


                        Self::broadcast_do_view_change(&self.inner.transport, sync);
                    }
                }
            }
            Message::<U, T>::FetchLeaderRecord(_) => {
                let (record, view) = if sync.record.has_sealed_view() {
                    (Some(Arc::new(sync.record.sealed_record().clone())), sync.record_base_view.clone())
                } else {
                    (None, None)
                };
                return Some(Message::<U, T>::LeaderRecordReply(LeaderRecordReply { record, view }));
            }
            Message::<U, T>::BootstrapRecord(BootstrapRecord { record, view }) => {
                self.inner.transport.do_send(
                    self.inner.transport.address(),
                    Message::<U, T>::StartView(StartView {
                        payload: RecordPayload::Full(record),
                        view,
                    }),
                );
            }
            Message::<U, T>::StatusBroadcast(StatusBroadcast { latest_normal_view }) => {
                let entry = sync.peer_normal_views.entry(address).or_insert(ViewNumber(0));
                if latest_normal_view > *entry {
                    *entry = latest_normal_view;
                }
            }
            Message::<U, T>::FinalizeInconsistentReply(_) => {
                // Handled by the client via transport.send() future resolution.
            }
            _ => {
                debug_assert!(false);
                warn!("unexpected message");
            }
        }
        None
    }

    /// Force this replica to initiate a view change.
    ///
    /// In IR (and TAPIR), there is no designated leader for normal operations.
    /// Any replica can independently initiate a view change — typically when
    /// its periodic tick detects no recent activity (timeout). This method
    /// simulates that timeout by bumping the view number and broadcasting
    /// `DoViewChange` to all peers, exactly as `tick()` does.
    ///
    /// The replica designated by `view.leader()` only serves as the view
    /// change *coordinator* (collecting addenda and running sync/merge),
    /// NOT as the initiator.
    pub fn force_view_change(&self) {
        let mut sync = self.inner.sync.lock().unwrap();
        if sync.view.membership.len() == 1 {
            // Single-replica: view change without membership change would hang
            // because there are no peers to exchange DoViewChange messages with.
            warn!("{:?} force_view_change skipped: single-replica cluster has no peers",
                self.inner.transport.address());
            return;
        }
        if sync.status.is_normal() {
            sync.status = Status::ViewChanging;
        }
        sync.view.make_mut().number.0 += 1;
        Self::broadcast_do_view_change(&self.inner.transport, &mut *sync);
    }

    /// Current view number of this replica. Advances immediately when
    /// AddMember/RemoveMember/DoViewChange is received, before the view
    /// change completes (i.e., during ViewChanging status).
    pub fn view_number(&self) -> u64 {
        self.inner.sync.lock().unwrap().view.number.0
    }

    /// Snapshot IR-level and application-level metrics for Prometheus exposition.
    ///
    /// Briefly acquires the sync mutex. Returns `None` if the lock is contended
    /// (try_lock fails), which the caller can treat as a scrape miss.
    pub fn collect_metrics(&self) -> Option<ReplicaMetrics> {
        let sync = self.inner.sync.try_lock().ok()?;
        let status = match sync.status {
            Status::Normal => 0,
            Status::ViewChanging => 1,
        };
        let app_metrics = sync.upcalls.metrics();
        Some(ReplicaMetrics {
            status,
            view_number: sync.view.number.0,
            record_inconsistent_len: sync.record.inconsistent_len(),
            record_consensus_len: sync.record.consensus_len(),
            membership_size: sync.view.membership.len(),
            view_change_count: self.inner.view_change_count.load(Ordering::Relaxed),
            app_metrics,
        })
    }
}

/// Point-in-time snapshot of IR replica metrics.
#[derive(Clone)]
pub struct ReplicaMetrics {
    /// Replica status: 0=Normal, 1=ViewChanging, 2=Recovering.
    pub status: u8,
    /// Current view number.
    pub view_number: u64,
    /// Number of inconsistent entries in the IR record.
    pub record_inconsistent_len: usize,
    /// Number of consensus entries in the IR record.
    pub record_consensus_len: usize,
    /// Number of replicas in the current membership.
    pub membership_size: usize,
    /// Total completed view changes since process start.
    pub view_change_count: u64,
    /// Application-level metrics from `Upcalls::metrics()`.
    pub app_metrics: Vec<(&'static str, f64)>,
}
