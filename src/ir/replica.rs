
use super::{
    ir_record_store::MergeInstallResult,
    message::{BootstrapRecord, FinalizeInconsistentReply, LeaderRecordReply, Reconfigure, StatusBroadcast, ViewChangeAddendum},
    shared_view::SharedView, AddMember, Confirm, DoViewChange,
    FinalizeConsensus, FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus,
    ProposeInconsistent, RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry,
    RemoveMember, ReplyConsensus, ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView,
    IrRecordStore, RecordBuilder, RecordView, View, ViewNumber,
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
    /// The payload type used for view-change messages (StartView, DoViewChange).
    type Payload: TransportMessage;

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
    fn sync<Rec: RecordView<IO = Self::IO, CO = Self::CO, CR = Self::CR>>(
        &mut self,
        local: &Rec,
        leader: &Rec,
    );
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
    fn on_install_leader_record_delta<Rec: RecordView<IO = Self::IO, CO = Self::CO, CR = Self::CR>>(
        &mut self,
        _base_view: u64,
        _new_view: u64,
        _delta: &Rec,
    ) {
    }

    /// Flush durable storage (seal VlogLsm memtables + save manifest).
    fn flush(&mut self);

    /// Returns application-level metrics as key-value pairs for Prometheus
    /// exposition. Default: empty (no app metrics).
    fn metrics(&self) -> Vec<(&'static str, f64)> {
        Vec::new()
    }
}

pub struct Replica<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR, Payload = U::Payload>> {
    inner: Arc<Inner<U, T, R>>,
}

impl<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR, Payload = U::Payload>> Debug for Replica<U, T, R> {
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

struct Inner<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR, Payload = U::Payload>> {
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

struct SyncInner<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR, Payload = U::Payload>> {
    status: Status,
    view: SharedView<T::Address>,
    latest_normal_view: SharedView<T::Address>,
    /// View at which the record's base was sealed, for StartView replies.
    record_base_view: Option<SharedView<T::Address>>,
    changed_view_recently: bool,
    upcalls: U,
    record: R,
    outstanding_do_view_changes: BTreeMap<T::Address, DoViewChange<U::IO, U::CO, U::CR, T::Address, U::Payload>>,
    /// Last time received message from each peer replica.
    peer_liveness: BTreeMap<T::Address, Instant>,
    /// Latest normal-view number confirmed by each peer via periodic status broadcasts.
    peer_normal_views: BTreeMap<T::Address, ViewNumber>,
}

impl<U: Upcalls, T: Transport<U>, R: IrRecordStore<U::IO, U::CO, U::CR, Payload = U::Payload>> Replica<U, T, R> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(
        rng: crate::Rng,
        membership: Membership<T::Address>,
        upcalls: U,
        transport: T,
        app_tick: Option<fn(&U, &T, &Membership<T::Address>, &mut crate::Rng)>,
        record: R,
    ) -> Self {
        Self::with_view_change_interval(rng, membership, upcalls, transport, app_tick, None, record)
    }

    pub fn with_view_change_interval(
        rng: crate::Rng,
        membership: Membership<T::Address>,
        upcalls: U,
        transport: T,
        app_tick: Option<fn(&U, &T, &Membership<T::Address>, &mut crate::Rng)>,
        view_change_interval: Option<Duration>,
        record: R,
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
                    record,
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
                        let peers_confirmed = sync.latest_normal_view.number.0 + 1 == sync.view.number.0
                            && sync.latest_normal_view.membership.iter()
                                .filter(|a| *a != transport.address())
                                .all(|a| {
                                    sync.peer_normal_views.get(&a)
                                        .is_some_and(|v| *v >= sync.latest_normal_view.number)
                                });
                        let payload = if peers_confirmed {
                            sync.record.build_view_change_payload(sync.view.number.0)
                        } else {
                            sync.record.build_full_view_change_payload()
                        };
                        ViewChangeAddendum::new(
                            payload,
                            sync.latest_normal_view.clone(),
                        )
                    }),
                }),
            )
        }
    }

    pub fn receive(&self, address: T::Address, message: Message<U, T>) -> Option<Message<U, T>> {
        let _span = trace_span!("recv", address = ?self.address()).entered();

        let mut sync = self.inner.sync.lock().unwrap_or_else(|e| panic!("ir::Replica sync mutex poisoned — a prior operation panicked while holding the lock. Original panic: {e}"));
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

                    let state = match sync.record.get_inconsistent_entry(&op_id) {
                        Some(existing) => {
                            debug_assert_eq!(existing.op, op);
                            existing.state
                        }
                        None => {
                            let entry = RecordInconsistentEntry {
                                op,
                                state: RecordEntryState::Tentative,
                                modified_view: sync.view.number.0,
                            };
                            let state = entry.state;
                            sync.record.insert_inconsistent_entry(op_id, entry);
                            state
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
                    let (result, state) = match sync.record.get_consensus_entry(&op_id) {
                        Some(existing) => {
                            debug_assert_eq!(existing.op, op);
                            (existing.result.clone(), existing.state)
                        }
                        None => {
                            let entry = RecordConsensusEntry {
                                result: sync.upcalls.exec_consensus(&op_id, &op),
                                op,
                                state: RecordEntryState::Tentative,
                                modified_view: sync.view.number.0,
                            };
                            let result = entry.result.clone();
                            let state = entry.state;
                            sync.record.insert_consensus_entry(op_id, entry);
                            (result, state)
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
                    && let Some(mut entry) = sync.record.get_inconsistent_entry(&op_id)
                {
                    if entry.state.is_tentative() {
                        entry.state = RecordEntryState::Finalized(sync.view.number);
                        entry.modified_view = sync.view.number.0;
                        sync.record.insert_inconsistent_entry(op_id, entry.clone());
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
                    && let Some(mut entry) = sync.record.get_consensus_entry(&op_id) {
                        // Don't allow a late `FinalizeConsensus` to overwrite
                        // a view change decision.
                        if entry.state.is_tentative() {
                            entry.state = RecordEntryState::Finalized(sync.view.number);
                            entry.result = result;
                            entry.modified_view = sync.view.number.0;
                            sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                            sync.record.insert_consensus_entry(op_id, entry);
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
                            addendum: Some(ViewChangeAddendum::new(
                                sync.record.build_view_change_payload(sync.view.number.0),
                                sync.latest_normal_view.clone(),
                            )),
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
                            let merge_result = {
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
                                // Reject if leader is behind the majority. A leader whose
                                // latest_normal_view is behind has stale sealed segments and
                                // cannot correctly compute the delta. f+1 eligible replicas
                                // are caught up — the view change tick will pick one.
                                if sync.latest_normal_view.number < latest_normal_view.number {
                                    warn!(
                                        "leader behind majority: own={:?} majority={:?}, aborting merge",
                                        sync.latest_normal_view.number, latest_normal_view.number
                                    );
                                    return None;
                                }
                                let latest_records = matching
                                    .clone()
                                    .filter(|(_, r)| {
                                        r.addendum.as_ref().unwrap().latest_normal_view.number
                                            == latest_normal_view.number
                                    })
                                    .map(|(_, r)| {
                                        let addendum = r.addendum.as_ref().unwrap();
                                        sync.record.resolve_do_view_change_payload(&addendum.payload)
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
                                let mut R = R::Record::default();
                                let mut entries_by_opid =
                                    BTreeMap::<OpId, Vec<RecordConsensusEntry<U::CO, U::CR>>>::new();
                                let mut finalized = HashSet::new();
                                for r in latest_records {
                                    for (op_id, entry) in r.inconsistent_entries() {
                                        if let Some(existing) = R.get_inconsistent(&op_id) {
                                            // Already in R — only update if incoming has a
                                            // higher-view finalization (never downgrade).
                                            if let RecordEntryState::Finalized(view) = entry.state {
                                                let dominated = match existing.state {
                                                    RecordEntryState::Tentative => true,
                                                    RecordEntryState::Finalized(ev) => view > ev,
                                                };
                                                if dominated {
                                                    let mut e = existing;
                                                    e.state = RecordEntryState::Finalized(view);
                                                    e.modified_view = view.0;
                                                    R.insert_inconsistent(op_id, e);
                                                }
                                            }
                                        } else {
                                            // Mark as finalized as `sync` will execute it.
                                            let mut e = entry;
                                            e.state = RecordEntryState::Finalized(sync.view.number);
                                            e.modified_view = sync.view.number.0;
                                            R.insert_inconsistent(op_id, e);
                                        }
                                    }
                                    for (op_id, entry) in r.consensus_entries() {
                                        match entry.state {
                                            RecordEntryState::Finalized(_) => {
                                                if let Some(existing) = R.get_consensus(&op_id) {
                                                    if existing.state.is_tentative() {
                                                        sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                                                        R.insert_consensus(op_id, entry);
                                                    } else {
                                                        debug_assert_eq!(existing.result, entry.result);
                                                    }
                                                } else {
                                                    sync.upcalls.finalize_consensus(&op_id, &entry.op, &entry.result);
                                                    R.insert_consensus(op_id, entry);
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

                                // Extract best DoViewChange payload (first replica with highest LNV).
                                // Clone it before the mutable sync borrows below.
                                let best_payload: Option<U::Payload> = matching
                                    .clone()
                                    .filter(|(_, r)| {
                                        r.addendum.as_ref().unwrap().latest_normal_view.number
                                            == latest_normal_view.number
                                    })
                                    .map(|(_, r)| r.addendum.as_ref().unwrap().payload.clone())
                                    .next();

                                {
                                    let old_record = sync.record.full_record();
                                    let sync = &mut *sync;
                                    sync.upcalls.sync(&old_record, &R);
                                }

                                let results_by_opid =
                                    sync.upcalls.merge(d, u);

                                debug_assert_eq!(results_by_opid.len(), entries_by_opid.len());

                                let mut resolved_ops = BTreeSet::<OpId>::new();
                                for (op_id, result) in results_by_opid {
                                    resolved_ops.insert(op_id);
                                    let entries = entries_by_opid.get(&op_id).unwrap();
                                    let entry = &entries[0];
                                    sync.upcalls.finalize_consensus(&op_id, &entry.op, &result);
                                    R.insert_consensus(
                                        op_id,
                                        RecordConsensusEntry {
                                            op: entry.op.clone(),
                                            result: result.clone(),
                                            state: RecordEntryState::Finalized(sync.view.number),
                                            modified_view: sync.view.number.0,
                                        },
                                    );
                                }

                                // Install merged record — imports raw segments from best payload,
                                // persists only the resolved delta as a new sealed segment.
                                let merge_result: MergeInstallResult<R::Record, U::Payload> =
                                    sync.record.install_merged_record(R, msg_view_number.0, best_payload.as_ref(), &resolved_ops);
                                let (from_view, ref changes) = merge_result.transition;
                                sync.upcalls.on_install_leader_record_delta(from_view, msg_view_number.0, changes);

                                merge_result
                            };
                            let merge_wall = std::time::Instant::now();
                            // Persist resolved state before transitioning to Normal.
                            sync.record.flush();
                            sync.upcalls.flush();
                            let leader_addr = self.inner.transport.address();
                            tracing::debug!("[ir-leader-{leader_addr}] merged view {:?} has_delta={} prev_base={:?}",
                                msg_view_number, merge_result.start_view_delta.is_some(), merge_result.previous_base_view);
                            let _ = merge_wall;

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

                            let full_payload = sync.record.build_start_view_payload(None);

                            for address in destinations {
                                if address == self.inner.transport.address() {
                                    continue;
                                }
                                let (recipient_same_base, delta_reason) = merge_result.previous_base_view
                                    .map(|bv| {
                                        let dvc = sync.outstanding_do_view_changes.get(&address);
                                        let dvc_view_match = dvc.map(|d| d.view.number == sync.view.number).unwrap_or(false);
                                        let addendum = dvc.and_then(|d| d.addendum.as_ref());
                                        let lnv = addendum.map(|a| a.latest_normal_view.number);
                                        let matches = lnv.map(|n| n == bv).unwrap_or(false) && dvc_view_match;
                                        (matches, format!("bv={bv:?} dvc_view_match={dvc_view_match} lnv={lnv:?}"))
                                    })
                                    .unwrap_or((false, "no_prev_base".to_string()));

                                let payload = if recipient_same_base {
                                    sync.record.build_start_view_payload(merge_result.start_view_delta.as_ref())
                                } else {
                                    full_payload.clone()
                                };
                                tracing::debug!("[ir-leader-{leader_addr}] → {address:?} view {:?} delta={recipient_same_base} {delta_reason}",
                                    msg_view_number);
                                let sv = StartView::new(
                                    payload,
                                    sync.view.clone(),
                                );
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
                    let sv = StartView::new(
                        sync.record.build_start_view_payload(None),
                        base_view.clone(),
                    );
                    self.inner.transport.do_send(address, Message::<U, T>::StartView(sv));
                }
            }
            Message::<U, T>::StartView(StartView { payload, view, .. }) => {
                if view.number > sync.view.number
                    || (view.number == sync.view.number && !sync.status.is_normal())
                {
                    let sv_wall = std::time::Instant::now();
                    let sv_addr = self.inner.transport.address();
                    tracing::debug!("[ir-replica-{sv_addr}] starting view {:?} (was {:?} in {:?})",
                        view.number, sync.status, sync.view.number);
                    let Some(result) = sync.record.install_start_view_payload(payload, view.number.0) else {
                        tracing::debug!("[ir-replica-{sv_addr}] REJECTED StartView v={:?}: payload validation failed (delta base mismatch)",
                            view.number);
                        debug_assert!(false, "StartView payload validation failed (delta base mismatch)");
                        warn!("ignoring StartView: payload validation failed (delta base mismatch)");
                        return None;
                    };
                    let install_ms = sv_wall.elapsed().as_millis();
                    let (from_view, ref changes) = result.transition;
                    sync.upcalls.on_install_leader_record_delta(from_view, view.number.0, changes);
                    sync.upcalls.sync(&result.previous_record, &result.new_record);
                    let sync_ms = sv_wall.elapsed().as_millis();
                    // Persist resolved state before transitioning to Normal.
                    sync.record.flush();
                    let rec_flush_ms = sv_wall.elapsed().as_millis();
                    sync.upcalls.flush();
                    let upcalls_flush_ms = sv_wall.elapsed().as_millis();
                    if upcalls_flush_ms > 10 {
                        tracing::debug!("[ir-replica-{sv_addr}] view {:?} wall: install={}ms sync={}ms rec_flush={}ms upcalls_flush={}ms",
                            view.number, install_ms, sync_ms - install_ms, rec_flush_ms - sync_ms, upcalls_flush_ms - rec_flush_ms);
                    }

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
                tracing::debug!("[ir.remove_member] me={:?} removing={address:?} is_normal={is_normal} in_membership={in_membership} self_skip={dominated_by_self} view={}", self.inner.transport.address(), sync.view.number.0);
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
                    tracing::debug!("[ir-replica-{}] reconfiguring with {} bytes (view={:?})",
                        self.inner.transport.address(), config.len(), sync.view.number);
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
                let (payload, view) = match sync.record.checkpoint_record() {
                    Some(r) => (Some(R::make_full_payload(r)), sync.record_base_view.clone()),
                    None => (None, None),
                };
                return Some(Message::<U, T>::LeaderRecordReply(LeaderRecordReply { payload, view }));
            }
            Message::<U, T>::BootstrapRecord(BootstrapRecord { payload, view }) => {
                self.inner.transport.do_send(
                    self.inner.transport.address(),
                    Message::<U, T>::StartView(StartView::new(
                        payload,
                        view,
                    )),
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
        let mut sync = self.inner.sync.lock().unwrap_or_else(|e| panic!("ir::Replica sync mutex poisoned — a prior operation panicked while holding the lock. Original panic: {e}"));
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
        self.inner.sync.lock().unwrap_or_else(|e| panic!("ir::Replica sync mutex poisoned — a prior operation panicked while holding the lock. Original panic: {e}")).view.number.0
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
            record_stored_bytes: sync.record.stored_bytes(),
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
    /// Total bytes across IR record VlogLsm segments. None for in-memory backends.
    pub record_stored_bytes: Option<u64>,
    /// Application-level metrics from `Upcalls::metrics()`.
    pub app_metrics: Vec<(&'static str, f64)>,
}
