use super::{
    message::{BootstrapRecord, LeaderRecord, LeaderRecordReply, RecordPayload, Reconfigure, ViewChangeAddendum},
    shared_view::SharedView, AddMember, Confirm, DoViewChange,
    FinalizeConsensus, FinalizeInconsistent, Membership, Message, OpId, ProposeConsensus,
    ProposeInconsistent, Record, RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry,
    RemoveMember, ReplyConsensus, ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView,
    View, ViewNumber,
};
use crate::{Transport, TransportMessage};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{info, trace, trace_span, warn};

#[derive(Debug)]
pub enum Status {
    Normal,
    ViewChanging,
    Recovering,
}

impl Status {
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    pub fn is_view_changing(&self) -> bool {
        matches!(self, Self::ViewChanging)
    }
}

pub trait Upcalls: Sized + Send + Serialize + DeserializeOwned + 'static {
    /// Unlogged operation.
    type UO: TransportMessage;
    /// Unlogged result.
    type UR: TransportMessage;
    /// Inconsistent operation.
    type IO: TransportMessage + Eq;
    /// Consensus operation.
    type CO: TransportMessage + Eq;
    /// Consensus result.
    type CR: TransportMessage + Eq + Hash;

    fn exec_unlogged(&self, op: Self::UO) -> Self::UR;
    fn exec_inconsistent(&mut self, op: &Self::IO);
    fn exec_consensus(&mut self, op: &Self::CO) -> Self::CR;
    /// Extension to TAPIR: Called when an entry becomes finalized. This
    /// addresses a potential issue with `merge` rolling back finalized
    /// operations. The application assumes responsibility for calling
    /// this during `sync` and, if necessary, `merge`.
    fn finalize_consensus(&mut self, op: &Self::CO, res: &Self::CR) {
        // No-op.
        let _ = (op, res);
    }
    /// In addition to the IR spec, this must not rely on the existence
    /// of any ancient records (from before the last view change) in the
    /// leader's record.
    fn sync(&mut self, local: &Record<Self>, leader: &Record<Self>);
    fn merge(
        &mut self,
        d: HashMap<OpId, (Self::CO, Self::CR)>,
        u: Vec<(OpId, Self::CO, Self::CR)>,
    ) -> HashMap<OpId, Self::CR>;

    /// Called after a view change completes with the current `app_config`.
    /// Default: no-op (static setups ignore config).
    fn apply_config(&mut self, _config: &[u8]) {}
}

pub struct Replica<U: Upcalls, T: Transport<U>> {
    inner: Arc<Inner<U, T>>,
}

impl<U: Upcalls, T: Transport<U>> Debug for Replica<U, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Replica");
        if let Ok(sync) = self.inner.sync.try_lock() {
            s.field("stat", &sync.status);
            s.field("view", &sync.view.number);
            s.field("norm", &sync.latest_normal_view);
            s.field("inc", &sync.record.inconsistent.len());
            s.field("con", &sync.record.consensus.len());
        }
        s.finish_non_exhaustive()
    }
}

struct Inner<U: Upcalls, T: Transport<U>> {
    transport: T,
    app_tick: Option<fn(&U, &T, &Membership<T::Address>)>,
    view_info_key: String,
    // Single Mutex for all state — faithfully implements the TLA+ single-threaded
    // state machine model. A RwLock<Upcalls> + Mutex<ProtocolState> split was
    // considered to make RequestUnlogged (exec_unlogged(&self)) concurrently
    // runnable via read locks. However, exec_consensus(&mut self) mutates state
    // (prepared list, min_prepare_time), so ProposeConsensus still needs a write
    // lock and blocks Gets. The only gain would be concurrent Gets with each
    // other — Gets are fast (BTreeMap lookup) and with thread-per-core the Mutex
    // is uncontended. Deemed unnecessary complexity; reconsider only if profiling
    // shows contention.
    sync: Mutex<SyncInner<U, T>>,
}

struct SyncInner<U: Upcalls, T: Transport<U>> {
    status: Status,
    view: SharedView<T::Address>,
    latest_normal_view: SharedView<T::Address>,
    /// Leader's record from the *start* of the current view, used for catching up other replicas.
    leader_record: Option<LeaderRecord<U::IO, U::CO, U::CR, T::Address>>,
    /// Op IDs inserted or modified since the last completed view change.
    delta_op_ids: HashSet<OpId>,
    changed_view_recently: bool,
    upcalls: U,
    record: Arc<Record<U>>,
    outstanding_do_view_changes: HashMap<T::Address, DoViewChange<U::IO, U::CO, U::CR, T::Address>>,
    /// Last time received message from each peer replica.
    peer_liveness: HashMap<T::Address, Instant>,
}

#[derive(Serialize, Deserialize)]
struct PersistentViewInfo<A> {
    view: SharedView<A>,
    latest_normal_view: SharedView<A>,
}

impl<U: Upcalls, T: Transport<U>> Replica<U, T> {
    const VIEW_CHANGE_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(
        membership: Membership<T::Address>,
        upcalls: U,
        transport: T,
        app_tick: Option<fn(&U, &T, &Membership<T::Address>)>,
    ) -> Self {
        let view = SharedView::new(View {
            membership,
            number: ViewNumber(0),
            app_config: None,
        });
        let view_info_key = format!("ir_replica_{}", transport.address());
        let ret = Self {
            inner: Arc::new(Inner {
                transport,
                app_tick,
                view_info_key,
                sync: Mutex::new(SyncInner {
                    status: Status::Normal,
                    latest_normal_view: view.clone(),
                    view,
                    changed_view_recently: true,
                    upcalls,
                    record: Arc::new(Record::<U>::default()),
                    leader_record: None,
                    delta_op_ids: HashSet::new(),
                    outstanding_do_view_changes: HashMap::new(),
                    peer_liveness: HashMap::new(),
                }),
            }),
        };
        let mut sync = ret.inner.sync.lock().unwrap();

        if let Some(persistent) = ret
            .inner
            .transport
            .persisted::<PersistentViewInfo<T::Address>>(&ret.inner.view_info_key)
        {
            sync.status = Status::Recovering;
            sync.view = persistent.view;
            sync.latest_normal_view = persistent.latest_normal_view;
            sync.view.make_mut().number.0 += 1;

            if sync.view.leader() == ret.inner.transport.address() {
                sync.view.make_mut().number.0 += 1;
            }

            ret.persist_view_info(&*sync);

            Self::broadcast_do_view_change(&ret.inner.transport, &mut *sync);
        } else {
            ret.persist_view_info(&*sync);
        }
        drop(sync);
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

    fn persist_view_info(&self, sync: &SyncInner<U, T>) {
        if sync.view.membership.len() == 1 {
            return;
        }
        self.inner.transport.persist(
            &self.inner.view_info_key,
            Some(&PersistentViewInfo {
                view: sync.view.clone(),
                latest_normal_view: sync.latest_normal_view.clone(),
            }),
        );
    }

    fn tick(&self) {
        let inner = Arc::downgrade(&self.inner);
        T::spawn(async move {
            loop {
                T::sleep(Self::VIEW_CHANGE_INTERVAL).await;

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let mut sync = inner.sync.lock().unwrap();
                let sync = &mut *sync;

                sync.peer_liveness
                    .retain(|a, _| sync.view.membership.contains(*a));

                if sync.changed_view_recently {
                    trace!("{:?} skipping view change", inner.transport.address());
                    sync.changed_view_recently = false;
                }
                /* else if sync
                    .peer_liveness
                    .get(&Index(
                        ((sync.view.number.0 + 1) % sync.view.membership.len() as u64) as usize,
                    ))
                    .map(|t| t.elapsed() > Duration::from_secs(3))
                    .unwrap_or(false)
                {
                    // skip this view change.
                } */
                else {
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
                    tick(&sync.upcalls, &transport, &sync.view.membership);
                } else {
                    break;
                }
            }
        });
    }

    fn broadcast_do_view_change(transport: &T, sync: &mut SyncInner<U, T>) {
        sync.changed_view_recently = true;
        let destinations = sync
            .view
            .membership
            .iter()
            .chain(sync.latest_normal_view.membership.iter())
            .collect::<HashSet<_>>();

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
                        let payload = if let Some(lr) = sync.leader_record.as_ref() {
                            RecordPayload::Delta {
                                base_view: lr.view.number,
                                entries: sync.record.filter_by_op_ids(&sync.delta_op_ids),
                            }
                        } else {
                            RecordPayload::Full((*sync.record).clone())
                        };
                        ViewChangeAddendum { payload, latest_normal_view: sync.latest_normal_view.clone() }
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

                    let state = match Arc::make_mut(&mut sync.record).inconsistent.entry(op_id) {
                        Entry::Vacant(vacant) => {
                            let state = vacant.insert(RecordInconsistentEntry {
                                op,
                                state: RecordEntryState::Tentative,
                            }).state;
                            sync.delta_op_ids.insert(op_id);
                            state
                        }
                        Entry::Occupied(occupied) => {
                            debug_assert_eq!(occupied.get().op, op);
                            occupied.get().state
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
                    let (result, state) = match Arc::make_mut(&mut sync.record).consensus.entry(op_id) {
                        Entry::Occupied(entry) => {
                            let entry = entry.get();
                            debug_assert_eq!(entry.op, op);
                            (entry.result.clone(), entry.state)
                        }
                        Entry::Vacant(vacant) => {
                            let entry = vacant.insert(RecordConsensusEntry {
                                result: sync.upcalls.exec_consensus(&op),
                                op,
                                state: RecordEntryState::Tentative,
                            });
                            sync.delta_op_ids.insert(op_id);
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
                if sync.status.is_normal() && let Some(entry) = Arc::make_mut(&mut sync.record).inconsistent.get_mut(&op_id) && entry.state.is_tentative() {
                    entry.state = RecordEntryState::Finalized(sync.view.number);
                    sync.upcalls.exec_inconsistent(&entry.op);
                    sync.delta_op_ids.insert(op_id);
                }
            }
            Message::<U, T>::FinalizeConsensus(FinalizeConsensus { op_id, result }) => {
                if sync.status.is_normal()
                    && let Some(entry) = Arc::make_mut(&mut sync.record).consensus.get_mut(&op_id) {
                        // Don't allow a late `FinalizeConsensus` to overwrite
                        // a view change decision.
                        if entry.state.is_tentative() {
                            entry.state = RecordEntryState::Finalized(sync.view.number);
                            entry.result = result;
                            sync.upcalls.finalize_consensus(&entry.op, &entry.result);
                            sync.delta_op_ids.insert(op_id);
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
                        self.persist_view_info(&*sync);
                        Self::broadcast_do_view_change(
                            &self.inner.transport,
                            &mut *sync,
                        );
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
                                payload: if let Some(lr) = sync.leader_record.as_ref() {
                                    RecordPayload::Delta {
                                        base_view: lr.view.number,
                                        entries: sync.record.filter_by_op_ids(&sync.delta_op_ids),
                                    }
                                } else {
                                    RecordPayload::Full((*sync.record).clone())
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
                            {
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
                                        let base = sync.leader_record.as_ref().map(|lr| &*lr.record);
                                        addendum.payload.clone().resolve(base)
                                    })
                                    .collect::<Vec<_>>();

                                trace!(
                                    "have {} latest records ({:?})",
                                    latest_records.len(),
                                    sync
                                        .outstanding_do_view_changes
                                        .iter()
                                        .map(|(a, dvt)| (*a, dvt.view.number, dvt.addendum.as_ref().unwrap().latest_normal_view.number))
                                        .chain(
                                            std::iter::once(
                                                (self.inner.transport.address(), sync.view.number, sync.latest_normal_view.number)
                                            )
                                        )
                                        .collect::<Vec<_>>()
                                );

                                #[allow(non_snake_case)]
                                let mut R = Record::<U>::default();
                                let mut entries_by_opid =
                                    HashMap::<OpId, Vec<RecordConsensusEntry<U::CO, U::CR>>>::new();
                                let mut finalized = HashSet::new();
                                for r in latest_records {
                                    for (op_id, entry) in r.inconsistent.clone() {
                                        match R.inconsistent.entry(op_id) {
                                            Entry::Vacant(vacant) => {
                                                // Mark as finalized as `sync` will execute it.
                                                vacant.insert(entry).state = RecordEntryState::Finalized(sync.view.number);
                                            }
                                            Entry::Occupied(mut occupied) => {
                                                if let RecordEntryState::Finalized(view) = entry.state {
                                                    let state = &mut occupied.get_mut().state;
                                                    *state = RecordEntryState::Finalized(view);
                                                }
                                            }
                                        }
                                    }
                                    for (op_id, entry) in r.consensus.clone() {
                                        match entry.state {
                                            RecordEntryState::Finalized(_) => {
                                                match R.consensus.entry(op_id) {
                                                    Entry::Vacant(vacant) => {
                                                        sync.upcalls.finalize_consensus(&entry.op, &entry.result);
                                                        vacant.insert(entry);
                                                    }
                                                    Entry::Occupied(mut occupied) => {
                                                        if occupied.get().state.is_tentative() {
                                                            sync.upcalls.finalize_consensus(&entry.op, &entry.result);
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
                                    HashMap::<OpId, (U::CO, U::CR)>::new();
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
                                    let sync = &mut *sync;
                                    sync.upcalls.sync(&sync.record, &R);
                                }

                                let results_by_opid =
                                    sync.upcalls.merge(d, u);

                                debug_assert_eq!(results_by_opid.len(), entries_by_opid.len());

                                for (op_id, result) in results_by_opid {
                                    let entries = entries_by_opid.get(&op_id).unwrap();
                                    let entry = &entries[0];
                                    sync.upcalls.finalize_consensus(&entry.op, &result);
                                    R.consensus.insert(
                                        op_id,
                                        RecordConsensusEntry {
                                            op: entry.op.clone(),
                                            result: result.clone(),
                                            state: RecordEntryState::Finalized(sync.view.number),
                                        },
                                    );
                                }

                                sync.record = Arc::new(R);
                            }
                            sync.changed_view_recently = true;
                            sync.status = Status::Normal;
                            sync.view.make_mut().number = msg_view_number;
                            if let Some(config) = sync.view.app_config.as_ref() {
                                sync.upcalls.apply_config(config);
                            }

                            let destinations = sync
                                .view
                                .membership
                                .iter()
                                .chain(sync.latest_normal_view.membership.iter())
                                .collect::<HashSet<_>>();

                            sync.latest_normal_view.make_mut().number = msg_view_number;
                            sync.latest_normal_view.make_mut().membership = sync.view.membership.clone();
                            self.persist_view_info(&*sync);

                            let base_view = sync.leader_record.as_ref().map(|lr| lr.view.number);
                            let delta_entries = sync.leader_record.as_ref().map(|lr| {
                                let base = &*lr.record;
                                let mut delta_ids = HashSet::new();
                                for id in sync.record.inconsistent.keys() {
                                    if !base.inconsistent.contains_key(id)
                                        || base.inconsistent.get(id) != sync.record.inconsistent.get(id)
                                    {
                                        delta_ids.insert(*id);
                                    }
                                }
                                for id in sync.record.consensus.keys() {
                                    if !base.consensus.contains_key(id)
                                        || base.consensus.get(id) != sync.record.consensus.get(id)
                                    {
                                        delta_ids.insert(*id);
                                    }
                                }
                                sync.record.filter_by_op_ids(&delta_ids)
                            });

                            let merged_record = Arc::new((*sync.record).clone());
                            sync.leader_record = Some(LeaderRecord {
                                record: Arc::clone(&merged_record),
                                view: sync.view.clone(),
                            });
                            sync.delta_op_ids.clear();

                            let full_payload = RecordPayload::Full((*merged_record).clone());

                            for address in destinations {
                                if address == self.inner.transport.address() {
                                    continue;
                                }
                                let recipient_same_base = base_view
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
                                            base_view: base_view.unwrap(),
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
                            self.inner.transport.persist(
                                &format!("checkpoint_{}", sync.view.number.0),
                                Some(&sync.upcalls),
                            );
                            self.inner.transport.on_membership_changed(&sync.view.membership);
                        }
                    }
                } else if !msg.from_client
                    && let Some(lr) = sync.leader_record.as_ref()
                    && lr.view.number >= msg.view.number
                {
                    warn!("{:?} sending leader record to help catch up {address:?}", self.address());
                    let sv = StartView {
                        payload: RecordPayload::Full((*lr.record).clone()),
                        view: lr.view.clone(),
                    };
                    self.inner.transport.do_send(address, Message::<U, T>::StartView(sv));
                }
            }
            Message::<U, T>::StartView(StartView { payload, view }) => {
                if view.number > sync.view.number
                    || (view.number == sync.view.number && !sync.status.is_normal())
                {
                    info!("starting view {:?} (was {:?} in {:?})", view.number, sync.status, sync.view.number);
                    let base = sync.leader_record.as_ref().map(|lr| &*lr.record);
                    // Validate Delta base before resolving — the leader-side
                    // view filter should prevent this, but defend against it.
                    if let RecordPayload::Delta { base_view, .. } = &payload {
                        let base_ok = sync.leader_record.as_ref()
                            .map(|lr| lr.view.number == *base_view)
                            .unwrap_or(false);
                        // Should never fire after the leader-side fix. If it does,
                        // it indicates a new bug in the Delta optimization.
                        debug_assert!(base_ok,
                            "Delta StartView with mismatched base: expected base_view={base_view:?}, \
                             leader_record view={:?}",
                            sync.leader_record.as_ref().map(|lr| lr.view.number));
                        if !base_ok {
                            warn!(
                                ?base_view,
                                leader_record_view = ?sync.leader_record.as_ref().map(|lr| lr.view.number),
                                "ignoring Delta StartView: base mismatch or missing"
                            );
                            // TODO: Send a NAK back to the leader so it can retry with
                            // Full payload, rather than silently dropping the StartView.
                            // For now, the replica will stay in ViewChanging and the next
                            // tick-driven view change will recover.
                            return None;
                        }
                    }
                    let new_record = payload.resolve(base);
                    sync.upcalls.sync(&sync.record, &new_record);
                    let new_record = Arc::new(new_record);
                    sync.record = Arc::clone(&new_record);
                    sync.delta_op_ids.clear();
                    sync.status = Status::Normal;
                    sync.view = view.clone();
                    sync.latest_normal_view = view.clone();
                    if let Some(config) = sync.view.app_config.as_ref() {
                        sync.upcalls.apply_config(config);
                    }
                    sync.changed_view_recently = true;
                    sync.leader_record = Some(LeaderRecord { record: new_record, view });
                    self.persist_view_info(&*sync);
                    self.inner.transport.on_membership_changed(&sync.view.membership);
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
                    self.persist_view_info(&*sync);

                    // Election.
                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            Message::<U, T>::RemoveMember(RemoveMember{address}) => {
                if sync.status.is_normal() && sync.view.membership.get_index(address).is_some() && sync.view.membership.len() > 1 && address != self.inner.transport.address() {
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
                    self.persist_view_info(&*sync);

                    // Election.
                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            Message::<U, T>::Reconfigure(Reconfigure { config }) => {
                if sync.status.is_normal() {
                    info!("reconfiguring with {} bytes", config.len());
                    sync.view.make_mut().app_config = Some(config);
                    sync.status = Status::ViewChanging;
                    sync.view.make_mut().number.0 += 3;
                    self.persist_view_info(&*sync);

                    Self::broadcast_do_view_change(&self.inner.transport, sync);
                }
            }
            Message::<U, T>::FetchLeaderRecord(_) => {
                let (record, view) = sync.leader_record.as_ref()
                    .map(|lr| (Some(Arc::clone(&lr.record)), Some(lr.view.clone())))
                    .unwrap_or((None, None));
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
        if sync.status.is_normal() {
            sync.status = Status::ViewChanging;
        }
        sync.view.make_mut().number.0 += 1;
        Self::broadcast_do_view_change(&self.inner.transport, &mut *sync);
    }
}
