use super::{
    message::{BootstrapRecord, FetchLeaderRecord, FinalizeInconsistentReply, LeaderRecordReply, Reconfigure},
    shared_view::SharedView, AddMember, Confirm, DoViewChange, FinalizeConsensus,
    RemoveMember,
    FinalizeInconsistent, Membership, MembershipSize, Message, OpId, ProposeConsensus,
    ProposeInconsistent, ReplicaUpcalls, ReplyConsensus, ReplyInconsistent, ReplyUnlogged,
    RequestUnlogged, View, ViewNumber,
};
use crate::{
    util::{join, Join},
    Transport,
};
use tracing::debug;
use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex},
    task::Context,
    time::Duration,
};

/// Configuration for an IR client.
#[derive(Clone, Copy)]
pub struct ClientConfig {
    /// Maximum total time `invoke_inconsistent_with_result` may run before
    /// returning whatever results are available. Bounds both Phase 1
    /// (propose) and Phase 2 (finalize) hard timeouts, and exits the
    /// retry loop when elapsed.
    pub inconsistent_result_deadline: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            inconsistent_result_deadline: Duration::from_secs(5),
        }
    }
}
use futures::future::Either;

/// Randomly chosen id, unique to each IR client.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Id(pub u64);

impl Id {
    pub fn new(rng: &mut crate::Rng) -> Self {
        Self(rng.random_u64())
    }
}

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "C({})", self.0)
    }
}

/// IR client, capable of invoking operations on an IR replica group.
pub struct Client<U: ReplicaUpcalls, T: Transport<U>> {
    id: Id,
    inner: Arc<Inner<U, T>>,
    inconsistent_result_deadline: Duration,
    _spooky: PhantomData<U>,
}

impl<U: ReplicaUpcalls, T: Transport<U>> Clone for Client<U, T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            inner: Arc::clone(&self.inner),
            inconsistent_result_deadline: self.inconsistent_result_deadline,
            _spooky: PhantomData,
        }
    }
}

struct Inner<U: ReplicaUpcalls, T: Transport<U>> {
    transport: T,
    sync: Mutex<SyncInner<U, T>>,
}

struct SyncInner<U: ReplicaUpcalls, T: Transport<U>> {
    operation_counter: u64,
    view: SharedView<T::Address>,
    rng: crate::Rng,
}

impl<U: ReplicaUpcalls, T: Transport<U>> SyncInner<U, T> {
    fn next_number(&mut self) -> u64 {
        let ret = self.operation_counter;
        self.operation_counter += 1;
        ret
    }

    fn random_index(&mut self, count: usize) -> usize {
        self.rng.random_index(count)
    }
}

impl<U: ReplicaUpcalls, T: Transport<U>> Client<U, T> {
    pub fn new(rng: crate::Rng, membership: Membership<T::Address>, transport: T) -> Self {
        Self::with_config(rng, membership, transport, ClientConfig::default())
    }

    pub fn with_config(mut rng: crate::Rng, membership: Membership<T::Address>, transport: T, config: ClientConfig) -> Self {
        let id = Id::new(&mut rng);
        Self {
            id,
            inner: Arc::new(Inner {
                transport,
                sync: Mutex::new(SyncInner {
                    operation_counter: 0,
                    view: SharedView::new(View {
                        membership,
                        number: ViewNumber(0),
                        app_config: None,
                    }),
                    rng,
                }),
            }),
            inconsistent_result_deadline: config.inconsistent_result_deadline,
            _spooky: PhantomData,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    /// Replace the membership and reset the view to 0 without resetting the
    /// operation counter. Used by `DnsRefreshingShardClient` when DNS resolution
    /// detects IP changes: the counter must continue monotonically to avoid
    /// op_id collisions with entries already in the replica's record from
    /// previous proposals.
    pub fn reset_membership(&self, membership: Membership<T::Address>) {
        let mut sync = self.inner.sync.lock().unwrap();
        sync.view = SharedView::new(View {
            membership,
            number: ViewNumber(0),
            app_config: None,
        });
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    /// Returns the current membership size (for computing f+1 thresholds).
    pub fn membership_size(&self) -> MembershipSize {
        self.inner.sync.lock().unwrap().view.membership.size()
    }

    /// Updates own view and those of lagging replicas.
    ///
    /// `Index`'s in `views` must correspond to `sync.view`.
    fn update_view<'a>(
        transport: &T,
        sync: &mut SyncInner<U, T>,
        views: impl IntoIterator<Item = (T::Address, &'a SharedView<T::Address>)> + Clone,
    ) {
        if let Some(latest_view) = views
            .clone()
            .into_iter()
            .map(|(_, n)| n)
            .max_by_key(|v| v.number)
        {
            for (address, view) in views {
                if view.number < latest_view.number {
                    transport.do_send(
                        address,
                        Message::<U, T>::DoViewChange(DoViewChange {
                            view: latest_view.clone(),
                            from_client: true,
                            addendum: None,
                        }),
                    )
                }
            }
            if latest_view.number > sync.view.number {
                sync.view = latest_view.clone();
            }
        }
    }

    /// Unlogged request against any single replica.
    pub fn invoke_unlogged(&self, op: U::UO) -> impl Future<Output = U::UR> {
        let inner = Arc::clone(&self.inner);
        let (index, count) = {
            let mut sync = inner.sync.lock().unwrap();
            let count = sync.view.membership.len();
            (sync.random_index(count), count)
        };

        async move {
            let mut futures = FuturesUnordered::new();

            loop {
                debug_assert!(futures.len() < count);
                {
                    let sync = inner.sync.lock().unwrap();
                    let address = sync
                        .view
                        .membership
                        .get((index + futures.len()) % count)
                        .unwrap();
                    drop(sync);
                    let future = inner.transport.send::<ReplyUnlogged<U::UR, T::Address>>(
                        address,
                        RequestUnlogged { op: op.clone() },
                    );
                    futures.push(future);
                }

                let response = if futures.len() < count {
                    let timeout = T::sleep(Duration::from_millis(250));
                    futures::pin_mut!(timeout);
                    match futures::future::select(timeout, futures.select_next_some()).await {
                        Either::Left(_) => continue,
                        Either::Right((response, _)) => response,
                    }
                } else {
                    futures.next().await.unwrap()
                };
                let mut sync = inner.sync.lock().unwrap();
                if response.view.number > sync.view.number {
                    sync.view = response.view;
                }
                return response.result;
            }
        }
    }

    /// Returns a `Join` over getting an unlogged response from all replicas.
    ///
    /// A consenSUS operation; can get a quorum but doesn't preserve decisions.
    pub fn invoke_unlogged_joined(
        &self,
        op: U::UO,
    ) -> (
        Join<T::Address, impl Future<Output = ReplyUnlogged<U::UR, T::Address>>>,
        MembershipSize,
    ) {
        let sync = self.inner.sync.lock().unwrap();
        let membership_size = sync.view.membership.size();

        let future = join(sync.view.membership.iter().map(|address| {
            (
                address,
                self.inner
                    .transport
                    .send::<ReplyUnlogged<U::UR, T::Address>>(
                        address,
                        RequestUnlogged { op: op.clone() },
                    ),
            )
        }));

        (future, membership_size)
    }

    /// Send an unlogged request to ALL replicas and wait for f+1 replies
    /// from the SAME view. Returns the view-consistent results. Used for
    /// read-only scan validation and CDC scan_changes where we need f+1
    /// view-consistent responses to merge.
    pub fn invoke_unlogged_quorum(&self, op: U::UO) -> impl Future<Output = Vec<U::UR>> + Send + use<U, T> {
        #[allow(clippy::disallowed_methods)] // .values().any() is order-independent
        fn has_quorum_same_view<UR, A: Ord>(
            results: &BTreeMap<A, ReplyUnlogged<UR, A>>,
            f_plus_one: usize,
        ) -> bool {
            let mut counts = BTreeMap::<ViewNumber, usize>::new();
            for r in results.values() {
                *counts.entry(r.view.number).or_default() += 1;
            }
            counts.values().any(|&c| c >= f_plus_one)
        }

        fn find_majority_view<UR, A: Ord>(
            results: &BTreeMap<A, ReplyUnlogged<UR, A>>,
            f_plus_one: usize,
        ) -> Option<ViewNumber> {
            let mut counts = BTreeMap::<ViewNumber, usize>::new();
            for r in results.values() {
                *counts.entry(r.view.number).or_default() += 1;
            }
            counts
                .into_iter()
                .filter(|&(_, c)| c >= f_plus_one)
                .max_by_key(|&(v, _)| v)
                .map(|(v, _)| v)
        }

        let inner = Arc::clone(&self.inner);

        async move {
            let (membership_size, future) = {
                let sync = inner.sync.lock().unwrap();
                let membership_size = sync.view.membership.size();

                let future = join(sync.view.membership.iter().map(|address| {
                    (
                        address,
                        inner.transport.send::<ReplyUnlogged<U::UR, T::Address>>(
                            address,
                            RequestUnlogged { op: op.clone() },
                        ),
                    )
                }));
                (membership_size, future)
            };

            let f_plus_one = membership_size.f_plus_one();
            let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(5000)));

            let results = future
                .until(
                    move |results: &BTreeMap<T::Address, ReplyUnlogged<U::UR, T::Address>>,
                          cx: &mut Context<'_>| {
                        has_quorum_same_view(results, f_plus_one)
                            || hard_timeout.as_mut().poll(cx).is_ready()
                    },
                )
                .await;

            {
                let mut sync = inner.sync.lock().unwrap();
                Self::update_view(
                    &inner.transport,
                    &mut *sync,
                    results.iter().map(|(i, r)| (*i, &r.view)),
                );
            }

            // Return only results from the majority-view group (f+1 from same view).
            // On hard timeout (no group reaches f+1), fall back to all results.
            let majority_view = find_majority_view(&results, f_plus_one);
            results
                .into_values()
                .filter(|r| majority_view.is_none_or(|v| r.view.number == v))
                .map(|r| r.result)
                .collect()
        }
    }

    /// Returns when the inconsistent operation is finalized, retrying indefinitely.
    pub fn invoke_inconsistent(&self, op: U::IO) -> impl Future<Output = ()> + use<U, T> {
        let inner = Arc::clone(&self.inner);
        let client_id = self.id;

        fn has_ancient<A>(results: &BTreeMap<A, ReplyInconsistent<A>>) -> bool {
            results.values().any(|v| v.state.is_none())
        }

        fn has_quorum<A>(
            membership: MembershipSize,
            results: &BTreeMap<A, ReplyInconsistent<A>>,
            check_views: bool,
        ) -> bool {
            if check_views {
                for result in results.values() {
                    let matching = results
                        .values()
                        .filter(|other| other.view.number == result.view.number)
                        .count();
                    if matching >= result.view.membership.size().f_plus_one() {
                        return true;
                    }
                }
                false
            } else {
                results.len() >= membership.f_plus_one()
            }
        }

        async move {
            let mut retry_backoff = Duration::from_millis(50);
            loop {
                // Generate a fresh op_id for each attempt. Reusing an op_id across
                // retries is incorrect: after a view change the replica's merged record
                // may already contain an entry for the old op_id with different state,
                // causing a debug_assert_eq!(occupied.op, op) failure on re-proposal.
                // This matches the invoke_consensus pattern (line ~627).
                let (membership_size, op_id, future) = {
                    let mut sync = inner.sync.lock().unwrap();
                    let op_id = OpId { client_id, number: sync.next_number() };
                    let membership_size = sync.view.membership.size();

                    let future = join(sync.view.membership.iter().map(|address| {
                        (
                            address,
                            inner.transport.send::<ReplyInconsistent<T::Address>>(
                                address,
                                ProposeInconsistent {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.view.number,
                                },
                            ),
                        )
                    }));
                    (membership_size, op_id, future)
                };

                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));

                // E.g. the replica group got smaller and we can't get a response from a majority of the old size.
                let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(5000)));

                let results = future
                    .until(
                        move |results: &BTreeMap<T::Address, ReplyInconsistent<T::Address>>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || has_quorum(
                                    membership_size,
                                    results,
                                    soft_timeout.as_mut().poll(cx).is_ready(),
                                )
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;

                let retry = {
                    let mut sync = inner.sync.lock().unwrap();
                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, &r.view)),
                    );

                    if has_ancient(&results) || !has_quorum(membership_size, &results, true) {
                        true
                    } else {
                        for address in &sync.view.membership {
                            inner
                                .transport
                                .do_send(address, FinalizeInconsistent { op_id });
                        }
                        false
                    }
                };
                if retry {
                    // Exponential backoff before retrying to prevent a tight async
                    // loop that freezes simulated time under `start_paused = true`.
                    T::sleep(retry_backoff).await;
                    retry_backoff = (retry_backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
                return;
            }
        }
    }

    /// Like `invoke_inconsistent`, but waits for f+1 `FinalizeInconsistentReply`
    /// responses and returns the collected results.
    ///
    /// Used by read-only transactions (QuorumRead): the IR protocol executes
    /// `ExecInconsistent` at FINALIZE time, so this method sends PROPOSE,
    /// waits for f+1 replies, then sends FINALIZE and waits for f+1 finalize
    /// replies that carry the execution results.
    pub fn invoke_inconsistent_with_result(&self, op: U::IO) -> impl Future<Output = Vec<U::IR>> + Send + use<U, T> {
        let inner = Arc::clone(&self.inner);
        let client_id = self.id;
        let deadline_duration = self.inconsistent_result_deadline;

        fn has_ancient<A>(results: &BTreeMap<A, ReplyInconsistent<A>>) -> bool {
            results.values().any(|v| v.state.is_none())
        }

        fn has_quorum<A>(
            membership: MembershipSize,
            results: &BTreeMap<A, ReplyInconsistent<A>>,
            check_views: bool,
        ) -> bool {
            if check_views {
                for result in results.values() {
                    let matching = results
                        .values()
                        .filter(|other| other.view.number == result.view.number)
                        .count();
                    if matching >= result.view.membership.size().f_plus_one() {
                        return true;
                    }
                }
                false
            } else {
                results.len() >= membership.f_plus_one()
            }
        }

        async move {
            let deadline = std::time::Instant::now() + deadline_duration;
            let mut retry_backoff = Duration::from_millis(50);
            let mut phase1_attempt = 0u32;
            loop {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    debug!("iir: deadline expired before phase1, returning empty");
                    return Vec::new();
                }

                phase1_attempt += 1;
                // Phase 1: Propose (same as invoke_inconsistent)
                //
                // Generate a fresh op_id per attempt — see comment in invoke_inconsistent.
                let (membership_size, op_id, future) = {
                    let mut sync = inner.sync.lock().unwrap();
                    let op_id = OpId { client_id, number: sync.next_number() };
                    let membership_size = sync.view.membership.size();
                    debug!(
                        phase1_attempt,
                        view = ?sync.view.number,
                        ?membership_size,
                        "iir: phase1 start",
                    );
                    let future = join(sync.view.membership.iter().map(|address| {
                        (
                            address,
                            inner.transport.send::<ReplyInconsistent<T::Address>>(
                                address,
                                ProposeInconsistent {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.view.number,
                                },
                            ),
                        )
                    }));
                    (membership_size, op_id, future)
                };

                let phase1_hard = remaining.min(Duration::from_millis(5000));
                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));
                let mut hard_timeout = std::pin::pin!(T::sleep(phase1_hard));

                let results = future
                    .until(
                        move |results: &BTreeMap<T::Address, ReplyInconsistent<T::Address>>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || has_quorum(
                                    membership_size,
                                    results,
                                    soft_timeout.as_mut().poll(cx).is_ready(),
                                )
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;
                debug!(
                    "iir: phase1 done attempt={phase1_attempt} responses={} ancient={} quorum={}",
                    results.len(),
                    has_ancient(&results),
                    has_quorum(membership_size, &results, true),
                );

                let phase2 = {
                    let mut sync = inner.sync.lock().unwrap();
                    // Per IR protocol: "If a client receives responses with
                    // different view numbers, it notifies the replicas in the
                    // older view." This may cause notified replicas to enter
                    // ViewChanging, temporarily preventing them from responding
                    // to Phase 2's FinalizeInconsistent. That is correct — the
                    // FinalizeInconsistent handler responds for already-Finalized
                    // entries (finalized during view-change merge/sync), so once
                    // replicas return to Normal they will reply.
                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, &r.view)),
                    );

                    if has_ancient(&results) || !has_quorum(membership_size, &results, true) {
                        None
                    } else {
                        // Phase 2: Finalize — send FinalizeInconsistent and wait for f+1 replies
                        let addrs: Vec<_> = sync.view.membership.iter().collect();
                        debug!("iir: phase2 sending to addrs={addrs:?} op_id={op_id:?}");
                        let finalize_future = join(sync.view.membership.iter().map(|address| {
                            (
                                address,
                                inner.transport.send::<FinalizeInconsistentReply<U::IR, T::Address>>(
                                    address,
                                    FinalizeInconsistent { op_id },
                                ),
                            )
                        }));
                        let finalize_membership_size = sync.view.membership.size();
                        Some((finalize_future, finalize_membership_size))
                    }
                };

                let Some((finalize_future, finalize_membership_size)) = phase2 else {
                    debug!("iir: phase1 failed, retrying (backoff={retry_backoff:?})");
                    // Exponential backoff before retrying to prevent a tight async
                    // loop that freezes simulated time under `start_paused = true`.
                    T::sleep(retry_backoff).await;
                    retry_backoff = (retry_backoff * 2).min(Duration::from_secs(1));
                    continue;
                };
                debug!("iir: phase2 start f_plus_one={}", finalize_membership_size.f_plus_one());

                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let phase2_hard = remaining.min(Duration::from_millis(5000));
                let mut finalize_timeout = std::pin::pin!(T::sleep(phase2_hard));

                let finalize_results = finalize_future
                    .until(
                        move |results: &BTreeMap<T::Address, FinalizeInconsistentReply<U::IR, T::Address>>,
                              cx: &mut Context<'_>| {
                            results.len() >= finalize_membership_size.f_plus_one()
                                || finalize_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;
                debug!("iir: phase2 done responses={} needed={}", finalize_results.len(), finalize_membership_size.f_plus_one());

                if finalize_results.len() >= finalize_membership_size.f_plus_one() {
                    let mut sync = inner.sync.lock().unwrap();
                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        finalize_results.iter().map(|(i, r)| (*i, &r.view)),
                    );
                    return finalize_results.into_values().map(|r| r.result).collect();
                }

                // Not enough finalize replies — update view from partial replies
                // and apply backoff before retrying.
                {
                    let mut sync = inner.sync.lock().unwrap();
                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        finalize_results.iter().map(|(i, r)| (*i, &r.view)),
                    );
                }
                T::sleep(retry_backoff).await;
                retry_backoff = (retry_backoff * 2).min(Duration::from_secs(1));
            }
        }
    }

    /// Returns the consensus result, retrying indefinitely.
    pub fn invoke_consensus<D: Fn(BTreeMap<U::CR, usize>, MembershipSize) -> U::CR + Send>(
        &self,
        op: U::CO,
        decide: D,
    ) -> impl Future<Output = U::CR> + Send + use<U, T, D> {
        fn has_ancient<R, A>(replies: &BTreeMap<A, ReplyConsensus<R, A>>) -> bool {
            replies.values().any(|v| v.result_state.is_none())
        }

        fn get_finalized<R, A>(replies: &BTreeMap<A, ReplyConsensus<R, A>>) -> Option<&R> {
            replies
                .values()
                .find(|r| r.result_state.as_ref().unwrap().1.is_finalized())
                .map(|r| &r.result_state.as_ref().unwrap().0)
        }

        fn get_quorum<R: PartialEq + Debug, A>(
            membership: MembershipSize,
            replies: &BTreeMap<A, ReplyConsensus<R, A>>,
            matching_results: bool,
        ) -> Option<(&View<A>, &R)> {
            let required = if matching_results {
                membership.three_over_two_f_plus_one()
            } else {
                membership.f_plus_one()
            };
            for candidate in replies.values() {
                let matches = replies
                    .values()
                    .filter(|other| {
                        other.view.number == candidate.view.number
                            && (!matching_results
                                || other.result_state.as_ref().unwrap().0
                                    == candidate.result_state.as_ref().unwrap().0)
                    })
                    .count();
                if matches >= required {
                    return Some((&candidate.view, &candidate.result_state.as_ref().unwrap().0));
                }
            }
            None
        }

        let client_id = self.id;
        let inner = Arc::clone(&self.inner);

        async move {
            let mut retry_backoff = Duration::from_millis(50);
            loop {
                // Fresh op_id per attempt: reusing an op_id across retries is
                // incorrect because the replica's merged record (after a view
                // change) may already contain an entry for the old op_id,
                // causing a conflict on re-proposal. All three invoke_* methods
                // follow this pattern.
                let (membership_size, op_id, future) = {
                    let mut sync = inner.sync.lock().unwrap();
                    let number = sync.next_number();
                    let op_id = OpId { client_id, number };
                    let membership_size = sync.view.membership.size();

                    let future = join(sync.view.membership.iter().map(|address| {
                        (
                            address,
                            inner.transport.send::<ReplyConsensus<U::CR, T::Address>>(
                                address,
                                ProposeConsensus {
                                    op_id,
                                    op: op.clone(),
                                    recent: sync.view.number,
                                },
                            ),
                        )
                    }));
                    (membership_size, op_id, future)
                };

                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));
                let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(500)));

                let results = future
                    .until(
                        move |results: &BTreeMap<T::Address, ReplyConsensus<U::CR, T::Address>>,
                              cx: &mut Context<'_>| {
                            has_ancient(results)
                                || get_finalized(results).is_some()
                                || get_quorum(
                                    membership_size,
                                    results,
                                    soft_timeout.as_mut().poll(cx).is_pending(),
                                )
                                .is_some()
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;

                fn get_quorum_view<A>(results: &BTreeMap<A, Confirm<A>>) -> Option<&View<A>> {
                    for result in results.values() {
                        let matching = results
                            .values()
                            .filter(|other| other.view.number == result.view.number)
                            .count();
                        if matching >= result.view.membership.size().f_plus_one() {
                            return Some(&result.view);
                        }
                    }
                    None
                }

                let next = {
                    let mut sync = inner.sync.lock().unwrap();

                    Self::update_view(
                        &inner.transport,
                        &mut *sync,
                        results.iter().map(|(i, r)| (*i, &r.view)),
                    );

                    if has_ancient(&results) {
                        // Our view number was very old — retry after backoff (below).
                        None
                    } else {

                    let membership_size = sync.view.membership.size();
                    let finalized = get_finalized(&results);

                    if finalized.is_none() && let Some((_, result)) = get_quorum(membership_size, &results, true) {
                        // Fast path.
                        for address in &sync.view.membership {
                            inner.transport.do_send(
                                address,
                                FinalizeConsensus {
                                    op_id,
                                    result: result.clone(),
                                },
                            );
                        }
                        return result.clone();
                    }

                    if let Some((result, reply_consensus_view)) =
                        finalized.map(|f| (f.clone(), None)).or_else(|| {
                            let view = get_quorum(membership_size, &results, false).map(|(v, _)| v);
                            view.cloned().map(|view| {
                                let results = results
                                    .into_values()
                                    .filter(|rc| rc.view.number == view.number)
                                    .map(|rc| rc.result_state.unwrap().0);
                                let mut totals = BTreeMap::new();
                                for result in results {
                                    *totals.entry(result).or_default() += 1;
                                }
                                (decide(totals, view.membership.size()), Some(view.number))
                            })
                        })
                    {
                        // Slow path.
                        let future = join(sync.view.membership.iter().map(|address| {
                            (
                                address,
                                inner.transport.send::<Confirm<T::Address>>(
                                    address,
                                    FinalizeConsensus {
                                        op_id,
                                        result: result.clone(),
                                    },
                                ),
                            )
                        }));

                        Some((result, reply_consensus_view, future))
                    } else {
                        None
                    }
                    } // else: !has_ancient
                };

                let Some((result, reply_consensus_view, future)) = next else {
                    // Exponential backoff before retrying to prevent a tight async
                    // loop that freezes simulated time under `start_paused = true`.
                    T::sleep(retry_backoff).await;
                    retry_backoff = (retry_backoff * 2).min(Duration::from_secs(1));
                    continue;
                };

                let mut soft_timeout = std::pin::pin!(T::sleep(Duration::from_millis(250)));
                let mut hard_timeout = std::pin::pin!(T::sleep(Duration::from_millis(500)));
                let results = future
                    .until(
                        |results: &BTreeMap<T::Address, Confirm<T::Address>>,
                         cx: &mut Context<'_>| {
                            get_quorum_view(results).is_some()
                                || (soft_timeout.as_mut().poll(cx).is_ready()
                                    && results.len() >= membership_size.f_plus_one())
                                || hard_timeout.as_mut().poll(cx).is_ready()
                        },
                    )
                    .await;
                let mut sync = inner.sync.lock().unwrap();
                Self::update_view(
                    &inner.transport,
                    &mut *sync,
                    results.iter().map(|(i, r)| (*i, &r.view)),
                );
                if let Some(quorum_view) = get_quorum_view(&results) && reply_consensus_view.map(|reply_consensus_view| quorum_view.number == reply_consensus_view).unwrap_or(true) {
                    return result;
                }
            }
        }
    }

    /// Force a client-initiated view change.
    ///
    /// In IR (and TAPIR), clients can trigger view changes independently.
    /// This happens naturally when a client observes replicas at different
    /// view numbers (via `update_view` in consensus/inconsistent ops).
    /// This method explicitly bumps the client's view and sends
    /// `DoViewChange { from_client: true }` to all replicas, which causes
    /// them to adopt the higher view and broadcast to their peers.
    ///
    /// Client-initiated DoViewChange messages carry `addendum: None` — they
    /// do NOT contribute to the f+1 quorum needed by the view change
    /// coordinator. They only nudge replicas to start their own view change.
    pub fn force_view_change(&self) {
        let mut sync = self.inner.sync.lock().unwrap();
        sync.view.make_mut().number.0 += 1;
        for address in &sync.view.membership {
            self.inner.transport.do_send(
                address,
                Message::<U, T>::DoViewChange(DoViewChange {
                    view: sync.view.clone(),
                    from_client: true,
                    addendum: None,
                }),
            );
        }
    }

    /// Broadcast a `Reconfigure` message to all replicas, triggering a view change
    /// that atomically propagates the new app_config to the entire group.
    pub fn reconfigure(&self, config: Vec<u8>) {
        let sync = self.inner.sync.lock().unwrap();
        for address in &sync.view.membership {
            self.inner
                .transport
                .do_send(address, Reconfigure { config: config.clone() });
        }
    }

    /// Fetch the stable leader_record from a random replica.
    pub fn fetch_leader_record(
        &self,
    ) -> impl Future<Output = Option<(SharedView<T::Address>, U::Payload)>> + Send + use<U, T>
    {
        let inner = Arc::clone(&self.inner);
        async move {
            let address = {
                let mut sync = inner.sync.lock().unwrap();
                let count = sync.view.membership.len();
                let idx = sync.random_index(count);
                sync.view.membership.get(idx).unwrap()
            };
            let reply = inner
                .transport
                .send::<LeaderRecordReply<U::Payload, T::Address>>(
                    address,
                    FetchLeaderRecord,
                )
                .await;
            reply.view.zip(reply.payload)
        }
    }

    /// Send a `BootstrapRecord` to all replicas in this client's membership.
    ///
    /// Used with a standalone client (membership=[R4]) to pre-load a fresh
    /// replica with a record before it joins the group.
    pub fn bootstrap_record(&self, payload: U::Payload, view: SharedView<T::Address>) {
        let sync = self.inner.sync.lock().unwrap();
        for address in &sync.view.membership {
            self.inner.transport.do_send(
                address,
                BootstrapRecord {
                    payload: payload.clone(),
                    view: view.clone(),
                },
            );
        }
    }

    /// Broadcast an `AddMember` to all replicas, triggering a view change
    /// that adds the new address to the group membership.
    pub fn add_member(&self, address: T::Address) {
        let sync = self.inner.sync.lock().unwrap();
        for addr in &sync.view.membership {
            self.inner
                .transport
                .do_send(addr, AddMember { address });
        }
    }

    /// Broadcast a `RemoveMember` to all replicas, triggering a view change
    /// that removes the address from the group membership.
    pub fn remove_member(&self, address: T::Address) {
        let sync = self.inner.sync.lock().unwrap();
        for addr in &sync.view.membership {
            self.inner
                .transport
                .do_send(addr, RemoveMember { address });
        }
    }
}
