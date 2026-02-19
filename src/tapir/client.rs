use super::{Key, ShardClient, ShardNumber, Sharded, Timestamp, TransactionError, Value};
use crate::{
    util::join, IrClientId, OccPrepareResult, OccScanEntry, OccTransaction, OccTransactionId,
    TapirTransport,
};
use futures::future::join_all;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Context,
    time::Duration,
};
use futures::future::Either;
use tracing::trace;

pub struct Client<K: Key, V: Value, T: TapirTransport<K, V>> {
    inner: Arc<Mutex<Inner<K, V, T>>>,
    next_transaction_number: AtomicU64,
}

pub struct Inner<K: Key, V: Value, T: TapirTransport<K, V>> {
    id: IrClientId,
    clients: HashMap<ShardNumber, ShardClient<K, V, T>>,
    transport: T,
    rng: crate::Rng,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Inner<K, V, T> {
    fn shard_client(
        this: &Arc<Mutex<Self>>,
        shard: ShardNumber,
    ) -> impl Future<Output = ShardClient<K, V, T>> + Send + 'static {
        let this = Arc::clone(this);

        async move {
            let future = {
                let lock = this.lock().unwrap();
                if let Some(client) = lock.clients.get(&shard) {
                    return client.clone();
                }
                lock.transport.shard_addresses(shard)
            };

            let membership = future.await;

            let mut lock = this.lock().unwrap();
            let lock = &mut *lock;
            lock.clients
                .entry(shard)
                .or_insert_with(|| {
                    ShardClient::new(lock.rng.fork(), lock.id, shard, membership, lock.transport.clone())
                })
                .clone()
        }
    }
}

pub struct Transaction<K: Key, V: Value, T: TapirTransport<K, V>> {
    id: OccTransactionId,
    client: Arc<Mutex<Inner<K, V, T>>>,
    inner: Arc<Mutex<TransactionInner<K, V>>>,
}

struct TransactionInner<K: Key, V: Value> {
    inner: OccTransaction<K, V, Timestamp>,
    read_cache: HashMap<Sharded<K>, Option<V>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Client<K, V, T> {
    /// Force a client-initiated view change for the given shard.
    ///
    /// In IR/TAPIR, any client can trigger a view change — there is no
    /// designated leader. This delegates to the IR client's
    /// `force_view_change`, which bumps the view and sends `DoViewChange`
    /// with `from_client: true` to all replicas in the shard.
    pub fn force_view_change(&self, shard: ShardNumber) {
        let inner = self.inner.lock().unwrap();
        if let Some(client) = inner.clients.get(&shard) {
            client.inner.force_view_change();
        }
    }

    pub fn new(mut rng: crate::Rng, transport: T) -> Self {
        let id = IrClientId::new(&mut rng);
        let txn_number = rng.random_u64();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                id,
                clients: Default::default(),
                transport,
                rng,
            })),
            next_transaction_number: AtomicU64::new(txn_number),
        }
    }

    pub fn begin(&self) -> Transaction<K, V, T> {
        let transaction_id = OccTransactionId {
            client_id: self.inner.lock().unwrap().id,
            number: self.next_transaction_number.fetch_add(1, Ordering::Relaxed),
        };
        Transaction {
            id: transaction_id,
            client: Arc::clone(&self.inner),
            inner: Arc::new(Mutex::new(TransactionInner {
                inner: Default::default(),
                read_cache: Default::default(),
            })),
        }
    }
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Transaction<K, V, T> {
    pub fn get(&self, key: impl Into<Sharded<K>>) -> impl Future<Output = Result<Option<V>, TransactionError>> {
        let key = key.into();
        let client = Arc::clone(&self.client);
        let inner = Arc::clone(&self.inner);

        async move {
            let client = Inner::shard_client(&client, key.shard).await;

            {
                let lock = inner.lock().unwrap();

                // Read own writes.
                if let Some(write) = lock.inner.write_set.get(&key) {
                    return Ok(write.as_ref().cloned());
                }

                // Consistent reads.
                if let Some(read) = lock.read_cache.get(&key) {
                    return Ok(read.as_ref().cloned());
                }
            }

            let (value, timestamp) = client.get(key.key.clone(), None).await?;

            let mut lock = inner.lock().unwrap();

            // Read own writes.
            if let Some(write) = lock.inner.write_set.get(&key) {
                return Ok(write.as_ref().cloned());
            }

            // Consistent reads.
            if let Some(read) = lock.read_cache.get(&key) {
                return Ok(read.as_ref().cloned());
            }

            lock.read_cache.insert(key.clone(), value.clone());
            lock.inner.add_read(key, timestamp);
            Ok(value)
        }
    }

    pub fn put(&self, key: impl Into<Sharded<K>>, value: Option<V>) {
        let key = key.into();
        let mut lock = self.inner.lock().unwrap();
        lock.inner.add_write(key, value);
    }

    /// Range scan on a single shard. Returns key-value pairs in
    /// `[start.key, end.key]`, overlaying the transaction's own buffered writes.
    /// The caller provides the shard explicitly via `start.shard` (must equal
    /// `end.shard`). Multi-shard scans should be split by the caller.
    pub fn scan(
        &self,
        start: Sharded<K>,
        end: Sharded<K>,
    ) -> impl Future<Output = Result<Vec<(K, V)>, TransactionError>> {
        assert_eq!(
            start.shard, end.shard,
            "scan start and end must target the same shard"
        );
        let shard = start.shard;
        let client = Arc::clone(&self.client);
        let inner = Arc::clone(&self.inner);

        async move {
            let sc = Inner::shard_client(&client, shard).await;

            let (results, ts) = sc.scan(start.key.clone(), end.key.clone(), None).await?;

            // Record the scan entry for phantom prevention.
            {
                let mut lock = inner.lock().unwrap();
                lock.inner.scan_set.push(OccScanEntry {
                    shard,
                    start_key: start.key.clone(),
                    end_key: end.key.clone(),
                    timestamp: ts,
                });
            }

            let mut merged = std::collections::BTreeMap::<K, Option<V>>::new();

            for (k, v) in results {
                merged.insert(k, v);
            }

            // Overlay the transaction's own buffered writes/deletes.
            {
                let lock = inner.lock().unwrap();
                for (sharded_key, value) in &lock.inner.write_set {
                    if sharded_key.shard == shard
                        && sharded_key.key >= start.key
                        && sharded_key.key <= end.key
                    {
                        merged.insert(sharded_key.key.clone(), value.clone());
                    }
                }
            }

            // Filter out tombstones (None values) and collect.
            Ok(merged
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect())
        }
    }

    fn commit_inner(&self, only_prepare: bool) -> impl Future<Output = Option<Timestamp>> + use<K, V, T> {
        let id = self.id;
        let client = self.client.clone();
        let inner = self.inner.clone();

        let transaction = {
            let lock = inner.lock().unwrap();
            Arc::new(lock.inner.clone())
        };

        let min_commit_timestamp = max_read_timestamp(&transaction).saturating_add(1);
        let mut timestamp = {
            let client = self.client.lock().unwrap();
            Timestamp {
                time: client.transport.time().max(min_commit_timestamp),
                client_id: client.id,
            }
        };
        let participants = transaction.participants();

        async move {
            // Writes are buffered; make sure the shard clients exist.
            for key in transaction.write_set.keys() {
                Inner::shard_client(&client, key.shard).await;
            }

            let mut remaining_tries = 3u8;

            loop {
                let future = {
                    let client = client.lock().unwrap();
                    join(participants.iter().map(|shard| {
                        let shard_client = client.clients.get(shard).unwrap();
                        let future = shard_client.prepare(id, &transaction, timestamp);
                        (*shard, future)
                    }))
                };

                let results = future
                    .until(
                        |results: &BTreeMap<ShardNumber, OccPrepareResult<Timestamp>>,
                         _cx: &mut Context<'_>| {
                            results.values().any(|v| {
                                v.is_fail()
                                    || v.is_abstain()
                                    || v.is_too_late()
                                    || v.is_too_old()
                                    || v.is_out_of_range()
                            })
                        },
                    )
                    .await;

                if results.values().any(|v| v.is_too_late() || v.is_too_old()) {
                    continue;
                }

                if participants.len() == 1 && let Some(OccPrepareResult::Retry { proposed }) = results.values().next() && let Some(new_remaining_tries) = remaining_tries.checked_sub(1) {
                    remaining_tries = new_remaining_tries;

                    let new_time =  client.lock().unwrap().transport.time().max(proposed.saturating_add(1)).max(min_commit_timestamp);
                    if new_time != timestamp.time {
                        timestamp.time = new_time;
                        continue;
                    }
                }

                // Ok if all participant shards are ok.
                let ok = results.len() == participants.len() && results.values().all(|r| r.is_ok());

                if !only_prepare {
                    let future = {
                        let client = client.lock().unwrap();
                        join_all(participants.iter().map(|shard| {
                            let shard_client = client.clients.get(shard).unwrap();
                            shard_client.end(id, &transaction, timestamp, ok)
                        }))
                    };

                    future.await;
                }

                if ok && remaining_tries != 3 {
                    trace!("retry actually worked!");
                }

                return Some(timestamp).filter(|_| ok);
            }
        }
    }

    #[doc(hidden)]
    pub fn only_prepare(self) -> impl Future<Output = Option<Timestamp>> {
        self.commit_inner(true)
    }

    pub fn commit(self) -> impl Future<Output = Option<Timestamp>> {
        self.commit_inner(false)
    }

    #[doc(hidden)]
    pub fn commit2(
        self,
        inject_fault: Option<Duration>,
    ) -> impl Future<Output = Option<Timestamp>> {
        let inner = self.commit();

        async move {
            if let Some(duration) = inject_fault {
                let sleep = T::sleep(duration);
                futures::pin_mut!(sleep);
                futures::pin_mut!(inner);
                match futures::future::select(sleep, inner).await {
                    Either::Left(_) => std::future::pending::<Option<Timestamp>>().await,
                    Either::Right((result, _)) => result,
                }
            } else {
                inner.await
            }
        }
    }
}

/// Read-only transaction for TAPIR (Section 6.1 of the paper).
///
/// # Context
///
/// TAPIR currently supports read-write transactions (OCC prepare/commit) and unlogged gets
/// (fast but potentially stale). Read-only transactions fill the gap: **consistent, up-to-date
/// reads** at a snapshot timestamp without prepare/commit overhead.
///
/// Per the TAPIR paper (Section 6.1):
/// - **Fast path** (1 replica, 1 RTT): if the replica has a "validated" version
///   (read_ts >= snapshot_ts)
/// - **Slow path** (quorum, 2 RTT): QuorumRead via IR inconsistent operation — FINALIZE
///   triggers `ExecInconsistent` which updates read timestamps and returns values. Record
///   entry ensures view-change durability.
///
/// # Design Decisions
///
/// - **QuorumRead goes through IR inconsistent operations**, faithful to the paper.
/// - **Execution happens at FINALIZE** (per IR protocol: "On FINALIZE, replicas upcall into
///   the application protocol with ExecInconsistent(op)"). No propose-time execution.
/// - **New associated type `IR`** added to Upcalls trait (parallel to `CR`), so
///   `exec_inconsistent` can return results.
/// - **`FinalizeInconsistentReply`** message added so clients receive results from FINALIZE.
/// - **2 RTTs** on the slow path: propose (f+1 replies) + finalize (f+1 replies with results).
///   Concurrent writes that prepare before QuorumRead's FINALIZE are concurrent transactions,
///   not "later" writes — this is correct per the paper's guarantees.
///
/// # Rejected Alternatives and Correctness Analysis
///
/// ## Why adding `propose_inconsistent` is wrong
///
/// The IR protocol (Section 5 of the paper) defines inconsistent operation processing as:
///
/// 1. The client sends `<PROPOSE, id, op>` to all replicas.
/// 2. Each replica writes id and op to its record as TENTATIVE, then responds to the client
///    with `<REPLY, id>`.
/// 3. Once the client receives replies from at least f+1 replicas, the client sends
///    `<FINALIZE, id>` to all replicas.
/// 4. On FINALIZE, replicas upcall into the application protocol with `ExecInconsistent(op)`
///    and mark the record entry as FINALIZED.
///
/// There is **no upcall at PROPOSE time**. Replicas only record the operation as TENTATIVE and
/// reply with an acknowledgment — they do not execute anything. Adding a `propose_inconsistent`
/// hook would violate the IR protocol specification by introducing an upcall that IR does not
/// define. The only application-level upcall for inconsistent operations is `ExecInconsistent`,
/// and it happens exclusively at FINALIZE.
///
/// A `propose_inconsistent` hook would also break IR's guarantees: IR ensures that inconsistent
/// operations are durable by recording them before execution. If we executed at PROPOSE time,
/// results could be returned to the client before the operation is recorded at a quorum, meaning
/// a view change could lose the operation while the client already acted on its results.
///
/// ## Why executing QuorumRead as an IR consensus operation is unnecessary
///
/// IR consensus operations (like TAPIR's Prepare) use a 2-phase protocol with a `decide`
/// function that merges results from a superquorum (f+1 matching replies or 3f/2+1 total).
/// This machinery exists because consensus operations need **agreement** — all replicas must
/// converge on the same result.
///
/// QuorumRead does not need agreement. Each replica independently reads the value at the
/// snapshot timestamp and sets the read timestamp. The client picks the result with the highest
/// write timestamp — there is no need for replicas to agree or for a decide function to
/// reconcile conflicting replies. Using consensus operations would add unnecessary overhead
/// (superquorum instead of simple quorum) and complexity (a decide function that just passes
/// through results) for no correctness benefit.
///
/// The paper explicitly places QuorumRead in the inconsistent operation category: it is a
/// "simple quorum" operation where each replica executes independently, and correctness comes
/// from the read timestamp being set at a quorum of replicas (f+1), which is exactly what IR
/// inconsistent operations provide.
///
/// ## Why executing QuorumRead at IR inconsistent PROPOSE (not FINALIZE) is incorrect
///
/// Consider what happens if `exec_inconsistent` runs at PROPOSE time and returns results
/// immediately:
///
/// 1. Client sends `PROPOSE QuorumRead(key, snapshot_ts)` to all replicas.
/// 2. Replica executes QuorumRead: reads value, sets `read_ts = snapshot_ts`, returns result
///    in `ReplyInconsistent`.
/// 3. Client receives f+1 replies with results and picks the one with the highest write
///    timestamp.
///
/// The problem: **the client now has results, but the operation is only TENTATIVE at f+1
/// replicas**. The client hasn't sent FINALIZE yet. If a view change happens between receiving
/// PROPOSE replies and sending FINALIZE:
///
/// - The TENTATIVE QuorumRead entries may be lost during view change reconciliation (they
///   require only f+1 replicas, and view change contacts 2f+1).
/// - However, the replicas that did execute have already set `read_ts`, and the replicas that
///   didn't have **not** set `read_ts`.
/// - A concurrent write could successfully prepare at the replicas that never received the
///   PROPOSE, bypassing the read timestamp protection.
///
/// More fundamentally, executing at PROPOSE time means TAPIR returns read results to the client
/// **before the read_ts has been durably persisted** at a quorum. The read_ts is what blocks
/// future writes from overwriting the version the client read. If TAPIR returns results before
/// read_ts is persisted, and a failure occurs, the client may have acted on a value that a later
/// write can now overwrite — violating the read-only transaction guarantee that "a later write
/// transaction cannot overwrite the version returned by the read-only transaction."
///
/// By executing at FINALIZE time:
/// - FINALIZE is only sent after f+1 replicas have the operation in their record (TENTATIVE).
/// - At FINALIZE, replicas execute `ExecInconsistent(QuorumRead)` which sets `read_ts` and
///   returns results.
/// - The client receives results only after `read_ts` is set at the executing replicas.
/// - The record entry is FINALIZED, ensuring durability across view changes.
///
/// **Concurrent writes that prepare between PROPOSE and FINALIZE** are not a correctness
/// concern. These are *concurrent* transactions, not *later* transactions. The paper's guarantee
/// (2) states: "a later write transaction cannot overwrite the version returned by the read-only
/// transaction." "Later" means writes starting **after** the QuorumRead completes (after
/// FINALIZE). Concurrent writes are expected and handled correctly — they may or may not see
/// the read_ts depending on timing, which is the correct behavior for concurrent operations.
pub struct ReadOnlyTransaction<K: Key, V: Value, T: TapirTransport<K, V>> {
    snapshot_ts: Timestamp,
    client: Arc<Mutex<Inner<K, V, T>>>,
    read_cache: Arc<Mutex<HashMap<Sharded<K>, Option<V>>>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Client<K, V, T> {
    pub fn begin_read_only(&self) -> ReadOnlyTransaction<K, V, T> {
        let inner = self.inner.lock().unwrap();
        let snapshot_ts = Timestamp {
            time: inner.transport.time(),
            client_id: inner.id,
        };
        ReadOnlyTransaction {
            snapshot_ts,
            client: Arc::clone(&self.inner),
            read_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> ReadOnlyTransaction<K, V, T> {
    /// Range scan on a single shard. Returns key-value pairs in
    /// `[start.key, end.key]` at the snapshot timestamp.
    ///
    /// Fast path: try `scan_validated` (1 RTT) — succeeds if a prior
    /// QuorumScan recorded a covering range_read at f+1 replicas.
    /// Slow path: fall through to `quorum_scan` (2 RTT) — records
    /// range_read via `commit_scan` at FINALIZE time.
    pub fn scan(
        &self,
        start: Sharded<K>,
        end: Sharded<K>,
    ) -> impl Future<Output = Result<Vec<(K, V)>, TransactionError>> {
        assert_eq!(
            start.shard, end.shard,
            "scan start and end must target the same shard"
        );
        let client = Arc::clone(&self.client);
        let read_cache = Arc::clone(&self.read_cache);
        let snapshot_ts = self.snapshot_ts;

        async move {
            let shard_client = Inner::shard_client(&client, start.shard).await;

            // Fast path: check if replicas have covering range_reads.
            let scan_results = if let Some(results) = shard_client
                .scan_validated(start.key.clone(), end.key.clone(), snapshot_ts)
                .await
            {
                results
            } else {
                // Slow path: quorum scan via IR inconsistent op.
                shard_client
                    .quorum_scan(start.key.clone(), end.key.clone(), snapshot_ts)
                    .await?
            };

            // Populate read cache (don't overwrite earlier reads).
            {
                let mut cache = read_cache.lock().unwrap();
                for (key, value, _write_ts) in &scan_results {
                    let sharded = Sharded {
                        shard: start.shard,
                        key: key.clone(),
                    };
                    cache.entry(sharded).or_insert_with(|| value.clone());
                }
            }

            // Filter tombstones and return.
            Ok(scan_results
                .into_iter()
                .filter_map(|(k, v, _ts)| v.map(|v| (k, v)))
                .collect())
        }
    }

    pub fn get(&self, key: impl Into<Sharded<K>>) -> impl Future<Output = Result<Option<V>, TransactionError>> {
        let key = key.into();
        let client = Arc::clone(&self.client);
        let read_cache = Arc::clone(&self.read_cache);
        let snapshot_ts = self.snapshot_ts;

        async move {
            // Check read cache for consistent reads within the transaction.
            {
                let cache = read_cache.lock().unwrap();
                if let Some(value) = cache.get(&key) {
                    return Ok(value.clone());
                }
            }

            let shard_client = Inner::shard_client(&client, key.shard).await;

            // Fast path: check if one replica has a validated version.
            if let Some((value, _write_ts)) =
                shard_client.read_validated(key.key.clone(), snapshot_ts).await
            {
                let mut cache = read_cache.lock().unwrap();
                cache.entry(key).or_insert(value.clone());
                return Ok(value);
            }

            // Slow path: quorum read via IR inconsistent op.
            let (value, _write_ts) =
                shard_client.quorum_read(key.key.clone(), snapshot_ts).await?;

            let mut cache = read_cache.lock().unwrap();
            cache.entry(key).or_insert(value.clone());
            Ok(value)
        }
    }
}

pub fn max_read_timestamp<K, V>(transaction: &OccTransaction<K, V, Timestamp>) -> u64 {
    let read_max = transaction
        .read_set
        .values()
        .map(|v| v.time)
        .max()
        .unwrap_or_default();
    let scan_max = transaction
        .scan_set
        .iter()
        .map(|e| e.timestamp.time)
        .max()
        .unwrap_or_default();
    read_max.max(scan_max)
}
