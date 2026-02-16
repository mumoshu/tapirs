use super::{client::{ReadOnlyTransaction, Transaction}, Client, Key, Sharded, Timestamp, Value};
use crate::tapir::shard_router::ShardRouter;
use crate::TapirTransport;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

/// Higher-level TAPIR client with automatic key-to-shard routing.
///
/// # Client layer hierarchy
///
/// ```text
/// RoutingClient           ← plain keys, auto-routing, multi-shard scan
///   └─ TapirClient        ← Sharded<K> (explicit shard), multi-shard prepare/commit
///        └─ ShardClient   ← per-shard IR RPCs (get, scan, prepare, commit/abort)
///             └─ IrClient ← IR consensus protocol
/// ```
///
/// [`super::ShardClient`] is a low-level per-shard RPC stub wrapping
/// [`crate::IrClient`]. [`Client`] (TapirClient) is a mid-level transaction
/// coordinator requiring explicit `Sharded<K>` keys. This `RoutingClient`
/// wraps `Client` + [`ShardRouter`] to accept plain keys and automatically
/// route them to the correct shard. Multi-shard range scans are split into
/// per-shard scans and merged.
pub struct RoutingClient<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> {
    inner: Arc<Client<K, V, T>>,
    router: Arc<R>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> RoutingClient<K, V, T, R> {
    pub fn new(inner: Arc<Client<K, V, T>>, router: Arc<R>) -> Self {
        Self { inner, router }
    }

    pub fn begin(&self) -> RoutingTransaction<K, V, T, R> {
        RoutingTransaction {
            inner: self.inner.begin(),
            router: Arc::clone(&self.router),
        }
    }

    pub fn begin_read_only(&self) -> RoutingReadOnlyTransaction<K, V, T, R> {
        RoutingReadOnlyTransaction {
            inner: self.inner.begin_read_only(),
            router: Arc::clone(&self.router),
        }
    }

    #[cfg(test)]
    pub fn inner(&self) -> &Client<K, V, T> {
        &self.inner
    }
}

pub struct RoutingTransaction<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> {
    inner: Transaction<K, V, T>,
    router: Arc<R>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> RoutingTransaction<K, V, T, R> {
    pub fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        let shard = self.router.route(&key);
        self.inner.get(Sharded { shard, key })
    }

    pub fn put(&self, key: K, value: Option<V>) {
        let shard = self.router.route(&key);
        self.inner.put(Sharded { shard, key }, value);
    }

    pub fn scan(&self, start: K, end: K) -> impl Future<Output = Vec<(K, V)>> {
        let shards = self.router.shards_for_range(&start, &end);

        // Eagerly create per-shard scan futures (each is 'static).
        let shard_scans: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                self.inner.scan(
                    Sharded { shard, key: start.clone() },
                    Sharded { shard, key: end.clone() },
                )
            })
            .collect();

        async move {
            let mut merged = BTreeMap::<K, V>::new();
            for scan_fut in shard_scans {
                for (k, v) in scan_fut.await {
                    merged.insert(k, v);
                }
            }
            merged.into_iter().collect()
        }
    }

    pub fn commit(self) -> impl Future<Output = Option<Timestamp>> {
        self.inner.commit()
    }
}

pub struct RoutingReadOnlyTransaction<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>>
{
    inner: ReadOnlyTransaction<K, V, T>,
    router: Arc<R>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>>
    RoutingReadOnlyTransaction<K, V, T, R>
{
    pub fn get(&self, key: K) -> impl Future<Output = Option<V>> {
        let shard = self.router.route(&key);
        self.inner.get(Sharded { shard, key })
    }
}
