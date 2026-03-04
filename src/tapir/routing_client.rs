use super::{client::{ReadOnlyTransaction, Transaction}, Client, Key, Sharded, Timestamp, TransactionError, Value};
use crate::tapir::shard_router::ShardRouter;
use crate::TapirTransport;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for automatic OutOfRange retry in [`RoutingClient`].
///
/// When a shard is re-ranged during resharding, operations on keys that
/// moved to a new shard receive `OutOfRange`. The RoutingClient can
/// automatically re-route the key and retry after a backoff delay,
/// giving the routing directory time to update.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before propagating the error.
    /// Each attempt re-reads the routing directory and re-routes the key.
    /// Default: 2.
    pub max_retries: u8,
    /// Initial backoff delay before the first retry. Subsequent retries
    /// double the delay (exponential backoff). Under `start_paused = true`,
    /// this is simulated time and does not affect wall-clock duration.
    /// Default: 100ms.
    pub initial_backoff: Duration,
    /// Maximum backoff delay (caps exponential growth). Default: 2s.
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

/// Retry an operation that may fail with transient `TransactionError`s.
///
/// Calls `op()` up to `config.max_retries + 1` times. On each `OutOfRange`
/// or `PrepareConflict` failure (except the last attempt), sleeps for
/// exponential backoff via `sleep_fn`, invokes the retry callback, then
/// calls `op()` again.
///
/// The `op` closure should re-read the routing directory on each call —
/// the directory may have been updated during the backoff sleep.
pub(crate) async fn retry_transient<F, Fut, R, Sl, SlFut>(
    config: &RetryConfig,
    on_retry: &Option<Arc<dyn Fn(&str) + Send + Sync>>,
    op_name: &str,
    sleep_fn: Sl,
    mut op: F,
) -> Result<R, TransactionError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, TransactionError>>,
    Sl: Fn(Duration) -> SlFut,
    SlFut: Future<Output = ()>,
{
    let mut backoff = config.initial_backoff;

    for attempt in 0..=config.max_retries {
        match op().await {
            Ok(v) => return Ok(v),
            Err(TransactionError::OutOfRange | TransactionError::PrepareConflict)
                if attempt < config.max_retries =>
            {
                sleep_fn(backoff).await;
                if let Some(cb) = on_retry {
                    cb(&format!(
                        "{op_name}: transient error, retry attempt={} backoff={backoff:?}",
                        attempt + 1,
                    ));
                }
                backoff = (backoff * 2).min(config.max_backoff);
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

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
    retry_config: RetryConfig,
    /// Optional callback invoked on each OutOfRange retry. Receives a
    /// human-readable message describing the retry (attempt number, backoff).
    on_retry: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> RoutingClient<K, V, T, R> {
    pub fn new(inner: Arc<Client<K, V, T>>, router: Arc<R>) -> Self {
        Self {
            inner,
            router,
            retry_config: RetryConfig::default(),
            on_retry: None,
        }
    }

    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    pub fn with_retry_callback(
        mut self,
        cb: impl Fn(&str) + Send + Sync + 'static,
    ) -> Self {
        self.on_retry = Some(Arc::new(cb));
        self
    }

    pub fn begin(&self) -> RoutingTransaction<K, V, T, R> {
        RoutingTransaction {
            inner: self.inner.begin(),
            router: Arc::clone(&self.router),
            retry_config: self.retry_config.clone(),
            on_retry: self.on_retry.clone(),
        }
    }

    pub fn begin_read_only(&self, clock_skew_uncertainty_bound: Duration) -> RoutingReadOnlyTransaction<K, V, T, R> {
        RoutingReadOnlyTransaction {
            inner: self.inner.begin_read_only(clock_skew_uncertainty_bound),
            router: Arc::clone(&self.router),
            retry_config: self.retry_config.clone(),
            on_retry: self.on_retry.clone(),
        }
    }

    /// Begin a time-travel query at the given timestamp.
    ///
    /// See [`crate::RoutingTimeTravelTransaction`] for full documentation on
    /// use cases, consistency guarantees, and limitations.
    pub fn begin_time_travel(
        &self,
        timestamp: Timestamp,
    ) -> crate::tapir::timetravel::RoutingTimeTravelTransaction<K, V, T, R> {
        crate::tapir::timetravel::RoutingTimeTravelTransaction {
            inner: self.inner.begin_time_travel(timestamp),
            router: Arc::clone(&self.router),
            retry_config: self.retry_config.clone(),
            on_retry: self.on_retry.clone(),
        }
    }

}

pub struct RoutingTransaction<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> {
    inner: Transaction<K, V, T>,
    router: Arc<R>,
    retry_config: RetryConfig,
    on_retry: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>> RoutingTransaction<K, V, T, R> {
    /// Read a key's value from the shard determined by the routing directory.
    ///
    /// If the target shard returns `OutOfRange` (the key moved to a different
    /// shard during resharding), the routing directory is re-consulted and
    /// the operation is retried after a backoff delay, giving time for
    /// membership/directory info to propagate to the client.
    ///
    /// The failed read is never recorded in the transaction's read set.
    pub fn get(&self, key: K) -> impl Future<Output = Result<Option<V>, TransactionError>> + use<'_, K, V, T, R> {
        let router = Arc::clone(&self.router);
        let config = self.retry_config.clone();
        let on_retry = self.on_retry.clone();

        async move {
            retry_transient(&config, &on_retry, "get", T::sleep, || {
                let shard = router.route(&key);
                let key = key.clone();
                async move {
                    match shard {
                        Some(s) => self.inner.get(Sharded { shard: s, key }).await,
                        None => Err(TransactionError::OutOfRange),
                    }
                }
            })
            .await
        }
    }

    pub fn put(&self, key: K, value: Option<V>) {
        let shard = self.router.route(&key)
            .expect("route unavailable at put — preceding get() should have caught this");
        self.inner.put(Sharded { shard, key }, value);
    }

    /// Range scan across one or more shards, with automatic OutOfRange retry.
    ///
    /// If any sub-scan returns `OutOfRange`, the entire scan is retried from
    /// scratch: the routing directory is re-consulted, the range is re-split,
    /// and all sub-scans are re-issued.
    /// Range scan across one or more shards, with automatic OutOfRange retry.
    ///
    /// The scan range is clipped to each shard's key range before sending,
    /// so a scan(a, z) spanning shards [None, g), [g, n), [n, None) sends
    /// scan(a, g) to shard 0, scan(g, n) to shard 2, scan(n, z) to shard 1.
    /// If any sub-scan returns `OutOfRange`, the entire scan is retried from
    /// scratch: the routing directory is re-consulted, the range is re-split,
    /// and all sub-scans are re-issued.
    pub fn scan(&self, start: K, end: K) -> impl Future<Output = Result<Vec<(K, V)>, TransactionError>> + use<'_, K, V, T, R> {
        let router = Arc::clone(&self.router);
        let config = self.retry_config.clone();
        let on_retry = self.on_retry.clone();

        async move {
            retry_transient(&config, &on_retry, "scan", T::sleep, || {
                let ranges = router.scan_ranges(&start, &end);

                let shard_scans: Vec<_> = ranges
                    .into_iter()
                    .map(|(shard, clipped_start, clipped_end)| {
                        self.inner.scan(
                            Sharded { shard, key: clipped_start },
                            Sharded { shard, key: clipped_end },
                        )
                    })
                    .collect();

                async move {
                    let mut merged = BTreeMap::<K, V>::new();
                    for scan_fut in shard_scans {
                        for (k, v) in scan_fut.await? {
                            merged.insert(k, v);
                        }
                    }
                    Ok(merged.into_iter().collect())
                }
            })
            .await
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
    retry_config: RetryConfig,
    on_retry: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>>
    RoutingReadOnlyTransaction<K, V, T, R>
{
    pub fn get(&self, key: K) -> impl Future<Output = Result<Option<V>, TransactionError>> + use<'_, K, V, T, R> {
        let router = Arc::clone(&self.router);
        let config = self.retry_config.clone();
        let on_retry = self.on_retry.clone();

        async move {
            retry_transient(&config, &on_retry, "get", T::sleep, || {
                let shard = router.route(&key);
                let key = key.clone();
                async move {
                    match shard {
                        Some(s) => self.inner.get(Sharded { shard: s, key }).await,
                        None => Err(TransactionError::OutOfRange),
                    }
                }
            })
            .await
        }
    }

    pub fn scan(&self, start: K, end: K) -> impl Future<Output = Result<Vec<(K, V)>, TransactionError>> + use<'_, K, V, T, R> {
        let router = Arc::clone(&self.router);
        let config = self.retry_config.clone();
        let on_retry = self.on_retry.clone();

        async move {
            retry_transient(&config, &on_retry, "scan", T::sleep, || {
                let ranges = router.scan_ranges(&start, &end);

                let shard_scans: Vec<_> = ranges
                    .into_iter()
                    .map(|(shard, clipped_start, clipped_end)| {
                        self.inner.scan(
                            Sharded { shard, key: clipped_start },
                            Sharded { shard, key: clipped_end },
                        )
                    })
                    .collect();

                async move {
                    let mut merged = BTreeMap::<K, V>::new();
                    for scan_fut in shard_scans {
                        for (k, v) in scan_fut.await? {
                            merged.insert(k, v);
                        }
                    }
                    Ok(merged.into_iter().collect())
                }
            })
            .await
        }
    }
}
