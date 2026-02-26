use super::TimeTravelTransaction;
use crate::tapir::routing_client::{retry_transient, RetryConfig};
use crate::tapir::shard_router::ShardRouter;
use crate::tapir::{Key, Sharded, Value};
use crate::{TapirTransport, TransactionError};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

/// Time-travel query with automatic shard routing and multi-shard scan support.
///
/// Reads a snapshot of past data at a caller-specified [`crate::TapirTimestamp`]
/// without any side effects on IR/TAPIR replica state. Unlike
/// [`crate::ReadOnlyTransaction`] which uses quorum reads and records `read_ts`
/// via `commit_get` for point reads and `range_reads` via `commit_scan` for
/// scans (protecting versions and ranges from future overwrites), time-travel
/// queries are purely observational single-replica reads.
///
/// Created via [`crate::RoutingClient::begin_time_travel`]. For the low-level
/// API with explicit [`Sharded`] keys, see [`TimeTravelTransaction`].
///
/// # Recommended use cases
///
/// - **Debugging**: Inspect what a key's value was at a specific point in time
///   to diagnose issues without perturbing live state.
/// - **Auditing / forensics**: Read historical snapshots for compliance or
///   post-incident analysis.
/// - **Backup verification**: Confirm that data at a known commit timestamp
///   matches expectations.
/// - **Historical analytics**: Aggregate or compare data across different
///   points in time (e.g., diff two snapshots).
///
/// # Not intended for
///
/// - **Consistent cross-shard snapshots at a past time**: Single-replica reads
///   are eventually consistent. A time-travel scan across shards may see
///   slightly different replication lag per shard.
///   [`crate::ReadOnlyTransaction`] provides quorum-consistent reads but only
///   at the *current* time, not at an arbitrary past timestamp — there is
///   currently no quorum-consistent historical read API.
/// - **Real-time reads of very recent writes**: A replica may not yet have
///   applied a FINALIZE for a write committed milliseconds ago. If the write
///   must be visible, use `begin_read_only()` which guarantees visibility via
///   quorum.
///
/// # Safety: read-only by design
///
/// Time-travel queries expose no `put()` method — writes are structurally
/// impossible. This prevents accidental "write back in time" scenarios where
/// stale data could bypass OCC conflict detection. If you need to read and
/// then write, use a regular RW transaction (`begin()`) which participates in
/// OCC validation.
///
/// # Consistency
///
/// Single-replica read via `invoke_unlogged()` — may miss recently-finalized
/// writes (same as RW `get()`). For past timestamps, replicas are typically
/// converged. Use `begin_read_only()` for linearizable reads at the
/// current time (it does not support reading at an arbitrary past timestamp).
///
/// # Limitations
///
/// LSM compaction drops old versions (keeps only the latest per key).
/// Time-travel queries can only see data that has not been compacted.
/// For long-term historical access, configure compaction retention
/// policies or use CDC-based snapshots.
pub struct RoutingTimeTravelTransaction<
    K: Key,
    V: Value,
    T: TapirTransport<K, V>,
    R: ShardRouter<K>,
> {
    pub(crate) inner: TimeTravelTransaction<K, V, T>,
    pub(crate) router: Arc<R>,
    pub(crate) retry_config: RetryConfig,
    pub(crate) on_retry: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>, R: ShardRouter<K>>
    RoutingTimeTravelTransaction<K, V, T, R>
{
    pub fn get(
        &self,
        key: K,
    ) -> impl Future<Output = Result<Option<V>, TransactionError>> + use<'_, K, V, T, R> {
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

    pub fn scan(
        &self,
        start: K,
        end: K,
    ) -> impl Future<Output = Result<Vec<(K, V)>, TransactionError>> + use<'_, K, V, T, R> {
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
