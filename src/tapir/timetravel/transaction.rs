use crate::tapir::client::{Client, Inner};
use crate::tapir::{Key, Sharded, Timestamp, Value};
use crate::{TapirTransport, TransactionError};
use std::future::Future;
use std::sync::{Arc, Mutex};

/// Time-travel query targeting a specific shard via explicit [`Sharded`] keys.
///
/// This is the low-level API. Most users should prefer
/// [`crate::RoutingTimeTravelTransaction`] (via
/// [`crate::RoutingClient::begin_time_travel`]) which auto-routes plain keys
/// and supports multi-shard scans.
///
/// See [`crate::RoutingTimeTravelTransaction`] for full documentation on use
/// cases, consistency guarantees, and limitations.
pub struct TimeTravelTransaction<K: Key, V: Value, T: TapirTransport<K, V>> {
    timestamp: Timestamp,
    client: Arc<Mutex<Inner<K, V, T>>>,
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> Client<K, V, T> {
    /// Begin a time-travel query at the given timestamp (low-level, explicit shards).
    ///
    /// Returns a [`TimeTravelTransaction`] that reads data as it existed at
    /// `timestamp` with zero side effects on replica state.
    ///
    /// Most users should prefer [`crate::RoutingClient::begin_time_travel`]
    /// which auto-routes plain keys. See [`crate::RoutingTimeTravelTransaction`]
    /// for full documentation on use cases, consistency, and limitations.
    pub fn begin_time_travel(&self, timestamp: Timestamp) -> TimeTravelTransaction<K, V, T> {
        TimeTravelTransaction {
            timestamp,
            client: Arc::clone(&self.inner),
        }
    }
}

impl<K: Key, V: Value, T: TapirTransport<K, V>> TimeTravelTransaction<K, V, T> {
    /// Read a key at the time-travel timestamp.
    pub fn get(
        &self,
        key: impl Into<Sharded<K>>,
    ) -> impl Future<Output = Result<Option<V>, TransactionError>> {
        let key = key.into();
        let client = Arc::clone(&self.client);
        let timestamp = self.timestamp;

        async move {
            let shard_client = Inner::shard_client(&client, key.shard).await;
            let (value, _write_ts) = shard_client.get(key.key, Some(timestamp)).await?;
            Ok(value)
        }
    }

    /// Range scan at the time-travel timestamp.
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
        let timestamp = self.timestamp;

        async move {
            let shard_client = Inner::shard_client(&client, start.shard).await;
            let (results, _ts) =
                shard_client.scan(start.key, end.key, Some(timestamp)).await?;
            Ok(results
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect())
        }
    }
}
