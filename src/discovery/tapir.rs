use std::fmt::Display;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::discovery::{
    membership_to_strings, strings_to_membership, DiscoveryError, RemoteShardDirectory,
    ShardDirectoryChange, ShardDirectoryChangeSet,
};
use crate::tapir::dns_shard_client::{DnsRefreshingShardClient, DnsResolveError};
use crate::tapir::{Client as TapirClient, KeyRange, ShardClient, ShardNumber, Sharded};
use crate::{IrClientId, IrMembership, TapirTransport};

const DISCOVERY_SHARD: ShardNumber = ShardNumber(0);
const SCAN_START: &str = "shard:";
const SCAN_END: &str = "shard:~";
const ROUTE_CHANGE_LAST_INDEX: &str = "route_change_last_index";

fn route_change_key(idx: u64) -> String {
    format!("route_change:{idx}")
}

fn shard_key(shard: ShardNumber) -> String {
    format!("shard:{}", shard.0)
}

fn parse_shard_number(key: &str) -> Option<ShardNumber> {
    key.strip_prefix("shard:")
        .and_then(|s| s.parse::<u32>().ok())
        .map(ShardNumber)
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: serde::Serialize",
    deserialize = "K: serde::de::DeserializeOwned"
))]
struct ShardRecord<K> {
    membership: Vec<String>,
    view: u64,
    status: ShardStatus,
    /// Typed key range (set by `strong_atomic_update_shards`, absent from `put`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_range: Option<KeyRange<K>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardStatus {
    Active,
    Pending,
    Tombstoned,
}

/// Whether reads use linearizable or eventual consistency.
pub enum ReadMode {
    /// RO transactions for linearizable reads. For ShardManager.
    Strong,
    /// Unlogged reads to 1 random replica. For clients/nodes.
    Eventual,
}

/// [`RemoteShardDirectory`] backed by a single-shard TAPIR cluster.
///
/// Stores shard metadata as `"shard:{id}"` → JSON in a TAPIR discovery
/// cluster (shard 0). Two constructors select the read mode:
/// - **Strong** (`with_strong_consistent_read`): RO transactions for
///   linearizable reads. Used by ShardManager.
/// - **Eventual** (`with_eventual_consistent_read`): unlogged reads to
///   1 random replica. Used by clients/nodes via `CachingShardDirectory`.
///
/// Writes always use RW transactions regardless of read mode.
pub struct TapirRemoteShardDirectory<A, T: TapirTransport<String, String>> {
    client: TapirClient<String, String, T>,
    shard_client: ShardClient<String, String, T>,
    dns_client: Option<DnsRefreshingShardClient<String, String, T>>,
    read_mode: ReadMode,
    _phantom: PhantomData<A>,
}

fn parse_record<K: serde::de::DeserializeOwned>(json: &str) -> Result<ShardRecord<K>, DiscoveryError> {
    serde_json::from_str(json)
        .map_err(|e| DiscoveryError::InvalidResponse(format!("invalid JSON: {e}")))
}

fn parse_membership_from<A, K>(record: &ShardRecord<K>) -> Result<IrMembership<A>, DiscoveryError>
where
    A: FromStr + Display + Copy + Eq,
    <A as FromStr>::Err: Display,
{
    strings_to_membership::<A>(&record.membership)
        .map_err(|e| DiscoveryError::InvalidResponse(format!("invalid membership: {e}")))
}

fn to_json<A, K>(membership: &IrMembership<A>, view: u64, status: ShardStatus, key_range: Option<KeyRange<K>>) -> String
where
    A: Copy + Eq + Display,
    K: Serialize,
{
    serde_json::to_string(&ShardRecord {
        membership: membership_to_strings(membership),
        view,
        status,
        key_range,
    })
    .unwrap()
}

impl<A, T> TapirRemoteShardDirectory<A, T>
where
    A: FromStr + Display + Copy + Eq + Send + Sync + 'static,
    <A as FromStr>::Err: Display,
    T: TapirTransport<String, String>,
{
    /// Create with linearizable reads (RO transactions).
    ///
    /// Used by ShardManager for linearizable shard authority reads.
    pub fn with_strong_consistent_read(
        mut rng: crate::Rng,
        membership: IrMembership<T::Address>,
        transport: T,
    ) -> Self {
        let shard_client = ShardClient::new(
            rng.fork(),
            IrClientId::new(&mut rng),
            DISCOVERY_SHARD,
            membership,
            transport.clone(),
        );
        let client = TapirClient::new(rng, transport);
        client.set_shard_client(DISCOVERY_SHARD, shard_client.clone());
        Self {
            client,
            shard_client,
            dns_client: None,
            read_mode: ReadMode::Strong,
            _phantom: PhantomData,
        }
    }

    /// Create with eventual consistent reads (unlogged to 1 random replica).
    ///
    /// Used by clients/nodes via `CachingShardDirectory` PULL.
    pub fn with_eventual_consistent_read(
        mut rng: crate::Rng,
        membership: IrMembership<T::Address>,
        transport: T,
    ) -> Self {
        let shard_client = ShardClient::new(
            rng.fork(),
            IrClientId::new(&mut rng),
            DISCOVERY_SHARD,
            membership,
            transport.clone(),
        );
        let client = TapirClient::new(rng, transport);
        client.set_shard_client(DISCOVERY_SHARD, shard_client.clone());
        Self {
            client,
            shard_client,
            dns_client: None,
            read_mode: ReadMode::Eventual,
            _phantom: PhantomData,
        }
    }

    /// Return the current ShardClient, refreshing from DNS if applicable.
    ///
    /// When DNS mode is active, gets the latest ShardClient from
    /// `DnsRefreshingShardClient` and updates TapirClient's cache so
    /// RO/RW transactions use the current membership.
    fn current_shard_client(&self) -> ShardClient<String, String, T> {
        match &self.dns_client {
            Some(dns) => {
                let sc = dns.get();
                self.client.set_shard_client(DISCOVERY_SHARD, sc.clone());
                sc
            }
            None => self.shard_client.clone(),
        }
    }

    /// Consistent read of a key's raw JSON value.
    ///
    /// Uses the configured read mode: RO transaction for strong (linearizable),
    /// unlogged read for eventual. This is the correct way to read committed
    /// data — RW transaction `get()` is inconsistent (reaches 1 replica,
    /// validated at commit time via OCC, not at read time).
    async fn read_raw(&self, key: &str) -> Result<Option<String>, DiscoveryError> {
        // Refresh shard client cache (no-op if not DNS mode).
        let sc = self.current_shard_client();
        let mode_str = match self.read_mode {
            ReadMode::Strong => "strong",
            ReadMode::Eventual => "eventual",
        };
        tracing::debug!(key, mode = mode_str, "discovery read_raw: starting");
        let result = match self.read_mode {
            ReadMode::Strong => {
                let ro = self.client.begin_read_only();
                ro.get(key.to_string())
                    .await
                    .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))
            }
            ReadMode::Eventual => sc
                .get(key.to_string(), None)
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))
                .map(|(v, _)| v),
        };
        tracing::debug!(key, mode = mode_str, ok = result.is_ok(), "discovery read_raw: completed");
        result
    }
}

impl<A, T> TapirRemoteShardDirectory<A, T>
where
    A: FromStr + Display + Copy + Eq + Send + Sync + 'static,
    <A as FromStr>::Err: Display,
    T: TapirTransport<String, String>,
    T::Address: From<SocketAddr> + Ord,
{
    /// Create with DNS-based membership that resolves periodically.
    ///
    /// Uses [`DnsRefreshingShardClient`] internally — the `ShardClient`'s
    /// membership is automatically updated when resolved IPs change.
    /// All reads and writes use the latest DNS-resolved membership via
    /// [`current_shard_client()`].
    pub async fn with_dns(
        dns_host: String,
        port: u16,
        resolve_interval: Duration,
        read_mode: ReadMode,
        transport: T,
        mut rng: crate::Rng,
    ) -> Result<Self, DnsResolveError> {
        let dns_client = DnsRefreshingShardClient::new(
            dns_host,
            port,
            DISCOVERY_SHARD,
            resolve_interval,
            transport.clone(),
            rng.fork(),
        )
        .await?;

        let shard_client = dns_client.get();
        let client = TapirClient::new(rng, transport);
        client.set_shard_client(DISCOVERY_SHARD, shard_client.clone());

        Ok(Self {
            client,
            shard_client,
            dns_client: Some(dns_client),
            read_mode,
            _phantom: PhantomData,
        })
    }
}

/// Parse a discovery endpoint string and create a [`TapirRemoteShardDirectory`].
///
/// Supports two schemes:
/// - **Static**: `"addr1:port,addr2:port,..."` — comma-separated socket addresses
/// - **DNS**: `"srv://hostname:port"` — headless service name, periodically re-resolved
///
/// DNS mode resolves every 30 seconds via [`DnsRefreshingShardClient`].
pub async fn parse_tapir_endpoint<A, T>(
    endpoint: &str,
    mode: ReadMode,
    transport: T,
    rng: crate::Rng,
) -> Result<TapirRemoteShardDirectory<A, T>, String>
where
    A: FromStr + Display + Copy + Eq + Send + Sync + 'static,
    <A as FromStr>::Err: Display,
    T: TapirTransport<String, String>,
    T::Address: From<SocketAddr> + Ord,
{
    if let Some(rest) = endpoint.strip_prefix("srv://") {
        let (host, port_str) = rest
            .rsplit_once(':')
            .ok_or_else(|| format!("invalid srv:// endpoint (expected host:port): {endpoint}"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|e| format!("invalid port in endpoint '{endpoint}': {e}"))?;
        TapirRemoteShardDirectory::with_dns(
            host.to_string(),
            port,
            Duration::from_secs(30),
            mode,
            transport,
            rng,
        )
        .await
        .map_err(|e| format!("DNS resolution for endpoint '{endpoint}': {e}"))
    } else {
        let addrs: Vec<SocketAddr> = endpoint
            .split(',')
            .map(|s| {
                s.trim()
                    .parse::<SocketAddr>()
                    .map_err(|e| format!("invalid address in endpoint '{endpoint}': {e}"))
            })
            .collect::<Result<_, _>>()?;
        let membership = IrMembership::new(addrs.into_iter().map(T::Address::from).collect());
        Ok(match mode {
            ReadMode::Strong => {
                TapirRemoteShardDirectory::with_strong_consistent_read(rng, membership, transport)
            }
            ReadMode::Eventual => {
                TapirRemoteShardDirectory::with_eventual_consistent_read(rng, membership, transport)
            }
        })
    }
}

impl<A, K, T> RemoteShardDirectory<A, K> for TapirRemoteShardDirectory<A, T>
where
    A: FromStr + Display + Copy + Eq + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static,
    <A as FromStr>::Err: Display,
    K: Clone + std::fmt::Debug + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    T: TapirTransport<String, String>,
{
    async fn weak_get_active_shard_membership(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<A>, u64)>, DiscoveryError> {
        match self.read_raw(&shard_key(shard)).await? {
            Some(json) => {
                let record = parse_record::<K>(&json)?;
                if record.status != ShardStatus::Active {
                    return Ok(None);
                }
                let membership = parse_membership_from(&record)?;
                Ok(Some((membership, record.view)))
            }
            None => Ok(None),
        }
    }

    async fn strong_put_active_shard_view_membership(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let key = shard_key(shard);

        // Consistent pre-read: check tombstone and monotonic view.
        // RW transaction get() is inconsistent (1 replica, OCC-validated
        // at commit), so we must use read_raw() for correctness checks.
        if let Some(json) = self.read_raw(&key).await? {
            let record = parse_record::<K>(&json)?;
            if record.status == ShardStatus::Tombstoned {
                return Err(DiscoveryError::Tombstoned);
            }
            if record.status == ShardStatus::Pending {
                return Err(DiscoveryError::NotActive);
            }
            if record.view >= view {
                return Ok(());
            }
        }

        let new_json = to_json::<_, K>(&membership, view, ShardStatus::Active, None);

        for _attempt in 0..5 {
            let txn = self.client.begin();

            // RW get for OCC read-set tracking. May return stale data
            // (routed to a replica outside the quorum that committed the
            // latest value, or FINALIZE not yet applied). OCC validates
            // the read at commit time — stale read → commit aborts → retry.
            let _ = txn
                .get(key.clone())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            txn.put(key.clone(), Some(new_json.clone()));

            if txn.commit().await.is_some() {
                // Allow spawned FINALIZE tasks to apply MVCC writes
                // (invoke_inconsistent sends FINALIZE via do_send which
                // spawns tasks) and advance the clock so subsequent RO
                // reads use a later timestamp than the committed data.
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                return Ok(());
            }
        }

        Err(DiscoveryError::ConnectionFailed(
            "put failed after retries".to_string(),
        ))
    }

    async fn strong_remove_shard(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        let key = shard_key(shard);

        // Consistent pre-read to verify shard exists and isn't tombstoned.
        let pre_read = self.read_raw(&key).await?;
        match &pre_read {
            Some(json) => {
                let record = parse_record::<K>(json)?;
                if record.status == ShardStatus::Tombstoned {
                    return Err(DiscoveryError::NotFound);
                }
            }
            None => return Err(DiscoveryError::NotFound),
        }

        // Build tombstone from the consistent pre-read value.
        let mut record = parse_record::<K>(pre_read.as_ref().unwrap())?;
        record.status = ShardStatus::Tombstoned;
        let tombstone_json = serde_json::to_string(&record).unwrap();

        for _attempt in 0..5 {
            let txn = self.client.begin();

            // RW get for OCC read-set tracking (inconsistent — may be stale).
            let _ = txn
                .get(key.clone())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            txn.put(key.clone(), Some(tombstone_json.clone()));

            if txn.commit().await.is_some() {
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                return Ok(());
            }
        }

        Err(DiscoveryError::ConnectionFailed(
            "remove failed after retries".to_string(),
        ))
    }

    async fn weak_all_active_shard_view_memberships(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError> {
        // Refresh shard client cache (no-op if not DNS mode).
        let sc = self.current_shard_client();
        let entries = match self.read_mode {
            ReadMode::Strong => {
                let ro = self.client.begin_read_only();
                let start = Sharded {
                    shard: DISCOVERY_SHARD,
                    key: SCAN_START.to_string(),
                };
                let end = Sharded {
                    shard: DISCOVERY_SHARD,
                    key: SCAN_END.to_string(),
                };
                ro.scan(start, end)
                    .await
                    .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?
            }
            ReadMode::Eventual => {
                let (results, _ts) = sc
                    .scan(SCAN_START.to_string(), SCAN_END.to_string(), None)
                    .await
                    .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;
                results
                    .into_iter()
                    .filter_map(|(k, v)| v.map(|v| (k, v)))
                    .collect()
            }
        };

        let mut result = Vec::new();
        for (key, json) in entries {
            let Some(shard) = parse_shard_number(&key) else {
                continue;
            };
            let record = parse_record::<K>(&json)?;
            if record.status != ShardStatus::Active {
                continue;
            }
            let membership = parse_membership_from(&record)?;
            result.push((shard, membership, record.view));
        }
        result.sort_by_key(|(s, _, _)| *s);
        Ok(result)
    }

    async fn strong_atomic_update_shards(
        &self,
        changes: Vec<ShardDirectoryChange<K, A>>,
    ) -> Result<(), DiscoveryError> {
        tracing::debug!(num_changes = changes.len(), "atomic_update_shards: starting");
        let changeset_json = serde_json::to_string(&changes)
            .map_err(|e| DiscoveryError::InvalidResponse(format!("serialize changeset: {e}")))?;

        // Consistent pre-read of last index.
        tracing::debug!("atomic_update_shards: reading last index");
        let current_idx: u64 = match self.read_raw(ROUTE_CHANGE_LAST_INDEX).await? {
            Some(s) => s.parse().unwrap_or(0),
            None => 0,
        };
        let new_idx = current_idx + 1;
        let idx_key = route_change_key(new_idx);
        tracing::debug!(current_idx, new_idx, "atomic_update_shards: last index read ok");

        // Pre-read shard entries and build JSON for each change.
        let mut shard_writes: Vec<(String, String)> = Vec::new();
        for change in &changes {
            match change {
                ShardDirectoryChange::ActivateShard { shard, range, membership, view } => {
                    let key = shard_key(*shard);
                    tracing::debug!(%key, ?shard, "atomic_update_shards: pre-reading shard entry");
                    if let Some(json) = self.read_raw(&key).await? {
                        let record = parse_record::<K>(&json)?;
                        if record.status == ShardStatus::Tombstoned {
                            // Tombstoned shards cannot be revived through route changes.
                            return Err(DiscoveryError::Tombstoned);
                        }
                        if record.view > *view {
                            // Stale view — update only key_range.
                            let mut updated = record;
                            updated.key_range = Some(range.clone());
                            let json = serde_json::to_string(&updated).unwrap();
                            shard_writes.push((key, json));
                            continue;
                        }
                    }
                    let json = to_json(membership, *view, ShardStatus::Active, Some(range.clone()));
                    shard_writes.push((key, json));
                }
                ShardDirectoryChange::TombstoneShard { shard } => {
                    let key = shard_key(*shard);
                    if let Some(json) = self.read_raw(&key).await? {
                        let mut record = parse_record::<K>(&json)?;
                        if record.status != ShardStatus::Tombstoned {
                            record.status = ShardStatus::Tombstoned;
                            let json = serde_json::to_string(&record).unwrap();
                            shard_writes.push((key, json));
                        }
                    }
                }
            }
        }

        for _attempt in 0..5 {
            tracing::debug!(_attempt, "atomic_update_shards: starting txn attempt");
            let txn = self.client.begin();

            // OCC-track the last-index key.
            tracing::debug!("atomic_update_shards: txn get(last_index)");
            let _ = txn
                .get(ROUTE_CHANGE_LAST_INDEX.to_string())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            // OCC-track the changeset key.
            tracing::debug!(%idx_key, "atomic_update_shards: txn get(changeset_key)");
            let _ = txn
                .get(idx_key.clone())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            txn.put(idx_key.clone(), Some(changeset_json.clone()));
            txn.put(
                ROUTE_CHANGE_LAST_INDEX.to_string(),
                Some(new_idx.to_string()),
            );

            // Shard entry writes in the same transaction.
            for (key, json) in &shard_writes {
                tracing::debug!(%key, "atomic_update_shards: txn get(shard_key)");
                let _ = txn
                    .get(key.clone())
                    .await
                    .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;
                txn.put(key.clone(), Some(json.clone()));
            }

            tracing::debug!("atomic_update_shards: committing txn");
            if txn.commit().await.is_some() {
                tracing::debug!("atomic_update_shards: txn committed successfully");
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                return Ok(());
            }
            tracing::debug!(_attempt, "atomic_update_shards: txn commit failed, retrying");
        }

        Err(DiscoveryError::ConnectionFailed(
            "atomic_update_shards failed after retries".to_string(),
        ))
    }

    async fn weak_route_changes_since(
        &self,
        after_index: u64,
    ) -> Result<Vec<(u64, ShardDirectoryChangeSet<K, A>)>, DiscoveryError> {
        // Read last index (eventual — 1 replica).
        let last_idx: u64 = match self.read_raw(ROUTE_CHANGE_LAST_INDEX).await? {
            Some(s) => s.parse().unwrap_or(0),
            None => return Ok(vec![]),
        };

        if last_idx <= after_index {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        for idx in (after_index + 1)..=last_idx {
            let key = route_change_key(idx);
            match self.read_raw(&key).await? {
                Some(json) => {
                    let changeset: ShardDirectoryChangeSet<K, A> = serde_json::from_str(&json)
                        .map_err(|e| {
                            DiscoveryError::InvalidResponse(format!(
                                "invalid changeset at {key}: {e}"
                            ))
                        })?;
                    result.push((idx, changeset));
                }
                None => {
                    // Gap — eventual consistency hasn't caught up.
                    break;
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{test_rng, discovery::build_test_discovery};
    use crate::IrMembership;
    use crate::discovery::RemoteShardDirectory;

    // Helpers to bind K=() for the blanket RemoteShardDirectory impl.
    async fn put(dir: &impl RemoteShardDirectory<usize, ()>, shard: ShardNumber, membership: IrMembership<usize>, view: u64) -> Result<(), DiscoveryError> {
        dir.strong_put_active_shard_view_membership(shard, membership, view).await
    }
    async fn get(dir: &impl RemoteShardDirectory<usize, ()>, shard: ShardNumber) -> Result<Option<(IrMembership<usize>, u64)>, DiscoveryError> {
        dir.weak_get_active_shard_membership(shard).await
    }
    async fn remove(dir: &impl RemoteShardDirectory<usize, ()>, shard: ShardNumber) -> Result<(), DiscoveryError> {
        dir.strong_remove_shard(shard).await
    }
    async fn all(dir: &impl RemoteShardDirectory<usize, ()>) -> Result<Vec<(ShardNumber, IrMembership<usize>, u64)>, DiscoveryError> {
        dir.weak_all_active_shard_view_memberships().await
    }

    #[tokio::test(start_paused = true)]
    async fn put_get_strong() {
        let mut rng = test_rng(42);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        put(&*dir, ShardNumber(1), membership, 5).await.unwrap();

        let (got, view) = get(&*dir, ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(view, 5);
        assert_eq!(got.len(), 3);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![10, 11, 12]);
    }

    #[tokio::test(start_paused = true)]
    async fn put_get_eventual() {
        let mut rng = test_rng(43);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote(&mut rng);

        let membership = IrMembership::new(vec![20usize, 21, 22]);
        put(&*dir, ShardNumber(2), membership, 3).await.unwrap();

        let (got, view) = get(&*dir, ShardNumber(2)).await.unwrap().unwrap();
        assert_eq!(view, 3);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![20, 21, 22]);
    }

    #[tokio::test(start_paused = true)]
    async fn get_nonexistent() {
        let mut rng = test_rng(44);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        assert!(get(&*dir, ShardNumber(99)).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn remove_tombstone() {
        let mut rng = test_rng(45);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        put(&*dir, ShardNumber(1), membership, 1).await.unwrap();

        remove(&*dir, ShardNumber(1)).await.unwrap();

        // get returns None for tombstoned shards.
        assert!(get(&*dir, ShardNumber(1)).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn put_on_tombstoned_rejected() {
        let mut rng = test_rng(46);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        put(&*dir, ShardNumber(1), membership.clone(), 1).await.unwrap();
        remove(&*dir, ShardNumber(1)).await.unwrap();

        let result = put(&*dir, ShardNumber(1), membership, 2).await;
        assert!(matches!(result, Err(DiscoveryError::Tombstoned)));
    }

    #[tokio::test(start_paused = true)]
    async fn put_on_pending_rejected() {
        let mut rng = test_rng(53);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        // Write a Pending-status entry directly via RW transaction.
        let shard = ShardNumber(1);
        let membership = IrMembership::new(vec![10usize, 11, 12]);
        let key = shard_key(shard);
        let pending_json = to_json::<_, ()>(&membership, 1, ShardStatus::Pending, None);
        let txn = dir.client.begin();
        let _ = txn.get(key.clone()).await.unwrap();
        txn.put(key, Some(pending_json));
        assert!(txn.commit().await.is_some());
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        // Attempting to put on a Pending shard should fail with NotActive.
        let result = put(&*dir, shard, membership, 2).await;
        assert!(matches!(result, Err(DiscoveryError::NotActive)));
    }

    #[tokio::test(start_paused = true)]
    async fn remove_nonexistent() {
        let mut rng = test_rng(47);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let result = remove(&*dir, ShardNumber(99)).await;
        assert!(matches!(result, Err(DiscoveryError::NotFound)));
    }

    #[tokio::test(start_paused = true)]
    async fn remove_already_tombstoned() {
        let mut rng = test_rng(48);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        put(&*dir, ShardNumber(1), membership, 1).await.unwrap();
        remove(&*dir, ShardNumber(1)).await.unwrap();

        let result = remove(&*dir, ShardNumber(1)).await;
        assert!(matches!(result, Err(DiscoveryError::NotFound)));
    }

    #[tokio::test(start_paused = true)]
    async fn all_omits_tombstoned() {
        let mut rng = test_rng(50);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        put(&*dir, ShardNumber(1), IrMembership::new(vec![10usize, 11, 12]), 1).await.unwrap();
        put(&*dir, ShardNumber(2), IrMembership::new(vec![20usize, 21, 22]), 1).await.unwrap();
        put(&*dir, ShardNumber(3), IrMembership::new(vec![30usize, 31, 32]), 1).await.unwrap();

        remove(&*dir, ShardNumber(2)).await.unwrap();

        let entries = all(&*dir).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[tokio::test(start_paused = true)]
    async fn monotonic_view() {
        let mut rng = test_rng(51);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote_strong(&mut rng);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        put(&*dir, ShardNumber(1), membership.clone(), 5).await.unwrap();

        // Put with lower view is silently accepted (idempotent).
        let membership_new = IrMembership::new(vec![20usize, 21, 22]);
        put(&*dir, ShardNumber(1), membership_new, 3).await.unwrap();

        // View stays at 5, membership unchanged.
        let (got, view) = get(&*dir, ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(view, 5);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![10, 11, 12]);
    }

    #[tokio::test(start_paused = true)]
    async fn all_eventual_mode() {
        let mut rng = test_rng(52);
        let disc = build_test_discovery(&mut rng, 3);
        let dir = disc.create_remote(&mut rng);

        put(&*dir, ShardNumber(1), IrMembership::new(vec![10usize, 11, 12]), 1).await.unwrap();
        put(&*dir, ShardNumber(2), IrMembership::new(vec![20usize, 21, 22]), 2).await.unwrap();

        let entries = all(&*dir).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[0].2, 1);
        assert_eq!(entries[1].0, ShardNumber(2));
        assert_eq!(entries[1].2, 2);
    }
}
