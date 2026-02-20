use std::fmt::Display;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::discovery::{
    membership_to_strings, strings_to_membership, DiscoveryError, RemoteShardDirectory,
};
use crate::tapir::{Client as TapirClient, ShardClient, ShardNumber, Sharded};
use crate::{IrClientId, IrMembership, TapirTransport};

const DISCOVERY_SHARD: ShardNumber = ShardNumber(0);
const SCAN_START: &str = "shard:";
const SCAN_END: &str = "shard:~";

fn shard_key(shard: ShardNumber) -> String {
    format!("shard:{}", shard.0)
}

fn parse_shard_number(key: &str) -> Option<ShardNumber> {
    key.strip_prefix("shard:")
        .and_then(|s| s.parse::<u32>().ok())
        .map(ShardNumber)
}

#[derive(Serialize, Deserialize)]
struct ShardRecord {
    membership: Vec<String>,
    view: u64,
    status: ShardStatus,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ShardStatus {
    Active,
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
    read_mode: ReadMode,
    _phantom: PhantomData<A>,
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
        Self {
            client,
            shard_client,
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
        Self {
            client,
            shard_client,
            read_mode: ReadMode::Eventual,
            _phantom: PhantomData,
        }
    }

    fn parse_record(json: &str) -> Result<ShardRecord, DiscoveryError> {
        serde_json::from_str(json)
            .map_err(|e| DiscoveryError::InvalidResponse(format!("invalid JSON: {e}")))
    }

    fn parse_membership(record: &ShardRecord) -> Result<IrMembership<A>, DiscoveryError> {
        strings_to_membership::<A>(&record.membership)
            .map_err(|e| DiscoveryError::InvalidResponse(format!("invalid membership: {e}")))
    }

    fn to_json(membership: &IrMembership<A>, view: u64, status: ShardStatus) -> String {
        serde_json::to_string(&ShardRecord {
            membership: membership_to_strings(membership),
            view,
            status,
        })
        .unwrap()
    }

    /// Consistent read of a key's raw JSON value.
    ///
    /// Uses the configured read mode: RO transaction for strong (linearizable),
    /// unlogged read for eventual. This is the correct way to read committed
    /// data — RW transaction `get()` is inconsistent (reaches 1 replica,
    /// validated at commit time via OCC, not at read time).
    async fn read_raw(&self, key: &str) -> Result<Option<String>, DiscoveryError> {
        match self.read_mode {
            ReadMode::Strong => {
                let ro = self.client.begin_read_only();
                ro.get(key.to_string())
                    .await
                    .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))
            }
            ReadMode::Eventual => self
                .shard_client
                .get(key.to_string(), None)
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))
                .map(|(v, _)| v),
        }
    }
}

impl<A, T> RemoteShardDirectory<A> for TapirRemoteShardDirectory<A, T>
where
    A: FromStr + Display + Copy + Eq + Send + Sync + 'static,
    <A as FromStr>::Err: Display,
    T: TapirTransport<String, String>,
{
    async fn get(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<A>, u64)>, DiscoveryError> {
        match self.read_raw(&shard_key(shard)).await? {
            Some(json) => {
                let record = Self::parse_record(&json)?;
                if record.status == ShardStatus::Tombstoned {
                    return Ok(None);
                }
                let membership = Self::parse_membership(&record)?;
                Ok(Some((membership, record.view)))
            }
            None => Ok(None),
        }
    }

    async fn put(
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
            let record = Self::parse_record(&json)?;
            if record.status == ShardStatus::Tombstoned {
                return Err(DiscoveryError::Tombstoned);
            }
            if record.view >= view {
                return Ok(());
            }
        }

        let new_json = Self::to_json(&membership, view, ShardStatus::Active);

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

    async fn remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        let key = shard_key(shard);

        // Consistent pre-read to verify shard exists and isn't tombstoned.
        let pre_read = self.read_raw(&key).await?;
        match &pre_read {
            Some(json) => {
                let record = Self::parse_record(json)?;
                if record.status == ShardStatus::Tombstoned {
                    return Err(DiscoveryError::NotFound);
                }
            }
            None => return Err(DiscoveryError::NotFound),
        }

        // Build tombstone from the consistent pre-read value.
        let mut record = Self::parse_record(pre_read.as_ref().unwrap())?;
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

    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError> {
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
                let (results, _ts) = self
                    .shard_client
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
            let record = Self::parse_record(&json)?;
            if record.status == ShardStatus::Tombstoned {
                continue;
            }
            let membership = Self::parse_membership(&record)?;
            result.push((shard, membership, record.view));
        }
        result.sort_by_key(|(s, _, _)| *s);
        Ok(result)
    }

    async fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let old_key = shard_key(old);
        let new_key = shard_key(new);
        let new_json = Self::to_json(&membership, view, ShardStatus::Active);

        // Consistent pre-read: build tombstone for old shard.
        let old_tombstone = if let Some(json) = self.read_raw(&old_key).await? {
            let mut record = Self::parse_record(&json)?;
            record.status = ShardStatus::Tombstoned;
            Some(serde_json::to_string(&record).unwrap())
        } else {
            None
        };

        for _attempt in 0..5 {
            let txn = self.client.begin();

            // RW get for OCC read-set tracking (inconsistent — may be stale).
            let _ = txn
                .get(old_key.clone())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            if let Some(ref tombstone) = old_tombstone {
                txn.put(old_key.clone(), Some(tombstone.clone()));
            }

            // OCC-track the new key too.
            let _ = txn
                .get(new_key.clone())
                .await
                .map_err(|e| DiscoveryError::ConnectionFailed(format!("{e:?}")))?;

            txn.put(new_key.clone(), Some(new_json.clone()));

            if txn.commit().await.is_some() {
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                return Ok(());
            }
        }

        Err(DiscoveryError::ConnectionFailed(
            "replace failed after retries".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{InMemoryShardDirectory, ShardDirectory as _};
    use crate::{ChannelRegistry, ChannelTransport, IrMembership, IrReplica, TapirReplica};
    use std::sync::Arc;

    type DiscoveryReplica = TapirReplica<String, String>;
    type DiscoveryTransport = ChannelTransport<DiscoveryReplica>;

    fn test_rng(seed: u64) -> crate::Rng {
        crate::Rng::from_seed(seed)
    }

    fn build_discovery_cluster(
        rng: &mut crate::Rng,
        num_replicas: usize,
    ) -> (
        Vec<Arc<IrReplica<DiscoveryReplica, DiscoveryTransport>>>,
        Arc<InMemoryShardDirectory<usize>>,
        ChannelRegistry<DiscoveryReplica>,
    ) {
        let registry = ChannelRegistry::<DiscoveryReplica>::default();
        let directory = Arc::new(InMemoryShardDirectory::new());
        let membership = IrMembership::new(
            (0..num_replicas).collect::<Vec<_>>(),
        );

        let replicas: Vec<_> = (0..num_replicas)
            .map(|_| {
                Arc::new_cyclic(
                    |weak: &std::sync::Weak<
                        IrReplica<DiscoveryReplica, DiscoveryTransport>,
                    >| {
                        let weak = weak.clone();
                        let channel = registry.channel(
                            move |from, message| weak.upgrade()?.receive(from, message),
                            Arc::clone(&directory),
                        );
                        channel.set_shard(DISCOVERY_SHARD);
                        let upcalls =
                            TapirReplica::<String, String>::new(DISCOVERY_SHARD, false);
                        IrReplica::new(
                            rng.fork(),
                            membership.clone(),
                            upcalls,
                            channel,
                            Some(TapirReplica::<String, String>::tick),
                        )
                    },
                )
            })
            .collect();

        directory.put(DISCOVERY_SHARD, membership, 0);

        (replicas, directory, registry)
    }

    fn create_dir(
        rng: &mut crate::Rng,
        registry: &ChannelRegistry<DiscoveryReplica>,
        directory: &Arc<InMemoryShardDirectory<usize>>,
        mode: ReadMode,
    ) -> TapirRemoteShardDirectory<usize, DiscoveryTransport> {
        let membership = directory
            .get(DISCOVERY_SHARD)
            .map(|(m, _)| m)
            .expect("discovery shard not in directory");
        let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(directory));
        match mode {
            ReadMode::Strong => TapirRemoteShardDirectory::with_strong_consistent_read(
                rng.fork(),
                membership,
                channel,
            ),
            ReadMode::Eventual => TapirRemoteShardDirectory::with_eventual_consistent_read(
                rng.fork(),
                membership,
                channel,
            ),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn put_get_strong() {
        let mut rng = test_rng(42);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), membership, 5).await.unwrap();

        let (got, view) = dir.get(ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(view, 5);
        assert_eq!(got.len(), 3);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![10, 11, 12]);
    }

    #[tokio::test(start_paused = true)]
    async fn put_get_eventual() {
        let mut rng = test_rng(43);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Eventual);

        let membership = IrMembership::new(vec![20usize, 21, 22]);
        dir.put(ShardNumber(2), membership, 3).await.unwrap();

        let (got, view) = dir.get(ShardNumber(2)).await.unwrap().unwrap();
        assert_eq!(view, 3);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![20, 21, 22]);
    }

    #[tokio::test(start_paused = true)]
    async fn get_nonexistent() {
        let mut rng = test_rng(44);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        assert!(dir.get(ShardNumber(99)).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn remove_tombstone() {
        let mut rng = test_rng(45);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), membership, 1).await.unwrap();

        dir.remove(ShardNumber(1)).await.unwrap();

        // get returns None for tombstoned shards.
        assert!(dir.get(ShardNumber(1)).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn put_on_tombstoned_rejected() {
        let mut rng = test_rng(46);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), membership.clone(), 1)
            .await
            .unwrap();
        dir.remove(ShardNumber(1)).await.unwrap();

        let result = dir.put(ShardNumber(1), membership, 2).await;
        assert!(matches!(result, Err(DiscoveryError::Tombstoned)));
    }

    #[tokio::test(start_paused = true)]
    async fn remove_nonexistent() {
        let mut rng = test_rng(47);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let result = dir.remove(ShardNumber(99)).await;
        assert!(matches!(result, Err(DiscoveryError::NotFound)));
    }

    #[tokio::test(start_paused = true)]
    async fn remove_already_tombstoned() {
        let mut rng = test_rng(48);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), membership, 1).await.unwrap();
        dir.remove(ShardNumber(1)).await.unwrap();

        let result = dir.remove(ShardNumber(1)).await;
        assert!(matches!(result, Err(DiscoveryError::NotFound)));
    }

    #[tokio::test(start_paused = true)]
    async fn replace_atomic() {
        let mut rng = test_rng(49);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let old_membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), old_membership, 1).await.unwrap();

        let new_membership = IrMembership::new(vec![20usize, 21, 22]);
        dir.replace(ShardNumber(1), ShardNumber(2), new_membership, 2)
            .await
            .unwrap();

        // Old shard tombstoned.
        assert!(dir.get(ShardNumber(1)).await.unwrap().is_none());

        // New shard active.
        let (got, view) = dir.get(ShardNumber(2)).await.unwrap().unwrap();
        assert_eq!(view, 2);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![20, 21, 22]);
    }

    #[tokio::test(start_paused = true)]
    async fn all_omits_tombstoned() {
        let mut rng = test_rng(50);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        dir.put(ShardNumber(1), IrMembership::new(vec![10usize, 11, 12]), 1)
            .await
            .unwrap();
        dir.put(ShardNumber(2), IrMembership::new(vec![20usize, 21, 22]), 1)
            .await
            .unwrap();
        dir.put(ShardNumber(3), IrMembership::new(vec![30usize, 31, 32]), 1)
            .await
            .unwrap();

        dir.remove(ShardNumber(2)).await.unwrap();

        let entries = dir.all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[tokio::test(start_paused = true)]
    async fn monotonic_view() {
        let mut rng = test_rng(51);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Strong);

        let membership = IrMembership::new(vec![10usize, 11, 12]);
        dir.put(ShardNumber(1), membership.clone(), 5)
            .await
            .unwrap();

        // Put with lower view is silently accepted (idempotent).
        let membership_new = IrMembership::new(vec![20usize, 21, 22]);
        dir.put(ShardNumber(1), membership_new, 3).await.unwrap();

        // View stays at 5, membership unchanged.
        let (got, view) = dir.get(ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(view, 5);
        let addrs: Vec<usize> = got.iter().collect();
        assert_eq!(addrs, vec![10, 11, 12]);
    }

    #[tokio::test(start_paused = true)]
    async fn all_eventual_mode() {
        let mut rng = test_rng(52);
        let (_replicas, directory, registry) = build_discovery_cluster(&mut rng, 3);
        let dir = create_dir(&mut rng, &registry, &directory, ReadMode::Eventual);

        dir.put(ShardNumber(1), IrMembership::new(vec![10usize, 11, 12]), 1)
            .await
            .unwrap();
        dir.put(ShardNumber(2), IrMembership::new(vec![20usize, 21, 22]), 2)
            .await
            .unwrap();

        let entries = dir.all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[0].2, 1);
        assert_eq!(entries[1].0, ShardNumber(2));
        assert_eq!(entries[1].2, 2);
    }
}
