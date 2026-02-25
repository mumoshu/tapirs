use crate::discovery::{RemoteShardDirectory, ShardStatus};
use super::ShardManager;
use crate::tapir::{Key, Replica, ShardClient, ShardNumber, Value};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership};
use std::time::Duration;

/// Membership-change operations for adding/removing replicas from shards.
///
/// [`join`](Self::join) is the sole entry point for adding a replica. It
/// auto-discovers the existing membership from the remote discovery
/// directory and retries `fetch_leader_record` up to 5 times with 1s
/// backoff. Exposed via HTTP `/v1/join`, called from
/// `Node::add_replica_join()` during runtime scaling.
///
/// Both `join` and `leave` follow the same pattern:
/// 1. Query the remote discovery directory for current membership
/// 2. Send AddMember/RemoveMember to trigger a view change
///
/// `join` additionally fetches the leader_record and bootstraps the new
/// replica before sending AddMember.
impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>, RD: RemoteShardDirectory<T::Address, K>> ShardManager<K, V, T, RD> {
    /// Add a replica to a shard.
    ///
    /// Auto-discovers the existing membership from the remote directory and
    /// retries `fetch_leader_record` up to 5 times with 1s backoff for
    /// transient cases where a recent bootstrap hasn't fully propagated.
    ///
    /// Exposed via HTTP `/v1/join`. Called from `Node::add_replica_join()` during
    /// runtime scaling (Go operator `scale.go` sends
    /// `AdminClient.AddReplica(membership=nil)` → admin server dynamic path
    /// → `Node::add_replica_join()` → POST `/v1/join`).
    ///
    /// **Requires**: The shard must already be registered in the remote discovery
    /// cluster with `Active` status — i.e., `register_active_shard()` (or the
    /// HTTP `/v1/register` endpoint) must have been called previously. Returns an
    /// error immediately if the shard is missing or not `Active`.
    ///
    /// See [`ShardManager`] module docs § "Membership Change Authority".
    pub async fn join(
        &mut self,
        shard: ShardNumber,
        new_address: T::Address,
    ) -> Result<(), String> {
        let record = self
            .remote
            .strong_get_shard(shard)
            .await
            .map_err(|e| format!("failed to get shard {shard:?}: {e:?}"))?
            .ok_or_else(|| format!("shard {shard:?} not found in remote directory"))?;
        if record.status != ShardStatus::Active {
            return Err(format!(
                "shard {shard:?} is {:?}, expected Active",
                record.status
            ));
        }

        let existing_client = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            record.membership,
            self.transport.clone(),
        );

        let mut last_err = String::new();
        for attempt in 0..5 {
            if attempt > 0 {
                T::sleep(Duration::from_secs(1)).await;
            }
            match existing_client.fetch_leader_record().await {
                Some((view, record)) => {
                    let new_client = ShardClient::<K, V, T>::new(
                        self.rng.fork(),
                        IrClientId::new(&mut self.rng),
                        shard,
                        IrMembership::new(vec![new_address]),
                        self.transport.clone(),
                    );
                    new_client.bootstrap_record((*record).clone(), view);
                    existing_client.add_member(new_address);
                    return Ok(());
                }
                None => {
                    last_err = format!(
                        "shard {shard:?} has no leader_record (attempt {})",
                        attempt + 1
                    );
                    tracing::warn!("{last_err}");
                }
            }
        }
        Err(last_err)
    }

    /// Sole entry point for removing a replica from a shard. Sends RemoveMember to IR.
    ///
    /// Discovers the current membership from the address directory, then
    /// broadcasts RemoveMember to trigger a view change that removes the
    /// address from the group. Symmetric with `join` which sends AddMember.
    ///
    /// See [`ShardManager`] module docs § "Membership Change Authority".
    pub async fn leave(
        &mut self,
        shard: ShardNumber,
        address: T::Address,
    ) -> Result<(), String> {
        eprintln!("[sm.leave] shard={shard:?} address={address:?}");
        let record = self
            .remote
            .strong_get_shard(shard)
            .await
            .map_err(|e| format!("failed to get shard {shard:?}: {e:?}"))?
            .ok_or_else(|| format!("shard {shard:?} not found in remote directory"))?;
        if record.status != ShardStatus::Active {
            return Err(format!(
                "shard {shard:?} is {:?}, expected Active",
                record.status
            ));
        }
        eprintln!("[sm.leave] remote.strong_get_shard returned membership len={} view={}", record.membership.len(), record.view);

        let client = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            record.membership,
            self.transport.clone(),
        );
        eprintln!("[sm.leave] broadcasting RemoveMember({address:?})");
        client.remove_member(address);
        eprintln!("[sm.leave] RemoveMember broadcast done");
        Ok(())
    }
}
