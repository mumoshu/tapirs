use crate::discovery::RemoteShardDirectory;
use crate::ir::{Record, SharedView, View, ViewNumber};
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Key, Replica, ShardClient, ShardNumber, Value};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership};
use std::time::Duration;

impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>, RD: RemoteShardDirectory<T::Address>> ShardManager<K, V, T, RD> {
    /// Add a new replica to an existing shard by pre-loading it with the
    /// shard's leader_record before triggering a membership change.
    pub async fn add_replica(
        &mut self,
        shard: ShardNumber,
        new_address: T::Address,
        new_membership: IrMembership<T::Address>,
    ) {
        let managed = self.shards.get(&shard).expect("shard not registered");

        // 1. Fetch the stable leader_record from any replica in the shard.
        let (view, record) = managed
            .client
            .fetch_leader_record()
            .await
            .expect("shard has no leader_record");

        // 2. Bootstrap R4 via a standalone client.
        //    Client sends BootstrapRecord → R4 converts to self-directed StartView.
        let standalone = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            new_membership,
            self.transport.clone(),
        );
        standalone.bootstrap_record((*record).clone(), view);

        // 3. Trigger AddMember → view change N → N+3.
        let managed = self.shards.get(&shard).expect("shard not registered");
        managed.client.add_member(new_address);
    }

    /// Bootstrap a shard with a single replica.
    ///
    /// Sends BootstrapRecord with an empty record at view 1. The replica
    /// converts it to a self-directed StartView, which sets leader_record
    /// and transitions to Normal status. Unconditional — no discovery lookup.
    pub fn bootstrap(&mut self, shard: ShardNumber, address: T::Address) {
        let client = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            IrMembership::new(vec![address]),
            self.transport.clone(),
        );
        let view = SharedView::new(View {
            membership: IrMembership::new(vec![address]),
            number: ViewNumber(1),
            app_config: None,
        });
        client.bootstrap_record(Record::<Replica<K, V>>::default(), view);
    }

    /// Sole entry point for adding a replica to a shard. Sends AddMember to IR.
    ///
    /// Discovers the existing membership from the address directory, fetches
    /// the leader_record, bootstraps the new replica, then triggers AddMember.
    /// Retries fetch_leader_record up to 5 times with 1s backoff for transient
    /// cases where a recent bootstrap hasn't fully propagated.
    ///
    /// See [`ShardManager`] module docs § "Membership Change Authority".
    pub async fn join(
        &mut self,
        shard: ShardNumber,
        new_address: T::Address,
    ) -> Result<(), String> {
        let (membership, _view) = self
            .remote
            .get(shard)
            .await
            .map_err(|e| format!("failed to get shard {shard:?}: {e:?}"))?
            .ok_or_else(|| format!("shard {shard:?} not found in remote directory"))?;

        let existing_client = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            membership,
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
        let (membership, _view) = self
            .remote
            .get(shard)
            .await
            .map_err(|e| format!("failed to get shard {shard:?}: {e:?}"))?
            .ok_or_else(|| format!("shard {shard:?} not found in remote directory"))?;

        let client = ShardClient::<K, V, T>::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            shard,
            membership,
            self.transport.clone(),
        );
        client.remove_member(address);
        Ok(())
    }
}
