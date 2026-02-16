use crate::discovery::ShardDirectory as AddressDirectory;
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Key, Replica, ShardClient, ShardNumber, Value};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership};

impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> ShardManager<K, V, T, D> {
    /// Add a new replica to an existing shard by pre-loading it with the
    /// shard's leader_record before triggering a membership change.
    pub async fn add_replica(
        &self,
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
            IrClientId::new(),
            shard,
            new_membership,
            self.transport.clone(),
        );
        standalone.bootstrap_record((*record).clone(), view);

        // 3. Trigger AddMember → view change N → N+3.
        managed.client.add_member(new_address);
    }
}
