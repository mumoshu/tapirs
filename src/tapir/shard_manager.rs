use super::{
    dynamic_router::ShardDirectory, Key, KeyRange, ShardClient, ShardNumber, Value,
};
use crate::discovery::{InMemoryShardDirectory, ShardDirectory as _};
use crate::transport::Transport;
use crate::tapir::Replica;
use crate::{IrClientId, IrMembership};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A shard group managed by the ShardManager.
pub struct ManagedShard<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    pub shard: ShardNumber,
    pub key_range: KeyRange<K>,
    pub client: ShardClient<K, V, T>,
}

/// Sidecar that orchestrates resharding operations (split, merge, migrate)
/// using `IrClient` to interact with shard groups via the normal protocol.
/// NOT in the message path — uses the same API as application clients.
pub struct ShardManager<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    pub(crate) shards: HashMap<ShardNumber, ManagedShard<K, V, T>>,
    pub(crate) directory: Arc<RwLock<ShardDirectory<K>>>,
    pub(crate) address_directory: Arc<InMemoryShardDirectory<T::Address>>,
    pub(crate) transport: T,
    client_id: IrClientId,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>> ShardManager<K, V, T> {
    pub fn new(
        transport: T,
        directory: Arc<RwLock<ShardDirectory<K>>>,
        address_directory: Arc<InMemoryShardDirectory<T::Address>>,
    ) -> Self {
        Self {
            shards: HashMap::new(),
            directory,
            address_directory,
            transport: transport.clone(),
            client_id: IrClientId::new(),
        }
    }

    pub fn register_shard(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
        self.address_directory.put(shard, membership.clone());
        let client = ShardClient::new(
            self.client_id,
            shard,
            membership,
            self.transport.clone(),
        );
        self.shards.insert(shard, ManagedShard {
            shard,
            key_range,
            client,
        });
    }

    pub fn deregister_shard(&mut self, shard: ShardNumber) {
        self.shards.remove(&shard);
        self.address_directory.remove(shard);
    }

    pub fn shard_client(&self, shard: ShardNumber) -> Option<&ShardClient<K, V, T>> {
        self.shards.get(&shard).map(|s| &s.client)
    }

    pub fn directory(&self) -> &Arc<RwLock<ShardDirectory<K>>> {
        &self.directory
    }
}
