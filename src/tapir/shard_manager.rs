use super::{
    dynamic_router::{ShardDirectory, ShardEntry},
    Key, KeyRange, ShardClient, ShardNumber, Value,
};
use crate::discovery::ShardDirectory as AddressDirectory;
use crate::transport::Transport;
use crate::tapir::Replica;
use crate::{IrClientId, IrMembership};
use std::collections::HashMap;

/// A shard group managed by the ShardManager.
pub struct ManagedShard<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    pub shard: ShardNumber,
    pub key_range: KeyRange<K>,
    pub client: ShardClient<K, V, T>,
}

/// Sidecar that orchestrates resharding operations (split, merge, migrate)
/// using `IrClient` to interact with shard groups via the normal protocol.
/// NOT in the message path — uses the same API as application clients.
pub struct ShardManager<K: Key, V: Value, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> {
    pub(crate) shards: HashMap<ShardNumber, ManagedShard<K, V, T>>,
    pub(crate) directory: ShardDirectory<K>,
    pub(crate) address_directory: D,
    pub(crate) transport: T,
    client_id: IrClientId,
    pub(crate) rng: crate::Rng,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> ShardManager<K, V, T, D> {
    pub fn new(
        mut rng: crate::Rng,
        transport: T,
        address_directory: D,
    ) -> Self {
        let client_id = IrClientId::new(&mut rng);
        Self {
            shards: HashMap::new(),
            directory: ShardDirectory::new(vec![]),
            address_directory,
            transport: transport.clone(),
            client_id,
            rng,
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
            self.rng.fork(),
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
        self.rebuild_directory();
    }

    pub fn deregister_shard(&mut self, shard: ShardNumber) {
        self.shards.remove(&shard);
        self.address_directory.remove(shard);
        self.rebuild_directory();
    }

    pub fn shard_client(&self, shard: ShardNumber) -> Option<&ShardClient<K, V, T>> {
        self.shards.get(&shard).map(|s| &s.client)
    }

    pub(crate) fn rebuild_directory(&mut self) {
        let entries = self
            .shards
            .values()
            .map(|s| ShardEntry {
                shard: s.shard,
                range: s.key_range.clone(),
            })
            .collect();
        self.directory = ShardDirectory::new(entries);
    }
}
