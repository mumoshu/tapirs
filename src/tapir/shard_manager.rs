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
    pub(crate) on_progress: Option<Box<dyn Fn(&str) + Send + Sync>>,
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
            on_progress: None,
        }
    }

    pub(crate) fn set_progress_callback(&mut self, cb: impl Fn(&str) + Send + Sync + 'static) {
        self.on_progress = Some(Box::new(cb));
    }

    pub(crate) fn report_progress(&self, msg: &str) {
        if let Some(ref cb) = self.on_progress {
            cb(msg);
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

    /// Create a shard client and insert into `self.shards` without touching
    /// the address directory or rebuilding the key-range directory.
    ///
    /// Used by compact to set up the new shard's client during migration
    /// without making it visible to cross-shard discovery.
    pub(crate) fn create_shard_client(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
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
    }

    /// Atomically replace one shard with another in all directories.
    ///
    /// Removes `old` from the local shard registry, performs an atomic swap
    /// in the address directory (cross-shard discovery), and swaps the shard
    /// number in-place in the key-range directory (no full rebuild needed
    /// since key ranges are unchanged).
    pub(crate) fn replace_shard(
        &mut self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<T::Address>,
    ) {
        self.shards.remove(&old);
        self.address_directory.replace(old, new, membership);
        self.directory.replace(old, new);
    }

    pub fn deregister_shard(&mut self, shard: ShardNumber) {
        self.shards.remove(&shard);
        self.address_directory.remove(shard);
        self.rebuild_directory();
    }

    pub fn shard_client(&self, shard: ShardNumber) -> Option<&ShardClient<K, V, T>> {
        self.shards.get(&shard).map(|s| &s.client)
    }

    #[allow(clippy::disallowed_methods)] // values() order is irrelevant — ShardDirectory sorts internally
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
