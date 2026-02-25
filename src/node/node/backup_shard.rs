use super::*;
use crate::node::types::ShardBackup;
use crate::{IrClient, IrMembership};

impl Node {
    /// Take a backup of a shard by fetching the leader_record from
    /// the local replica.
    ///
    /// Returns the IR record and view from the last completed view change.
    /// Force a view change before backup if the most up-to-date state is
    /// needed — view change is TAPIR's synchronization mechanism.
    pub async fn backup_shard(&self, shard: ShardNumber) -> Option<ShardBackup> {
        let (transport, addr) = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas.get(&shard)?;
            (handle.replica.transport().clone(), handle.listen_addr)
        };
        let client = IrClient::new(
            (self.new_rng)(),
            IrMembership::new(vec![TcpAddress(addr)]),
            transport,
        );
        let (view, record) = client.fetch_leader_record().await?;
        Some(ShardBackup {
            record: (*record).clone(),
            view,
        })
    }
}
