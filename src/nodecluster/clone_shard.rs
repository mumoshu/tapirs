use super::types::{CloneError, SoloClusterManager};
use crate::tapir::replica::{ShardConfig, ShardPhase};
use crate::sharding::shardmanager::cdc::{ship_changes, CdcCursor};
use crate::tapir::{Key, Replica, ShardNumber, Value};
use crate::transport::Transport;
use std::time::Duration;

impl SoloClusterManager {
    /// Clone a shard using pre-constructed ShardClients.
    ///
    /// Low-level entry point — the caller is responsible for building
    /// ShardClients with correct membership. Used by tests (ChannelTransport)
    /// and by `clone_shard_direct()` internally.
    ///
    /// # Algorithm (adapted from compact() in sharding/shardmanager/cdc.rs)
    ///
    /// | Phase | Action |
    /// |-------|--------|
    /// | 1 — Bulk copy | `source.scan_changes(0)` -> ship all changes to dest |
    /// | 2 — Catch-up | Loop `source.scan_changes(cursor)` -> ship to dest |
    /// | 3a — Freeze | `source.reconfigure(ReadOnly)` — reject new Prepare |
    /// | 3b — Drain | Wait for `pending_prepares == 0` + final seal |
    /// | 3c — Read protection | Freeze reads, transfer min_prepare_time |
    ///
    /// # Differences from compact()
    ///
    /// - **No atomic swap**: Source and destination are separate clusters.
    ///   The caller handles client switchover.
    /// - **No directory operations**: No remote discovery writes.
    /// - **Source left frozen**: After clone, source is in `ReadOnly` phase
    ///   (then `Decommissioning` after read protection transfer). Caller
    ///   decides whether to unfreeze or tear down.
    ///
    /// # Safety analysis
    ///
    /// The same safety properties as `compact()` apply:
    ///
    /// - **transaction_log**: New shard starts empty. Stale IO::Commit is
    ///   idempotent (same key/value/timestamp). Stale IO::Abort is no-op.
    /// - **range_reads**: New shard starts empty. `min_prepare_time` transfer
    ///   subsumes all historical read protections from the source.
    /// - **prepared**: All drained to zero before switchover.
    pub async fn clone_shard<K, V, T>(
        &mut self,
        source: &crate::tapir::ShardClient<K, V, T>,
        dest: &crate::tapir::ShardClient<K, V, T>,
        dest_shard: ShardNumber,
    ) -> Result<(), CloneError>
    where
        K: Key + Clone,
        V: Value + Clone,
        T: Transport<Replica<K, V>>,
    {
        let mut cursor = CdcCursor::new();

        // Phase 1: Bulk copy via scan_changes.
        self.report_progress("clone:bulk-copy");
        let r = source.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        if changes.is_empty() && r.effective_end_view.is_none() {
            // Source has no data and no view changes — possibly not bootstrapped.
            // This is not necessarily an error; the source may just be empty.
        }
        ship_changes(dest, dest_shard, &changes, &mut self.rng).await;
        cursor.advance(r.effective_end_view);

        // Phase 2: Catch-up tailing.
        self.report_progress("clone:catch-up");
        loop {
            let r = source.scan_changes(cursor.next_from()).await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(dest, dest_shard, &changes, &mut self.rng).await;
            }
            if changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
            cursor.advance(r.effective_end_view);
        }

        // Phase 3a: Freeze source — reject all new Prepare with Fail.
        self.report_progress("clone:freeze-source");
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            phase: ShardPhase::ReadOnly,
        })
        .expect("serialize freeze config");
        source.reconfigure(freeze);

        // Phase 3b: Drain — wait for ALL prepared transactions to resolve + final seal.
        self.report_progress("clone:drain-prepared");
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = source.scan_changes(cursor.next_from()).await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(dest, dest_shard, &changes, &mut self.rng).await;
            }
            cursor.advance(r.effective_end_view);

            if r.pending_prepares == 0
                && changes.is_empty()
                && cursor.stabilized(r.effective_end_view)
            {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = source.scan_changes(cursor.next_from()).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        if !changes.is_empty() {
            ship_changes(dest, dest_shard, &changes, &mut self.rng).await;
        }

        // Phase 3c: Transfer read protection from source to destination.
        //
        // Freeze ALL operations on source (Decommissioning blocks IO::QuorumScan
        // and IO::QuorumRead) to freeze range_reads and last_read_commit_ts.
        self.report_progress("clone:transfer-read-protection");
        let decommission = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            phase: ShardPhase::Decommissioning,
        })
        .expect("serialize decommission config");
        source.reconfigure(decommission);

        // Wait for in-flight reads to complete after freeze propagates.
        T::sleep(Duration::from_secs(3)).await;

        // Query min_prepare_baseline from source (f+1 replicas).
        let (max_range_read_time, max_read_commit_time) = source.min_prepare_baseline().await;

        // Raise min_prepare_time on destination shard.
        let barrier = max_range_read_time.max(max_read_commit_time) + 1;
        if barrier > 1 {
            dest.raise_min_prepare_time(barrier).await;
        }

        self.report_progress("clone:complete");
        Ok(())
    }

    /// Clone a shard using node admin addresses for discovery.
    ///
    /// High-level entry point — queries source nodes' admin API to discover
    /// shard membership, creates destination replicas via admin API, builds
    /// ShardClients with static membership, then delegates to `clone_shard()`.
    ///
    /// No discovery store or ShardManager involved.
    ///
    /// # Arguments
    ///
    /// * `source_node_addrs` — Admin API addresses (TCP `host:port`) of source nodes
    /// * `source_shard` — Shard number on source nodes
    /// * `dest_node_addrs` — Admin API addresses (TCP `host:port`) of destination nodes
    /// * `dest_shard` — Shard number to create on destination nodes
    /// * `dest_base_port` — Base TAPIR protocol port for destination replicas
    pub async fn clone_shard_direct(
        &mut self,
        source_node_addrs: &[String],
        source_shard: ShardNumber,
        dest_node_addrs: &[String],
        dest_shard: ShardNumber,
        dest_base_port: u16,
    ) -> Result<(), CloneError> {
        use crate::discovery::InMemoryShardDirectory;
        use crate::node::node_client::send_admin_request;
        use crate::{IrClientId, IrMembership, TapirReplica, TcpAddress, TcpTransport};
        use std::sync::Arc;

        // 1. Query source nodes for shard membership.
        self.report_progress("clone-direct:discover-source");
        let mut source_listen_addrs = Vec::new();
        for admin_addr in source_node_addrs {
            let resp = send_admin_request(admin_addr, r#"{"command":"status"}"#)
                .await
                .map_err(|e| CloneError::AdminError(format!("query source {admin_addr}: {e}")))?;
            if !resp.ok {
                return Err(CloneError::AdminError(format!(
                    "source {admin_addr} status failed: {:?}",
                    resp.message
                )));
            }
            if let Some(shards) = resp.shards {
                for s in shards {
                    if s.shard == source_shard.0 {
                        let addr: std::net::SocketAddr = s.listen_addr.parse().map_err(|e| {
                            CloneError::AdminError(format!(
                                "invalid listen_addr '{}': {e}",
                                s.listen_addr
                            ))
                        })?;
                        source_listen_addrs.push(TcpAddress(addr));
                    }
                }
            }
        }
        if source_listen_addrs.is_empty() {
            return Err(CloneError::AdminError(format!(
                "shard {} not found on any source node",
                source_shard.0
            )));
        }

        // 2. Compute destination listen addresses.
        let dest_admin_sockaddrs: Vec<std::net::SocketAddr> = dest_node_addrs
            .iter()
            .map(|a| {
                a.parse()
                    .map_err(|e| CloneError::AdminError(format!("invalid dest admin addr '{a}': {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dest_listen_addrs: Vec<TcpAddress> = dest_admin_sockaddrs
            .iter()
            .map(|sa| TcpAddress(std::net::SocketAddr::new(sa.ip(), dest_base_port)))
            .collect();
        let dest_membership_strs: Vec<String> =
            dest_listen_addrs.iter().map(|a| a.0.to_string()).collect();

        // 3. Create destination replicas via admin API.
        self.report_progress("clone-direct:create-dest-replicas");
        for (admin_addr, listen_addr) in dest_node_addrs.iter().zip(dest_listen_addrs.iter()) {
            let body = serde_json::json!({
                "command": "add_replica",
                "shard": dest_shard.0,
                "listen_addr": listen_addr.0.to_string(),
                "membership": dest_membership_strs,
            });
            let resp = send_admin_request(admin_addr, &serde_json::to_string(&body).unwrap())
                .await
                .map_err(|e| {
                    CloneError::AdminError(format!("create replica on {admin_addr}: {e}"))
                })?;
            if !resp.ok {
                return Err(CloneError::AdminError(format!(
                    "create replica on {admin_addr} failed: {:?}",
                    resp.message
                )));
            }
        }

        // 4. Build transport and ShardClients.
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_clone_{}", std::process::id());
        let transport: TcpTransport<TapirReplica<String, String>> =
            TcpTransport::with_directory(ephemeral_addr, persist_dir, directory);

        let source_membership = IrMembership::new(source_listen_addrs);
        transport.set_shard_addresses(source_shard, source_membership.clone());
        let source_client = crate::tapir::ShardClient::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            source_shard,
            source_membership,
            transport.clone(),
        );

        let dest_membership = IrMembership::new(dest_listen_addrs);
        transport.set_shard_addresses(dest_shard, dest_membership.clone());
        let dest_client = crate::tapir::ShardClient::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
            dest_shard,
            dest_membership,
            transport,
        );

        // 5. Delegate to generic clone_shard.
        self.clone_shard(&source_client, &dest_client, dest_shard)
            .await
    }
}
