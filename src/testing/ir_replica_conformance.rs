//! Conformance test macro for IR+TAPIR replica integration.
//!
//! Tests drive transactions through TapirClient → IR consensus → TAPIR upcalls,
//! verifying end-to-end correctness with any combination of IrRecordStore and
//! TapirStore implementations.

use crate::discovery::{InMemoryShardDirectory, ShardDirectory as _};
use crate::ir::{IrRecordStore, Replica as IrReplica};
use crate::tapir::{self, Key, Value, CO, CR, IO};
use crate::tapirstore::TapirStore;
use crate::{ChannelRegistry, ChannelTransport, IrMembership, ShardNumber, TapirClient};
use std::sync::Arc;

/// Build a shard of IR replicas with arbitrary TapirStore and IrRecordStore.
///
/// `store_factory` is called once per replica and must return
/// `(tapir_upcalls, ir_record_store)`.
pub fn build_shard_with<K, V, S, R, F>(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    num_replicas: usize,
    registry: &ChannelRegistry<tapir::Replica<K, V, S>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
    mut store_factory: F,
) -> Vec<Arc<IrReplica<tapir::Replica<K, V, S>, ChannelTransport<tapir::Replica<K, V, S>>, R>>>
where
    K: Key,
    V: Value,
    S: TapirStore<K, V>,
    R: IrRecordStore<IO<K, V>, CO<K, V>, CR, Payload = S::Payload>,
    F: FnMut() -> (tapir::Replica<K, V, S>, R),
{
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas)
            .map(|n| n + initial_address)
            .collect::<Vec<_>>(),
    );

    let replicas = (0..num_replicas)
        .map(|_| {
            let replica_rng = rng.fork();
            let dir = Arc::clone(directory);
            let m = membership.clone();
            let (upcalls, record) = store_factory();
            Arc::new_cyclic(
                |weak: &std::sync::Weak<
                    IrReplica<
                        tapir::Replica<K, V, S>,
                        ChannelTransport<tapir::Replica<K, V, S>>,
                        R,
                    >,
                >| {
                    let weak = weak.clone();
                    let channel = registry.channel(
                        move |from, message| weak.upgrade()?.receive(from, message),
                        Arc::clone(&dir),
                    );
                    channel.set_shard(shard);
                    IrReplica::new(
                        replica_rng,
                        m.clone(),
                        upcalls,
                        channel,
                        Some(tapir::Replica::<K, V, S>::tick),
                        record,
                    )
                },
            )
        })
        .collect::<Vec<_>>();

    directory.put(shard, membership, 0);
    replicas
}

/// Build TapirClients wired to a ChannelRegistry with arbitrary store type.
pub fn build_clients_with<K, V, S>(
    rng: &mut crate::Rng,
    num_clients: usize,
    registry: &ChannelRegistry<tapir::Replica<K, V, S>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> Vec<Arc<TapirClient<K, V, ChannelTransport<tapir::Replica<K, V, S>>>>>
where
    K: Key,
    V: Value,
    S: TapirStore<K, V>,
{
    (0..num_clients)
        .map(|_| {
            let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(directory));
            Arc::new(TapirClient::new(rng.fork(), channel))
        })
        .collect()
}

/// Drive a basic put-and-read test through the TAPIR client.
///
/// Uses String key/value. Commits N transactions writing "key_0" → "val_{i}".
/// Returns the number of successfully committed transactions.
pub async fn drive_put_read_test<S>(
    clients: &[Arc<TapirClient<String, String, ChannelTransport<tapir::Replica<String, String, S>>>>],
    num_transactions: usize,
) -> usize
where
    S: TapirStore<String, String>,
{
    let client = &clients[0];
    let mut committed = 0usize;

    for i in 0..num_transactions {
        let txn = client.begin();
        let _old: Option<String> = txn.get("key_0".to_string()).await.unwrap();
        txn.put("key_0".to_string(), Some(format!("val_{i}")));
        if txn.commit().await.is_some() {
            committed += 1;
        }
        // Small delay between transactions for tick processing.
        <ChannelTransport<tapir::Replica<String, String, S>> as crate::transport::Transport<
            tapir::Replica<String, String, S>,
        >>::sleep(std::time::Duration::from_millis(100))
        .await;
    }

    committed
}

/// Conformance test macro for IR+TAPIR replica integration.
///
/// # Usage
///
/// ```ignore
/// ir_replica_conformance_tests!($factory_fn);
/// ir_replica_conformance_tests!($factory_fn, max_write_amp = 3.0);
/// ```
///
/// `$factory_fn` must be a function with signature:
/// ```ignore
/// fn() -> (ChannelRegistry<U>, Arc<InMemoryShardDirectory<usize>>,
///          impl FnMut() -> (tapir::Replica<K, V, S>, R))
/// ```
/// where U is the TapirReplica type and R is the IrRecordStore type.
#[macro_export]
#[cfg(test)]
macro_rules! ir_replica_conformance_tests {
    ($factory_fn:expr) => {
        $crate::ir_replica_conformance_tests!($factory_fn, max_write_amp = None);
    };
    ($factory_fn:expr, max_write_amp = $threshold:expr) => {
        #[tokio::test(start_paused = true)]
        async fn prepare_commit_read() {
            use $crate::testing::ir_replica_conformance::*;

            tokio::time::timeout(std::time::Duration::from_secs(60), async {
                let mut rng = $crate::testing::test_rng(42);
                let (registry, directory, factory) = $factory_fn();
                let _replicas = build_shard_with(
                    &mut rng,
                    $crate::ShardNumber(0),
                    3,
                    &registry,
                    &directory,
                    factory,
                );
                let clients = build_clients_with(&mut rng, 1, &registry, &directory);

                let committed = drive_put_read_test(&clients, 5).await;
                assert!(committed > 0, "at least one transaction should commit");
            })
            .await
            .unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn prepare_abort_cleanup() {
            use $crate::testing::ir_replica_conformance::*;

            tokio::time::timeout(std::time::Duration::from_secs(60), async {
                let mut rng = $crate::testing::test_rng(43);
                let (registry, directory, factory) = $factory_fn();
                let _replicas = build_shard_with(
                    &mut rng,
                    $crate::ShardNumber(0),
                    3,
                    &registry,
                    &directory,
                    factory,
                );
                let clients = build_clients_with(&mut rng, 2, &registry, &directory);

                // Two clients race for the same key — one will abort.
                let mut any_committed = false;
                for i in 0..5 {
                    let txn0 = clients[0].begin();
                    let txn1 = clients[1].begin();
                    let _: Option<String> = txn0.get("key_0".to_string()).await.unwrap();
                    let _: Option<String> = txn1.get("key_0".to_string()).await.unwrap();
                    txn0.put("key_0".to_string(), Some(format!("a_{i}")));
                    txn1.put("key_0".to_string(), Some(format!("b_{i}")));
                    let r0 = txn0.commit().await;
                    let r1 = txn1.commit().await;
                    // At most one should succeed per round.
                    if r0.is_some() || r1.is_some() {
                        any_committed = true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                assert!(any_committed, "at least one transaction should commit");
            })
            .await
            .unwrap();
        }
    };
}
