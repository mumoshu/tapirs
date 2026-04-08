use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    ChannelRegistry, ChannelTransport, IrMembership,
    discovery::{InMemoryShardDirectory, ShardDirectory as _},
};
use crate::mvcc::disk::disk_io::{BufferedIo, OpenFlags};
use crate::remote_store::config::S3StorageConfig;
use crate::remote_store::cross_shard_snapshot::{CrossShardSnapshot, ShardSnapshotInfo};
use crate::store_defaults::{S3BackedTapirReplica, S3BackedIrRecordStore};
use crate::tapir::{ShardNumber, Sharded};
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{create_s3_stores, poll_manifest_versions};

type Transport = ChannelTransport<S3BackedTapirReplica>;
type IrRep = crate::ir::Replica<S3BackedTapirReplica, Transport, S3BackedIrRecordStore>;

/// Build a source cluster: 3 BufferedIo replicas with S3 config.
fn build_source_shard(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    s3_config: &S3StorageConfig,
    registry: &ChannelRegistry<S3BackedTapirReplica>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> (Vec<Arc<IrRep>>, Vec<tempfile::TempDir>) {
    let num_replicas = 3;
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas).map(|n| n + initial_address).collect::<Vec<_>>(),
    );

    let mut tempdirs = Vec::new();
    let replicas: Vec<Arc<IrRep>> = (0..num_replicas)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            let mut inner = CombinedStoreInner::<String, String, BufferedIo>::open(
                dir.path(),
                OpenFlags { create: true, direct: false },
                shard,
                true,
            ).unwrap();
            inner.set_s3_config(s3_config.clone());
            let record_handle = inner.into_record_handle();
            let tapir_handle = record_handle.tapir_handle();
            let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);
            tempdirs.push(dir);

            let replica_rng = rng.fork();
            let d = Arc::clone(directory);
            let m = membership.clone();
            Arc::new_cyclic(|weak: &std::sync::Weak<IrRep>| {
                let weak = weak.clone();
                let channel: Transport = registry.channel(
                    move |from, message| weak.upgrade()?.receive(from, message),
                    Arc::clone(&d),
                );
                channel.set_shard(shard);
                crate::ir::Replica::new(
                    replica_rng,
                    m.clone(),
                    upcalls,
                    channel,
                    Some(S3BackedTapirReplica::tick),
                    record_handle,
                )
            })
        })
        .collect();

    directory.put(shard, membership, 0);
    (replicas, tempdirs)
}

/// Build a clone cluster: 3 BufferedIo replicas opened from S3 snapshot.
fn build_clone_shard(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    s3_config: &S3StorageConfig,
    snapshot: &CrossShardSnapshot,
    registry: &ChannelRegistry<S3BackedTapirReplica>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> (Vec<Arc<IrRep>>, Vec<tempfile::TempDir>) {
    let num_replicas = 3;
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas).map(|n| n + initial_address).collect::<Vec<_>>(),
    );

    let mut tempdirs = Vec::new();
    let replicas: Vec<Arc<IrRep>> = (0..num_replicas)
        .map(|_| {
            let clone_dir = tempfile::tempdir().unwrap();
            let (upcalls, record_handle) = crate::store_defaults::open_production_stores_from_s3(
                shard,
                clone_dir.path().to_str().unwrap(),
                0,
                true,
                s3_config,
                snapshot,
                None,
            ).unwrap();
            tempdirs.push(clone_dir);

            let replica_rng = rng.fork();
            let d = Arc::clone(directory);
            let m = membership.clone();
            Arc::new_cyclic(|weak: &std::sync::Weak<IrRep>| {
                let weak = weak.clone();
                let channel: Transport = registry.channel(
                    move |from, message| weak.upgrade()?.receive(from, message),
                    Arc::clone(&d),
                );
                channel.set_shard(shard);
                crate::ir::Replica::new(
                    replica_rng,
                    m.clone(),
                    upcalls,
                    channel,
                    Some(S3BackedTapirReplica::tick),
                    record_handle,
                )
            })
        })
        .collect();

    directory.put(shard, membership, 0);
    (replicas, tempdirs)
}

/// End-to-end: source cluster writes → S3 sync → clone cluster quorum_read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_source_write_clone_quorum_read() {
    let (_seg_store, man_store, s3_config, _storage) =
        create_s3_stores("e2e-clone-read").await;

    let shard = ShardNumber(0);

    // --- Build source cluster ---
    let mut rng = crate::Rng::from_seed(42);
    let src_registry = ChannelRegistry::<S3BackedTapirReplica>::default();
    let src_dir = Arc::new(InMemoryShardDirectory::new());

    let (src_replicas, _src_tempdirs) = build_source_shard(
        &mut rng, shard, &s3_config, &src_registry, &src_dir,
    );

    // Wait for initial view change on source cluster.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- Write data through source client ---
    let src_client_channel: Transport = src_registry.channel(
        move |_, _| unreachable!(),
        Arc::clone(&src_dir),
    );
    let src_client = Arc::new(crate::tapir::Client::<String, String, Transport>::new(
        rng.fork(),
        src_client_channel,
    ));

    eprintln!("[e2e] writing data to source cluster...");
    let txn = src_client.begin();
    txn.put(Sharded { shard, key: "hello".to_string() }, Some("world".to_string()));
    let commit_result = txn.commit().await;
    eprintln!("[e2e] commit result: {commit_result:?}");
    assert!(commit_result.is_some(), "source write should commit");
    let commit_ts = commit_result.unwrap();
    eprintln!("[e2e] committed at ts={commit_ts:?}");

    // --- Force view change to trigger S3 sync ---
    eprintln!("[e2e] forcing source view change for S3 sync...");
    src_replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- Wait for S3 upload ---
    eprintln!("[e2e] waiting for S3 upload...");
    let versions = poll_manifest_versions(&man_store, "shard_0", 1, 10).await;
    let manifest_view = *versions.last().unwrap();
    eprintln!("[e2e] S3 manifest_view={manifest_view}");

    // --- Build clone cluster ---
    let mut shards = BTreeMap::new();
    shards.insert(0u32, ShardSnapshotInfo { manifest_view });
    let snapshot = CrossShardSnapshot {
        timestamp: String::new(),
        cutoff_ts: commit_ts.time,
        ceiling_ts: commit_ts.time,
        shards,
    };

    let clone_registry = ChannelRegistry::<S3BackedTapirReplica>::default();
    let clone_dir = Arc::new(InMemoryShardDirectory::new());

    // Diagnostic: check what's in the S3 manifest for ir_inc
    {
        let man_bytes = man_store.download_manifest("shard_0", manifest_view).await.unwrap();
        let manifest: crate::unified::wisckeylsm::manifest::UnifiedManifest = bitcode::deserialize(&man_bytes).unwrap();
        eprintln!("[e2e] ir_inc: sealed_segs={} active_id={} active_offset={} sst_metas={}",
            manifest.ir_inc.sealed_vlog_segments.len(),
            manifest.ir_inc.active_segment_id,
            manifest.ir_inc.active_write_offset,
            manifest.ir_inc.sst_metas.len(),
        );
        for (i, seg) in manifest.ir_inc.sealed_vlog_segments.iter().enumerate() {
            eprintln!("[e2e] ir_inc sealed[{i}]: id={} path={:?} size={} views={:?}",
                seg.segment_id, seg.path, seg.total_size, seg.views);
        }
        for (i, sst) in manifest.ir_inc.sst_metas.iter().enumerate() {
            eprintln!("[e2e] ir_inc sst[{i}]: path={:?}", sst.path);
        }
    }

    let (clone_replicas, _clone_tempdirs) = tokio::task::block_in_place(|| {
        build_clone_shard(
            &mut rng, shard, &s3_config, &snapshot, &clone_registry, &clone_dir,
        )
    });

    // Force clone view change.
    eprintln!("[e2e] forcing clone view change...");
    clone_replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- Clone quorum_read ---
    let clone_shard_client = crate::tapir::ShardClient::<String, String, Transport>::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        shard,
        IrMembership::new(vec![0, 1, 2]),
        clone_registry.channel(move |_, _| unreachable!(), Arc::clone(&clone_dir)),
    );

    eprintln!("[e2e] clone quorum_read at ts={commit_ts:?}...");
    let result = clone_shard_client.quorum_read("hello".to_string(), commit_ts).await;
    eprintln!("[e2e] quorum_read result={result:?}");

    assert!(
        result.is_ok(),
        "clone quorum_read should succeed, got: {result:?}"
    );
    let (value, _ts) = result.unwrap();
    assert_eq!(
        value.as_deref(),
        Some("world"),
        "clone should read source data"
    );
}
