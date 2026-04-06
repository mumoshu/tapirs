//! End-to-end S3 lifecycle test.
//!
//! Exercises multi-shard writes → flush → S3 upload → incremental backup
//! → BackupDescriptor → CrossShardSnapshot → zero-copy clone → read replica
//! refresh in a single linear test against a shared MinIO bucket.

use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::s3_caching_io::S3CachingIo;
use crate::remote_store::backup_descriptor::create_backup;
use crate::remote_store::cow_clone::clone_from_remote_lazy;
use crate::remote_store::cross_shard_snapshot::create_cross_shard_snapshot;
use crate::remote_store::read_replica::ReadReplica;
use crate::remote_store::read_replica_refresh::refresh_once;
use crate::remote_store::upload::diff_manifests;
use crate::tapir::store::TapirStore;
use crate::tapir::{ShardNumber, Timestamp};
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_multi_shard_s3_lifecycle() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("e2e-lifecycle").await;

    let shard0 = ShardNumber(0);
    let shard1 = ShardNumber(1);
    let shard0_name = "shard_0";
    let shard1_name = "shard_1";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };
    let ts200 = Timestamp { time: 200, client_id: IrClientId(1) };
    let ts400 = Timestamp { time: 400, client_id: IrClientId(1) };

    // ── Phase 1: Source cluster — multi-shard writes + flush + verify S3 ──

    let (mut rec0, mut tap0, dir0) = open_buffered_store(shard0);
    let (mut rec1, mut tap1, dir1) = open_buffered_store(shard1);

    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v100"), ("b", "v100")], ts100);
    write_and_commit(&mut rec1, &mut tap1, shard1, &[("x", "v100"), ("y", "v100")], ts100);

    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;
    flush_and_upload(&mut rec1, &mut tap1, &seg_store, &man_store, shard1_name, dir1.path()).await;

    let versions0 = man_store.list_manifest_versions(shard0_name).await.unwrap();
    let versions1 = man_store.list_manifest_versions(shard1_name).await.unwrap();
    assert!(!versions0.is_empty(), "shard 0 should have manifest on S3");
    assert!(!versions1.is_empty(), "shard 1 should have manifest on S3");

    // ── Phase 2: Incremental backup — additional writes, diff, descriptors ──

    let manifest_before = tap0.inner.lock().unwrap().tapir_manifest.clone();
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v200")], ts200);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;
    let manifest_after = tap0.inner.lock().unwrap().tapir_manifest.clone();

    let diff = diff_manifests(&manifest_before, &manifest_after);
    assert!(!diff.is_empty(), "incremental diff should contain newly sealed segments");

    let shard_names = vec![(0, shard0_name.to_string()), (1, shard1_name.to_string())];
    let backup = create_backup(&man_store, &shard_names).await.unwrap();
    assert!(backup.shards.contains_key(&0), "backup should contain shard 0");
    assert!(backup.shards.contains_key(&1), "backup should contain shard 1");

    let snapshot = create_cross_shard_snapshot(&man_store, &shard_names).await.unwrap();
    assert!(snapshot.shards.contains_key(&0), "snapshot should contain shard 0");
    assert!(snapshot.shards.contains_key(&1), "snapshot should contain shard 1");
    // Shard 0 is ahead (ts=200), shard 1 at ts=100.
    // Ghost filter hides entries above cutoff on the ahead shard.
    let _gf0 = snapshot.ghost_filter_for(0);
    let _gf1 = snapshot.ghost_filter_for(1);

    // ── Phase 3: Zero-copy clone ──

    let clone_dir = tempfile::tempdir().unwrap();
    clone_from_remote_lazy(&man_store, &s3_config, shard0_name, None, clone_dir.path())
        .await
        .unwrap();

    let clone_inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        clone_dir.path(),
        OpenFlags { create: true, direct: false },
        shard0,
        true,
    )
    .unwrap();
    let clone_record = clone_inner.into_record_handle();
    let clone_tapir = clone_record.tapir_handle();

    // Clone reads source's data (triggers lazy S3 download).
    let (val, _) = clone_tapir.do_uncommitted_get_at(&"a".to_string(), ts100).unwrap();
    assert_eq!(val.as_deref(), Some("v100"), "clone should see ts=100 data");
    let (val, _) = clone_tapir.do_uncommitted_get_at(&"a".to_string(), ts200).unwrap();
    assert_eq!(val.as_deref(), Some("v200"), "clone should see ts=200 data");

    // Source still has its data (isolation).
    let (val_src, _) = tap0.do_uncommitted_get_at(&"a".to_string(), ts200).unwrap();
    assert_eq!(val_src.as_deref(), Some("v200"), "source should still have data");

    // ── Phase 4: Read-only auto-refreshing replica ──

    let replica_dir = tempfile::tempdir().unwrap();
    let replica = ReadReplica::open(&man_store, s3_config, shard0, replica_dir.path())
        .await
        .unwrap();

    // Verify initial data.
    let initial = replica.get_at(&"a".to_string(), ts200);
    assert!(initial.is_some(), "replica should see ts=200 data");
    let (val, _) = initial.unwrap();
    assert_eq!(val.as_deref(), Some("v200"));
    let view_before = replica.current_view();

    // Source writes more, flushes, uploads.
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v400")], ts400);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;

    // Refresh picks up new data.
    let refreshed = refresh_once(&replica, &man_store).await.unwrap();
    assert!(refreshed, "refresh should detect newer manifest");
    assert!(replica.current_view() > view_before, "view should advance");

    let h = replica.load_handle();
    let (val, _) = h.do_uncommitted_get_at(&"a".to_string(), ts400).unwrap();
    assert_eq!(val.as_deref(), Some("v400"), "replica should see ts=400 after refresh");

    // Second refresh is a no-op (already up to date).
    let refreshed_again = refresh_once(&replica, &man_store).await.unwrap();
    assert!(!refreshed_again, "second refresh should be no-op");
}
