use crate::ir::IrRecordStore;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::s3_caching_io::S3CachingIo;
use crate::remote_store::cow_clone::clone_from_remote_lazy;
use crate::remote_store::cross_shard_snapshot::{CrossShardSnapshot, ShardSnapshotInfo};
use crate::remote_store::open_remote::prepare_local_lazy;
use crate::tapir::store::TapirStore;
use crate::tapir::{ShardNumber, Timestamp};
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{create_s3_stores, write_and_commit};

use std::collections::BTreeMap;

/// Production-path test: write data, flush via sync_to_remote (not the
/// cheating flush_and_upload), clone from S3, verify the clone reads
/// the data via resolve_value (the full IR chain).
///
/// This exercises the exact code path that failed in K8s E2E: the leader's
/// install_merged_record must accumulate IR inc data to the active segment,
/// and sync_to_remote must upload it via write_if_larger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_reads_source_data_via_production_s3_path() {
    let (_seg_store, man_store, s3_config, _storage) =
        create_s3_stores("clone-production-path").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source: open with S3 config so flush triggers sync_to_remote.
    let dir = tempfile::tempdir().unwrap();
    let mut inner =
        CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
            dir.path(),
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .unwrap();
    inner.set_s3_config(s3_config.clone());
    let mut record = inner.into_record_handle();
    let mut tapir = record.tapir_handle();

    // Write data and flush (view 1).
    write_and_commit(&mut record, &mut tapir, shard, &[("hello", "world")], ts100);
    record.flush();
    tapir.flush();

    // Second flush (view 2) — exercises install_merged_record with base_view > 0.
    record.flush();
    tapir.flush();

    // Wait for spawned S3 upload tasks to complete.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Verify manifest was uploaded.
    let versions = man_store
        .list_manifest_versions(shard_name)
        .await
        .unwrap();
    assert!(
        versions.len() >= 2,
        "expected >= 2 manifest versions, got {}",
        versions.len()
    );

    // Clone from S3 and verify data is readable.
    let clone_dir = tempfile::tempdir().unwrap();
    let manifest_view = *versions.last().unwrap();
    let mut shards = BTreeMap::new();
    shards.insert(
        0u32,
        ShardSnapshotInfo {
            manifest_view,
        },
    );
    let snapshot = CrossShardSnapshot {
        timestamp: String::new(),
        cutoff_ts: ts100.time,
        ceiling_ts: ts100.time,
        shards,
    };

    tokio::task::block_in_place(|| {
        crate::store_defaults::open_production_stores_from_s3(
            shard,
            clone_dir.path().to_str().unwrap(),
            0,
            true,
            &s3_config,
            &snapshot,
            None,
        )
    })
    .unwrap();

    // Open the cloned store directly — exercises resolve_value chain
    // (MVCC entry → committed VlogLsm → IR inc_lsm → IO::Commit).
    let clone_inner =
        CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
            &clone_dir.path().join("shard_0"),
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .unwrap();
    let clone_record = clone_inner.into_record_handle();
    let clone_tapir = clone_record.tapir_handle();

    let (val, _) = clone_tapir
        .do_uncommitted_get_at(&"hello".to_string(), ts100)
        .unwrap();
    assert_eq!(
        val.as_deref(),
        Some("world"),
        "clone should read source data via production S3 path"
    );
}

/// Verify that after multiple view changes, the S3 bucket contains IR inc
/// segment data (active segment with non-zero size).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ir_inc_segments_uploaded_after_view_changes() {
    let (_seg_store, man_store, s3_config, storage) =
        create_s3_stores("ir-inc-uploaded").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts = Timestamp { time: 300, client_id: IrClientId(1) };

    let dir = tempfile::tempdir().unwrap();
    let mut inner =
        CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
            dir.path(),
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .unwrap();
    inner.set_s3_config(s3_config.clone());
    let mut record = inner.into_record_handle();
    let mut tapir = record.tapir_handle();

    // Write + 3 view changes.
    write_and_commit(&mut record, &mut tapir, shard, &[("k", "v")], ts);
    for _ in 0..3 {
        record.flush();
        tapir.flush();
    }

    // Wait for S3 uploads.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Check IR inc active segment on S3 has data.
    use crate::backup::storage::BackupStorage;
    let seg_sub = storage.sub(shard_name).sub("segments");
    let ir_inc_size = seg_sub
        .size("ir_inc_vlog_0000.dat")
        .await
        .unwrap();
    assert!(
        ir_inc_size.is_some_and(|s| s > 0),
        "ir_inc_vlog_0000.dat should have non-zero size on S3, got {:?}",
        ir_inc_size
    );
}

/// Verify that resolve_value returns an explicit error (not silent None)
/// when the IR chain is broken (IR inc segment missing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_value_errors_on_broken_ir_chain() {
    let (_seg_store, man_store, s3_config, _storage) =
        create_s3_stores("broken-ir-chain").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts = Timestamp { time: 400, client_id: IrClientId(1) };

    // Source: write + flush with S3 config.
    let dir = tempfile::tempdir().unwrap();
    let mut inner =
        CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
            dir.path(),
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .unwrap();
    inner.set_s3_config(s3_config.clone());
    let mut record = inner.into_record_handle();
    let mut tapir = record.tapir_handle();

    write_and_commit(&mut record, &mut tapir, shard, &[("broken_k", "broken_v")], ts);
    record.flush();
    tapir.flush();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Clone from S3.
    let versions = man_store.list_manifest_versions(shard_name).await.unwrap();
    let manifest_view = *versions.last().unwrap();
    let clone_dir = tempfile::tempdir().unwrap();
    prepare_local_lazy(&man_store, &s3_config, shard_name, clone_dir.path())
        .await
        .unwrap();

    // Delete the IR inc segment to break the chain.
    let ir_inc_path = clone_dir.path().join("ir_inc_vlog_0000.dat");
    if ir_inc_path.exists() {
        std::fs::remove_file(&ir_inc_path).unwrap();
    }
    // Create an empty file so open doesn't fail on missing file.
    std::fs::write(&ir_inc_path, b"").unwrap();

    let clone_inner =
        CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
            clone_dir.path(),
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .unwrap();
    let clone_record = clone_inner.into_record_handle();
    let clone_tapir = clone_record.tapir_handle();

    // Reading should return an error, not Ok(None).
    let result = clone_tapir.do_uncommitted_get_at(&"broken_k".to_string(), ts);
    assert!(
        result.is_err(),
        "resolve_value should return error on broken IR chain, got {:?}",
        result
    );
}
