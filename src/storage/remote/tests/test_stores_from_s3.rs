use std::collections::BTreeMap;
use crate::storage::remote::cross_shard_snapshot::{CrossShardSnapshot, ShardSnapshotInfo};
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

/// Verify that open_production_stores_from_s3 creates a store pre-populated
/// with data from S3 via zero-copy clone (manifest only, segments lazy).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_stores_from_s3_reads_source_data() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("stores-from-s3").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source: write and upload.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(&mut record, &mut tapir, shard, &[("key1", "val1"), ("key2", "val2")], ts100);
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Build a single-shard CrossShardSnapshot. Single shard means
    // cutoff_ts == ceiling_ts → no ghost range → no prepared txns removed.
    let versions = man_store.list_manifest_versions(shard_name).await.unwrap();
    let manifest_view = *versions.last().expect("no manifests uploaded");
    let mut shards_map = BTreeMap::new();
    shards_map.insert(0u32, ShardSnapshotInfo { manifest_view });
    let snapshot = CrossShardSnapshot {
        timestamp: String::new(),
        cutoff_ts: ts100.time,
        ceiling_ts: ts100.time,
        shards: shards_map,
    };

    // Open production stores from S3 with the snapshot.
    let source_s3 = s3_config.clone();
    let clone_dir = tempfile::tempdir().unwrap();
    let persist_dir = clone_dir.path().to_str().unwrap().to_string();

    let (_upcalls, record_handle) = tokio::task::block_in_place(|| {
        crate::storage::defaults::open_production_stores_from_s3(
            shard,
            &persist_dir,
            0,
            true,
            &source_s3,
            &snapshot,
            None, // no destination S3
        )
        .unwrap()
    });

    // Verify the store reads source data (triggers lazy S3 download).
    let tapir_handle = record_handle.tapir_handle();
    let (val, _) = tapir_handle.do_uncommitted_get_at(&"key1".to_string(), ts100).unwrap();
    assert_eq!(val.as_deref(), Some("val1"), "should read key1 from S3");

    let (val, _) = tapir_handle.do_uncommitted_get_at(&"key2".to_string(), ts100).unwrap();
    assert_eq!(val.as_deref(), Some("val2"), "should read key2 from S3");
}
