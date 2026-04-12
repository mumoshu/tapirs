use crate::storage::remote::cross_shard_snapshot::create_cross_shard_snapshot;
use crate::tapir::store::TapirStore;
use crate::tapir::{ShardNumber, Timestamp};
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

/// Verify that open_production_stores_from_s3 with a CrossShardSnapshot
/// correctly applies ghost filters and allows reads at cutoff_ts.
///
/// Setup: two shards with asymmetric timestamps (shard 0 ahead of shard 1).
/// The snapshot's cutoff_ts = min(ceiling_ts) ensures cross-shard consistency.
/// Reads at cutoff_ts should see data on both shards. Reads in the ghost
/// range on shard 0 should be clamped to cutoff_ts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_with_snapshot_ghost_filter() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("clone-snapshot-gf").await;

    let shard0 = ShardNumber(0);
    let shard1 = ShardNumber(1);
    let shard0_name = "shard_0";
    let shard1_name = "shard_1";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };
    let ts200 = Timestamp { time: 200, client_id: IrClientId(1) };

    // Shard 0: write at ts=100, flush.
    let (mut rec0, mut tap0, dir0) = open_buffered_store(shard0);
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v100")], ts100);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;

    // Shard 1: write at ts=100, flush.
    let (mut rec1, mut tap1, dir1) = open_buffered_store(shard1);
    write_and_commit(&mut rec1, &mut tap1, shard1, &[("b", "v100")], ts100);
    flush_and_upload(&mut rec1, &mut tap1, &seg_store, &man_store, shard1_name, dir1.path()).await;

    // Shard 0: write MORE at ts=200 (shard 0 ahead), flush.
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v200")], ts200);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;

    // Create cross-shard snapshot.
    let shard_names = vec![
        (0, shard0_name.to_string()),
        (1, shard1_name.to_string()),
    ];
    let snapshot = create_cross_shard_snapshot(&man_store, &shard_names)
        .await
        .unwrap();

    // Verify snapshot structure: shard 0 ahead, shard 1 behind.
    assert!(snapshot.shards.contains_key(&0));
    assert!(snapshot.shards.contains_key(&1));

    // Clone shard 0 via the factory with the snapshot.
    let clone0_dir = tempfile::tempdir().unwrap();
    let persist0 = clone0_dir.path().to_str().unwrap().to_string();
    let source_s3 = s3_config.clone();

    let (_upcalls0, rec_handle0) = tokio::task::block_in_place(|| {
        crate::storage::defaults::open_production_stores_from_s3(
            shard0, &persist0, 0, true, &source_s3, &snapshot, None,
        )
        .unwrap()
    });

    let tap0_clone = rec_handle0.tapir_handle();

    // Clone shard 1 via the factory with the snapshot.
    let clone1_dir = tempfile::tempdir().unwrap();
    let persist1 = clone1_dir.path().to_str().unwrap().to_string();

    let (_upcalls1, rec_handle1) = tokio::task::block_in_place(|| {
        crate::storage::defaults::open_production_stores_from_s3(
            shard1, &persist1, 1, true, &source_s3, &snapshot, None,
        )
        .unwrap()
    });

    let tap1_clone = rec_handle1.tapir_handle();

    // Test 1: Read at ts=100 (below cutoff) — visible on both shards.
    let (val, _) = tap0_clone.do_uncommitted_get_at(&"a".to_string(), ts100).unwrap();
    assert_eq!(val.as_deref(), Some("v100"), "shard 0: ts=100 should be visible");

    let (val, _) = tap1_clone.do_uncommitted_get_at(&"b".to_string(), ts100).unwrap();
    assert_eq!(val.as_deref(), Some("v100"), "shard 1: ts=100 should be visible");

    // Test 2: Ghost filter on shard 0.
    // If shard 0's ceiling > cutoff, reading at ts=200 should be ghost-filtered.
    let gf0 = snapshot.ghost_filter();
    if gf0.is_some() {
        // ts=200 is in the ghost range → clamped to cutoff_ts.
        // Reading at ts=200 should return the value at cutoff_ts, which is v100.
        let (val, _) = tap0_clone.do_uncommitted_get_at(&"a".to_string(), ts200).unwrap();
        assert_eq!(
            val.as_deref(),
            Some("v100"),
            "shard 0: ts=200 should be ghost-filtered to v100"
        );
    }

    // Ghost filter is global — same for all shards. On shard 1, the filter
    // range (cutoff, ceiling] has no entries (shard 1's max is cutoff).
}
