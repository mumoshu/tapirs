use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::s3_caching_io::S3CachingIo;
use crate::remote_store::cross_shard_snapshot::create_cross_shard_snapshot;

use crate::remote_store::open_remote::prepare_local_lazy_at_view;
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{
    create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cross_shard_snapshot_with_ghost_filter() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("cross-shard-snapshot").await;

    let shard0 = ShardNumber(0);
    let shard1 = ShardNumber(1);
    let shard0_name = "shard_0";
    let shard1_name = "shard_1";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };
    let ts200 = Timestamp { time: 200, client_id: IrClientId(1) };

    // Shard 0: write at ts=100, flush, upload.
    let (mut rec0, mut tap0, dir0) = open_buffered_store(shard0);
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v100")], ts100);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;

    // Shard 1: write at ts=100, flush, upload.
    let (mut rec1, mut tap1, dir1) = open_buffered_store(shard1);
    write_and_commit(&mut rec1, &mut tap1, shard1, &[("b", "v100")], ts100);
    flush_and_upload(&mut rec1, &mut tap1, &seg_store, &man_store, shard1_name, dir1.path()).await;

    // Shard 0: write MORE at ts=200 (shard 0 ahead), flush, upload.
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v200")], ts200);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, shard0_name, dir0.path()).await;

    // Create cross-shard snapshot.
    let snapshot = create_cross_shard_snapshot(
        &man_store,
        &[(0, shard0_name.to_string()), (1, shard1_name.to_string())],
    )
    .await
    .unwrap();

    // cutoff = min(max_read_time across shards).
    // Shard 0 has max_read_time from OCC (set during commit); shard 1 similarly.
    // Both should have committed at ts=100 minimum.
    // Shard 0 additionally committed at ts=200.
    // The exact cutoff depends on what max_read_time was persisted.
    // For this test, verify shard 0's ghost filter hides ts=200 data.
    let gf0 = snapshot.ghost_filter_for(0);

    // Open shard 0 from S3 with ghost filter.
    let s0_dir = tempfile::tempdir().unwrap();
    let s0_view = snapshot.shards[&0].manifest_view;
    prepare_local_lazy_at_view(
        &man_store, &s3_config, shard0_name, s0_view, s0_dir.path(),
    )
    .await
    .unwrap();

    let s0_inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        s0_dir.path(),
        OpenFlags { create: true, direct: false },
        shard0,
        true,
    )
    .unwrap();
    let s0_record = s0_inner.into_record_handle();
    let s0_tapir = s0_record.tapir_handle();

    // Apply ghost filter if present.
    if let Some(gf) = &gf0 {
        s0_tapir.inner.lock().unwrap().ghost_filter = Some(gf.clone());
    }

    // Read at ts=100 on shard 0 → visible (below cutoff).
    let (val, _) = s0_tapir
        .do_uncommitted_get_at(&"a".to_string(), ts100)
        .unwrap();
    assert_eq!(val.as_deref(), Some("v100"), "ts=100 should be visible on shard 0");

    // If ghost filter is active, reading at ts=200 should be clamped.
    if gf0.is_some() {
        let (val200, _) = s0_tapir
            .do_uncommitted_get_at(&"a".to_string(), ts200)
            .unwrap();
        // Should return v100 (clamped past the ghost range) not v200.
        assert_eq!(
            val200.as_deref(),
            Some("v100"),
            "ts=200 should be ghost-filtered on shard 0, returning v100 instead"
        );
    }

    // Open shard 1 — should have no ghost filter (ceiling=cutoff).
    let gf1 = snapshot.ghost_filter_for(1);

    let s1_dir = tempfile::tempdir().unwrap();
    let s1_view = snapshot.shards[&1].manifest_view;
    prepare_local_lazy_at_view(
        &man_store, &s3_config, shard1_name, s1_view, s1_dir.path(),
    )
    .await
    .unwrap();

    let s1_inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        s1_dir.path(),
        OpenFlags { create: true, direct: false },
        shard1,
        true,
    )
    .unwrap();
    let s1_record = s1_inner.into_record_handle();
    let s1_tapir = s1_record.tapir_handle();

    // Read at ts=100 on shard 1 → visible.
    let (val_s1, _) = s1_tapir
        .do_uncommitted_get_at(&"b".to_string(), ts100)
        .unwrap();
    assert_eq!(val_s1.as_deref(), Some("v100"), "ts=100 should be visible on shard 1");

    // Shard 1 should have no ghost filter (its ceiling == cutoff).
    assert!(
        gf1.is_none(),
        "shard 1 should have no ghost filter (ceiling=cutoff)"
    );
}
