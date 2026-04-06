use std::collections::BTreeMap;

use crate::remote_store::cross_shard_snapshot::{CrossShardSnapshot, ShardSnapshotInfo};
use crate::tapir::store::TapirStore;
use crate::tapir::{ShardNumber, Timestamp};
use crate::IrClientId;

use super::helpers::{
    create_s3_stores, flush_and_upload, open_buffered_store, prepare_only, write_and_commit,
};

/// Prepared txn at ts in ghost range (cutoff_ts, ceiling_ts] is removed.
/// Prepared txn at ts <= cutoff_ts is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_removes_prepared_in_ghost_range_preserves_below_cutoff() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("clone-prepared-txn").await;

    let shard0 = ShardNumber(0);
    let shard1 = ShardNumber(1);
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };
    let ts200 = Timestamp { time: 200, client_id: IrClientId(1) };
    let ts100b = Timestamp { time: 100, client_id: IrClientId(2) };

    // Shard 0: commit at ts=100, prepare-only at ts=200.
    let (mut rec0, mut tap0, dir0) = open_buffered_store(shard0);
    write_and_commit(&mut rec0, &mut tap0, shard0, &[("a", "v100")], ts100);
    prepare_only(&mut rec0, &mut tap0, shard0, &[("a", "v200")], ts200);
    flush_and_upload(&mut rec0, &mut tap0, &seg_store, &man_store, "shard_0", dir0.path()).await;

    // Shard 1: commit at ts=100, prepare-only at ts=100.
    let (mut rec1, mut tap1, dir1) = open_buffered_store(shard1);
    write_and_commit(&mut rec1, &mut tap1, shard1, &[("b", "v100")], ts100);
    prepare_only(&mut rec1, &mut tap1, shard1, &[("b", "v100b")], ts100b);
    flush_and_upload(&mut rec1, &mut tap1, &seg_store, &man_store, "shard_1", dir1.path()).await;

    // Construct snapshot manually with known ceiling values.
    // Shard 0 ceiling=200, shard 1 ceiling=100 → cutoff=100.
    // Ghost range on shard 0: (100, 200]. Shard 1: empty (100==100).
    let v0 = *man_store.list_manifest_versions("shard_0").await.unwrap().last().unwrap();
    let v1 = *man_store.list_manifest_versions("shard_1").await.unwrap().last().unwrap();
    let mut shards = BTreeMap::new();
    shards.insert(0, ShardSnapshotInfo { manifest_view: v0, ceiling_ts: 200 });
    shards.insert(1, ShardSnapshotInfo { manifest_view: v1, ceiling_ts: 100 });
    let snapshot = CrossShardSnapshot { timestamp: String::new(), cutoff_ts: 100, shards };

    // Clone shard 0: prepared txn at ts=200 is in ghost range → removed.
    let d0 = tempfile::tempdir().unwrap();
    let (_u0, r0) = tokio::task::block_in_place(|| {
        crate::store_defaults::open_production_stores_from_s3(
            shard0, d0.path().to_str().unwrap(), 0, true, &s3_config, &snapshot, None,
        ).unwrap()
    });
    assert_eq!(r0.tapir_handle().prepared_count(), 0,
        "shard 0: prepared at ts=200 (ghost range) should be removed");

    // Clone shard 1: prepared txn at ts=100 is at cutoff → preserved.
    let d1 = tempfile::tempdir().unwrap();
    let (_u1, r1) = tokio::task::block_in_place(|| {
        crate::store_defaults::open_production_stores_from_s3(
            shard1, d1.path().to_str().unwrap(), 1, true, &s3_config, &snapshot, None,
        ).unwrap()
    });
    assert_eq!(r1.tapir_handle().prepared_count(), 1,
        "shard 1: prepared at ts<=cutoff should be preserved");
}
