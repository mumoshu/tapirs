use crate::remote_store::read_replica::ReadReplica;
use crate::remote_store::read_replica_refresh::refresh_once;
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

/// refresh_once picks up new data uploaded by the source shard.
/// Verifies ETag-based S3 cache invalidation: segments that changed
/// on S3 (e.g. active segments that grew between seals) are fully
/// re-downloaded, ensuring the read replica sees the latest data.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn refresh_sees_new_data() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("refresh-sees-new").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts1 = Timestamp { time: 300, client_id: IrClientId(1) };
    let ts2 = Timestamp { time: 400, client_id: IrClientId(1) };

    // Source: write initial data, flush, upload.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(&mut record, &mut tapir, shard, &[("rk1", "rv1")], ts1);
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Open read replica from S3.
    let replica_dir = tempfile::tempdir().unwrap();
    let replica = ReadReplica::open(&man_store, s3_config, shard, replica_dir.path())
        .await
        .unwrap();

    // Verify initial data is visible.
    let (val, _) = replica.get_at(&"rk1".to_string(), ts1).unwrap();
    assert_eq!(val.as_deref(), Some("rv1"));
    let view_before = replica.current_view();
    assert!(view_before > 0, "read replica should have non-zero view");

    // Source: write more data, flush, upload again.
    write_and_commit(&mut record, &mut tapir, shard, &[("rk2", "rv2")], ts2);
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Before refresh: new key not visible (None value or missing).
    if let Some((val, _)) = replica.get_at(&"rk2".to_string(), ts2) {
        assert!(val.is_none(), "rk2 should not have a value before refresh");
    }

    // Refresh: picks up the newer manifest and range-downloads grown segments.
    let refreshed = refresh_once(&replica, &man_store).await.unwrap();
    assert!(refreshed, "refresh should detect newer manifest");
    let view_after = replica.current_view();
    assert!(
        view_after > view_before,
        "view should advance: {view_before} -> {view_after}"
    );

    // After refresh: new key is visible.
    let h = replica.load_handle();
    let result: Result<(Option<String>, Timestamp), _> =
        h.do_uncommitted_get_at(&"rk2".to_string(), ts2);
    assert!(result.is_ok(), "rk2 should be readable after refresh: {:?}", result.err());
    let (val, _) = result.unwrap();
    assert_eq!(val.as_deref(), Some("rv2"));

    // Old key still visible (sealed segments cached, not re-downloaded).
    let result: Result<(Option<String>, Timestamp), _> =
        h.do_uncommitted_get_at(&"rk1".to_string(), ts1);
    assert!(result.is_ok(), "rk1 should still be readable: {:?}", result.err());
    let (val, _) = result.unwrap();
    assert_eq!(val.as_deref(), Some("rv1"));
}
