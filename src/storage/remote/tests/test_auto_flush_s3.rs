use crate::ir::IrRecordStore;
use crate::storage::io::disk_io::OpenFlags;
use crate::storage::io::s3_caching_io::S3CachingIo;
use crate::storage::remote::open_remote::prepare_local_lazy;
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::storage::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{create_s3_stores, poll_manifest_versions, write_and_commit};

/// Setting s3_config on CombinedStoreInner causes seal_tapir_side to
/// automatically upload segments and manifest to S3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_flush_uploads_to_s3() {
    let (_seg_store, man_store, s3_config, _storage) =
        create_s3_stores("auto-flush-uploads").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts = Timestamp { time: 200, client_id: IrClientId(1) };

    let dir = tempfile::tempdir().unwrap();
    let mut inner = CombinedStoreInner::<String, String, crate::storage::io::disk_io::BufferedIo>::open(
        dir.path(),
        OpenFlags { create: true, direct: false },
        shard,
        true,
    )
    .unwrap();
    inner.set_s3_config(s3_config.clone());
    let mut record = inner.into_record_handle();
    let mut tapir = record.tapir_handle();

    write_and_commit(&mut record, &mut tapir, shard, &[("auto_k", "auto_v")], ts);
    record.flush();
    tapir.flush();

    // Poll for S3 upload — the spawned sync_to_remote task is fire-and-forget.
    poll_manifest_versions(&man_store, shard_name, 1, 10).await;

    // Open read replica from S3 and verify data.
    let new_dir = tempfile::tempdir().unwrap();
    prepare_local_lazy(&man_store, &s3_config, shard_name, new_dir.path())
        .await
        .unwrap();

    let read_inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        new_dir.path(),
        OpenFlags { create: true, direct: false },
        shard,
        true,
    )
    .unwrap();
    let read_record = read_inner.into_record_handle();
    let read_tapir = read_record.tapir_handle();

    let (value, _) = read_tapir
        .do_uncommitted_get_at(&"auto_k".to_string(), ts)
        .unwrap();
    assert_eq!(value.as_deref(), Some("auto_v"));
}
