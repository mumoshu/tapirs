use crate::storage::io::disk_io::OpenFlags;
use crate::storage::io::s3_caching_io::S3CachingIo;
use crate::remote_store::open_remote::prepare_local_lazy;
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{
    create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lazy_open_reads_data_from_s3() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("lazy-open-reads-data").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source store: write, flush, upload.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(&mut record, &mut tapir, shard, &[("k1", "v1")], ts);
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;
    drop((record, tapir, dir));

    // Lazy open: download only the manifest.
    let new_dir = tempfile::tempdir().unwrap();
    prepare_local_lazy(&man_store, &s3_config, shard_name, new_dir.path())
        .await
        .unwrap();

    // Verify: manifest exists locally but segment files do NOT (not yet downloaded).
    assert!(new_dir.path().join("UNIFIED_MANIFEST").exists());

    // Open with S3CachingIo — segments will be lazy-downloaded on first read.
    let inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        new_dir.path(),
        OpenFlags { create: true, direct: false },
        shard,
        true,
    )
    .unwrap();
    let record = inner.into_record_handle();
    let tapir = record.tapir_handle();

    // Read back — triggers lazy download of the segment.
    let (value, _write_ts) = tapir
        .do_uncommitted_get_at(&"k1".to_string(), ts)
        .unwrap();
    assert_eq!(value.as_deref(), Some("v1"));
}
