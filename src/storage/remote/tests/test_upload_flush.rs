use crate::tapir::{ShardNumber, Timestamp};
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn upload_segments_after_flush() {
    let (seg_store, man_store, _s3_config, _storage) =
        create_s3_stores("upload-segments-after-flush").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";

    let (mut record, mut tapir, dir) = open_buffered_store(shard);

    write_and_commit(
        &mut record, &mut tapir, shard,
        &[("key1", "val1"), ("key2", "val2")],
        Timestamp { time: 100, client_id: IrClientId(1) },
    );

    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Verify manifest was uploaded.
    let versions = man_store.list_manifest_versions(shard_name).await.unwrap();
    assert!(!versions.is_empty(), "expected at least one manifest version");

    // Verify we can download the manifest back.
    let (_view, bytes) = man_store.download_latest_manifest(shard_name).await.unwrap();
    assert!(!bytes.is_empty());
}
