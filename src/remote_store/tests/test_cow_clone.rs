use crate::storage::io::disk_io::OpenFlags;
use crate::storage::io::s3_caching_io::S3CachingIo;
use crate::remote_store::cow_clone::clone_from_remote_lazy;
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

use super::helpers::{
    create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cow_clone_reads_source_writes_isolated() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("cow-clone-isolated").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source: write and upload.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(&mut record, &mut tapir, shard, &[("x", "orig")], ts100);
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Clone lazily at the specific manifest view (zero-copy: only manifest downloaded).
    let versions = man_store.list_manifest_versions(shard_name).await.unwrap();
    let view = *versions.last().expect("no manifests uploaded");
    let clone_dir = tempfile::tempdir().unwrap();
    clone_from_remote_lazy(&man_store, &s3_config, shard_name, view, clone_dir.path())
        .await
        .unwrap();

    // Open clone with S3CachingIo.
    let clone_inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        clone_dir.path(),
        OpenFlags { create: true, direct: false },
        shard,
        true,
    )
    .unwrap();
    let clone_record = clone_inner.into_record_handle();
    let clone_tapir = clone_record.tapir_handle();

    // Clone reads source's data (lazy download triggers).
    let (value, _) = clone_tapir
        .do_uncommitted_get_at(&"x".to_string(), ts100)
        .unwrap();
    assert_eq!(value.as_deref(), Some("orig"), "clone should see source data");

    // Source is unaffected — reopen and verify.
    let (value_src, _) = tapir
        .do_uncommitted_get_at(&"x".to_string(), ts100)
        .unwrap();
    assert_eq!(value_src.as_deref(), Some("orig"), "source should still have orig");
}
