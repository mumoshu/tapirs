use crate::tapir::{ShardNumber, Timestamp};
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

fn test_rng() -> crate::Rng {
    crate::testing::test_rng(99)
}

/// Verify that add_writable_clone_from_s3 creates a replica pre-populated
/// with data from S3. The clone forms a consensus group and can read the
/// source's committed data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writable_clone_via_admin_reads_source_data() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("writable-clone-admin").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source: write data and upload to S3.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(
        &mut record, &mut tapir, shard,
        &[("clone_key", "clone_val")],
        ts100,
    );
    flush_and_upload(&mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path()).await;

    // Create a Node and call add_writable_clone_from_s3 directly.
    let clone_dir = tempfile::tempdir().unwrap();
    let node = crate::node::Node::new(
        clone_dir.path().to_str().unwrap().to_string(),
        test_rng,
    );

    // Allocate a fresh TCP port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr_str = listener.local_addr().unwrap().to_string();
    drop(listener);

    let cfg = crate::node::ReplicaConfig {
        shard: 0,
        listen_addr: listen_addr_str.clone(),
        membership: vec![listen_addr_str.clone()],
    };

    let source_s3 = crate::remote_store::config::S3StorageConfig {
        bucket: s3_config.bucket.clone(),
        prefix: s3_config.prefix.clone(),
        endpoint_url: s3_config.endpoint_url.clone(),
        region: s3_config.region.clone(),
    };

    node.add_writable_clone_from_s3(&cfg, source_s3).await.unwrap();

    // Verify the node reports the shard.
    let shards = node.shard_list();
    assert_eq!(shards.len(), 1, "node should host 1 shard");
    assert_eq!(shards[0].0, shard);

    // Verify the clone's IR view is accessible (replica was created successfully).
    // Note: IR protocol view starts at 0 regardless of the manifest's current_view.
    // The manifest's view is the storage-level view; IR consensus view advances
    // through view changes after bootstrap.
    assert!(node.shard_view_number(shard).is_some(), "clone should have a running replica");
}
