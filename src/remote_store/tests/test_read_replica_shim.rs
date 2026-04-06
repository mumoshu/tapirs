use crate::tapir::{ShardNumber, Timestamp};
use crate::IrClientId;

use super::helpers::{create_s3_stores, flush_and_upload, open_buffered_store, write_and_commit};

fn test_rng() -> crate::Rng {
    crate::testing::test_rng(77)
}

/// Verify that add_read_replica_from_s3 creates a read-only replica backed
/// by S3 with a functioning ReadReplicaShim that serves RequestUnlogged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_replica_shim_serves_reads() {
    let (seg_store, man_store, s3_config, _storage) =
        create_s3_stores("read-replica-shim").await;
    let shard = ShardNumber(0);
    let shard_name = "shard_0";
    let ts100 = Timestamp { time: 100, client_id: IrClientId(1) };

    // Source: write data and upload to S3.
    let (mut record, mut tapir, dir) = open_buffered_store(shard);
    write_and_commit(
        &mut record,
        &mut tapir,
        shard,
        &[("rr_key1", "rr_val1"), ("rr_key2", "rr_val2")],
        ts100,
    );
    flush_and_upload(
        &mut record, &mut tapir, &seg_store, &man_store, shard_name, dir.path(),
    )
    .await;

    // Create a Node and add a read replica from S3.
    let replica_dir = tempfile::tempdir().unwrap();
    let node = crate::node::Node::new(
        replica_dir.path().to_str().unwrap().to_string(),
        test_rng,
    );

    // Allocate a fresh TCP port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr = listener.local_addr().unwrap();
    drop(listener);

    let source_s3 = crate::remote_store::config::S3StorageConfig {
        bucket: s3_config.bucket.clone(),
        prefix: s3_config.prefix.clone(),
        endpoint_url: s3_config.endpoint_url.clone(),
        region: s3_config.region.clone(),
    };

    node.add_read_replica_from_s3(
        shard,
        listen_addr,
        source_s3,
        std::time::Duration::from_secs(60), // long interval — we don't test refresh here
    )
    .await
    .unwrap();

    // Verify the node reports the read replica.
    let read_list = node.read_replica_list();
    assert_eq!(read_list.len(), 1, "node should host 1 read replica");
    assert_eq!(read_list[0].0, shard);
    assert_eq!(read_list[0].1, listen_addr);

    // Verify no writable replicas.
    assert!(node.shard_list().is_empty(), "no writable replicas expected");

    // Send a RequestUnlogged(GetAt) via TCP to verify the shim works.
    // Use the bitcode TCP transport to send a message to the read replica.
    use crate::ir::message::{MessageImpl, RequestUnlogged};
    use crate::tapir::UO;
    use crate::transport::tokio_bitcode_tcp::TcpTransport;
    use crate::{TcpAddress, Transport as _};

    // Create a client transport to send to the read replica.
    let client_addr = TcpAddress("127.0.0.1:0".parse().unwrap());
    let client_dir = std::sync::Arc::new(
        crate::discovery::InMemoryShardDirectory::new(),
    );
    let client_transport = TcpTransport::<crate::store_defaults::ProductionTapirReplica>::with_directory(
        client_addr, client_dir,
    );

    let msg: MessageImpl<_, _, _, _, _, _, _, _> =
        MessageImpl::RequestUnlogged(RequestUnlogged {
            op: UO::GetAt {
                key: "rr_key1".to_string(),
                timestamp: ts100,
            },
        });

    let reply = client_transport
        .send::<crate::ir::message::ReplyUnlogged<
            crate::tapir::UR<String, String>,
            TcpAddress,
        >>(TcpAddress(listen_addr), msg)
        .await;

    match reply.result {
        crate::tapir::UR::GetAt(val, _ts) => {
            assert_eq!(val.as_deref(), Some("rr_val1"), "shim should return source data");
        }
        other => panic!("unexpected reply: {other:?}"),
    }
}
