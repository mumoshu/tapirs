use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backup::s3backup::S3BackupStorage;
use crate::backup::storage::BackupStorage;
use crate::ir::{IrRecordStore, OpId, RecordConsensusEntry, RecordEntryState,
    RecordInconsistentEntry, ViewNumber};
use crate::mvcc::disk::disk_io::{BufferedIo, DiskIo, OpenFlags};
use crate::occ::{PrepareResult, Transaction, TransactionId};
use crate::remote_store::config::S3StorageConfig;
use crate::remote_store::manifest_store::RemoteManifestStore;
use crate::remote_store::segment_store::RemoteSegmentStore;
use crate::tapir::{CO, CR, IO, ShardNumber, Timestamp};
use crate::tapir::store::TapirStore;
use crate::unified::combined::record_handle::CombinedRecordHandle;
use crate::unified::combined::tapir_handle::CombinedTapirHandle;
use crate::unified::combined::CombinedStoreInner;
use crate::IrClientId;

static COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn test_flags() -> OpenFlags {
    OpenFlags { create: true, direct: false }
}

/// Open a CombinedStore with BufferedIo in a tempdir.
pub fn open_buffered_store(
    shard: ShardNumber,
) -> (
    CombinedRecordHandle<String, String, BufferedIo>,
    CombinedTapirHandle<String, String, BufferedIo>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let inner = CombinedStoreInner::<String, String, BufferedIo>::open(
        dir.path(), test_flags(), shard, true,
    ).unwrap();
    let record_handle = inner.into_record_handle();
    let tapir_handle = record_handle.tapir_handle();
    (record_handle, tapir_handle, dir)
}

/// Prepare and commit a transaction with the given writes.
/// Generic over DiskIo type so it works with BufferedIo and S3CachingIo.
pub fn write_and_commit<DIO: DiskIo + Sync>(
    record: &mut CombinedRecordHandle<String, String, DIO>,
    tapir: &mut CombinedTapirHandle<String, String, DIO>,
    shard: ShardNumber,
    writes: &[(&str, &str)],
    commit_ts: Timestamp,
) {
    use std::sync::Arc;

    let prep_id = OpId { client_id: IrClientId(next_id()), number: next_id() };
    let commit_id = OpId { client_id: IrClientId(next_id()), number: next_id() };
    let txn_id = TransactionId {
        client_id: IrClientId(next_id()),
        number: next_id(),
    };

    let mut txn = Transaction::<String, String, Timestamp>::default();
    for (key, value) in writes {
        txn.add_write(
            crate::tapir::Sharded { shard, key: key.to_string() },
            Some(value.to_string()),
        );
    }
    let shared_txn = Arc::new(txn);

    record.insert_consensus_entry(
        prep_id,
        RecordConsensusEntry {
            op: CO::Prepare {
                transaction_id: txn_id,
                transaction: shared_txn.clone(),
                commit: commit_ts,
            },
            result: CR::Prepare(PrepareResult::Ok),
            state: RecordEntryState::Finalized(ViewNumber(0)),
            modified_view: 0,
        },
    );
    record.insert_inconsistent_entry(
        commit_id,
        RecordInconsistentEntry {
            op: IO::Commit {
                transaction_id: txn_id,
                transaction: shared_txn.clone(),
                commit: commit_ts,
            },
            state: RecordEntryState::Finalized(ViewNumber(0)),
            modified_view: 0,
        },
    );

    tapir.add_or_replace_or_finalize_prepared_txn(
        prep_id, txn_id, shared_txn.clone(), commit_ts, true,
    );
    tapir.commit_txn(commit_id, txn_id, &shared_txn, commit_ts);
}

/// Flush both IR and TAPIR sides, then upload ALL files to S3.
///
/// Uses upload_new_segments with the full file list from the manifest
/// (not just the diff) to ensure active segments are also uploaded.
pub async fn flush_and_upload(
    record: &mut CombinedRecordHandle<String, String, BufferedIo>,
    tapir: &mut CombinedTapirHandle<String, String, BufferedIo>,
    seg_store: &RemoteSegmentStore<S3BackupStorage>,
    man_store: &RemoteManifestStore<S3BackupStorage>,
    shard: &str,
    base_dir: &Path,
) {
    record.flush();
    tapir.flush();
    let manifest = tapir.inner.lock().unwrap().tapir_manifest.clone();
    // Force-upload all files (sealed segments may overwrite stale active copies).
    let all_files = crate::remote_store::upload::all_manifest_files(&manifest);
    crate::remote_store::upload::upload_segments_force(seg_store, shard, base_dir, &all_files)
        .await
        .unwrap();
    let manifest_bytes = bitcode::serialize(&manifest).unwrap();
    crate::remote_store::upload::upload_manifest_snapshot(
        man_store, shard, manifest.current_view, &manifest_bytes,
    )
    .await
    .unwrap();
}

/// Create S3 stores from a MinIO test bucket.
pub async fn create_s3_stores(
    test_name: &str,
) -> (
    RemoteSegmentStore<S3BackupStorage>,
    RemoteManifestStore<S3BackupStorage>,
    S3StorageConfig,
    S3BackupStorage,
) {
    let storage = crate::remote_store::test_helpers::minio::test_s3_storage(test_name).await;
    let s3_config = S3StorageConfig {
        bucket: crate::remote_store::test_helpers::minio::create_test_bucket(test_name).await,
        prefix: String::new(),
        endpoint_url: Some(crate::remote_store::test_helpers::minio::minio_endpoint().to_string()),
        region: None,
    };
    let seg_store = RemoteSegmentStore::new(storage.sub(""));
    let man_store = RemoteManifestStore::new(storage.sub(""));
    (seg_store, man_store, s3_config, storage)
}
