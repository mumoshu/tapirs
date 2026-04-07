//! Combined IR+TAPIR store with deduplicated transaction storage.
//!
//! # Design Goal
//!
//! Let TAPIR/IR handle data larger than main memory. All retained state uses
//! VlogLsm — never raw BTreeMap/BTreeSet.
//!
//! # Problem
//!
//! Transaction data is stored twice: once in the IR record (CO::Prepare and
//! IO::Commit carry the full transaction) and again in the TapirStore's
//! prepared/committed VlogLsms.
//!
//! # Solution
//!
//! CombinedStore implements both IrRecordStore and TapirStore where TAPIR
//! VlogLsms store lightweight references (OpId) back to IR entries instead
//! of duplicating transaction data.
//!
//! ```text
//! ir::Replica
//! ├── record: CombinedRecordHandle ──┐
//! │                                   ├── Arc<Mutex<CombinedStoreInner>>
//! └── upcalls: tapir::Replica         │   ├── ir: PersistentIrRecordStore (embedded, has inc_lsm + con_lsm)
//!     └── store: CombinedTapirHandle ─┘   ├── mvcc       VlogLsm (MVCC index)
//!                                         ├── committed  VlogLsm (TxnLogRef: op_id → inc_lsm)
//!                                         ├── prepared   VlogLsm (PreparedRef: op_id → con_lsm)
//!                                         └── runtime    occ_cache, min_prepare_times, etc. (rebuilt on recovery)
//! ```
//!
//! Each side seals independently; crash gaps recovered by replaying IR ops to
//! rebuild TAPIR state.

pub mod record_handle;
pub mod tapir_handle;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memtable::CompositeKey;
use crate::occ::TransactionId;
use crate::tapir::{Key, ShardNumber, Timestamp, Value, CO, CR, IO};
use crate::tapir::store::{MinPrepareTimes, RecordDeltaDuringView};
use crate::unified::ir::ir_record_store::PersistentIrRecordStore;
use crate::unified::tapir::occ_cache::OccCache;
use crate::unified::tapir::storage_types::MvccIndexEntry;
use crate::unified::wisckeylsm::lsm::{IndexMode, VlogLsm};
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::vlog::VlogSegment;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Entry type markers for combined store VlogLsm seals.
const COMB_COMMITTED_ENTRY_TYPE: u8 = 0x90;
const COMB_PREPARED_ENTRY_TYPE: u8 = 0x91;
const COMB_MVCC_ENTRY_TYPE: u8 = 0x92;

/// Lightweight reference to a prepared transaction in con_lsm.
///
/// Stored in the prepared VlogLsm instead of the full cross-shard transaction.
/// The op_id points to a ConsensusEntry in con_lsm where the full
/// CO::Prepare { transaction, .. } can be resolved lazily.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreparedRef {
    pub(crate) commit_ts: Timestamp,
    pub(crate) op_id: OpId,
    pub(crate) finalized: bool,
}

/// Lightweight reference to a committed/aborted transaction in inc_lsm.
///
/// Stored in the committed VlogLsm instead of the full transaction data.
/// Committed variant's op_id points to an InconsistentEntry in inc_lsm where
/// the full IO::Commit { transaction, .. } can be resolved lazily.
/// Aborted variant stores timestamp directly (no IR reference needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TxnLogRef {
    Committed { op_id: OpId, commit: Timestamp },
    Aborted(Timestamp),
}

/// Shared inner state for both CombinedRecordHandle and CombinedTapirHandle.
///
/// Embeds PersistentIrRecordStore (2 VlogLsms) and adds 3 TAPIR VlogLsms.
/// The prepared VlogLsm stores lightweight PreparedRef and the committed
/// VlogLsm stores TxnLogRef instead of full transactions — resolving the
/// data duplication.
pub struct CombinedStoreInner<K: Ord, V, DIO: DiskIo> {
    // --- IR (embedded, owns inc_lsm + con_lsm internally) ---
    pub(crate) ir: PersistentIrRecordStore<IO<K, V>, CO<K, V>, CR, DIO>,

    // --- TAPIR VlogLsms ---
    pub(crate) mvcc: VlogLsm<CompositeKey<K, Timestamp>, MvccIndexEntry, DIO>,
    pub(crate) committed: VlogLsm<TransactionId, TxnLogRef, DIO>,
    pub(crate) prepared: VlogLsm<TransactionId, Option<PreparedRef>, DIO>,

    // --- Runtime (rebuilt on recovery) ---
    pub(crate) occ_cache: OccCache<K>,
    pub(crate) min_prepare_times: MinPrepareTimes,
    pub(crate) record_delta_during_view: RecordDeltaDuringView<K, V>,
    pub(crate) txn_log_count: usize,

    // --- Config ---
    pub(crate) shard: ShardNumber,
    pub(crate) linearizable: bool,
    pub(crate) tapir_manifest: UnifiedManifest,
    pub(crate) base_dir: PathBuf,
    pub(crate) tapir_view: u64,

    // --- S3 remote storage (set via set_s3_config before into_record_handle) ---
    pub(crate) s3_config: Option<crate::remote_store::config::S3StorageConfig>,

    // --- Cross-shard consistency (set after restoring from snapshot) ---
    pub(crate) ghost_filter: Option<crate::remote_store::ghost_filter::GhostFilter>,
}

impl<K: Key, V: Value, DIO: DiskIo> CombinedStoreInner<K, V, DIO> {
    /// Open a CombinedStore. If a manifest exists on disk, restore sealed
    /// segments from it (reopen path); otherwise create fresh VlogLsm segments.
    pub fn open(
        base_dir: &Path,
        io_flags: OpenFlags,
        shard: ShardNumber,
        linearizable: bool,
    ) -> Result<Self, StorageError> {
        DIO::create_dir_all(base_dir)?;

        let tapir_manifest = match UnifiedManifest::load::<DIO>(base_dir) {
            Ok(Some(m)) => m,
            Ok(None) | Err(_) => UnifiedManifest::new(),
        };

        if tapir_manifest.current_view > 0 {
            // Reopen from existing manifest — restore sealed segments.
            let ir =
                PersistentIrRecordStore::open_from_manifest(base_dir, io_flags, &tapir_manifest)?;
            let committed = VlogLsm::open_from_manifest(
                "comb_comm",
                base_dir,
                &tapir_manifest.committed,
                tapir_manifest.current_view,
                io_flags,
                IndexMode::SstOnly,
            )?;
            let prepared = VlogLsm::open_from_manifest(
                "comb_prep",
                base_dir,
                &tapir_manifest.prepared,
                tapir_manifest.current_view,
                io_flags,
                IndexMode::InMemory,
            )?;
            let mvcc = VlogLsm::open_from_manifest(
                "comb_mvcc",
                base_dir,
                &tapir_manifest.mvcc,
                tapir_manifest.current_view,
                io_flags,
                IndexMode::InMemory,
            )?;
            let txn_log_count = tapir_manifest.txn_log_count as usize;
            let tapir_view = tapir_manifest.current_view;
            return Ok(Self {
                ir,
                mvcc,
                committed,
                prepared,
                occ_cache: OccCache::new(linearizable, None),
                min_prepare_times: MinPrepareTimes::default(),
                record_delta_during_view: RecordDeltaDuringView::default(),
                txn_log_count,
                shard,
                linearizable,
                tapir_manifest,
                base_dir: base_dir.to_path_buf(),
                tapir_view,
                s3_config: None,
                ghost_filter: None,
            });
        }

        // Fresh open — no manifest on disk.
        let ir = PersistentIrRecordStore::open(base_dir, io_flags)?;

        let committed_active = VlogSegment::<DIO>::open(
            0,
            base_dir.join("comb_comm_vlog_0000.dat"),
            io_flags,
        )?;
        let committed = VlogLsm::open_from_parts(
            "comb_comm",
            base_dir,
            committed_active,
            std::collections::BTreeMap::new(),
            io_flags,
            1,
            Vec::new(),
            0,
            IndexMode::SstOnly,
        )?;

        let prepared_active = VlogSegment::<DIO>::open(
            0,
            base_dir.join("comb_prep_vlog_0000.dat"),
            io_flags,
        )?;
        let prepared = VlogLsm::open_from_parts(
            "comb_prep",
            base_dir,
            prepared_active,
            std::collections::BTreeMap::new(),
            io_flags,
            1,
            Vec::new(),
            0,
            IndexMode::InMemory,
        )?;

        let mvcc_active = VlogSegment::<DIO>::open(
            0,
            base_dir.join("comb_mvcc_vlog_0000.dat"),
            io_flags,
        )?;
        let mvcc = VlogLsm::open_from_parts(
            "comb_mvcc",
            base_dir,
            mvcc_active,
            std::collections::BTreeMap::new(),
            io_flags,
            1,
            Vec::new(),
            0,
            IndexMode::InMemory,
        )?;

        Ok(Self {
            ir,
            mvcc,
            committed,
            prepared,
            occ_cache: OccCache::new(linearizable, None),
            min_prepare_times: MinPrepareTimes::default(),
            record_delta_during_view: RecordDeltaDuringView::default(),
            txn_log_count: 0,
            shard,
            linearizable,
            tapir_manifest,
            base_dir: base_dir.to_path_buf(),
            tapir_view: 0,
            s3_config: None,
            ghost_filter: None,
        })
    }

    /// Set S3 config for automatic upload on flush.
    pub fn set_s3_config(&mut self, config: crate::remote_store::config::S3StorageConfig) {
        self.s3_config = Some(config);
    }

    /// Wrap self in Arc<Mutex> and return the IR record handle.
    pub fn into_record_handle(
        self,
    ) -> record_handle::CombinedRecordHandle<K, V, DIO> {
        record_handle::CombinedRecordHandle {
            inner: Arc::new(Mutex::new(self)),
        }
    }

    /// Seal the three TAPIR VlogLsms (committed, prepared, mvcc) and save
    /// the unified manifest with both IR and TAPIR metadata.
    ///
    /// Called by CombinedTapirHandle::flush(). IrReplica always calls
    /// record.flush() (IR seal) before upcalls.flush() (this method),
    /// so the IR manifest fields are up-to-date when we copy them.
    /// Seal all TAPIR VlogLsms, save manifest, and return the list of
    /// newly-sealed segment/SST filenames (for remote upload).
    pub(crate) fn seal_tapir_side(&mut self) -> Result<Vec<String>, StorageError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize,
    {
        let manifest_before = self.tapir_manifest.clone();

        let sealed_comm =
            self.committed
                .seal_view(0, |txn_id, _| {
                    Some((
                        COMB_COMMITTED_ENTRY_TYPE,
                        txn_id.client_id.0,
                        txn_id.number,
                    ))
                })?;
        if let Some(meta) = sealed_comm {
            self.tapir_manifest.committed.sealed_vlog_segments.push(meta);
        }

        let sealed_prep =
            self.prepared
                .seal_view(0, |txn_id, _| {
                    Some((
                        COMB_PREPARED_ENTRY_TYPE,
                        txn_id.client_id.0,
                        txn_id.number,
                    ))
                })?;
        if let Some(meta) = sealed_prep {
            self.tapir_manifest.prepared.sealed_vlog_segments.push(meta);
        }

        let sealed_mvcc =
            self.mvcc
                .seal_view(0, |_ck, _| {
                    Some((COMB_MVCC_ENTRY_TYPE, 0, 0))
                })?;
        if let Some(meta) = sealed_mvcc {
            self.tapir_manifest.mvcc.sealed_vlog_segments.push(meta);
        }

        // Update TAPIR VlogLsm metadata in manifest.
        self.tapir_manifest.committed.active_segment_id = self.committed.active_vlog_id();
        self.tapir_manifest.committed.active_write_offset = self.committed.active_write_offset();
        self.tapir_manifest.committed.next_segment_id = self.committed.next_segment_id();
        self.tapir_manifest.committed.sst_metas = self.committed.sst_metas().to_vec();
        self.tapir_manifest.committed.next_sst_id = self.committed.next_sst_id();
        self.tapir_manifest.prepared.active_segment_id = self.prepared.active_vlog_id();
        self.tapir_manifest.prepared.active_write_offset = self.prepared.active_write_offset();
        self.tapir_manifest.prepared.next_segment_id = self.prepared.next_segment_id();
        self.tapir_manifest.prepared.sst_metas = self.prepared.sst_metas().to_vec();
        self.tapir_manifest.prepared.next_sst_id = self.prepared.next_sst_id();
        self.tapir_manifest.mvcc.active_segment_id = self.mvcc.active_vlog_id();
        self.tapir_manifest.mvcc.active_write_offset = self.mvcc.active_write_offset();
        self.tapir_manifest.mvcc.next_segment_id = self.mvcc.next_segment_id();
        self.tapir_manifest.mvcc.sst_metas = self.mvcc.sst_metas().to_vec();
        self.tapir_manifest.mvcc.next_sst_id = self.mvcc.next_sst_id();
        self.tapir_manifest.max_read_time = self.occ_cache.max_read_time().map(|ts| ts.time);
        self.tapir_manifest.txn_log_count = self.txn_log_count as u64;
        self.tapir_view += 1;
        self.tapir_manifest.current_view = self.tapir_view;

        // Copy IR manifest fields so the unified manifest is complete.
        let ir_m = self.ir.manifest();
        self.tapir_manifest.ir_inc = ir_m.ir_inc.clone();
        self.tapir_manifest.ir_con = ir_m.ir_con.clone();

        self.tapir_manifest.save::<DIO>(&self.base_dir)?;
        self.committed.start_view(self.tapir_view);
        self.prepared.start_view(self.tapir_view);
        self.mvcc.start_view(self.tapir_view);

        let new_files = crate::remote_store::upload::diff_manifests(
            &manifest_before,
            &self.tapir_manifest,
        );
        tracing::debug!(
            shard = self.shard.0,
            view = self.tapir_view,
            new_files = ?new_files,
            ir_inc_offset = self.tapir_manifest.ir_inc.active_write_offset,
            "seal completed"
        );

        // Fire-and-forget S3 upload if configured.
        if let Some(ref s3_cfg) = self.s3_config {
            use crate::backup::storage::BackupStorage as _;
            let base_dir = self.base_dir.clone();
            let cfg = s3_cfg.clone();
            let shard = format!("shard_{}", self.shard.0);
            let before = manifest_before;
            let after = self.tapir_manifest.clone();
            tokio::task::spawn(async move {
                let storage = crate::backup::s3backup::S3BackupStorage::new(
                    &cfg.bucket,
                    &cfg.prefix,
                    cfg.region.as_deref(),
                    cfg.endpoint_url.as_deref(),
                )
                .await;
                let seg_store =
                    crate::remote_store::segment_store::RemoteSegmentStore::new(storage.sub(""));
                let man_store =
                    crate::remote_store::manifest_store::RemoteManifestStore::new(storage.sub(""));
                crate::remote_store::sync_to_remote::sync_to_remote(
                    &seg_store, &man_store, &shard, &base_dir, &before, &after,
                )
                .await;
            });
        }

        Ok(new_files)
    }

    /// Total bytes stored across all three TAPIR VlogLsms (sealed + active).
    pub(crate) fn tapir_stored_bytes(&self) -> u64 {
        let committed_sealed: u64 = self
            .tapir_manifest
            .committed
            .sealed_vlog_segments
            .iter()
            .map(|seg| seg.total_size)
            .sum();
        let prepared_sealed: u64 = self
            .tapir_manifest
            .prepared
            .sealed_vlog_segments
            .iter()
            .map(|seg| seg.total_size)
            .sum();
        let mvcc_sealed: u64 = self
            .tapir_manifest
            .mvcc
            .sealed_vlog_segments
            .iter()
            .map(|seg| seg.total_size)
            .sum();
        committed_sealed
            + prepared_sealed
            + mvcc_sealed
            + self.committed.active_write_offset()
            + self.prepared.active_write_offset()
            + self.mvcc.active_write_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::IrRecordStore;
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::mvcc::disk::memory_io::MemoryIo;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    #[test]
    fn combined_record_handle_delegates_ir_operations() {
        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let handle = inner.into_record_handle();

        // Verify all fields are accessible through the shared inner.
        let guard = handle.inner.lock().unwrap();
        assert_eq!(guard.ir.base_view(), 0);
        assert_eq!(guard.txn_log_count, 0);
        assert_eq!(guard.shard, ShardNumber(0));
        assert!(guard.linearizable);
        assert_eq!(guard.tapir_view, 0);
        assert_eq!(guard.base_dir, base_dir);

        // VlogLsm memtables start empty.
        assert_eq!(guard.mvcc.memtable_len(), 0);
        assert_eq!(guard.committed.memtable_len(), 0);
        assert_eq!(guard.prepared.memtable_len(), 0);

        // Runtime caches start empty.
        assert_eq!(guard.min_prepare_times.min_prepare_time(), 0);
        assert!(guard.record_delta_during_view.cdc_max_view().is_none());
        assert!(guard.occ_cache.max_read_time().is_none());
        // Manifest starts fresh.
        assert_eq!(guard.tapir_manifest.current_view, 0);
        drop(guard);

        // IrRecordStore delegation works through the handle.
        assert_eq!(handle.inconsistent_len(), 0);
        assert_eq!(handle.consensus_len(), 0);
        assert!(handle.stored_bytes().is_some());
    }

    #[test]
    fn combined_tapir_handle_basic_operations() {
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let record_handle = inner.into_record_handle();
        let tapir_handle = record_handle.tapir_handle();

        // Basic TapirStore queries on empty store.
        assert_eq!(tapir_handle.shard(), ShardNumber(0));
        assert_eq!(tapir_handle.prepared_count(), 0);
        assert_eq!(tapir_handle.txn_log_len(), 0);
        assert!(tapir_handle.get_oldest_prepared_txn().is_none());
        assert!(tapir_handle.min_prepare_baseline().is_none());
        assert!(tapir_handle.cdc_max_view().is_none());
        assert!(tapir_handle.stored_bytes().is_some());
    }

    #[test]
    fn combined_seal_updates_manifest_and_stored_bytes() {
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let record_handle = inner.into_record_handle();
        let mut tapir_handle = record_handle.tapir_handle();

        // Insert a txn log entry to have data to seal.
        tapir_handle.txn_log_insert(
            TransactionId {
                client_id: crate::IrClientId(1),
                number: 1,
            },
            Timestamp::default(),
            false,
        );
        assert_eq!(tapir_handle.txn_log_len(), 1);

        // Before seal, memtable data isn't in vlog yet (put is memtable-only).
        let bytes_before = tapir_handle.stored_bytes().unwrap();

        // Seal TAPIR side — flushes memtable to vlog.
        tapir_handle.flush();

        let bytes_after = tapir_handle.stored_bytes().unwrap();
        assert!(
            bytes_after > bytes_before,
            "stored_bytes should increase after seal: {bytes_before} -> {bytes_after}"
        );

        // Manifest updated.
        let guard = record_handle.inner.lock().unwrap();
        assert_eq!(guard.tapir_view, 1);
        assert_eq!(guard.tapir_manifest.current_view, 1);
        assert_eq!(guard.tapir_manifest.txn_log_count, 1);
    }

    #[test]
    fn combined_ir_and_tapir_seal_independently() {
        use crate::ir::IrRecordStore;
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let mut record_handle = inner.into_record_handle();
        let mut tapir_handle = record_handle.tapir_handle();

        // Insert TAPIR data.
        tapir_handle.txn_log_insert(
            TransactionId {
                client_id: crate::IrClientId(2),
                number: 2,
            },
            Timestamp::default(),
            false,
        );

        // Seal IR side first (as IrReplica would do).
        // IR memtables are empty, but seal still advances the view.
        record_handle.flush();

        // Seal TAPIR side.
        tapir_handle.flush();

        // Verify TAPIR sealed.
        let tapir_bytes = tapir_handle.stored_bytes().unwrap();
        assert!(tapir_bytes > 0, "TAPIR should have sealed data");

        // Both views advanced independently.
        let guard = record_handle.inner.lock().unwrap();
        assert_eq!(guard.tapir_view, 1);
        // flush() seals at the current base_view without advancing it.
        assert_eq!(guard.ir.base_view(), 0);
    }

    #[test]
    fn combined_prepare_commit_ref_resolution() {
        use crate::ir::{RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry};
        use crate::occ::{Transaction, TransactionId};
        use crate::tapir::{Timestamp, CO, CR, IO};
        use crate::tapir::store::TapirStore;
        use crate::IrClientId;
        use std::sync::Arc;

        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let mut record_handle = inner.into_record_handle();
        let mut tapir_handle = record_handle.tapir_handle();

        let txn_id = TransactionId {
            client_id: IrClientId(1),
            number: 1,
        };
        let commit_ts = Timestamp {
            time: 5,
            client_id: IrClientId(1),
        };
        let prepare_op_id = crate::ir::OpId {
            client_id: IrClientId(1),
            number: 1,
        };
        let commit_op_id = crate::ir::OpId {
            client_id: IrClientId(1),
            number: 2,
        };

        // Build a transaction with a write to shard 0.
        let mut txn = Transaction::<String, String, Timestamp>::default();
        txn.add_write(
            crate::tapir::Sharded {
                shard: ShardNumber(0),
                key: "key1".to_string(),
            },
            Some("val1".to_string()),
        );
        let shared_txn = Arc::new(txn);

        // Insert IR entries (simulating what ir::Replica would do).
        // CO::Prepare into con_lsm.
        record_handle.insert_consensus_entry(
            prepare_op_id,
            RecordConsensusEntry {
                op: CO::Prepare {
                    transaction_id: txn_id,
                    transaction: shared_txn.clone(),
                    commit: commit_ts,
                },
                result: CR::Prepare(crate::occ::PrepareResult::Ok),
                state: RecordEntryState::Finalized(crate::ir::ViewNumber(0)),
                modified_view: 0,
            },
        );
        // IO::Commit into inc_lsm.
        record_handle.insert_inconsistent_entry(
            commit_op_id,
            RecordInconsistentEntry {
                op: IO::Commit {
                    transaction_id: txn_id,
                    transaction: shared_txn.clone(),
                    commit: commit_ts,
                },
                state: RecordEntryState::Finalized(crate::ir::ViewNumber(0)),
                modified_view: 0,
            },
        );

        // Prepare through TapirStore (registers OCC, stores PreparedRef).
        tapir_handle.add_or_replace_or_finalize_prepared_txn(
            prepare_op_id,
            txn_id,
            shared_txn.clone(),
            commit_ts,
            true,
        );

        // Verify PreparedRef exists and resolves.
        assert!(tapir_handle.get_prepared_txn(&txn_id).is_some());

        // Commit through TapirStore (stores TxnLogRef, inserts MVCC, removes prepared).
        tapir_handle.commit_txn(commit_op_id, txn_id, &shared_txn, commit_ts);

        // Verify committed ref resolves to correct value via inc_lsm.
        {
            let guard = record_handle.inner.lock().unwrap();
            let txn_log_ref = guard.committed.get(&txn_id).unwrap();
            assert!(
                matches!(txn_log_ref, Some(TxnLogRef::Committed { op_id, .. }) if op_id == commit_op_id),
                "Expected TxnLogRef::Committed with correct op_id"
            );

            // PreparedRef cleaned up after commit.
            let prep = guard.prepared.get(&txn_id).unwrap();
            assert!(
                matches!(prep, Some(None)),
                "Prepared entry should be None after commit"
            );
        }

        // MVCC entry readable through TapirStore trait.
        let (value, ts) = tapir_handle
            .do_uncommitted_get_at(&"key1".to_string(), commit_ts)
            .unwrap();
        assert_eq!(value.as_deref(), Some("val1"));
        assert_eq!(ts, commit_ts);
    }

    #[test]
    fn combined_multi_seal_accumulates_manifest() {
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let mut record_handle = inner.into_record_handle();
        let mut tapir_handle = record_handle.tapir_handle();

        // Seal 1: insert data, seal IR + TAPIR.
        tapir_handle.txn_log_insert(
            TransactionId {
                client_id: crate::IrClientId(1),
                number: 1,
            },
            Timestamp::default(),
            false,
        );
        record_handle.flush();
        tapir_handle.flush();

        let bytes_after_seal1 = tapir_handle.stored_bytes().unwrap();
        assert!(bytes_after_seal1 > 0, "stored_bytes should be > 0 after first seal");
        {
            let guard = record_handle.inner.lock().unwrap();
            assert_eq!(guard.tapir_view, 1);
            assert_eq!(guard.tapir_manifest.current_view, 1);
        }

        // Seal 2: insert more data, seal again.
        tapir_handle.txn_log_insert(
            TransactionId {
                client_id: crate::IrClientId(2),
                number: 1,
            },
            Timestamp::default(),
            true,
        );
        record_handle.flush();
        tapir_handle.flush();

        let bytes_after_seal2 = tapir_handle.stored_bytes().unwrap();
        assert!(
            bytes_after_seal2 > bytes_after_seal1,
            "stored_bytes should grow: {bytes_after_seal1} -> {bytes_after_seal2}"
        );
        {
            let guard = record_handle.inner.lock().unwrap();
            assert_eq!(guard.tapir_view, 2);
            assert_eq!(guard.tapir_manifest.current_view, 2);
            // Each seal produces one sealed segment for committed VlogLsm.
            assert_eq!(
                guard.tapir_manifest.committed.sealed_vlog_segments.len(),
                2,
                "Should have 2 sealed committed segments after 2 seals"
            );
        }
    }

    /// Helper: prepare + commit a transaction through both IR and TAPIR sides.
    fn test_prepare_and_commit(
        record_handle: &mut super::record_handle::CombinedRecordHandle<String, String, MemoryIo>,
        tapir_handle: &mut super::tapir_handle::CombinedTapirHandle<String, String, MemoryIo>,
        prepare_op_id: crate::ir::OpId,
        commit_op_id: crate::ir::OpId,
        txn_id: crate::occ::TransactionId,
        writes: Vec<(&str, &str)>,
        commit_ts: crate::tapir::Timestamp,
    ) {
        use crate::ir::{RecordConsensusEntry, RecordEntryState, RecordInconsistentEntry};
        use crate::occ::Transaction;
        use crate::tapir::{CO, CR, IO};
        use crate::tapir::store::TapirStore;
        use std::sync::Arc;

        let mut txn = Transaction::<String, String, crate::tapir::Timestamp>::default();
        for (key, value) in &writes {
            txn.add_write(
                crate::tapir::Sharded {
                    shard: ShardNumber(0),
                    key: key.to_string(),
                },
                Some(value.to_string()),
            );
        }
        let shared_txn = Arc::new(txn);

        // Insert IR entries.
        record_handle.insert_consensus_entry(
            prepare_op_id,
            RecordConsensusEntry {
                op: CO::Prepare {
                    transaction_id: txn_id,
                    transaction: shared_txn.clone(),
                    commit: commit_ts,
                },
                result: CR::Prepare(crate::occ::PrepareResult::Ok),
                state: RecordEntryState::Finalized(crate::ir::ViewNumber(0)),
                modified_view: 0,
            },
        );
        record_handle.insert_inconsistent_entry(
            commit_op_id,
            RecordInconsistentEntry {
                op: IO::Commit {
                    transaction_id: txn_id,
                    transaction: shared_txn.clone(),
                    commit: commit_ts,
                },
                state: RecordEntryState::Finalized(crate::ir::ViewNumber(0)),
                modified_view: 0,
            },
        );

        // Prepare + commit through TapirStore.
        tapir_handle.add_or_replace_or_finalize_prepared_txn(
            prepare_op_id,
            txn_id,
            shared_txn.clone(),
            commit_ts,
            true,
        );
        tapir_handle.commit_txn(commit_op_id, txn_id, &shared_txn, commit_ts);
    }

    #[test]
    fn combined_reopen_preserves_committed_data() {
        use crate::ir::IrRecordStore;
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();

        let bytes_before_drop;
        // Phase 1: Create store, commit data, seal.
        {
            let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
                &base_dir,
                test_flags(),
                ShardNumber(0),
                true,
            )
            .unwrap();
            let mut record_handle = inner.into_record_handle();
            let mut tapir_handle = record_handle.tapir_handle();

            let txn_id = TransactionId {
                client_id: crate::IrClientId(1),
                number: 1,
            };
            let commit_ts = Timestamp {
                time: 5,
                client_id: crate::IrClientId(1),
            };
            test_prepare_and_commit(
                &mut record_handle,
                &mut tapir_handle,
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 1,
                },
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 2,
                },
                txn_id,
                vec![("x", "v1"), ("y", "v2")],
                commit_ts,
            );

            // Verify readable before seal.
            let (val, ts) = tapir_handle
                .do_uncommitted_get_at(&"x".to_string(), commit_ts)
                .unwrap();
            assert_eq!(val.as_deref(), Some("v1"));
            assert_eq!(ts, commit_ts);

            // Seal IR + TAPIR.
            record_handle.flush();
            tapir_handle.flush();

            bytes_before_drop = tapir_handle.stored_bytes().unwrap();
            assert!(bytes_before_drop > 0);
        }
        // Store dropped — simulates crash/shutdown.

        // Phase 2: Reopen from the same path.
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let record_handle = inner.into_record_handle();
        let tapir_handle = record_handle.tapir_handle();

        // Manifest restored.
        {
            let guard = record_handle.inner.lock().unwrap();
            assert_eq!(guard.tapir_view, 1);
            assert_eq!(guard.tapir_manifest.current_view, 1);
            assert_eq!(guard.txn_log_count, 1);
        }

        // Committed data still readable through ref resolution.
        let commit_ts = Timestamp {
            time: 5,
            client_id: crate::IrClientId(1),
        };
        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"x".to_string(), commit_ts)
            .unwrap();
        assert_eq!(val.as_deref(), Some("v1"));
        assert_eq!(ts, commit_ts);

        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"y".to_string(), commit_ts)
            .unwrap();
        assert_eq!(val.as_deref(), Some("v2"));
        assert_eq!(ts, commit_ts);

        // stored_bytes should be at least what we had before drop.
        let bytes_after_reopen = tapir_handle.stored_bytes().unwrap();
        assert!(
            bytes_after_reopen >= bytes_before_drop,
            "stored_bytes after reopen ({bytes_after_reopen}) < before drop ({bytes_before_drop})"
        );
    }

    #[test]
    fn combined_reopen_multi_view() {
        use crate::ir::IrRecordStore;
        use crate::tapir::store::TapirStore;

        let base_dir = MemoryIo::temp_path();

        // Phase 1: Build state across multiple views.
        {
            let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
                &base_dir,
                test_flags(),
                ShardNumber(0),
                true,
            )
            .unwrap();
            let mut record_handle = inner.into_record_handle();
            let mut tapir_handle = record_handle.tapir_handle();

            // View 0: Commit txn1 and txn2.
            test_prepare_and_commit(
                &mut record_handle,
                &mut tapir_handle,
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 1,
                },
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 2,
                },
                TransactionId {
                    client_id: crate::IrClientId(1),
                    number: 1,
                },
                vec![("a", "val_a"), ("b", "val_b")],
                Timestamp {
                    time: 5,
                    client_id: crate::IrClientId(1),
                },
            );
            test_prepare_and_commit(
                &mut record_handle,
                &mut tapir_handle,
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 3,
                },
                crate::ir::OpId {
                    client_id: crate::IrClientId(1),
                    number: 4,
                },
                TransactionId {
                    client_id: crate::IrClientId(1),
                    number: 2,
                },
                vec![("c", "val_c")],
                Timestamp {
                    time: 10,
                    client_id: crate::IrClientId(1),
                },
            );

            // Seal → view 1.
            record_handle.flush();
            tapir_handle.flush();

            // View 1: Commit txn3 (overwrites "a", adds "d").
            test_prepare_and_commit(
                &mut record_handle,
                &mut tapir_handle,
                crate::ir::OpId {
                    client_id: crate::IrClientId(2),
                    number: 1,
                },
                crate::ir::OpId {
                    client_id: crate::IrClientId(2),
                    number: 2,
                },
                TransactionId {
                    client_id: crate::IrClientId(2),
                    number: 1,
                },
                vec![("d", "val_d"), ("a", "val_a_v2")],
                Timestamp {
                    time: 20,
                    client_id: crate::IrClientId(2),
                },
            );

            // Seal → view 2.
            record_handle.flush();
            tapir_handle.flush();
        }

        // Phase 2: Reopen and verify all data across both views.
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &base_dir,
            test_flags(),
            ShardNumber(0),
            true,
        )
        .unwrap();
        let record_handle = inner.into_record_handle();
        let tapir_handle = record_handle.tapir_handle();

        {
            let guard = record_handle.inner.lock().unwrap();
            assert_eq!(guard.tapir_view, 2);
            assert_eq!(guard.txn_log_count, 3);
        }

        // View 0 data (sealed in first seal).
        let ts5 = Timestamp {
            time: 5,
            client_id: crate::IrClientId(1),
        };
        let ts10 = Timestamp {
            time: 10,
            client_id: crate::IrClientId(1),
        };
        let ts20 = Timestamp {
            time: 20,
            client_id: crate::IrClientId(2),
        };

        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"a".to_string(), ts5)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_a"));
        assert_eq!(ts, ts5);

        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"b".to_string(), ts5)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_b"));
        assert_eq!(ts, ts5);

        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"c".to_string(), ts10)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_c"));
        assert_eq!(ts, ts10);

        // View 1 data (sealed in second seal).
        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"d".to_string(), ts20)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_d"));
        assert_eq!(ts, ts20);

        // "a" at ts=20 should return val_a_v2 (overwritten in view 1).
        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"a".to_string(), ts20)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_a_v2"));
        assert_eq!(ts, ts20);

        // "a" at ts=15 should return val_a (view 0 version, before overwrite).
        let ts15 = Timestamp {
            time: 15,
            client_id: crate::IrClientId(1),
        };
        let (val, ts) = tapir_handle
            .do_uncommitted_get_at(&"a".to_string(), ts15)
            .unwrap();
        assert_eq!(val.as_deref(), Some("val_a"));
        assert_eq!(ts, ts5);
    }

    mod ir_conformance {
        use crate::discovery::InMemoryShardDirectory;
        use crate::mvcc::disk::disk_io::OpenFlags;
        use crate::mvcc::disk::memory_io::MemoryIo;
        use crate::tapir;
        use crate::unified::combined::tapir_handle::CombinedTapirHandle;
        use crate::unified::combined::record_handle::CombinedRecordHandle;
        use crate::unified::combined::CombinedStoreInner;
        use crate::{ChannelRegistry, ShardNumber};
        use std::sync::Arc;

        type S = CombinedTapirHandle<String, String, MemoryIo>;
        type U = tapir::Replica<String, String, S>;
        type R = CombinedRecordHandle<String, String, MemoryIo>;

        fn combined_factory() -> (
            ChannelRegistry<U>,
            Arc<InMemoryShardDirectory<usize>>,
            impl FnMut() -> (U, R),
        ) {
            let registry = ChannelRegistry::default();
            let directory = Arc::new(InMemoryShardDirectory::new());
            let factory = || {
                let base_dir = MemoryIo::temp_path();
                let flags = OpenFlags {
                    create: true,
                    direct: false,
                };
                let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
                    &base_dir,
                    flags,
                    ShardNumber(0),
                    true,
                )
                .unwrap();
                let record_handle = inner.into_record_handle();
                let tapir_handle = record_handle.tapir_handle();
                let upcalls = tapir::Replica::new_with_store(tapir_handle);
                (upcalls, record_handle)
            };
            (registry, directory, factory)
        }

        crate::ir_replica_conformance_tests!(combined_factory);
    }

}
