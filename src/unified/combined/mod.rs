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

pub(crate) mod record_handle;
pub(crate) mod tapir_handle;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memtable::CompositeKey;
use crate::occ::TransactionId;
use crate::tapir::{Key, ShardNumber, Timestamp, Value, CO, CR, IO};
use crate::tapirstore::{MinPrepareTimes, RecordDeltaDuringView};
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
    Committed(OpId),
    Aborted(Timestamp),
}

/// Shared inner state for both CombinedRecordHandle and CombinedTapirHandle.
///
/// Embeds PersistentIrRecordStore (2 VlogLsms) and adds 3 TAPIR VlogLsms.
/// The prepared VlogLsm stores lightweight PreparedRef and the committed
/// VlogLsm stores TxnLogRef instead of full transactions — resolving the
/// data duplication.
pub(crate) struct CombinedStoreInner<K: Ord, V, DIO: DiskIo> {
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
}

impl<K: Key, V: Value, DIO: DiskIo> CombinedStoreInner<K, V, DIO> {
    /// Open a new CombinedStore, creating VlogLsm segments on disk.
    pub(crate) fn open(
        base_dir: &Path,
        io_flags: OpenFlags,
        shard: ShardNumber,
        linearizable: bool,
    ) -> Result<Self, StorageError> {
        DIO::create_dir_all(base_dir)?;

        let ir = PersistentIrRecordStore::open(base_dir, io_flags)?;

        let tapir_manifest = match UnifiedManifest::load::<DIO>(base_dir) {
            Ok(Some(m)) => m,
            Ok(None) | Err(_) => UnifiedManifest::new(),
        };

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
        })
    }

    /// Wrap self in Arc<Mutex> and return the IR record handle.
    pub(crate) fn into_record_handle(
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
    pub(crate) fn seal_tapir_side(&mut self) -> Result<(), StorageError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize,
    {
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
        Ok(())
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
        use crate::tapirstore::TapirStore;

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
        use crate::tapirstore::TapirStore;

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
        use crate::tapirstore::TapirStore;

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
        assert_eq!(guard.ir.base_view(), 1);
    }
}
