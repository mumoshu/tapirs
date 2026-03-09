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
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
}
