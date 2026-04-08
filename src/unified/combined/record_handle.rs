use crate::ir::{
    IrRecordStore, MergeInstallResult, OpId, RecordConsensusEntry as ConsensusEntry,
    RecordInconsistentEntry as InconsistentEntry, ViewInstallResult,
};
use crate::mvcc::disk::disk_io::DiskIo;
use crate::unified::ir::ir_record_store::{PersistentPayload, PersistentRecord};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use super::CombinedStoreInner;

/// Handle for the IR record side of a [`CombinedStoreInner`].
///
/// Wraps `Arc<Mutex<CombinedStoreInner>>` and implements [`IrRecordStore`]
/// by delegating every method to the embedded
/// [`PersistentIrRecordStore`](crate::unified::ir::ir_record_store::PersistentIrRecordStore).
///
/// A companion [`CombinedTapirHandle`](super::CombinedTapirHandle) (Commit 3)
/// clones the same Arc, giving both handles shared ownership of the inner.
pub struct CombinedRecordHandle<K: Ord, V, DIO: DiskIo> {
    pub(crate) inner: Arc<Mutex<CombinedStoreInner<K, V, DIO>>>,
}

impl<K: Ord, V, DIO: DiskIo> CombinedRecordHandle<K, V, DIO> {
    /// Create a TAPIR store handle sharing the same inner state.
    pub fn tapir_handle(&self) -> super::tapir_handle::CombinedTapirHandle<K, V, DIO> {
        super::tapir_handle::CombinedTapirHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K: Ord + Debug, V: Debug, DIO: DiskIo> Debug for CombinedRecordHandle<K, V, DIO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("CombinedRecordHandle")
            .field("ir", &inner.ir)
            .finish()
    }
}

impl<K, V, DIO> IrRecordStore<
    crate::tapir::IO<K, V>,
    crate::tapir::CO<K, V>,
    crate::tapir::CR,
> for CombinedRecordHandle<K, V, DIO>
where
    K: Clone + Debug + Ord + Serialize + DeserializeOwned + PartialEq + Send + Sync + 'static,
    V: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + Sync + 'static,
    DIO: DiskIo,
{
    type Record = PersistentRecord<
        crate::tapir::IO<K, V>,
        crate::tapir::CO<K, V>,
        crate::tapir::CR,
    >;
    type Payload = PersistentPayload<
        crate::tapir::IO<K, V>,
        crate::tapir::CO<K, V>,
        crate::tapir::CR,
    >;

    fn get_inconsistent_entry(
        &self,
        op_id: &OpId,
    ) -> Option<InconsistentEntry<crate::tapir::IO<K, V>>> {
        self.inner.lock().unwrap().ir.get_inconsistent_entry(op_id)
    }

    fn get_consensus_entry(
        &self,
        op_id: &OpId,
    ) -> Option<ConsensusEntry<crate::tapir::CO<K, V>, crate::tapir::CR>> {
        self.inner.lock().unwrap().ir.get_consensus_entry(op_id)
    }

    fn insert_inconsistent_entry(
        &mut self,
        op_id: OpId,
        entry: InconsistentEntry<crate::tapir::IO<K, V>>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .ir
            .insert_inconsistent_entry(op_id, entry);
    }

    fn insert_consensus_entry(
        &mut self,
        op_id: OpId,
        entry: ConsensusEntry<crate::tapir::CO<K, V>, crate::tapir::CR>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .ir
            .insert_consensus_entry(op_id, entry);
    }

    fn full_record(&self) -> Self::Record {
        self.inner.lock().unwrap().ir.full_record()
    }

    fn inconsistent_len(&self) -> usize {
        self.inner.lock().unwrap().ir.inconsistent_len()
    }

    fn consensus_len(&self) -> usize {
        self.inner.lock().unwrap().ir.consensus_len()
    }

    fn build_view_change_payload(&self, next_view: u64) -> Self::Payload {
        self.inner
            .lock()
            .unwrap()
            .ir
            .build_view_change_payload(next_view)
    }

    fn build_full_view_change_payload(&self) -> Self::Payload {
        self.inner
            .lock()
            .unwrap()
            .ir
            .build_full_view_change_payload()
    }

    fn build_start_view_payload(&self, delta: Option<&Self::Payload>) -> Self::Payload {
        self.inner
            .lock()
            .unwrap()
            .ir
            .build_start_view_payload(delta)
    }

    fn make_full_payload(record: Self::Record) -> Self::Payload {
        // Static method — delegates directly, no lock needed.
        crate::unified::ir::ir_record_store::PersistentIrRecordStore::<
            crate::tapir::IO<K, V>,
            crate::tapir::CO<K, V>,
            crate::tapir::CR,
            DIO,
        >::make_full_payload_static(record)
    }

    fn install_start_view_payload(
        &mut self,
        payload: Self::Payload,
        new_view: u64,
    ) -> Option<ViewInstallResult<Self::Record>> {
        self.inner
            .lock()
            .unwrap()
            .ir
            .install_start_view_payload(payload, new_view)
    }

    fn install_merged_record(
        &mut self,
        merged: Self::Record,
        new_view: u64,
        best_payload: Option<&Self::Payload>,
        resolved_ops: &BTreeSet<OpId>,
    ) -> MergeInstallResult<Self::Record, Self::Payload> {
        self.inner
            .lock()
            .unwrap()
            .ir
            .install_merged_record(merged, new_view, best_payload, resolved_ops)
    }

    fn resolve_do_view_change_payload(&self, payload: &Self::Payload) -> Self::Record {
        self.inner
            .lock()
            .unwrap()
            .ir
            .resolve_do_view_change_payload(payload)
    }

    fn checkpoint_record(&self) -> Option<Self::Record> {
        self.inner.lock().unwrap().ir.checkpoint_record()
    }

    fn flush(&mut self) {
        // Delegates to PersistentIrRecordStore::flush which seals inc_lsm + con_lsm.
        self.inner.lock().unwrap().ir.flush();
    }

    fn stored_bytes(&self) -> Option<u64> {
        self.inner.lock().unwrap().ir.stored_bytes()
    }
}
