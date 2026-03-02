use crate::ir::OpId;
use crate::occ::TransactionId as OccTransactionId;
use std::collections::BTreeMap;

use super::IrEntryRef;
use super::types::{IrMemEntry, IrSstEntry, IrState, UnifiedVlogPtr};

/// In-memory IR record state: overlay (current view), base (sealed views),
/// and the prepare VLog index for cross-view commits.
pub(crate) struct IrRecord<K: Ord, V> {
    /// Current view's IR entries (in-memory only).
    ///
    /// At seal time, finalized entries are serialized to the VLog and
    /// replaced by `IrSstEntry` records in `ir_base`.  The overlay is
    /// then cleared.  Typed K, V so that `Prepare` entries can be
    /// inspected without deserializing from the VLog.
    pub(super) ir_overlay: BTreeMap<OpId, IrMemEntry<K, V>>,

    /// IR base: maps OpId → IrSstEntry for the sealed IR base record.
    /// In a full implementation this would be an on-disk IR SST.
    /// For now, we use an in-memory BTreeMap as a stepping stone.
    pub(super) ir_base: BTreeMap<OpId, IrSstEntry>,

    /// Maps transaction_id → VLog pointer for sealed CO::Prepare entries.
    ///
    /// Populated at seal time.  Used by `commit_prepared` when
    /// the commit arrives after the prepare's view has been sealed (cross-view
    /// commit).  Without this index, the commit path would have to scan the
    /// VLog to find the prepare.
    pub(super) prepare_vlog_index: BTreeMap<OccTransactionId, UnifiedVlogPtr>,
}

impl<K: Ord, V> IrRecord<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            ir_overlay: BTreeMap::new(),
            ir_base: BTreeMap::new(),
            prepare_vlog_index: BTreeMap::new(),
        }
    }

    /// Iterate over all IR overlay entries.
    pub(crate) fn ir_overlay_entries(&self) -> impl Iterator<Item = (&OpId, &IrMemEntry<K, V>)> {
        self.ir_overlay.iter()
    }

    /// Insert an IR entry into the overlay.
    pub(crate) fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<K, V>) {
        self.ir_overlay.insert(op_id, entry);
    }

    /// Look up an IR entry by OpId (overlay first, then base).
    pub(crate) fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_, K, V>> {
        if let Some(mem_entry) = self.ir_overlay.get(op_id) {
            Some(IrEntryRef::Overlay(mem_entry))
        } else {
            self.ir_base.get(op_id).map(IrEntryRef::Base)
        }
    }

    /// Look up an IR base SST entry by OpId.
    pub(crate) fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        self.ir_base.get(&op_id)
    }

    /// Get a reference to the prepare VLog index.
    pub(crate) fn prepare_vlog_index(&self) -> &BTreeMap<OccTransactionId, UnifiedVlogPtr> {
        &self.prepare_vlog_index
    }
}

impl<K: Ord + Clone, V: Clone> IrRecord<K, V> {
    /// Extract only finalized entries from the overlay (for leader merge simulation).
    pub(crate) fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<K, V>)> {
        self.ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.clone()))
            .collect()
    }
}
