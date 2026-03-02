use crate::ir::OpId;
use crate::occ::TransactionId as OccTransactionId;
use std::collections::BTreeMap;

use super::types::{IrMemEntry, IrSstEntry, UnifiedVlogPtr};

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
}
