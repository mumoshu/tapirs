use super::record::{ConsensusEntry, InconsistentEntry, RecordImpl, VersionedEntry};
use super::OpId;
use std::fmt::Debug;

/// Abstracts the IR record store, allowing alternative backends
/// (e.g., LSM-backed, unified store spanning IR and TAPIR).
///
/// The IR replica uses this trait for all record operations: entry
/// lookup/insert, snapshot extraction for view change messages,
/// and lifecycle management (install from view change resolution).
///
/// The default implementation is [`super::VersionedRecord`], which uses
/// in-memory BTreeMaps with base/overlay semantics.
pub trait IrRecordStore<IO, CO, CR>: Default + Debug + Send + 'static
where
    IO: Clone,
    CO: Clone,
    CR: Clone,
{
    /// Look up or insert an inconsistent entry by OpId.
    fn entry_inconsistent(&mut self, op_id: OpId) -> VersionedEntry<'_, InconsistentEntry<IO>>;

    /// Look up or insert a consensus entry by OpId.
    fn entry_consensus(&mut self, op_id: OpId) -> VersionedEntry<'_, ConsensusEntry<CO, CR>>;

    /// Get a mutable reference to an inconsistent entry, promoting it
    /// to the current view's writable layer if needed.
    fn get_mut_inconsistent(&mut self, op_id: &OpId) -> Option<&mut InconsistentEntry<IO>>;

    /// Get a mutable reference to a consensus entry, promoting it
    /// to the current view's writable layer if needed.
    fn get_mut_consensus(&mut self, op_id: &OpId) -> Option<&mut ConsensusEntry<CO, CR>>;

    /// Returns entries modified during the current view (since the last seal).
    fn current_view_delta(&self) -> RecordImpl<IO, CO, CR>;

    /// Returns all entries (sealed + current view) merged into a single record.
    fn full_record(&self) -> RecordImpl<IO, CO, CR>;

    /// Whether a sealed checkpoint exists (at least one view change has completed).
    fn has_sealed_view(&self) -> bool;

    /// The view number of the last sealed checkpoint.
    fn sealed_view_number(&self) -> u64;

    /// The record as of the last sealed view. Only meaningful when
    /// `has_sealed_view()` returns true.
    fn sealed_record(&self) -> &RecordImpl<IO, CO, CR>;

    /// Create from a full record after view change resolution.
    /// The record becomes the sealed checkpoint with an empty current view.
    fn install(record: RecordImpl<IO, CO, CR>, view: u64) -> Self;

    /// Total number of unique inconsistent entries.
    fn inconsistent_len(&self) -> usize;

    /// Total number of unique consensus entries.
    fn consensus_len(&self) -> usize;
}
