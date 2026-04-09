use super::payload::IrPayload;
use super::record::{ConsensusEntry, InconsistentEntry, RecordBuilder, RecordView};
use super::{OpId, ViewNumber};
use std::fmt::Debug;

/// Result of install_start_view_payload — data the replica needs for upcalls.
#[derive(Clone)]
pub struct ViewInstallResult<R> {
    /// Full record before the install (for upcalls.sync).
    pub previous_record: R,
    /// CDC: (from_view, changes). The replica adds new_view from context.
    pub transition: (u64, R),
    /// Full record after the install — avoids caller needing a second full_record() call.
    pub new_record: R,
}

/// Result of install_merged_record.
#[derive(Clone)]
pub struct MergeInstallResult<R, P> {
    /// CDC: (from_view, changes).
    pub transition: (u64, R),
    /// Optional delta payload for wire-efficient StartView messages.
    pub start_view_delta: Option<P>,
    /// Sealed view number before this install — for StartView recipient selection.
    pub previous_base_view: Option<ViewNumber>,
}

/// Abstracts the IR record store, allowing alternative backends
/// (e.g., LSM-backed, unified store spanning IR and TAPIR).
///
/// The IR replica uses this trait for all record operations: entry
/// lookup/insert, snapshot extraction for view change messages,
/// and lifecycle management (install from view change resolution).
///
/// The default implementation is [`super::VersionedRecord`], which uses
/// in-memory BTreeMaps with base/overlay semantics.
pub trait IrRecordStore<IO, CO, CR>: Debug + Send + 'static
where
    IO: Clone,
    CO: Clone,
    CR: Clone,
{
    type Record: RecordView<IO = IO, CO = CO, CR = CR> + RecordBuilder + Debug + Default + Send + 'static;
    type Payload: IrPayload<Record = Self::Record>;

    /// Look up an inconsistent entry by OpId (owned return).
    fn get_inconsistent_entry(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>>;

    /// Look up a consensus entry by OpId (owned return).
    fn get_consensus_entry(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>>;

    /// Insert or overwrite an inconsistent entry.
    fn insert_inconsistent_entry(&mut self, op_id: OpId, entry: InconsistentEntry<IO>);

    /// Insert or overwrite a consensus entry.
    fn insert_consensus_entry(&mut self, op_id: OpId, entry: ConsensusEntry<CO, CR>);

    /// Returns current-view (memtable) entries as a record.
    ///
    /// Only reads memtable O(D), not sealed segments. Used on the leader
    /// merge path where all replicas share identical sealed segments
    /// (same latest_normal_view).
    fn memtable_record(&self) -> Self::Record;

    /// Total number of unique inconsistent entries.
    fn inconsistent_len(&self) -> usize;

    /// Total number of unique consensus entries.
    fn consensus_len(&self) -> usize;

    /// Build a DoViewChange addendum payload (memtable-only delta).
    ///
    /// DVC payloads always contain only memtable entries — sealed segments
    /// are never sent in DVC because all peers with matching LNV have
    /// identical sealed data.
    fn build_view_change_payload(&self) -> Self::Payload;

    /// Build a StartView payload. If delta is Some, clones it; else builds full.
    fn build_start_view_payload(&self, delta: Option<&Self::Payload>) -> Self::Payload;

    /// Wrap an external record as a full payload (bootstrap).
    fn make_full_payload(record: Self::Record) -> Self::Payload;

    /// Resolve a received StartView payload, validate delta base, install in place.
    /// Returns None if delta validation fails (bad base_view).
    /// Returns ViewInstallResult with previous_record (for sync) and CDC transition.
    fn install_start_view_payload(
        &mut self, payload: Self::Payload, new_view: u64,
    ) -> Option<ViewInstallResult<Self::Record>>;

    /// Install a merged record (leader path) in place.
    /// Returns MergeInstallResult with CDC data, delta payload, and previous base view.
    ///
    /// `merged` contains only memtable entries from the current view — the
    /// output of merge_record + merge(d,u). Since these entries are not in
    /// the VlogLsm sealed index, the entire merged record IS the delta and
    /// is persisted directly as a new sealed segment.
    fn install_merged_record(
        &mut self, merged: Self::Record, new_view: u64,
    ) -> MergeInstallResult<Self::Record, Self::Payload>;

    /// The highest view whose entries have been sealed to durable storage.
    fn base_view(&self) -> u64;

    /// Return a Record containing only the payload's own segment entries,
    /// without resolving against a base. For delta payloads this contains
    /// only the delta entries; for full payloads, all entries.
    fn payload_as_record(&self, payload: &Self::Payload) -> Self::Record {
        payload.as_unresolved_record()
    }

    /// Seal VlogLsm memtables to durable storage and save manifest.
    fn flush(&mut self);

    /// Total bytes across all VlogLsm segments. None for in-memory backends.
    fn stored_bytes(&self) -> Option<u64>;
}
