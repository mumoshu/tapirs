use super::payload::IrPayload;
use super::record::{ConsensusEntry, InconsistentEntry, RecordBuilder, RecordIter, RecordView};
use super::{OpId, ViewNumber};
use std::fmt::Debug;

/// Result of prepare_start_view_install — transition record + opaque data
/// for complete_start_view_install.
pub struct PreparedInstall<R, P> {
    /// CDC transition: (from_view, delta_record).
    pub(crate) transition: (u64, R),
    /// The original payload, returned for complete_start_view_install to
    /// import segments.
    pub(crate) payload: P,
    pub(crate) new_view: u64,
}

impl<R, P> PreparedInstall<R, P> {
    /// CDC transition: (from_view, delta_record).
    pub fn transition(&self) -> &(u64, R) {
        &self.transition
    }
}

/// Adapter: presents an [`IrRecordStore`] as a [`RecordView`] for lookups.
///
/// Used as the `local` argument to `Upcalls::sync` on the non-leader path,
/// where only `get_consensus`/`get_inconsistent` are called (O(log N) via
/// VlogLsm index). Iteration methods return empty — the `leader` argument
/// (a different RecordView impl) handles iteration.
pub struct RecordStoreView<'a, IO: Clone, CO: Clone, CR: Clone, R: IrRecordStore<IO, CO, CR>>(
    &'a R,
    std::marker::PhantomData<fn() -> (IO, CO, CR)>,
);

impl<'a, IO: Clone, CO: Clone, CR: Clone, R: IrRecordStore<IO, CO, CR>> RecordStoreView<'a, IO, CO, CR, R> {
    pub fn new(store: &'a R) -> Self {
        Self(store, std::marker::PhantomData)
    }
}

impl<IO, CO, CR, R> RecordIter for RecordStoreView<'_, IO, CO, CR, R>
where
    IO: Clone,
    CO: Clone,
    CR: Clone,
    R: IrRecordStore<IO, CO, CR>,
{
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<CO, CR>)> {
        std::iter::empty()
    }
    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<IO>)> {
        std::iter::empty()
    }
}

impl<IO, CO, CR, R> RecordView for RecordStoreView<'_, IO, CO, CR, R>
where
    IO: Clone,
    CO: Clone,
    CR: Clone,
    R: IrRecordStore<IO, CO, CR>,
{
    fn get_consensus(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>> {
        self.0.get_consensus_entry(op_id)
    }
    fn get_inconsistent(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>> {
        self.0.get_inconsistent_entry(op_id)
    }
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
    type RawRecord: RecordIter<IO = IO, CO = CO, CR = CR> + Send + 'static;
    type Payload: IrPayload<Record = Self::Record, RawRecord = Self::RawRecord>;

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

    /// Phase 1: partition segments and build transition record.
    ///
    /// Does NOT modify VlogLsm state — the store's `get_*` methods still
    /// reflect pre-install state, enabling O(log N) sync lookups via
    /// [`RecordStoreView`].
    fn prepare_start_view_install(
        &self, payload: Self::Payload, new_view: u64,
    ) -> Option<PreparedInstall<Self::Record, Self::Payload>>;

    /// Phase 2: import segments, clear memtable, advance base_view.
    fn complete_start_view_install(
        &mut self, prepared: PreparedInstall<Self::Record, Self::Payload>,
    );

    /// Combined prepare + complete for callers that don't need VlogLsm
    /// lookups between the two phases. Returns (from_view, transition_record).
    fn install_start_view_payload(
        &mut self, payload: Self::Payload, new_view: u64,
    ) -> Option<(u64, Self::Record)> {
        let prepared = self.prepare_start_view_install(payload, new_view)?;
        let (from_view, transition) = prepared.transition;
        self.complete_start_view_install(PreparedInstall {
            transition: (from_view, Self::Record::default()),
            payload: prepared.payload,
            new_view: prepared.new_view,
        });
        Some((from_view, transition))
    }

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

    /// Return a raw record for iteration over the payload's entries.
    /// The raw record supports iteration only (RecordIter), not point lookups.
    fn payload_as_raw_record(&self, payload: &Self::Payload) -> Self::RawRecord {
        payload.as_raw_record()
    }

    /// Seal VlogLsm memtables to durable storage and save manifest.
    fn flush(&mut self);

    /// Total bytes across all VlogLsm segments. None for in-memory backends.
    fn stored_bytes(&self) -> Option<u64>;
}
