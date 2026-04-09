use crate::ir::{
    ClientId, IrPayload, IrRecordStore, MergeInstallResult, OpId,
    PreparedInstall, RecordBuilder, RecordConsensusEntry as ConsensusEntry,
    RecordInconsistentEntry as InconsistentEntry,
    RecordIter, RecordView, ViewNumber,
};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::unified::wisckeylsm::lsm::{IndexMode, VlogLsm};
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::ViewRange;
use crate::unified::wisckeylsm::vlog::{RawVlogEntry, VlogSegment, VLOG_RAW_ENTRY_OVERHEAD};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Entry type markers for the two VlogLsm instances.
const ENTRY_TYPE_INCONSISTENT: u8 = 0x10;
const ENTRY_TYPE_CONSENSUS: u8 = 0x11;

/// Reconstruct an OpId from a raw vlog entry's header fields.
fn op_id_from_raw(raw: &RawVlogEntry) -> Option<OpId> {
    Some(OpId {
        client_id: ClientId(raw.id_client),
        number: raw.id_number,
    })
}

/// Header function for seal_view / encode_memtable_as_segment.
fn inc_header_fn<IO>(op_id: &OpId, _entry: &InconsistentEntry<IO>) -> Option<(u8, u64, u64)> {
    Some((ENTRY_TYPE_INCONSISTENT, op_id.client_id.0, op_id.number))
}

fn con_header_fn<CO, CR>(
    op_id: &OpId,
    _entry: &ConsensusEntry<CO, CR>,
) -> Option<(u8, u64, u64)> {
    Some((ENTRY_TYPE_CONSENSUS, op_id.client_id.0, op_id.number))
}

// ---------------------------------------------------------------------------
// PersistentRecord
// ---------------------------------------------------------------------------

/// IR record backed by raw vlog segment bytes (Raw) or in-memory BTreeMaps (Indexed).
///
/// Raw vlog segment bytes — supports iteration only (RecordIter), no point lookups.
///
/// Use `into_indexed()` to convert to a `PersistentRecord` for O(log N) lookups.
/// Point lookups on raw data would be O(N) per call — this type prevents that misuse.
pub struct RawRecord<IO, CO, CR> {
    pub(crate) inc_segments: Vec<Vec<u8>>,
    pub(crate) con_segments: Vec<Vec<u8>>,
    _phantom: std::marker::PhantomData<fn() -> (IO, CO, CR)>,
}

impl<IO: Clone + DeserializeOwned, CO: Clone + DeserializeOwned, CR: Clone + DeserializeOwned>
    RawRecord<IO, CO, CR>
{
    /// Convert raw segment bytes to an indexed PersistentRecord with BTreeMaps.
    pub fn into_indexed(self) -> PersistentRecord<IO, CO, CR> {
        PersistentRecord {
            inconsistent: scan_entries::<InconsistentEntry<IO>>(&self.inc_segments)
                .into_iter()
                .collect(),
            consensus: scan_entries::<ConsensusEntry<CO, CR>>(&self.con_segments)
                .into_iter()
                .collect(),
        }
    }
}

/// BTreeMap-backed IR record with O(log N) lookups.
///
/// Implements RecordIter + RecordView + RecordBuilder.
pub struct PersistentRecord<IO, CO, CR> {
    pub(crate) inconsistent: BTreeMap<OpId, InconsistentEntry<IO>>,
    pub(crate) consensus: BTreeMap<OpId, ConsensusEntry<CO, CR>>,
}

impl<IO: Debug, CO: Debug, CR: Debug> Debug for PersistentRecord<IO, CO, CR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentRecord")
            .field("inconsistent", &self.inconsistent.len())
            .field("consensus", &self.consensus.len())
            .finish()
    }
}

impl<IO, CO, CR> Default for PersistentRecord<IO, CO, CR> {
    fn default() -> Self {
        Self {
            inconsistent: BTreeMap::new(),
            consensus: BTreeMap::new(),
        }
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> PersistentRecord<IO, CO, CR> {
    /// Compute the delta by OpId: entries in `self` whose OpId is NOT in `base`.
    /// Unlike equality-based delta_from, this only checks for OpId presence,
    /// making it robust against `modified_view` discrepancies across view changes.
    pub fn delta_from(&self, base: &Self) -> Self {
        Self {
            inconsistent: self.inconsistent
                .iter()
                .filter(|(id, _)| !base.inconsistent.contains_key(id))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
            consensus: self.consensus
                .iter()
                .filter(|(id, _)| !base.consensus.contains_key(id))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
        }
    }
}

/// Scan raw vlog segment bytes and yield deserialized entries.
fn scan_entries<V: DeserializeOwned>(
    segments: &[Vec<u8>],
) -> Vec<(OpId, V)> {
    let mut result = Vec::new();
    for seg_bytes in segments {
        let mut offset = 0usize;
        let end = seg_bytes.len();
        while offset + VLOG_RAW_ENTRY_OVERHEAD <= end {
            let entry_len =
                u32::from_le_bytes([
                    seg_bytes[offset + 1],
                    seg_bytes[offset + 2],
                    seg_bytes[offset + 3],
                    seg_bytes[offset + 4],
                ]) as usize;
            if entry_len < VLOG_RAW_ENTRY_OVERHEAD || offset + entry_len > end {
                break;
            }
            let id_client = u64::from_le_bytes(
                seg_bytes[offset + 5..offset + 13].try_into().unwrap(),
            );
            let id_number = u64::from_le_bytes(
                seg_bytes[offset + 13..offset + 21].try_into().unwrap(),
            );
            let payload_bytes = &seg_bytes[offset + 21..offset + entry_len - 4];
            if let Ok(value) = bitcode::deserialize::<V>(payload_bytes) {
                let op_id = OpId {
                    client_id: ClientId(id_client),
                    number: id_number,
                };
                result.push((op_id, value));
            }
            offset += entry_len;
        }
    }
    result
}

impl<IO, CO, CR> RecordIter for RawRecord<IO, CO, CR>
where
    IO: Clone + DeserializeOwned,
    CO: Clone + DeserializeOwned,
    CR: Clone + DeserializeOwned,
{
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<CO, CR>)> {
        scan_entries::<ConsensusEntry<CO, CR>>(&self.con_segments).into_iter()
    }

    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<IO>)> {
        scan_entries::<InconsistentEntry<IO>>(&self.inc_segments).into_iter()
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordIter for PersistentRecord<IO, CO, CR> {
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<CO, CR>)> {
        self.consensus.iter().map(|(k, v)| (*k, v.clone()))
    }

    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<IO>)> {
        self.inconsistent.iter().map(|(k, v)| (*k, v.clone()))
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordView for PersistentRecord<IO, CO, CR> {
    fn get_consensus(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>> {
        self.consensus.get(op_id).cloned()
    }

    fn get_inconsistent(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>> {
        self.inconsistent.get(op_id).cloned()
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordBuilder for PersistentRecord<IO, CO, CR> {
    fn insert_inconsistent(&mut self, op_id: OpId, entry: InconsistentEntry<Self::IO>) {
        self.inconsistent.insert(op_id, entry);
    }

    fn insert_consensus(&mut self, op_id: OpId, entry: ConsensusEntry<Self::CO, Self::CR>) {
        self.consensus.insert(op_id, entry);
    }
}

// ---------------------------------------------------------------------------
// PersistentPayload
// ---------------------------------------------------------------------------

/// Payload for view-change messages carrying raw vlog segment bytes.
///
/// Arc-wrapped for O(1) clone during StartView broadcast.
#[derive(Clone, Debug)]
pub struct PersistentPayload<IO, CO, CR> {
    inner: Arc<PayloadInner>,
    _phantom: std::marker::PhantomData<fn() -> (IO, CO, CR)>,
}

#[derive(Debug, Serialize, Deserialize)]
enum PayloadInner {
    Delta {
        base_view: ViewNumber,
        inc_segments: Vec<(Vec<ViewRange>, Vec<u8>)>,
        con_segments: Vec<(Vec<ViewRange>, Vec<u8>)>,
    },
}

impl<IO, CO, CR> Serialize for PersistentPayload<IO, CO, CR> {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        self.inner.serialize(serializer)
    }
}

impl<'de, IO, CO, CR> Deserialize<'de> for PersistentPayload<IO, CO, CR> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let inner = PayloadInner::deserialize(deserializer)?;
        Ok(Self {
            inner: Arc::new(inner),
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<IO, CO, CR> PersistentPayload<IO, CO, CR> {
    fn full(inc_segments: Vec<(Vec<ViewRange>, Vec<u8>)>, con_segments: Vec<(Vec<ViewRange>, Vec<u8>)>) -> Self {
        Self {
            inner: Arc::new(PayloadInner::Delta {
                base_view: ViewNumber(0),
                inc_segments,
                con_segments,
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    fn delta(
        base_view: ViewNumber,
        inc_bytes: Vec<u8>,
        con_bytes: Vec<u8>,
        views: Vec<ViewRange>,
    ) -> Self {
        let inc_segments = if inc_bytes.is_empty() { vec![] } else { vec![(views.clone(), inc_bytes)] };
        let con_segments = if con_bytes.is_empty() { vec![] } else { vec![(views, con_bytes)] };
        Self {
            inner: Arc::new(PayloadInner::Delta {
                base_view,
                inc_segments,
                con_segments,
            }),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<IO, CO, CR> IrPayload for PersistentPayload<IO, CO, CR>
where
    IO: Clone + Debug + DeserializeOwned + Send + 'static,
    CO: Clone + Debug + DeserializeOwned + Send + 'static,
    CR: Clone + Debug + DeserializeOwned + Send + 'static,
{
    type Record = PersistentRecord<IO, CO, CR>;
    type RawRecord = RawRecord<IO, CO, CR>;

    fn base_view(&self) -> Option<ViewNumber> {
        let PayloadInner::Delta { base_view, .. } = self.inner.as_ref();
        Some(*base_view)
    }

    fn as_raw_record(&self) -> Self::RawRecord {
        let PayloadInner::Delta {
            inc_segments,
            con_segments,
            ..
        } = self.inner.as_ref();
        let strip = |segs: &[(Vec<ViewRange>, Vec<u8>)]| -> Vec<Vec<u8>> {
            segs.iter().map(|(_, bytes)| bytes.clone()).collect()
        };
        RawRecord {
            inc_segments: strip(inc_segments),
            con_segments: strip(con_segments),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// PersistentIrRecordStore
// ---------------------------------------------------------------------------

/// VlogLsm-native IR record store.
///
/// Uses two VlogLsms — one for inconsistent entries, one for consensus entries.
/// The VlogLsm memtable serves as the current-view overlay; the sealed vlog+index
/// as the base. This is a drop-in alternative to `VersionedRecord` that provides
/// durable persistence via WiscKey vlog segments.
pub struct PersistentIrRecordStore<IO, CO, CR, DIO: DiskIo> {
    inc_lsm: VlogLsm<OpId, InconsistentEntry<IO>, DIO>,
    con_lsm: VlogLsm<OpId, ConsensusEntry<CO, CR>, DIO>,
    base_view: u64,
    manifest: UnifiedManifest,
    base_dir: PathBuf,
}

impl<IO: Debug, CO: Debug, CR: Debug, DIO: DiskIo> Debug
    for PersistentIrRecordStore<IO, CO, CR, DIO>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentIrRecordStore")
            .field("base_view", &self.base_view)
            .field("inc_memtable", &self.inc_lsm.memtable_len())
            .field("con_memtable", &self.con_lsm.memtable_len())
            .finish()
    }
}

impl<IO, CO, CR, DIO: DiskIo> PersistentIrRecordStore<IO, CO, CR, DIO>
where
    IO: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
    CO: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
    CR: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
{
    /// Current base view number.
    pub(crate) fn base_view(&self) -> u64 {
        self.base_view
    }

    /// Borrow the manifest (for CombinedStore unified manifest save).
    pub(crate) fn manifest(&self) -> &UnifiedManifest {
        &self.manifest
    }

    /// Log diagnostic state of ir_inc VlogLsm for debugging.
    pub(crate) fn log_ir_inc_state(&self, label: &str) {
        let sealed_ids: Vec<u64> = self.inc_lsm.sealed_segments_ref().keys().copied().collect();
        let active_id = self.inc_lsm.active_vlog_id();
        let active_offset = self.inc_lsm.active_write_offset();
        let memtable_len = self.inc_lsm.memtable_len();
        tracing::debug!(
            "[ir_inc_state] {label}: sealed_ids={sealed_ids:?} active_id={active_id} active_offset={active_offset} memtable={memtable_len} base_view={}",
            self.base_view
        );
    }

    /// Borrow the inconsistent-op VlogLsm (for lazy resolution by CombinedTapirHandle).
    pub(crate) fn inc_lsm(&self) -> &VlogLsm<OpId, InconsistentEntry<IO>, DIO> {
        &self.inc_lsm
    }

    /// Borrow the consensus-op VlogLsm (for lazy resolution by CombinedTapirHandle).
    pub(crate) fn con_lsm(&self) -> &VlogLsm<OpId, ConsensusEntry<CO, CR>, DIO> {
        &self.con_lsm
    }

    /// Open a store from a persisted manifest, restoring sealed segments.
    pub(crate) fn open_from_manifest(
        base_dir: &Path,
        io_flags: OpenFlags,
        manifest: &UnifiedManifest,
    ) -> Result<Self, StorageError> {
        let inc_lsm = VlogLsm::open_from_manifest(
            "ir_inc",
            base_dir,
            &manifest.ir_inc,
            manifest.current_view,
            io_flags,
            IndexMode::InMemory,
        )?;
        let con_lsm = VlogLsm::open_from_manifest(
            "ir_con",
            base_dir,
            &manifest.ir_con,
            manifest.current_view,
            io_flags,
            IndexMode::InMemory,
        )?;
        Ok(Self {
            inc_lsm,
            con_lsm,
            base_view: manifest.current_view,
            manifest: manifest.clone(),
            base_dir: base_dir.to_path_buf(),
        })
    }

    /// Create a new store with empty VlogLsms at the given directory.
    pub(crate) fn open(
        base_dir: &Path,
        io_flags: OpenFlags,
    ) -> Result<Self, StorageError> {
        let inc_active = VlogSegment::<DIO>::open(
            0,
            base_dir.join("ir_inc_vlog_0000.dat"),
            io_flags,
        )?;
        let con_active = VlogSegment::<DIO>::open(
            0,
            base_dir.join("ir_con_vlog_0000.dat"),
            io_flags,
        )?;
        let inc_lsm = VlogLsm::open_from_parts(
            "ir_inc",
            base_dir,
            inc_active,
            BTreeMap::new(),
            io_flags,
            1,
            Vec::new(),
            0,
            IndexMode::InMemory,
        )?;
        let con_lsm = VlogLsm::open_from_parts(
            "ir_con",
            base_dir,
            con_active,
            BTreeMap::new(),
            io_flags,
            1,
            Vec::new(),
            0,
            IndexMode::InMemory,
        )?;
        Ok(Self {
            inc_lsm,
            con_lsm,
            base_view: 0,
            manifest: UnifiedManifest::new(),
            base_dir: base_dir.to_path_buf(),
        })
    }

    /// Seal both VlogLsms (flush memtable to vlog, clear memtable) and save manifest.
    /// Only finalized entries are persisted; tentative entries are discarded.
    pub(crate) fn seal(&mut self, new_view: u64) -> Result<(), StorageError> {
        let inc_sealed = self.inc_lsm.seal_view(u64::MAX, |_op_id, entry| {
            if entry.state.is_finalized() {
                Some((ENTRY_TYPE_INCONSISTENT, _op_id.client_id.0, _op_id.number))
            } else {
                None
            }
        })?;
        if let Some(meta) = inc_sealed {
            self.manifest.ir_inc.sealed_vlog_segments.push(meta);
        }
        let con_sealed = self.con_lsm.seal_view(u64::MAX, |_op_id, entry| {
            if entry.state.is_finalized() {
                Some((ENTRY_TYPE_CONSENSUS, _op_id.client_id.0, _op_id.number))
            } else {
                None
            }
        })?;
        if let Some(meta) = con_sealed {
            self.manifest.ir_con.sealed_vlog_segments.push(meta);
        }
        self.base_view = new_view;

        // Persist manifest with updated segment metadata.
        self.manifest.ir_inc.active_segment_id = self.inc_lsm.active_vlog_id();
        self.manifest.ir_inc.active_write_offset = self.inc_lsm.active_write_offset();
        self.manifest.ir_inc.next_segment_id = self.inc_lsm.next_segment_id();
        self.manifest.ir_inc.sst_metas = self.inc_lsm.sst_metas().to_vec();
        self.manifest.ir_inc.next_sst_id = self.inc_lsm.next_sst_id();
        self.manifest.ir_con.active_segment_id = self.con_lsm.active_vlog_id();
        self.manifest.ir_con.active_write_offset = self.con_lsm.active_write_offset();
        self.manifest.ir_con.next_segment_id = self.con_lsm.next_segment_id();
        self.manifest.ir_con.sst_metas = self.con_lsm.sst_metas().to_vec();
        self.manifest.ir_con.next_sst_id = self.con_lsm.next_sst_id();
        self.manifest.save::<DIO>(&self.base_dir)?;

        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);
        Ok(())
    }

    /// Build segment bytes paired with ViewRange metadata for all sealed + active vlog data.
    fn all_segment_bytes_with_views(
        &self,
    ) -> Result<(Vec<(Vec<ViewRange>, Vec<u8>)>, Vec<(Vec<ViewRange>, Vec<u8>)>), StorageError> {
        let inc = self.inc_lsm.export_segment_bytes_with_views()?;
        let con = self.con_lsm.export_segment_bytes_with_views()?;
        Ok((inc, con))
    }

    /// Encode current memtable as vlog-format bytes.
    fn memtable_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
        let inc = self.inc_lsm.encode_memtable_as_segment(inc_header_fn)?;
        let con = self.con_lsm.encode_memtable_as_segment(con_header_fn)?;
        Ok((inc, con))
    }

    /// Encode a PersistentRecord as segment bytes (for install_merged_record).
    fn encode_as_segments(
        record: &PersistentRecord<IO, CO, CR>,
    ) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
        let mut inc_bytes = Vec::new();
        for (op_id, entry) in &record.inconsistent {
            let payload = bitcode::serialize(entry)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            let raw = VlogSegment::<DIO>::encode_raw_entry(
                ENTRY_TYPE_INCONSISTENT,
                op_id.client_id.0,
                op_id.number,
                &payload,
            );
            inc_bytes.extend_from_slice(&raw);
        }
        let mut con_bytes = Vec::new();
        for (op_id, entry) in &record.consensus {
            let payload = bitcode::serialize(entry)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            let raw = VlogSegment::<DIO>::encode_raw_entry(
                ENTRY_TYPE_CONSENSUS,
                op_id.client_id.0,
                op_id.number,
                &payload,
            );
            con_bytes.extend_from_slice(&raw);
        }
        Ok((inc_bytes, con_bytes))
    }

    /// Static helper for `IrRecordStore::make_full_payload` — callable from
    /// CombinedRecordHandle without needing a `&self` reference.
    pub(crate) fn make_full_payload_static(
        record: PersistentRecord<IO, CO, CR>,
    ) -> PersistentPayload<IO, CO, CR> {
        {
            let mut inc_bytes = Vec::new();
            for (op_id, entry) in &record.inconsistent {
                let payload = bitcode::serialize(entry).expect("serialize inc entry");
                let raw = VlogSegment::<DIO>::encode_raw_entry(
                    ENTRY_TYPE_INCONSISTENT,
                    op_id.client_id.0,
                    op_id.number,
                    &payload,
                );
                inc_bytes.extend_from_slice(&raw);
            }
            let mut con_bytes = Vec::new();
            for (op_id, entry) in &record.consensus {
                let payload = bitcode::serialize(entry).expect("serialize con entry");
                let raw = VlogSegment::<DIO>::encode_raw_entry(
                    ENTRY_TYPE_CONSENSUS,
                    op_id.client_id.0,
                    op_id.number,
                    &payload,
                );
                con_bytes.extend_from_slice(&raw);
            }
            let inc = if inc_bytes.is_empty() {
                Vec::new()
            } else {
                vec![(Vec::new(), inc_bytes)]
            };
            let con = if con_bytes.is_empty() {
                Vec::new()
            } else {
                vec![(Vec::new(), con_bytes)]
            };
            PersistentPayload::full(inc, con)
        }
    }

    /// Prepare phase: partition payload segments and build transition record.
    ///
    /// Does NOT modify VlogLsm state — the store's `get_*` methods still
    /// reflect pre-install state, enabling O(log N) sync lookups via
    /// RecordStoreView between prepare and complete.
    fn prepare_start_view_unified(
        &self,
        payload: PersistentPayload<IO, CO, CR>,
        new_view: u64,
    ) -> Option<PreparedInstall<PersistentRecord<IO, CO, CR>, PersistentPayload<IO, CO, CR>>> {
        tracing::debug!("[install_sv] PREPARE view={new_view} base_view={}", self.base_view);
        let iw = std::time::Instant::now();

        let PayloadInner::Delta {
            inc_segments,
            con_segments,
            ..
        } = payload.inner.as_ref();

        // Helper: max view in a ViewRange list, or None if empty (unknown provenance).
        let max_view = |views: &[ViewRange]| -> Option<u64> {
            views.iter().map(|v| v.view).max()
        };

        // Partition payload segments into existing (skip) vs new (import).
        // Only new segments contribute to the transition record.
        let mut new_inc_bytes: Vec<Vec<u8>> = Vec::new();
        let mut new_con_bytes: Vec<Vec<u8>> = Vec::new();
        let mut skipped_inc = 0usize;
        let mut skipped_con = 0usize;

        for (views, bytes) in inc_segments {
            if max_view(views).is_some_and(|mv| mv <= self.base_view) {
                skipped_inc += 1;
            } else if !bytes.is_empty() {
                new_inc_bytes.push(bytes.clone());
            }
        }
        for (views, bytes) in con_segments {
            if max_view(views).is_some_and(|mv| mv <= self.base_view) {
                skipped_con += 1;
            } else if !bytes.is_empty() {
                new_con_bytes.push(bytes.clone());
            }
        }

        tracing::debug!(
            "[install_sv] view={new_view} skipped_inc={skipped_inc} new_inc={} skipped_con={skipped_con} new_con={}",
            new_inc_bytes.len(),
            new_con_bytes.len(),
        );

        // transition: bytes from new segments only → O(|delta| log |delta|)
        let from_view = if self.base_view > 0 { self.base_view } else { 0 };
        let transition_record = RawRecord::<IO, CO, CR> {
            inc_segments: new_inc_bytes,
            con_segments: new_con_bytes,
            _phantom: std::marker::PhantomData,
        }
        .into_indexed();

        let prepare_ms = iw.elapsed().as_millis();
        tracing::debug!(
            "[install_sv] view={new_view} base={from_view} prepare={prepare_ms}ms",
        );

        Some(PreparedInstall {
            transition: (from_view, transition_record),
            payload,
            new_view,
        })
    }

    /// Complete phase: import new segments, clear memtable, advance base_view.
    ///
    /// Re-partitions the payload segments (same view-based logic as prepare)
    /// and imports only the new ones into VlogLsm via persist_sealed_segment.
    fn complete_start_view_unified(
        &mut self,
        prepared: PreparedInstall<PersistentRecord<IO, CO, CR>, PersistentPayload<IO, CO, CR>>,
    ) {
        let new_view = prepared.new_view;
        self.log_ir_inc_state(&format!("install_sv BEFORE view={new_view}"));
        let iw = std::time::Instant::now();

        let PayloadInner::Delta {
            inc_segments,
            con_segments,
            ..
        } = prepared.payload.inner.as_ref();

        let max_view = |views: &[ViewRange]| -> Option<u64> {
            views.iter().map(|v| v.view).max()
        };

        // Import new segments into VlogLsm.
        for (views, bytes) in inc_segments {
            if max_view(views).is_none_or(|mv| mv > self.base_view)
                && !bytes.is_empty()
                && let Some(meta) = self
                    .inc_lsm
                    .persist_sealed_segment(bytes, op_id_from_raw, views.clone())
                    .unwrap_or_else(|e| panic!("install_sv: import inc failed: {e}"))
            {
                self.manifest.ir_inc.sealed_vlog_segments.push(meta);
            }
        }
        for (views, bytes) in con_segments {
            if max_view(views).is_none_or(|mv| mv > self.base_view)
                && !bytes.is_empty()
                && let Some(meta) = self
                    .con_lsm
                    .persist_sealed_segment(bytes, op_id_from_raw, views.clone())
                    .unwrap_or_else(|e| panic!("install_sv: import con failed: {e}"))
            {
                self.manifest.ir_con.sealed_vlog_segments.push(meta);
            }
        }

        let import_ms = iw.elapsed().as_millis();

        // Update state.
        self.inc_lsm.clear_memtable();
        self.con_lsm.clear_memtable();
        self.base_view = new_view;
        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);

        // Update manifest with current LSM state.
        self.manifest.ir_inc.active_segment_id = self.inc_lsm.active_vlog_id();
        self.manifest.ir_inc.active_write_offset = self.inc_lsm.active_write_offset();
        self.manifest.ir_inc.next_segment_id = self.inc_lsm.next_segment_id();
        self.manifest.ir_con.active_segment_id = self.con_lsm.active_vlog_id();
        self.manifest.ir_con.active_write_offset = self.con_lsm.active_write_offset();
        self.manifest.ir_con.next_segment_id = self.con_lsm.next_segment_id();

        self.log_ir_inc_state(&format!("install_sv AFTER view={new_view}"));

        let total_ms = iw.elapsed().as_millis();
        if total_ms > 10 {
            tracing::debug!("[install_sv] view={new_view} import={import_ms}ms total={total_ms}ms");
        }
    }
}

impl<IO, CO, CR, DIO: DiskIo> IrRecordStore<IO, CO, CR>
    for PersistentIrRecordStore<IO, CO, CR, DIO>
where
    IO: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
    CO: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
    CR: Clone + Debug + Serialize + DeserializeOwned + PartialEq + Send + 'static,
{
    type Record = PersistentRecord<IO, CO, CR>;
    type RawRecord = RawRecord<IO, CO, CR>;
    type Payload = PersistentPayload<IO, CO, CR>;

    fn get_inconsistent_entry(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>> {
        // Check memtable first, then sealed data (index → vlog)
        self.inc_lsm.get(op_id).ok().flatten()
    }

    fn get_consensus_entry(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>> {
        self.con_lsm.get(op_id).ok().flatten()
    }

    fn insert_inconsistent_entry(&mut self, op_id: OpId, entry: InconsistentEntry<IO>) {
        self.inc_lsm.put(op_id, entry);
    }

    fn insert_consensus_entry(&mut self, op_id: OpId, entry: ConsensusEntry<CO, CR>) {
        self.con_lsm.put(op_id, entry);
    }

    fn base_view(&self) -> u64 {
        self.base_view
    }

    fn memtable_record(&self) -> Self::Record {
        let (inc_mem, con_mem) = self
            .memtable_bytes()
            .expect("memtable_record: memtable_bytes failed");
        let inc_segments = if inc_mem.is_empty() { vec![] } else { vec![inc_mem] };
        let con_segments = if con_mem.is_empty() { vec![] } else { vec![con_mem] };
        RawRecord::<IO, CO, CR> {
            inc_segments,
            con_segments,
            _phantom: std::marker::PhantomData,
        }
        .into_indexed()
    }

    fn inconsistent_len(&self) -> usize {
        // Memtable entries + indexed entries (approximate — may double-count promotions)
        self.inc_lsm.memtable_len()
    }

    fn consensus_len(&self) -> usize {
        self.con_lsm.memtable_len()
    }

    fn build_view_change_payload(&self) -> Self::Payload {
        // DVC payloads are always memtable-only delta. Sealed segments
        // are never sent — all peers with matching LNV have identical
        // sealed data.
        let (inc_bytes, con_bytes) = self
            .memtable_bytes()
            .expect("build_view_change_payload: memtable_bytes failed");
        PersistentPayload::delta(
            ViewNumber(self.base_view),
            inc_bytes,
            con_bytes,
            vec![ViewRange {
                view: self.base_view,
                start_offset: 0,
                end_offset: 0,
                num_entries: 0,
            }],
        )
    }

    fn build_start_view_payload(&self, delta: Option<&Self::Payload>) -> Self::Payload {
        delta.cloned().unwrap_or_else(|| {
            let (inc, con) = self
                .all_segment_bytes_with_views()
                .expect("build_start_view_payload: export failed");
            PersistentPayload::full(inc, con)
        })
    }

    fn make_full_payload(record: Self::Record) -> Self::Payload {
        Self::make_full_payload_static(record)
    }

    fn prepare_start_view_install(
        &self,
        payload: Self::Payload,
        new_view: u64,
    ) -> Option<PreparedInstall<Self::Record, Self::Payload>> {
        let bv = payload.base_view().expect("all payloads have base_view");
        // Reject delta if base doesn't match (but allow base_view=0 aka full reset).
        if bv.0 > 0 && (self.base_view == 0 || ViewNumber(self.base_view) != bv) {
            tracing::debug!("[install_sv_payload] REJECTED view={new_view} bv={bv:?} self.base_view={}", self.base_view);
            return None;
        }
        tracing::debug!("[install_sv_payload] ACCEPTED view={new_view} bv={bv:?} self.base_view={}", self.base_view);
        self.prepare_start_view_unified(payload, new_view)
    }

    fn complete_start_view_install(
        &mut self,
        prepared: PreparedInstall<Self::Record, Self::Payload>,
    ) {
        self.complete_start_view_unified(prepared);
    }

    fn install_merged_record(
        &mut self,
        merged: Self::Record,
        new_view: u64,
    ) -> MergeInstallResult<Self::Record, Self::Payload> {
        self.log_ir_inc_state(&format!("install_merged_record BEFORE view={new_view}"));
        let iw = std::time::Instant::now();
        let previous_base_view = if self.base_view > 0 {
            Some(ViewNumber(self.base_view))
        } else {
            None
        };
        let from_view = self.base_view;

        // Encode and persist merged record as a sealed segment.
        // R IS the delta — it contains only merged memtable entries from
        // the current view. All peers in the merge have matching LNV, so
        // their sealed segments are identical to the leader's. No segment
        // import from peer payloads is needed.
        let (merged_inc_bytes, merged_con_bytes) =
            Self::encode_as_segments(&merged)
                .expect("install_merged: encode merged failed");

        let new_view_range = vec![ViewRange {
            view: new_view,
            start_offset: 0,
            end_offset: 0,
            num_entries: 0,
        }];

        if !merged_inc_bytes.is_empty()
            && let Some(meta) = self
                .inc_lsm
                .persist_sealed_segment(
                    &merged_inc_bytes,
                    op_id_from_raw,
                    new_view_range.clone(),
                )
                .unwrap_or_else(|e| panic!("install_merged: persist inc failed: {e}"))
        {
            self.manifest.ir_inc.sealed_vlog_segments.push(meta);
        }
        if !merged_con_bytes.is_empty()
            && let Some(meta) = self
                .con_lsm
                .persist_sealed_segment(
                    &merged_con_bytes,
                    op_id_from_raw,
                    new_view_range.clone(),
                )
                .unwrap_or_else(|e| panic!("install_merged: persist con failed: {e}"))
        {
            self.manifest.ir_con.sealed_vlog_segments.push(meta);
        }

        let persist_ms = iw.elapsed().as_millis();

        // --- Step 3: Finalize state ---
        self.inc_lsm.clear_memtable();
        self.con_lsm.clear_memtable();
        self.base_view = new_view;
        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);
        self.log_ir_inc_state(&format!("install_merged_record AFTER view={new_view}"));

        let total_ms = iw.elapsed().as_millis();
        tracing::debug!(
            "[install_merged] view={new_view} base={from_view} persist={persist_ms}ms total={total_ms}ms",
        );

        // Build start_view_delta for same-base recipients.
        let start_view_delta = previous_base_view.map(|prev_bv| {
            PersistentPayload::delta(
                prev_bv,
                merged_inc_bytes.clone(),
                merged_con_bytes.clone(),
                new_view_range.clone(),
            )
        });

        // Transition = the merged memtable entries from this view change.
        let transition = (from_view, merged);

        MergeInstallResult {
            transition,
            start_view_delta,
            previous_base_view,
        }
    }

    fn flush(&mut self) {
        // seal() flushes both VlogLsm memtables and saves the manifest.
        // Pass current base_view — flush must NOT advance the base view.
        // Advancing it causes an off-by-one that breaks delta payload matching:
        // the leader's base_view would be 1 ahead of recipients' latest_normal_view.
        self.seal(self.base_view)
            .expect("PersistentIrRecordStore::flush: seal failed");
    }

    fn stored_bytes(&self) -> Option<u64> {
        let inc_sealed: u64 = self
            .manifest
            .ir_inc
            .sealed_vlog_segments
            .iter()
            .map(|seg| seg.total_size)
            .sum();
        let con_sealed: u64 = self
            .manifest
            .ir_con
            .sealed_vlog_segments
            .iter()
            .map(|seg| seg.total_size)
            .sum();
        let inc_active = self.inc_lsm.active_write_offset();
        let con_active = self.con_lsm.active_write_offset();
        Some(inc_sealed + con_sealed + inc_active + con_active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::RecordEntryState as State;
    use crate::mvcc::disk::memory_io::MemoryIo;

    type Store = PersistentIrRecordStore<String, String, String, MemoryIo>;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    fn make_store() -> Store {
        let dir = MemoryIo::temp_path();
        Store::open(&dir, test_flags()).unwrap()
    }

    fn op_id(client: u64, num: u64) -> OpId {
        OpId {
            client_id: ClientId(client),
            number: num,
        }
    }

    fn inc_entry(op: &str, view: u64) -> InconsistentEntry<String> {
        InconsistentEntry {
            op: op.to_string(),
            state: State::Tentative,
            modified_view: view,
        }
    }

    fn fin_inc_entry(op: &str, view: u64) -> InconsistentEntry<String> {
        InconsistentEntry {
            op: op.to_string(),
            state: State::Finalized(ViewNumber(view)),
            modified_view: view,
        }
    }

    fn con_entry(op: &str, result: &str, view: u64) -> ConsensusEntry<String, String> {
        ConsensusEntry {
            op: op.to_string(),
            result: result.to_string(),
            state: State::Tentative,
            modified_view: view,
        }
    }

    fn fin_con_entry(
        op: &str,
        result: &str,
        view: u64,
    ) -> ConsensusEntry<String, String> {
        ConsensusEntry {
            op: op.to_string(),
            result: result.to_string(),
            state: State::Finalized(ViewNumber(view)),
            modified_view: view,
        }
    }

    #[test]
    fn entry_insert_into_empty() {
        let mut store = make_store();
        assert!(store.get_inconsistent_entry(&op_id(1, 1)).is_none());
        store.insert_inconsistent_entry(op_id(1, 1), inc_entry("op1", 0));
        assert_eq!(store.inconsistent_len(), 1);
    }

    #[test]
    fn entry_occupied_from_sealed() {
        let mut store = make_store();
        // Insert and finalize an entry
        store.insert_inconsistent_entry(op_id(1, 1), fin_inc_entry("op1", 0));
        // Seal to move to vlog
        store.seal(1).unwrap();

        // Entry should be found from sealed data
        let e = store.get_inconsistent_entry(&op_id(1, 1)).expect("expected from sealed");
        assert_eq!(e.op, "op1");
    }

    #[test]
    fn get_modify_insert_memtable() {
        let mut store = make_store();
        // No entry → None
        assert!(store.get_inconsistent_entry(&op_id(1, 1)).is_none());

        // Insert to memtable
        store.insert_inconsistent_entry(op_id(1, 1), inc_entry("op1", 0));
        // Get, modify, insert back
        let mut entry = store.get_inconsistent_entry(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(ViewNumber(1));
        store.insert_inconsistent_entry(op_id(1, 1), entry);
        assert!(store.get_inconsistent_entry(&op_id(1, 1)).unwrap().state.is_finalized());
    }

    #[test]
    fn memtable_record_returns_only_memtable() {
        let mut store = make_store();
        // Seal a finalized entry into sealed storage
        store.insert_inconsistent_entry(op_id(1, 1), fin_inc_entry("sealed_op", 0));
        store.seal(1).unwrap();

        // Add entry to memtable (current view)
        store.insert_inconsistent_entry(op_id(2, 1), inc_entry("memtable_op", 1));

        // memtable_record returns only memtable entry
        let record = store.memtable_record();
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.op, "memtable_op");
    }

    #[test]
    fn seal_discards_tentative() {
        let mut store = make_store();
        // Insert tentative entry
        store.insert_inconsistent_entry(op_id(1, 1), inc_entry("tentative", 0));
        // Insert finalized entry
        store.insert_inconsistent_entry(op_id(2, 1), fin_inc_entry("finalized", 0));
        store.seal(1).unwrap();

        // Tentative entry should be discarded after seal
        assert!(store.get_inconsistent_entry(&op_id(1, 1)).is_none());
        // Finalized entry should survive
        let entry = store.get_inconsistent_entry(&op_id(2, 1)).unwrap();
        assert_eq!(entry.op, "finalized");
    }

    #[test]
    fn persistent_record_raw_streaming() {
        // Build a Raw record from segment bytes
        let op = op_id(1, 1);
        let entry = fin_inc_entry("test", 0);
        let payload = bitcode::serialize(&entry).unwrap();
        let raw = VlogSegment::<MemoryIo>::encode_raw_entry(
            ENTRY_TYPE_INCONSISTENT,
            op.client_id.0,
            op.number,
            &payload,
        );
        let record: RawRecord<String, String, String> = RawRecord {
            inc_segments: vec![raw],
            con_segments: Vec::new(),
            _phantom: std::marker::PhantomData,
        };
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, op);
        assert_eq!(entries[0].1.op, "test");
    }

    #[test]
    fn persistent_record_indexed_builder() {
        let mut record: PersistentRecord<String, String, String> = PersistentRecord::default();
        record.insert_inconsistent(op_id(1, 1), inc_entry("a", 0));
        record.insert_consensus(op_id(2, 1), con_entry("b", "r", 0));

        assert_eq!(record.inconsistent_entries().count(), 1);
        assert_eq!(record.consensus_entries().count(), 1);
        assert!(record.get_inconsistent(&op_id(1, 1)).is_some());
        assert!(record.get_consensus(&op_id(2, 1)).is_some());
    }

    #[test]
    fn build_view_change_payload_always_delta() {
        let mut store = make_store();
        // Insert and seal to establish base
        store.insert_inconsistent_entry(op_id(1, 1), fin_inc_entry("op1", 0));
        store.seal(1).unwrap();

        // Add overlay entry
        store.insert_inconsistent_entry(op_id(2, 1), inc_entry("op2", 1));

        // Always delta with base_view = store's base_view
        let payload = store.build_view_change_payload();
        assert_eq!(payload.base_view(), Some(ViewNumber(1)));
    }

    #[test]
    fn install_start_view_roundtrip() {
        let mut store = make_store();
        // Insert entries and seal (simulating a completed view change)
        store.insert_inconsistent_entry(op_id(1, 1), fin_inc_entry("op1", 0));
        store.insert_consensus_entry(op_id(1, 2), fin_con_entry("cop1", "r1", 0));
        store.seal(1).unwrap();

        // Build full StartView payload (not DVC — StartView carries sealed segments)
        let payload = store.build_start_view_payload(None);

        // Create a new store and install
        let mut store2 = make_store();
        let result = store2.install_start_view_payload(payload, 1);
        assert!(result.is_some());

        // Verify entries survive
        let e = store2.get_inconsistent_entry(&op_id(1, 1)).expect("expected after install");
        assert_eq!(e.op, "op1");
        let e = store2.get_consensus_entry(&op_id(1, 2)).expect("expected after install");
        assert_eq!(e.op, "cop1");
    }

    #[test]
    fn install_merged_record_roundtrip() {
        let mut store = make_store();
        // Build an Indexed record (as leader would)
        let mut merged: PersistentRecord<String, String, String> = PersistentRecord::default();
        merged.insert_inconsistent(op_id(1, 1), fin_inc_entry("op1", 0));
        merged.insert_consensus(op_id(1, 2), fin_con_entry("cop1", "r1", 0));

        let result = store.install_merged_record(merged, 1);
        assert!(result.start_view_delta.is_none()); // no base yet
        assert_eq!(store.base_view, 1);

        // Verify entries accessible
        let e = store.get_inconsistent_entry(&op_id(1, 1)).expect("expected occupied");
        assert_eq!(e.op, "op1");
    }

    #[test]
    fn persistent_payload_serde_round_trip() {
        let payload: PersistentPayload<String, String, String> =
            PersistentPayload::full(vec![(Vec::new(), vec![1, 2, 3])], vec![(Vec::new(), vec![4, 5])]);
        let bytes = bitcode::serialize(&payload).unwrap();
        let decoded: PersistentPayload<String, String, String> =
            bitcode::deserialize(&bytes).unwrap();
        assert!(format!("{:?}", decoded).contains("Delta"));
        assert_eq!(decoded.base_view(), Some(ViewNumber(0)));

        let delta: PersistentPayload<String, String, String> =
            PersistentPayload::delta(ViewNumber(5), vec![10, 20], vec![30], Vec::new());
        let bytes2 = bitcode::serialize(&delta).unwrap();
        let decoded2: PersistentPayload<String, String, String> =
            bitcode::deserialize(&bytes2).unwrap();
        assert!(format!("{:?}", decoded2).contains("Delta"));
    }

    #[test]
    fn as_unresolved_record_full() {
        let _store = make_store();
        // Create a full payload with some entries
        let op = op_id(1, 1);
        let entry = fin_inc_entry("test", 0);
        let payload_bytes = bitcode::serialize(&entry).unwrap();
        let raw = VlogSegment::<MemoryIo>::encode_raw_entry(
            ENTRY_TYPE_INCONSISTENT,
            op.client_id.0,
            op.number,
            &payload_bytes,
        );
        let payload: PersistentPayload<String, String, String> =
            PersistentPayload::full(vec![(Vec::new(), raw)], Vec::new());

        let record = payload.as_raw_record();
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.op, "test");
    }

    #[test]
    fn ir_inc_manifest_tracks_segments_after_full_install() {
        let mut store = make_store();
        store.insert_inconsistent_entry(op_id(1, 1), fin_inc_entry("op1", 0));
        store.insert_consensus_entry(op_id(1, 2), fin_con_entry("cop1", "r1", 0));
        store.seal(1).unwrap();

        // Full StartView payload for lagging followers
        let payload = store.build_start_view_payload(None);
        assert_eq!(
            payload.base_view(),
            Some(ViewNumber(0)),
            "full StartView payload has base_view=0"
        );

        // Install on fresh store
        let mut store2 = make_store();
        let result = store2.install_start_view_payload(payload, 2);
        assert!(result.is_some());

        // Entries accessible
        assert!(store2.get_inconsistent_entry(&op_id(1, 1)).is_some());
        assert!(store2.get_consensus_entry(&op_id(1, 2)).is_some());

        // Manifest must track the imported sealed segments.
        // On the buggy code, import_segments() discards VlogSegmentMeta,
        // so the manifest stays empty while the LSM has sealed segments.
        let inc_lsm_sealed = store2.inc_lsm.sealed_segments_ref().len();
        let manifest_sealed = store2.manifest.ir_inc.sealed_vlog_segments.len();
        assert_eq!(
            manifest_sealed, inc_lsm_sealed,
            "manifest sealed_vlog_segments ({manifest_sealed}) must match \
             LSM sealed segments ({inc_lsm_sealed})"
        );

        // After flush, entries should still be accessible
        store2.flush();
        assert!(store2.get_inconsistent_entry(&op_id(1, 1)).is_some());
    }

    #[test]
    fn always_delta_install_on_established_recipient() {
        // View 1: Leader installs merged record with initial entries
        let mut leader = make_store();
        let mut merged_v1: PersistentRecord<String, String, String> =
            PersistentRecord::default();
        merged_v1.insert_inconsistent(op_id(1, 1), fin_inc_entry("op1", 0));
        merged_v1.insert_consensus(op_id(1, 2), fin_con_entry("cop1", "r1", 0));
        leader.install_merged_record(merged_v1, 1);

        // Leader sends StartView payload to recipient B
        let payload_v1 = leader.build_start_view_payload(None);
        let mut store_b = make_store();
        let result1 = store_b.install_start_view_payload(payload_v1, 1);
        assert!(result1.is_some());
        assert_eq!(store_b.base_view, 1);
        let b_sealed_v1 = store_b.manifest.ir_inc.sealed_vlog_segments.len();
        assert!(b_sealed_v1 > 0, "B should have sealed segments after first install");

        // View 2: Leader installs merged record with more entries
        let mut merged_v2: PersistentRecord<String, String, String> =
            PersistentRecord::default();
        merged_v2.insert_inconsistent(op_id(1, 1), fin_inc_entry("op1", 0));
        merged_v2.insert_consensus(op_id(1, 2), fin_con_entry("cop1", "r1", 0));
        merged_v2.insert_inconsistent(op_id(2, 1), fin_inc_entry("op2", 1));
        merged_v2.insert_consensus(op_id(2, 2), fin_con_entry("cop2", "r2", 1));
        leader.install_merged_record(merged_v2, 2);

        // Leader sends full StartView payload (non-same-base)
        let payload_v2 = leader.build_start_view_payload(None);
        let (from_view, transition) = store_b.install_start_view_payload(payload_v2, 2)
            .expect("install should succeed");
        assert_eq!(store_b.base_view, 2);

        // ALL entries accessible
        assert!(store_b.get_inconsistent_entry(&op_id(1, 1)).is_some(), "old entry missing");
        assert!(store_b.get_inconsistent_entry(&op_id(2, 1)).is_some(), "new entry missing");
        assert!(store_b.get_consensus_entry(&op_id(1, 2)).is_some(), "old con entry missing");
        assert!(store_b.get_consensus_entry(&op_id(2, 2)).is_some(), "new con entry missing");

        // Manifest sealed count matches LSM sealed count
        let inc_lsm_sealed = store_b.inc_lsm.sealed_segments_ref().len();
        let manifest_sealed = store_b.manifest.ir_inc.sealed_vlog_segments.len();
        assert_eq!(
            manifest_sealed, inc_lsm_sealed,
            "manifest ({manifest_sealed}) must match LSM ({inc_lsm_sealed})"
        );

        // transition should contain only new entries
        assert_eq!(from_view, 1);
        let delta_inc: Vec<_> = transition.inconsistent_entries().collect();
        assert!(delta_inc.iter().any(|(_, e)| e.op == "op2"), "delta should contain new entry");
    }

    #[test]
    fn always_delta_leader_unknown_recipient_lnv() {
        // View 1: Leader installs merged with v1 entry
        let mut leader = make_store();
        let mut merged_v1: PersistentRecord<String, String, String> =
            PersistentRecord::default();
        merged_v1.insert_inconsistent(op_id(1, 1), fin_inc_entry("v1_op", 0));
        leader.install_merged_record(merged_v1, 1);

        // Recipient B installs from view 1
        let payload_v1 = leader.build_start_view_payload(None);
        let mut store_b = make_store();
        let r = store_b.install_start_view_payload(payload_v1, 1);
        assert!(r.is_some());
        assert_eq!(store_b.base_view, 1);

        // View 2+3: Leader installs more merged records
        let mut merged_v2: PersistentRecord<String, String, String> =
            PersistentRecord::default();
        merged_v2.insert_inconsistent(op_id(1, 1), fin_inc_entry("v1_op", 0));
        merged_v2.insert_inconsistent(op_id(2, 1), fin_inc_entry("v2_op", 1));
        leader.install_merged_record(merged_v2, 2);

        let mut merged_v3: PersistentRecord<String, String, String> =
            PersistentRecord::default();
        merged_v3.insert_inconsistent(op_id(1, 1), fin_inc_entry("v1_op", 0));
        merged_v3.insert_inconsistent(op_id(2, 1), fin_inc_entry("v2_op", 1));
        merged_v3.insert_inconsistent(op_id(3, 1), fin_inc_entry("v3_op", 2));
        leader.install_merged_record(merged_v3, 3);

        // Leader sends full payload for view 3 to B (doesn't know B's LNV)
        let payload_v3 = leader.build_start_view_payload(None);

        // B installs: skips view-1 segments, imports view-2 and view-3 segments
        let result = store_b.install_start_view_payload(payload_v3, 3);
        assert!(result.is_some());
        assert_eq!(store_b.base_view, 3);

        // B has all entries from views 1-3
        assert!(store_b.get_inconsistent_entry(&op_id(1, 1)).is_some(), "v1 entry missing");
        assert!(store_b.get_inconsistent_entry(&op_id(2, 1)).is_some(), "v2 entry missing");
        assert!(store_b.get_inconsistent_entry(&op_id(3, 1)).is_some(), "v3 entry missing");
    }
}
