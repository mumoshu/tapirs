use crate::ir::{
    ClientId, IrPayload, IrRecordStore, MergeInstallResult, OpId,
    RecordBuilder, RecordConsensusEntry as ConsensusEntry,
    RecordInconsistentEntry as InconsistentEntry,
    RecordView, VersionedEntry, VersionedVacantEntry, ViewInstallResult, ViewNumber,
};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::unified::wisckeylsm::lsm::{IndexMode, VlogLsm};
use crate::unified::wisckeylsm::vlog::{RawVlogEntry, VlogSegment, VLOG_RAW_ENTRY_OVERHEAD};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
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
/// Raw: holds raw vlog segment bytes, streams entries via RecordView — deserializing
/// one entry at a time without materializing BTreeMaps.
/// Indexed: holds BTreeMaps for the leader's merged record (efficient O(log n) lookup).
///
/// Default creates empty Indexed; full_record()/resolve return Raw.
pub enum PersistentRecord<IO, CO, CR> {
    Raw {
        inc_segments: Vec<Vec<u8>>,
        con_segments: Vec<Vec<u8>>,
    },
    Indexed {
        inconsistent: BTreeMap<OpId, InconsistentEntry<IO>>,
        consensus: BTreeMap<OpId, ConsensusEntry<CO, CR>>,
    },
}

impl<IO: Debug, CO: Debug, CR: Debug> Debug for PersistentRecord<IO, CO, CR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raw {
                inc_segments,
                con_segments,
            } => f
                .debug_struct("PersistentRecord::Raw")
                .field("inc_segments", &inc_segments.len())
                .field("con_segments", &con_segments.len())
                .finish(),
            Self::Indexed {
                inconsistent,
                consensus,
            } => f
                .debug_struct("PersistentRecord::Indexed")
                .field("inconsistent", &inconsistent.len())
                .field("consensus", &consensus.len())
                .finish(),
        }
    }
}

impl<IO, CO, CR> Default for PersistentRecord<IO, CO, CR> {
    fn default() -> Self {
        Self::Indexed {
            inconsistent: BTreeMap::new(),
            consensus: BTreeMap::new(),
        }
    }
}

/// Iterator adapter that unifies two concrete iterator types.
enum EitherIter<A, B> {
    Left(A),
    Right(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for EitherIter<A, B> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        match self {
            Self::Left(a) => a.next(),
            Self::Right(b) => b.next(),
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

impl<IO, CO, CR> RecordView for PersistentRecord<IO, CO, CR>
where
    IO: Clone + DeserializeOwned,
    CO: Clone + DeserializeOwned,
    CR: Clone + DeserializeOwned,
{
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<CO, CR>)> {
        match self {
            Self::Raw { con_segments, .. } => {
                EitherIter::Left(scan_entries::<ConsensusEntry<CO, CR>>(con_segments).into_iter())
            }
            Self::Indexed { consensus, .. } => {
                EitherIter::Right(consensus.iter().map(|(k, v)| (*k, v.clone())))
            }
        }
    }

    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<IO>)> {
        match self {
            Self::Raw { inc_segments, .. } => {
                EitherIter::Left(scan_entries::<InconsistentEntry<IO>>(inc_segments).into_iter())
            }
            Self::Indexed { inconsistent, .. } => {
                EitherIter::Right(inconsistent.iter().map(|(k, v)| (*k, v.clone())))
            }
        }
    }

    fn get_consensus(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>> {
        match self {
            Self::Raw { con_segments, .. } => {
                scan_entries::<ConsensusEntry<CO, CR>>(con_segments)
                    .into_iter()
                    .find(|(id, _)| id == op_id)
                    .map(|(_, e)| e)
            }
            Self::Indexed { consensus, .. } => consensus.get(op_id).cloned(),
        }
    }

    fn get_inconsistent(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>> {
        match self {
            Self::Raw { inc_segments, .. } => {
                scan_entries::<InconsistentEntry<IO>>(inc_segments)
                    .into_iter()
                    .find(|(id, _)| id == op_id)
                    .map(|(_, e)| e)
            }
            Self::Indexed { inconsistent, .. } => inconsistent.get(op_id).cloned(),
        }
    }
}

impl<IO, CO, CR> RecordBuilder for PersistentRecord<IO, CO, CR>
where
    IO: Clone + DeserializeOwned,
    CO: Clone + DeserializeOwned,
    CR: Clone + DeserializeOwned,
{
    fn insert_inconsistent(&mut self, op_id: OpId, entry: InconsistentEntry<Self::IO>) {
        match self {
            Self::Indexed { inconsistent, .. } => {
                inconsistent.insert(op_id, entry);
            }
            Self::Raw { .. } => {
                panic!("RecordBuilder::insert_inconsistent called on Raw variant");
            }
        }
    }

    fn insert_consensus(&mut self, op_id: OpId, entry: ConsensusEntry<Self::CO, Self::CR>) {
        match self {
            Self::Indexed { consensus, .. } => {
                consensus.insert(op_id, entry);
            }
            Self::Raw { .. } => {
                panic!("RecordBuilder::insert_consensus called on Raw variant");
            }
        }
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
    Full {
        inc_segments: Vec<Vec<u8>>,
        con_segments: Vec<Vec<u8>>,
    },
    Delta {
        base_view: ViewNumber,
        inc_bytes: Vec<u8>,
        con_bytes: Vec<u8>,
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
    fn full(inc_segments: Vec<Vec<u8>>, con_segments: Vec<Vec<u8>>) -> Self {
        Self {
            inner: Arc::new(PayloadInner::Full {
                inc_segments,
                con_segments,
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    fn delta(base_view: ViewNumber, inc_bytes: Vec<u8>, con_bytes: Vec<u8>) -> Self {
        Self {
            inner: Arc::new(PayloadInner::Delta {
                base_view,
                inc_bytes,
                con_bytes,
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

    fn resolve(self, base: Option<&Self::Record>) -> Self::Record {
        match self.inner.as_ref() {
            PayloadInner::Full {
                inc_segments,
                con_segments,
            } => PersistentRecord::Raw {
                inc_segments: inc_segments.clone(),
                con_segments: con_segments.clone(),
            },
            PayloadInner::Delta {
                inc_bytes,
                con_bytes,
                ..
            } => {
                let base = base.expect("delta requires matching base");
                let (mut inc_segments, mut con_segments) = match base {
                    PersistentRecord::Raw {
                        inc_segments,
                        con_segments,
                    } => (inc_segments.clone(), con_segments.clone()),
                    PersistentRecord::Indexed { .. } => {
                        panic!("delta resolve on Indexed base")
                    }
                };
                if !inc_bytes.is_empty() {
                    inc_segments.push(inc_bytes.clone());
                }
                if !con_bytes.is_empty() {
                    con_segments.push(con_bytes.clone());
                }
                PersistentRecord::Raw {
                    inc_segments,
                    con_segments,
                }
            }
        }
    }

    fn base_view(&self) -> Option<ViewNumber> {
        match self.inner.as_ref() {
            PayloadInner::Full { .. } => None,
            PayloadInner::Delta { base_view, .. } => Some(*base_view),
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
pub(crate) struct PersistentIrRecordStore<IO, CO, CR, DIO: DiskIo> {
    inc_lsm: VlogLsm<OpId, InconsistentEntry<IO>, DIO>,
    con_lsm: VlogLsm<OpId, ConsensusEntry<CO, CR>, DIO>,
    base_view: u64,
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
        })
    }

    /// Seal both VlogLsms (flush memtable to vlog, clear memtable).
    /// Only finalized entries are persisted; tentative entries are discarded.
    pub(crate) fn seal(&mut self, new_view: u64) -> Result<(), StorageError> {
        self.inc_lsm.seal_view(u64::MAX, |_op_id, entry| {
            if entry.state.is_finalized() {
                Some((ENTRY_TYPE_INCONSISTENT, _op_id.client_id.0, _op_id.number))
            } else {
                None
            }
        })?;
        self.con_lsm.seal_view(u64::MAX, |_op_id, entry| {
            if entry.state.is_finalized() {
                Some((ENTRY_TYPE_CONSENSUS, _op_id.client_id.0, _op_id.number))
            } else {
                None
            }
        })?;
        self.base_view = new_view;
        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);
        Ok(())
    }

    /// Build segment bytes for all sealed + active vlog data.
    fn all_segment_bytes(&self) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), StorageError> {
        let inc = self.inc_lsm.export_segment_bytes()?;
        let con = self.con_lsm.export_segment_bytes()?;
        Ok((inc, con))
    }

    /// Encode current memtable as vlog-format bytes.
    fn memtable_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
        let inc = self.inc_lsm.encode_memtable_as_segment(inc_header_fn)?;
        let con = self.con_lsm.encode_memtable_as_segment(con_header_fn)?;
        Ok((inc, con))
    }

    /// Import raw segment bytes into both VlogLsms and rebuild index.
    fn import_segments(
        &mut self,
        inc_segments: &[Vec<u8>],
        con_segments: &[Vec<u8>],
    ) -> Result<(), StorageError> {
        for bytes in inc_segments {
            self.inc_lsm.import_raw_segment(bytes, op_id_from_raw)?;
        }
        for bytes in con_segments {
            self.con_lsm.import_raw_segment(bytes, op_id_from_raw)?;
        }
        Ok(())
    }

    /// Encode a PersistentRecord::Indexed as segment bytes (for install_merged_record).
    fn encode_indexed_as_segments(
        record: &PersistentRecord<IO, CO, CR>,
    ) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
        match record {
            PersistentRecord::Indexed {
                inconsistent,
                consensus,
            } => {
                let mut inc_bytes = Vec::new();
                for (op_id, entry) in inconsistent {
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
                for (op_id, entry) in consensus {
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
            PersistentRecord::Raw { .. } => {
                panic!("encode_indexed_as_segments called on Raw variant");
            }
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
    type Payload = PersistentPayload<IO, CO, CR>;

    fn entry_inconsistent(&mut self, op_id: OpId) -> VersionedEntry<'_, InconsistentEntry<IO>> {
        // Check memtable first, then sealed data (index → vlog)
        if let Ok(Some(entry)) = self.inc_lsm.get(&op_id) {
            return VersionedEntry::Occupied(entry);
        }
        VersionedEntry::Vacant(VersionedVacantEntry {
            map: self.inc_lsm.memtable_mut(),
            op_id,
        })
    }

    fn entry_consensus(&mut self, op_id: OpId) -> VersionedEntry<'_, ConsensusEntry<CO, CR>> {
        if let Ok(Some(entry)) = self.con_lsm.get(&op_id) {
            return VersionedEntry::Occupied(entry);
        }
        VersionedEntry::Vacant(VersionedVacantEntry {
            map: self.con_lsm.memtable_mut(),
            op_id,
        })
    }

    fn get_mut_inconsistent(&mut self, op_id: &OpId) -> Option<&mut InconsistentEntry<IO>> {
        // Memtable only — tentative entries are always in the current view's memtable.
        self.inc_lsm.mem_get_mut(op_id)
    }

    fn get_mut_consensus(&mut self, op_id: &OpId) -> Option<&mut ConsensusEntry<CO, CR>> {
        self.con_lsm.mem_get_mut(op_id)
    }

    fn full_record(&self) -> Self::Record {
        let (inc, con) = self
            .all_segment_bytes()
            .expect("full_record: export_segment_bytes failed");
        let (inc_mem, con_mem) = self
            .memtable_bytes()
            .expect("full_record: memtable_bytes failed");
        let mut inc_segments = inc;
        if !inc_mem.is_empty() {
            inc_segments.push(inc_mem);
        }
        let mut con_segments = con;
        if !con_mem.is_empty() {
            con_segments.push(con_mem);
        }
        PersistentRecord::Raw {
            inc_segments,
            con_segments,
        }
    }

    fn inconsistent_len(&self) -> usize {
        // Memtable entries + indexed entries (approximate — may double-count promotions)
        self.inc_lsm.memtable_len()
    }

    fn consensus_len(&self) -> usize {
        self.con_lsm.memtable_len()
    }

    fn build_view_change_payload(&self, next_view: u64) -> Self::Payload {
        if self.base_view > 0 && self.base_view + 1 == next_view {
            // Delta: just the memtable entries from the current view
            let (inc_bytes, con_bytes) = self
                .memtable_bytes()
                .expect("build_view_change_payload: memtable_bytes failed");
            PersistentPayload::delta(ViewNumber(self.base_view), inc_bytes, con_bytes)
        } else {
            // Full: all segment bytes + memtable
            let (mut inc, mut con) = self
                .all_segment_bytes()
                .expect("build_view_change_payload: export failed");
            let (inc_mem, con_mem) = self
                .memtable_bytes()
                .expect("build_view_change_payload: memtable_bytes failed");
            if !inc_mem.is_empty() {
                inc.push(inc_mem);
            }
            if !con_mem.is_empty() {
                con.push(con_mem);
            }
            PersistentPayload::full(inc, con)
        }
    }

    fn build_start_view_payload(&self, delta: Option<&Self::Payload>) -> Self::Payload {
        delta.cloned().unwrap_or_else(|| {
            let (inc, con) = self
                .all_segment_bytes()
                .expect("build_start_view_payload: export failed");
            PersistentPayload::full(inc, con)
        })
    }

    fn make_full_payload(record: Self::Record) -> Self::Payload {
        match record {
            PersistentRecord::Raw {
                inc_segments,
                con_segments,
            } => PersistentPayload::full(inc_segments, con_segments),
            PersistentRecord::Indexed {
                inconsistent,
                consensus,
            } => {
                // Serialize Indexed entries to segment bytes
                let mut inc_bytes = Vec::new();
                for (op_id, entry) in &inconsistent {
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
                for (op_id, entry) in &consensus {
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
                    vec![inc_bytes]
                };
                let con = if con_bytes.is_empty() {
                    Vec::new()
                } else {
                    vec![con_bytes]
                };
                PersistentPayload::full(inc, con)
            }
        }
    }

    fn install_start_view_payload(
        &mut self,
        payload: Self::Payload,
        new_view: u64,
    ) -> Option<ViewInstallResult<Self::Record>> {
        // Validate delta base
        if let Some(bv) = payload.base_view()
            && (self.base_view == 0 || ViewNumber(self.base_view) != bv)
        {
            return None;
        }

        let previous_record = self.full_record();

        // Resolve payload to a full record
        let base = if self.base_view > 0 {
            let (inc, con) = self
                .all_segment_bytes()
                .expect("install_start_view: export failed");
            Some(PersistentRecord::Raw {
                inc_segments: inc,
                con_segments: con,
            })
        } else {
            None
        };
        let new_record = payload.resolve(base.as_ref());

        // Compute transition (CDC delta)
        // For Raw records, the transition is the full new record (caller extracts changes)
        let transition = (
            if self.base_view > 0 { self.base_view } else { 0 },
            new_record.clone_as_raw(),
        );

        // Install: clear everything, import new segments
        let is_full = payload_was_full(&new_record);
        if is_full {
            self.inc_lsm.clear_index();
            self.con_lsm.clear_index();
        }
        match &new_record {
            PersistentRecord::Raw {
                inc_segments,
                con_segments,
            } => {
                self.import_segments(inc_segments, con_segments)
                    .expect("install_start_view: import failed");
            }
            PersistentRecord::Indexed { .. } => {
                panic!("install_start_view_payload: expected Raw record");
            }
        }
        self.inc_lsm.clear_memtable();
        self.con_lsm.clear_memtable();
        self.base_view = new_view;
        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);

        Some(ViewInstallResult {
            previous_record,
            transition,
        })
    }

    fn install_merged_record(
        &mut self,
        merged: Self::Record,
        new_view: u64,
    ) -> MergeInstallResult<Self::Record, Self::Payload> {
        // Compute CDC transition
        let (transition, start_view_delta, previous_base_view) = if self.base_view > 0 {
            let (base_inc, base_con) = self
                .all_segment_bytes()
                .expect("install_merged: export failed");
            let _base: PersistentRecord<IO, CO, CR> = PersistentRecord::Raw {
                inc_segments: base_inc,
                con_segments: base_con,
            };

            // Encode merged into segment bytes for delta computation
            let (merged_inc, merged_con) =
                Self::encode_indexed_as_segments(&merged).expect("install_merged: encode failed");

            let delta_record = PersistentRecord::Raw {
                inc_segments: if merged_inc.is_empty() {
                    Vec::new()
                } else {
                    vec![merged_inc.clone()]
                },
                con_segments: if merged_con.is_empty() {
                    Vec::new()
                } else {
                    vec![merged_con.clone()]
                },
            };

            let prev_bv = ViewNumber(self.base_view);
            let delta_payload =
                PersistentPayload::delta(prev_bv, merged_inc, merged_con);

            (
                (self.base_view, delta_record),
                Some(delta_payload),
                Some(prev_bv),
            )
        } else {
            let (inc, con) =
                Self::encode_indexed_as_segments(&merged).expect("install_merged: encode failed");
            let full_record = PersistentRecord::Raw {
                inc_segments: if inc.is_empty() {
                    Vec::new()
                } else {
                    vec![inc.clone()]
                },
                con_segments: if con.is_empty() {
                    Vec::new()
                } else {
                    vec![con.clone()]
                },
            };
            ((0, full_record), None, None)
        };

        // Install: encode merged Indexed → segment bytes → import
        let (inc_bytes, con_bytes) =
            Self::encode_indexed_as_segments(&merged).expect("install_merged: encode failed");
        self.inc_lsm.clear_index();
        self.con_lsm.clear_index();
        if !inc_bytes.is_empty() {
            self.inc_lsm
                .import_raw_segment(&inc_bytes, op_id_from_raw)
                .expect("install_merged: import inc failed");
        }
        if !con_bytes.is_empty() {
            self.con_lsm
                .import_raw_segment(&con_bytes, op_id_from_raw)
                .expect("install_merged: import con failed");
        }
        self.inc_lsm.clear_memtable();
        self.con_lsm.clear_memtable();
        self.base_view = new_view;
        self.inc_lsm.start_view(new_view);
        self.con_lsm.start_view(new_view);

        MergeInstallResult {
            transition,
            start_view_delta,
            previous_base_view,
        }
    }

    fn resolve_do_view_change_payload(&self, payload: &Self::Payload) -> Self::Record {
        if let Some(bv) = payload.base_view() {
            assert!(
                self.base_view > 0 && ViewNumber(self.base_view) == bv,
                "Delta addendum base_view={bv:?} mismatches coordinator base={:?}",
                (self.base_view > 0).then_some(ViewNumber(self.base_view)),
            );
        }
        let base = if self.base_view > 0 {
            let (inc, con) = self
                .all_segment_bytes()
                .expect("resolve_do_view_change: export failed");
            Some(PersistentRecord::Raw {
                inc_segments: inc,
                con_segments: con,
            })
        } else {
            None
        };
        payload.clone().resolve(base.as_ref())
    }

    fn checkpoint_record(&self) -> Option<Self::Record> {
        if self.base_view > 0 {
            let (inc, con) = self
                .all_segment_bytes()
                .expect("checkpoint_record: export failed");
            Some(PersistentRecord::Raw {
                inc_segments: inc,
                con_segments: con,
            })
        } else {
            None
        }
    }
}

/// Check if a PersistentRecord was created from a Full payload (vs Delta).
fn payload_was_full<IO, CO, CR>(_record: &PersistentRecord<IO, CO, CR>) -> bool {
    // After resolve, we can't distinguish Full from Delta origin.
    // Conservatively treat all installs as Full (clear index before import).
    true
}

impl<IO: Clone, CO: Clone, CR: Clone> PersistentRecord<IO, CO, CR> {
    fn clone_as_raw(&self) -> Self {
        match self {
            Self::Raw {
                inc_segments,
                con_segments,
            } => Self::Raw {
                inc_segments: inc_segments.clone(),
                con_segments: con_segments.clone(),
            },
            Self::Indexed {
                inconsistent,
                consensus,
            } => Self::Indexed {
                inconsistent: inconsistent.clone(),
                consensus: consensus.clone(),
            },
        }
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
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(inc_entry("op1", 0));
            }
            VersionedEntry::Occupied(_) => panic!("expected vacant"),
        }
        assert_eq!(store.inconsistent_len(), 1);
    }

    #[test]
    fn entry_occupied_from_sealed() {
        let mut store = make_store();
        // Insert and finalize an entry
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_inc_entry("op1", 0));
            }
            _ => panic!("expected vacant"),
        }
        // Seal to move to vlog
        store.seal(1).unwrap();

        // Entry should be found as Occupied from sealed data
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "op1"),
            VersionedEntry::Vacant(_) => panic!("expected occupied from sealed"),
        }
    }

    #[test]
    fn get_mut_memtable_only() {
        let mut store = make_store();
        // No entry → None
        assert!(store.get_mut_inconsistent(&op_id(1, 1)).is_none());

        // Insert to memtable
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(inc_entry("op1", 0));
            }
            _ => panic!(),
        }
        // Can mutate
        let entry = store.get_mut_inconsistent(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(ViewNumber(1));
        assert!(store
            .get_mut_inconsistent(&op_id(1, 1))
            .unwrap()
            .state
            .is_finalized());
    }

    #[test]
    fn full_record_merges_memtable_and_sealed() {
        let mut store = make_store();
        // Insert and finalize an entry, then seal
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_inc_entry("op1", 0));
            }
            _ => panic!(),
        }
        store.seal(1).unwrap();

        // Add another entry to memtable (current view)
        match store.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(inc_entry("op2", 1));
            }
            _ => panic!(),
        }

        let record = store.full_record();
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn seal_discards_tentative() {
        let mut store = make_store();
        // Insert tentative entry
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(inc_entry("tentative", 0));
            }
            _ => panic!(),
        }
        // Insert finalized entry
        match store.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_inc_entry("finalized", 0));
            }
            _ => panic!(),
        }
        store.seal(1).unwrap();

        // Only finalized entry should survive in sealed data
        let record = store.full_record();
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.op, "finalized");
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
        let record: PersistentRecord<String, String, String> = PersistentRecord::Raw {
            inc_segments: vec![raw],
            con_segments: Vec::new(),
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
    fn build_view_change_payload_delta_vs_full() {
        let mut store = make_store();
        // Insert and seal to establish base
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_inc_entry("op1", 0));
            }
            _ => panic!(),
        }
        store.seal(1).unwrap();

        // Add overlay entry
        match store.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(inc_entry("op2", 1));
            }
            _ => panic!(),
        }

        // Delta: base_view=1, next_view=2
        let delta = store.build_view_change_payload(2);
        assert!(delta.base_view().is_some());
        assert_eq!(delta.base_view().unwrap(), ViewNumber(1));

        // Full: non-consecutive view
        let full = store.build_view_change_payload(5);
        assert!(full.base_view().is_none());
    }

    #[test]
    fn install_start_view_roundtrip() {
        let mut store = make_store();
        // Insert entries and seal
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_inc_entry("op1", 0));
            }
            _ => panic!(),
        }
        match store.entry_consensus(op_id(1, 2)) {
            VersionedEntry::Vacant(v) => {
                v.insert(fin_con_entry("cop1", "r1", 0));
            }
            _ => panic!(),
        }

        // Build full payload
        let payload = store.build_view_change_payload(1);

        // Create a new store and install
        let mut store2 = make_store();
        let result = store2.install_start_view_payload(payload, 1);
        assert!(result.is_some());

        // Verify entries survive
        match store2.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "op1"),
            _ => panic!("expected occupied after install"),
        }
        match store2.entry_consensus(op_id(1, 2)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "cop1"),
            _ => panic!("expected occupied after install"),
        }
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
        match store.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "op1"),
            _ => panic!("expected occupied"),
        }
    }

    #[test]
    fn persistent_payload_serde_round_trip() {
        let payload: PersistentPayload<String, String, String> =
            PersistentPayload::full(vec![vec![1, 2, 3]], vec![vec![4, 5]]);
        let bytes = bitcode::serialize(&payload).unwrap();
        let decoded: PersistentPayload<String, String, String> =
            bitcode::deserialize(&bytes).unwrap();
        assert!(format!("{:?}", decoded).contains("Full"));

        let delta: PersistentPayload<String, String, String> =
            PersistentPayload::delta(ViewNumber(5), vec![10, 20], vec![30]);
        let bytes2 = bitcode::serialize(&delta).unwrap();
        let decoded2: PersistentPayload<String, String, String> =
            bitcode::deserialize(&bytes2).unwrap();
        assert!(format!("{:?}", decoded2).contains("Delta"));
    }

    #[test]
    fn resolve_do_view_change_full() {
        let store = make_store();
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
        let payload = PersistentPayload::full(vec![raw], Vec::new());

        let record = store.resolve_do_view_change_payload(&payload);
        let entries: Vec<_> = record.inconsistent_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.op, "test");
    }
}
