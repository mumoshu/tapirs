use super::{OpId, ReplicaUpcalls, ViewNumber};
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

/// The state of a record entry according to a replica.
#[derive(Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum State {
    /// Operation was applied at the replica in some view.
    Finalized(ViewNumber),
    /// Operation has not yet been applied at the replica.
    Tentative,
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finalized(view) => write!(f, "Fin({view:?})"),
            Self::Tentative => f.write_str("Tnt"),
        }
    }
}

impl State {
    pub fn is_tentative(&self) -> bool {
        matches!(self, Self::Tentative)
    }

    pub fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized(_))
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum Consistency {
    Inconsistent,
    Consensus,
}

impl Debug for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inconsistent => "Inc",
            Self::Consensus => "Con",
        })
    }
}

impl Consistency {
    #[allow(unused)]
    pub fn is_inconsistent(&self) -> bool {
        matches!(self, Self::Inconsistent)
    }

    #[allow(unused)]
    pub fn is_consensus(&self) -> bool {
        matches!(self, Self::Consensus)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InconsistentEntry<IO> {
    pub op: IO,
    pub state: State,
    /// View in which this entry was last inserted or modified.
    pub modified_view: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsensusEntry<CO, CR> {
    pub op: CO,
    pub result: CR,
    pub state: State,
    /// View in which this entry was last inserted or modified.
    pub modified_view: u64,
}

pub type Record<U> =
    RecordImpl<<U as ReplicaUpcalls>::IO, <U as ReplicaUpcalls>::CO, <U as ReplicaUpcalls>::CR>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordImpl<IO, CO, CR> {
    // BTreeMap for deterministic view change merge iteration. HashMap's RandomState
    // seeds from OS entropy, making iteration order vary per process. O(log n)
    // overhead is negligible for protocol operation counts. Wire format is unchanged
    // (vectorize_btree serializes as Vec<(K,V)> identical to vectorize).
    #[serde(
        with = "vectorize_btree",
        bound(serialize = "IO: Serialize", deserialize = "IO: Deserialize<'de>")
    )]
    pub inconsistent: BTreeMap<OpId, InconsistentEntry<IO>>,
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "CO: Serialize, CR: Serialize",
            deserialize = "CO: Deserialize<'de>, CR: Deserialize<'de>"
        )
    )]
    pub consensus: BTreeMap<OpId, ConsensusEntry<CO, CR>>,
}

/// Read-only view over IR record entries.
///
/// Abstracts away the concrete record storage structure so that
/// consumers (like [`super::replica::Upcalls`]) can iterate and
/// look up entries without depending on `RecordImpl`'s BTreeMap fields.
pub trait RecordView {
    type IO;
    type CO;
    type CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (&OpId, &ConsensusEntry<Self::CO, Self::CR>)>;
    fn inconsistent_entries(&self) -> impl Iterator<Item = (&OpId, &InconsistentEntry<Self::IO>)>;
    fn get_consensus(&self, op_id: &OpId) -> Option<&ConsensusEntry<Self::CO, Self::CR>>;
    fn get_inconsistent(&self, op_id: &OpId) -> Option<&InconsistentEntry<Self::IO>>;
}

impl<IO, CO, CR> RecordView for RecordImpl<IO, CO, CR> {
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (&OpId, &ConsensusEntry<CO, CR>)> {
        self.consensus.iter()
    }
    fn inconsistent_entries(&self) -> impl Iterator<Item = (&OpId, &InconsistentEntry<IO>)> {
        self.inconsistent.iter()
    }
    fn get_consensus(&self, op_id: &OpId) -> Option<&ConsensusEntry<CO, CR>> {
        self.consensus.get(op_id)
    }
    fn get_inconsistent(&self, op_id: &OpId) -> Option<&InconsistentEntry<IO>> {
        self.inconsistent.get(op_id)
    }
}

impl<IO, CO, CR> Default for RecordImpl<IO, CO, CR> {
    fn default() -> Self {
        Self {
            inconsistent: Default::default(),
            consensus: Default::default(),
        }
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordImpl<IO, CO, CR> {
    /// Returns entries modified at or after `since_view`.
    pub fn entries_since(&self, since_view: u64) -> Self {
        Self {
            inconsistent: self
                .inconsistent
                .iter()
                .filter(|(_, e)| e.modified_view >= since_view)
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
            consensus: self
                .consensus
                .iter()
                .filter(|(_, e)| e.modified_view >= since_view)
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
        }
    }

}

impl<IO: Clone + PartialEq, CO: Clone + PartialEq, CR: Clone + PartialEq> RecordImpl<IO, CO, CR> {
    /// Returns a record containing only entries in `self` that differ from `base`.
    pub fn delta_from(&self, base: &Self) -> Self {
        Self {
            inconsistent: self
                .inconsistent
                .iter()
                .filter(|(id, e)| base.inconsistent.get(id) != Some(e))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
            consensus: self
                .consensus
                .iter()
                .filter(|(id, e)| base.consensus.get(id) != Some(e))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
        }
    }
}

/// A two-layer record structure with base/overlay semantics.
///
/// Unifies three previously separate fields (`record`, `leader_record`, `delta_op_ids`)
/// into a single abstraction. The base holds the snapshot from the last view change;
/// the overlay accumulates changes during the current view.
///
/// - **Read**: check overlay first, fall back to base — O(log n)
/// - **Write**: always to overlay — O(log n), no full record clone
/// - **Delta**: the overlay IS the delta — O(1) access
/// - **Seal**: merge overlay into base on view change — O(overlay)
pub struct VersionedRecord<IO, CO, CR> {
    base: RecordImpl<IO, CO, CR>,
    base_view: u64,
    overlay: RecordImpl<IO, CO, CR>,
}

impl<IO: Debug, CO: Debug, CR: Debug> Debug for VersionedRecord<IO, CO, CR> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedRecord")
            .field("base_view", &self.base_view)
            .field("base_inc", &self.base.inconsistent.len())
            .field("base_con", &self.base.consensus.len())
            .field("overlay_inc", &self.overlay.inconsistent.len())
            .field("overlay_con", &self.overlay.consensus.len())
            .finish()
    }
}

impl<IO, CO, CR> Default for VersionedRecord<IO, CO, CR> {
    fn default() -> Self {
        Self {
            base: RecordImpl::default(),
            base_view: 0,
            overlay: RecordImpl::default(),
        }
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> VersionedRecord<IO, CO, CR> {
    /// Create from a full record (e.g. after coordinator merge or StartView resolve).
    /// The record becomes the base with an empty overlay.
    pub fn from_full(record: RecordImpl<IO, CO, CR>, view: u64) -> Self {
        Self {
            base: record,
            base_view: view,
            overlay: RecordImpl::default(),
        }
    }

    // --- Entry API (for Propose: insert-or-read) ---

    pub fn entry_inconsistent(&mut self, op_id: OpId) -> VersionedEntry<'_, InconsistentEntry<IO>> {
        if self.overlay.inconsistent.contains_key(&op_id) {
            VersionedEntry::Occupied(self.overlay.inconsistent.get(&op_id).unwrap())
        } else if self.base.inconsistent.contains_key(&op_id) {
            VersionedEntry::Occupied(self.base.inconsistent.get(&op_id).unwrap())
        } else {
            VersionedEntry::Vacant(VersionedVacantEntry {
                map: &mut self.overlay.inconsistent,
                op_id,
            })
        }
    }

    pub fn entry_consensus(&mut self, op_id: OpId) -> VersionedEntry<'_, ConsensusEntry<CO, CR>> {
        if self.overlay.consensus.contains_key(&op_id) {
            VersionedEntry::Occupied(self.overlay.consensus.get(&op_id).unwrap())
        } else if self.base.consensus.contains_key(&op_id) {
            VersionedEntry::Occupied(self.base.consensus.get(&op_id).unwrap())
        } else {
            VersionedEntry::Vacant(VersionedVacantEntry {
                map: &mut self.overlay.consensus,
                op_id,
            })
        }
    }

    // --- Mutable access (for Finalize: promote from base to overlay if needed) ---

    pub fn get_mut_inconsistent(&mut self, op_id: &OpId) -> Option<&mut InconsistentEntry<IO>> {
        if !self.overlay.inconsistent.contains_key(op_id)
            && let Some(entry) = self.base.inconsistent.get(op_id)
        {
            self.overlay.inconsistent.insert(*op_id, entry.clone());
        }
        self.overlay.inconsistent.get_mut(op_id)
    }

    pub fn get_mut_consensus(&mut self, op_id: &OpId) -> Option<&mut ConsensusEntry<CO, CR>> {
        if !self.overlay.consensus.contains_key(op_id)
            && let Some(entry) = self.base.consensus.get(op_id)
        {
            self.overlay.consensus.insert(*op_id, entry.clone());
        }
        self.overlay.consensus.get_mut(op_id)
    }

    // --- Delta extraction ---

    /// Returns a clone of the overlay.
    pub fn overlay_clone(&self) -> RecordImpl<IO, CO, CR> {
        self.overlay.clone()
    }

    // --- Full record ---

    /// Returns the merged base ∪ overlay record.
    pub fn full(&self) -> RecordImpl<IO, CO, CR> {
        let mut full = self.base.clone();
        for (op_id, entry) in &self.overlay.inconsistent {
            full.inconsistent.insert(*op_id, entry.clone());
        }
        for (op_id, entry) in &self.overlay.consensus {
            full.consensus.insert(*op_id, entry.clone());
        }
        full
    }

    // --- Base access ---

    pub fn base(&self) -> &RecordImpl<IO, CO, CR> {
        &self.base
    }

    pub fn base_view(&self) -> u64 {
        self.base_view
    }

    /// Returns true if a base has been established (view change has occurred).
    pub fn has_base(&self) -> bool {
        self.base_view > 0
    }

    // --- Size ---

    /// Total number of unique inconsistent entries across base and overlay.
    pub fn inconsistent_len(&self) -> usize {
        let overridden = self.overlay.inconsistent.keys()
            .filter(|k| self.base.inconsistent.contains_key(k))
            .count();
        self.base.inconsistent.len() - overridden + self.overlay.inconsistent.len()
    }

    /// Total number of unique consensus entries across base and overlay.
    pub fn consensus_len(&self) -> usize {
        let overridden = self.overlay.consensus.keys()
            .filter(|k| self.base.consensus.contains_key(k))
            .count();
        self.base.consensus.len() - overridden + self.overlay.consensus.len()
    }
}

/// Abstracts the IR record store, allowing alternative backends
/// (e.g., LSM-backed, unified store spanning IR and TAPIR).
///
/// The IR replica uses this trait for all record operations: entry
/// lookup/insert, snapshot extraction for view change messages,
/// and lifecycle management (install from view change resolution).
///
/// The default implementation is [`VersionedRecord`], which uses
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

impl<IO: Clone, CO: Clone, CR: Clone> IrRecordStore<IO, CO, CR> for VersionedRecord<IO, CO, CR>
where
    IO: Debug + Send + 'static,
    CO: Debug + Send + 'static,
    CR: Debug + Send + 'static,
{
    fn entry_inconsistent(&mut self, op_id: OpId) -> VersionedEntry<'_, InconsistentEntry<IO>> {
        self.entry_inconsistent(op_id)
    }

    fn entry_consensus(&mut self, op_id: OpId) -> VersionedEntry<'_, ConsensusEntry<CO, CR>> {
        self.entry_consensus(op_id)
    }

    fn get_mut_inconsistent(&mut self, op_id: &OpId) -> Option<&mut InconsistentEntry<IO>> {
        self.get_mut_inconsistent(op_id)
    }

    fn get_mut_consensus(&mut self, op_id: &OpId) -> Option<&mut ConsensusEntry<CO, CR>> {
        self.get_mut_consensus(op_id)
    }

    fn current_view_delta(&self) -> RecordImpl<IO, CO, CR> {
        self.overlay_clone()
    }

    fn full_record(&self) -> RecordImpl<IO, CO, CR> {
        self.full()
    }

    fn has_sealed_view(&self) -> bool {
        self.has_base()
    }

    fn sealed_view_number(&self) -> u64 {
        self.base_view()
    }

    fn sealed_record(&self) -> &RecordImpl<IO, CO, CR> {
        self.base()
    }

    fn install(record: RecordImpl<IO, CO, CR>, view: u64) -> Self {
        Self::from_full(record, view)
    }

    fn inconsistent_len(&self) -> usize {
        self.inconsistent_len()
    }

    fn consensus_len(&self) -> usize {
        self.consensus_len()
    }
}

pub enum VersionedEntry<'a, V> {
    Vacant(VersionedVacantEntry<'a, V>),
    Occupied(&'a V),
}

pub struct VersionedVacantEntry<'a, V> {
    map: &'a mut BTreeMap<OpId, V>,
    op_id: OpId,
}

impl<'a, V> VersionedVacantEntry<'a, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        self.map.entry(self.op_id).or_insert(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ClientId;

    fn op_id(client: u64, num: u64) -> OpId {
        OpId { client_id: ClientId(client), number: num }
    }

    fn inc_entry(op: &str, view: u64) -> InconsistentEntry<String> {
        InconsistentEntry {
            op: op.to_string(),
            state: State::Tentative,
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

    type R = RecordImpl<String, String, String>;

    #[test]
    fn entry_insert_into_empty() {
        let mut vr: VersionedRecord<String, String, String> = VersionedRecord::default();
        match vr.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op1", 0)); }
            VersionedEntry::Occupied(_) => panic!("expected vacant"),
        }
        assert_eq!(vr.inconsistent_len(), 1);
        assert_eq!(vr.overlay_clone().inconsistent.len(), 1);
        assert_eq!(vr.base().inconsistent.len(), 0);
    }

    #[test]
    fn entry_occupied_from_base() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        match vr.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "op1"),
            VersionedEntry::Vacant(_) => panic!("expected occupied"),
        }
        // No promotion to overlay — just reading
        assert_eq!(vr.overlay_clone().inconsistent.len(), 0);
    }

    #[test]
    fn entry_occupied_from_overlay() {
        let mut vr: VersionedRecord<String, String, String> = VersionedRecord::default();
        match vr.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op1", 0)); }
            _ => panic!("expected vacant"),
        }
        match vr.entry_inconsistent(op_id(1, 1)) {
            VersionedEntry::Occupied(e) => assert_eq!(e.op, "op1"),
            VersionedEntry::Vacant(_) => panic!("expected occupied"),
        }
    }

    #[test]
    fn get_mut_promotes_from_base() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        let entry = vr.get_mut_inconsistent(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(super::super::ViewNumber(2));
        entry.modified_view = 2;

        // Entry promoted to overlay
        assert_eq!(vr.overlay_clone().inconsistent.len(), 1);
        assert!(vr.overlay_clone().inconsistent.get(&op_id(1, 1)).unwrap().state.is_finalized());
        // Base unchanged
        assert!(vr.base().inconsistent.get(&op_id(1, 1)).unwrap().state.is_tentative());
    }

    #[test]
    fn get_mut_absent_returns_none() {
        let mut vr: VersionedRecord<String, String, String> = VersionedRecord::default();
        assert!(vr.get_mut_inconsistent(&op_id(1, 1)).is_none());
        assert!(vr.get_mut_consensus(&op_id(1, 1)).is_none());
    }

    #[test]
    fn full_merges_base_and_overlay() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        base.consensus.insert(op_id(1, 2), con_entry("cop1", "r1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        // Add new entry to overlay
        match vr.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op2", 2)); }
            _ => panic!("expected vacant"),
        }

        let full = vr.full();
        assert_eq!(full.inconsistent.len(), 2);
        assert_eq!(full.consensus.len(), 1);
    }

    #[test]
    fn full_overlay_overwrites_base() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        // Promote and modify
        let entry = vr.get_mut_inconsistent(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(super::super::ViewNumber(2));

        let full = vr.full();
        assert!(full.inconsistent.get(&op_id(1, 1)).unwrap().state.is_finalized());
    }

    #[test]
    fn overlay_clone_is_delta() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        // Modify existing (promote) + add new
        let entry = vr.get_mut_inconsistent(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(super::super::ViewNumber(2));
        match vr.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op2", 2)); }
            _ => panic!("expected vacant"),
        }

        let delta = vr.overlay_clone();
        assert_eq!(delta.inconsistent.len(), 2); // promoted + new
    }

    #[test]
    fn seal_merges_overlay_into_base() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        // Add to overlay
        match vr.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op2", 2)); }
            _ => panic!("expected vacant"),
        }
        // Promote + modify
        let entry = vr.get_mut_inconsistent(&op_id(1, 1)).unwrap();
        entry.state = State::Finalized(super::super::ViewNumber(2));

        // Inline seal: merge overlay into base
        for (op_id, entry) in std::mem::take(&mut vr.overlay.inconsistent) {
            vr.base.inconsistent.insert(op_id, entry);
        }
        for (op_id, entry) in std::mem::take(&mut vr.overlay.consensus) {
            vr.base.consensus.insert(op_id, entry);
        }
        vr.base_view = 2;

        assert_eq!(vr.base_view(), 2);
        assert!(vr.overlay_clone().inconsistent.is_empty());
        assert!(vr.overlay_clone().consensus.is_empty());
        assert_eq!(vr.base().inconsistent.len(), 2);
        // Modified entry in base reflects the overlay update
        assert!(vr.base().inconsistent.get(&op_id(1, 1)).unwrap().state.is_finalized());
    }

    #[test]
    fn len_accounts_for_overlaps() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        base.inconsistent.insert(op_id(1, 2), inc_entry("op2", 0));
        let mut vr = VersionedRecord::from_full(base, 1);

        // Promote one entry (now in both base and overlay)
        vr.get_mut_inconsistent(&op_id(1, 1));
        // Add a new entry
        match vr.entry_inconsistent(op_id(2, 1)) {
            VersionedEntry::Vacant(v) => { v.insert(inc_entry("op3", 2)); }
            _ => panic!("expected vacant"),
        }

        // base: {(1,1), (1,2)}, overlay: {(1,1), (2,1)}
        // unique: {(1,1), (1,2), (2,1)} = 3
        assert_eq!(vr.inconsistent_len(), 3);
    }

    #[test]
    fn consensus_entry_api() {
        let mut vr: VersionedRecord<String, String, String> = VersionedRecord::default();

        // Insert via entry API
        match vr.entry_consensus(op_id(1, 1)) {
            VersionedEntry::Vacant(v) => {
                let e = v.insert(con_entry("cop1", "r1", 0));
                assert_eq!(e.result, "r1");
            }
            _ => panic!("expected vacant"),
        }

        // Modify via get_mut
        let entry = vr.get_mut_consensus(&op_id(1, 1)).unwrap();
        entry.result = "r2".to_string();
        entry.state = State::Finalized(super::super::ViewNumber(1));

        assert_eq!(vr.overlay_clone().consensus.get(&op_id(1, 1)).unwrap().result, "r2");
    }

    #[test]
    fn has_base_reflects_state() {
        let vr: VersionedRecord<String, String, String> = VersionedRecord::default();
        assert!(!vr.has_base());

        let base = R::default();
        let vr = VersionedRecord::from_full(base, 1);
        assert!(vr.has_base());
    }

    #[test]
    fn from_full_empty_overlay() {
        let mut base = R::default();
        base.inconsistent.insert(op_id(1, 1), inc_entry("op1", 0));
        let vr = VersionedRecord::from_full(base, 5);

        assert_eq!(vr.base_view(), 5);
        assert!(vr.overlay_clone().inconsistent.is_empty());
        assert_eq!(vr.inconsistent_len(), 1);
    }
}
