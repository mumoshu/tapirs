use super::{OpId, ReplicaUpcalls, ViewNumber};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub use super::inmem::record::RecordImpl;

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

/// Iteration over IR record entries.
///
/// Provides sequential access to consensus and inconsistent entries
/// without requiring indexed lookups. Implemented by both indexed
/// records (BTreeMap-backed) and raw records (vlog byte scans).
pub trait RecordIter {
    type IO;
    type CO;
    type CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<Self::CO, Self::CR>)>;
    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<Self::IO>)>;
}

/// Read-only view over IR record entries with O(log N) point lookups.
///
/// Extends [`RecordIter`] with indexed access. Implemented by types
/// backed by BTreeMaps or LSM indexes — not by raw vlog records.
pub trait RecordView: RecordIter {
    fn get_consensus(&self, op_id: &OpId) -> Option<ConsensusEntry<Self::CO, Self::CR>>;
    fn get_inconsistent(&self, op_id: &OpId) -> Option<InconsistentEntry<Self::IO>>;
}

/// Trait for building a record by inserting entries.
///
/// Used by the leader during merge to construct the merged record.
/// No `get_mut` — use `get` (owned) → modify → `insert` (overwrite).
pub trait RecordBuilder: RecordView {
    fn insert_inconsistent(&mut self, op_id: OpId, entry: InconsistentEntry<Self::IO>);
    fn insert_consensus(&mut self, op_id: OpId, entry: ConsensusEntry<Self::CO, Self::CR>);
}

