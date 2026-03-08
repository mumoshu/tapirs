use super::{OpId, ReplicaUpcalls, ViewNumber};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

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

pub enum VersionedEntry<'a, V> {
    Vacant(VersionedVacantEntry<'a, V>),
    Occupied(V),
}

pub struct VersionedVacantEntry<'a, V> {
    pub(crate) map: &'a mut BTreeMap<OpId, V>,
    pub(crate) op_id: OpId,
}

impl<'a, V> VersionedVacantEntry<'a, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        self.map.entry(self.op_id).or_insert(value)
    }
}
