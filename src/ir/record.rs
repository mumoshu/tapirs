use super::{OpId, ReplicaUpcalls, ViewNumber};
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::{collections::{BTreeMap, HashSet}, fmt::Debug};

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsensusEntry<CO, CR> {
    pub op: CO,
    pub result: CR,
    pub state: State,
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

impl<IO, CO, CR> Default for RecordImpl<IO, CO, CR> {
    fn default() -> Self {
        Self {
            inconsistent: Default::default(),
            consensus: Default::default(),
        }
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordImpl<IO, CO, CR> {
    pub fn filter_by_op_ids(&self, op_ids: &HashSet<OpId>) -> Self {
        Self {
            inconsistent: self
                .inconsistent
                .iter()
                .filter(|(id, _)| op_ids.contains(id))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
            consensus: self
                .consensus
                .iter()
                .filter(|(id, _)| op_ids.contains(id))
                .map(|(id, e)| (*id, e.clone()))
                .collect(),
        }
    }
}

impl<IO: Clone + PartialEq, CO: Clone + PartialEq, CR: Clone + PartialEq> RecordImpl<IO, CO, CR> {
    /// Returns a record containing only entries in `self` that differ from `base`.
    pub fn delta_from(&self, base: &Self) -> Self {
        let mut delta_ids = HashSet::new();
        for id in self.inconsistent.keys() {
            if base.inconsistent.get(id) != self.inconsistent.get(id) {
                delta_ids.insert(*id);
            }
        }
        for id in self.consensus.keys() {
            if base.consensus.get(id) != self.consensus.get(id) {
                delta_ids.insert(*id);
            }
        }
        self.filter_by_op_ids(&delta_ids)
    }
}
