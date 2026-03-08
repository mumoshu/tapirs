use super::super::record::{ConsensusEntry, InconsistentEntry, RecordView};
use super::super::OpId;
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

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
