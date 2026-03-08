use super::super::record::{ConsensusEntry, InconsistentEntry, RecordBuilder, RecordView};
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

impl<IO: Clone, CO: Clone, CR: Clone> RecordView for RecordImpl<IO, CO, CR> {
    type IO = IO;
    type CO = CO;
    type CR = CR;

    fn consensus_entries(&self) -> impl Iterator<Item = (OpId, ConsensusEntry<CO, CR>)> {
        self.consensus.iter().map(|(k, v)| (*k, v.clone()))
    }
    fn inconsistent_entries(&self) -> impl Iterator<Item = (OpId, InconsistentEntry<IO>)> {
        self.inconsistent.iter().map(|(k, v)| (*k, v.clone()))
    }
    fn get_consensus(&self, op_id: &OpId) -> Option<ConsensusEntry<CO, CR>> {
        self.consensus.get(op_id).cloned()
    }
    fn get_inconsistent(&self, op_id: &OpId) -> Option<InconsistentEntry<IO>> {
        self.inconsistent.get(op_id).cloned()
    }
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordBuilder for RecordImpl<IO, CO, CR> {
    fn insert_inconsistent(&mut self, op_id: OpId, entry: InconsistentEntry<Self::IO>) {
        self.inconsistent.insert(op_id, entry);
    }
    fn insert_consensus(&mut self, op_id: OpId, entry: ConsensusEntry<Self::CO, Self::CR>) {
        self.consensus.insert(op_id, entry);
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
