use super::record::RecordImpl;
use super::super::ViewNumber;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RecordPayload<IO, CO, CR> {
    Full(RecordImpl<IO, CO, CR>),
    Delta {
        base_view: ViewNumber,
        entries: RecordImpl<IO, CO, CR>,
    },
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordPayload<IO, CO, CR> {
    pub fn resolve(self, base: Option<&RecordImpl<IO, CO, CR>>) -> RecordImpl<IO, CO, CR> {
        match self {
            Self::Full(record) => record,
            Self::Delta { entries, .. } => {
                let base = base.expect("delta requires matching base");
                let mut full = base.clone();
                // insert (overwrite), not or_insert: delta entries may be updates
                // to existing entries (FinalizeConsensus changes result + state).
                for (op_id, entry) in entries.inconsistent {
                    full.inconsistent.insert(op_id, entry);
                }
                for (op_id, entry) in entries.consensus {
                    full.consensus.insert(op_id, entry);
                }
                full
            }
        }
    }
}
