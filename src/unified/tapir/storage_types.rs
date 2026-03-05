use crate::occ::TransactionId as OccTransactionId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmEntry {
    pub txn_id: OccTransactionId,
    pub write_index: u16,
    pub last_read_ts: Option<u64>,
}
