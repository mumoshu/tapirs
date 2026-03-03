use crate::occ::TransactionId as OccTransactionId;
use crate::unified::wisckeylsm::types::VlogPtr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VlogTransactionPtr {
    pub txn_ptr: VlogPtr,
    pub write_index: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueLocation {
    InMemory {
        txn_id: OccTransactionId,
        write_index: u16,
    },
    OnDisk(VlogTransactionPtr),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmEntry {
    pub value_ref: Option<ValueLocation>,
    pub last_read_ts: Option<u64>,
}
