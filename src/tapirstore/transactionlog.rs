use crate::occ::TransactionId;
use crate::tapir::Timestamp;
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Tracks the final outcome (committed or aborted) of each transaction.
///
/// Each entry maps a `TransactionId` to `(Timestamp, bool)` where
/// `bool = true` means committed at the given timestamp and
/// `bool = false` means aborted (timestamp is `Timestamp::default()`).
#[derive(Serialize, Deserialize)]
pub struct TransactionLog {
    #[serde(with = "vectorize_btree")]
    inner: BTreeMap<TransactionId, (Timestamp, bool)>,
}

impl TransactionLog {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)> {
        self.inner.get(id).copied()
    }

    pub fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        self.inner.insert(id, (ts, committed))
    }

    pub fn txn_log_contains(&self, id: &TransactionId) -> bool {
        self.inner.contains_key(id)
    }

    pub fn txn_log_len(&self) -> usize {
        self.inner.len()
    }
}
