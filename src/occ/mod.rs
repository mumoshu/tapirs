mod timestamp;
mod transaction;

pub use timestamp::Timestamp;
pub use transaction::{Id as TransactionId, ScanEntry, SharedTransaction, Transaction};

use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum PrepareResult<TS: Timestamp> {
    /// The transaction is possible.
    Ok,
    /// There was a conflict that might be resolved by retrying prepare at a different timestamp.
    Retry { proposed: TS::Time },
    /// There was a conflict with a prepared transaction (which may later abort).
    Abstain,
    /// There was a conflict with a committed transaction.
    Fail,
    /// It is too late to prepare with this commit timestamp.
    TooLate,
    /// The commit time is too old (would be or was already garbage collected).
    ///
    /// It isn't known whether such transactions were prepared, committed, or aborted.
    /// - Clients can safely hang i.e. while polling more replicas, or return an
    ///   indeterminate result.
    /// - Backup coordinators can safely give up (transaction guaranteed to have
    ///   committed or aborted already).
    /// - Merging replicas can safely self-destruct (TODO: is there a better option?)
    TooOld,
    /// The transaction touches keys outside this shard's current key range.
    OutOfRange,
}

/// Error from `do_committed_get` / `do_committed_scan`.
#[derive(Debug)]
pub enum CommittedReadError {
    /// A prepared-but-uncommitted write conflicts with this read.
    PrepareConflict,
    /// Internal storage error (e.g. missing IR inc segment, corrupt data).
    StorageError(crate::storage::io::StorageError),
}

impl<TS: Timestamp> PrepareResult<TS> {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Fail)
    }

    pub fn is_abstain(&self) -> bool {
        matches!(self, Self::Abstain)
    }

    pub fn is_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    pub fn is_too_late(&self) -> bool {
        matches!(self, Self::TooLate)
    }

    pub fn is_too_old(&self) -> bool {
        matches!(self, Self::TooOld)
    }

    pub fn is_out_of_range(&self) -> bool {
        matches!(self, Self::OutOfRange)
    }
}
