use crate::ir::OpId;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::mvcc_backend::UnifiedMvccBackend;
use crate::unified::UnifiedStore;
use crate::IrClientId;

pub type TestUnifiedStore = UnifiedStore<String, MemoryIo>;
pub type TestBackend = UnifiedMvccBackend<String, String, MemoryIo>;

/// Create a fresh UnifiedStore at view 0 with MemoryIo.
pub fn new_test_store() -> TestUnifiedStore {
    TestUnifiedStore::open(MemoryIo::temp_path()).unwrap()
}

/// Create a fresh UnifiedMvccBackend.
pub fn new_test_backend() -> TestBackend {
    TestBackend::new(new_test_store())
}

/// Create a Timestamp from a time value (client_id=1 by default).
pub fn test_ts(time: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(1),
    }
}

/// Create a TransactionId.
pub fn test_txn_id(client: u64, num: u64) -> OccTransactionId {
    OccTransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

/// Create an OpId.
pub fn test_op_id(client: u64, num: u64) -> OpId {
    OpId {
        client_id: IrClientId(client),
        number: num,
    }
}
