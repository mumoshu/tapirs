use crate::ir::OpId;
use crate::occ::TransactionId;
use crate::tapir::Timestamp;
use crate::IrClientId;

pub fn test_ts(time: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(1),
    }
}

pub fn test_txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

pub fn test_op_id(client: u64, num: u64) -> OpId {
    OpId {
        client_id: IrClientId(client),
        number: num,
    }
}
