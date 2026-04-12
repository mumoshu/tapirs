/// A single operation within a transaction.
#[derive(Clone)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Scan { start: String, end: String },
}

/// A complete transaction's operations.
#[derive(Clone)]
pub struct TxnOps {
    pub read_only: bool,
    pub ops: Vec<Op>,
}
