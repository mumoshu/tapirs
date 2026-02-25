#![allow(dead_code)]

/// A single operation within a transaction.
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Scan { start: String, end: String },
}

/// A complete transaction's operations.
pub struct TxnOps {
    pub read_only: bool,
    pub ops: Vec<Op>,
}
