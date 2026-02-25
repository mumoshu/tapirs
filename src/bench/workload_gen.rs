#![allow(dead_code)]

use super::ops::{Op, TxnOps};
use super::WorkloadType;

pub fn key(i: usize) -> String {
    format!("{:06}", i)
}

/// Lazy iterator yielding transaction operations from forked RNG.
pub struct WorkloadGen {
    rng: crate::Rng,
    workload_type: WorkloadType,
    key_space_size: usize,
}

impl WorkloadGen {
    pub fn new(rng: crate::Rng, workload_type: WorkloadType, key_space_size: usize) -> Self {
        Self { rng, workload_type, key_space_size }
    }
}

impl Iterator for WorkloadGen {
    type Item = TxnOps;

    fn next(&mut self) -> Option<TxnOps> {
        Some(match &self.workload_type {
            WorkloadType::ReadWrite { reads_per_txn, writes_per_txn } => {
                let mut ops = Vec::new();
                for _ in 0..*reads_per_txn {
                    ops.push(Op::Get { key: key(self.rng.random_index(self.key_space_size)) });
                }
                for _ in 0..*writes_per_txn {
                    ops.push(Op::Put {
                        key: key(self.rng.random_index(self.key_space_size)),
                        value: format!("{}", self.rng.random_u64()),
                    });
                }
                TxnOps { read_only: false, ops }
            }
            WorkloadType::ReadOnlyGet { reads_per_txn } => {
                let mut ops = Vec::new();
                for _ in 0..*reads_per_txn {
                    ops.push(Op::Get { key: key(self.rng.random_index(self.key_space_size)) });
                }
                TxnOps { read_only: true, ops }
            }
            WorkloadType::ReadOnlyScan { scan_range_size } => {
                let max_start = self.key_space_size.saturating_sub(*scan_range_size);
                let start = if max_start > 0 { self.rng.random_index(max_start) } else { 0 };
                let end = (start + scan_range_size).min(self.key_space_size);
                TxnOps {
                    read_only: true,
                    ops: vec![Op::Scan { start: key(start), end: key(end) }],
                }
            }
        })
    }
}

/// Generate prepopulation TxnOps batched by BENCH_PREPOPULATE_BATCH_SIZE.
pub fn prepopulate_ops(key_space_size: usize) -> Vec<TxnOps> {
    let batch_size: usize = std::env::var("BENCH_PREPOPULATE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let mut batches = Vec::new();
    let mut i = 0;
    while i < key_space_size {
        let end = (i + batch_size).min(key_space_size);
        let mut ops = Vec::new();
        for j in i..end {
            ops.push(Op::Get { key: key(j) });
            ops.push(Op::Put { key: key(j), value: format!("init_{j}") });
        }
        batches.push(TxnOps { read_only: false, ops });
        i = end;
    }
    batches
}
