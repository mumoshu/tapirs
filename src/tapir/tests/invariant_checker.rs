//! Invariant checker for TAPIR transaction serializability.
//!
//! Data items are identified by **logical key only**, not (shard, key).
//! The checker verifies that committed results are consistent with some
//! serial order — this is a property of logical operations on logical
//! data items, independent of physical shard placement.
//!
//! Using logical keys is also more robust: if a routing bug maps the same
//! key to two different shards, (shard, key) tracking would treat them as
//! separate items and miss the conflict; logical key tracking catches it.

// HashMap used for lookup-only tracking data (order-independent per-key processing).
#![allow(dead_code, clippy::disallowed_types)]

use crate::TapirTimestamp;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOutcome {
    Committed(TapirTimestamp),
    Aborted,
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct TxnRecord {
    pub index: usize,
    pub client_id: usize,
    /// Keys read and the values observed (None = key absent).
    pub read_set: Vec<(i64, Option<i64>)>,
    /// Keys written and the values written.
    pub write_set: Vec<(i64, i64)>,
    pub outcome: TxnOutcome,
    /// Simulated time when the transaction began.
    pub wall_start: tokio::time::Instant,
    /// Simulated time when commit/abort returned.
    pub wall_end: tokio::time::Instant,
}

// ---------------------------------------------------------------------------
// Dependency graph
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum DepType {
    /// Write-Read: `from` wrote a value that `to` read.
    WR,
    /// Read-Write (anti-dependency): `from` read a version overwritten by `to`.
    RW,
    /// Write-Write: both wrote the same key; `from` committed earlier.
    WW,
}

struct DepGraph {
    /// adjacency[node] = outgoing edges as (target_node, dep_type).
    adjacency: Vec<Vec<(usize, DepType)>>,
    /// Graph node index -> TxnRecord index.
    node_to_record: Vec<usize>,
    /// TxnRecord index -> graph node index (committed txns only).
    record_to_node: HashMap<usize, usize>,
}

// ---------------------------------------------------------------------------
// InvariantChecker
// ---------------------------------------------------------------------------

pub struct InvariantChecker {
    records: Vec<TxnRecord>,
    seed: u64,
}

impl InvariantChecker {
    pub fn new(records: Vec<TxnRecord>, seed: u64) -> Self {
        Self { records, seed }
    }

    /// Run all invariant checks. Panics with a descriptive message on failure.
    pub fn check_all(&self) {
        self.check_no_duplicate_outcomes();
        let graph = self.build_dependency_graph();
        self.check_serializability(&graph);
        self.check_strict_serializability(&graph);
    }

    /// Return expected committed increment counts per key, so the fuzz test
    /// can compare against actual reads.
    pub fn expected_counts(&self) -> BTreeMap<i64, i64> {
        let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
        for rec in &self.records {
            if !matches!(rec.outcome, TxnOutcome::Committed(_)) {
                continue;
            }
            for &(key, _) in &rec.write_set {
                *counts.entry(key).or_default() += 1;
            }
        }
        counts
    }

    // ------------------------------------------------------------------
    // Dependency graph construction
    // ------------------------------------------------------------------

    fn build_dependency_graph(&self) -> DepGraph {
        let committed: Vec<&TxnRecord> = self
            .records
            .iter()
            .filter(|r| matches!(r.outcome, TxnOutcome::Committed(_)))
            .collect();

        // Assign graph node indices to committed transactions.
        let mut record_to_node: HashMap<usize, usize> = HashMap::new();
        let mut node_to_record: Vec<usize> = Vec::new();
        for (i, rec) in committed.iter().enumerate() {
            record_to_node.insert(rec.index, i);
            node_to_record.push(rec.index);
        }

        let n = committed.len();
        let mut adjacency: Vec<Vec<(usize, DepType)>> = vec![Vec::new(); n];

        // Per-key write history: key -> Vec<(commit_ts, written_value, txn_index)>
        // sorted by commit_ts.
        let mut write_history: HashMap<i64, Vec<(TapirTimestamp, i64, usize)>> =
            HashMap::new();
        for rec in &committed {
            let ts = match rec.outcome {
                TxnOutcome::Committed(ts) => ts,
                _ => unreachable!(),
            };
            for &(key, value) in &rec.write_set {
                write_history.entry(key).or_default().push((ts, value, rec.index));
            }
        }
        #[allow(clippy::disallowed_methods, clippy::iter_over_hash_type)] // Order-independent per-key processing
        for history in write_history.values_mut() {
            history.sort_by_key(|(ts, _, _)| *ts);
        }

        // WW edges: consecutive writers to the same key.
        #[allow(clippy::disallowed_methods, clippy::iter_over_hash_type)] // Order-independent per-key processing
        for history in write_history.values() {
            for window in history.windows(2) {
                let from_rec = window[0].2;
                let to_rec = window[1].2;
                if let (Some(&from_node), Some(&to_node)) =
                    (record_to_node.get(&from_rec), record_to_node.get(&to_rec))
                {
                    adjacency[from_node].push((to_node, DepType::WW));
                }
            }
        }

        // WR and RW edges.
        for rec in &committed {
            let reader_ts = match rec.outcome {
                TxnOutcome::Committed(ts) => ts,
                _ => unreachable!(),
            };
            let reader_node = record_to_node[&rec.index];

            for &(key, observed_value) in &rec.read_set {
                if let Some(history) = write_history.get(&key) {
                    // Find the writer whose value matches what we observed and
                    // whose commit_ts <= reader_ts (latest such writer).
                    let writer_pos = history
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(_, (ts, val, _))| *ts <= reader_ts && Some(*val) == observed_value)
                        .map(|(pos, _)| pos);

                    if let Some(w_pos) = writer_pos {
                        let writer_rec_idx = history[w_pos].2;
                        if writer_rec_idx != rec.index {
                            // WR edge: writer -> reader.
                            let writer_node = record_to_node[&writer_rec_idx];
                            adjacency[writer_node].push((reader_node, DepType::WR));
                        }

                        // RW edge: if there is a next writer after w_pos.
                        if w_pos + 1 < history.len() {
                            let next_writer_rec_idx = history[w_pos + 1].2;
                            if next_writer_rec_idx != rec.index {
                                let next_writer_node = record_to_node[&next_writer_rec_idx];
                                adjacency[reader_node].push((next_writer_node, DepType::RW));
                            }
                        }
                    } else if observed_value.is_none() {
                        // Read None (initial state). RW edge to the first
                        // writer of this key, if any.
                        if let Some(first) = history.first() {
                            let first_writer_node = record_to_node[&first.2];
                            if first_writer_node != reader_node {
                                adjacency[reader_node].push((first_writer_node, DepType::RW));
                            }
                        }
                    } else {
                        panic!(
                            "Transaction {} read {:?} for key {:?} but no matching \
                             committed writer exists (seed={})",
                            rec.index, observed_value, key, self.seed
                        );
                    }
                } else if observed_value.is_some() {
                    panic!(
                        "Transaction {} read {:?} for key {:?} but no committed \
                         writer exists (seed={})",
                        rec.index, observed_value, key, self.seed
                    );
                }
                // observed_value == None and no write_history → initial empty state, OK.
            }
        }

        DepGraph {
            adjacency,
            node_to_record,
            record_to_node,
        }
    }

    // ------------------------------------------------------------------
    // Serializability: dependency graph must be acyclic
    // ------------------------------------------------------------------

    fn check_serializability(&self, graph: &DepGraph) {
        if let Some(cycle) = find_cycle(&graph.adjacency) {
            let txn_indices: Vec<usize> =
                cycle.iter().map(|&n| graph.node_to_record[n]).collect();
            let details = self.format_cycle_details(graph, &cycle);
            panic!(
                "Serializability violation: dependency cycle among transactions \
                 {:?} (seed={})\n{}",
                txn_indices, self.seed, details
            );
        }
    }

    // ------------------------------------------------------------------
    // Strict serializability: real-time ordering must be consistent
    // ------------------------------------------------------------------

    fn check_strict_serializability(&self, graph: &DepGraph) {
        let committed: Vec<&TxnRecord> = self
            .records
            .iter()
            .filter(|r| matches!(r.outcome, TxnOutcome::Committed(_)))
            .collect();

        // Clone adjacency and add real-time precedence edges.
        let mut adj = graph.adjacency.clone();

        for a in &committed {
            for b in &committed {
                if a.index == b.index {
                    continue;
                }
                // If A completed before B started, A must precede B.
                if a.wall_end < b.wall_start {
                    let a_node = graph.record_to_node[&a.index];
                    let b_node = graph.record_to_node[&b.index];
                    adj[a_node].push((b_node, DepType::WR)); // type is placeholder
                }
            }
        }

        if let Some(cycle) = find_cycle(&adj) {
            let txn_indices: Vec<usize> =
                cycle.iter().map(|&n| graph.node_to_record[n]).collect();
            panic!(
                "Strict serializability violation: real-time ordering creates \
                 cycle among transactions {:?} (seed={})",
                txn_indices, self.seed
            );
        }
    }

    // ------------------------------------------------------------------
    // Sanity: no duplicate outcomes
    // ------------------------------------------------------------------

    fn check_no_duplicate_outcomes(&self) {
        let mut outcomes: HashMap<usize, TxnOutcome> = HashMap::new();
        for rec in &self.records {
            if let Some(prev) = outcomes.insert(rec.index, rec.outcome) {
                assert_eq!(
                    prev, rec.outcome,
                    "Transaction {} recorded with conflicting outcomes (seed={})",
                    rec.index, self.seed
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // Diagnostics
    // ------------------------------------------------------------------

    fn format_cycle_details(&self, graph: &DepGraph, cycle: &[usize]) -> String {
        let mut lines = Vec::new();
        for i in 0..cycle.len() {
            let from_node = cycle[i];
            let to_node = cycle[(i + 1) % cycle.len()];
            let from_rec = graph.node_to_record[from_node];
            let to_rec = graph.node_to_record[to_node];

            // Find the edge type between from_node and to_node.
            let dep = graph.adjacency[from_node]
                .iter()
                .find(|(t, _)| *t == to_node)
                .map(|(_, d)| *d);

            lines.push(format!(
                "  txn {} --{:?}--> txn {}",
                from_rec,
                dep.unwrap_or(DepType::WR),
                to_rec,
            ));
        }
        lines.join("\n")
    }
}

// ---------------------------------------------------------------------------
// DFS cycle detection (White/Gray/Black coloring)
// ---------------------------------------------------------------------------

fn find_cycle(adjacency: &[Vec<(usize, DepType)>]) -> Option<Vec<usize>> {
    let n = adjacency.len();
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }
    let mut color = vec![Color::White; n];
    let mut stack = Vec::new();

    for start in 0..n {
        if color[start] == Color::White
            && let Some(cycle) = dfs(start, adjacency, &mut color, &mut stack)
        {
            return Some(cycle);
        }
    }
    return None;

    fn dfs(
        node: usize,
        adjacency: &[Vec<(usize, DepType)>],
        color: &mut [Color],
        stack: &mut Vec<usize>,
    ) -> Option<Vec<usize>> {
        color[node] = Color::Gray;
        stack.push(node);
        for &(next, _) in &adjacency[node] {
            match color[next] {
                Color::Gray => {
                    let cycle_start = stack.iter().position(|&n| n == next).unwrap();
                    return Some(stack[cycle_start..].to_vec());
                }
                Color::White => {
                    if let Some(cycle) = dfs(next, adjacency, color, stack) {
                        return Some(cycle);
                    }
                }
                Color::Black => {}
            }
        }
        stack.pop();
        color[node] = Color::Black;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_committed_record(
        index: usize,
        time: u64,
        reads: Vec<(i64, Option<i64>)>,
        writes: Vec<(i64, i64)>,
    ) -> TxnRecord {
        let now = tokio::time::Instant::now();
        TxnRecord {
            index,
            client_id: 0,
            read_set: reads,
            write_set: writes,
            outcome: TxnOutcome::Committed(TapirTimestamp {
                time,
                client_id: crate::IrClientId(0),
            }),
            wall_start: now,
            wall_end: now + std::time::Duration::from_millis(time),
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn serializable_no_conflict() {
        // T0: write key 1 = 1 at ts=1
        // T1: read key 1 = 1, write key 1 = 2 at ts=2
        // Dependencies: T0 --WR--> T1, T0 --WW--> T1. Acyclic.
        let records = vec![
            make_committed_record(0, 1, vec![], vec![(1, 1)]),
            make_committed_record(1, 2, vec![(1, Some(1))], vec![(1, 2)]),
        ];
        let checker = InvariantChecker::new(records, 42);
        checker.check_all();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(expected = "Serializability violation")]
    async fn serializability_violation_detected() {
        // T0 reads key 1 = None → RW: T0 -> T1
        // T1 reads key 2 = None → RW: T1 -> T0 (since T0 wrote key 2 = 1)
        // This creates a cycle: T0 -> T1 -> T0.
        let records = vec![
            make_committed_record(0, 2, vec![(1, None)], vec![(2, 1)]),
            make_committed_record(1, 1, vec![(2, None)], vec![(1, 1)]),
        ];
        let checker = InvariantChecker::new(records, 42);
        checker.check_all();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn aborted_transactions_excluded() {
        // T0: committed write key 1 = 1
        // T1: aborted (would have conflicted)
        // T2: committed read key 1 = 1
        let now = tokio::time::Instant::now();
        let records = vec![
            make_committed_record(0, 1, vec![], vec![(1, 1)]),
            TxnRecord {
                index: 1,
                client_id: 1,
                read_set: vec![(1, Some(1))],
                write_set: vec![(1, 2)],
                outcome: TxnOutcome::Aborted,
                wall_start: now,
                wall_end: now + std::time::Duration::from_millis(2),
            },
            make_committed_record(2, 3, vec![(1, Some(1))], vec![]),
        ];
        let checker = InvariantChecker::new(records, 42);
        checker.check_all();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn expected_counts_correct() {
        let records = vec![
            make_committed_record(0, 1, vec![], vec![(1, 1)]),
            make_committed_record(1, 2, vec![(1, Some(1))], vec![(1, 2)]),
            make_committed_record(2, 3, vec![], vec![(2, 1), (3, 1)]),
        ];
        let checker = InvariantChecker::new(records, 42);
        let counts = checker.expected_counts();
        assert_eq!(counts[&1], 2);
        assert_eq!(counts[&2], 1);
        assert_eq!(counts[&3], 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(expected = "Strict serializability violation")]
    async fn strict_serializability_violation_detected() {
        // T0 (ts=1): writes key 1 = 1. wall=[0ms, 10ms]
        // T1 (ts=2): writes key 1 = 2. wall=[20ms, 30ms]
        // T2 (ts=3): reads key 1 = 1. wall=[40ms, 50ms]
        // Data deps: WW: T0->T1, WR: T0->T2, RW: T2->T1
        // Real-time: T1->T2 (T1 wall_end=30ms < T2 wall_start=40ms)
        // Cycle: T2->T1 (data) and T1->T2 (real-time)
        let base = tokio::time::Instant::now();
        let records = vec![
            TxnRecord {
                index: 0,
                client_id: 0,
                read_set: vec![],
                write_set: vec![(1, 1)],
                outcome: TxnOutcome::Committed(TapirTimestamp {
                    time: 1,
                    client_id: crate::IrClientId(0),
                }),
                wall_start: base,
                wall_end: base + std::time::Duration::from_millis(10),
            },
            TxnRecord {
                index: 1,
                client_id: 0,
                read_set: vec![],
                write_set: vec![(1, 2)],
                outcome: TxnOutcome::Committed(TapirTimestamp {
                    time: 2,
                    client_id: crate::IrClientId(0),
                }),
                wall_start: base + std::time::Duration::from_millis(20),
                wall_end: base + std::time::Duration::from_millis(30),
            },
            TxnRecord {
                index: 2,
                client_id: 0,
                read_set: vec![(1, Some(1))],
                write_set: vec![],
                outcome: TxnOutcome::Committed(TapirTimestamp {
                    time: 3,
                    client_id: crate::IrClientId(0),
                }),
                wall_start: base + std::time::Duration::from_millis(40),
                wall_end: base + std::time::Duration::from_millis(50),
            },
        ];
        let checker = InvariantChecker::new(records, 42);
        checker.check_all();
    }
}
