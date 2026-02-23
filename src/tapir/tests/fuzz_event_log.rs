use std::fmt;
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

/// Events recorded during fuzz execution.
#[derive(Debug, Clone)]
pub enum FuzzEvent {
    Config {
        seed: u64,
        num_shards: u32,
        replica_counts: Vec<usize>,
        num_clients: usize,
        num_keys: i64,
    },

    // Fault injection
    FaultReplicaViewChange {
        round: u32,
        shard: usize,
        replica: usize,
    },
    FaultClientViewChange {
        round: u32,
        client: usize,
        shard: usize,
    },
    FaultPartition {
        round: u32,
        shard: usize,
        replica: usize,
        address: usize,
    },
    FaultHeal {
        round: u32,
        address: usize,
        hold_ms: u64,
    },

    // Transaction lifecycle
    TxnBegin {
        txn_index: usize,
        client_id: usize,
        txn_type: &'static str,
        keys: Vec<i64>,
    },
    TxnCommitted {
        txn_index: usize,
        client_id: usize,
        commit_ts: u64,
    },
    TxnAborted {
        txn_index: usize,
        client_id: usize,
    },
    TxnTimedOut {
        txn_index: usize,
        client_id: usize,
    },
    TxnGet {
        txn_index: usize,
        client_id: usize,
        key: i64,
        value: Option<i64>,
        stale_note: &'static str,
    },
    TxnPut {
        txn_index: usize,
        client_id: usize,
        key: i64,
        value: i64,
    },
    TxnScan {
        txn_index: usize,
        client_id: usize,
        lo: i64,
        hi: i64,
        count: usize,
        stale_note: &'static str,
    },
    TxnRetry {
        txn_index: usize,
        client_id: usize,
        attempt: u8,
        keys: Vec<i64>,
    },
    TxnDropped {
        txn_index: usize,
        client_id: usize,
    },
    TxnOutOfRange {
        txn_index: usize,
        client_id: usize,
        key: i64,
    },
    TxnOutOfRangeRetry {
        client_id: usize,
        message: String,
    },

    // Resharding
    ReshardSplitAttempt {
        round: usize,
        source_shard: u32,
        split_key: i64,
    },
    ReshardSplitOk {
        round: usize,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardSplitErr {
        round: usize,
        error: String,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardMergeAttempt {
        round: usize,
        absorbed: u32,
        surviving: u32,
    },
    ReshardMergeOk {
        round: usize,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardMergeErr {
        round: usize,
        error: String,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardCompactAttempt {
        round: usize,
        source_shard: u32,
    },
    ReshardCompactOk {
        round: usize,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardCompactErr {
        round: usize,
        error: String,
        wall_ms: u128,
        sim_ms: u128,
    },
    ReshardPhase {
        round: usize,
        phase: String,
    },

    // Diagnostics
    MayHaveCommittedCount {
        count: u64,
    },

    // Invariant checking
    InvariantCheckStart,
    InvariantCheckPassed,
    CounterVerifyPassed,
    WorkloadTimedOut,

    // Verification phase
    VerifyClusterRemoteShardOk {
        shard: u32,
        wall_ms: u128,
        sim_ms: u128,
    },
    VerifyCounterKeyStart {
        key: i64,
    },
    VerifyCounterKeyDone {
        key: i64,
        expected: i64,
        actual: i64,
        wall_ms: u128,
        sim_ms: u128,
    },
    VerifyPhaseTimedOut {
        phase: String,
        wall_ms: u128,
        sim_ms: u128,
    },
}

impl FuzzEvent {
    /// Derive the actor from the event variant for log grouping.
    fn actor(&self) -> String {
        match self {
            FuzzEvent::TxnBegin { client_id, .. }
            | FuzzEvent::TxnCommitted { client_id, .. }
            | FuzzEvent::TxnAborted { client_id, .. }
            | FuzzEvent::TxnTimedOut { client_id, .. }
            | FuzzEvent::TxnGet { client_id, .. }
            | FuzzEvent::TxnPut { client_id, .. }
            | FuzzEvent::TxnScan { client_id, .. }
            | FuzzEvent::TxnRetry { client_id, .. }
            | FuzzEvent::TxnDropped { client_id, .. }
            | FuzzEvent::TxnOutOfRange { client_id, .. }
            | FuzzEvent::TxnOutOfRangeRetry { client_id, .. } => format!("CLIENT[{client_id}]"),
            FuzzEvent::FaultReplicaViewChange { .. }
            | FuzzEvent::FaultClientViewChange { .. }
            | FuzzEvent::FaultPartition { .. }
            | FuzzEvent::FaultHeal { .. } => "FAULT".to_string(),
            FuzzEvent::ReshardSplitAttempt { .. }
            | FuzzEvent::ReshardSplitOk { .. }
            | FuzzEvent::ReshardSplitErr { .. }
            | FuzzEvent::ReshardMergeAttempt { .. }
            | FuzzEvent::ReshardMergeOk { .. }
            | FuzzEvent::ReshardMergeErr { .. }
            | FuzzEvent::ReshardCompactAttempt { .. }
            | FuzzEvent::ReshardCompactOk { .. }
            | FuzzEvent::ReshardCompactErr { .. }
            | FuzzEvent::ReshardPhase { .. } => "ADMIN".to_string(),
            FuzzEvent::Config { .. }
            | FuzzEvent::MayHaveCommittedCount { .. }
            | FuzzEvent::InvariantCheckStart
            | FuzzEvent::InvariantCheckPassed
            | FuzzEvent::CounterVerifyPassed
            | FuzzEvent::WorkloadTimedOut => "SYSTEM".to_string(),
            FuzzEvent::VerifyClusterRemoteShardOk { .. }
            | FuzzEvent::VerifyCounterKeyStart { .. }
            | FuzzEvent::VerifyCounterKeyDone { .. }
            | FuzzEvent::VerifyPhaseTimedOut { .. } => "VERIFY".to_string(),
        }
    }
}

impl fmt::Display for FuzzEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FuzzEvent::Config { seed, num_shards, replica_counts, num_clients, num_keys } =>
                write!(f, "CONFIG seed={seed} shards={num_shards} replicas={replica_counts:?} clients={num_clients} keys={num_keys}"),
            FuzzEvent::FaultReplicaViewChange { round, shard, replica } =>
                write!(f, "[{round}] replica-view-change shard={shard} replica={replica}"),
            FuzzEvent::FaultClientViewChange { round, client, shard } =>
                write!(f, "[{round}] client-view-change client={client} shard={shard}"),
            FuzzEvent::FaultPartition { round, shard, replica, address } =>
                write!(f, "[{round}] partition shard={shard} replica={replica} addr={address}"),
            FuzzEvent::FaultHeal { round, address, hold_ms } =>
                write!(f, "[{round}] heal addr={address} after {hold_ms}ms"),
            FuzzEvent::TxnBegin { txn_index, txn_type, keys, .. } =>
                write!(f, "TXN[{txn_index}] begin type={txn_type} keys={keys:?}"),
            FuzzEvent::TxnCommitted { txn_index, commit_ts, .. } =>
                write!(f, "TXN[{txn_index}] committed ts={commit_ts}"),
            FuzzEvent::TxnAborted { txn_index, .. } =>
                write!(f, "TXN[{txn_index}] aborted"),
            FuzzEvent::TxnTimedOut { txn_index, .. } =>
                write!(f, "TXN[{txn_index}] timed-out"),
            FuzzEvent::TxnGet { txn_index, key, value, stale_note, .. } => {
                let val_str = value.map_or("None".to_string(), |v| v.to_string());
                if stale_note.is_empty() {
                    write!(f, "TXN[{txn_index}] get key={key} => {val_str}")
                } else {
                    write!(f, "TXN[{txn_index}] get key={key} => {val_str} {stale_note}")
                }
            }
            FuzzEvent::TxnPut { txn_index, key, value, .. } =>
                write!(f, "TXN[{txn_index}] put key={key} val={value}"),
            FuzzEvent::TxnScan { txn_index, lo, hi, count, stale_note, .. } => {
                if stale_note.is_empty() {
                    write!(f, "TXN[{txn_index}] scan [{lo}, {hi}) => {count} keys")
                } else {
                    write!(f, "TXN[{txn_index}] scan [{lo}, {hi}) => {count} keys {stale_note}")
                }
            }
            FuzzEvent::TxnRetry { txn_index, attempt, keys, .. } =>
                write!(f, "TXN[{txn_index}] retry attempt={attempt} keys={keys:?}"),
            FuzzEvent::TxnDropped { txn_index, .. } =>
                write!(f, "TXN[{txn_index}] dropped (no commit)"),
            FuzzEvent::TxnOutOfRange { txn_index, key, .. } =>
                write!(f, "TXN[{txn_index}] out-of-range key={key}"),
            FuzzEvent::TxnOutOfRangeRetry { message, .. } =>
                write!(f, "out-of-range-retry: {message}"),
            FuzzEvent::ReshardSplitAttempt { round, source_shard, split_key } =>
                write!(f, "RESHARD[{round}] split-attempt shard={source_shard} key={split_key}"),
            FuzzEvent::ReshardSplitOk { round, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] split-ok wall={wall_ms}ms sim={sim_ms}ms"),
            FuzzEvent::ReshardSplitErr { round, error, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] split-err wall={wall_ms}ms sim={sim_ms}ms: {error}"),
            FuzzEvent::ReshardMergeAttempt { round, absorbed, surviving } =>
                write!(f, "RESHARD[{round}] merge-attempt absorbed={absorbed} surviving={surviving}"),
            FuzzEvent::ReshardMergeOk { round, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] merge-ok wall={wall_ms}ms sim={sim_ms}ms"),
            FuzzEvent::ReshardMergeErr { round, error, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] merge-err wall={wall_ms}ms sim={sim_ms}ms: {error}"),
            FuzzEvent::ReshardCompactAttempt { round, source_shard } =>
                write!(f, "RESHARD[{round}] compact-attempt shard={source_shard}"),
            FuzzEvent::ReshardCompactOk { round, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] compact-ok wall={wall_ms}ms sim={sim_ms}ms"),
            FuzzEvent::ReshardCompactErr { round, error, wall_ms, sim_ms } =>
                write!(f, "RESHARD[{round}] compact-err wall={wall_ms}ms sim={sim_ms}ms: {error}"),
            FuzzEvent::ReshardPhase { round, phase } =>
                write!(f, "RESHARD[{round}] phase: {phase}"),
            FuzzEvent::MayHaveCommittedCount { count } =>
                write!(f, "may-have-committed count={count}"),
            FuzzEvent::InvariantCheckStart =>
                write!(f, "INVARIANT check-start"),
            FuzzEvent::InvariantCheckPassed =>
                write!(f, "INVARIANT check-passed"),
            FuzzEvent::CounterVerifyPassed =>
                write!(f, "COUNTER verify-passed"),
            FuzzEvent::WorkloadTimedOut =>
                write!(f, "WORKLOAD timed-out"),
            FuzzEvent::VerifyClusterRemoteShardOk { shard, wall_ms, sim_ms } =>
                write!(f, "cluster-remote shard={shard} ok wall={wall_ms}ms sim={sim_ms}ms"),
            FuzzEvent::VerifyCounterKeyStart { key } =>
                write!(f, "counter key={key} starting"),
            FuzzEvent::VerifyCounterKeyDone { key, expected, actual, wall_ms, sim_ms } =>
                write!(f, "counter key={key} expected={expected} actual={actual} wall={wall_ms}ms sim={sim_ms}ms"),
            FuzzEvent::VerifyPhaseTimedOut { phase, wall_ms, sim_ms } =>
                write!(f, "TIMED OUT phase={phase} wall={wall_ms}ms sim={sim_ms}ms"),
        }
    }
}

struct FuzzEventEntry {
    elapsed: std::time::Duration,
    event: FuzzEvent,
}

/// Thread-safe event log shared across spawned tasks.
///
/// **Time conventions:**
/// - The `[sim Xms]` column in dump output is **simulated time** from
///   `tokio::time::Instant` (deterministic under `start_paused = true`).
/// - Event fields named `wall_ms` are **wall-clock time** from
///   `std::time::Instant` (real elapsed, varies between runs).
/// - Event fields named `sim_ms` are **simulated elapsed time** for that
///   specific operation (not the global timeline offset).
/// - Events spanning async work (resharding, verification) report both
///   `wall_ms` and `sim_ms` so you can distinguish protocol overhead
///   (sim) from CPU/scheduling overhead (wall).
#[derive(Clone)]
pub struct FuzzEventLog {
    start: Instant,
    entries: Arc<Mutex<Vec<FuzzEventEntry>>>,
    verbose: bool,
}

impl FuzzEventLog {
    pub fn new() -> Self {
        let verbose = std::env::var("FUZZ_VERBOSE").map_or(false, |v| v == "1");
        Self {
            start: Instant::now(),
            entries: Arc::new(Mutex::new(Vec::with_capacity(256))),
            verbose,
        }
    }

    pub fn record(&self, event: FuzzEvent) {
        let elapsed = self.start.elapsed();
        self.entries.lock().unwrap().push(FuzzEventEntry { elapsed, event });
    }

    /// Dump the event log to stderr.
    pub fn dump(&self, seed: u64) {
        let entries = self.entries.lock().unwrap();
        eprintln!("=== FUZZ EVENT LOG (seed={seed}, {} events) ===", entries.len());
        for entry in entries.iter() {
            let ms = entry.elapsed.as_millis();
            let actor = entry.event.actor();
            eprintln!("[sim {ms:>8}ms] {actor:<10} {}", entry.event);
        }
        eprintln!("=== END FUZZ EVENT LOG ===");
    }

    /// Dump if verbose mode is on or if `force` is true.
    pub fn dump_if(&self, seed: u64, force: bool) {
        if self.verbose || force {
            self.dump(seed);
        }
    }

    /// Return the time window (first, last) of transaction events.
    pub fn txn_time_window(&self) -> Option<(std::time::Duration, std::time::Duration)> {
        let entries = self.entries.lock().unwrap();
        let mut first = None;
        let mut last = None;
        for entry in entries.iter() {
            if matches!(entry.event,
                FuzzEvent::TxnBegin { .. }
                | FuzzEvent::TxnCommitted { .. }
                | FuzzEvent::TxnAborted { .. }
                | FuzzEvent::TxnTimedOut { .. }
            ) {
                if first.is_none() {
                    first = Some(entry.elapsed);
                }
                last = Some(entry.elapsed);
            }
        }
        first.zip(last)
    }

    /// Check if any resharding event falls within the given time window.
    pub fn has_reshard_event_between(
        &self,
        first: std::time::Duration,
        last: std::time::Duration,
    ) -> bool {
        let entries = self.entries.lock().unwrap();
        entries.iter().any(|e| {
            e.elapsed >= first
                && e.elapsed <= last
                && matches!(e.event,
                    FuzzEvent::ReshardSplitAttempt { .. }
                    | FuzzEvent::ReshardSplitOk { .. }
                    | FuzzEvent::ReshardMergeAttempt { .. }
                    | FuzzEvent::ReshardMergeOk { .. }
                    | FuzzEvent::ReshardCompactAttempt { .. }
                    | FuzzEvent::ReshardCompactOk { .. }
                    | FuzzEvent::ReshardPhase { .. }
                )
        })
    }
}
