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

    // Resharding
    ReshardSplitAttempt {
        round: usize,
        source_shard: u32,
        split_key: i64,
    },
    ReshardSplitOk {
        round: usize,
    },
    ReshardSplitErr {
        round: usize,
        error: String,
    },
    ReshardMergeAttempt {
        round: usize,
        absorbed: u32,
        surviving: u32,
    },
    ReshardMergeOk {
        round: usize,
    },
    ReshardMergeErr {
        round: usize,
        error: String,
    },

    // Invariant checking
    InvariantCheckStart,
    InvariantCheckPassed,
    CounterVerifyPassed,
    WorkloadTimedOut,
}

impl fmt::Display for FuzzEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FuzzEvent::Config { seed, num_shards, replica_counts, num_clients, num_keys } =>
                write!(f, "CONFIG seed={seed} shards={num_shards} replicas={replica_counts:?} clients={num_clients} keys={num_keys}"),
            FuzzEvent::FaultReplicaViewChange { round, shard, replica } =>
                write!(f, "FAULT[{round}] replica-view-change shard={shard} replica={replica}"),
            FuzzEvent::FaultClientViewChange { round, client, shard } =>
                write!(f, "FAULT[{round}] client-view-change client={client} shard={shard}"),
            FuzzEvent::FaultPartition { round, shard, replica, address } =>
                write!(f, "FAULT[{round}] partition shard={shard} replica={replica} addr={address}"),
            FuzzEvent::FaultHeal { round, address, hold_ms } =>
                write!(f, "FAULT[{round}] heal addr={address} after {hold_ms}ms"),
            FuzzEvent::TxnBegin { txn_index, client_id, txn_type, keys } =>
                write!(f, "TXN[{txn_index}] begin client={client_id} type={txn_type} keys={keys:?}"),
            FuzzEvent::TxnCommitted { txn_index, client_id, commit_ts } =>
                write!(f, "TXN[{txn_index}] committed client={client_id} ts={commit_ts}"),
            FuzzEvent::TxnAborted { txn_index, client_id } =>
                write!(f, "TXN[{txn_index}] aborted client={client_id}"),
            FuzzEvent::TxnTimedOut { txn_index, client_id } =>
                write!(f, "TXN[{txn_index}] timed-out client={client_id}"),
            FuzzEvent::ReshardSplitAttempt { round, source_shard, split_key } =>
                write!(f, "RESHARD[{round}] split-attempt shard={source_shard} key={split_key}"),
            FuzzEvent::ReshardSplitOk { round } =>
                write!(f, "RESHARD[{round}] split-ok"),
            FuzzEvent::ReshardSplitErr { round, error } =>
                write!(f, "RESHARD[{round}] split-err: {error}"),
            FuzzEvent::ReshardMergeAttempt { round, absorbed, surviving } =>
                write!(f, "RESHARD[{round}] merge-attempt absorbed={absorbed} surviving={surviving}"),
            FuzzEvent::ReshardMergeOk { round } =>
                write!(f, "RESHARD[{round}] merge-ok"),
            FuzzEvent::ReshardMergeErr { round, error } =>
                write!(f, "RESHARD[{round}] merge-err: {error}"),
            FuzzEvent::InvariantCheckStart =>
                write!(f, "INVARIANT check-start"),
            FuzzEvent::InvariantCheckPassed =>
                write!(f, "INVARIANT check-passed"),
            FuzzEvent::CounterVerifyPassed =>
                write!(f, "COUNTER verify-passed"),
            FuzzEvent::WorkloadTimedOut =>
                write!(f, "WORKLOAD timed-out"),
        }
    }
}

struct FuzzEventEntry {
    elapsed: std::time::Duration,
    event: FuzzEvent,
}

/// Thread-safe event log shared across spawned tasks.
///
/// Timestamps are simulated time from `tokio::time::Instant` (deterministic
/// under `start_paused = true`), so the log output is identical across runs
/// for the same seed.
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
            eprintln!("[{ms:>8}ms] {}", entry.event);
        }
        eprintln!("=== END FUZZ EVENT LOG ===");
    }

    /// Dump if verbose mode is on or if `force` is true.
    pub fn dump_if(&self, seed: u64, force: bool) {
        if self.verbose || force {
            self.dump(seed);
        }
    }
}
