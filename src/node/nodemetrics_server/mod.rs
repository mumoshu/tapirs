mod render;
mod server;

#[cfg(test)]
mod tests;

use crate::{IrReplicaMetrics, ShardNumber};

/// Metric type declarations for Prometheus exposition format.
pub(crate) struct MetricDesc {
    pub name: &'static str,
    pub help: &'static str,
    pub mtype: &'static str, // "gauge" or "counter"
}

pub(crate) const IR_METRICS: &[MetricDesc] = &[
    MetricDesc {
        name: "tapirs_replica_status",
        help: "Replica status (0=normal, 1=view_changing, 2=recovering)",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_view_number",
        help: "Current view number",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_ir_record_inconsistent_entries",
        help: "Number of inconsistent entries in IR record",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_ir_record_consensus_entries",
        help: "Number of consensus entries in IR record",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_membership_size",
        help: "Number of replicas in shard membership",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_view_changes_total",
        help: "Total completed view changes since process start",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_ir_record_stored_bytes",
        help: "Total bytes across IR record VlogLsm segments",
        mtype: "gauge",
    },
];

pub(crate) const APP_METRICS: &[MetricDesc] = &[
    MetricDesc {
        name: "tapirs_prepared_transactions",
        help: "Number of in-flight prepared transactions",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_transaction_log_size",
        help: "Size of transaction log (committed + aborted entries)",
        mtype: "gauge",
    },
    MetricDesc {
        name: "tapirs_commits_total",
        help: "Total committed transactions",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_aborts_total",
        help: "Total aborted transactions",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_prepare_ok_total",
        help: "Total successful OCC prepares",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_prepare_fail_total",
        help: "Total failed OCC prepares (conflicts)",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_prepare_retry_total",
        help: "Total OCC prepares needing retry (timestamp conflict)",
        mtype: "counter",
    },
    MetricDesc {
        name: "tapirs_prepare_too_late_total",
        help: "Total OCC prepares rejected as too late",
        mtype: "counter",
    },
];

/// Trait for types that can provide per-shard IR replica metrics.
pub trait MetricsCollector: Send + Sync + 'static {
    fn collect_metrics(&self) -> Vec<(ShardNumber, IrReplicaMetrics)>;
}

pub use render::render_metrics;
pub use server::start;
