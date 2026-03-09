use super::{APP_METRICS, IR_METRICS};
use crate::{IrReplicaMetrics, ShardNumber};
use std::fmt::Write;

/// Render Prometheus exposition-format text from per-shard IR replica metrics.
pub fn render_metrics(shard_metrics: &[(ShardNumber, IrReplicaMetrics)]) -> String {
    let mut out = String::with_capacity(4096);

    // Node-level metric.
    let _ = writeln!(
        out,
        "# HELP tapirs_node_replicas_count Number of shard replicas on this node"
    );
    let _ = writeln!(out, "# TYPE tapirs_node_replicas_count gauge");
    let _ = writeln!(out, "tapirs_node_replicas_count {}", shard_metrics.len());

    // Emit HELP/TYPE headers and per-shard values for IR metrics.
    for desc in IR_METRICS {
        let _ = writeln!(out, "# HELP {} {}", desc.name, desc.help);
        let _ = writeln!(out, "# TYPE {} {}", desc.name, desc.mtype);
        for (shard, m) in shard_metrics {
            let val = match desc.name {
                "tapirs_replica_status" => m.status as f64,
                "tapirs_view_number" => m.view_number as f64,
                "tapirs_ir_record_inconsistent_entries" => m.record_inconsistent_len as f64,
                "tapirs_ir_record_consensus_entries" => m.record_consensus_len as f64,
                "tapirs_membership_size" => m.membership_size as f64,
                "tapirs_view_changes_total" => m.view_change_count as f64,
                "tapirs_ir_record_stored_bytes" => m.record_stored_bytes.unwrap_or(0) as f64,
                _ => continue,
            };
            let _ = writeln!(
                out,
                "{}{{shard=\"{}\"}} {}",
                desc.name,
                shard.0,
                format_value(val)
            );
        }
    }

    // Emit HELP/TYPE headers and per-shard values for app-level metrics.
    for desc in APP_METRICS {
        let _ = writeln!(out, "# HELP {} {}", desc.name, desc.help);
        let _ = writeln!(out, "# TYPE {} {}", desc.name, desc.mtype);
        for (shard, m) in shard_metrics {
            if let Some((_, val)) = m.app_metrics.iter().find(|(name, _)| *name == desc.name) {
                let _ = writeln!(
                    out,
                    "{}{{shard=\"{}\"}} {}",
                    desc.name,
                    shard.0,
                    format_value(*val)
                );
            }
        }
    }

    out
}

/// Format a float value for Prometheus: integers render without decimal point.
pub(super) fn format_value(v: f64) -> String {
    if v == v.trunc() && v.is_finite() {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}
