use crate::node::Node;
use std::fmt::Write;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Metric type declarations for Prometheus exposition format.
struct MetricDesc {
    name: &'static str,
    help: &'static str,
    mtype: &'static str, // "gauge" or "counter"
}

const IR_METRICS: &[MetricDesc] = &[
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
];

const APP_METRICS: &[MetricDesc] = &[
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

fn render_metrics(node: &Node) -> String {
    let shard_metrics = node.collect_metrics();
    let mut out = String::with_capacity(4096);

    // Node-level metric.
    let _ = writeln!(out, "# HELP tapirs_node_replicas_count Number of shard replicas on this node");
    let _ = writeln!(out, "# TYPE tapirs_node_replicas_count gauge");
    let _ = writeln!(out, "tapirs_node_replicas_count {}", shard_metrics.len());

    // Emit HELP/TYPE headers and per-shard values for IR metrics.
    for desc in IR_METRICS {
        let _ = writeln!(out, "# HELP {} {}", desc.name, desc.help);
        let _ = writeln!(out, "# TYPE {} {}", desc.name, desc.mtype);
        for (shard, m) in &shard_metrics {
            let val = match desc.name {
                "tapirs_replica_status" => m.status as f64,
                "tapirs_view_number" => m.view_number as f64,
                "tapirs_ir_record_inconsistent_entries" => m.record_inconsistent_len as f64,
                "tapirs_ir_record_consensus_entries" => m.record_consensus_len as f64,
                "tapirs_membership_size" => m.membership_size as f64,
                "tapirs_view_changes_total" => m.view_change_count as f64,
                _ => continue,
            };
            let _ = writeln!(out, "{}{{shard=\"{}\"}} {}", desc.name, shard.0, format_value(val));
        }
    }

    // Emit HELP/TYPE headers and per-shard values for app-level metrics.
    for desc in APP_METRICS {
        let _ = writeln!(out, "# HELP {} {}", desc.name, desc.help);
        let _ = writeln!(out, "# TYPE {} {}", desc.name, desc.mtype);
        for (shard, m) in &shard_metrics {
            if let Some((_, val)) = m.app_metrics.iter().find(|(name, _)| *name == desc.name) {
                let _ = writeln!(out, "{}{{shard=\"{}\"}} {}", desc.name, shard.0, format_value(*val));
            }
        }
    }

    out
}

/// Format a float value for Prometheus: integers render without decimal point.
fn format_value(v: f64) -> String {
    if v == v.trunc() && v.is_finite() {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

pub async fn start(addr: std::net::SocketAddr, node: Arc<Node>) {
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("metrics: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "metrics server starting");

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let node = Arc::clone(&node);
                    tokio::spawn(async move {
                        let (reader, mut writer) = stream.into_split();
                        let mut buf_reader = BufReader::new(reader);

                        // Read request line.
                        let mut request_line = String::new();
                        if buf_reader.read_line(&mut request_line).await.is_err() {
                            return;
                        }
                        let parts: Vec<&str> = request_line.trim().splitn(3, ' ').collect();
                        if parts.len() < 2 {
                            return;
                        }
                        let method = parts[0];
                        let path = parts[1];

                        // Drain remaining headers (we don't need them).
                        loop {
                            let mut header_line = String::new();
                            if buf_reader.read_line(&mut header_line).await.is_err() {
                                return;
                            }
                            if header_line.trim().is_empty() {
                                break;
                            }
                        }

                        let (status, content_type, body) = if method == "GET" && path == "/metrics" {
                            let body = render_metrics(&node);
                            (200, "text/plain; version=0.0.4; charset=utf-8", body)
                        } else {
                            (404, "text/plain", "Not Found\n".to_string())
                        };

                        let status_text = match status {
                            200 => "OK",
                            _ => "Not Found",
                        };

                        let response = format!(
                            "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len(),
                        );
                        let _ = writer.write_all(response.as_bytes()).await;
                    });
                }
                Err(e) => {
                    tracing::warn!("metrics accept error: {e}");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_value_integers() {
        assert_eq!(format_value(0.0), "0");
        assert_eq!(format_value(42.0), "42");
        assert_eq!(format_value(1000.0), "1000");
    }

    #[test]
    fn format_value_floats() {
        #[allow(clippy::approx_constant)]
        let pi_ish = 3.14;
        assert_eq!(format_value(pi_ish), "3.14");
        assert_eq!(format_value(0.5), "0.5");
    }
}
