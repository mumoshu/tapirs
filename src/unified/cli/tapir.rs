use std::path::PathBuf;

use crate::mvcc::disk::disk_io::{BufferedIo, OpenFlags};
use crate::unified::tapir::store as tapir_store;

use super::{
    TapirContext, parse_tapir_transaction, parse_ts, parse_txn_id, write_kv_result,
};

pub(super) fn execute_tapir_command<W: std::io::Write>(
    ctx: &mut TapirContext,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    match parts[0] {
        "open" => {
            if parts.len() != 2 {
                return Err("usage: open <dir>".to_string());
            }
            let path = PathBuf::from(parts[1]);
            let io_flags = OpenFlags {
                create: true,
                direct: false,
            };
            let store = tapir_store::open::<String, String, BufferedIo>(&path, io_flags)
                .map_err(|e| format!("open failed: {e}"))?;
            ctx.store = Some(store);
            Ok(())
        }
        "open-with" => {
            if parts.len() != 3 {
                return Err("usage: open-with <dir> <min_vlog_size>".to_string());
            }
            let path = PathBuf::from(parts[1]);
            let min_size: u64 = parts[2]
                .parse()
                .map_err(|_| format!("invalid min_vlog_size: {}", parts[2]))?;
            let io_flags = OpenFlags {
                create: true,
                direct: false,
            };
            let store = tapir_store::open::<String, String, BufferedIo>(&path, io_flags)
                .map_err(|e| format!("open-with failed: {e}"))?;
            ctx.store = Some(store);
            ctx.min_view_vlog_size = min_size;
            Ok(())
        }
        "prepare" => {
            if parts.len() < 3 {
                return Err("usage: prepare <client:num> <ts> [r:key@ts ...] [w:key=value ...]".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let commit_ts = parse_ts(parts[2])?;
            let txn = parse_tapir_transaction(&parts[3..])?;
            let store = ctx.store_mut()?;
            store.prepare(txn_id, &txn, commit_ts);
            Ok(())
        }
        "abort" => {
            if parts.len() != 2 {
                return Err("usage: abort <client:num>".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let store = ctx.store_mut()?;
            store.abort(&txn_id);
            Ok(())
        }
        "commit" => {
            if parts.len() != 3 {
                return Err("usage: commit <client:num> <ts>".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let commit_ts = parse_ts(parts[2])?;
            let store = ctx.store_mut()?;
            store
                .commit_prepared(txn_id, commit_ts)
                .map_err(|e| format!("commit failed: {e}"))
        }
        "get" => {
            if parts.len() != 2 {
                return Err("usage: get <key>".to_string());
            }
            let key = parts[1].to_string();
            let store = ctx.store()?;
            let (value, ts) = store
                .do_uncommitted_get(&key)
                .map_err(|e| format!("get failed: {e}"))?;
            write_kv_result(stdout, &key, value.as_deref(), ts)
        }
        "get-at" => {
            if parts.len() != 3 {
                return Err("usage: get-at <key> <ts>".to_string());
            }
            let key = parts[1].to_string();
            let ts = parse_ts(parts[2])?;
            let store = ctx.store()?;
            let (value, actual_ts) = store
                .do_uncommitted_get_at(&key, ts)
                .map_err(|e| format!("get-at failed: {e}"))?;
            write_kv_result(stdout, &key, value.as_deref(), actual_ts)
        }
        "scan" => {
            if parts.len() != 4 {
                return Err("usage: scan <start> <end> <ts>".to_string());
            }
            let start = parts[1].to_string();
            let end = parts[2].to_string();
            let ts = parse_ts(parts[3])?;
            let store = ctx.store()?;
            let results = store
                .do_uncommitted_scan(&start, &end, ts)
                .map_err(|e| format!("scan failed: {e}"))?;
            for (key, value, entry_ts) in &results {
                write_kv_result(stdout, key, value.as_deref(), *entry_ts)?;
            }
            Ok(())
        }
        "seal" => {
            let min_view_vlog_size = ctx.min_view_vlog_size;
            let store = ctx.store_mut()?;
            store
                .seal_current_view(min_view_vlog_size)
                .map_err(|e| format!("seal failed: {e}"))
        }
        "status" => {
            let store = ctx.store()?;
            let report = store.status().map_err(|e| format!("status failed: {e}"))?;
            writeln!(stdout, "view={} sealed_segments={}", report.view, report.sealed_segments)
                .map_err(|e| format!("write failed: {e}"))?;
            for seg in &report.segments {
                let view_list: Vec<String> = seg.views.iter().map(|v| v.to_string()).collect();
                write!(
                    stdout,
                    "vlog_seg_{:04} size={} views=[{}]",
                    seg.id,
                    seg.size,
                    view_list.join(",")
                )
                .map_err(|e| format!("write failed: {e}"))?;
                writeln!(stdout).map_err(|e| format!("write failed: {e}"))?;

                for commit in &seg.commits {
                    write!(
                        stdout,
                        "@{} TAPIR_COMMIT txn={}:{} ts={}",
                        commit.offset,
                        commit.transaction_id.client_id.0,
                        commit.transaction_id.number,
                        commit.commit_ts.time
                    )
                    .map_err(|e| format!("write failed: {e}"))?;
                    for (k, v) in &commit.write_set {
                        let v_str = v
                            .as_ref()
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "<tombstone>".to_string());
                        write!(stdout, " {k}={v_str}")
                            .map_err(|e| format!("write failed: {e}"))?;
                    }
                    writeln!(stdout).map_err(|e| format!("write failed: {e}"))?;
                }
            }
            Ok(())
        }
        other => Err(format!("unknown command: {other}")),
    }
}
