use std::path::PathBuf;

use crate::mvcc::disk::disk_io::{BufferedIo, OpenFlags};
use crate::unified::tapir::store as tapir_store;

use super::{
    TapirContext, parse_prepare_payload, parse_ts, parse_txn_id, write_kv_result,
    write_tapir_segment,
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
            let payload = parse_prepare_payload(&parts[3..])?;
            ctx.prepared.insert(txn_id, payload);
            Ok(())
        }
        "commit" => {
            if parts.len() != 3 {
                return Err("usage: commit <client:num> <ts>".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let commit_ts = parse_ts(parts[2])?;
            let payload = ctx
                .prepared
                .remove(&txn_id)
                .ok_or_else(|| format!("no prepared transaction: {}", parts[1]))?;
            let store = ctx.store_mut()?;
            tapir_store::commit_transaction_data(
                store,
                txn_id,
                &payload.read_set,
                &payload.write_set,
                &payload.scan_set,
                commit_ts,
            )
            .map_err(|e| format!("commit failed: {e}"))
        }
        "get" => {
            if parts.len() != 2 {
                return Err("usage: get <key>".to_string());
            }
            let key = parts[1].to_string();
            let store = ctx.store()?;
            let (value, ts) =
                tapir_store::do_uncommitted_get(store, &key).map_err(|e| format!("get failed: {e}"))?;
            write_kv_result(stdout, &key, value.as_deref(), ts)
        }
        "get-at" => {
            if parts.len() != 3 {
                return Err("usage: get-at <key> <ts>".to_string());
            }
            let key = parts[1].to_string();
            let ts = parse_ts(parts[2])?;
            let store = ctx.store()?;
            let (value, actual_ts) = tapir_store::do_uncommitted_get_at(store, &key, ts)
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
            let results = tapir_store::do_uncommitted_scan(store, &start, &end, ts)
                .map_err(|e| format!("scan failed: {e}"))?;
            for (key, value, entry_ts) in &results {
                write_kv_result(stdout, key, value.as_deref(), *entry_ts)?;
            }
            Ok(())
        }
        "get-range" => {
            if parts.len() != 3 {
                return Err("usage: get-range <key> <ts>".to_string());
            }
            let key = parts[1].to_string();
            let ts = parse_ts(parts[2])?;
            let store = ctx.store()?;
            let (write_ts, next_ts) = store
                .get_range(&key, ts)
                .map_err(|e| format!("get-range failed: {e}"))?;
            let next_str = match next_ts {
                Some(t) => t.time.to_string(),
                None => "none".to_string(),
            };
            writeln!(stdout, "write_ts={} next_ts={next_str}", write_ts.time)
                .map_err(|e| format!("write failed: {e}"))
        }
        "has-writes" => {
            if parts.len() != 5 {
                return Err("usage: has-writes <start> <end> <after_ts> <before_ts>".to_string());
            }
            let start = parts[1].to_string();
            let end = parts[2].to_string();
            let after = parse_ts(parts[3])?;
            let before = parse_ts(parts[4])?;
            let store = ctx.store()?;
            let has = store
                .has_writes_in_range(&start, &end, after, before)
                .map_err(|e| format!("has-writes failed: {e}"))?;
            writeln!(stdout, "{has}").map_err(|e| format!("write failed: {e}"))
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
            let view = store.current_view();
            let segments = store.sealed_vlog_segments().len();
            writeln!(stdout, "view={view} sealed_segments={segments}")
                .map_err(|e| format!("write failed: {e}"))
        }
        "list-vlogs" => {
            let store = ctx.store()?;
            if let Some(seg) = store.active_or_sealed_segment_ref(store.active_vlog_id()) {
                write_tapir_segment(stdout, seg)?;
            }
            for seg in store.sealed_vlog_segments().values() {
                write_tapir_segment(stdout, seg)?;
            }
            Ok(())
        }
        "dump-vlog" => {
            if parts.len() != 2 {
                return Err("usage: dump-vlog <segment-id>".to_string());
            }
            let seg_id: u64 = parts[1]
                .parse()
                .map_err(|_| format!("invalid segment id: {}", parts[1]))?;
            let store = ctx.store()?;
            let seg = store
                .active_or_sealed_segment_ref(seg_id)
                .ok_or_else(|| format!("dump-vlog failed: VLog segment {seg_id} not found"))?;
            let entries = tapir_store::iter_committed_txn_entries::<String, String, _>(seg)
                .map_err(|e| format!("dump-vlog failed: {e}"))?;
            for (offset, prepared) in &entries {
                write!(
                    stdout,
                    "@{offset} TAPIR_COMMIT txn={}:{} ts={}",
                    prepared.transaction_id.client_id.0,
                    prepared.transaction_id.number,
                    prepared.commit_ts.time
                )
                .map_err(|e| format!("write failed: {e}"))?;
                for (k, v) in &prepared.write_set {
                    let v_str = v.as_deref().unwrap_or("<tombstone>");
                    write!(stdout, " {k}={v_str}").map_err(|e| format!("write failed: {e}"))?;
                }
                writeln!(stdout).map_err(|e| format!("write failed: {e}"))?;
            }
            Ok(())
        }
        other => Err(format!("unknown command: {other}")),
    }
}
