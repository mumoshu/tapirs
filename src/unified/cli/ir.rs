use std::path::PathBuf;

use crate::storage::io::disk_io::{BufferedIo, OpenFlags};
use crate::unified::ir::record::{IrMemEntry, IrPayloadInline, IrState, PrepareRef, VlogEntryType};
use crate::unified::ir::store as ir_store;

use super::{IrContext, parse_kv_pair, parse_ts, parse_txn_id};

const CLI_IO_FLAGS: OpenFlags = OpenFlags {
    create: true,
    direct: false,
};

pub(super) fn execute_ir_command<W: std::io::Write>(
    ctx: &mut IrContext,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    match parts[0] {
        "open" => {
            if parts.len() != 2 {
                return Err("usage: open <dir>".to_string());
            }
            let path = PathBuf::from(parts[1]);
            let store = ir_store::open_store_state::<String, String, BufferedIo>(&path, CLI_IO_FLAGS)
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
            let store = ir_store::open_store_state::<String, String, BufferedIo>(&path, CLI_IO_FLAGS)
                .map_err(|e| format!("open-with failed: {e}"))?;
            ctx.store = Some(store);
            ctx.min_view_vlog_size = min_size;
            Ok(())
        }
        "prepare" => {
            if parts.len() < 3 {
                return Err("usage: prepare <client:num> <ts> [key=value ...]".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let commit_ts = parse_ts(parts[2])?;
            let mut writes = Vec::new();
            for kv in &parts[3..] {
                writes.push(parse_kv_pair(kv)?);
            }

            let op_id = ctx.next_op_id();
            let current_view = ctx.store()?.current_view();
            let store = ctx.store_mut()?;
            ir_store::insert_ir_entry(
                store,
                op_id,
                IrMemEntry {
                    entry_type: VlogEntryType::Prepare,
                    state: IrState::Finalized(current_view),
                    payload: IrPayloadInline::Prepare {
                        transaction_id: txn_id,
                        commit_ts,
                        read_set: vec![],
                        write_set: writes,
                        scan_set: vec![],
                    },
                },
            );
            ctx.prepared.insert(txn_id, (op_id, current_view));
            Ok(())
        }
        "commit" => {
            if parts.len() != 3 {
                return Err("usage: commit <client:num> <ts>".to_string());
            }
            let txn_id = parse_txn_id(parts[1])?;
            let commit_ts = parse_ts(parts[2])?;
            let op_id = ctx.next_op_id();
            let prepare_ref = if let Some((prepare_op_id, _prepare_view)) = ctx.prepared.remove(&txn_id) {
                PrepareRef::SameView(prepare_op_id)
            } else {
                PrepareRef::SameView(op_id)
            };

            let current_view = ctx.store()?.current_view();
            let store = ctx.store_mut()?;
            ir_store::insert_ir_entry(
                store,
                op_id,
                IrMemEntry {
                    entry_type: VlogEntryType::Commit,
                    state: IrState::Finalized(current_view),
                    payload: IrPayloadInline::Commit {
                        transaction_id: txn_id,
                        commit_ts,
                        prepare_ref,
                    },
                },
            );
            Ok(())
        }
        "seal" => {
            let min_view_vlog_size = ctx.min_view_vlog_size;
            let store = ctx.store_mut()?;
            ir_store::seal_current_view(store, min_view_vlog_size)
                .map_err(|e| format!("seal failed: {e}"))?;
            Ok(())
        }
        "status" => {
            let store = ctx.store()?;
            let view = store.current_view();
            let segments = store.sealed_segments_ref().len();
            writeln!(stdout, "view={view} sealed_segments={segments}")
                .map_err(|e| format!("write failed: {e}"))
        }
        "list-vlogs" => {
            let store = ctx.store()?;
            let active = store.active_vlog_ref();
            let active_id = active.id;
            let active_size = active.write_offset();
            let view_list: Vec<String> = active.views.iter().map(|v| v.view.to_string()).collect();
            writeln!(
                stdout,
                "vlog_seg_{active_id:04} size={active_size} views=[{}]",
                view_list.join(",")
            )
            .map_err(|e| format!("write failed: {e}"))?;

            for (id, seg) in store.sealed_segments_ref() {
                let size = seg.write_offset();
                let view_list: Vec<String> = seg.views.iter().map(|v| v.view.to_string()).collect();
                writeln!(
                    stdout,
                    "vlog_seg_{id:04} size={size} views=[{}]",
                    view_list.join(",")
                )
                .map_err(|e| format!("write failed: {e}"))?;
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
                .segment_ref(seg_id)
                .ok_or_else(|| format!("dump-vlog failed: VLog segment {seg_id} not found"))?;
            let entries = ir_store::iter_entries::<String, String, _>(seg)
                .map_err(|e| format!("dump-vlog failed: {e}"))?;
            for (offset, op_id, entry) in &entries {
                write!(
                    stdout,
                    "@{offset} op={}:{} ",
                    op_id.client_id.0, op_id.number
                )
                .map_err(|e| format!("write failed: {e}"))?;
                match &entry.payload {
                    IrPayloadInline::Prepare {
                        transaction_id,
                        commit_ts,
                        write_set,
                        ..
                    } => {
                        write!(
                            stdout,
                            "PREPARE txn={}:{} ts={}",
                            transaction_id.client_id.0,
                            transaction_id.number,
                            commit_ts.time
                        )
                        .map_err(|e| format!("write failed: {e}"))?;
                        for (k, v) in write_set {
                            let v_str = v.as_deref().unwrap_or("<tombstone>");
                            write!(stdout, " {k}={v_str}")
                                .map_err(|e| format!("write failed: {e}"))?;
                        }
                        writeln!(stdout).map_err(|e| format!("write failed: {e}"))?;
                    }
                    IrPayloadInline::Commit {
                        transaction_id,
                        commit_ts,
                        prepare_ref,
                    } => {
                        let ref_str = match prepare_ref {
                            PrepareRef::SameView(op) => {
                                format!("same_view({}:{})", op.client_id.0, op.number)
                            }
                            PrepareRef::CrossView { view, vlog_ptr } => {
                                format!(
                                    "cross_view(v={} seg={} off={} len={})",
                                    view, vlog_ptr.segment_id, vlog_ptr.offset, vlog_ptr.length
                                )
                            }
                        };
                        writeln!(
                            stdout,
                            "COMMIT txn={}:{} ts={} ref={}",
                            transaction_id.client_id.0,
                            transaction_id.number,
                            commit_ts.time,
                            ref_str
                        )
                        .map_err(|e| format!("write failed: {e}"))?;
                    }
                    IrPayloadInline::Abort {
                        transaction_id,
                        commit_ts,
                    } => {
                        let ts_str = commit_ts.map_or("none".to_string(), |t| t.time.to_string());
                        writeln!(
                            stdout,
                            "ABORT txn={}:{} ts={}",
                            transaction_id.client_id.0, transaction_id.number, ts_str
                        )
                        .map_err(|e| format!("write failed: {e}"))?;
                    }
                    IrPayloadInline::QuorumRead { key, timestamp } => {
                        writeln!(stdout, "QUORUM_READ key={key} ts={}", timestamp.time)
                            .map_err(|e| format!("write failed: {e}"))?;
                    }
                    IrPayloadInline::QuorumScan {
                        start_key,
                        end_key,
                        snapshot_ts,
                    } => {
                        writeln!(
                            stdout,
                            "QUORUM_SCAN start={start_key} end={end_key} ts={}",
                            snapshot_ts.time
                        )
                        .map_err(|e| format!("write failed: {e}"))?;
                    }
                    IrPayloadInline::RaiseMinPrepareTime { time } => {
                        writeln!(stdout, "RAISE_MIN_PREPARE_TIME ts={time}")
                            .map_err(|e| format!("write failed: {e}"))?;
                    }
                }
            }
            Ok(())
        }
        other => Err(format!("unknown command: {other}")),
    }
}
