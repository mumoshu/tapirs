use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::BufferedIo;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::types::*;
use crate::tapirstore::TapirStore;
use crate::unified::UnifiedStore;
use crate::IrClientId;

type Store = UnifiedStore<String, String, BufferedIo>;

/// Run the tapirstore interpreter.
///
/// Reads a script from args (joined by space) or stdin, splits into
/// commands separated by `;` or newlines, and executes them sequentially
/// against a single store session.
///
/// Returns exit code: 0 on success, 1 if any command failed.
pub fn run<I, R, W, E>(args: I, mut stdin: R, mut stdout: W, mut stderr: E) -> i32
where
    I: IntoIterator<Item = String>,
    R: Read,
    W: Write,
    E: Write,
{
    // Collect args after program name
    let args: Vec<String> = args.into_iter().collect();
    let script = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        let mut buf = String::new();
        if stdin.read_to_string(&mut buf).is_err() {
            let _ = writeln!(stderr, "error: failed to read stdin");
            return 1;
        }
        buf
    };

    let mut ctx = Context {
        store: None,
        tapir_prepared_txns: BTreeMap::new(),
        ir_prepared_txns: BTreeMap::new(),
        op_counter: 0,
        had_error: false,
    };

    // Split by `;` and newlines into individual commands
    for line in script.split([';', '\n']) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        if let Err(e) = execute_command(&mut ctx, &parts, &mut stdout) {
            let _ = writeln!(stderr, "error: {e}");
            ctx.had_error = true;
        }
    }

    if ctx.had_error { 1 } else { 0 }
}

struct Context {
    store: Option<Store>,
    /// TAPIR prepared transaction payloads (independent from IR entries).
    tapir_prepared_txns: BTreeMap<OccTransactionId, PreparedTxnData>,
    /// IR prepare op tracking for optional `ir-commit` metadata linkage.
    ir_prepared_txns: BTreeMap<OccTransactionId, (OpId, u64)>,
    /// Auto-incrementing OpId counter.
    op_counter: u64,
    had_error: bool,
}

#[derive(Clone)]
struct PreparedTxnData {
    read_set: Vec<(String, Timestamp)>,
    write_set: Vec<(String, Option<String>)>,
    scan_set: Vec<(String, String, Timestamp)>,
}

impl Context {
    fn next_op_id(&mut self) -> OpId {
        self.op_counter += 1;
        OpId {
            client_id: IrClientId(1),
            number: self.op_counter,
        }
    }

    fn store(&self) -> Result<&Store, String> {
        self.store.as_ref().ok_or_else(|| "no store open".to_string())
    }

    fn store_mut(&mut self) -> Result<&mut Store, String> {
        self.store.as_mut().ok_or_else(|| "no store open".to_string())
    }
}

fn parse_ts(s: &str) -> Result<Timestamp, String> {
    let time: u64 = s.parse().map_err(|_| format!("invalid timestamp: {s}"))?;
    Ok(Timestamp {
        time,
        client_id: IrClientId(1),
    })
}

fn parse_txn_id(s: &str) -> Result<OccTransactionId, String> {
    let (client_str, num_str) = s
        .split_once(':')
        .ok_or_else(|| format!("invalid txn_id (expected client:number): {s}"))?;
    let client: u64 = client_str
        .parse()
        .map_err(|_| format!("invalid client_id: {client_str}"))?;
    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid txn number: {num_str}"))?;
    Ok(OccTransactionId {
        client_id: IrClientId(client),
        number: num,
    })
}

fn parse_kv_pair(s: &str) -> Result<(String, Option<String>), String> {
    if let Some((k, v)) = s.split_once('=') {
        Ok((k.to_string(), Some(v.to_string())))
    } else {
        // Bare key without =value means tombstone (delete)
        Ok((s.to_string(), None))
    }
}

fn parse_read_item(s: &str) -> Result<(String, Timestamp), String> {
    let (k, ts_str) = s
        .split_once('@')
        .ok_or_else(|| format!("invalid read item (expected key@ts): {s}"))?;
    Ok((k.to_string(), parse_ts(ts_str)?))
}

fn parse_prepare_payload(parts: &[&str]) -> Result<PreparedTxnData, String> {
    let mut read_set = Vec::new();
    let mut write_set = Vec::new();
    let scan_set = Vec::new();

    for item in parts {
        if let Some(rest) = item.strip_prefix("r:") {
            read_set.push(parse_read_item(rest)?);
        } else if let Some(rest) = item.strip_prefix("w:") {
            write_set.push(parse_kv_pair(rest)?);
        } else {
            // Backward-compatible shorthand: bare token is a write item.
            write_set.push(parse_kv_pair(item)?);
        }
    }

    Ok(PreparedTxnData {
        read_set,
        write_set,
        scan_set,
    })
}

fn execute_command<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    match parts[0] {
        "open" => cmd_open(ctx, parts),
        "open-with" => cmd_open_with(ctx, parts),
        "prepare" | "tapir-prepare" => cmd_tapir_prepare(ctx, parts),
        "commit" | "tapir-commit" => cmd_tapir_commit(ctx, parts),
        "ir-prepare" => cmd_ir_prepare(ctx, parts),
        "ir-commit" => cmd_ir_commit(ctx, parts),
        "get" => cmd_get(ctx, parts, stdout),
        "get-at" => cmd_get_at(ctx, parts, stdout),
        "get-range" => cmd_get_range(ctx, parts, stdout),
        "scan" => cmd_scan(ctx, parts, stdout),
        "has-writes" => cmd_has_writes(ctx, parts, stdout),
        "seal" => cmd_seal(ctx, parts),
        "status" => cmd_status(ctx, parts, stdout),
        "list-vlogs" => cmd_list_vlogs(ctx, parts, stdout),
        "dump-vlog" | "dump-ir-vlog" => cmd_dump_vlog(ctx, parts, stdout),
        "dump-tapir-vlog" => cmd_dump_tapir_vlog(ctx, parts, stdout),
        _ => Err(format!("unknown command: {}", parts[0])),
    }
}

fn cmd_open(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() != 2 {
        return Err("usage: open <dir>".to_string());
    }
    let path = PathBuf::from(parts[1]);
    let store = UnifiedStore::<String, String, BufferedIo>::open(path)
        .map_err(|e| format!("open failed: {e}"))?;
    ctx.store = Some(store);
    Ok(())
}

fn cmd_open_with(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() != 3 {
        return Err("usage: open-with <dir> <min_vlog_size>".to_string());
    }
    let path = PathBuf::from(parts[1]);
    let min_size: u64 = parts[2]
        .parse()
        .map_err(|_| format!("invalid min_vlog_size: {}", parts[2]))?;
    let store = UnifiedStore::<String, String, BufferedIo>::open_with_options(path, min_size)
        .map_err(|e| format!("open-with failed: {e}"))?;
    ctx.store = Some(store);
    Ok(())
}

fn cmd_tapir_prepare(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() < 3 {
        return Err("usage: tapir-prepare <client:num> <ts> [r:key@ts ...] [w:key=value ...]".to_string());
    }
    let txn_id = parse_txn_id(parts[1])?;
    let payload = parse_prepare_payload(&parts[3..])?;

    // Keep legacy prepare registry path populated for now (used by some
    // TapirStore methods), but TAPIR durability is commit-owned.
    let mut txn = crate::occ::Transaction::default();
    for (k, ts) in &payload.read_set {
        txn.add_read(crate::tapir::Sharded::from(k.clone()), *ts);
    }
    for (k, v) in &payload.write_set {
        txn.add_write(crate::tapir::Sharded::from(k.clone()), v.clone());
    }
    let txn = Arc::new(txn);
    let commit_ts = parse_ts(parts[2])?;
    let store = ctx.store_mut()?;
    store.register_prepare(txn_id, &txn, commit_ts);

    ctx.tapir_prepared_txns.insert(txn_id, payload);
    Ok(())
}

fn cmd_tapir_commit(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() != 3 {
        return Err("usage: tapir-commit <client:num> <ts>".to_string());
    }
    let txn_id = parse_txn_id(parts[1])?;
    let commit_ts = parse_ts(parts[2])?;

    let payload = if let Some(p) = ctx.tapir_prepared_txns.remove(&txn_id) {
        p
    } else {
        return Err(format!("no prepared transaction: {}", parts[1]));
    };

    let store = ctx.store_mut()?;
    store
        .commit_transaction_data(
            txn_id,
            &payload.read_set,
            &payload.write_set,
            &payload.scan_set,
            commit_ts,
        )
        .map_err(|e| format!("commit failed: {e}"))
}

fn cmd_ir_prepare(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() < 3 {
        return Err("usage: ir-prepare <client:num> <ts> [key=value ...]".to_string());
    }
    let txn_id = parse_txn_id(parts[1])?;
    let commit_ts = parse_ts(parts[2])?;
    let mut writes = Vec::new();
    for kv in &parts[3..] {
        writes.push(parse_kv_pair(kv)?);
    }

    let op_id = ctx.next_op_id();
    let store = ctx.store_mut()?;
    let current_view = store.current_view();
    store.insert_ir_entry(
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
    ctx.ir_prepared_txns
        .insert(txn_id, (op_id, current_view));
    Ok(())
}

fn cmd_ir_commit(ctx: &mut Context, parts: &[&str]) -> Result<(), String> {
    if parts.len() != 3 {
        return Err("usage: ir-commit <client:num> <ts>".to_string());
    }
    let txn_id = parse_txn_id(parts[1])?;
    let commit_ts = parse_ts(parts[2])?;
    let op_id = ctx.next_op_id();
    let prepare_ref = if let Some((prepare_op_id, _prepare_view)) = ctx.ir_prepared_txns.remove(&txn_id) {
        PrepareRef::SameView(prepare_op_id)
    } else {
        PrepareRef::SameView(op_id)
    };

    let store = ctx.store_mut()?;
    let current_view = store.current_view();

    store.insert_ir_entry(
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

fn cmd_dump_tapir_vlog<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    if parts.len() != 2 {
        return Err("usage: dump-tapir-vlog <segment-id>".to_string());
    }
    let seg_id: u64 = parts[1]
        .parse()
        .map_err(|_| format!("invalid segment id: {}", parts[1]))?;
    let store = ctx.store()?;
    let entries = store
        .dump_tapir_vlog_segment(seg_id)
        .map_err(|e| format!("dump-tapir-vlog failed: {e}"))?;
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

fn cmd_get<W: Write>(ctx: &mut Context, parts: &[&str], stdout: &mut W) -> Result<(), String> {
    if parts.len() != 2 {
        return Err("usage: get <key>".to_string());
    }
    let key = parts[1].to_string();
    let store = ctx.store()?;
    let (value, ts) =
        store.do_uncommitted_get(&key).map_err(|e| format!("get failed: {e}"))?;
    write_kv_result(stdout, &key, value.as_deref(), ts)
}

fn cmd_get_at<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    if parts.len() != 3 {
        return Err("usage: get-at <key> <ts>".to_string());
    }
    let key = parts[1].to_string();
    let ts = parse_ts(parts[2])?;
    let store = ctx.store()?;
    let (value, actual_ts) =
        store.do_uncommitted_get_at(&key, ts).map_err(|e| format!("get-at failed: {e}"))?;
    write_kv_result(stdout, &key, value.as_deref(), actual_ts)
}

fn cmd_get_range<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    if parts.len() != 3 {
        return Err("usage: get-range <key> <ts>".to_string());
    }
    let key = parts[1].to_string();
    let ts = parse_ts(parts[2])?;
    let store = ctx.store()?;
    let (write_ts, next_ts) =
        store.get_range(&key, ts).map_err(|e| format!("get-range failed: {e}"))?;
    let next_str = match next_ts {
        Some(t) => t.time.to_string(),
        None => "none".to_string(),
    };
    writeln!(stdout, "write_ts={} next_ts={next_str}", write_ts.time)
        .map_err(|e| format!("write failed: {e}"))
}

fn cmd_scan<W: Write>(ctx: &mut Context, parts: &[&str], stdout: &mut W) -> Result<(), String> {
    if parts.len() != 4 {
        return Err("usage: scan <start> <end> <ts>".to_string());
    }
    let start = parts[1].to_string();
    let end = parts[2].to_string();
    let ts = parse_ts(parts[3])?;
    let store = ctx.store()?;
    let results =
        store.do_uncommitted_scan(&start, &end, ts).map_err(|e| format!("scan failed: {e}"))?;
    for (key, value, entry_ts) in &results {
        write_kv_result(stdout, key, value.as_deref(), *entry_ts)?;
    }
    Ok(())
}

fn cmd_has_writes<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    if parts.len() != 5 {
        return Err("usage: has-writes <start> <end> <after_ts> <before_ts>".to_string());
    }
    let start = parts[1].to_string();
    let end = parts[2].to_string();
    let after = parse_ts(parts[3])?;
    let before = parse_ts(parts[4])?;
    let store = ctx.store()?;
    let has = store.has_writes_in_range(&start, &end, after, before)
        .map_err(|e| format!("has-writes failed: {e}"))?;
    writeln!(stdout, "{has}").map_err(|e| format!("write failed: {e}"))
}

fn cmd_seal(ctx: &mut Context, _parts: &[&str]) -> Result<(), String> {
    let store = ctx.store_mut()?;
    store
        .seal_current_view()
        .map_err(|e| format!("seal failed: {e}"))
}

fn cmd_status<W: Write>(
    ctx: &mut Context,
    _parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    let store = ctx.store()?;
    let view = store.current_view();
    let segments = store.sealed_vlog_segments().len();
    writeln!(stdout, "view={view} sealed_segments={segments}")
        .map_err(|e| format!("write failed: {e}"))
}

fn cmd_list_vlogs<W: Write>(
    ctx: &mut Context,
    _parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    let store = ctx.store()?;

    // Active segment
    let active_id = store.active_vlog_id();
    let active_size = store.active_vlog_write_offset();
    let active_views = store.active_vlog_views();
    let view_list: Vec<String> = active_views.iter().map(|v| v.view.to_string()).collect();
    writeln!(
        stdout,
        "vlog_seg_{active_id:04} size={active_size} views=[{}]",
        view_list.join(",")
    )
    .map_err(|e| format!("write failed: {e}"))?;

    // Sealed segments
    for (id, seg) in store.sealed_vlog_segments() {
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

fn cmd_dump_vlog<W: Write>(
    ctx: &mut Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    if parts.len() != 2 {
        return Err("usage: dump-vlog <segment-id>".to_string());
    }
    let seg_id: u64 = parts[1]
        .parse()
        .map_err(|_| format!("invalid segment id: {}", parts[1]))?;
    let store = ctx.store()?;
    let entries = store
        .dump_vlog_segment(seg_id)
        .map_err(|e| format!("dump-vlog failed: {e}"))?;
    for (offset, op_id, _entry_type, payload) in &entries {
        write!(
            stdout,
            "@{offset} op={}:{} ",
            op_id.client_id.0, op_id.number
        )
        .map_err(|e| format!("write failed: {e}"))?;
        match payload {
            IrPayloadInline::Prepare {
                transaction_id,
                commit_ts,
                write_set,
                ..
            } => {
                write!(
                    stdout,
                    "PREPARE txn={}:{} ts={}",
                    transaction_id.client_id.0, transaction_id.number, commit_ts.time
                )
                .map_err(|e| format!("write failed: {e}"))?;
                for (k, v) in write_set {
                    let v_str = v.as_deref().unwrap_or("<tombstone>");
                    write!(stdout, " {k}={v_str}").map_err(|e| format!("write failed: {e}"))?;
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

fn write_kv_result<W: Write>(
    stdout: &mut W,
    key: &str,
    value: Option<&str>,
    ts: Timestamp,
) -> Result<(), String> {
    let val_str = value.unwrap_or("<none>");
    writeln!(stdout, "{key}={val_str} @{}", ts.time)
        .map_err(|e| format!("write failed: {e}"))
}
