use std::collections::BTreeMap;
use std::io::{Read, Write};

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::BufferedIo;
use crate::occ::{SharedTransaction, Transaction};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::unified::ir::ir_record_store::PersistentIrRecordStore;
use crate::unified::ir::record::IrRecord;
use crate::unified::tapir::persistent_store::PersistentTapirStore;
use crate::IrClientId;
use std::sync::Arc;

mod ir;
mod ir2;
mod tapir;

const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

type Tapir = PersistentTapirStore<String, String, BufferedIo>;
type Ir = IrRecord<String, String, BufferedIo>;
type Ir2 = PersistentIrRecordStore<String, String, String, BufferedIo>;

pub fn run<I, R, W, E>(args: I, mut stdin: R, mut stdout: W, mut stderr: E) -> i32
where
    I: IntoIterator<Item = String>,
    R: Read,
    W: Write,
    E: Write,
{
    let args: Vec<String> = args.into_iter().collect();
    if args.len() < 2 {
        let _ = writeln!(stderr, "usage: tapirstore <tapir|ir|ir2> <script | stdin>");
        return 1;
    }

    let mode = match args[1].as_str() {
        "tapir" => Mode::Tapir,
        "ir" => Mode::Ir,
        "ir2" => Mode::Ir2,
        other => {
            let _ = writeln!(stderr, "error: unknown mode: {other}");
            let _ = writeln!(stderr, "usage: tapirstore <tapir|ir|ir2> <script | stdin>");
            return 1;
        }
    };

    let script = if args.len() > 2 {
        args[2..].join(" ")
    } else {
        let mut buf = String::new();
        if stdin.read_to_string(&mut buf).is_err() {
            let _ = writeln!(stderr, "error: failed to read stdin");
            return 1;
        }
        buf
    };

    let mut had_error = false;
    match mode {
        Mode::Tapir => {
            let mut ctx = TapirContext {
                store: None,
                min_view_vlog_size: DEFAULT_MIN_VIEW_VLOG_SIZE,
            };
            for line in script.split([';', '\n']) {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }
                if let Err(e) = tapir::execute_tapir_command(&mut ctx, &parts, &mut stdout) {
                    let _ = writeln!(stderr, "error: {e}");
                    had_error = true;
                }
            }
        }
        Mode::Ir => {
            let mut ctx = IrContext {
                store: None,
                prepared: BTreeMap::new(),
                op_counter: 0,
                min_view_vlog_size: DEFAULT_MIN_VIEW_VLOG_SIZE,
            };
            for line in script.split([';', '\n']) {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }
                if let Err(e) = ir::execute_ir_command(&mut ctx, &parts, &mut stdout) {
                    let _ = writeln!(stderr, "error: {e}");
                    had_error = true;
                }
            }
        }
        Mode::Ir2 => {
            let mut ctx = Ir2Context { store: None };
            for line in script.split([';', '\n']) {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }
                if let Err(e) = ir2::execute_ir2_command(&mut ctx, &parts, &mut stdout) {
                    let _ = writeln!(stderr, "error: {e}");
                    had_error = true;
                }
            }
        }
    }

    if had_error { 1 } else { 0 }
}

struct TapirContext {
    store: Option<Tapir>,
    min_view_vlog_size: u64,
}

impl TapirContext {
    fn store(&self) -> Result<&Tapir, String> {
        self.store.as_ref().ok_or_else(|| "no store open".to_string())
    }

    fn store_mut(&mut self) -> Result<&mut Tapir, String> {
        self.store.as_mut().ok_or_else(|| "no store open".to_string())
    }
}

struct IrContext {
    store: Option<Ir>,
    prepared: BTreeMap<OccTransactionId, (OpId, u64)>,
    op_counter: u64,
    min_view_vlog_size: u64,
}

impl IrContext {
    fn next_op_id(&mut self) -> OpId {
        self.op_counter += 1;
        OpId {
            client_id: IrClientId(1),
            number: self.op_counter,
        }
    }

    fn store(&self) -> Result<&Ir, String> {
        self.store.as_ref().ok_or_else(|| "no store open".to_string())
    }

    fn store_mut(&mut self) -> Result<&mut Ir, String> {
        self.store.as_mut().ok_or_else(|| "no store open".to_string())
    }
}

struct Ir2Context {
    store: Option<Ir2>,
}

impl Ir2Context {
    fn store(&self) -> Result<&Ir2, String> {
        self.store.as_ref().ok_or_else(|| "no store open".to_string())
    }

    fn store_mut(&mut self) -> Result<&mut Ir2, String> {
        self.store.as_mut().ok_or_else(|| "no store open".to_string())
    }
}

enum Mode {
    Tapir,
    Ir,
    Ir2,
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
        Ok((s.to_string(), None))
    }
}

fn parse_read_item(s: &str) -> Result<(String, Timestamp), String> {
    let (k, ts_str) = s
        .split_once('@')
        .ok_or_else(|| format!("invalid read item (expected key@ts): {s}"))?;
    Ok((k.to_string(), parse_ts(ts_str)?))
}

fn parse_tapir_transaction(parts: &[&str]) -> Result<SharedTransaction<String, String, Timestamp>, String> {
    let mut txn = Transaction::<String, String, Timestamp>::default();

    for item in parts {
        if let Some(rest) = item.strip_prefix("r:") {
            let (key, ts) = parse_read_item(rest)?;
            txn.add_read(
                Sharded {
                    shard: ShardNumber(0),
                    key,
                },
                ts,
            );
        } else if let Some(rest) = item.strip_prefix("w:") {
            let (key, value) = parse_kv_pair(rest)?;
            txn.add_write(
                Sharded {
                    shard: ShardNumber(0),
                    key,
                },
                value,
            );
        } else {
            let (key, value) = parse_kv_pair(item)?;
            txn.add_write(
                Sharded {
                    shard: ShardNumber(0),
                    key,
                },
                value,
            );
        }
    }

    Ok(Arc::new(txn))
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
