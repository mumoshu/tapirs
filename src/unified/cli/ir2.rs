use std::path::PathBuf;

use crate::storage::io::disk_io::{BufferedIo, OpenFlags};
use crate::unified::ir::ir_record_store::PersistentIrRecordStore;

use super::Ir2Context;

const CLI_IO_FLAGS: OpenFlags = OpenFlags {
    create: true,
    direct: false,
};

pub(super) fn execute_ir2_command<W: std::io::Write>(
    ctx: &mut Ir2Context,
    parts: &[&str],
    stdout: &mut W,
) -> Result<(), String> {
    match parts[0] {
        "open" => {
            if parts.len() != 2 {
                return Err("usage: open <dir>".to_string());
            }
            let path = PathBuf::from(parts[1]);
            let store =
                PersistentIrRecordStore::<String, String, String, BufferedIo>::open(
                    &path,
                    CLI_IO_FLAGS,
                )
                .map_err(|e| format!("open failed: {e}"))?;
            ctx.store = Some(store);
            Ok(())
        }
        "seal" => {
            if parts.len() != 2 {
                return Err("usage: seal <new_view>".to_string());
            }
            let new_view: u64 = parts[1]
                .parse()
                .map_err(|_| format!("invalid view: {}", parts[1]))?;
            let store = ctx.store_mut()?;
            store
                .seal(new_view)
                .map_err(|e| format!("seal failed: {e}"))
        }
        "status" => {
            let store = ctx.store()?;
            writeln!(stdout, "base_view={}", store.base_view())
                .map_err(|e| format!("write failed: {e}"))
        }
        other => Err(format!("unknown command: {other}")),
    }
}
