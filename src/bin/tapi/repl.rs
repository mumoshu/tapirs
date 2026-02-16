//! # Transactional REPL
//!
//! TAPIR is a transactional KV store — operations must happen within explicit
//! transactions (begin -> ops -> commit/abort). One-shot `get`/`put` commands
//! would hide the transactional nature and mislead users into thinking this
//! is a simple KV store like Redis or Memcached.
//!
//! The REPL makes transaction boundaries explicit: `begin` starts a
//! transaction, `get`/`put`/`delete`/`scan` operate within it,
//! `commit`/`abort` finalizes. Script/pipe mode comes for free via stdin.

use std::io::{self, BufRead, Write};
use std::sync::Arc;
use tapirs::{
    DynamicRouter, RoutingClient, RoutingReadOnlyTransaction, RoutingTransaction, TapirClient,
    TcpTransport, TapirReplica,
};

type Client = RoutingClient<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    DynamicRouter<String>,
>;

type Txn = RoutingTransaction<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    DynamicRouter<String>,
>;

type RoTxn = RoutingReadOnlyTransaction<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    DynamicRouter<String>,
>;

enum ActiveTxn {
    ReadWrite(Txn),
    ReadOnly(RoTxn),
}

pub async fn run(
    tapir_client: Arc<TapirClient<String, String, TcpTransport<TapirReplica<String, String>>>>,
    router: Arc<DynamicRouter<String>>,
) {
    let client = Client::new(tapir_client, router);
    let stdin = io::stdin();
    let is_tty = atty_check();

    let mut active_txn: Option<ActiveTxn> = None;

    loop {
        if is_tty {
            let prompt = match &active_txn {
                Some(ActiveTxn::ReadWrite(_)) => "tapi:txn> ",
                Some(ActiveTxn::ReadOnly(_)) => "tapi:ro> ",
                None => "tapi> ",
            };
            print!("{prompt}");
            io::stdout().flush().ok();
        }

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Err(_) => break,
            Ok(_) => {}
        }

        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "begin" => {
                if active_txn.is_some() {
                    println!("Error: transaction already active. Use 'commit' or 'abort' first.");
                } else if parts.get(1) == Some(&"ro") {
                    active_txn = Some(ActiveTxn::ReadOnly(client.begin_read_only()));
                    println!("Read-only transaction started.");
                } else {
                    active_txn = Some(ActiveTxn::ReadWrite(client.begin()));
                    println!("Transaction started.");
                }
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let key = parts[1].to_string();
                let result = match &active_txn {
                    Some(ActiveTxn::ReadWrite(txn)) => txn.get(key.clone()).await,
                    Some(ActiveTxn::ReadOnly(txn)) => txn.get(key.clone()).await,
                    None => {
                        println!("Error: no active transaction. Use 'begin' to start one.");
                        continue;
                    }
                };
                match result {
                    Some(val) => println!("{key} = \"{val}\""),
                    None => println!("{key} = (not found)"),
                }
            }
            "put" => {
                if parts.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                match &active_txn {
                    Some(ActiveTxn::ReadWrite(txn)) => {
                        let key = parts[1].to_string();
                        let value = parts[2..].join(" ");
                        txn.put(key, Some(value));
                        println!("OK (buffered in write set)");
                    }
                    Some(ActiveTxn::ReadOnly(_)) => {
                        println!("Error: 'put' is not available in a read-only transaction.");
                    }
                    None => {
                        println!("Error: no active transaction. Use 'begin' to start one.");
                    }
                }
            }
            "delete" => {
                if parts.len() < 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                match &active_txn {
                    Some(ActiveTxn::ReadWrite(txn)) => {
                        let key = parts[1].to_string();
                        txn.put(key, None);
                        println!("OK (buffered in write set)");
                    }
                    Some(ActiveTxn::ReadOnly(_)) => {
                        println!("Error: 'delete' is not available in a read-only transaction.");
                    }
                    None => {
                        println!("Error: no active transaction. Use 'begin' to start one.");
                    }
                }
            }
            "scan" => {
                if parts.len() < 3 {
                    println!("Usage: scan <start> <end>");
                    continue;
                }
                let start = parts[1].to_string();
                let end = parts[2].to_string();
                let results = match &active_txn {
                    Some(ActiveTxn::ReadWrite(txn)) => txn.scan(start, end).await,
                    Some(ActiveTxn::ReadOnly(txn)) => txn.scan(start, end).await,
                    None => {
                        println!("Error: no active transaction. Use 'begin' to start one.");
                        continue;
                    }
                };
                if results.is_empty() {
                    println!("(no results)");
                } else {
                    for (k, v) in &results {
                        println!("  {k} = \"{v}\"");
                    }
                }
            }
            "commit" => {
                if matches!(&active_txn, Some(ActiveTxn::ReadOnly(_))) {
                    println!("Error: read-only transactions cannot be committed. Use 'abort' to end.");
                } else if let Some(ActiveTxn::ReadWrite(txn)) = active_txn.take() {
                    match txn.commit().await {
                        Some(ts) => println!("Committed at timestamp {ts:?}."),
                        None => println!("Commit failed (transaction aborted by OCC)."),
                    }
                } else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                }
            }
            "abort" => {
                if active_txn.take().is_none() {
                    println!("Error: no active transaction.");
                } else {
                    println!("Transaction aborted.");
                }
            }
            "help" => {
                println!("Commands:");
                println!("  begin            Start a read-write transaction");
                println!("  begin ro         Start a read-only transaction");
                println!("  get <key>        Read a key (requires active txn)");
                println!("  put <key> <val>  Write a key-value pair (read-write txn only)");
                println!("  delete <key>     Delete a key (read-write txn only)");
                println!("  scan <start> <end>  Range scan (requires active txn)");
                println!("  commit           Commit the active transaction (read-write only)");
                println!("  abort            Abort the active transaction");
                println!("  help             Show this help");
                println!("  quit / exit      Exit the REPL");
            }
            "quit" | "exit" => break,
            other => {
                println!("Unknown command: {other}. Type 'help' for available commands.");
            }
        }
    }

    if active_txn.is_some() {
        println!("Warning: active transaction was aborted on exit.");
    }
}

fn atty_check() -> bool {
    unsafe { libc::isatty(libc::STDIN_FILENO) != 0 }
}
