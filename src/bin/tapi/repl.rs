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
    DynamicRouter, RoutingClient, RoutingTransaction, TapirClient, TcpTransport,
    TapirReplica,
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

pub async fn run(
    tapir_client: Arc<TapirClient<String, String, TcpTransport<TapirReplica<String, String>>>>,
    router: Arc<DynamicRouter<String>>,
) {
    let client = Client::new(tapir_client, router);
    let stdin = io::stdin();
    let is_tty = atty_check();

    let mut active_txn: Option<Txn> = None;

    loop {
        if is_tty {
            let prompt = if active_txn.is_some() {
                "tapi:txn> "
            } else {
                "tapi> "
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
                } else {
                    active_txn = Some(client.begin());
                    println!("Transaction started.");
                }
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let Some(txn) = &active_txn else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    continue;
                };
                let key = parts[1].to_string();
                match txn.get(key.clone()).await {
                    Some(val) => println!("{key} = \"{val}\""),
                    None => println!("{key} = (not found)"),
                }
            }
            "put" => {
                if parts.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                let Some(txn) = &active_txn else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    continue;
                };
                let key = parts[1].to_string();
                let value = parts[2..].join(" ");
                txn.put(key, Some(value));
                println!("OK (buffered in write set)");
            }
            "delete" => {
                if parts.len() < 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                let Some(txn) = &active_txn else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    continue;
                };
                let key = parts[1].to_string();
                txn.put(key, None);
                println!("OK (buffered in write set)");
            }
            "scan" => {
                if parts.len() < 3 {
                    println!("Usage: scan <start> <end>");
                    continue;
                }
                let Some(txn) = &active_txn else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    continue;
                };
                let start = parts[1].to_string();
                let end = parts[2].to_string();
                let results = txn.scan(start, end).await;
                if results.is_empty() {
                    println!("(no results)");
                } else {
                    for (k, v) in &results {
                        println!("  {k} = \"{v}\"");
                    }
                }
            }
            "commit" => {
                let Some(txn) = active_txn.take() else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    continue;
                };
                match txn.commit().await {
                    Some(ts) => println!("Committed at timestamp {ts:?}."),
                    None => println!("Commit failed (transaction aborted by OCC)."),
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
                println!("  begin            Start a new transaction");
                println!("  get <key>        Read a key (requires active txn)");
                println!("  put <key> <val>  Write a key-value pair (requires active txn)");
                println!("  delete <key>     Delete a key (requires active txn)");
                println!("  scan <start> <end>  Range scan (requires active txn)");
                println!("  commit           Commit the active transaction");
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
