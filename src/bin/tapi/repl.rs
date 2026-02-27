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

use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tapirs::{
    DynamicRouter, RoutingClient, RoutingReadOnlyTransaction, RoutingTransaction, TapirClient,
    TcpTransport, TapirReplica,
};

/// Where the REPL reads commands from.
pub enum InputSource {
    /// Interactive or piped stdin.
    Stdin,
    /// Inline commands from `-e` flags. Semicolons separate commands within
    /// each string; multiple `-e` flags are concatenated.
    Commands(Vec<String>),
    /// Commands from a script file.
    File(PathBuf),
}

/// Internal input abstraction to handle stdin lifetime issues.
enum Input {
    /// TTY stdin — read line-by-line interactively.
    Interactive,
    /// Pre-read lines (piped stdin, -e commands, or script file).
    Lines(std::vec::IntoIter<String>),
}

type TapirTcpClient = TapirClient<String, String, TcpTransport<TapirReplica<String, String>>>;

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
    tapir_client: Arc<TapirTcpClient>,
    router: Arc<DynamicRouter<String>>,
    input_source: InputSource,
) -> i32 {
    let client = Client::new(tapir_client, router);

    let mut input = match input_source {
        InputSource::Stdin if atty_check() => Input::Interactive,
        InputSource::Stdin => {
            let lines: Vec<String> = io::stdin()
                .lock()
                .lines()
                .map(|l| l.expect("stdin read error"))
                .collect();
            Input::Lines(lines.into_iter())
        }
        InputSource::Commands(cmds) => {
            let lines: Vec<String> = cmds
                .iter()
                .flat_map(|s| s.split(';').map(|part| part.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect();
            Input::Lines(lines.into_iter())
        }
        InputSource::File(path) => {
            let file = std::fs::File::open(&path).unwrap_or_else(|e| {
                eprintln!("error: cannot open script file '{}': {e}", path.display());
                std::process::exit(2);
            });
            let lines: Vec<String> = BufReader::new(file)
                .lines()
                .map(|l| l.expect("script read error"))
                .collect();
            Input::Lines(lines.into_iter())
        }
    };

    let is_tty = matches!(input, Input::Interactive);
    let mut active_txn: Option<ActiveTxn> = None;
    let mut exit_code: i32 = 0;

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

        let line = match &mut input {
            Input::Interactive => {
                let mut buf = String::new();
                match io::stdin().lock().read_line(&mut buf) {
                    Ok(0) => break,
                    Err(_) => break,
                    Ok(_) => buf,
                }
            }
            Input::Lines(iter) => match iter.next() {
                Some(l) => l,
                None => break,
            },
        };

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "begin" => {
                if active_txn.is_some() {
                    println!("Error: transaction already active. Use 'commit' or 'abort' first.");
                } else if parts.get(1) == Some(&"ro") {
                    active_txn = Some(ActiveTxn::ReadOnly(client.begin_read_only(std::time::Duration::ZERO)));
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
                    Ok(Some(val)) => println!("{key} = \"{val}\""),
                    Ok(None) => println!("{key} = (not found)"),
                    Err(e) => println!("{key} = (error: {e:?})"),
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
                let results = match results {
                    Ok(r) => r,
                    Err(e) => {
                        println!("Error: {e:?}");
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
                    if !is_tty {
                        exit_code = 2;
                    }
                } else if let Some(ActiveTxn::ReadWrite(txn)) = active_txn.take() {
                    match txn.commit().await {
                        Some(ts) => println!("Committed at timestamp {ts:?}."),
                        None => {
                            println!("Commit failed (transaction aborted by OCC).");
                            if !is_tty {
                                exit_code = 1;
                            }
                        }
                    }
                } else {
                    println!("Error: no active transaction. Use 'begin' to start one.");
                    if !is_tty {
                        exit_code = 2;
                    }
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
                if !is_tty {
                    exit_code = 2;
                }
            }
        }
    }

    if matches!(&active_txn, Some(ActiveTxn::ReadWrite(_))) {
        println!("Warning: active read-write transaction was aborted on exit.");
    }

    exit_code
}

fn atty_check() -> bool {
    unsafe { libc::isatty(libc::STDIN_FILENO) != 0 }
}
