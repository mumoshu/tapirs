# tapi client

```
                    Connection Modes

  +--------+        +----------+        +----------+
  | --repl |        | -e "..." |        | -s file  |
  |        |        |          |        |          |
  | Inter- |        | One-line |        | Script   |
  | active |        | evaluate |        | file     |
  | shell  |        | and exit |        | input    |
  +--------+        +----------+        +----------+
      |                  |                   |
      +------------------+-------------------+
                         |
                    tapirs cluster
```

**P1 -- Three connection modes:** `tapi client` connects to a running tapirs cluster and provides three ways to issue transactions. `--repl` opens an interactive transaction shell where you type commands one at a time. `-e "expression"` evaluates a single transaction expression and exits -- useful for scripting and CI. `-s script.txt` reads commands from a file, one per line. All three modes provide the same strict serializable transaction semantics.

**P2 -- REPL commands:** In REPL mode, seven commands are available: `begin` starts a new transaction, `get <key>` reads a value, `put <key> <value>` buffers a write, `delete <key>` buffers a deletion, `scan <start> <end>` reads a key range, `commit` finalizes the transaction (running OCC validation and IR consensus), and `abort` discards it. Multi-shard operations are handled transparently by the routing layer.

**P3 -- Related docs:** For the transaction API behind these commands, see [Transaction API](../integrate/rust-client-sdk-rw-txn.md). For cluster connection setup, see [Configuration](cli-config.md). Back to [tapi](cli-tapi.md).

| Command | Args | Description |
|---------|------|-------------|
| `begin` | -- | Start a new transaction |
| `get` | `<key>` | Read a value by key |
| `put` | `<key> <value>` | Buffer a write (applied on commit) |
| `delete` | `<key>` | Buffer a deletion (applied on commit) |
| `scan` | `<start> <end>` | Read all key-value pairs in range `[start, end)` |
| `commit` | -- | Finalize the transaction (OCC validation + IR consensus) |
| `abort` | -- | Discard the transaction |
