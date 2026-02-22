# Read-Write Transactions

```
  begin()          get/put/delete/scan          commit()
    │                     │                        │
    ▼                     ▼                        ▼
┌────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│ Create │  │   Execution phase     │  │   Commit protocol     │
│  txn   │─▶│ get: unlogged read    │─▶│ Prepare at all shards │
│        │  │ put: buffer locally   │  │ OCC check per replica │
│        │  │ scan: multi-shard     │  │ Finalize (commit/abort)│
└────────┘  └───────────────────────┘  └───────────────────────┘
```

**Execution phase:** A read-write transaction provides strict serializability --- the strongest practical consistency guarantee. Start with `routing_client.begin()`, which returns a `RoutingTransaction`. During the execution phase, `get(key)` reads from a single replica (an IR unlogged operation --- fast but individually inconsistent), `put(key, value)` buffers a write locally (synchronous, not sent to replicas yet), and `scan(start, end)` reads a key range from single replicas per shard and merges results. Writes are not visible to other transactions until commit. Deleting a key is done via `put(key, None)`.

**Commit protocol:** When you call `commit()`, the client drives the full TAPIR protocol: it sends an IR consensus Prepare to all participating shards with the transaction's read-set, write-set, scan-set, and a proposed timestamp. Each replica runs TAPIR-OCC-CHECK to validate against concurrent transactions. If f+1 replicas return PREPARE-OK (via the `decide` function), the client sends an IR inconsistent Commit to all replicas, which apply the writes to their MVCC stores. `commit()` returns `Some(timestamp)` on success or `None` on abort. If the client crashes mid-commit, backup coordinators at replicas automatically finalize the transaction --- no manual intervention required.

**Abort and retry:** Transactions that are dropped without calling `commit()` are effectively aborted. `RetryConfig` controls the exponential backoff for `OutOfRange` retries during resharding (default: max 2 retries, 100ms initial backoff, 2s max). For how OCC conflict detection works at commit time, see [OCC concepts](../learn/concepts/occ.md). For the full protocol mechanics, see [Protocol](../learn/internals/protocol-tapir.md). Back to [Rust Client SDK](rust-client-sdk.md).

```rust
let mut txn = routing_client.begin();
let balance = txn.get("account:alice").await?;
txn.put("account:alice", Some("900"));
txn.put("account:bob", Some("1100"));
match txn.commit().await {
    Some(ts) => println!("committed at {ts}"),
    None => println!("aborted — retry from begin()"),
}
```

| Method | Signature | Async | Description |
|--------|-----------|-------|-------------|
| `get(key)` | `get(K) -> Result<Option<V>, TransactionError>` | Yes | Unlogged read from single replica |
| `put(key, value)` | `put(K, Option<V>)` | No (sync) | Buffer write locally; `None` = delete |
| `scan(start, end)` | `scan(K, K) -> Result<Vec<(K, V)>, TransactionError>` | Yes | Multi-shard range read with merge |
| `commit()` | `commit(self) -> Option<Timestamp>` | Yes | OCC prepare + commit; `Some` = success |
