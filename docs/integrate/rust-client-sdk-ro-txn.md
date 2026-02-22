# Read-Only Transactions

```
┌───────────────────────────────────────────────┐
│              Read Paths                        │
│                                                │
│  Fast path (1 RTT):                            │
│  Client ──▶ 1 replica ──▶ validated read       │
│             (write_ts < snap_ts < read_ts)     │
│                                                │
│  Slow path (2 RTT):                            │
│  Client ──▶ f+1 replicas ──▶ quorum read       │
│             (updates read_ts to prevent         │
│              future overwrites)                 │
└───────────────────────────────────────────────┘
```

**Snapshot reads:** A read-only transaction provides linearizability --- a consistent, up-to-date multi-key snapshot at a single point in real time. Start with `routing_client.begin_read_only()`, which returns a `RoutingReadOnlyTransaction` and captures a snapshot timestamp. All reads within the transaction see a consistent view as of that timestamp. There are no writes, no prepare phase, and no commit --- just reads. This makes RO transactions significantly cheaper than RW transactions: the fast path completes in a single round-trip to one replica.

**Fast path and fallback:** Under the hood, each `get(key)` first tries the fast path: `read_validated` checks if a single replica has a version where `write_ts < snapshot_ts < read_ts`, proving the version is both current and won't be overwritten by a concurrent transaction. If validation succeeds, the read completes in one round-trip. If it fails (the replica doesn't have a covering read timestamp), the client falls back to `quorum_read` --- an IR inconsistent operation that contacts f+1 replicas and updates the read timestamp to prevent future overwrites. `scan(start, end)` works similarly: `scan_validated` tries the fast path, falling back to `quorum_scan`. Multi-shard scans are split per shard and merged transparently by the routing layer.

**Read-only by construction:** RO transactions have no `put()` method and no `commit()` --- they are read-only by construction, not by convention. The snapshot timestamp is captured at `begin_read_only()` time, so all reads within the transaction see a consistent point-in-time view regardless of concurrent write transactions. For when to use RO vs RW transactions, see [Choosing Consistency](choosing-consistency.md). For the paper's description of validated reads, see [Paper Extensions](../learn/internals/protocol-tapir-paper-extensions.md) (section 6.1). Back to [Rust Client SDK](rust-client-sdk.md).

```rust
let txn = routing_client.begin_read_only();
let alice = txn.get("account:alice").await?;
let bob = txn.get("account:bob").await?;
// Both reads see a consistent snapshot — no commit needed
println!("alice={alice:?}, bob={bob:?}");
```

| Method | Signature | Async | Read path |
|--------|-----------|-------|-----------|
| `get(key)` | `get(K) -> Result<Option<V>, TransactionError>` | Yes | Fast: validated read (1 RTT). Slow: quorum read (2 RTT) |
| `scan(start, end)` | `scan(K, K) -> Result<Vec<(K, V)>, TransactionError>` | Yes | Fast: scan_validated (1 RTT). Slow: quorum_scan (2 RTT) |
