# Read-Only Transactions

```
┌───────────────────────────────────────────────┐
│              Read Paths                        │
│                                                │
│  Default path (2 RTT):                         │
│  Client ──▶ f+1 replicas ──▶ quorum read       │
│             (merge by highest write_ts,         │
│              updates read_ts)                   │
│                                                │
│  Opt-in fast path (1 RTT):                     │
│  Client ──[delay]──▶ 1 replica ──▶ validated   │
│             (set_ro_fast_path_delay)            │
│             Falls back to quorum on miss        │
└───────────────────────────────────────────────┘
```

**Snapshot reads:** A read-only transaction provides linearizability --- a consistent, up-to-date multi-key snapshot at a single point in real time. Start with `routing_client.begin_read_only(uncertainty_bound)`, which takes a mandatory `Duration` parameter for the clock skew uncertainty bound and returns a `RoutingReadOnlyTransaction`. The bound adjusts the snapshot timestamp upward and adds a commit-wait delay before each quorum read, ensuring linearizability under clock skew (see [TrueTime Uncertainty Bound](../learn/internals/protocol-tapir-paper-extensions-truetime.md)). Pass `Duration::ZERO` when clocks are perfectly synchronized (e.g., tests, single-node). All reads within the transaction see a consistent view as of the adjusted snapshot timestamp.

**Default path and opt-in fast path:** By default, each `get(key)` uses `quorum_read` --- an IR inconsistent operation that contacts f+1 replicas, merges results by highest `write_ts`, and updates the read timestamp to prevent future overwrites (2 round-trips). For lower bandwidth, the fast path can be enabled via `Client::set_ro_fast_path_delay(Some(delay))`: each cache-miss `get(key)` first sleeps for the delay (allowing view change sync to propagate committed values), then tries `read_validated` at a single replica (1 round-trip). If validation fails or times out, it falls back to `quorum_read`. The fast path timeout is configurable via `Client::set_read_timeout(Duration)` (default 2s). `scan(start, end)` works similarly: `quorum_scan` by default, `scan_validated` with fast path. Multi-shard scans are split per shard and merged transparently by the routing layer.

**Read-only by construction:** RO transactions have no `put()` method and no `commit()` --- they are read-only by construction, not by convention. The snapshot timestamp is captured at `begin_read_only(uncertainty_bound)` time, so all reads within the transaction see a consistent point-in-time view regardless of concurrent write transactions. For when to use RO vs RW transactions, see [Choosing Consistency](choosing-consistency.md). For the paper's description of validated reads, see [Paper Extensions](../learn/internals/protocol-tapir-paper-extensions.md) (section 6.1). Back to [Rust Client SDK](rust-client-sdk.md).

```rust
use std::time::Duration;
// 10ms bound: clocks synchronized within 10ms (e.g., NTP)
let txn = routing_client.begin_read_only(Duration::from_millis(10));
let alice = txn.get("account:alice").await?;
let bob = txn.get("account:bob").await?;
// Both reads see a consistent snapshot — no commit needed
println!("alice={alice:?}, bob={bob:?}");
```

| Method | Signature | Async | Read path |
|--------|-----------|-------|-----------|
| `get(key)` | `get(K) -> Result<Option<V>, TransactionError>` | Yes | Default: quorum read (2 RTT). With fast path delay: validated read (1 RTT), fallback to quorum |
| `scan(start, end)` | `scan(K, K) -> Result<Vec<(K, V)>, TransactionError>` | Yes | Default: quorum_scan (2 RTT). With fast path delay: scan_validated (1 RTT), fallback to quorum |
