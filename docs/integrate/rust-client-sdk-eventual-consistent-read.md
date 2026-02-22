# Eventually Consistent Reads

```
┌─────────┐     IR Unlogged      ┌───────────┐
│  Client  │────────────────────▶│ 1 replica  │
└─────────┘     1 round-trip     └───────────┘
                No prepare
                No quorum
                No validation
```

**Lowest latency, lowest cost:** Not every read needs to be linearizable. When your application can tolerate brief staleness --- for background syncing, caching, warm-up reads, or metadata lookups --- eventually consistent reads provide the lowest latency at the lowest cost. In tapirs, eventually consistent reads are simply individual `get()` or `scan()` calls within a read-write transaction that is never committed. Each call is an IR unlogged operation to a single replica: one round-trip, no prepare phase, no quorum, no validation. The read may return a version that is slightly behind the latest committed state, but it will always return a valid committed version --- never a partially-applied or uncommitted write.

**Dog-food pattern:** tapirs itself uses this exact pattern for its TAPIR-backed discovery service. The `CachingShardDirectory` periodically pulls shard membership from the remote backend using eventually consistent reads --- it doesn't need to see the absolute latest membership on every sync cycle, because a few milliseconds of staleness in shard routing is harmless (the transport retries with exponential backoff if a request lands on a stale shard). Meanwhile, shard registration writes --- when a new shard comes online or a shard's membership changes --- use strict serializable read-write transactions, because correctness requires that registration is globally ordered and durable. This mixed-consistency pattern demonstrates that eventual reads and strong writes coexist naturally within the same tapirs cluster.

**When to use something stronger:** The key trade-off: eventual reads are fast (1 RTT to 1 replica) but may miss writes that committed after the replica's last sync. If your application needs a consistent multi-key snapshot --- where all reads see the same point-in-time state --- use a [read-only transaction](rust-client-sdk-ro-txn.md) instead. If your application needs both reads and writes in the same operation, use a [read-write transaction](rust-client-sdk-rw-txn.md) --- the reads will be eventually consistent individually, but OCC validation at commit time ensures the transaction as a whole is strictly serializable. For deciding which API to use, see [Choosing Consistency](choosing-consistency.md). Back to [Rust Client SDK](rust-client-sdk.md).

| Property | Value | Comparison |
|----------|-------|------------|
| Latency | 1 RTT to 1 replica | Same as RO fast path; cheaper than RO quorum fallback |
| Consistency | Eventually consistent (single-replica, unvalidated) | Weaker than RO (linearizable) and RW commit (strict serializable) |
| Replicas contacted | 1 | RO fast path: 1; RO fallback: f+1; RW prepare: all shards |
| Freshness guarantee | Returns a valid committed version, but may be stale | RO: consistent snapshot. RW commit: validated against concurrent txns |
| Use case | Background sync, caching, metadata, warm-up | RO: dashboards, APIs. RW: any writes |
| tapirs dog-food | `CachingShardDirectory` membership sync reads | Registration writes use RW transactions |
