# Rust Client SDK

```
┌──────────────────────────────────────┐
│          RoutingClient               │
│  (auto-route, retry, scan merge)     │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│            Client                    │
│  (shard-aware, Sharded<K> keys)      │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│          ShardClient                 │
│  (per-shard TAPIR ops)               │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│           IrClient                   │
│  (IR protocol: propose, decide)      │
└──────────────────────────────────────┘
```

**P1 --- Layered API:** The tapirs Rust client SDK provides a layered API for interacting with a tapirs cluster. `RoutingClient` is the top-level entry point that most applications should use --- it accepts plain keys, automatically routes them to the correct shard, retries on `OutOfRange` errors during resharding with configurable exponential backoff, merges multi-shard scan results, and handles coordinator recovery transparently. Below `RoutingClient` are two internal layers (`Client` and `ShardClient`) that you normally don't interact with directly --- see [SDK Internals](rust-client-sdk-internals.md) if you need lower-level control or want to understand the client architecture.

**P2 --- Two transaction types:** Two transaction types are available: `RoutingTransaction` for read-write transactions (started with `begin()`) and `RoutingReadOnlyTransaction` for read-only transactions (started with `begin_read_only()`). RW transactions support `get()`, `put()`, `scan()`, and `commit()`. RO transactions support only `get()` and `scan()` --- no writes, no commit needed. Both handle multi-shard operations transparently. Integration is via Rust crate dependency: add tapirs to your `Cargo.toml`, construct a `RoutingClient` with a discovery URL, and call `begin()` or `begin_read_only()`. See [Choosing Consistency](choosing-consistency.md) for which transaction type to use.

**P3 --- API reference links:** For the full RW transaction API (including OCC prepare semantics, commit return values, and error handling), see [RW Transactions](rust-client-sdk-rw-txn.md). For the RO transaction API (including validated reads, quorum fallback, and snapshot semantics), see [RO Transactions](rust-client-sdk-ro-txn.md). For the lowest-cost reads when staleness is acceptable, see [Eventually Consistent Reads](rust-client-sdk-eventual-consistent-read.md). For the internal client layers (`Client`, `ShardClient`, `IrClient`) and how they map TAPIR operations to IR messages, see [SDK Internals](rust-client-sdk-internals.md). Back to [Building on tapirs](README.md). Key file: `src/tapir/routing_client.rs`

| Layer | Type | Accepts | Handles routing | Use case |
|-------|------|---------|-----------------|----------|
| `RoutingClient` | Top-level | Plain keys | Yes (auto-route, OutOfRange retry, scan merge) | Most applications |
| `Client` | Mid-level | `Sharded<K>` keys | No (caller provides shard) | Custom routing / [SDK Internals](rust-client-sdk-internals.md) |
| `ShardClient` | Low-level | Per-shard | No (single shard) | Advanced use / [SDK Internals](rust-client-sdk-internals.md) |
