# SDK Internals

```
┌──────────────────────────────────────────┐
│            RoutingClient                  │
│  (plain keys, auto-route, retry)          │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│              Client                       │
│  (Sharded<K> keys, per-shard dispatch,    │
│   drives 2PC across shards)               │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│            ShardClient                    │
│  (per-shard: get->unlogged,               │
│   prepare->consensus, commit->inconsist.) │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│             IrClient                      │
│  (IR protocol: propose, collect quorum,   │
│   run decide, handle view changes)        │
└──────────────────────────────────────────┘
```

**P1 --- Client and ShardClient:** Below the `RoutingClient` that most applications use, two internal layers translate TAPIR operations into IR messages. `Client` is the shard-aware layer: it takes `Sharded<K>` keys (a key paired with an explicit shard number), maintains a `ShardClient` per shard, partitions multi-shard operations, and drives the two-phase commit protocol across participating shards. When you call `RoutingClient::begin()`, it creates a `Client`-level transaction internally and uses the `DynamicRouter` to resolve plain keys into `Sharded<K>` keys transparently. If you need to bypass automatic routing --- for example, to implement a custom sharding strategy or to interact with specific shards directly --- you can use `Client` directly with explicit shard numbers.

**P2 --- TAPIR-to-IR mapping:** `ShardClient` wraps `IrClient` and maps TAPIR operations to IR message types: `get()` becomes an IR unlogged operation (single replica, no replication), `prepare()` becomes an IR consensus operation (f+1 quorum, OCC validation at each replica), and `end()` (commit or abort) becomes an IR inconsistent operation (f+1, any order). For read-only transactions, `ShardClient` provides `read_validated()` and `scan_validated()` for the fast path (single replica, validated read), and `quorum_read()` and `quorum_scan()` for the fallback (f+1 replicas via IR inconsistent operation). `IrClient` at the bottom implements the IR protocol itself: sending proposals, collecting quorum responses, running the `decide` function, and handling view changes.

**P3 --- Routing vs discovery:** `DynamicRouter` and `CachingShardDirectory` (the [discovery](../learn/concepts/discovery.md) layer) are independent systems that serve different purposes. `DynamicRouter` reads from a `ShardDirectory<K>` --- a data structure holding key ranges --- to route keys to shard numbers. `CachingShardDirectory` syncs membership information (shard -> replica addresses) from the remote discovery backend and stores it in `InMemoryShardDirectory<A>`, which the transport layer uses to connect to replicas. During resharding, the `ShardManager` updates both: it writes new key ranges to `DynamicRouter`'s directory (so routing reflects the new shard topology) and registers new shard membership with the discovery backend (so the transport can connect to the new replicas). When `RoutingClient` receives `OutOfRange`, it retries with exponential backoff --- the backoff window gives the `ShardManager` time to update both systems. Most applications should use `RoutingClient` exclusively. For the top-level API, see [Rust Client SDK](rust-client-sdk.md). For how `ShardClient` operations map to IR protocol messages, see [Protocol](../learn/internals/protocol-tapir.md). For how the discovery layer syncs membership, see [Discovery](../learn/concepts/discovery.md). Back to [Rust Client SDK](rust-client-sdk.md).

| Layer | Type | Key file | Responsibilities |
|-------|------|----------|-----------------|
| `Client` | Shard-aware | `src/tapir/client.rs` | Accepts `Sharded<K>`, maintains per-shard `ShardClient`s, drives 2PC across shards |
| `ShardClient` | Per-shard | `src/tapir/shard_client.rs` | Maps TAPIR ops to IR messages: get->unlogged, prepare->consensus, commit->inconsistent |
| `IrClient` | IR protocol | `src/ir/client.rs` | Sends proposals, collects quorum responses, runs `decide`, handles view changes |
| `DynamicRouter` | Key routing | `src/tapir/dynamic_router.rs` | Maps plain keys to `Sharded<K>` via `ShardDirectory<K>` (key ranges, NOT membership) |
| `CachingShardDirectory` | Membership discovery | `src/discovery/mod.rs` | Syncs shard->replica addresses; used by transport layer, NOT by DynamicRouter |
