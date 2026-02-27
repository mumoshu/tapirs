# Choosing a Consistency Level

```
                    ┌──────────────┐
                    │ Need writes? │
                    └──────┬───────┘
                     yes/      \no
                       ▼        ▼
              ┌──────────┐  ┌────────────────┐
              │ RW txn   │  │ Need freshness?│
              │ begin()  │  └───────┬────────┘
              └──────────┘    yes/      \no
                               ▼        ▼
                    ┌────────────┐  ┌──────────────┐
                    │  RO txn    │  │  Eventual    │
                    │begin_ro(ε) │  │  read        │
                    └────────────┘  └──────────────┘
```

**Three levels, one decision:** tapirs exposes three consistency levels through its client API, and the right choice depends on whether your operation writes data, how fresh your reads need to be, and how much latency you can tolerate. The decision is straightforward: if you write, use a [read-write transaction](rust-client-sdk-rw-txn.md) (`begin()`); if you only read and need an up-to-date snapshot, use a [read-only transaction](rust-client-sdk-ro-txn.md) (`begin_read_only(uncertainty_bound)`) with an uncertainty bound ε >= max clock skew; if staleness is acceptable, use [eventually consistent reads](rust-client-sdk-eventual-consistent-read.md) --- individual reads within an RW transaction without committing.

**Protocol mapping:** The three levels map directly to TAPIR's protocol design. Read-write transactions go through the full prepare phase (IR consensus at all participating shards), giving you strict serializability --- the strongest guarantee, where all transactions appear to execute in a single global order that respects real time. Read-only transactions skip the prepare phase and default to quorum reads (f+1 replicas, 2 round-trips), giving you linearizability at lower cost. `begin_read_only(ε)` takes a mandatory uncertainty bound (ε >= max clock skew) to ensure linearizability under clock skew --- pass `Duration::ZERO` for perfectly synchronized clocks. An opt-in fast path (`set_ro_fast_path_delay()`) reduces to 1 round-trip when conditions allow. Eventual consistency comes from the fact that each individual `get()` or `scan()` within an RW transaction is an unlogged read to a single replica --- fast but potentially stale. tapirs itself uses this mixed pattern for its TAPIR-backed discovery service: shard registration writes are strict serializable, but directory sync reads are eventually consistent.

**Trade-off summary:** The table below summarizes the trade-offs. For the full programming interface of each transaction type, see [RW Transactions](rust-client-sdk-rw-txn.md) and [RO Transactions](rust-client-sdk-ro-txn.md). For the theory behind these consistency levels, see [Consistency concepts](../learn/concepts/consistency.md). For the Rust SDK overview, see [Rust Client SDK](rust-client-sdk.md). Back to [Building on tapirs](README.md).

| Use case | API | Consistency | Latency | When to use |
|----------|-----|-------------|---------|-------------|
| Read + write | `begin()` -> `get/put/scan` -> `commit()` | Strict serializability | 2 RT (execute + prepare) | Any operation that modifies data |
| Read-only, must be fresh | `begin_read_only(ε)` -> `get/scan` | Linearizability | 2 RT (quorum); opt-in fast path: 1 RT | Dashboards, read APIs, point-in-time queries. ε >= max clock skew |
| Read-only, staleness OK | `begin()` -> `get/scan` (no commit) | Eventual (per-read) | 1 RT (unlogged, single replica) | Background sync, caching, discovery |
| Mixed: strong writes + weak reads | RW for writes, RW reads for background sync | Mixed | Varies | tapirs' own TAPIR-backed discovery pattern |
