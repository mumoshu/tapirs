# Error Handling

```
┌───────────┐      ┌─────────┐
│ Operation │─────▶│ Error?  │
└───────────┘      └────┬────┘
                   yes/ │ \no
                     ▼  │  ▼
          ┌──────────┐  │  ┌─────────┐
          │ Transient│  │  │ Success │
          └─────┬────┘  │  └─────────┘
                │       │
       ┌────────┴────────┐
       ▼                 ▼
┌──────────────┐  ┌──────────────────┐
│  Automatic   │  │  Application     │
│  Retry       │  │  Retry / Abort   │
│ (OutOfRange, │  │ (Conflict,       │
│  ViewChanging│  │  Timeout,        │
│  )           │  │  ShardUnavail.)  │
└──────────────┘  └──────────────────┘
```

**P1 --- Transient vs application errors:** Errors in tapirs fall into two categories: transient errors that the SDK handles automatically, and application-level errors that your code needs to handle. `OutOfRange` errors (caused by resharding moving a key to a different shard) are handled automatically by `RoutingClient` with configurable exponential backoff --- your application never sees them. `ViewChanging` errors (caused by an IR view change in progress) are also handled automatically by the transport layer's retry mechanism. These transient errors are a normal part of a distributed system's lifecycle and do not indicate a problem.

**P2 --- Application-level errors:** Application-level errors require your code to react. When `commit()` returns `None`, the transaction was aborted due to an OCC conflict --- a concurrent transaction wrote to a key in your read-set or read a key in your write-set. The standard response is to retry the entire transaction from `begin()` with a fresh read-set and write-set. `TransactionError` variants include `Conflict` (OCC abort), `Timeout` (replica unreachable within deadline), and `ShardUnavailable` (all replicas in a shard are down). For `Timeout` and `ShardUnavailable`, retrying may succeed if the underlying issue is transient, but persistent failures indicate a cluster health problem --- check [Monitoring](../operate/monitoring.md).

**P3 --- RetryConfig tuning:** `RetryConfig` controls the automatic retry behavior for `OutOfRange` errors: `max_retries` (default 2), `initial_backoff` (default 100ms), `max_backoff` (default 2s), and `backoff_multiplier` (default 2.0). For most applications, the defaults are sufficient. Increase `max_retries` if your cluster performs frequent resharding operations. For application-level retry of OCC conflicts, implement your own retry loop around the full transaction --- do not retry individual operations within a transaction, as the read-set may be stale. For the full method signatures, see [RW Transactions](rust-client-sdk-rw-txn.md) and [RO Transactions](rust-client-sdk-ro-txn.md). Back to [Rust Client SDK](rust-client-sdk.md).

| Error | Source | Automatic? | Application action |
|-------|--------|------------|-------------------|
| `OutOfRange` | Key moved during resharding | Yes (RoutingClient retries with backoff) | None --- transparent to application |
| `ViewChanging` | IR view change in progress | Yes (transport retries with backoff) | None --- transparent to application |
| `Conflict` (commit -> None) | OCC abort | No | Retry full transaction from `begin()` |
| `Timeout` | Replica unreachable | No | Retry transaction; check cluster health if persistent |
| `ShardUnavailable` | All replicas down | No | Retry transaction; check cluster health |
