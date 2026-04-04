# Paper Extensions

```
Paper Extensions (S5.2.3, S6)
+-------------------------------------------+
|                                           |
|  RO Quorum Path (S6.1) — DEFAULT        |
|  +-------------------------------------+ |
|  | QuorumRead / QuorumScan              | |
|  | IR inconsistent op, f+1 replicas     | |
|  | begin_read_only(ε): mandatory        | |
|  | uncertainty bound for linearizability| |
|  +-------------------------------------+ |
|                                           |
|  RO Fast Path (S6.1) — OPT-IN          |
|  +-------------------------------------+ |
|  | get_validated / scan_validated       | |
|  | Opt-in: set_ro_fast_path_delay()    | |
|  | Saves bandwidth (1 vs f+1 replicas) | |
|  | Falls back to quorum on miss        | |
|  +-------------------------------------+ |
|                                           |
|  Coordinator Recovery (S5.2.3)          |
|  +-------------------------------------+ |
|  | Backup shard polls participants      | |
|  | Modified decide: f+1 NO-VOTE->abort | |
|  +-------------------------------------+ |
|                                           |
|  Retry Timestamps (S6.4)               |
|  +-------------------------------------+ |
|  | Exponential increase on retry        | |
|  +-------------------------------------+ |
|                                           |
+-------------------------------------------+
```

**RO quorum path (default, mandatory uncertainty bound):** `begin_read_only(ε)` takes the uncertainty bound as a mandatory `Duration` parameter. The bound addresses a fundamental limitation: under any clock skew δ, `snapshot_ts = local_time` can be less than a completed write's `commit_ts` (assigned by a faster-clock node), causing `quorum_read` to miss the write — a linearizability violation. Paper [S6.1](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf): "this read-only protocol would provide linearizability guarantees only if the clock skew did not exceed the TrueTime bound." The bound ε adjusts `snapshot_ts` to `local_time + ε` and adds a commit-wait delay before each `quorum_read`/`quorum_scan`, ensuring all writes with `commit_ts ≤ snapshot_ts` have completed. Passing `Duration::ZERO` explicitly documents the assumption of perfectly synchronized clocks (e.g., in-process transport in tests). See [TrueTime Uncertainty Bound](protocol-tapir-paper-extensions-truetime.md) for the full correctness argument with timelines.

**RO fast path (opt-in):** The TAPIR paper ([S6.1](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf)) describes a single round-trip fast path where a replica returns immediately if it has a validated version (`write_ts < snapshot_ts < read_ts`). This is unsafe under partitions — `commit_get` can set `read_ts` on stale replicas, causing `get_validated` to trust stale MVCC data (see [Gotcha §19](ir-tapir-gotchas.md#19-ro-fast-path-returns-stale-data-under-partitions)). tapirs makes it opt-in via `Client::set_ro_fast_path_delay(Some(delay))`: each cache-miss `get()`/`scan()` sleeps for the delay (allowing view change sync to propagate), then tries `read_validated`/`scan_validated` (1 replica). On failure or timeout, falls back to quorum. Configurable timeout via `Client::set_read_timeout(Duration)` (default 2s). The fast path saves bandwidth (1 vs f+1 replicas) but requires sufficient delay and uncertainty bound for linearizability.

**Recovery and retry:** Coordinator recovery ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S5.2.3): if a client crashes mid-commit, the backup shard's replicas recover the transaction using Bernstein's cooperative termination protocol adapted for IR. The modified `decide` function (Paper Fig. 13) requires f+1 NO-VOTE to safely abort -- because the original coordinator may have already received PREPARE-OK and committed. Retry timestamp selection (S6.4): clients exponentially increase proposed timestamps on retry to reduce conflict likelihood.

**Related docs:** See [Protocol](protocol-tapir.md) for the base RW protocol, [Custom Extensions](protocol-tapir-custom-extensions.md) for tapirs-specific additions, [TrueTime Uncertainty Bound](protocol-tapir-paper-extensions-truetime.md) for the RO linearizability correctness argument, and [Consistency](../concepts/consistency.md) for how RO linearizability differs from RW strict serializability. Key files: `src/tapir/client.rs` (RO transactions, uncertainty bound), `src/tapir/replica.rs` (coordinator recovery).

| Extension | Paper S | How it works | Key file |
|-----------|---------|-------------|----------|
| RO quorum path | S6.1 | QuorumRead/QuorumScan, f+1 replicas. Default. Requires `begin_read_only(ε)` for linearizability | `tapir/shard_client.rs` |
| TrueTime uncertainty bound | S6.1 | Mandatory ε in `begin_read_only()`: adjusts snapshot_ts + commit-wait. See [deep dive](protocol-tapir-paper-extensions-truetime.md) | `tapir/client.rs` |
| RO fast path (get) | S6.1 | `get_validated` — opt-in via `set_ro_fast_path_delay()`, falls back to quorum | `unified/tapir/occ_cache.rs` |
| RO fast path (scan) | S6.1 | `scan_validated` — opt-in via `set_ro_fast_path_delay()`, falls back to quorum | `unified/tapir/occ_cache.rs` |
| Read timeout | — | Configurable timeout for fast path `read_validated` (default 2s) | `tapir/shard_client.rs` |
| Coordinator recovery | S5.2.3 | Backup shard polls; modified decide: f+1 NO-VOTE to abort | `tapir/replica.rs` |
| Retry timestamp | S6.4 | Exponentially increase proposed timestamp on retry | `tapir/client.rs` |
