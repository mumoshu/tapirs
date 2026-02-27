# Paper Extensions

```
Paper Extensions (S5.2.3, S6)
+-------------------------------------------+
|                                           |
|  RO Fast Path (S6.1) — DISABLED         |
|  +-------------------------------------+ |
|  | get_validated / scan_validated       | |
|  | Unsafe under partitions: commit_get  | |
|  | sets read_ts on stale replicas,      | |
|  | causing get_validated to trust stale | |
|  | MVCC data. Always use quorum path.   | |
|  +-------------------------------------+ |
|                                           |
|  RO Quorum Path (S6.1)                  |
|  +-------------------------------------+ |
|  | QuorumRead / QuorumScan              | |
|  | IR inconsistent op, f+1 replicas     | |
|  | updates read_ts to prevent overwrite | |
|  +-------------------------------------+ |
|                                           |
|  Coordinator Recovery (S5.2.3)           |
|  +-------------------------------------+ |
|  | Backup shard polls participants      | |
|  | Modified decide: f+1 NO-VOTE->abort | |
|  +-------------------------------------+ |
|                                           |
|  Retry Timestamps (S6.4)                |
|  +-------------------------------------+ |
|  | Exponential increase on retry        | |
|  +-------------------------------------+ |
|                                           |
+-------------------------------------------+
```

**RO fast path (disabled):** The TAPIR paper ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S6.1) describes a single round-trip fast path for read-only transactions: if a replica has a *validated* version (write timestamp < snapshot timestamp < read timestamp), it responds immediately. tapirs intentionally does not use this fast path because its correctness assumption — "the returned object is valid, because it is up-to-date" — breaks under partitions. When `FinalizeInconsistent(Commit)` hasn't propagated to all replicas, a prior `quorum_read`'s `commit_get` can set `read_ts` on a stale replica, causing a subsequent `get_validated` to trust stale MVCC data. tapirs always uses `QuorumRead`/`QuorumScan` (f+1 replicas, merge by highest `write_ts`) for linearizable reads. See [IR/TAPIR Gotchas §19](ir-tapir-gotchas.md#19-ro-fast-path-returns-stale-data-under-partitions) for the concrete failure scenario.

**Recovery and retry:** Coordinator recovery ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S5.2.3): if a client crashes mid-commit, the backup shard's replicas recover the transaction using Bernstein's cooperative termination protocol adapted for IR. The modified `decide` function (Paper Fig. 13) requires f+1 NO-VOTE to safely abort -- because the original coordinator may have already received PREPARE-OK and committed. Retry timestamp selection (S6.4): clients exponentially increase proposed timestamps on retry to reduce conflict likelihood.

**Related docs:** See [Protocol](protocol-tapir.md) for the base RW protocol, [Custom Extensions](protocol-tapir-custom-extensions.md) for tapirs-specific additions, and [Consistency](../concepts/consistency.md) for how RO linearizability differs from RW strict serializability. Key files: `src/tapir/client.rs` (RO fast path), `src/tapir/replica.rs` (coordinator recovery).

| Extension | Paper S | How it works | Key file |
|-----------|---------|-------------|----------|
| RO fast path (get) | S6.1 | `get_validated` — **disabled**: stale under partitions (see gotcha #19) | `occ/store.rs` |
| RO fast path (scan) | S6.1 | `scan_validated` — **disabled**: same issue as get | `occ/store.rs` |
| RO quorum path | S6.1 | QuorumRead/QuorumScan via IR inconsistent op, always used for RO reads | `tapir/shard_client.rs` |
| Coordinator recovery | S5.2.3 | Backup shard polls with modified decide; f+1 NO-VOTE to abort safely | `tapir/replica.rs` |
| Retry timestamp selection | S6.4 | Exponentially increase proposed timestamp on retry | `tapir/client.rs` |
