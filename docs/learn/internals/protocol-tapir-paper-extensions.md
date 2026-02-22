# Paper Extensions

```
Paper Extensions (S5.2.3, S6)
+-------------------------------------------+
|                                           |
|  RO Fast Path (S6.1)                     |
|  +-------------------------------------+ |
|  | get_validated / scan_validated       | |
|  | write_ts < snapshot_ts < read_ts     | |
|  | --> respond immediately (1 replica)  | |
|  +-------------------------------------+ |
|                                           |
|  RO Quorum Fallback (S6.1)              |
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

**RO fast path:** Read-only transactions ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S6.1) have a single round-trip fast path: if a replica has a *validated* version (write timestamp < snapshot timestamp < read timestamp), it responds immediately -- the read is both current and won't be overwritten. For both get and scan, if validation fails at one replica, the quorum fallback (`QuorumRead`/`QuorumScan` via IR inconsistent operation) updates the read timestamp at f+1 replicas to prevent later overwrites.

**Recovery and retry:** Coordinator recovery ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S5.2.3): if a client crashes mid-commit, the backup shard's replicas recover the transaction using Bernstein's cooperative termination protocol adapted for IR. The modified `decide` function (Paper Fig. 13) requires f+1 NO-VOTE to safely abort -- because the original coordinator may have already received PREPARE-OK and committed. Retry timestamp selection (S6.4): clients exponentially increase proposed timestamps on retry to reduce conflict likelihood.

**Related docs:** See [Protocol](protocol-tapir.md) for the base RW protocol, [Custom Extensions](protocol-tapir-custom-extensions.md) for tapirs-specific additions, and [Consistency](../concepts/consistency.md) for how RO linearizability differs from RW strict serializability. Key files: `src/tapir/client.rs` (RO fast path), `src/tapir/replica.rs` (coordinator recovery).

| Extension | Paper S | How it works | Key file |
|-----------|---------|-------------|----------|
| RO fast path (get) | S6.1 | `get_validated`: write_ts < snapshot_ts < read_ts -- respond immediately | `occ/store.rs` |
| RO fast path (scan) | S6.1 | `scan_validated`: same validation for range reads | `occ/store.rs` |
| RO quorum fallback | S6.1 | QuorumRead/QuorumScan via IR inconsistent op, updates read_ts at f+1 replicas | `tapir/shard_client.rs` |
| Coordinator recovery | S5.2.3 | Backup shard polls with modified decide; f+1 NO-VOTE to abort safely | `tapir/replica.rs` |
| Retry timestamp selection | S6.4 | Exponentially increase proposed timestamp on retry | `tapir/client.rs` |
