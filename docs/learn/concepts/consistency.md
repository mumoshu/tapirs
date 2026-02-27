# Consistency Model

```
  Eventual          Linearizable         Strict Serializable
  Consistency       (RO transactions)    (RW transactions)
     |                    |                      |
     |                    |                      |
  <--+--------------------+----------------------+-->
  weaker                                     stronger

  tapirs provides all three, depending on operation type.
```

**Three levels:** tapirs provides three consistency levels. RW transactions provide **strict serializability** — serializable (global transaction ordering) + linearizable (ordering respects real time) — via OCC conflict detection at the prepare phase. RO transactions provide **linearizability** — a consistent, up-to-date multi-key snapshot at a single point in real time, via quorum reads (f+1 replicas) with a mandatory uncertainty bound. `begin_read_only(ε)` requires ε >= max clock skew for linearizability (Paper S6.1); pass `Duration::ZERO` for perfectly synchronized clocks. An opt-in fast path (`set_ro_fast_path_delay()`) reduces to 1 replica when conditions allow. Both levels provide strong consistency across multiple keys; the difference is operational: RO transactions skip the prepare phase because they don't write.

**RO reads:** By default, each RO `get()`/`scan()` uses the quorum path: `QuorumRead`/`QuorumScan` via IR inconsistent operation, contacting f+1 replicas and updating the read timestamp to prevent future overwrites (2 round-trips). The mandatory uncertainty bound ε in `begin_read_only(ε)` adjusts `snapshot_ts` upward and adds a commit-wait delay, ensuring linearizability under clock skew. An opt-in fast path is available via `Client::set_ro_fast_path_delay(Some(delay))`: for each key, `get_validated` checks `write_ts < snapshot_ts < read_ts` at a single replica (1 round-trip), falling back to quorum on miss. For higher read throughput, reads can be lowered to eventual consistency — tapirs itself uses this mixed model for its own discovery service.

**Paper references:** [Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf): §5.3.1 (strict serializability proof), §6.1 (read-only transactions). Previous: [TAPIR](tapir.md). Next: [IR](ir.md) → [OCC](occ.md) → [Storage](storage.md) → [Resharding](resharding.md) → [Discovery](discovery.md) → [Client](client.md). Back to [Concepts](README.md).

| Level | Applies to | How it works | Round-trips | Paper |
|-------|-----------|-------------|-------------|-------|
| Strict Serializability | RW transactions | OCC prepare phase: validate read-set, write-set, scan-set against concurrent transactions. Client-proposed timestamps ensure real-time ordering | 2 (execute + prepare) | §5.3.1 |
| Linearizability | RO transactions | QuorumRead/QuorumScan (f+1 replicas) with mandatory `begin_read_only(ε)`. Opt-in fast path: `set_ro_fast_path_delay()` (1 replica) | 2 (quorum); opt-in: 1 | §6.1 |
| Eventual Consistency | Relaxed reads | Periodic sync via CachingShardDirectory. tapirs discovery dog-foods this pattern | Async | — |
