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

**Three levels:** tapirs provides three consistency levels. RW transactions provide **strict serializability** — serializable (global transaction ordering) + linearizable (ordering respects real time) — via OCC conflict detection at the prepare phase. RO transactions provide **linearizability** — a consistent, up-to-date multi-key snapshot at a single point in real time, with validated fast paths for both get and scan. Both levels provide strong consistency across multiple keys; the difference is operational: RO transactions skip the prepare phase because they don't write.

**RO validated reads:** RO validated reads: for each key, `get_validated` checks `write_ts < snapshot_ts < read_ts` — proving the version is current and won't be overwritten. `scan_validated` does the same for range scans. If validation fails at one replica, the quorum fallback (`QuorumRead`/`QuorumScan` via IR inconsistent operation) updates the read timestamp at f+1 replicas. Fast path: 1 replica, 1 round-trip. For higher read throughput, reads can be lowered to eventual consistency — tapirs itself uses this mixed model for its own discovery service.

**Paper references:** [Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf): §5.3.1 (strict serializability proof), §6.1 (read-only transactions). Previous: [TAPIR](tapir.md). Next: [IR](ir.md) → [OCC](occ.md) → [Storage](storage.md) → [Resharding](resharding.md) → [Discovery](discovery.md) → [Client](client.md). Back to [Concepts](README.md).

| Level | Applies to | How it works | Round-trips | Paper |
|-------|-----------|-------------|-------------|-------|
| Strict Serializability | RW transactions | OCC prepare phase: validate read-set, write-set, scan-set against concurrent transactions. Client-proposed timestamps ensure real-time ordering | 2 (execute + prepare) | §5.3.1 |
| Linearizability | RO transactions | Validated reads at consistent snapshot timestamp. get_validated + scan_validated fast path (1 replica); QuorumRead/QuorumScan fallback (f+1) | 1 (fast path) | §6.1 |
| Eventual Consistency | Relaxed reads | Periodic sync via CachingShardDirectory. tapirs discovery dog-foods this pattern | Async | — |
