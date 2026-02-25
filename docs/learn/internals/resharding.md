# Resharding Internals

```
Source Shard                                        Target Shard
+-----------+                                      +-----------+
|           |  1. ReadOnly (block new Prepare)     |           |
|  Replicas | ---------------------------------->  |  (empty)  |
|           |                                      |           |
|           |  2. Prepared-Drain                   |           |
|           |  (wait for in-flight txns)           |           |
|           |                                      |           |
|           |  3. CDC Pull                         |           |
|           |  (view-based deltas) =============>  |  Replicas |
|           |                                      |           |
|           |  4. Read-Protection Transfer         |           |
|           |  (range_reads + watermarks) ======>  |           |
|           |                                      |           |
|           |  5. Activation                       |           |
|           |  (register target, tombstone source) |           |
+-----------+                                      +-----------+
     |                                                  |
     v                                                  v
 Decommissioned                                    Active (serving)
```

**Resharding lifecycle:** Online resharding (split, merge, compact) moves data between shards without downtime for reads. Compact also reclaims the IR record, which accumulates finalized operation entries forever. The ShardManager coordinates the lifecycle: (1) source enters ReadOnly phase, blocking new CO::Prepare, (2) prepared-drain waits for in-flight (potentially cross-shard) transactions to commit or abort, (3) CDC pulls committed data via view-based deltas, (4) read-protection watermarks are transferred, (5) target shard is activated and source decommissioned.

**CDC cursor design:** CDC uses view-based cursors (monotonic) rather than timestamps (non-monotonic in TAPIR -- clients pick from system clocks). Each IR view change fires the `on_install_leader_record_delta` upcall, producing a delta of changes accumulated during that view. The cursor tracks `last_view: Option<u64>` and queries `from_view = last_view + 1`. `scan_changes()` merges responses from f+1 replicas -- by quorum intersection, at least one has fine-grained deltas for each view transition.

**Related docs:** See [Resharding concepts](../concepts/resharding.md) for term definitions, [Protocol](protocol-tapir.md) for how Prepare interacts with ReadOnly phase, and [Discovery](discovery.md) for how shard registration updates during resharding. Key files: `src/sharding/shardmanager/cdc.rs`, `src/sharding/shardmanager/catchup.rs`, `src/sharding/shardmanager/mod.rs`.

| Phase | What happens | Key constraint |
|-------|-------------|----------------|
| ReadOnly | Source blocks new CO::Prepare | Existing reads continue |
| Prepared-Drain | Wait for in-flight txns to resolve | Cross-shard txns may take multiple round-trips |
| CDC Pull | View-based delta replication | Cursor uses view numbers, not timestamps |
| Read-Protection Transfer | Copy range_reads + max_read_commit_time to target | Target raises min_prepare_time to reject stale writes |
| Activation | Register target shard, tombstone source in discovery | rebuild_directory() validates no overlapping ranges |
