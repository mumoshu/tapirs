# Online Resharding

```
  Source Shard                              Target Shard
  +------------------+                     +------------------+
  |  Committed Data  |------- CDC -------->|  Replicated Data |
  |                  |  (view-based        |                  |
  |  Prepared Txns   |   cursors)          |                  |
  +--------+---------+                     +------------------+
           |                                        ^
           |  Prepared-Drain                        |
           |  (block new prepares,                  |
           |   wait for in-flight                   |
           |   to resolve)                          |
           |                                        |
           +--- Transfer --------------------------+
                (read-protection watermarks)
```

**What resharding does:** Online resharding lets you split a hot shard into two, merge underutilized shards back together, or compact a shard to reclaim the ever-growing IR record and consolidate storage — all without stopping reads. The central challenge is handling in-flight transactions: when a shard is being split, there may be transactions that have already been prepared (possibly spanning multiple shards via cross-shard 2PC) that must either commit or abort before data can be safely transferred to the target shard. The prepared-drain phase handles this by blocking new prepares on the source shard while waiting for existing ones to resolve.

**View-based CDC:** Change Data Capture (CDC) replicates committed data from source to target shards incrementally, using view-based cursors rather than timestamps. This is a deliberate design choice: in TAPIR, timestamps are non-monotonic because clients pick them from loosely synchronized system clocks, but IR view numbers are strictly monotonic. Each IR view change produces a delta record of the changes accumulated during that view, and the CDC cursor tracks which deltas have been consumed. This gives the resharding system a reliable, gap-free stream of changes without depending on clock synchronization.

**Deep-dives:** For the full resharding lifecycle (ReadOnly → Drain → CDC Pull → Read-Protection Transfer → Activation), see the [Resharding internals](../internals/resharding.md) deep-dive. For how the data being transferred is structured, see [Storage](storage.md). For the view changes that drive CDC, see [IR](ir.md). Next: [Discovery](discovery.md) → [Client](client.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| CDC Cursor | View-based cursor tracking consumed deltas during resharding. Uses view numbers (monotonic) not timestamps (non-monotonic) | `sharding/shardmanager/cdc.rs` — `CdcCursor` |
| Prepared-Drain | Source shard goes read-only, blocks new prepares, waits for in-flight (potentially cross-shard) transactions to resolve before transferring data | `sharding/shardmanager/cdc.rs` |
| LeaderRecordDelta | Per-view change record of committed transactions — used by CDC to replicate changes incrementally | `tapir/message.rs` |
| effective_end_view | `Option<u64>` — highest base_view with a delta. None = no deltas, Some(0) = delta at view 0 | `sharding/shardmanager/cdc.rs` |
| Read-Protection Transfer | Before decommissioning source, range_reads and max_read_commit_time watermarks transferred to target shard | `sharding/shardmanager/cdc.rs` |
