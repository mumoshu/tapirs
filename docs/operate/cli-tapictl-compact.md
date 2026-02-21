# tapictl compact shard

```
                Compact Operation

  Before:                         After:
  +-------------------------+     +-------------------------+
  |    Old Replicas         |     |    Fresh Replicas       |
  |                         |     |                         |
  |  IR Record: 50k entries |     |  IR Record: 0 entries  |
  |  LSM: 12 L0 SSTables   | --> |  LSM: 1 sorted run     |
  |  VLog: fragmented       |     |  VLog: compact         |
  |  Memory: 2.1 GB         |     |  Memory: 400 MB        |
  +-------------------------+     +-------------------------+
```

**P1 -- Reclaim resources:** `tapictl compact shard` migrates a shard onto fresh replicas, compacting both the IR record and the LSM storage. The IR record -- the unordered set of all operations processed by a shard's replicas -- accumulates entries forever (finalized operations are never garbage-collected in place). Over time this grows unboundedly and increases memory usage and view-change recovery time. Compaction solves this by creating fresh replicas that start with only the current committed state -- no historical IR record entries, no accumulated L0 SSTable files, no fragmented value log. This is also useful for reclaiming disk space after many deletes or migrating a shard to different hardware.

**P2 -- Same lifecycle, clean state:** The compaction follows the same resharding lifecycle as split and merge -- read-only mode, drain, CDC transfer, activation -- but the key range doesn't change. The target replicas receive only the committed state (MVCC data + OCC watermarks), not the full IR record history. The result is a shard with the same key range on fresh replicas with a clean IR record and fully compacted LSM tree. Key flag: `--shard-id` identifies the shard to compact.

**P3 -- Related docs:** For how IR records accumulate and why they're not garbage-collected in place, see [IR concepts](../learn/concepts/ir.md). For how LSM compaction works, see [Storage internals](../learn/internals/storage.md). For the resharding lifecycle, see [Resharding internals](../learn/internals/resharding.md). Back to [tapictl](cli-tapictl.md).

| Flag | Required | Description |
|------|----------|-------------|
| `--shard-id` | Yes | ID of the shard to compact |
| `--shard-manager` | No | Shard manager address (default: from discovery) |
