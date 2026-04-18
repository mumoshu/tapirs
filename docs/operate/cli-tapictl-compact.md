# tapictl compact shard

```
                Compact Operation

  Before:                          After:
  +--------------------------+     +--------------------------+
  |    Old Replicas          |     |    Fresh Replicas        |
  |                          |     |                          |
  |  IR Record: 50k entries  |     |  IR Record: 0 entries    |
  |  SSTs: 1 per seal_view,  | --> |  SSTs: just the initial  |
  |         accumulated      |     |         seal (if any)    |
  |  Vlog: many segments,    |     |  Vlog: single active     |
  |        dead entries      |     |        segment, live     |
  |  Memory: 2.1 GB          |     |  Memory: 400 MB          |
  +--------------------------+     +--------------------------+
```

**Reclaim resources:** `tapictl compact shard` migrates a shard onto fresh replicas, reclaiming both the IR record and the storage engine's accumulated state. The IR record -- the unordered set of all operations processed by a shard's replicas -- accumulates entries forever (finalized operations are never garbage-collected in place). Over time this grows unboundedly and increases memory usage and view-change recovery time. The WiscKey-style engine (see [Storage internals](../learn/internals/storage.md)) also has no in-place value-log GC: each `seal_view()` appends a per-seal SST and may rotate the active vlog segment, but nothing ever reclaims dead entries within a segment. Compaction solves both by creating fresh replicas that start with only the current committed state -- no historical IR record entries, no per-seal SST accumulation, no fragmented vlog segments. This is also useful for reclaiming disk space after many deletes or migrating a shard to different hardware.

**Same lifecycle, clean state:** The compaction follows the same resharding lifecycle as split and merge -- read-only mode, drain, CDC transfer, activation -- but the key range doesn't change. The target replicas receive only the committed MVCC data (re-played as `IO::Commit`s) plus OCC watermarks, not the full IR record history. The result is a shard with the same key range on fresh replicas, a clean IR record, and a single fresh vlog segment. Key flag: `--shard-id` identifies the shard to compact.

**Related docs:** For how IR records accumulate and why they're not garbage-collected in place, see [IR concepts](../learn/concepts/ir.md). For why the storage engine has no in-place GC and how shard compaction reclaims vlog space, see [Storage internals](../learn/internals/storage.md). For the resharding lifecycle, see [Resharding internals](../learn/internals/resharding.md). Back to [tapictl](cli-tapictl.md).

| Flag | Required | Description |
|------|----------|-------------|
| `--shard-id` | Yes | ID of the shard to compact |
| `--shard-manager` | No | Shard manager address (default: from discovery) |
