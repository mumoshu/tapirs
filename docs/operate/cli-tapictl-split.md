# tapictl split shard

```
                  Split Operation

  Before:                        After:
  +------------------------+     +-----------+ +-----------+
  |       Shard A          |     |  Shard A' | |  Shard A" |
  |  [start --------> end)|     | [start,   | | [split,   |
  |                        | --> |  split)   | |  end)     |
  |  key range: [a, z)     |     | [a, m)    | | [m, z)    |
  +------------------------+     +-----------+ +-----------+
```

**P1 -- Split at key boundary:** `tapictl split shard` divides a shard into two at a specified key boundary. The source shard's key range `[start, end)` becomes two non-overlapping ranges: `[start, split_key)` and `[split_key, end)`. This is the primary tool for handling hotspots -- when a shard receives disproportionate traffic, splitting it distributes the load across two sets of replicas.

**P2 -- Resharding lifecycle:** The split proceeds through the standard resharding lifecycle: the source enters read-only mode, in-flight transactions drain, committed data is replicated to the two target shards via CDC, and finally both targets are activated and the source is tombstoned in the discovery directory. The CLI reports progress for each phase. Key flags: `--shard-id` identifies the source shard, `--split-key` specifies the boundary.

**P3 -- Related docs:** For the full resharding lifecycle, see [Resharding internals](../learn/internals/resharding.md). For how the discovery directory is updated, see [Discovery internals](../learn/internals/discovery.md). Back to [tapictl](cli-tapictl.md).

| Flag | Required | Description |
|------|----------|-------------|
| `--shard-id` | Yes | ID of the source shard to split |
| `--split-key` | Yes | Key boundary for the split point |
| `--shard-manager` | No | Shard manager address (default: from discovery) |
