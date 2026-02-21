# tapictl merge shard

```
                  Merge Operation

  Before:                        After:
  +-----------+ +-----------+    +------------------------+
  |  Shard A  | |  Shard B  |    |       Shard A          |
  | [a, m)    | | [m, z)    |    |  [a ---------> z)      |
  | surviving | | absorbed  | -->|  surviving shard       |
  |           | |           |    |  expanded range        |
  +-----------+ +-----------+    +------------------------+
```

**P1 -- Combine adjacent shards:** `tapictl merge shard` combines two adjacent shards into one. The "absorbed" shard's data is transferred to the "surviving" shard, which expands its key range to cover both. This is useful when shards are underutilized -- merging them reduces the number of replica groups and simplifies cluster management.

**P2 -- Adjacency requirement:** The merge requires that the two shards have adjacent, non-overlapping key ranges (e.g., `[a, m)` and `[m, z)`). The absorbed shard enters read-only mode and drains, its data is transferred via CDC, and then it is tombstoned while the surviving shard's range expands. Key flags: `--surviving-shard-id` and `--absorbed-shard-id` identify the two shards.

**P3 -- Related docs:** For the full resharding lifecycle, see [Resharding internals](../learn/internals/resharding.md). Back to [tapictl](cli-tapictl.md).

| Flag | Required | Description |
|------|----------|-------------|
| `--surviving-shard-id` | Yes | ID of the shard that will expand its key range |
| `--absorbed-shard-id` | Yes | ID of the shard whose data will be transferred and tombstoned |
| `--shard-manager` | No | Shard manager address (default: from discovery) |
