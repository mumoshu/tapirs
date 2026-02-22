# tapictl solo

```
                 Solo Mode (Direct Access)

  +----------+                    +----------+
  |          |   direct connect   |          |
  | tapictl  |<------------------>|  Node    |
  |  solo    |    (no shard mgr)  |          |
  |          |                    |          |
  +----------+                    +----------+

  +----------+        CDC         +----------+
  | Source   |===================>| Target   |
  | Cluster  |   cross-cluster    | Cluster  |
  |  Node    |      clone         |  Node    |
  +----------+                    +----------+
```

**Direct node access:** `tapictl solo` provides operations that connect directly to nodes without going through the shard manager. The primary use case is `tapictl solo clone`, which replicates a shard from one cluster to another using CDC -- useful for disaster recovery, cluster migration, or seeding a new cluster from an existing one.

**Lower-level operations:** Solo operations are lower-level than the standard split/merge/compact commands. They don't coordinate through the shard manager, so the operator is responsible for ensuring that the source shard is in a consistent state. This makes them more flexible but requires more care. Key flags: `--source-address` and `--target-address` specify the node endpoints directly.

**Related docs:** For how CDC replication works, see [Resharding internals](../learn/internals/resharding.md). Back to [tapictl](cli-tapictl.md).

| Subcommand | Args | Description |
|------------|------|-------------|
| `clone` | `--source-address`, `--target-address`, `--shard-id` | Replicate a shard across clusters via CDC |
