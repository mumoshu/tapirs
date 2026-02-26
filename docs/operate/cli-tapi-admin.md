# tapi admin

```
                     tapi admin
                         |
       +---------+-------+--------+-----------+
       |         |       |        |           |
  +--------+ +-----+ +--------+ +------+ +---------+
  | status | | add | | remove | | view | | backup  |
  |        | | rep | | rep    | |change| | restore |
  +--------+ +-----+ +--------+ +------+ +---------+
    query     membership ops     consensus   data
```

**Cluster management:** `tapi admin` provides operational commands for managing a running tapirs cluster. These commands connect to a node's admin endpoint and issue control-plane operations. Status commands show node health and replica state. Membership commands (`add-replica`, `remove-replica`, `leave`) manage which replicas participate in a shard's consensus group. `view-change` triggers a manual view change for debugging or recovery scenarios.

**Backup and restore:** Backup and restore operate at the cluster level: `tapi admin backup` takes a consistent snapshot of all shards by coordinating with the shard manager, and `tapi admin restore` rebuilds a cluster from a backup. Both are designed to be safe to run against a live cluster -- backup doesn't block transactions, and restore is a full cluster replacement (not a selective merge). See [Backup & Restore Guide](backup-restore.md) for detailed usage including S3 support.

**Related docs:** For how membership changes interact with IR consensus, see [Protocol](../learn/internals/protocol-tapir.md). For how view changes work, see [IR concepts](../learn/concepts/ir.md). Back to [tapi](cli-tapi.md).

| Subcommand | Args | Description |
|------------|------|-------------|
| `status` | `--address <node>` | Show node health, replica state, and shard assignments |
| `add-replica` | `--shard-id <id> --address <node>` | Add a replica to a shard's consensus group |
| `remove-replica` | `--shard-id <id> --address <node>` | Remove a replica from a shard's consensus group |
| `leave` | `--address <node>` | Gracefully remove a node from all shard groups |
| `view-change` | `--shard-id <id>` | Trigger a manual view change for a shard |
| `backup` | `--output <path>` | Take a consistent snapshot of all shards |
| `restore` | `--input <path>` | Rebuild a cluster from a backup snapshot |
