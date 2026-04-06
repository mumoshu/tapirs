# tapictl

```
                            tapictl
                               |
     +------+------+-------+--+--+-----+--------+---------+
     |      |      |       |     |     |        |         |
  +------+ +-----+ +-----+ +--+ +---+ +------+ +-------+ +------+
  |split | |merge| |comp.| |cr| |get| |backup| |restore| | solo |
  +------+ +-----+ +-----+ +--+ +---+ +------+ +-------+ +------+
```

**Cluster management:** `tapictl` is the control plane CLI for managing shard lifecycle operations and cluster-wide tasks. Resharding commands (split, merge, compact) communicate with the shard manager to coordinate multi-phase changes: entering read-only mode, draining in-flight transactions, pulling committed data via CDC, transferring read-protection watermarks, and activating the target shard. Every command is idempotent -- if interrupted, re-running the same command resumes from where it left off. Reads continue throughout; writes pause briefly during the drain and activation phases.

**Eight subcommands:** [split](cli-tapictl-split.md) divides a hot shard at a key boundary, [merge](cli-tapictl-merge.md) combines two adjacent underutilized shards, [compact](cli-tapictl-compact.md) compacts the IR record and LSM storage onto fresh replicas, create initializes shards and registers them with the shard manager, get lists backups/shards/cluster status, backup creates full or incremental backups to local directories or S3, restore ships CDC deltas from a backup to target replicas, and [solo](cli-tapictl-solo.md) provides direct node access operations (like cross-cluster CDC cloning, backup, and restore) that bypass the shard manager.

**Related docs:** For a detailed explanation of what happens at each resharding phase, see [Resharding internals](../learn/internals/resharding.md). For how the shard directory is updated during splits and merges, see [Discovery internals](../learn/internals/discovery.md). For S3 backup/restore details, see [S3 Backup & Restore](backup-restore-s3.md). Back to [CLI Reference](cli-reference.md). Key file: `src/bin/tapictl/main.rs`

| Subcommand | Purpose | Details |
|------------|---------|---------|
| [split](cli-tapictl-split.md) | Split a shard at a key boundary | Two new shards, each covering half the key range |
| [merge](cli-tapictl-merge.md) | Merge two adjacent shards | Absorbed shard's data transferred to surviving shard |
| [compact](cli-tapictl-compact.md) | Compact a shard onto fresh replicas | Compacts IR record + LSM storage, reclaims resources |
| create | Create a cluster resource | Initialize shards and register with shard manager |
| get | Get cluster information | List backups, shards, and cluster status |
| backup | Back up cluster data | Full or incremental backup to local dir or S3 |
| restore | Restore from backup | Ships CDC deltas to target replicas |
| [solo](cli-tapictl-solo.md) | Direct node access (no shard manager) | Cross-cluster CDC clone, backup, restore |
