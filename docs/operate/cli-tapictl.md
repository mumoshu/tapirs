# tapictl

```
                      tapictl
                         |
          +---------+----+----+---------+
          |         |         |         |
       +-------+ +-------+ +---------+ +------+
       | split | | merge | | compact | | solo |
       +-------+ +-------+ +---------+ +------+
         shard     shard      shard      direct
         at key    adjacent   onto       node
         boundary  shards     fresh      access
```

**P1 -- Shard lifecycle management:** `tapictl` is the control plane CLI for managing shard lifecycle operations. All commands communicate with the shard manager to coordinate multi-phase resharding: entering read-only mode, draining in-flight transactions, pulling committed data via CDC, transferring read-protection watermarks, and activating the target shard. Every command is idempotent -- if interrupted, re-running the same command resumes from where it left off. Reads continue throughout; writes pause briefly during the drain and activation phases.

**P2 -- Four subcommands:** Four subcommands are available: [split](cli-tapictl-split.md) divides a hot shard at a key boundary, [merge](cli-tapictl-merge.md) combines two adjacent underutilized shards, [compact](cli-tapictl-compact.md) compacts the IR record and LSM storage onto fresh replicas, and [solo](cli-tapictl-solo.md) provides direct node access operations (like cross-cluster CDC cloning) that bypass the shard manager.

**P3 -- Related docs:** For a detailed explanation of what happens at each resharding phase, see [Resharding internals](../learn/internals/resharding.md). For how the shard directory is updated during splits and merges, see [Discovery internals](../learn/internals/discovery.md). Back to [CLI Reference](cli-reference.md). Key file: `src/bin/tapictl/main.rs`

| Subcommand | Purpose | Details |
|------------|---------|---------|
| [split](cli-tapictl-split.md) | Split a shard at a key boundary | Two new shards, each covering half the key range |
| [merge](cli-tapictl-merge.md) | Merge two adjacent shards | Absorbed shard's data transferred to surviving shard |
| [compact](cli-tapictl-compact.md) | Compact a shard onto fresh replicas | Compacts IR record + LSM storage, reclaims resources |
| [solo](cli-tapictl-solo.md) | Direct node access (no shard manager) | Cross-cluster CDC clone and other bypass operations |
