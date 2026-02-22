# tapi shard-manager

```
                 Resharding Flow

  tapictl ----request----> Shard Manager
                               |
                     +---------+---------+
                     |                   |
                     v                   v
              +------------+      +------------+
              |   Source   |      |   Target   |
              |   Shard    |      |   Shard    |
              |            |      |            |
              | read-only  |      |  activate  |
              | drain      | CDC  |            |
              | ---------->|----->|            |
              +------------+      +------------+
```

**Resharding coordinator:** `tapi shard-manager` runs the shard manager -- the cluster-singleton coordinator that orchestrates online resharding operations (split, merge, compact). When `tapictl` issues a resharding command, it's the shard manager that drives the multi-phase lifecycle: entering read-only mode on the source shard, draining in-flight transactions, pulling committed data via CDC, transferring read-protection watermarks, and activating the target shard.

**Cluster singleton:** The shard manager is a cluster singleton -- only one instance should run at a time. It maintains state about in-progress resharding operations and resumes them on restart. Key flags: `--address` sets the listen address (where `tapictl` connects), and `--discovery-url` specifies the discovery service for looking up shard locations.

**Related docs:** For the resharding lifecycle in detail, see [Resharding internals](../learn/internals/resharding.md). For the `tapictl` commands that trigger resharding, see [tapictl](cli-tapictl.md). Back to [tapi](cli-tapi.md).

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `127.0.0.1:9200` | Listen address (where `tapictl` connects) |
| `--discovery-url` | (required) | Discovery service URL for shard lookups |
| `--config` | (none) | Path to TOML configuration file |
