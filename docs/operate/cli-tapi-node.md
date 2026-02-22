# tapi node

```
  +------------------------------------------+
  |             tapi node                    |
  |                                          |
  |  +----------+  +----------+  +--------+  |
  |  | Shard 0  |  | Shard 1  |  |Shard N |  |
  |  | Replica  |  | Replica  |  |Replica |  |
  |  |          |  |          |  |        |  |
  |  | IR       |  | IR       |  | IR     |  |
  |  | OCC      |  | OCC      |  | OCC    |  |
  |  | MVCC     |  | MVCC     |  | MVCC   |  |
  |  +----------+  +----------+  +--------+  |
  |                                          |
  |  Listen: --address host:port             |
  +------------------------------------------+
```

**Replica hosting:** `tapi node` starts a long-running process that hosts one or more shard replicas. Each replica participates in IR consensus for its shard -- receiving proposals from clients, running OCC validation at prepare time, and executing committed transactions against the local MVCC store. A single node process can host replicas for many shards, which is how tapirs achieves multi-core utilization: each replica is a single-threaded state machine, and sharding distributes work across replicas.

**Key flags:** Key flags: `--address` sets the listen address (host:port), `--discovery-url` specifies where to register and discover other nodes, `--data-dir` sets the persistent storage directory (for WiscKey SSD mode), and `--solo` runs in single-node mode with all shards in one process. The node automatically registers its replicas with the discovery service on startup and deregisters on shutdown.

**Related docs:** For how replicas participate in consensus, see [Protocol](../learn/internals/protocol-tapir.md). For single-node mode, see [Getting Started (solo)](getting-started-testbed-solo.md). Back to [tapi](cli-tapi.md).

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `127.0.0.1:9000` | Listen address (host:port) |
| `--discovery-url` | (required) | Discovery service URL (`file://`, `dns://`, `tapirs://`) |
| `--data-dir` | (in-memory) | Persistent storage directory for WiscKey SSD mode |
| `--solo` | `false` | Run in single-node mode with all shards in one process |
| `--config` | (none) | Path to TOML configuration file |
