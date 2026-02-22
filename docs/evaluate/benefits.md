# Why tapirs

```
  +-------------------+  +-------------------+  +-------------------+  +-------------------+
  |  Always Available |  | Simple to Deploy  |  |  Grows with You   |  | Correct by Design |
  |                   |  |                   |  |                   |  |                   |
  |  No leader in     |  |  One binary       |  |  Online           |  |  Deterministic    |
  |  hot path         |  |  One cluster      |  |  resharding       |  |  simulation       |
  |  No failover      |  |  No external      |  |  Split / Merge /  |  |  Fault injection  |
  |  delay            |  |  dependencies     |  |  Compact          |  |  Jepsen-compat    |
  +-------------------+  +-------------------+  +-------------------+  +-------------------+
```

**Always available:** tapirs is always available because there is no leader in the transaction hot path. When a replica goes down, the remaining replicas continue serving transactions immediately — there is no failover delay, no leader election, and no window of unavailability. View changes (the IR equivalent of leader re-election) use a temporary coordinator that merges replica records and then returns to fully leaderless operation. Even during online resharding, where a cluster-singleton ShardManager briefly coordinates the affected shards, reads continue uninterrupted and write stalls last at most one replica tick interval while backup coordinators resolve in-flight transactions.

**Simple to deploy:** Deploying tapirs is simple because the entire system is self-contained: one binary, one cluster, no external dependencies. There is no ZooKeeper to provision, no etcd to maintain, and no separate discovery service to configure and monitor. Nodes discover each other through an embedded directory service with three backend options — static JSON for development, DNS for Kubernetes (resolving headless service hostnames), or TAPIR-backed for production clusters where tapirs uses itself as its own discovery service. A Docker testbed lets you bootstrap a multi-node cluster in under a minute.

**Grows with you, correct by design:** tapirs grows with your workload through online resharding: split hot shards to distribute load, merge cold shards to reclaim resources, and compact shards to reclaim the ever-growing IR record and consolidate storage — all without stopping reads. The system is correct by design: every test is deterministic and 100% reproducible (seeded RNG, paused clock), fault injection covers both network failures (message loss, delay, reordering) and disk failures (read/write errors, corruption), fuzz testing injects concurrent view changes during random transaction workloads, and linearizability is verified by Jepsen-compatible tooling.

**Benefits at a glance:**

- **Always available** — no leader, no failover delay | [Protocol](../learn/internals/protocol-tapir.md)
- **Simple to deploy** — no external dependencies | [Discovery](../learn/internals/discovery.md)
- **Grows with you** — online resharding (split/merge/compact) | [Resharding](../learn/internals/resharding.md)
- **Correct by design** — deterministic simulation testing | [Testing](../learn/internals/testing.md)
- **Linearizable transactions** — strict serializability for RW, linearizability for RO | [Paper Extensions](../learn/internals/protocol-tapir-paper-extensions.md)
- **Durable storage** — crash-safe WiscKey SSD engine | [Storage](../learn/internals/storage.md)
