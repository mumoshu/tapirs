# Discovery Internals

```
+--------+      +--------+      +--------+
| Node A |      | Node B |      | Node C |
+---+----+      +---+----+      +---+----+
    |               |               |
    v               v               v
+-------+       +-------+       +-------+
|Caching|       |Caching|       |Caching|
| Shard |       | Shard |       | Shard |
|  Dir  |       |  Dir  |       |  Dir  |
+---+---+       +---+---+       +---+---+
    |               |               |
    |   pull/push   |   pull/push   |
    +-------+-------+-------+-------+
            |               |
            v               v
    +-------+---------------+-------+
    |       RemoteShardDirectory    |
    |  (static JSON / DNS / TAPIR)  |
    +-------------------------------+
```

**P1 -- Self-contained discovery:** tapirs is self-contained -- no ZooKeeper, etcd, or external coordinator required. Three discovery backends: static JSON (testing/dev), DNS (Kubernetes headless services), and TAPIR-backed (tapirs uses itself as its own discovery service with linearizable writes + eventually consistent reads). All backends implement `RemoteShardDirectory`.

**P2 -- Caching and tombstones:** `CachingShardDirectory` wraps a local `InMemoryShardDirectory` and spawns a background task that periodically pulls all remote entries and pushes its own shards (`own_shards` filter prevents echo traffic but never blocks pull). Tombstones prevent decommissioned shards from being resurrected by stale pushes -- `DiscoveryError::Tombstoned` rejects future puts. View number monotonicity ensures stale updates are rejected.

**P3 -- Related docs:** See [Discovery concepts](../concepts/discovery.md) for term definitions, [Resharding](resharding.md) for how shard registration changes during splits, and [Architecture Decisions](architecture-decisions.md) for why TAPIR-backed discovery. Key files: `src/discovery/mod.rs`, `src/discovery/tapir.rs`.

| Backend | Config | Use case | Key file |
|---------|--------|----------|----------|
| Static JSON | File path | Testing, small fixed clusters | `discovery/mod.rs` |
| DNS | Headless service hostname | Kubernetes | `discovery/mod.rs` |
| TAPIR-backed | tapirs cluster endpoint | Self-contained production clusters | `discovery/tapir.rs` |
