# Discovery

```
  Node A                                          Node B
  +-------------------+                           +-------------------+
  | CachingShardDir   |                           | CachingShardDir   |
  | +---------------+ |                           | +---------------+ |
  | | Local Cache   | |                           | | Local Cache   | |
  | +-------+-------+ |                           | +-------+-------+ |
  |         |         |                           |         |         |
  |    PULL | PUSH    |                           |    PULL | PUSH    |
  +---------+---------+                           +---------+---------+
            |                                               |
            v                                               v
       +--------------------------------------------------------+
       |            RemoteShardDirectory                        |
       |  (Static JSON / DNS / TAPIR-backed)                    |
       +--------------------------------------------------------+
```

**No external coordinator:** Most distributed databases depend on an external coordination service — ZooKeeper, etcd, or Consul — for nodes to discover each other and agree on cluster membership. tapirs eliminates this dependency entirely. Each node runs a `CachingShardDirectory` that maintains a local copy of the shard-to-replica mapping and periodically syncs with a `RemoteShardDirectory` backend. Three backends are supported: static JSON for testing, DNS for Kubernetes headless services, and TAPIR-backed for production — where tapirs uses *itself* as its own discovery service.

**Sync and tombstones:** The sync loop is deliberately simple: periodically pull all shard entries from the remote backend, and push the node's own shards. Tombstones prevent a subtle race condition: without them, a decommissioned shard could be resurrected if a stale node pushes an outdated entry that the remote backend hasn't yet forgotten. Once a shard is tombstoned, future puts are rejected with `DiscoveryError::Tombstoned`, ensuring that decommissioned shards stay dead. View number monotonicity provides an additional safeguard — stale updates with older view numbers are silently discarded.

**Eventual consistency trade-off:** The discovery layer uses eventual consistency — not the strict serializability or linearizability of tapirs' transaction protocol. This is a deliberate trade-off: discovery metadata changes infrequently and can tolerate brief staleness, while the simplicity of periodic sync avoids the complexity of distributed consensus for metadata. For the full discovery architecture, see the [Discovery internals](../internals/discovery.md) deep-dive. For how this eventual consistency model relates to transaction consistency, see [Consistency](consistency.md). Next: [Client](client.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| CachingShardDirectory | Local cache + background sync with remote. PULL all shards, PUSH own_shards only | `discovery/mod.rs` |
| Tombstone (Discovery) | Decommissioned shard marker — future puts rejected with `DiscoveryError::Tombstoned`, prevents resurrection | `discovery/mod.rs` |
| RemoteShardDirectory | Async interface to external directory backend (HTTP, TAPIR-backed, etc.) | `discovery/mod.rs` |
| InMemoryShardDirectory | Thread-safe HashMap-backed local directory. Rejects stale updates via view number monotonicity | `discovery/mod.rs` |
