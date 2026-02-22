# Building on tapirs

```
┌─────────────────────────────────────┐
│  Your SQL/NoSQL DB or Stateful App  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│        tapirs Client API            │
│  (RoutingClient / transactions)     │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│         tapirs Cluster              │
│   (shards, replicas, IR consensus)  │
└─────────────────────────────────────┘
```

**Storage engine for your database:** tapirs provides a transactional key-value interface with strict serializability for read-write transactions and linearizability for read-only transactions, backed by a horizontally-scalable cluster with no single point of failure. If you're building a SQL database, a document store, a graph database, or any stateful application that needs strongly-consistent distributed transactions, tapirs can serve as the underlying storage engine --- handling replication, conflict detection, and shard management so your application layer doesn't have to.

**Flexible consistency model:** The consistency model is flexible by design. Read-write transactions always provide strict serializability --- the strongest practical guarantee. Read-only transactions provide linearizability with a validated fast path (one replica, one round-trip) and quorum fallback. For use cases where read throughput matters more than freshness, you can lower reads to eventual consistency --- issuing linearizable writes and eventually consistent reads. tapirs itself uses this exact mixed-consistency pattern for its own TAPIR-backed discovery service, demonstrating that the pattern works in practice at the infrastructure level.

**Where to start:** Start with [Choosing Consistency](choosing-consistency.md) to understand which API level fits your use case --- strict serializability, linearizability, or eventual consistency. Then see the [Rust Client SDK](rust-client-sdk.md) for the programming interface. For the theory behind these guarantees, see [Consistency](../learn/concepts/consistency.md). For cluster configuration, see [Configuration](../operate/cli-config.md).

| Pattern | Description | Example |
|---------|-------------|---------|
| SQL database engine | Use tapirs as distributed storage backend | Build SQL layer on top of tapirs transactions |
| NoSQL database backend | Use tapirs for strongly-consistent document/graph storage | Key-range sharding maps naturally to document collections |
| Stateful application | Embed tapirs client for distributed state | Microservices sharing transactional state |
| Dog-food: tapirs discovery | tapirs uses itself with mixed consistency | Linearizable writes + eventual reads |
