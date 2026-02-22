# When to Use tapirs

```
  +---------------------------+-------+-----------------------------------------+
  |        Use Case           |  Fit  |              Why / Alternative           |
  +---------------------------+-------+-----------------------------------------+
  | SQL database backend      | Good  | Replication + sharding + OCC built in   |
  | NoSQL document/graph      | Good  | Key-range sharding maps to collections  |
  | Stateful microservices    | Good  | Distributed txns without app-level 2PC  |
  | TAPIR-backed discovery    | Good  | Dog-food: linearizable writes + EC reads|
  +---------------------------+-------+-----------------------------------------+
  | Single-node workloads     | Poor  | Use SQLite / RocksDB instead            |
  | Analytics / OLAP          | Poor  | Use ClickHouse / DuckDB instead         |
  | Eventual consistency      | Poor  | Use DynamoDB / Cassandra instead        |
  +---------------------------+-------+-----------------------------------------+
```

**Good fit:** tapirs is a good fit when you need linearizable distributed transactions with horizontal scalability and no single point of failure. Concrete use cases include: using tapirs as the distributed storage engine underneath a SQL database (where tapirs handles replication, sharding, and conflict detection while your SQL layer handles parsing, planning, and execution), as a strongly-consistent backend for a NoSQL document or graph store (key-range sharding maps naturally to document collections or graph partitions), or as a transactional state store for stateful microservices that need to coordinate distributed state with strict serializability.

**Not a good fit:** tapirs is *not* a good fit for single-node workloads (where an embedded database like SQLite or RocksDB is simpler and faster), analytics or OLAP queries (where columnar stores like ClickHouse or DuckDB are purpose-built), use cases where eventual consistency suffices (where DynamoDB or Cassandra have larger ecosystems and lower operational overhead), or latency-sensitive workloads that can tolerate weaker isolation (where a single-leader system may be simpler). If you need strong transactions but your data fits on a single node, tapirs' [single-node mode](../operate/getting-started-testbed-solo.md) still works — but you're paying for distributed transaction overhead that a simpler KV store wouldn't require.

**Dog-food pattern:** tapirs itself demonstrates one advanced integration pattern: the TAPIR-backed discovery service uses linearizable writes for shard registration and eventually consistent reads for directory sync — proving that mixed-consistency patterns work at the infrastructure level. For the programming interface, see [Rust Client SDK](../integrate/rust-client-sdk.md). For consistency guarantees, see [Consistency](../learn/concepts/consistency.md). Back to [Evaluate](README.md).

| Use case | Fit | Why |
|----------|-----|-----|
| SQL database storage engine | Good | Handles replication + sharding + conflict detection; SQL layer focuses on query processing |
| NoSQL document/graph backend | Good | Key-range sharding maps to collections/partitions; strict serializability for cross-document transactions |
| Stateful microservices | Good | Distributed transactional state without application-level 2PC |
| tapirs-backed discovery | Good | Mixed consistency: linearizable writes + eventual reads (dog-food pattern) |
| Single-node workloads | Poor | Distributed overhead unnecessary; use SQLite/RocksDB |
| Analytics / OLAP | Poor | Not optimized for columnar scans; use ClickHouse/DuckDB |
| Eventual-consistency workloads | Poor | Over-provisioned guarantees; use DynamoDB/Cassandra |
