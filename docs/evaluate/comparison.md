# tapirs vs Alternatives

```
  Raft-based (etcd, CockroachDB, TiKV)         tapirs (leaderless)

  +--------+                                    +--------+
  | Client |                                    | Client |
  +---+----+                                    +---+----+
      |                                             |
      v                                        +----+----+
  +--------+     +--------+     +--------+     |    |    |
  | Leader | --> |Follower| --> |Follower|     v    v    v
  +--------+     +--------+     +--------+   +--+ +--+ +--+
  All writes                                 |R1| |R2| |R3|
  go through                                 +--+ +--+ +--+
  one node                                   Any replica
                                             serves writes
```

**Where tapirs fits:** tapirs occupies a unique point in the distributed database landscape: a leaderless transactional key-value store that provides strict serializability without a centralized coordinator. It can serve as a standalone KV store or as the distributed storage engine underneath a SQL or NoSQL database. Unlike most alternatives, tapirs includes built-in online resharding and self-contained discovery — there is no external service to provision for coordination or metadata management.

**How alternatives compare:** Raft-based systems like etcd, CockroachDB, and TiKV funnel all writes through a leader per range, which bounds write throughput to what a single node can handle. They replicate for durability, not for horizontal write scalability. tapirs takes a fundamentally different approach: all replicas are symmetric, clients coordinate transactions directly, and sharding distributes both read and write load across the cluster. FoundationDB uses a centralized sequencer to order all transactions — simpler to reason about, but a potential bottleneck under high contention. tapirs avoids this by using client-proposed timestamps validated via OCC at each replica independently.

**Next steps:** If you're building a system that needs linearizable distributed transactions with horizontal scalability and no single point of failure, tapirs is worth a close look. Ready to try it? See [Getting Started](../operate/getting-started-testbed.md). Want to understand the protocol? See [Learn](../learn/). Back to [Overview](../README.md).

| Feature | tapirs | etcd | FoundationDB | CockroachDB | TiKV |
|---------|--------|------|-------------|-------------|------|
| Txn coordination | TAPIR (unified with replication) | Raft leader | Sequencer + resolvers | 2PC (parallel commits) | 2PC (Percolator) |
| Storage replication | TAPIR (unified with coordination) | Raft | Paxos (log servers) | Raft | Raft |
| Transactions | [Strict serializable](../learn/concepts/consistency.md) | [Strict serializable](https://etcd.io/docs/v3.5/learning/api_guarantees/) | [Strict serializable](https://apple.github.io/foundationdb/consistency.html) | [Serializable](https://www.cockroachlabs.com/docs/stable/transactions) | [Snapshot isolation](https://tikv.org/deep-dive/distributed-transaction/isolation-level/) |
| Horizontal scalability | Yes (all replicas serve writes, no per-shard leader) | [No](https://etcd.io/docs/v3.2/learning/why/) (single Raft group) | [Yes](https://apple.github.io/foundationdb/scalability.html) (sequencer ceiling) | [Yes](https://www.cockroachlabs.com/docs/stable/frequently-asked-questions) (per-range leaders) | [Yes](https://tikv.org/deep-dive/scalability/introduction/) (per-range leaders) |
| Sharding | Built-in, online | None (single range) | Automatic | Automatic | Automatic |
| Leader required in txn processing | No | Yes | Yes (sequencer) | Yes (per-range) | Yes (per-range) |
| Use as backend of high-level DB | Yes | Metadata only | Yes | SQL built-in | Yes (via TiDB) |
| Use as metadata store | Yes | Yes (primary use) | Yes | Possible (overkill) | No (PD is metadata store) |
| Use as document store | No | No | Yes (Document Layer) | Yes (JSONB) | No |
| Use as cache store | Yes | No | No | No | No |

> **Note:** Performance benchmarks for this implementation are planned — see [Roadmap](../roadmap.md). The original [TAPIR paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) (S7) reports 50% lower latency and 3x better throughput than Paxos-based systems.
