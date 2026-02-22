# tapirs at a Glance

```
  +----------+     +----------+     +----------+
  | Client A |     | Client B |     | Client C |
  +----+-----+     +----+-----+     +----+-----+
       |                |                |
       +-------+--------+--------+-------+
               |                 |
               v                 v
  +------+  +------+  +------+  +------+  +------+
  |  R1  |  |  R2  |  |  R3  |  |  R4  |  |  R5  |
  +------+  +------+  +------+  +------+  +------+
     Symmetric Replicas — no leader, no SPOF

  Discovery: embedded (static JSON | DNS | TAPIR-backed)
  Storage:   crash-safe WiscKey SSD engine per replica
  Binary:    one binary, one cluster, self-contained
```

**What it is:** tapirs is a distributed transactional key-value store that provides the strongest consistency guarantee — strict serializability — without a single point of failure. Every replica is symmetric: there is no leader in the transaction hot path, so there is no failover delay when a node goes down. The system is entirely self-contained — one binary, one cluster, no external coordination service required.

**Who it's for:** tapirs is designed for workloads that need strongly-consistent distributed transactions with horizontal scalability: SQL and NoSQL database backends, stateful microservices, and any application where data must be correct across multiple nodes. The system grows with your workload through online resharding — split hot shards to distribute load, merge cold shards to save resources — all without downtime for reads.

**How it's built:** tapirs is built in Rust, extended from the [TAPIR protocol](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) with persistent storage (crash-safe WiscKey SSD engine), range scan with phantom-write protection, online resharding (split/merge/compact), and self-contained discovery (no ZooKeeper/etcd). Every test is deterministic and 100% reproducible. Built by [@mumoshu](https://github.com/mumoshu).

**Key points for decision-makers:**

- **Always available** — no leader, no failover delay, no single point of failure
- **Simple to deploy** — one binary, self-contained discovery, Docker testbed included
- **Horizontally scalable** — online resharding distributes load without downtime
- **Strongest consistency** — strict serializability for writes, linearizability for reads
- **Correct by design** — deterministic simulation testing, fault injection, Jepsen-compatible verification
