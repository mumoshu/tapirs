# tapirs Documentation

```
  ┌─────────┐         ┌─────────────────────────────────────┐
  │         │   txn   │          tapirs node                 │
  │  Client │────────►│                                      │
  │         │◄────────│  ┌───────────┐    ┌───────────────┐  │
  └─────────┘  result │  │  Shard A  │    │   Discovery   │  │
                      │  │ (replica) │◄──►│  (embedded)   │  │
  ┌─────────┐   txn   │  ├───────────┤    └───────────────┘  │
  │         │────────►│  │  Shard B  │                       │
  │  Client │◄────────│  │ (replica) │    one binary          │
  │         │  result │  └───────────┘    no external deps    │
  └─────────┘         └─────────────────────────────────────┘
```

**What tapirs is:** tapirs is a leaderless transactional key-value store built on the [TAPIR protocol](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf). Read-write transactions provide strict serializability — serializable ordering that also respects real time. Read-only transactions provide linearizability with a validated single-replica fast path and quorum fallback. The entire system is self-contained: nodes discover each other through an embedded directory service — no ZooKeeper, etcd, or external coordinator to provision and maintain.

**Who it's for:** Use tapirs as the distributed storage engine for SQL or NoSQL databases, or directly as a transactional key-value store for stateful applications that need strongly-consistent distributed transactions. See [Use Cases](evaluate/usecases.md) for a detailed fit matrix covering when tapirs is — and isn't — the right choice.

**How to navigate:** The documentation is organized by what you're trying to do. Pick the section that matches your intent, or follow the links in order for a complete tour.

- **[Evaluate](evaluate/)** — deciding whether to adopt? Read [Benefits](evaluate/benefits.md), [Use Cases](evaluate/usecases.md), and the [Comparison](evaluate/comparison.md) with etcd, FDB, CockroachDB, TiKV
- **[Learn](learn/)** — understanding how tapirs works? Start with [Concepts](learn/concepts/) for foundational terms, then explore [Internals](learn/internals/) for architecture deep-dives
- **[Operate](operate/)** — running a cluster? See [Getting Started](operate/getting-started-testbed.md) and the [CLI Reference](operate/cli-reference.md)
- **[Integrate](integrate/)** — building an app or database on tapirs? See [Integration Patterns](integrate/README.md) and the [Rust Client SDK](integrate/rust-client-sdk.md)
- **[Roadmap](roadmap.md)** — what's planned next: Kubernetes testbed, observability, operator, benchmarks
- **[History](history.md)** — where tapirs came from and what it took to build
