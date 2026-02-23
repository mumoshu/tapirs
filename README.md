# tapirs

```
                       tapirs cluster

  ┌─────────┐   strict serializable txn   ┌────────────────┐
  │ Clients │        ───────────►         │  Sharded and   │
  └─────────┘                             │   leaderless   │
                                          │    replicas    │
                                          └────────────────┘

            one binary · no external deps
```

tapirs is a Rust implementation of the [TAPIR protocol](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) — a leaderless transactional key-value store. Read-write transactions provide strict serializability; read-only transactions provide linearizability with a single-replica fast path. No single point of failure, built-in online resharding, and no external dependencies.

| | |
|---|---|
| **No leader, no failover delay** — symmetric replicas, no SPOF | **One binary, zero dependencies** — embedded discovery, no ZooKeeper/etcd |
| **Online resharding** — split, merge, compact without downtime | **Deterministic testing** — simulation, fault injection, Jepsen-compatible |

## Quick start

```sh
scripts/testbed.sh up     # 3-node cluster, 2 shards
tapi client --repl        # interactive transaction shell
scripts/testbed.sh down   # teardown
```

Full walkthrough: [Docker testbed](docs/operate/getting-started-testbed.md) · [Single-node mode](docs/operate/getting-started-testbed-solo.md)

## Documentation

**Evaluating tapirs for your project?** [Evaluate tapirs](docs/evaluate/) walks you through what makes it different, whether it fits your use case, and how it compares with etcd, FoundationDB, CockroachDB, and TiKV.

**Building an application on tapirs?** [Building on tapirs](docs/integrate/) covers the Rust client SDK, consistency levels, and integration patterns for SQL databases, document stores, and stateful services.

**Running a cluster?** [Operate tapirs](docs/operate/) has everything from a one-minute Docker testbed to CLI reference, monitoring, and troubleshooting.

**Curious how it works?** [Learn how tapirs works](docs/learn/) covers the foundational concepts — TAPIR, IR consensus, OCC — and goes deeper into architecture, protocol internals, and design decisions.

**Contributing or studying the code?** The [Code Tour](docs/learn/code-tour.md) gives a recommended reading order through the source, with a [Paper Map](docs/learn/paper-map.md) connecting each section of the TAPIR paper to the corresponding code.

Full documentation: [docs/](docs/README.md) · [Roadmap](docs/roadmap.md) · [History](docs/history.md)

## Status

Protocol complete and verified by deterministic simulation testing and Jepsen-compatible linearizability checks. Production hardening is ongoing. See [Roadmap](docs/roadmap.md) for what's planned next.

Built by [@mumoshu](https://github.com/mumoshu), extended from [tapi-rs](https://github.com/finnbear/tapi-rs) by Finn Bear. See [History](docs/history.md).
