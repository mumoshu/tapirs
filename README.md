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

tapirs is a distributed transactional key-value store written in Rust, with no leader on the hot path. All transactions — read-write and read-only — are linearizable in a single round trip from client to storage replicas. Compare that with 2PC over Raft, where every shard crossing requires client → leader → followers → leader → client before the coordinator can even begin the second phase. Eventual-consistent reads are available when you want even more read scalability. A temporary leader coordinates view changes only — never transaction processing.

| | |
|---|---|
| **1 round trip, linearizable** — no leader relay, no multi-phase overhead | **One binary, zero dependencies** — embedded discovery, no ZooKeeper/etcd |
| **Online resharding** — split, merge, compact without downtime | **Deterministic testing** — simulation, fault injection, Jepsen-compatible |

## Quick start

```sh
scripts/testbed-docker-compose.sh up     # 3-node cluster, 2 shards
tapi client --repl                      # interactive transaction shell
scripts/testbed-docker-compose.sh down   # teardown
```

Full walkthrough: [Getting Started](docs/operate/getting-started.md)

## Documentation

- [Does tapirs fit my project?](docs/evaluate/) — comparisons with etcd, FoundationDB, CockroachDB, TiKV
- [How do I build on tapirs?](docs/integrate/) — Rust client SDK, consistency levels, integration patterns
- [How do I run a cluster?](docs/operate/) — Docker testbed, CLI reference, monitoring, troubleshooting
- [How does it work?](docs/learn/) — consensus, concurrency control, architecture, protocol internals
- [How do I contribute?](docs/learn/code-tour.md) — code tour, [paper map](docs/learn/paper-map.md)

Full documentation: [docs/](docs/README.md) · [Roadmap](docs/roadmap.md) · [History](docs/history.md)

## Status

Protocol complete and verified by deterministic simulation testing and Jepsen-compatible linearizability checks. Production hardening is ongoing. See [Roadmap](docs/roadmap.md) for what's planned next.

Built by [@mumoshu](https://github.com/mumoshu), extended from [tapi-rs](https://github.com/finnbear/tapi-rs) by Finn Bear. See [History](docs/history.md).
