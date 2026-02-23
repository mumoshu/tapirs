# tapirs

> A horizontally-scalable transactional key-value store with linearizable transactions — leaderless and no single point of failure.

```
                       tapirs cluster

  ┌─────────┐   strict serializable txn   ┌────────────────┐
  │ Clients │        ───────────►         │  Sharded and   │
  └─────────┘                             │   leaderless   │
                                          │    replicas    │
                                          └────────────────┘

            one binary · no external deps
```

**What is tapirs:** tapirs is a Rust implementation of the [TAPIR protocol](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) extended with persistent storage, range scan, online resharding, and self-contained discovery. Every replica is symmetric — there is no leader in the transaction hot path, so there is no single point of failure and no failover delay. Read-write transactions provide strict serializability; read-only transactions provide linearizability with a single-replica fast path. See the [full documentation](docs/README.md) for architecture, concepts, and operations guides.

**Quick start:** Get started in under a minute: `scripts/testbed.sh up` bootstraps a 3-node Docker cluster with 2 shards, `tapi client --repl` opens a transaction shell, and `scripts/testbed.sh down` tears it all down. See [Getting Started](docs/operate/getting-started-testbed.md) for the full walkthrough, or [single-node mode](docs/operate/getting-started-testbed-solo.md) for local development without Docker. Deciding whether to adopt? See [Evaluate](docs/evaluate/) for benefits and a comparison with etcd, FoundationDB, CockroachDB, and TiKV.

- **Protocol** — leaderless transactions (no single point of failure), strict serializability (strongest consistency guarantee), coordinator recovery (automatic cleanup when a client crashes mid-transaction), read-only fast path (single-replica reads without a prepare phase), range scan with phantom-write protection (detects writes into previously-scanned ranges)
- **Operations** — online resharding (split hot shards, merge cold shards, compact to reclaim resources — all without downtime), backup/restore, self-contained discovery (no ZooKeeper/etcd required), Docker orchestration, CLI + REPL
- **Engineering** — deterministic simulation testing (every test is 100% reproducible), fault injection (network + disk failures), fuzz with view-change injection (random workloads + concurrent consensus disruption), Maelstrom/Jepsen verification (external linearizability checker), WiscKey SSD engine (reduced write amplification), optional io_uring transport (kernel-bypass I/O)

tapirs is in active development. The protocol implementation is complete and verified by deterministic simulation testing and Jepsen-compatible linearizability checks. Production hardening is ongoing. See [Roadmap](docs/roadmap.md) for what's planned next.

Built by [@mumoshu](https://github.com/mumoshu). Extended from [tapi-rs](https://github.com/finnbear/tapi-rs) by Finn Bear. See [History](docs/history.md) for the full story.
