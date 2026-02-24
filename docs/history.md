# History

<!-- Commit range: 38a86d19..fabf61c2 -->
<!-- Original tapir-rs: 38a86d19cfa4e78fc83c3ad39dba27fccb85f733 -->
<!-- Current tapirs:     fabf61c2e21e29b0da095b6847c687b91e0f130e -->

```
 2014─2015              2023                     2025─2026                                   2026
─────────────────────────────────────────────────────────────────────────────────────────────────────
  [UWSysLab/tapir]       [tapi-rs]             [377 commits]                             [tapirs]
  Research C++:          Foundation:           Extensions:                                Result:
  IR consensus,          Async Rust port,      WiscKey-inspired replica storage,           Production-grade
  TAPIR txns,            IR consensus,         range scan, online resharding, CDC,        transactional KV
  OCC, NFS/YCSB          TAPIR txns,           discovery, CLI, Docker,                    with linearizable
  benchmarks             OCC, MVCC,            K8s operator, TLS, sim testing             transactions
                         channel transport
─────────────────────────────────────────────────────────────────────────────────────────────────────
```

## Origin

tapirs is built by [Yusuke Kuoka](https://github.com/mumoshu) ([@mumoshu](https://github.com/mumoshu)), starting from a fork of [tapi-rs](https://github.com/finnbear/tapi-rs) by Finn Bear.

The TAPIR protocol was designed at the University of Washington Systems Lab by Irene Zhang, Naveen Kr. Sharma, Adriana Szekeres, Arvind Krishnamurthy, and Dan Ports. Their [C++ research implementation](https://github.com/UWSysLab/tapir) demonstrated IR consensus and TAPIR transactions with NFS and YCSB benchmarks. The [paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) was published at SOSP 2015.

The original tapi-rs was a clean Rust implementation of the TAPIR protocol's core transaction lifecycle (get, put, prepare, commit, abort) with IR consensus, view changes, and recovery.

It provided a solid foundation of an async Rust IR replica, TAPIR client, OCC store, MVCC memory store, and an in-process channel transport for testing.

## Engineering challenge

Extending TAPIR from a research prototype into a production-grade system — with persistent storage, range scan, online resharding, and self-contained discovery, while preserving its ability to execute linearizable transactions across a horizontally-scaled cluster — was far from straightforward.

Each extension had to integrate with the leaderless consensus protocol without breaking correctness: a range scan must detect phantom writes across shards, a shard split must drain in-flight transactions without losing prepared state, and discovery must bootstrap without an external coordinator.

Deterministic simulation testing (seeded RNG, paused clock, fault injection, fuzz with view-change injection) was indispensable — without reproducible test runs that expose subtle concurrency and ordering bugs, this scale of extension would not have been feasible.

## By the numbers

377 commits, 50,000+ lines of changes across 304 files — growing the codebase from 5,700 lines (41 files) to 55,000 lines (307 files), roughly 9.7x the original.

The original tapir-rs ends at commit `38a86d19`; all subsequent work through `fabf61c2` is tapirs.

See the table below for what came from tapi-rs and what tapirs added.

| Area | tapi-rs (foundation) | tapirs (added) |
|------|---------------------|----------------|
| Consensus | IR replica, view change, recovery | VersionedRecord (base/overlay), delta view change, membership reconfigure, exponential backoff retry |
| Transactions | Get, put, prepare, commit, abort | Range scan, phantom protection, coordinator recovery, read-only fast path, backup/restore |
| Storage | MVCC memory store | WiscKey-inspired replica storage (LSM + value log), crash-safe compaction, vlog GC |
| Sharding | Static shard routing | Online split/merge/compact, CDC view-based cursors, prepared-drain |
| Transport | In-process channel | Tokio TCP (default), io_uring TCP + O_DIRECT disk (optional), fault-injecting channel |
| Discovery | — | Static JSON, DNS, TAPIR-backed self-discovery |
| Operations | — | CLI (tapi/tapictl/tapiadm), Docker Compose, backup/restore, REPL |
| Kubernetes | — | Go operator (CRD, phase-based reconciler, Helm charts, Kind E2E) |
| TLS | — | Optional mTLS via rustls, cert-manager integration, reloadable certs |
| Testing | Basic IR lock server, TAPIR-KV | Deterministic sim, fault injection, fuzz with view changes, Maelstrom linearizability |
| CI/CD | — | GitHub Actions (test, Maelstrom, operator, testbed, fuzz), `make ci` |
| Toolchain | Nightly Rust (nightly-2023-06-08) | Migrated to stable Rust 1.93.0, edition 2024; removed 5 nightly feature gates |

Run `git log --oneline 38a86d19..fabf61c2` to see the full commit list.
