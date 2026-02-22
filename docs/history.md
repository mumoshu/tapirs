# History

```
 2023                          2025─2026                            2026
──────────────────────────────────────────────────────────────────────────
  [tapi-rs]                  [217 commits]                      [tapirs]
  Foundation:                Extensions:                         Result:
  IR consensus,              WiscKey SSD, range scan,            Production-grade
  TAPIR txns,                online resharding, CDC,             transactional KV
  OCC, MVCC,                 discovery, CLI, Docker,             with linearizable
  channel transport           deterministic sim testing          transactions
──────────────────────────────────────────────────────────────────────────
```

**Origin:** tapirs is built by [Yusuke Kuoka](https://github.com/mumoshu) ([@mumoshu](https://github.com/mumoshu)), starting from a fork of [tapi-rs](https://github.com/finnbear/tapi-rs) by Finn Bear. The original tapi-rs was a clean Rust implementation of the TAPIR protocol's core transaction lifecycle (get, put, prepare, commit, abort) with IR consensus, view changes, and recovery — providing a solid foundation of an async Rust IR replica, TAPIR client, OCC store, MVCC memory store, and an in-process channel transport for testing.

**Engineering challenge:** Extending TAPIR from a research prototype into a production-grade system — with persistent storage, range scan, online resharding, and self-contained discovery, while preserving its ability to execute linearizable transactions across a horizontally-scaled cluster — was far from straightforward. Each extension had to integrate with the leaderless consensus protocol without breaking correctness: a range scan must detect phantom writes across shards, a shard split must drain in-flight transactions without losing prepared state, and discovery must bootstrap without an external coordinator. Deterministic simulation testing (seeded RNG, paused clock, fault injection, fuzz with view-change injection) was indispensable — without reproducible test runs that expose subtle concurrency and ordering bugs, this scale of extension would not have been feasible.

**By the numbers:** 217 commits, 30,000+ lines across 128 files — roughly 28x the original codebase. See the table below for what came from tapi-rs and what tapirs added.

| Area | tapi-rs (foundation) | tapirs (added) |
|------|---------------------|----------------|
| Consensus | IR replica, view change, recovery | Membership change, exponential backoff retry |
| Transactions | Get, put, prepare, commit, abort | Range scan, phantom protection, coordinator recovery, RO fast path |
| Storage | MVCC memory store | WiscKey SSD (LSM + value log), crash-safe compaction |
| Sharding | Static shard routing | Online split/merge/compact, CDC cursors, prepared-drain |
| Transport | In-process channel | Tokio TCP (default), io_uring TCP + O_DIRECT disk (optional), fault-injecting channel |
| Discovery | — | Static JSON, DNS, TAPIR-backed self-discovery |
| Operations | — | CLI (tapi/tapictl/tapiadm), Docker, backup/restore, REPL, TOML config |
| Testing | Basic IR lock server, TAPIR-KV | Deterministic sim, fault injection, fuzz with view changes, Maelstrom |
| Toolchain | Nightly Rust (nightly-2023-06-08) | Migrated to stable Rust 1.93.0, edition 2024; removed 5 nightly feature gates |
