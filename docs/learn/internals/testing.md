# Testing Strategy

```
                    +-------------------+
                    |    Maelstrom      |  External linearizability
                    | (Jepsen checker)  |  verification
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Fuzz Harness    |  Random txn workloads +
                    | fuzz_tapir_txns   |  concurrent view change
                    +--------+----------+  injection
                             |
                    +--------v----------+
                    |   Integration     |  Multi-shard txns,
                    |   20 tests        |  resharding, recovery
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Unit Tests      |  OCC, MVCC, LSM,
                    |   202 lib tests   |  IR replica logic
                    +-------------------+

    Deterministic simulation at every layer:
    start_paused=true + seeded StdRng + FaultyChannel + FaultyDiskIo
```

**P1 -- Deterministic by default:** Every test in tapirs is deterministic -- `start_paused = true` decouples simulated time from the wall clock, and all randomness uses seeded RNG (`StdRng`). This means test failures are 100% reproducible: same seed, same execution, same result. FaultyChannel (network) and FaultyDiskIo (disk) inject failures at the transport and storage layers independently, without modifying protocol logic.

**P2 -- Fuzz and Jepsen:** The custom fuzz harness (`fuzz_tapir_transactions`) generates random transaction workloads with concurrent view changes injected at random points, running thousands of iterations to find subtle ordering bugs. Maelstrom/Jepsen integration provides external linearizability verification. The test suite covers unit (OCC store, MVCC store), integration (multi-shard transactions, resharding), and stress (lock server contention, coordinator recovery under failures).

**P3 -- Related docs:** See [Testing concepts](../concepts/ir.md) for how view changes interact with consensus, [Architecture Decisions](architecture-decisions.md) for why deterministic simulation, and [Storage](storage.md) for FaultyDiskIo. Key files: `tests/`, `src/transport/faulty_channel.rs`, `src/mvcc/disk/faulty_disk_io.rs`.

| Layer | What it tests | Key tool |
|-------|--------------|----------|
| Unit | OCC conflict detection, MVCC versioning, LSM compaction | `cargo test` -- 202 lib tests |
| Integration | Multi-shard txns, resharding, coordinator recovery, view changes | `cargo test` -- 20 integration tests |
| Fuzz | Random txn workloads + concurrent view change injection | `fuzz_tapir_transactions` harness |
| Maelstrom | External linearizability verification | Jepsen-compatible checker |
| Fault injection (network) | Message loss, delay, reordering | `FaultyChannel` transport |
| Fault injection (disk) | Read/write errors, corruption | `FaultyDiskIo` |
