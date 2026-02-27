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
                    |   9 tests         |  resharding, recovery
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Unit Tests      |  OCC, MVCC, LSM,
                    |   293 lib tests   |  IR replica logic
                    +-------------------+

    Deterministic simulation at every layer:
    start_paused=true + seeded StdRng + FaultyChannel + FaultyDiskIo
```

**Deterministic by default:** Every test in tapirs is deterministic -- `start_paused = true` decouples simulated time from the wall clock, and all randomness uses seeded RNG (`StdRng`). This means test failures are 100% reproducible: same seed, same execution, same result. FaultyChannel (network) and FaultyDiskIo (disk) inject failures at the transport and storage layers independently, without modifying protocol logic.

**Fuzz and Jepsen:** The custom fuzz harness (`fuzz_tapir_transactions`) generates random transaction workloads with concurrent view changes injected at random points, running thousands of iterations to find subtle ordering bugs. Maelstrom/Jepsen integration provides external linearizability verification. The test suite covers unit (OCC store, MVCC store), integration (multi-shard transactions, resharding), and stress (lock server contention, coordinator recovery under failures).

**Related docs:** See [Testing concepts](../concepts/ir.md) for how view changes interact with consensus, [Architecture Decisions](architecture-decisions.md) for why deterministic simulation, and [Storage](storage.md) for FaultyDiskIo. Key files: `tests/`, `src/transport/faulty_channel.rs`, `src/mvcc/disk/faulty_disk_io.rs`.

| Layer | What it tests | Key tool |
|-------|--------------|----------|
| Unit | OCC conflict detection, MVCC versioning, LSM compaction | `cargo test` -- 293 lib tests |
| Integration | Multi-shard txns, resharding, coordinator recovery, view changes | `cargo test` -- 9 integration tests |
| Fuzz | Random txn workloads + concurrent view change injection | `fuzz_tapir_transactions` harness |
| Maelstrom | External linearizability verification (5 modes) | Jepsen-compatible checker |
| Fault injection (network) | Message loss, delay, reordering | `FaultyChannel` transport |
| Fault injection (disk) | Read/write errors, corruption | `FaultyDiskIo` |

## Maelstrom / Jepsen

Maelstrom provides external linearizability verification via the Jepsen/Knossos checker. `make maelstrom` runs five test modes that validate different read methods under different clock configurations. The suite includes both positive tests (must pass linearizability) and negative tests (must fail, validating that known violations are detectable).

**Environment variables:**

| Variable | Description | Example |
|----------|-------------|---------|
| `TAPIR_CLOCK_SKEW_MAX` | Maximum clock jitter in ms (0 = synchronized) | `0`, `100`, `1000` |
| `TAPIR_LINEARIZABLE_READ_METHOD` | Read path: `ro_txn_get` (RO quorum) or `rw_txn_get_commit` (RW OCC) | `ro_txn_get` |
| `TAPIR_RO_FAST_PATH_DELAY_MS` | Fast path delay in ms (enables opt-in fast path) | `200` |
| `TAPIR_READ_TIMEOUT_MS` | Fast path `read_validated` timeout in ms | `200` |
| `TAPIR_VIEW_CHANGE_INTERVAL_MS` | View change interval in ms (for fast path sync) | `200` |
| `TAPIR_RO_CLOCK_SKEW_UNCERTAINTY_BOUND` | TrueTime uncertainty bound in ms | `200` |

**Test modes:**

| Target | Clock skew | Read method | Expected | What it validates |
|--------|-----------|-------------|----------|-------------------|
| `maelstrom-sync-ro-txn-get` | 0 (sync) | RO quorum | Pass | Quorum reads linearizable with ε=0 (perfectly synchronized clocks) |
| `maelstrom-skewed-rw-txn-get-commit` | 1000ms | RW OCC | Pass | OCC validates reads regardless of clock skew |
| `maelstrom-skewed-ro-txn-get-fail` | 1000ms | RO quorum | **Fail** | Paper S6.1: RO quorum reads NOT linearizable under unbounded clock skew without sufficient ε |
| `maelstrom-sync-ro-fast-path` | 0 (sync) | RO fast path (200ms delay) | Pass | Fast path linearizable when delay >= view change interval |
| `maelstrom-skew-ro-slow-path-truetime` | 100ms | RO quorum + TrueTime (200ms ε) | Pass | TrueTime uncertainty bound makes quorum reads linearizable under bounded skew |

The negative test (`skewed-ro-txn-get-fail`) inverts the exit code: it expects maelstrom to report `:valid? false` and fails CI if the test unexpectedly passes.

An additional target `maelstrom-sync-ro-fast-path-may-fail` is available for manual investigation but not included in `make maelstrom` because the result is probabilistic — with zero-latency transport, async `FinalizeInconsistent` propagates nearly instantly, so `read_validated` often returns correct data even with a 1ms delay.
