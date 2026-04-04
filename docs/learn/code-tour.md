# Code Tour

```
  Reading Order
  =============

  1. src/ir/replica.rs        IR consensus (foundation)
         |
  2. src/occ/mod.rs           Conflict detection types
         |
  3. src/tapir/replica.rs     TAPIR = IR + OCC
         |
  4. src/tapir/client.rs      Transaction lifecycle
         |
  5. src/unified/combined/    CombinedStore (unified IR + TAPIR storage)
         |
  6. src/mvcc/disk/           WiscKey persistent storage
         |
  7. src/transport/tcp.rs     Network transport
         |
  8. tests/tapir_integration  End-to-end tests
```

**How to use this guide:** This guide provides a recommended reading order for the tapirs source code. Whether you're evaluating the implementation quality, planning to contribute, or studying distributed systems through working code, following this order builds understanding incrementally — each file builds on concepts from the previous one. You don't need to read every line; the guide highlights the key functions and data structures in each file.

**Core protocol (steps 1--4):** Start with the IR consensus layer: `src/ir/replica.rs` (~700 lines) is the most complex file and the foundation everything else builds on — read `handle_propose_consensus()`, `handle_do_view_change()`, and `handle_finalize()`. Next, `src/occ/mod.rs` defines the OCC types (`PrepareResult`, `PrepareConflict`, `Transaction`). Then `src/tapir/replica.rs` ties IR and OCC together — read `exec_consensus()` (the TAPIR-OCC-CHECK from Fig. 9) and `merge()`/`sync()` (the view change upcalls). Then `src/tapir/client.rs` shows the client-side transaction lifecycle — read `commit()` and `tapir_decide()`.

**Supporting layers (steps 5--8):** After the core protocol, explore the supporting layers: `src/unified/combined/` (CombinedStore — unified IR record + TAPIR transaction storage with deduplication), `src/mvcc/disk/` (WiscKey persistent storage — VlogLsm, SSTable, value log, crash-safe manifest), `src/transport/tcp.rs` (production TCP transport), and `src/discovery/mod.rs` (self-contained discovery). The test files in `src/tapir/tests/` show how all layers work together — start with the kv tests for multi-shard transaction tests. For mapping paper sections to source files, see [Paper Map](paper-map.md). Back to [Learn](README.md).

| Order | File | What to read | What you'll learn |
|-------|------|-------------|-------------------|
| 1 | `src/ir/replica.rs` | `handle_propose_consensus()`, `handle_do_view_change()` | IR consensus without ordering |
| 2 | `src/occ/mod.rs` | `PrepareResult`, `PrepareConflict`, `Transaction` | OCC conflict types |
| 3 | `src/tapir/replica.rs` | `exec_consensus()`, `merge()`, `sync()` | How TAPIR composes IR + OCC |
| 4 | `src/tapir/client.rs` | `commit()`, `tapir_decide()` | Client-side transaction lifecycle |
| 5 | `src/unified/combined/` | `CombinedStoreInner`, `CombinedTapirHandle`, `CombinedRecordHandle` | Unified IR + TAPIR storage with deduplication |
| 6 | `src/mvcc/disk/` | `lsm.rs`, `vlog.rs`, `memtable.rs`, `manifest.rs` | WiscKey persistent storage (VlogLsm + value log) |
| 7 | `src/transport/tcp.rs` | `TcpTransport`, connection handling | Production network transport |
| 8 | `src/tapir/tests/kv/` | Multi-shard transaction tests, fuzz tests | How all layers work together |
