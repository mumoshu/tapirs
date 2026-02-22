# Code Tour

```
  Reading Order
  =============

  1. src/ir/replica.rs        IR consensus (foundation)
         |
  2. src/occ/store.rs         Conflict detection
         |
  3. src/tapir/replica.rs     TAPIR = IR + OCC
         |
  4. src/tapir/client.rs      Transaction lifecycle
         |
  5. src/mvcc/store.rs        Versioned KV storage
         |
  6. src/mvcc/disk/           WiscKey persistent storage
         |
  7. src/transport/tcp.rs     Network transport
         |
  8. tests/tapir_integration  End-to-end tests
```

**How to use this guide:** This guide provides a recommended reading order for the tapirs source code. Whether you're evaluating the implementation quality, planning to contribute, or studying distributed systems through working code, following this order builds understanding incrementally — each file builds on concepts from the previous one. You don't need to read every line; the guide highlights the key functions and data structures in each file.

**Core protocol (steps 1--4):** Start with the IR consensus layer: `src/ir/replica.rs` (~700 lines) is the most complex file and the foundation everything else builds on — read `handle_propose_consensus()`, `handle_do_view_change()`, and `handle_finalize()`. Next, `src/occ/store.rs` shows how transaction conflicts are detected — read `prepare()` and the conflict check helper. Then `src/tapir/replica.rs` ties IR and OCC together — read `exec_consensus()` (the TAPIR-OCC-CHECK from Fig. 9) and `merge()`/`sync()` (the view change upcalls). Then `src/tapir/client.rs` shows the client-side transaction lifecycle — read `commit()` and `tapir_decide()`.

**Supporting layers (steps 5--8):** After the core protocol, explore the supporting layers: `src/mvcc/store.rs` (versioned key-value store), `src/mvcc/disk/` (WiscKey persistent storage — LSM tree, value log, crash-safe manifest), `src/transport/tcp.rs` (production TCP transport), and `src/discovery/mod.rs` (self-contained discovery). The test files in `tests/` show how all layers work together — start with `tests/tapir_integration.rs` for multi-shard transaction tests. For mapping paper sections to source files, see [Paper Map](paper-map.md). Back to [Learn](README.md).

| Order | File | Lines | What to read | What you'll learn |
|-------|------|-------|-------------|-------------------|
| 1 | `src/ir/replica.rs` | ~700 | `handle_propose_consensus()`, `handle_do_view_change()` | IR consensus without ordering |
| 2 | `src/occ/store.rs` | ~500 | `prepare()`, conflict checks | Transaction conflict detection |
| 3 | `src/tapir/replica.rs` | ~400 | `exec_consensus()`, `merge()`, `sync()` | How TAPIR composes IR + OCC |
| 4 | `src/tapir/client.rs` | ~300 | `commit()`, `tapir_decide()` | Client-side transaction lifecycle |
| 5 | `src/mvcc/store.rs` | ~200 | `get()`, `put()`, versioning | Multi-version key-value storage |
| 6 | `src/mvcc/disk/` | ~800 | `lsm.rs`, `vlog.rs`, `memtable.rs`, `manifest.rs` | WiscKey persistent storage (LSM + value log) |
| 7 | `src/transport/tcp.rs` | ~300 | `TcpTransport`, connection handling | Production network transport |
| 8 | `tests/tapir_integration.rs` | ~300 | Multi-shard transaction tests | How all layers work together |
