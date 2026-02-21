# IR (Inconsistent Replication)

```
  Client                    Replicas (2f+1)
    |                       R0   R1   R2   R3   R4
    |--- PROPOSE (op) ----->|    |    |    |    |
    |                       |    |    |    |    |
    |                    execute independently
    |                       |    |    |    |    |
    |<-- REPLY (result) ----|    |    |    |    |
    |<-- REPLY (result) ---------|    |    |    |
    |<-- REPLY (result) --------------|    |    |
    |                                          |
    |  (f+1 replies: run decide function)      |
    |                                          |
    |--- FINALIZE (result) ->|    |    |    |  |
    |--- FINALIZE (result) ------>|    |    |  |
    |--- FINALIZE (result) ----------->|    |  |
    |--- FINALIZE (result) --------------->|   |
    |--- FINALIZE (result) ------------------->|
```

**P1 — What IR is:** IR (Inconsistent Replication) is a replication protocol that provides fault tolerance *without* consistency ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) §3). Unlike Paxos or Raft, IR presents an unordered operation set rather than an ordered log. It supports two operation modes: *inconsistent* operations that execute in any order and persist across failures, and *consensus* operations where the application protocol's `decide` function selects a single result from candidate results returned by replicas. IR is designed to be composed with a higher-level protocol (like TAPIR) that enforces application-level consistency.

**P2 — IR guarantees:** IR guarantees four properties ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) §3.1.2): P1 (fault tolerance) — every successful operation is in the record of at least one replica in any f+1 quorum; P2 (visibility) — for any two consensus operations, at least one is visible to the other; P3 (consensus results) — successful consensus results persist unless explicitly modified by Merge; P4 (liveness) — all operations eventually execute if all replicas are non-faulty. View changes are coordinated by a per-view leader that merges f+1 records and syncs the result (Paper §3.2.2). Any replica or client can initiate a view change.

**P3 — Paper references:** [Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf): §3 (IR), §3.2 (view change), §3.2.3 (client recovery), §3.3 (correctness proof). Deep-dive: [Protocol](../internals/protocol-tapir.md). Next: [OCC](occ.md). Back to [Concepts](README.md).

| Term | Definition (Paper reference) | Where it appears in tapirs |
|------|---------------------|-----------------|
| Inconsistent op | Executes in any order, persists across failures. One round-trip to f+1 replicas (§3.2.1) | `ir/message.rs` — `InconsistentOp` |
| Consensus op | Client calls `decide` on candidate results; IR ensures chosen result persists. Fast path: ⌈3f/2⌉+1 matching; slow path: f+1 (§3.2.1) | `ir/message.rs` — `ConsensusOp` |
| Decide function | Application-provided function that selects a single result from candidate results (§3.1.1) | `ir/client.rs` — TAPIR implements this as `tapir_decide()` |
| Tentative | Operation recorded but not yet finalized — may be rolled back during view change (§3.2.1) | `ir/record.rs` — `RecordEntryState::Tentative` |
| Finalized | Operation durably applied — survives view changes. ExecInconsistent/ExecConsensus called (§3.2.1) | `ir/record.rs` — `RecordEntryState::Finalized(view)` |
| View Change | Leader merges f+1 records (highest-view entry per OpId), calls Merge then Sync (§3.2.2) | `ir/replica.rs` — `handle_do_view_change()` |
| Merge | Application upcall to reconcile inconsistent replicas into master record (§3.1.1, §3.2.2) | `ir/replica.rs` — TAPIR's `merge()` |
| Sync | Application upcall at each replica to reconcile with master record (§3.1.1, §3.2.2) | `ir/replica.rs` — TAPIR's `sync()` |
| Record | Unordered set of operations — not an ordered log. Each entry: (OpId, op, result, state). Accumulates forever; reclaimed via shard compaction (§3.1) | `ir/record.rs` |
| Quorum | f+1 out of 2f+1 replicas. Fast quorum: ⌈3f/2⌉+1. Quorum intersection guarantees visibility (§3.1.2) | `ir/membership.rs` |
| Client Recovery | Recovering client polls majority for latest OpId, increments counter. View change finalizes tentative ops (§3.2.3) | `ir/client.rs` |
