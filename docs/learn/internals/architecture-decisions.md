# Architecture Decisions

```
+-------+  +-------+  +-------+  +-------+  +-------+  +-------+
| ADR-1 |  | ADR-2 |  | ADR-3 |  | ADR-4 |  | ADR-5 |  | ADR-6 |
|Bitcode|  | View  |  |Single |  |Shared |  |Remote |  |Effect.|
|  not  |  | CDC   |  |Thread |  | Txn   |  |Tomb-  |  |EndView|
| JSON  |  |  not  |  | State |  | Arc   |  |stones |  |Option |
|       |  |  TS   |  |Machine|  |       |  |       |  | u64   |
+-------+  +-------+  +-------+  +-------+  +-------+  +-------+
```

**What this is:** Every system has decisions that seem arbitrary from the outside but were carefully chosen for specific reasons. This document records the key architectural decisions in tapirs using ADR (Architecture Decision Record) style -- each entry explains the context that motivated the decision, the alternatives that were considered and rejected, and the consequences (both positive and negative) of the chosen approach. These are the decisions you'd want to know about before proposing a change to the codebase.

**How they were validated:** These decisions are stable -- they emerged from implementation experience and extensive testing, not upfront speculation. Bitcode was chosen over JSON after profiling showed consensus message serialization was a bottleneck. View-based CDC came from discovering that TAPIR timestamps are non-monotonic (because clients pick them from system clocks). Single-threaded state machines follow directly from the original paper's TLA+ specification, which models each replica as a conceptually single-threaded process -- tapirs preserves this with Mutex per replica, and sharding provides parallelism across replicas. Each decision has been validated by the test suite: if you change one, expect tests to break in ways that reveal exactly why the decision was made.

**Further reading:** For deeper context on any specific decision, see the corresponding [Internals](README.md) deep-dive. The [Testing](testing.md) doc explains the deterministic simulation framework that validated these decisions.

- **ADR-1: Bitcode not JSON** -- *Context:* Wire format needs to be compact and fast; JSON too slow for consensus messages. *Alternatives rejected:* JSON (too slow), bincode (less compact). *Consequence:* bitcode has no `skip_serializing_if` support (trade-off accepted).

- **ADR-2: View-based CDC not timestamp-based** -- *Context:* TAPIR timestamps are non-monotonic (client-chosen from system clocks); views are monotonic. *Alternatives rejected:* Timestamp-based cursors (non-monotonic, would require complex deduplication). *Consequence:* Simpler cursor logic; CDC naturally aligns with view change boundaries.

- **ADR-3: Single-threaded state machine** -- *Context:* The IR/TAPIR replica model defined and verified using TLA+ in the original paper is conceptually single-threaded; tapirs preserves this by using Mutex (not RwLock) per replica, keeping the implementation faithful to the proven model. *Alternatives rejected:* RwLock (would diverge from TLA+ model, risking subtle concurrency bugs). *Consequence:* No parallelism within a replica, but sharding provides parallelism across replicas -- you can run hundreds of shard replicas in a single node to use tapirs as a high-performance single-node KV store.

- **ADR-4: `SharedTransaction = Arc<Transaction>`** -- *Context:* Avoid deep clones for multi-shard prepare. *Alternatives rejected:* Deep clone (expensive for large transactions). *Consequence:* Refcount bump instead of copy; requires serde `rc` feature for serialization.

- **ADR-5: Remote tombstones** -- *Context:* Prevent push-pull cycles in discovery sync where decommissioned shards get resurrected by stale pushes. *Alternatives rejected:* Timestamp-based expiry (requires clock sync). *Consequence:* Tombstoned shards cannot be re-registered; `DiscoveryError::Tombstoned` rejects future puts.

- **ADR-6: `effective_end_view` as `Option<u64>`** -- *Context:* Distinguish "no deltas" from "delta at view 0". *Alternatives rejected:* Plain `u64` (cannot distinguish zero-deltas from view-0-deltas). *Consequence:* `CdcCursor` consumes correctly; slightly more complex type signature.
