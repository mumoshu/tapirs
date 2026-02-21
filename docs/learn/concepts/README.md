# Concepts

```
                    Linearizability
                         ^
                         |
        +----------------+----------------+
        |                                 |
   Strict Serializability          RO Validated Reads
        ^                                 ^
        |                                 |
   OCC (conflict detection)         MVCC (versioned reads)
        ^                                 ^
        |                                 |
   IR Consensus -------> View Change --> Quorum
```

**P1 — Start here:** tapirs combines ideas from distributed systems, database internals, and systems programming. If you're familiar with Paxos or Raft, the first thing to understand is that tapirs is *leaderless* — there is no leader for normal operations. Instead, clients coordinate transactions directly, replicas validate independently via OCC, and IR consensus ensures agreement without ordering. Start with [TAPIR](tapir.md) for a high-level overview of the protocol and how it combines IR + OCC, then read [Consistency](consistency.md) to understand what "strict serializability" and "linearizability" mean in tapirs, and [IR](ir.md) to see how the consensus layer achieves fault tolerance without consistency.

**P2 — Supporting concepts:** Once you understand the protocol and consistency model, the remaining concepts build on them naturally: [OCC](occ.md) explains how conflicts are detected at prepare time, [Storage](storage.md) covers the WiscKey engine that persists MVCC data, [Resharding](resharding.md) explains how shards split and merge online, [Discovery](discovery.md) shows how nodes find each other without external coordinators, and [Client](client.md) describes the routing layer that ties everything together.

**P3 — How to use this index:** Each concept doc follows the same structure: a brief explanation of the concept in tapirs context, a glossary table mapping terms to code, and links to the [Internals](../internals/) deep-dives for implementation details. When you encounter an unfamiliar term in the internals docs, come back here.

| Concept | Start here if you're asking... |
|---------|-------------------------------|
| [TAPIR](tapir.md) | "What is TAPIR? How does it combine IR and OCC to achieve linearizable transactions?" |
| [Consistency](consistency.md) | "What isolation level do I get? What's the difference between RW and RO transactions?" |
| [IR (Inconsistent Replication)](ir.md) | "How does consensus work without a leader? What are unlogged/inconsistent/consensus ops?" |
| [OCC](occ.md) | "How are transaction conflicts detected? What does PREPARE-OK vs RETRY vs ABSTAIN mean?" |
| [Storage](storage.md) | "How is data persisted to disk? What's WiscKey, LSM, vlog?" |
| [Resharding](resharding.md) | "How do shards split without losing transactions? What's CDC and prepared-drain?" |
| [Discovery](discovery.md) | "How do nodes find each other? Why no ZooKeeper?" |
| [Client](client.md) | "How does a request get routed to the right shard? What happens on OutOfRange?" |
