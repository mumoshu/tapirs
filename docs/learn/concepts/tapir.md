# TAPIR Protocol

```
  Application
      |
      v
  +--------+
  | TAPIR  |  OCC + client-coordinated 2PC
  +--------+
      |
      v
  +--------+
  |   IR   |  Fault-tolerant replication (no ordering)
  +--------+
      |
      v
  +-----------+
  | Transport |  Channels / TCP
  +-----------+
```

**Key insight:** TAPIR (Transactional Application Protocol for Inconsistent Replication) is a transaction protocol built specifically for [IR](ir.md), a replication layer that provides fault tolerance *without* consistency ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) §4). The key insight is a division of labor: IR handles making operations durable and visible across replicas (without ordering them), while TAPIR enforces transaction-level consistency using optimistic concurrency control ([OCC](occ.md)) and client-coordinated two-phase commit. This separation is what makes TAPIR leaderless — since the replication layer doesn't need to order operations, there is no need for a leader to serialize them.

**Transaction lifecycle:** A TAPIR transaction has a simple lifecycle. During the *execution phase*, the client reads keys via IR unlogged operations (served by a single replica from its MVCC store — no replication needed for reads) and buffers writes locally. At commit time, the client enters the *prepare phase*: it sends the transaction's read-set, write-set, scan-set, and a proposed timestamp to all participating shards as an IR consensus operation. Each replica independently runs TAPIR-OCC-CHECK ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) Fig. 9) to detect conflicts with concurrent transactions. The client's `decide` function collects the results: if f+1 replicas return PREPARE-OK (with matching timestamps), the client proceeds to the *commit phase* — sending an IR inconsistent Commit that executes in any order but persists across failures. If any replica returns ABORT, the transaction aborts.

**How correctness is achieved:** TAPIR achieves strict serializability through the combination of OCC validation (which ensures transactions are serializable — no conflicting concurrent mutations) and client-proposed timestamps (which ensure the serialization order respects real time — making it linearizable as well). The original [TAPIR paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) also describes read-only transactions (§6.1), coordinator recovery (§5.2.3), and retry timestamp selection (§6.4) — all implemented in tapirs. For the IR protocol that underpins TAPIR, see [IR](ir.md). For the consistency guarantees TAPIR provides, see [Consistency](consistency.md). For OCC conflict detection, see [OCC](occ.md). For how TAPIR's prepared/committed logs reference IR's record rather than duplicating transaction data, see [Combined store](../internals/combined-store.md). Next: [Consistency](consistency.md). Back to [Concepts](README.md).

| Phase | What happens | IR operation mode | Paper reference |
|-------|-------------|-------------------|-----------------|
| Execute (reads) | Client reads keys from single replica's MVCC store | Unlogged (1 replica, no replication) | §5.2, Fig. 7 |
| Execute (writes) | Client buffers writes locally (not sent to replicas yet) | None (client-side only) | §5.2 |
| Prepare | Client proposes transaction + timestamp to all shards; each replica runs OCC check | Consensus (f+1 quorum; fast: ⌈3f/2⌉+1) | §5.2, Fig. 9-10 |
| Commit | Client sends commit to all participating replicas; replicas apply writes to MVCC store | Inconsistent (f+1, any order) | §5.2 step 7 |
| Abort | Client sends abort (or backup coordinator aborts on crash) | Inconsistent (f+1, any order) | §5.2 step 8 |
