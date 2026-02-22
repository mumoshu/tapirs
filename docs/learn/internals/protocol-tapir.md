# TAPIR Transaction Protocol

```
Client: Begin --> Read(key) --> Write(key,val) --> Commit
                    |                                |
              IR Unlogged                    IR Consensus: Prepare
              (to 1 replica)               (to all replicas in shard)
                                                     |
                                           OCC Check at each replica
                                           -> PREPARE-OK / RETRY / ABSTAIN / ABORT
                                                     |
                                           decide(): f+1 PREPARE-OK -> commit
                                                     |
                                           IR Inconsistent: Commit
                                           (async to all replicas)
```

**Why TAPIR is different:** TAPIR is the first transaction protocol designed for inconsistent replication ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S4). It provides strict serializable (linearizable) transactions without a leader -- clients coordinate 2PC directly. The key insight: by eliminating consistency from the replication layer (IR) and enforcing it only in the transaction protocol (OCC), TAPIR commits transactions in a single round-trip with no centralized coordination. Clients propose timestamps (using loosely synchronized clocks); replicas validate via OCC and agree via IR consensus. If all shards return PREPARE-OK, the client commits asynchronously. This gives TAPIR 50% lower latency and 3x better throughput than Paxos-based systems ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S7).

**IR operation mapping:** The read-write transaction protocol maps to IR operations as follows ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S5.2). During execution, reads use IR *unlogged* operations -- a single replica serves from its MVCC store, no replication needed. At commit time, Prepare uses IR *consensus* -- replicas run TAPIR-OCC-CHECK ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) Fig. 9) and return PREPARE-OK, RETRY(timestamp), ABSTAIN, or ABORT. The client's `decide` function ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) Fig. 10) requires f+1 matching PREPARE-OK to commit; if any replica returns ABORT, the transaction aborts. Commit and Abort use IR *inconsistent* operations -- they execute in any order but persist across failures. Read-only transactions skip the prepare phase entirely -- they use validated reads (fast path) or QuorumRead/QuorumScan (fallback); see [Paper Extensions](protocol-tapir-paper-extensions.md).

**Coordinator recovery:** Coordinator recovery ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S5.2.3): if a client crashes mid-commit, a backup shard's replicas recover the transaction using Bernstein's cooperative termination protocol adapted for IR. The backup coordinator polls participants with a modified `decide` function ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) Fig. 13) that requires f+1 NO-VOTE to abort (not just any single NO-VOTE), because the original coordinator may have already committed. See [Paper Extensions](protocol-tapir-paper-extensions.md) for RO transactions and coordinator recovery, and [Custom Extensions](protocol-tapir-custom-extensions.md) for range scan and other additions beyond the paper. Key files: `src/tapir/client.rs`, `src/tapir/replica.rs`, `src/ir/client.rs`, `src/ir/replica.rs`.

| TAPIR Operation | IR Mode | Quorum | OCC Action | Paper Reference |
|-----------------|---------|--------|------------|-----------------|
| Read(key) | Unlogged | 1 replica | Serve from MVCC store at read timestamp | S5.2, Fig. 7 |
| Prepare(txn, ts) | Consensus | f+1 (fast: 3f/2+1) | TAPIR-OCC-CHECK: validate read-set, write-set | S5.2, Fig. 9 |
| Commit(txn, ts) | Inconsistent | f+1 | Log commit, update MVCC store, remove from prepared | S5.2 step 7 |
| Abort(txn, ts) | Inconsistent | f+1 | Log abort, remove from prepared | S5.2 step 8 |
| Recovery Prepare | Consensus | f+1 | Modified decide: f+1 NO-VOTE to abort (Fig. 13) | S5.2.3 |
