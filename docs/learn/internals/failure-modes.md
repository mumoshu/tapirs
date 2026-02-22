# Failure Modes

```
+-------------------+  +-------------------+  +-------------------+
|   Node Crash      |  | Network Partition |  |   Client Crash    |
|                   |  |                   |  |                   |
| f+1 quorum        |  | Majority: serves  |  | Orphaned prepares |
| continues, zero   |  | Minority: stops   |  | detected by       |
| unavailability    |  | until heal        |  | backup coordinator|
+-------------------+  +-------------------+  +-------------------+

+-------------------+  +-------------------+
|   Disk Failure    |  | Resharding Crash  |
|                   |  |                   |
| Replica returns   |  | ShardManager      |
| error, client     |  | resumes from      |
| retries in quorum |  | checkpoint        |
+-------------------+  +-------------------+
```

**Quorum invariant:** Understanding how tapirs behaves during failures is essential for architects planning around it and operators running it in production. Because tapirs is leaderless, most failure scenarios are handled more gracefully than in leader-based systems -- there is no "who becomes the new leader?" question. The fundamental invariant is: any f+1 quorum out of 2f+1 replicas can make progress. As long as a majority of replicas are reachable, transactions commit. The table below covers each failure scenario, what happens to in-flight transactions, and what the operator (or tapirs itself) does to recover.

**Common failures:** The most common failure -- a single node crash -- is invisible to clients. Because every operation requires only f+1 replicas, the remaining replicas continue serving transactions with no interruption and no failover delay. The crashed node's replicas rejoin via view change when the node restarts, and the view change coordinator (a temporary role, not a permanent leader) merges their stale record with the current state. Client crashes are handled by backup coordinators: replicas detect orphaned prepared transactions and automatically commit (if all shards prepared) or abort them. Network partitions are more nuanced: the majority partition continues serving transactions, while the minority partition's replicas stop accepting proposals (correct IR behavior -- they don't reply during ViewChanging state) until the partition heals.

**Storage and resharding failures:** Disk failures are handled at the storage layer by `FaultyDiskIo`: if a read fails, the replica returns an error and the client retries with another replica in the quorum. If a write fails, the replica logs the error and the operation is retried via the normal IR retry mechanism. During resharding, the most critical edge case is a crash during the prepared-drain phase -- the ShardManager resumes from its last checkpoint on restart. For operational guidance on diagnosing these scenarios, see [Troubleshooting](../../operate/troubleshooting.md). For how view changes restore consistency after failures, see [IR concepts](../concepts/ir.md). For how backup coordinators work, see [Paper Extensions](protocol-tapir-paper-extensions.md). Back to [Internals](README.md).

| Failure | Impact on in-flight txns | Recovery mechanism | Unavailability window |
|---------|------------------------|-------------------|----------------------|
| Single node crash | None -- f+1 quorum continues | View change on rejoin merges stale record | Zero (majority unaffected) |
| Client crash mid-commit | Orphaned prepares | Backup coordinator at replicas commits or aborts | Seconds (backup coordinator timeout) |
| Network partition (majority) | Majority continues; minority stops | View change on partition heal | Zero for majority; until heal for minority |
| Disk read failure | Replica returns error | Client retries with another replica in quorum | Zero (quorum handles it) |
| Disk write failure | Replica logs error, operation retried | IR retry mechanism + view change if persistent | Zero (quorum handles it) |
| Crash during resharding | Resharding paused | ShardManager resumes from checkpoint on restart | Reads continue; writes resume after restart |
