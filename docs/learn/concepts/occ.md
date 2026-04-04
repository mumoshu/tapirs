# Optimistic Concurrency Control

```
  Begin Txn       Read/Write        Prepare         OCC Check
      |               |                |                |
      v               v                v                v
  +-------+     +-----------+    +-----------+    +-----------+
  | alloc |---->| execute   |--->| submit    |--->| validate  |
  | tx_id |     | (no locks)|    | read-set  |    | read-set  |
  +-------+     +-----------+    | write-set |    | write-set |
                                 | scan-set  |    | scan-set  |
                                 +-----------+    +-----+-----+
                                                        |
                                              +---------+---------+
                                              |                   |
                                              v                   v
                                         +--------+         +---------+
                                         | Commit |         | Abort/  |
                                         +--------+         | Retry   |
                                                             +---------+
```

**What OCC does:** Optimistic Concurrency Control (OCC) is how tapirs detects transaction conflicts without locks. A transaction executes optimistically — reads and writes proceed without coordination — and then at prepare time the transaction submits its read-set, write-set, and scan-set for conflict detection. If no conflicts are found with concurrent transactions, the prepare succeeds (`PREPARE-OK`). Otherwise, the OCC check returns one of several conflict resolution signals: `Retry` with a proposed timestamp if the conflict is resolvable, `Abstain` if a concurrent prepare might resolve on its own, or `Fail` if the conflict is irreconcilable.

**In-flight validation:** What makes tapirs' OCC different from textbook implementations is that it validates against *in-flight* transactions (those still in the prepare phase), not just committed ones. The OCC store maintains indexes of prepared reads, prepared writes, and range reads so that overlapping transactions are detected as early as possible. This is critical for TAPIR's leaderless model: since there is no leader to serialize prepares, multiple transactions may prepare concurrently at the same replica, and the OCC store must correctly detect all pairwise conflicts. Phantom-write detection extends this to range scans — a write into a previously-scanned key range is caught at prepare time, even if the specific key didn't exist when the scan was issued.

**Deep-dives:** For the full conflict detection rules and how they map to IR consensus results, see the [Protocol deep-dive](../internals/protocol-tapir.md). For the custom range scan extension, see [Custom Extensions](../internals/protocol-tapir-custom-extensions.md). Next: [Storage](storage.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| Prepared | Transaction passed OCC check, awaiting commit/abort. Blocks conflicting transactions via prepared_reads/writes indexes | `occ/mod.rs` — `prepared` BTreeMap |
| Committed | Transaction executed and durable in transaction_log. Protected by `max_read_commit_time` | `tapir/replica.rs` |
| PrepareResult | Ok, Retry(timestamp), Abstain, Fail, TooLate, TooOld, OutOfRange — each indicates a different conflict resolution path | `occ/mod.rs` — `PrepareResult` enum |
| SharedTransaction | `Arc<Transaction>` — refcount bumps instead of deep clones during consensus broadcast, view change merge, backup coordinator recovery | `occ/transaction.rs` |
| Phantom Write | A write into a previously-scanned range — detected by scan-set conflict check at prepare time | `occ/mod.rs` — scan-set validation |
| Read-Set Conflict | A later committed write invalidates a read → Fail. A later prepared write → Abstain (may resolve) | `occ/mod.rs` — read-set check |
| Write-Set Conflict | A later committed read or write blocks this write → Retry with proposed timestamp | `occ/mod.rs` — write-set check |
