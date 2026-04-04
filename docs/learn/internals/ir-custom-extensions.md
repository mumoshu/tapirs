# IR Custom Extensions

```
IR Custom Extensions (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  PersistentIrRecordStore (VlogLsm-backed storage)      |
|  +----------------------------------------------------+ |
|  | Per-view sealed segments with memtable overlay.    | |
|  | Crash recovery via manifest. CDC delta = sealed    | |
|  | segment diff by OpId.                              | |
|  +----------------------------------------------------+ |
|                                                          |
|  Record Compaction (correctness constraints)            |
|  +----------------------------------------------------+ |
|  | In-place compaction is unsafe (write set loss +    | |
|  | ambiguous transaction fate). Only shard replacement | |
|  | via CDC is safe.                                   | |
|  +----------------------------------------------------+ |
|                                                          |
+----------------------------------------------------------+
```

**What these extensions are:** The original IR paper ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) S3) specifies the record as an unordered set of operations with no guidance on representation or lifecycle management. These extensions address two practical concerns: how to represent and persist the record efficiently, and why its entries cannot be selectively removed.

| Extension | What it does | Deep-dive |
|-----------|-------------|-----------|
| PersistentIrRecordStore | VlogLsm-backed IR record with per-view sealing, crash recovery, and efficient CDC delta extraction | [PersistentIrRecordStore](ir-custom-extensions-versioned-record.md) |
| Record compaction | Documents why in-place compaction of CO::Prepare is unsafe and why shard replacement is the only safe mechanism | [Record Compaction](ir-custom-extensions-compaction.md) |

**Related docs:** See [IR concepts](../concepts/ir.md) for record terminology, [Protocol](protocol-tapir.md) for the base view change flow, [Resharding](resharding.md) for CDC-based shard replacement. Back to [Architecture](README.md).
