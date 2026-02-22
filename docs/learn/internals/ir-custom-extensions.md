# IR Custom Extensions

```
IR Custom Extensions (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  VersionedRecord (replica-internal optimization)        |
|  +----------------------------------------------------+ |
|  | Base/overlay structure eliminates full record clone | |
|  | on mutation and makes delta extraction O(overlay)  | |
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

**What these extensions are:** The original IR paper ([Paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) §3) specifies the record as an unordered set of operations with no guidance on representation or lifecycle management. These extensions address two practical concerns: how to represent the record efficiently in memory, and why its entries cannot be selectively removed.

| Extension | What it does | Deep-dive |
|-----------|-------------|-----------|
| VersionedRecord | Base/overlay structure that eliminates full record clone on mutation and makes delta extraction O(overlay) | [VersionedRecord](ir-custom-extensions-versioned-record.md) |
| Record compaction | Documents why in-place compaction of CO::Prepare is unsafe and why shard replacement is the only safe mechanism | [Record Compaction](ir-custom-extensions-compaction.md) |

**Related docs:** See [IR concepts](../concepts/ir.md) for record terminology, [Protocol](protocol-tapir.md) for the base view change flow, [Resharding](resharding.md) for CDC-based shard replacement. Back to [Architecture](README.md).
