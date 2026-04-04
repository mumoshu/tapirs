# PersistentIrRecordStore

```
PersistentIrRecordStore (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  VlogLsm-backed IR record storage                       |
|  +----------------------------------------------------+ |
|  | inc_lsm: VlogLsm<OpId, InconsistentEntry>          | |
|  | con_lsm: VlogLsm<OpId, ConsensusEntry>             | |
|  | base_view: u64   (view when last sealed)            | |
|  | manifest: UnifiedManifest (crash recovery)          | |
|  +----------------------------------------------------+ |
|                                                          |
|  Read:  memtable first, then sealed SST segments        |
|  Write: always to memtable (current view)               |
|  Delta: entries in memtable = changes since last seal   |
|  Seal:  flush memtable to vlog + SST on view change     |
|  Recovery: reload sealed segments from manifest          |
|                                                          |
+----------------------------------------------------------+
```

**Design:** `PersistentIrRecordStore` stores the IR consensus record using two VlogLsm instances: one for inconsistent entries (`inc_lsm`) and one for consensus entries (`con_lsm`). Each VlogLsm uses a memtable (in-memory BTreeMap) for the current view's entries, with sealed segments (vlog + SST files) for previous views.

**Per-view sealing:** On each view change, the memtable is flushed to a new sealed segment. Each sealed segment naturally represents the delta for that view. This enables efficient CDC delta extraction: the delta for a view transition is exactly the entries that were sealed at that boundary.

**CombinedStore integration:** In `CombinedStore`, `PersistentIrRecordStore` is embedded inside `CombinedStoreInner` and shared (via `Arc<Mutex<>>`) between `CombinedRecordHandle` (IR interface) and `CombinedTapirHandle` (TAPIR interface). TAPIR's committed/prepared VlogLsms store lightweight references (`TxnLogRef`, `PreparedRef`) that resolve lazily from the IR VlogLsms, deduplicating transaction storage.

**Recovery:** On reopen, sealed segments are loaded from the manifest. The memtable starts empty. IR's view-change protocol recovers any data lost between the last seal and the crash.

**Complexity:**

| Operation | Behavior |
|-----------|----------|
| Normal op (Propose/Finalize) | `memtable.put(op_id, entry)` O(log n) |
| DoViewChange payload | `encode_memtable_as_segment()` + `all_segment_bytes()` |
| Coordinator merge install | Clear VlogLsms, import merged record, advance base_view |
| StartView install (delta) | Import delta bytes alongside existing sealed segments |
| StartView install (full) | Clear all, import full payload |
| View change seal | Flush memtable to vlog + SST, start new view |
| CDC delta | OpId-based diff against sealed segments (previous view checkpoint) |

**Related docs:** Back to [IR Custom Extensions](ir-custom-extensions.md). See [Protocol](protocol-tapir.md) for view change flow, [IR concepts](../concepts/ir.md) for record terminology. Key files: `src/unified/ir/ir_record_store.rs` (PersistentIrRecordStore), `src/unified/combined/` (CombinedStore integration).
