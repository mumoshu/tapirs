# VersionedRecord

```
VersionedRecord (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  Before: three separate fields                          |
|  +----------------------------------------------------+ |
|  | record: Arc<Record>        (full state)            | |
|  | leader_record: Option<...> (base snapshot clone)   | |
|  | delta_op_ids: HashSet      (changed OpId tracking) | |
|  +----------------------------------------------------+ |
|                                                          |
|  After: single VersionedRecord                          |
|  +----------------------------------------------------+ |
|  | base: RecordImpl   (sealed at last view change)    | |
|  | base_view: u64     (view when base was sealed)     | |
|  | overlay: RecordImpl (changes during current view)  | |
|  +----------------------------------------------------+ |
|                                                          |
|  Read:  check overlay first, fall back to base          |
|  Write: always to overlay (promote from base on mutate) |
|  Delta: overlay IS the delta (O(1) access)              |
|  Seal:  merge overlay into base on view change          |
|                                                          |
+----------------------------------------------------------+
```

**The problem:** The IR replica previously maintained three overlapping data structures to track the consensus record: (1) `record: Arc<Record>` -- the full state of all operations ever seen, (2) `leader_record: Option<LeaderRecord>` -- a full clone of the record snapshot at the last view-change boundary, used as the resolution base for StartView deltas, and (3) `delta_op_ids: HashSet<OpId>` -- tracking which OpIds changed since the last view change, used to extract delta payloads for DoViewChange. These three had to be manually kept in sync: every mutation updated both `record` and `delta_op_ids`, and every view change cloned the full record into `leader_record`.

**Memory cost of the old design:** After the first mutation post-view-change, `Arc::make_mut(&mut record)` triggered a full record clone because `leader_record.record` held an `Arc` reference to the same data. Both copies held all entries -- O(record) memory duplication at every view boundary. For a shard with 10K entries, each view change allocated a second 10K-entry BTreeMap.

**VersionedRecord design:** A single `VersionedRecord` replaces all three fields. It holds an immutable `base` (the snapshot from the last view change) and a mutable `overlay` (changes during the current view):

```rust
struct VersionedRecord<IO, CO, CR> {
    base: RecordImpl<IO, CO, CR>,     // Immutable until next seal
    base_view: u64,                    // View number when base was sealed
    overlay: RecordImpl<IO, CO, CR>,  // Changes during current view
}
```

The API maps directly to protocol operations:

- **Propose** (insert new op): `entry_inconsistent(op_id)` / `entry_consensus(op_id)` -- returns Vacant (insert into overlay) or Occupied (already exists in overlay or base).
- **Finalize** (mutate existing op): `get_mut_inconsistent(op_id)` / `get_mut_consensus(op_id)` -- if the entry is in base, promotes a clone to overlay first, then returns `&mut` to the overlay copy. Base is never modified.
- **DoViewChange** (extract delta): `overlay_clone()` -- the overlay IS the delta. O(overlay) instead of O(record) filtering.
- **StartView** (resolve delta): `base()` returns a reference to the resolution base. No separate `leader_record` field needed.
- **Coordinator merge** (install new record): `from_full(merged_record, new_view)` -- the merged record becomes the new base with an empty overlay. No clone of the old record.
- **Seal** (view boundary): `seal(new_view)` -- merges overlay into base, clears overlay. O(overlay) instead of O(record).

**modified_view annotation:** Each record entry carries a `modified_view: u64` field stamped with the current view number at insert or mutation time. This enables `entries_since(since_view)` for CDC delta extraction without comparing against a separate snapshot. The field is structural metadata only -- it does not affect protocol correctness.

**What VersionedRecord does NOT do:** VersionedRecord is a replica-internal optimization. It does not change the wire format (`RecordImpl` remains the serialized type for DoViewChange and StartView payloads), does not change protocol semantics (all operations produce the same results), and does not enable record compaction (see [Record Compaction](ir-custom-extensions-compaction.md) for why in-place compaction is unsafe).

**Complexity comparison:**

| Operation | Before (triple storage) | After (VersionedRecord) |
|-----------|------------------------|------------------------|
| Normal op (Propose/Finalize) | `Arc::make_mut` O(record) on first mutation + `delta_op_ids.insert` | `overlay.insert` O(log n) |
| DoViewChange delta | `record.filter_by_op_ids(&delta_op_ids)` O(delta x log(record)) | `overlay_clone()` O(overlay) |
| Coordinator merge install | `record = merged; leader_record = clone(merged)` O(record) | `from_full(merged, view)` O(1) move |
| StartView resolve | `payload.resolve(Some(&leader_record.record))` | `payload.resolve(Some(record.base()))` |
| View change seal | `leader_record = clone(record); delta_op_ids.clear()` O(record) | `seal(new_view)` O(overlay) |
| CDC delta | `record.delta_from(&leader_record)` O(record) | `record.delta_from(&record.base())` O(record) |

**Related docs:** Back to [IR Custom Extensions](ir-custom-extensions.md). See [Protocol](protocol-tapir.md) for view change flow, [IR concepts](../concepts/ir.md) for record terminology. Key files: `src/ir/record.rs` (VersionedRecord type), `src/ir/replica.rs` (usage in SyncInner).
