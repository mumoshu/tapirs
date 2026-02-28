# Unified TAPIR/IR Storage Layer

> This is a working progress documentation that introduces on-going research
> about tapirs's replica storage architecture.

## Context

The current tapirs architecture stores IR record entries and TAPIR committed values in separate, independent storage paths.

The IR record (`VersionedRecord`) is purely in-memory (recovered via view change), while TAPIR's MVCC store (`DiskStore`) uses a WiscKey-style LSM tree with its own memtable, SSTs, and value log. This causes data duplication:

1. `CO::Prepare` carries the full `SharedTransaction` (read/write sets) in the IR record
2. `IO::Commit` carries the same `SharedTransaction` AGAIN in the IR record
3. On commit, each key-value pair is written INDIVIDUALLY to the DiskStore vlog (third copy of values)
4. The OCC prepared list stores the `SharedTransaction` via Arc (free in-memory, but checkpoint serialization duplicates)

**Goal**: Create a unified storage layer where IR record entries and MVCC committed values share a single physical value log. IR "views" map to LSM tree levels. TAPIR prepared list entries hold pointers into IR record vlog entries instead of duplicating transaction data. MVCC index entries store sub-pointers into `CO::Prepare` vlog entries to read committed values without duplication.

**Constraint**: TAPIR and IR core code remains unchanged. The unified storage exposes `MvccBackend` for TAPIR and is transparent to the `VersionedRecord` used by IR.

---

## Storage Requirements

### IR Replica Requirements

**API Requirements**:
- `entry_inconsistent(op_id) -> Option<InconsistentEntry>` — lookup by OpId (overlay first, then base)
- `entry_consensus(op_id) -> Option<ConsensusEntry>` — same pattern
- `get_mut_inconsistent(op_id) -> &mut InconsistentEntry` — mutable access, promotes from base to overlay
- `get_mut_consensus(op_id) -> &mut ConsensusEntry` — same
- `overlay_clone() -> Record` — extract current view's delta for DoViewChange
- `full() -> Record` — merged base ∪ overlay for merge/sync
- `seal(new_view)` — merge overlay into base, clear overlay
- `from_full(record, view)` — install merged record as new base
- Iterate all inconsistent/consensus entries (for sync, merge, CDC)

**Functional Requirements**:
- Each IR record entry has `state: Tentative | Finalized(ViewNumber)` and `modified_view: u64` (the unified IrMemEntry omits `modified_view` — always equals current view)
- State transitions: Tentative → Finalized (via finalize_consensus, or during merge/sync)
- Base is immutable snapshot from last view change; overlay accumulates current view mutations
- `RecordPayload::Delta` or `RecordPayload::Full` for StartView messages
- Wire protocol (DoViewChange, StartView) MUST carry full operation data — PrepareRef is local to a single replica's VLog and cannot cross the wire

**Non-functional Requirements**:
- O(log n) lookup by OpId
- O(overlay_size) for overlay_clone and seal
- In-memory base + overlay for fast access during normal operation
- On-disk IR SST mandatory for larger-than-memory records; overlay recovered via view change
- Deterministic iteration order (BTreeMap)

### TAPIR Replica Requirements

**API Requirements (MvccBackend trait)**:
- `get(key) -> (Option<V>, TS)` — latest committed value
- `get_at(key, ts) -> (Option<V>, TS)` — value at or before timestamp
- `put(key, value, ts)` — write value at timestamp (internally uses transaction path: creates a single-write CO::Prepare + IO::Commit)
- `commit_get(key, read_ts, commit_ts)` — record read protection (last_read_ts)
- `get_last_read(key) -> Option<TS>` — OCC read timestamp
- `scan(start, end, ts) -> Vec<(K, Option<V>, TS)>` — range scan at timestamp
- `has_writes_in_range(start, end, after_ts, before_ts) -> bool` — phantom detection
- `commit_batch(writes, reads, commit_ts)` — atomic batch commit

**API Requirements (OCC prepared list)**:
- `prepare(id, transaction, commit_ts, finalized)` — add to prepared list
- `add_prepared(id, transaction, commit_ts, finalized)` — add/update prepared entry
- `remove_prepared(id)` — remove from prepared list
- `commit(id, transaction, commit_ts)` — commit to MVCC, remove from prepared

**Functional Requirements**:
- OCC conflict detection using `prepared_reads`, `prepared_writes` caches
- `last_read_ts` tracking per key for read protection (survives view changes)
- Transaction log: `BTreeMap<OccTransactionId, (Timestamp, bool)>` (commit/abort status)
- `commit_batch` writes values and updates read timestamps atomically
- CDC: `record_delta_during_view` records committed changes per view

**Non-functional Requirements**:
- Commit path should minimize I/O (zero value-write I/O when Prepare is already in local VLog)
- Read path: O(log n) memtable lookup + O(levels × log n) SST lookups
- Crash-safe: committed values must survive process restart
- LRU cache for cross-view Prepare payload access (rare case: prepared in view N, committed in view N+M)

---

## Architecture Diagrams

### Overall System Architecture

```
┌───────────────────────────────────────────────────────────┐
│                      IR Replica                           │
│  VersionedRecord<IO, CO, CR>  (in-memory, UNCHANGED)     │
│  ┌─────────────┐ ┌──────────────┐                        │
│  │    Base      │ │   Overlay    │                        │
│  │ BTreeMap<    │ │ BTreeMap<    │                        │
│  │  OpId,Entry >│ │  OpId,Entry >│                        │
│  └──────┬──────┘ └──────┬───────┘                        │
│         │  sync/merge   │ exec_consensus/exec_inconsistent│
└─────────┼───────────────┼────────────────────────────────┘
          │               │
┌─────────┼───────────────┼────────────────────────────────┐
│         ▼               ▼                                │
│  ┌────────────────────────────────────────┐               │
│  │         TAPIR Replica (UNCHANGED)      │               │
│  │  OccStore.prepared: BTreeMap<          │               │
│  │    TxnId, (TS, SharedTransaction, F)>  │               │
│  │  transaction_log: BTreeMap<TxnId, ..>  │               │
│  └──────────────┬─────────────────────────┘               │
│                 │ MvccBackend trait                        │
│  ┌──────────────┴─────────────────────────┐               │
│  │      UnifiedMvccBackend (NEW)          │               │
│  │                                        │               │
│  │  ┌────────────┐  ┌─────────────────┐   │               │
│  │  │ MVCC Index │  │  Prepare LRU    │   │               │
│  │  │ Memtable + │  │  Cache          │   │               │
│  │  │ View SSTs  │  │ (deserialized   │   │               │
│  │  │            │  │  CO::Prepare)   │   │               │
│  │  └─────┬──────┘  └───────┬─────────┘   │               │
│  │        │                 │              │               │
│  │  ┌─────┴─────────────────┴──────────┐   │               │
│  │  │    Shared VLog (per-view files)  │   │               │
│  │  │  CO::Prepare | IO::Commit | ...  │   │               │
│  │  └──────────────────────────────────┘   │               │
│  └─────────────────────────────────────────┘               │
└───────────────────────────────────────────────────────────┘
```

### LSM Tree: Views as Levels

```
              IR Index                          MVCC Index
       (2-level: base+overlay)          (multi-level: views as levels)
    ┌─────────────────────┐          ┌──────────────────────────┐
    │  IR Overlay         │          │  MVCC Memtable           │
    │  (current view N)   │          │  (current view N)        │
    │  BTreeMap<OpId,     │          │  BTreeMap<CompKey<bytes>, │
    │    IrMemEntry>      │          │    UnifiedLsmEntry>      │
    │  [in-memory,mutable]│          │  [in-memory, mutable]    │
    ├─────────────────────┤          ├──────────────────────────┤
    │  IR Base SST        │          │  MVCC SST (view N-1)     │
    │  (merged record)    │          │  (bytes,Rev<TS>)→LsmEntry│
    │  OpId→IrSstEntry    │          │  [on disk + opt. cache]  │
    │  [ON DISK + bloom   │          ├──────────────────────────┤
    │   + block cache]    │          │  MVCC SST (view N-2)     │
    │                     │          │  [on disk + opt. cache]  │
    │  Enables IR records │          ├──────────────────────────┤
    │  larger than memory │          │  ...                     │
    │  (replaced entirely │          ├──────────────────────────┤
    │   on each view      │          │  MVCC SST (view 0)      │
    │   change merge)     │          │  [on disk + opt. cache]  │
    └─────────────────────┘          └──────────────────────────┘
     Both indices share the same per-view VLog files:

     View N:   (in memory only — written to vlog_seg_XX.dat at seal time)
     View N-1: vlog_seg_XX.dat   (sealed, immutable)
     View N-2: vlog_seg_XX.dat   (sealed, may share segment)
     ...
```

**Key difference**: IR index has exactly 2 levels (base replaced on each merge). MVCC index has N levels (one per sealed view), compacted periodically to reduce read amplification.

**IR base on disk (mandatory)**: The IR base is stored as an IR SST on disk, NOT held entirely in memory. This enables IR records larger than available memory — critical for high-throughput systems (e.g., 100K txns/sec with infrequent view changes produces millions of entries per view). The overlay (current view) remains in-memory. Lookups check overlay first, then IR base SST (with bloom filter + block cache). `from_full()` writes the merged record to a new IR SST. `overlay_clone()` returns in-memory overlay entries only.

### VLog Entry Format (WiscKey Value Separation)

**Per-view VLog file** (append-only during view, sealed on view change):

```
Sealed VLog file format:
┌──────────────────────────────────────────────────┐
│ num_entries: u32                                 │
├──────────────────────────────────────────────────┤
│ Entry 0                                         │
│ ┌──────────────────────────────────────────────┐ │
│ │ entry_type: u8                               │ │
│ │   0x01 = CO::Prepare                         │ │
│ │   0x02 = IO::Commit                          │ │
│ │   0x03 = IO::Abort                           │ │
│ │   0x04 = IO::QuorumRead                      │ │
│ │   0x05 = IO::QuorumScan                      │ │
│ │   0x06 = CO::RaiseMinPrepareTime             │ │
│ │ entry_len: u32 (total bytes incl. header)     │ │
│ │ op_id: (client_id: u64, number: u64)         │ │
│ │ payload: [u8] (bitcode-serialized, varies)   │ │
│ │ crc32: u32                                   │ │
│ └──────────────────────────────────────────────┘ │
│ Entry 1 ...                                      │
│ Entry N-1 ...                                    │
├──────────────────────────────────────────────────┤
│ footer_checksum: u32                             │
│   (CRC of num_entries XOR all entry CRCs)        │
└──────────────────────────────────────────────────┘
```

**CO::Prepare payload** (entry_type = 0x01):

```
┌──────────────────────────────────────────────────┐
│ transaction_id: (client_id: u64, number: u64)    │
│ commit_ts: (time: u64, client_id: u64)           │
│ read_set_count: u16                              │
│ write_set_count: u16                             │
│ scan_set_count: u16                              │
│ read_entries:                                    │
│   [(key_len: u32, key: [u8],                     │
│     read_ts: (u64, u64))]                        │
│ write_entries:                                   │
│   [(key_len: u32, key: [u8],                     │
│     val_len: u32, val: [u8])]                    │
│     ↑ keys & values are opaque bytes             │
│     ↑ MVCC sub-pointers reference by write_index │
│     ↑ no shard — data already shard-filtered     │
│ scan_entries:                                    │
│   [(start_len: u32, start: [u8],                 │
│     end_len: u32, end: [u8], ts: (u64, u64))]    │
└──────────────────────────────────────────────────┘

  NOTE: Key bytes and value bytes are opaque — no encoding
  assumed. The storage layer treats them as raw byte vectors.
  The MvccBackend adapter (UnifiedMvccBackend) handles
  typed K/V ↔ bytes conversion via KeyValueCodec.
```

**IO::Commit payload** (entry_type = 0x02, deduplicated):

```
┌──────────────────────────────────────────────────┐
│ transaction_id: (client_id: u64, number: u64)    │
│ commit_ts: (time: u64, client_id: u64)           │
│ prepare_ref_type: u8                             │
│   0x01 = SameView (OpId in this view's memtable) │
│   0x02 = CrossView (sealed view VLog pointer)    │
│ prepare_ref_data:                                │
│   SameView:  op_id (16 bytes)                    │
│   CrossView: view(u64), offset(u64), len(u32)    │
└──────────────────────────────────────────────────┘
  NOTE: Full SharedTransaction is NOT stored here.
  Resolved by following prepare_ref to CO::Prepare.
  Wire protocol (DoViewChange/StartView) still
  carries full transaction data inline.
```

**IO::Abort payload** (entry_type = 0x03):

```
┌──────────────────────────────────────────────────┐
│ transaction_id: (client_id: u64, number: u64)    │
│ has_commit_ts: u8 (0=None, 1=Some)               │
│ commit_ts: (time: u64, client_id: u64) if Some   │
└──────────────────────────────────────────────────┘
```

**IO::QuorumRead payload** (entry_type = 0x04):

```
┌──────────────────────────────────────────────────┐
│ key: bitcode                                     │
│ timestamp: (time: u64, client_id: u64)           │
└──────────────────────────────────────────────────┘
```

**IO::QuorumScan payload** (entry_type = 0x05):

```
┌──────────────────────────────────────────────────┐
│ start_key: bitcode                               │
│ end_key: bitcode                                 │
│ snapshot_ts: (time: u64, client_id: u64)         │
└──────────────────────────────────────────────────┘
```

**CO::RaiseMinPrepareTime payload** (entry_type = 0x06):

```
┌──────────────────────────────────────────────────┐
│ time: u64                                        │
└──────────────────────────────────────────────────┘
  NOTE: No result_time — the result is deterministic:
  max(current_min_prepare_time, time). Since VLog only
  stores Finalized entries (f+1 agreed), replaying in
  order reproduces the correct result.
```

### RW Transaction Data Flow

```
Client                  IR Replica              UnifiedStore
  │                        │                        │
  │ invoke_consensus       │                        │
  │ (CO::Prepare{txn_id,   │                        │
  │  transaction, ts})     │                        │
  ├───────────────────────►│                        │
  │                        │ insert into overlay    │
  │                        │ (in-memory BTreeMap)   │
  │                        │                        │
  │                        │ exec_consensus ────────┤
  │                        │ (TAPIR Replica)        │
  │                        │                        │
  │                        │ OCC validate ──────────┤
  │                        │                        │
  │                        │  ┌──────────────────┐  │
  │                        │  │ register_prepare │  │
  │                        │  │ stores txn data  │  │
  │                        │  │ IN MEMORY ONLY   │  │
  │                        │  │ (no VLog write)  │  │
  │                        │  └──────────────────┘  │
  │                        │                        │
  │                        │ add_prepared(txn_id,   │
  │                        │  Arc<Txn>, ts, false)  │
  │                        │                        │
  │◄───CR::Prepare(Ok)─────│                        │
  │                        │                        │
  │ invoke_inconsistent    │                        │
  │ (IO::Commit{txn_id,    │                        │
  │  transaction, ts})     │                        │
  ├───────────────────────►│                        │
  │                        │ insert into overlay    │
  │                        │                        │
  │                        │ exec_inconsistent ─────┤
  │                        │                        │
  │                        │  ┌──────────────────┐  │
  │                        │  │ commit_batch_for │  │
  │                        │  │ _transaction():  │  │
  │                        │  │                  │  │
  │                        │  │ MVCC memtable    │  │
  │                        │  │ insert per key:  │  │
  │                        │  │ (key,ts) →       │  │
  │                        │  │ InMemory {       │  │
  │                        │  │   txn_id,        │  │
  │                        │  │   write_index    │  │
  │                        │  │ }                │  │
  │                        │  │                  │  │
  │                        │  │ NO VLog WRITE!   │  │
  │                        │  │ Values live in   │  │
  │                        │  │ overlay's IrMem  │  │
  │                        │  │ Entry (in-memory)│  │
  │                        │  │ Resolved on read │  │
  │                        │  │ via prepare_     │  │
  │                        │  │ registry lookup  │  │
  │                        │  │                  │  │
  │                        │  │ VLog written at  │  │
  │                        │  │ VIEW SEAL only   │  │
  │                        │  └──────────────────┘  │
  │                        │                        │
  │                        │ remove_prepared(txn_id)│
  │                        │ update transaction_log │
  │                        │                        │
```

**Read path** (MVCC get_at):

```
  MVCC get_at(key, ts)
        │
        ▼
  ┌─────────────────────┐
  │ 1. Check MVCC       │    Found: ValueLocation
  │    Memtable          │──────────────────────────┐
  │ (current view)       │                          │
  └──────────┬──────────┘                          │
     miss    │                                      │
        ▼                                           │
  ┌─────────────────────┐                          │
  │ 2. Check MVCC SSTs  │    Found: ValueLocation   │
  │ (view N-1, N-2, ..) │──────────────────────────┤
  │ newest first         │                          │
  └──────────┬──────────┘                          │
     miss    │                                      ▼
        ▼                              ┌──────────────────────┐
  return (None, TS::default())         │ 3. Dispatch on       │
                                       │    ValueLocation      │
                                       └───┬──────────────┬───┘
                                           │              │
                                   InMemory│       OnDisk │
                                           ▼              ▼
                              ┌──────────────┐  ┌──────────────────┐
                              │ prepare_     │  │ 4. Check LRU     │
                              │ registry     │  │    Prepare Cache  │
                              │ [txn_id]     │  │ key=(seg_id,off) │
                              │ (zero I/O)   │  └───────┬──────────┘
                              └──────┬───────┘     hit  │  miss
                                     │  ┌───────────────┘  │
                                     │  │                   ▼
                                     │  │          ┌────────────────┐
                                     │  │          │ 5. pread VLog  │
                                     │  │          │ CO::Prepare    │
                                     │  │          │ deserialize    │
                                     │  │          │ add to LRU     │
                                     │  │          └───────┬────────┘
                                     │  │                  │
                                     ▼  ▼                  ▼
                             ┌─────────────────────────────────┐
                             │ 6. Extract write_set[index]     │
                             │    Return (value, commit_ts)    │
                             └─────────────────────────────────┘
```

### View Change: LSM Seal + Merge

```
View N (current)                      View N+1 (new)
─────────────────                     ──────────────────

1. SEAL CURRENT VIEW:
   ┌─────────────────┐
   │ IR Overlay       │──overlay_clone()──►DoViewChange
   │ (BTreeMap)       │                    to leader
   └─────────────────┘
   ┌─────────────────┐                ┌──────────────────┐
   │ MVCC Memtable   │──flush────────►│ MVCC SST (view N)│
   │ (in-memory)     │                │ (on disk)        │
   └─────────────────┘                └──────────────────┘
   ┌─────────────────┐
   │ VLog (active)   │──fsync+seal───► vlog_N sealed
   └─────────────────┘

2. LEADER MERGES (collects f+1 DoViewChange):
   ┌──────────────────────────────────────────────────┐
   │ merge(d, u):                                     │
   │   d = CO entries with f+1 same result             │
   │   u = undecided CO entries                       │
   │   → re-execute consensus, produce merged record  │
   │                                                  │
   │ All entries marked Finalized(new_view)           │
   └──────────────────────────────────────────────────┘

3. LEADER SENDS StartView:
   ┌──────────────────────────────────────────────────┐
   │ RecordPayload::Delta { base_view, entries }      │
   │   (if follower was up-to-date)                   │
   │ RecordPayload::Full(merged_record)               │
   │   (if follower was stale)                        │
   │                                                  │
   │ NOTE: Wire payload carries FULL transaction data │
   │ (SharedTransaction inline, NOT PrepareRef)       │
   └──────────────────────────────────────────────────┘

4. FOLLOWER APPLIES StartView:
   ┌─────────────────┐
   │ sync(old, new)  │  TAPIR replay:
   │ - CO::Prepare → │  add_prepared or remove_prepared
   │ - IO::Commit  → │  commit to MVCC
   │ - IO::Abort   → │  remove from prepared
   └─────────────────┘
   ┌─────────────────┐
   │ from_full(R, v) │  IR: install merged record as new base
   │                 │  overlay = empty
   └─────────────────┘
                                      ┌──────────────────┐
                                      │ New empty         │
                                      │ IR overlay        │
                                      │ MVCC memtable     │
                                      │ VLog (new active) │
                                      └──────────────────┘

   NOTE on sync()-time commits:
   sync() processes consensus entries (CO::Prepare) BEFORE
   inconsistent entries (IO::Commit). During Prepare processing,
   register_prepare() stores the CO::Prepare in memory.
   By the time IO::Commit runs via commit_batch_for_transaction(),
   the CO::Prepare data is ALWAYS available (in memory from
   register_prepare). commit_batch_for_transaction() creates MVCC
   entries with ValueLocation::InMemory. VLog writes are deferred
   to view seal time. This means:
   - ALL commits use ValueLocation::InMemory during the view
   - At seal time, InMemory entries are converted to OnDisk
     (UnifiedVlogPrepareValuePtr) as VLog is written
   - The leader's merged record always contains both CO::Prepare and
     IO::Commit for committed transactions (CO::Prepare is the sole
     carrier of the write set and cannot be compacted)
   - ALL values use UnifiedVlogPrepareValuePtr
```

### PrepareRef Pointer Resolution

```
                  OCC Prepared List
                  ┌──────────────────────────────┐
                  │ txn_id → (ts, Arc<Txn>, fin) │
                  │                              │
                  │ In-memory: Arc<Transaction>  │
                  │ is shared with IR overlay    │
                  │ (same Arc, zero-copy)        │
                  └──────────────────────────────┘

    Case 1: Prepare & Commit in SAME view (common path)
    ────────────────────────────────────────────────────
    IO::Commit in overlay:
      prepare_ref = SameView(op_id)
                        │
                        ▼
    IR Overlay (in-memory BTreeMap):
      op_id → IrMemEntry { payload: Prepare { write_set, ... } }
                                            │
                                            ▼
    MVCC Memtable entry (during view, in-memory):
      (key, ts) → ValueLocation::InMemory { txn_id, write_index: 2 }
                        │
                        ▼
    prepare_registry[txn_id] → CachedPrepare (in-memory)
                        │
                        ▼
    write_set[2] = (key_bytes, value_bytes)  ← the committed value
    (zero I/O — all in memory, no VLog access needed)


    Case 2: Prepare in view N, Commit in view N+M (rare, cross-view)
    ────────────────────────────────────────────────────────────────
    IO::Commit VLog entry (view N+M):
      prepare_ref = CrossView { view: N, offset: 4096, len: 512 }
                        │
                        ▼
    Sealed VLog (view N, on disk):
      offset 4096 → [CO::Prepare entry with full write_set]
                        │
                        ▼
    LRU Cache lookup: key = (segment_id, 4096)
      hit  → deserialized PreparePayload (fast)
      miss → pread + deserialize + cache (1 I/O)
                        │
                        ▼
    write_entries[write_index] → (key, value)

    MVCC index entry:
      (key, commit_ts) → UnifiedVlogPrepareValuePtr {
          prepare_ptr: { segment_id, offset: 4096, len: 512 },
          write_index: i
      }
```

### On-Disk File Layout

```
<data_dir>/
├── MANIFEST.bitcode              # Unified manifest (atomic write-temp-rename)
│
│   # No vlog_current.dat — VLog writes are deferred to seal time.
│   # At seal, entries are batch-written to the segment containing
│   # the sealed view (vlog_seg_XXXX.dat, new or appended).
│
├── vlog_seg_0000.dat             # Sealed VLog segment (views 0..3)
│   ├── [View 0 entries + footer] # ← grouped to save file descriptors
│   ├── [View 1 entries + footer] #    when min_view_vlog_size not met
│   └── [View 2 entries + footer]
│
├── vlog_seg_0001.dat             # Sealed VLog segment (views 3..5)
│   ├── [View 3 entries + footer] # ← new segment when previous exceeds
│   └── [View 4 entries + footer] #    min_view_vlog_size
│
├── mvcc_sst_0000.db              # MVCC SST for view 0 (or compacted range)
├── mvcc_sst_0001.db              # MVCC SST for view 1
├── mvcc_sst_0002.db              # MVCC SST for views 2-4 (after compaction)
│                                 # Format: same as current SSTable
│                                 # (data blocks + index + bloom + footer)
│                                 # but LsmEntry uses UnifiedLsmEntry
```

**Segment grouping** (configurable `min_view_vlog_size`, default 256KB):
```
View 0: 50KB  ─┐
View 1: 30KB   ├─ grouped into vlog_seg_0000.dat (total 120KB < 256KB)
View 2: 40KB  ─┘
View 3: 300KB ─── vlog_seg_0001.dat (exceeds threshold, own segment)
View 4: 10KB  ─┐
View 5: 15KB   ├─ grouped into vlog_seg_0002.dat
View 6: 200KB ─┘
```

---

## Data Structures

**Design principle**: The unified storage layer uses **opaque bytes** (`Vec<u8>` / `&[u8]`) for all user keys and values. No specific encoding (bitcode, JSON, etc.) is assumed. The typed `K`/`V` conversion happens at the `MvccBackend` adapter boundary. Protocol types (OpId, Timestamp, TransactionId) remain typed since they have known fixed-size layouts.

### Unified VLog Pointer Types

```rust
/// Physical pointer to an entry within the unified VLog.
/// (segment_id, offset, length) uniquely identifies any VLog entry.
pub struct UnifiedVlogPtr {
    pub segment_id: u64,
    pub offset: u64,
    pub length: u32,
}

/// Sub-pointer into a CO::Prepare VLog entry's write_set.
/// The key insight: instead of duplicating committed values in a
/// separate MVCC vlog, each MVCC index entry stores a pointer into
/// the CO::Prepare's write_set.
pub struct UnifiedVlogPrepareValuePtr {
    pub prepare_ptr: UnifiedVlogPtr,
    pub write_index: u16,
}

/// How a committed value is physically located.
pub enum ValueLocation {
    /// Value is in memory in the prepare_registry (current view).
    InMemory { txn_id: OccTransactionId, write_index: u16 },
    /// Value is in a sealed VLog segment (on disk).
    OnDisk(UnifiedVlogPrepareValuePtr),
}
```

### MVCC Index Entry

```rust
/// Entry in the MVCC index (memtable and SST).
/// Replaces current LsmEntry.
pub struct UnifiedLsmEntry {
    /// Where to find the committed value bytes.
    /// None = delete tombstone.
    pub value_ref: Option<ValueLocation>,
    /// OCC last-read timestamp for write-after-read conflict detection.
    pub last_read_ts: Option<u64>,
}
```

### IR Index Entries

```rust
/// VLog entry type discriminator (first byte of every entry).
pub enum VlogEntryType {
    Prepare = 0x01,
    Commit = 0x02,
    Abort = 0x03,
    QuorumRead = 0x04,
    QuorumScan = 0x05,
    RaiseMinPrepareTime = 0x06,
}

/// IR memtable entry (in-memory overlay, current view).
/// No vlog_ptr — VLog writes deferred to seal time.
/// No modified_view — always equals current view.
pub struct IrMemEntry {
    pub entry_type: VlogEntryType,
    pub state: IrState,
    pub payload: IrPayloadInline,
}

/// IR SST entry (on-disk, sealed views).
/// All entries are Finalized — no state field needed.
pub struct IrSstEntry {
    pub entry_type: VlogEntryType,
    pub vlog_ptr: UnifiedVlogPtr,
}
```

### IR Payload Types

```rust
/// Inline payload for IR entries.
/// Only Finalized IR ops are written to the VLog.
pub enum IrPayloadInline {
    /// CO::Prepare — sole carrier of write_set values.
    Prepare {
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        read_set: Vec<(Vec<u8>, Timestamp)>,
        write_set: Vec<(Vec<u8>, Vec<u8>)>,  // key_bytes, value_bytes
        scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
    },
    /// IO::Commit — only PrepareRef, no transaction data.
    Commit {
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        prepare_ref: PrepareRef,
    },
    Abort { transaction_id: OccTransactionId, commit_ts: Option<Timestamp> },
    QuorumRead { key: Vec<u8>, timestamp: Timestamp },
    QuorumScan { start_key: Vec<u8>, end_key: Vec<u8>, snapshot_ts: Timestamp },
    RaiseMinPrepareTime { time: u64 },
}

/// Reference from IO::Commit to its CO::Prepare.
pub enum PrepareRef {
    SameView(OpId),
    CrossView { view: u64, vlog_ptr: UnifiedVlogPtr },
}
```

### Unified Storage Engine

```rust
/// The unified storage engine.
pub struct UnifiedStore<K, IO: DiskIo> {
    mvcc_memtable: LsmMemtable<K>,
    mvcc_tree: LsmTree<IO>,
    active_vlog: UnifiedVlogSegment<IO>,
    sealed_vlog_segments: BTreeMap<u64, UnifiedVlogSegment<IO>>,
    prepare_registry: BTreeMap<OccTransactionId, Arc<CachedPrepare>>,
    prepare_cache: PrepareCache,  // LRU cache for deserialized CO::Prepare
    current_view: u64,
    manifest: UnifiedManifest,
    // ...
}

/// Typed adapter implementing MvccBackend<K, V, Timestamp>.
pub struct UnifiedMvccBackend<K, V, IO: DiskIo> {
    store: UnifiedStore<K, IO>,
    _marker: PhantomData<V>,
}
```

---

## Data Formats

### On-Disk VLog Entry (Binary, NOT bitcode for the outer frame)

The outer entry frame uses a custom binary format for seeking and recovery:

```
Byte offset  Field            Size     Encoding
─────────────────────────────────────────────────
0            entry_type       1        u8 (VlogEntryType discriminant)
1            entry_len        4        u32 LE (total bytes incl. this header)
5            op_id.client_id  8        u64 LE
13           op_id.number     8        u64 LE
21           payload          varies   bitcode-serialized (type-specific)
21+N         crc32            4        u32 LE (CRC of bytes 0..21+N)
```

Total: 25 bytes header + payload_len + 4 bytes CRC.

**No `state`, `finalized_view`, or `modified_view` fields in VLog entries:**
- **`state` unnecessary**: The VLog only persists Finalized entries. Tentative entries are either promoted to Finalized (if f+1 agree) or dropped during leader merge.
- **`finalized_view` redundant**: All entries are Finalized, so finalized_view always equals the view of the segment. Already tracked by `ViewRange` in segment metadata.
- **`modified_view` redundant**: Entries are written to the VLog of their own view, so modified_view equals the segment's view number.

The payload is bitcode-serialized according to `entry_type`. This hybrid approach allows:
- Custom binary header for sequential scanning and recovery (known field offsets)
- Bitcode for complex payloads (variable-length keys, values, transaction data)

### On-Disk MVCC SST

Same format as current SSTable (`src/mvcc/disk/sstable.rs`), but:
- `CompositeKey<Vec<u8>, Timestamp>` — key is opaque `Vec<u8>`, timestamp is typed
- `UnifiedLsmEntry` replaces `LsmEntry` as the value
- Footer, index block, bloom filter, data block format all identical
- Key ordering: byte-lexicographic for key bytes, then Reverse<Timestamp>

### On-Disk IR SST (mandatory — enables larger-than-memory IR records)

Same SSTable format but:
- Key: `OpId` (sorted by client_id then number)
- Value: `IrSstEntry { entry_type, vlog_ptr }`
- **Mandatory** — enables IR records that exceed available memory
- Lookups: check overlay first (O(log n) in-memory), then IR base SST (bloom filter + binary search + block read, with block cache)
- `from_full()`: streams the merged record directly to a new IR SST file
- `overlay_clone()`: returns only the in-memory overlay entries
- `full()`: merged iteration over overlay + IR base SST (lazy)

### Manifest Format

Same atomic write-temp-rename strategy as current `Manifest`. Bitcode-serialized `UnifiedManifest` with CRC32 checksum.

---

## How TAPIR/IR Replica Code Changes Avoided

### IR Layer: Zero Changes

The IR replica (`src/ir/replica.rs`) continues to use `VersionedRecord<IO, CO, CR>` with its in-memory `BTreeMap`-based base and overlay. The `VersionedRecord` struct is completely unchanged.

The unified storage is invisible to the IR layer because:
1. IR record entries are still `Arc<Transaction>`-based in memory (wire protocol unchanged)
2. The `transport.persist()` checkpoint can serialize using VLog references internally, but this is transparent to the IR replica
3. `DoViewChange` and `StartView` messages carry full `RecordImpl` with inline data (no PrepareRef in wire protocol)

### TAPIR Layer: Zero Core Changes

The TAPIR replica (`src/tapir/replica.rs`) continues to implement `IrReplicaUpcalls` with unchanged type signatures. The `OccStore` still stores `BTreeMap<TransactionId, (Timestamp, SharedTransaction, bool)>`.

The unification happens at the `MvccBackend` implementation level:
1. `UnifiedMvccBackend` implements `MvccBackend<K, V, Timestamp>` (same trait, 2 new default methods)
2. `commit_batch_for_transaction()` looks up the registered CO::Prepare VLog entry and uses `PrepareRef`
3. `get_at()` resolves `UnifiedVlogPrepareValuePtr` through the VLog transparently
4. `last_read_ts` is tracked in the MVCC index memtable/SST (same as current)

### Minimal Changes to Support Layer (NOT protocol code)

Two small additions to implementation utilities (NOT IR/TAPIR protocol code):

**1. `MvccBackend` trait** (`src/mvcc/backend.rs`) — 2 new methods with default no-ops:
```rust
trait MvccBackend<K, V, TS> {
    // ... all existing methods unchanged ...

    fn register_prepare(
        &mut self,
        _txn_id: TransactionId,
        _transaction: &Transaction<K, V, TS>,
        _commit_ts: TS,
    ) {}

    fn commit_batch_for_transaction(
        &mut self,
        _txn_id: TransactionId,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, TS)>,
        commit: TS,
    ) -> Result<(), Self::Error>
    where TS: Copy,
    {
        self.commit_batch(writes, reads, commit)
    }
}
```

Existing backends (MemoryStore, DiskStore, SurrealKvStore) use the defaults — zero changes needed.

**2. `OccStore`** (`src/occ/store.rs`) — 2 one-line changes:
```rust
// In add_prepared(): add one line after updating the prepared list
self.inner.register_prepare(id, &transaction, commit);

// In commit(): change commit_batch → commit_batch_for_transaction
MvccBackend::commit_batch_for_transaction(&mut self.inner, id, writes, reads, commit).unwrap();
```

### Prepare-Commit Deduplication Path

**Critical ordering guarantee**: The CO::Prepare is ALWAYS processed before IO::Commit on EVERY replica, in ALL scenarios:

1. **Normal operation**: IR protocol order — `exec_consensus(CO::Prepare)` is called before `exec_inconsistent(IO::Commit)`
2. **sync() on view change**: sync() processes `leader.consensus` FIRST (includes CO::Prepare), then `leader.inconsistent` SECOND (includes IO::Commit)
3. **Merged record completeness**: The leader's merged record ALWAYS contains both CO::Prepare and IO::Commit for committed transactions

```
exec_consensus(CO::Prepare) or sync() consensus loop:
  → OccStore::add_prepared()
    → MvccBackend::register_prepare()   ← stores in-memory reference
    → stores mapping: txn_id → Arc<Transaction> (in-memory only)

exec_inconsistent(IO::Commit):
  → OccStore::commit()
    → MvccBackend::commit_batch_for_transaction(txn_id, writes, reads, ts)
    → UnifiedMvccBackend looks up txn_id in prepare_registry (in-memory)
    → Creates MVCC entries with ValueLocation::InMemory { txn_id, write_index }
    → NO VLog write — values live in memory until view seal
```

---

## GC Analysis

### What Can Be GC'd?

| Data | Can GC? | Why |
|---|---|---|
| CO::Prepare VLog entries (committed txn) | **No** | MVCC index entries reference the write_set values. Must retain until all referenced values are superseded AND compacted away. |
| CO::Prepare VLog entries (aborted txn) | **Yes** | No MVCC references. But detecting aborted status requires checking transaction_log — expensive. |
| IO::Commit VLog entries | **Theoretically yes** | Small metadata-only entries (with PrepareRef). Side effects already applied to MVCC. But removing them breaks VLog sequential scanning. |
| IO::Abort VLog entries | **Theoretically yes** | Same as IO::Commit — small, side effects applied. |
| IO::QuorumRead/QuorumScan VLog entries | **Theoretically yes** | Side effects (last_read_ts) are in MVCC memtable, not VLog. |
| CO::RaiseMinPrepareTime VLog entries | **Theoretically yes** | Side effect (min_prepare_time) is in TAPIR replica state. |
| MVCC SST entries (old versions superseded by newer commits) | **Yes** | Standard LSM compaction — drop old versions during L0→L1 merge. |
| IR SST entries (old views superseded by merge) | **Yes** | After merge, old IR base SST is replaced. Only latest base matters. |
| Sealed VLog segments (all entries unreferenced) | **Only if all MVCC refs gone** | Need reference counting or full MVCC index scan to verify. |

### Recommendation: Intra-UnifiedStore GC is Unnecessary

1. **Primary space consumer** = committed values in CO::Prepare write_sets. These MUST be retained for MVCC reads. No GC possible without losing data.
2. **Dead IR entries** (QuorumRead, Abort, etc.) are tiny (tens of bytes each). The space waste is negligible.
3. **MVCC SST compaction** (merging old view SSTs) is the only worthwhile GC. This reduces read amplification (fewer SSTs to check) without needing VLog changes. Standard LSM compaction logic applies.
4. **VLog segment reclamation** requires proving no MVCC index entries reference any entry in the segment. This is complex (requires full MVCC index scan) and the benefit is marginal for typical workloads.
5. **CDC-based shard replacement** (Tier 2, already designed) replaces the entire shard's storage, which is the natural "GC" for old data.

---

## Prepared Transaction Payload Caching

### Problem

Cross-view prepared transactions (prepared in view N, committed in view N+M where M >= 1) need to resolve the CO::Prepare payload from a sealed view's VLog. Without caching, each access requires a disk read + deserialization.

### Design

Single LRU cache shared between two access patterns:

1. **MVCC value reads**: `get_at(key, ts)` → `UnifiedVlogPrepareValuePtr` → resolve CO::Prepare from VLog → extract `write_set[index]`
2. **OCC prepared list access**: During sync() or merge(), prepared transactions from sealed views need their transaction data

```rust
/// LRU cache for deserialized CO::Prepare payloads.
/// Key: (segment_id, offset) — uniquely identifies a VLog entry.
/// Value: Arc<CachedPrepare> — deserialized and shared.
prepare_cache: PrepareCache
```

### Sizing

- **MVCC reads** (high volume): Typical transaction has 1-10 keys. Reading ANY key from a committed transaction caches the entire Prepare, benefiting subsequent reads to other keys in the same transaction. Cache should be sized for the working set of recently-committed transactions.
- **Cross-view prepares** (rare): Prepared-but-not-yet-committed transactions spanning view changes are rare (requires coordinator delay >= view change interval). A few entries suffice.

**Default cache size**: 1024 entries (configurable). Each entry is ~1-10 KB (typical transaction size). Total memory: ~1-10 MB.

---

## View VLog Grouping

### Problem

Each view change creates a new VLog file. Under frequent view changes (e.g., every few seconds), this creates many small files consuming file descriptors.

### Design

Group consecutive small view VLogs into segment files:

```rust
/// Configurable minimum VLog segment size before starting a new file.
/// Default: 256 KB.
min_view_vlog_size: u64,
```

**Algorithm** (on view seal):
1. Check current active segment size
2. If `current_segment.write_offset >= min_view_vlog_size`:
   - Close and seal current segment
   - Open new segment for the new view
3. Else:
   - Append a view footer to current segment (marks view boundary)
   - Continue appending to same segment for the new view

**View footer** (appended between views within a segment):
```
| marker: u32 (0xVIEWEND) | view_num: u64 | num_entries: u32 |
| view_start_offset: u64  | view_end_offset: u64              |
| crc32: u32                                                   |
```

**Lookup**: To read an entry at `(view, offset)`:
1. Find segment containing this view (from manifest's `VlogSegmentMeta.views`)
2. pread at `segment_start + view_start_offset + entry_offset`
