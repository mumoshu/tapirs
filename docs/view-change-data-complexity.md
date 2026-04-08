# View Change Data Catalog — Build Complexity

What every replica builds and exchanges during an IR view change, with Big-O complexity for each.

## Variables

| Symbol | Meaning |
|--------|---------|
| **N** | Total IR record entries (inconsistent + consensus) across all sealed views |
| **D** | Entries added/changed in the current (unsealed) view |
| **f** | Tolerated failures; replica count = 2f+1 |
| **S** | Sealed VlogLsm segments on disk |
| **M** | Unique entries across all f+1 DoViewChange records received by leader |
| **U** | Undecided consensus entries (no majority among tentative) |
| **T** | TAPIR transactions in the prepared-txn store |
| **W** | Average write-set size per committed transaction |

## Quick-Reference Table

| # | Data | Who Builds | Build Complexity | Wire/Memory Size |
|---|------|-----------|-----------------|-----------------|
| 1 | SharedView | all replicas | O(1) | O(f) |
| 2 | DVC addendum (delta) | sender → leader | O(D) | O(D) |
| 3 | DVC addendum (full) | sender → leader | O(N log N + S) | O(N) |
| 4 | latest_normal_view | sender | O(1) | O(f) |
| 5 | Resolved record | leader, per addendum | O(N + S) each, O(f(N+S)) total | O(N) each |
| 6 | Merged record R | leader | O(f · N · log M) | O(M) |
| 7 | entries_by_opid | leader | O(f · N) | O(f · U) |
| 8 | d and u sets | leader | O(U · f²) | O(\|d\| + \|u\|) |
| 9 | merge() result | leader (TAPIR) | O((\|d\|+\|u\|) · log T) | O(\|d\|+\|u\|) |
| 10 | install_merged_record | leader | O(M + S) | O(\|delta\|) new |
| 11 | StartView (full) | leader → lagging | O(S) reads | O(N) |
| 12 | StartView (delta) | leader → caught-up | O(1) Arc clone | O(\|delta\|) |
| 13 | Routing decision | leader | O(f) | — |
| 14 | install_start_view | non-leader | O(N log N + S) | O(N) indexed |
| 15 | sync() | all replicas (TAPIR) | O(M · log N) | — |
| 16 | CDC delta | all replicas (TAPIR) | O(\|delta\| · W) | O(\|delta\| · W) |
| 17 | flush/seal | all replicas | O(D + S) | O(D) disk |
| 18 | S3 sync | all replicas (async) | O(\|new bytes\|) net | O(\|new bytes\|) |

## Phase 1: DoViewChange (each replica → new leader)

### 1. SharedView

View number + membership struct.

- **Source**: `src/ir/replica.rs:299-337` — `broadcast_do_view_change`
- **Build**: O(1) — clone of existing struct.
- **Size**: O(f) — membership list of 2f+1 addresses.

### 2. DVC addendum payload (delta)

Sent when `base_view + 1 == next_view` AND all peers confirmed latest normal view via periodic status broadcasts.

- **Source**: `src/unified/ir/ir_record_store.rs:950-966` — `build_view_change_payload`
- **How**: Calls `memtable_bytes()` which serializes D memtable entries to vlog-format bytes, then wraps as `PersistentPayload::delta`.
- **Decision logic**: `src/ir/replica.rs:318-326` — checks `peer_normal_views` for all peers: O(f) lookups.
- **Build**: O(D).
- **Size**: O(D) bytes.

### 3. DVC addendum payload (full)

Sent when there is a view gap or peers have not confirmed latest normal view.

- **Source**: `src/ir/replica.rs:328` — `make_full_payload(full_record())`
- **How**:
  1. `full_record()` at `src/unified/ir/ir_record_store.rs:916-939`: calls `all_segment_bytes()` to read S sealed segments from disk (O(S) disk reads, O(N) bytes), then `memtable_bytes()` for D current entries, then `into_indexed()` which deserializes all bytes and inserts into BTreeMaps.
  2. `make_full_payload` at line 994-1045: re-serializes the Indexed record back to vlog-format segment bytes.
- **Build**: O(N log N + S) — dominated by `into_indexed()` building BTreeMaps from N entries.
- **Size**: O(N) bytes.

### 4. latest_normal_view

Piggybacked on the addendum. Clone of the replica's existing `SharedView`.

- **Source**: `src/ir/replica.rs:330-333`
- **Build**: O(1).
- **Size**: O(f).

## Phase 2: Leader Merge

### 5. Resolved record (per addendum)

The leader resolves each DoViewChange addendum payload into a full record for merging.

- **Source**: `src/unified/ir/ir_record_store.rs:1220-1242` — `resolve_do_view_change_payload`
- **How**: Each call exports the leader's own base via `all_segment_bytes()` (O(S) disk reads, O(N) bytes). Then `payload.resolve(base)` concatenates delta segments onto the base (O(D) for delta, O(1) for full). Returns `PersistentRecord::Raw`.
- **Called**: f+1 times (once per addendum).
- **Build**: O(N + S) per call. **O(f · (N + S)) total**.
- **Size**: O(N) per resolved record.
- **Note**: The base is re-exported from disk for each call. Caching would reduce total disk I/O from O(f · S) to O(S).

### 6. Merged record R

BTreeMap union of all entries from f+1 resolved records.

- **Source**: `src/ir/replica.rs:602-658`
- **How**: For each of f+1 resolved Raw records, iterates entries via `scan_entries()` (O(N) deserialization per record). Each entry is inserted into R's BTreeMap (O(log M) per insert).
  - **Inconsistent entries** (lines 608-631): If entry already in R, keeps the one with higher finalization view. Otherwise marks as Finalized in new view.
  - **Consensus entries** (lines 632-658): Finalized entries go directly into R. Tentative entries are collected into `entries_by_opid` for majority voting.
- **Build**: **O(f · N · log M)**.
- **Size**: O(M) entries.

### 7. entries_by_opid

Tentative consensus entries grouped by OpId, collected during the merge loop.

- **Source**: `src/ir/replica.rs:649-655`
- **How**: O(1) per tentative entry to push into the Vec for that OpId.
- **Build**: O(f · N) amortized (built in the same pass as R).
- **Size**: O(f · U) — up to f+1 entries per undecided OpId.

### 8. d (majority-decided) and u (undecided)

Partition of tentative consensus entries based on majority voting.

- **Source**: `src/ir/replica.rs:661-695`
- **How**: For each OpId in `entries_by_opid`, a nested filter counts how many of the f+1 entries share the same result (lines 675-686). If count >= f/2+1, the OpId goes into `d` with the majority result. Otherwise all entries for that OpId go into `u`.
- **Build**: **O(U · f²)** — nested comparison. With f typically 1-2, effectively O(U).
- **Size**: O(|d| + |u|).

### 9. merge() result (TAPIR upcall)

The TAPIR application layer decides the final result for all undecided operations.

- **Source**: `src/tapir/replica.rs:735-805`
- **How**:
  1. Cleanup: `reset_min_prepare_time_to_finalized()` + `remove_all_unfinalized_prepared_txns()`.
  2. Process d entries (lines 747-794): For each `Prepare` with Ok result, re-executes via `exec_consensus` (OCC validation, O(log T) BTreeMap ops). For `RaiseMinPrepareTime`, validates received >= requested.
  3. Process u entries (lines 798-802): Leader executes `exec_consensus` to choose result.
- **Build**: **O((|d| + |u|) · log T)**.
- **Size**: O(|d| + |u|) result map.

### 10. install_merged_record

Persists the merged record and produces delta for StartView.

- **Source**: `src/unified/ir/ir_record_store.rs:1062-1218`
- **How**:
  1. **Import best_payload segments** (lines 1081-1108): For each segment from the best DoViewChange payload, if max(view) > base_view, import via `persist_sealed_segment`. O(S) segment operations.
  2. **Compute delta** (lines 1117-1144): Iterate merged BTreeMap; entries whose OpId is not in the VlogLsm index OR is in `resolved_ops` form the delta. O(M) iteration with O(1) amortized index check.
  3. **Encode and persist delta** (lines 1148-1183): Serialize delta entries to vlog-format bytes, write as sealed segment. O(|delta|).
  4. **Finalize state** (lines 1187-1193): Clear memtable, update base_view, start new view.
- **Build**: **O(M + S)**.
- **Produces**:
  - `transition`: (from_view, delta record) — for CDC.
  - `start_view_delta`: Optional delta payload for same-base recipients.
  - `previous_base_view`: For StartView routing decisions.
- **New disk bytes**: O(|delta|).

## Phase 3: StartView Broadcast (leader → all replicas)

### 11. StartView payload (full)

Sent to recipients whose latest_normal_view does NOT match the leader's previous base view.

- **Source**: `src/ir/replica.rs:773` — `build_start_view_payload(None)`
- **How**: `src/unified/ir/ir_record_store.rs:985-991` — reads all sealed segment bytes with ViewRange metadata via `all_segment_bytes_with_views()`.
- **Build**: O(S) disk reads. Built **once**, then Arc-cloned O(1) per additional recipient.
- **Size**: O(N) bytes.

### 12. StartView payload (delta)

Sent to recipients whose latest_normal_view matches the leader's previous base view.

- **Source**: `src/ir/replica.rs:791` — `build_start_view_payload(Some(delta))`
- **How**: Clones the pre-computed `start_view_delta` from `install_merged_record`. Arc-wrapped for O(1) clone.
- **Build**: **O(1)**.
- **Size**: O(|delta|) bytes.

### 13. Per-recipient routing decision

Determines whether each recipient gets a full or delta StartView.

- **Source**: `src/ir/replica.rs:779-788`
- **How**: For each destination, looks up `outstanding_do_view_changes` to check if the recipient's DoViewChange had a latest_normal_view matching the leader's previous base view. O(1) per recipient.
- **Build**: O(1) each, **O(f) total** across ~2f+1 destinations.

## Phase 4: install_start_view (non-leader replicas)

### 14. install_start_view_unified

Installs the StartView payload, producing three records for upcalls.

- **Source**: `src/unified/ir/ir_record_store.rs:747-886`
- **How**:
  1. **Partition segments** (lines 775-810): For each payload segment, if max(view) <= base_view, classify as existing (skip). Otherwise import via `persist_sealed_segment`. O(S) comparisons + O(|new bytes|) disk persist.
  2. **Build three records** (lines 815-851):
     - `previous_record`: existing segments + memtable → `into_indexed()`: O((N_existing + D) log(N_existing + D))
     - `transition`: new segments only → `into_indexed()`: O(|delta| log |delta|)
     - `new_record`: all segments → `into_indexed()`: O(N log N)
  3. **Update state** (lines 860-872): Clear memtable, advance base_view, update manifest.
- **Build**: **O(N log N + S)** — dominated by the three `into_indexed()` calls.
- **Produces**: `ViewInstallResult` with previous_record, transition, new_record.

### 15. sync() (TAPIR upcall)

Synchronizes local TAPIR state with the leader's merged record.

- **Source**: `src/tapir/replica.rs:642-732`
- **How**:
  - Iterates all consensus entries in the leader's record (new_record). For each:
    - Looks up local record (previous_record) via `get_consensus`: O(log N) on Indexed BTreeMap.
    - If not locally finalized with same result: syncs prepared txn state via `add_or_replace_or_finalize_prepared_txn` (O(log T)).
  - Iterates all inconsistent entries. For each:
    - Looks up local record: O(log N).
    - If not locally finalized: calls `exec_inconsistent`.
  - Calls `gc_stale_state()` at end.
- **Build**: **O(M · log N)**.

### 16. CDC delta (on_install_leader_record_delta)

Extracts committed write-set changes for change data capture.

- **Source**: `src/tapir/replica.rs:818-853`
- **How**: Iterates the transition record's inconsistent entries. For each `IO::Commit`, extracts the shard write set (O(W) per transaction) and records it as a `LeaderRecordDelta`.
- **Build**: **O(|delta| · W)**.
- **Size**: O(|delta| · W).

## Phase 5: Post-View-Change (all replicas)

### 17. flush/seal

Seals VlogLsm memtables and saves manifest to disk.

- **Source**: `src/unified/ir/ir_record_store.rs:576-615` — `seal()`
- **How**:
  1. Seal both inc_lsm and con_lsm: iterate memtable entries, serialize only finalized ones to vlog-format bytes, write as sealed segment. O(D) serialization + disk write.
  2. Update manifest with segment metadata and save. O(S) to write segment list.
- **Build**: **O(D + S)**.
- **Disk bytes**: O(D).

### 18. S3 sync (async, fire-and-forget)

Uploads new segments and manifest to S3 after view change.

- **Source**: `src/remote_store/sync_to_remote.rs`
- **How**: Spawned as a tokio task. Uploads new sealed segments + updated manifest.
  - Segment upload: O(|new bytes|) network I/O.
  - Manifest upload: O(1).
  - Manifest discovery (`latest_manifest_view`): O(total_manifests) — lists all `v*.manifest` files on S3.
- **Build**: **O(|new bytes|)** network I/O.
- **Known caveat**: `list_files_reverse` is O(total_manifests) and called every 5s by read replicas.

## Observations

### 1. Leader merge is the bottleneck

The dominant cost is on the leader: **O(f · N · log M + f · S)**.

`resolve_do_view_change_payload` (line 1230) re-exports the base sealed segments via `all_segment_bytes()` for each of f+1 addenda. This means O(f · S) disk reads. Caching the base bytes across calls would reduce this to O(S).

### 2. Delta optimization is critical

On consecutive views (the common case), the DVC payload shrinks from O(N) to O(D), StartView from O(N) to O(|delta|), and the expensive `into_indexed()` O(N log N) is avoided on the sender side. The `peers_confirmed` check at `replica.rs:318-324` gates this optimization.

### 3. N grows without bound

The IR record accumulates entries across all views. There is no pruning or compaction of old entries. N increases monotonically over the cluster's lifetime, degrading every O(N) and O(N log N) term.

### 4. Triple into_indexed() in install_start_view

Lines 831, 839, and 851 each call `into_indexed()` which is O(N log N). `previous_record` and `new_record` share most of their segments — a single-pass overlay approach could reduce the total to O(N log N) once plus O(|delta| log |delta|).

### 5. Redundant base export in resolve

`resolve_do_view_change_payload` at line 1230-1237 re-reads all sealed segments for each of f+1 payloads. Caching the byte export across calls would reduce O(f · S) disk reads to O(S).

### 6. Quadratic majority detection

The d/u computation at lines 675-686 is O(f²) per undecided OpId due to the nested filter. With f typically 1-2 in production, this is not a practical concern but is theoretically quadratic in f.
