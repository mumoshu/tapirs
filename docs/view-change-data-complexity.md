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
| 3 | DVC addendum (full) | sender → leader | O(S) reads | O(N) |
| 4 | latest_normal_view | sender | O(1) | O(f) |
| 5 | Merged record R | leader | O(N log N + S + f·D·log M) | O(M) |
| 6 | entries_by_opid | leader | O(f · D) | O(f · U) |
| 7 | d and u sets | leader | O(U · f²) | O(\|d\| + \|u\|) |
| 8 | merge() result | leader (TAPIR) | O((\|d\|+\|u\|) · log T) | O(\|d\|+\|u\|) |
| 9 | install_merged_record | leader | O(M + S) | O(\|delta\|) new |
| 10 | StartView (full) | leader → lagging | O(S) reads | O(N) |
| 11 | StartView (delta) | leader → caught-up | O(1) Arc clone | O(\|delta\|) |
| 12 | Routing decision | leader | O(f) | — |
| 13 | install_start_view | non-leader | O(N log N + S) | O(N) indexed |
| 14 | sync() | all replicas (TAPIR) | O(M · log N) | — |
| 15 | CDC delta | all replicas (TAPIR) | O(\|delta\| · W) | O(\|delta\| · W) |
| 16 | flush/seal | all replicas | O(D + S) | O(D) disk |
| 17 | S3 sync | all replicas (async) | O(\|new bytes\|) net | O(\|new bytes\|) |

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

- **Source**: `src/ir/replica.rs:328` — `build_full_view_change_payload()`
- **How**: Reads all sealed segment bytes with ViewRange metadata via `all_segment_bytes_with_views()`, appends memtable bytes, wraps as `PersistentPayload::full`. No deserialization or re-serialization round-trip.
- **Build**: O(S) disk reads, O(N) bytes.
- **Size**: O(N) bytes.

### 4. latest_normal_view

Piggybacked on the addendum. Clone of the replica's existing `SharedView`.

- **Source**: `src/ir/replica.rs:330-333`
- **Build**: O(1).
- **Size**: O(f).

## Phase 2: Leader Merge

### 5. Merged record R

BTreeMap union of leader's record plus peer payload entries.

- **Source**: `src/ir/replica.rs:602-658`
- **How**: The leader builds its own `full_record()` once (O(N log N + S): reads all sealed segments via `all_segment_bytes_with_views`, strips views, adds memtable, deserializes via `into_indexed`). R is initialized from this record. Then for each of f peers' DVC payloads, entries are scanned from the payload segments directly via `as_unresolved_record()` and merged into R. Delta payloads contribute only O(D) entries each; full payloads contribute O(N) but base entries are harmless duplicates.
  - **Inconsistent entries**: If entry already in R, keeps the one with higher finalization view. Otherwise marks as Finalized in new view.
  - **Consensus entries**: Finalized entries go directly into R. Tentative entries are collected into `entries_by_opid` for majority voting.
- **Build**: **O(N log N + S + f · D · log M)** — one base read, f delta scans.
- **Size**: O(M) entries.

### 6. entries_by_opid

Tentative consensus entries grouped by OpId, collected during the merge loop.

- **Source**: `src/ir/replica.rs:649-655`
- **How**: O(1) per tentative entry to push into the Vec for that OpId.
- **Build**: O(f · D) amortized (built in the same pass as R; only delta entries are tentative).
- **Size**: O(f · U) — up to f+1 entries per undecided OpId.

### 7. d (majority-decided) and u (undecided)

Partition of tentative consensus entries based on majority voting.

- **Source**: `src/ir/replica.rs:661-695`
- **How**: For each OpId in `entries_by_opid`, a nested filter counts how many of the f+1 entries share the same result (lines 675-686). If count >= f/2+1, the OpId goes into `d` with the majority result. Otherwise all entries for that OpId go into `u`.
- **Build**: **O(U · f²)** — nested comparison. With f typically 1-2, effectively O(U).
- **Size**: O(|d| + |u|).

### 8. merge() result (TAPIR upcall)

The TAPIR application layer decides the final result for all undecided operations.

- **Source**: `src/tapir/replica.rs:735-805`
- **How**:
  1. Cleanup: `reset_min_prepare_time_to_finalized()` + `remove_all_unfinalized_prepared_txns()`.
  2. Process d entries (lines 747-794): For each `Prepare` with Ok result, re-executes via `exec_consensus` (OCC validation, O(log T) BTreeMap ops). For `RaiseMinPrepareTime`, validates received >= requested.
  3. Process u entries (lines 798-802): Leader executes `exec_consensus` to choose result.
- **Build**: **O((|d| + |u|) · log T)**.
- **Size**: O(|d| + |u|) result map.

### 9. install_merged_record

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

### 10. StartView payload (full)

Sent to recipients whose latest_normal_view does NOT match the leader's previous base view.

- **Source**: `src/ir/replica.rs:773` — `build_start_view_payload(None)`
- **How**: `src/unified/ir/ir_record_store.rs` — reads all sealed segment bytes with ViewRange metadata.
- **Build**: O(S) disk reads. Built **once**, then Arc-cloned O(1) per additional recipient.
- **Size**: O(N) bytes.

### 11. StartView payload (delta)

Sent to recipients whose latest_normal_view matches the leader's previous base view.

- **Source**: `src/ir/replica.rs:791` — `build_start_view_payload(Some(delta))`
- **How**: Clones the pre-computed `start_view_delta` from `install_merged_record`. Arc-wrapped for O(1) clone.
- **Build**: **O(1)**.
- **Size**: O(|delta|) bytes.

### 12. Per-recipient routing decision

Determines whether each recipient gets a full or delta StartView.

- **Source**: `src/ir/replica.rs:779-788`
- **How**: For each destination, looks up `outstanding_do_view_changes` to check if the recipient's DoViewChange had a latest_normal_view matching the leader's previous base view. O(1) per recipient.
- **Build**: O(1) each, **O(f) total** across ~2f+1 destinations.

## Phase 4: install_start_view (non-leader replicas)

### 13. install_start_view_unified

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

### 14. sync() (TAPIR upcall)

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

### 15. CDC delta (on_install_leader_record_delta)

Extracts committed write-set changes for change data capture.

- **Source**: `src/tapir/replica.rs:818-853`
- **How**: Iterates the transition record's inconsistent entries. For each `IO::Commit`, extracts the shard write set (O(W) per transaction) and records it as a `LeaderRecordDelta`.
- **Build**: **O(|delta| · W)**.
- **Size**: O(|delta| · W).

## Phase 5: Post-View-Change (all replicas)

### 16. flush/seal

Seals VlogLsm memtables and saves manifest to disk.

- **Source**: `src/unified/ir/ir_record_store.rs:576-615` — `seal()`
- **How**:
  1. Seal both inc_lsm and con_lsm: iterate memtable entries, serialize only finalized ones to vlog-format bytes, write as sealed segment. O(D) serialization + disk write.
  2. Update manifest with segment metadata and save. O(S) to write segment list.
- **Build**: **O(D + S)**.
- **Disk bytes**: O(D).

### 17. S3 sync (async, fire-and-forget)

Uploads new segments and manifest to S3 after view change.

- **Source**: `src/remote_store/sync_to_remote.rs`
- **How**: Spawned as a tokio task. Uploads new sealed segments + updated manifest.
  - Segment upload: O(|new bytes|) network I/O.
  - Manifest upload: O(1).
  - Manifest discovery (`latest_manifest_view`): O(total_manifests) — lists all `v*.manifest` files on S3.
- **Build**: **O(|new bytes|)** network I/O.
- **Known caveat**: `list_files_reverse` is O(total_manifests) and called every 5s by read replicas.

## Observations

### 1. Leader merge reads base once

The leader builds its own `full_record()` once (O(N log N + S)), initializes R from it, then scans only delta entries from each peer's DVC payload. The dominant cost is **O(N log N + S + f · D · log M)**. Previously `resolve_do_view_change_payload` re-read the base for each of f+1 payloads — O(f · (N + S)) — which was eliminated by scanning payload segments directly.

### 2. Delta optimization is critical

On consecutive views (the common case), the DVC payload shrinks from O(N) to O(D), StartView from O(N) to O(|delta|), and the expensive `into_indexed()` O(N log N) is avoided on the sender side. The `peers_confirmed` check at `replica.rs:318-324` gates this optimization.

### 3. N grows without bound

The IR record accumulates entries across all views. There is no pruning or compaction of old entries. N increases monotonically over the cluster's lifetime, degrading every O(N) and O(N log N) term.

### 4. Triple into_indexed() in install_start_view

Lines 831, 839, and 851 each call `into_indexed()` which is O(N log N). `previous_record` and `new_record` share most of their segments — a single-pass overlay approach could reduce the total to O(N log N) once plus O(|delta| log |delta|).

### 5. Quadratic majority detection

The d/u computation at lines 675-686 is O(f²) per undecided OpId due to the nested filter. With f typically 1-2 in production, this is not a practical concern but is theoretically quadratic in f.

### 6. Remaining read-and-deserialize-all-segments sites

Three places still read and deserialize all segment bytes into Indexed (BTreeMap) records:

**1. `full_record()`** — `ir_record_store.rs:914`

Reads all sealed segments via `all_segment_bytes_with_views()` + memtable → `into_indexed()`. Called once per view change on the leader (for merge + sync).

**2. `install_start_view_unified()`** — 3 calls at lines 805, 813, 825

Builds three Indexed records from segment bytes:

- `previous_record` (line 805): existing segments + memtable → `into_indexed()` — O((N_existing + D) log(N_existing + D))
- `transition` (line 813): new segments only → `into_indexed()` — O(|delta| log |delta|)
- `new_record` (line 825): ALL segments (existing + new) → `into_indexed()` — O(N log N)

`previous_record` and `new_record` share most of their segments — observation #4 notes a single-pass overlay approach could reduce this to O(N log N) once + O(|delta| log |delta|).

**3. `RecordView` methods on `PersistentRecord::Raw`** — lines 222, 233, 244, 256

When `consensus_entries()`, `inconsistent_entries()`, `get_consensus()`, `get_inconsistent()` are called on a Raw record, they call `scan_entries()` which deserializes all segment bytes. This happens when the merge loop iterates peer payload records (via `as_unresolved_record()` → Raw → iterator). Those are only delta-sized though, not the full record.

**Summary:**

| Site | Reads | Deserializes | When |
|------|-------|-------------|------|
| `full_record()` | all sealed segments + memtable | O(N log N) via `into_indexed` | leader merge (once per VC) |
| `install_start_view_unified` ×3 | existing + new segment bytes | O(N log N) × 3 | non-leader install (per VC) |
| `RecordView` on Raw | already in memory | O(D) per scan | merge loop (peer deltas) |

The biggest remaining opportunity is `install_start_view_unified` with its triple `into_indexed()`. The `full_record()` in the leader path reads all segments once — that's harder to avoid since the `sync()` upcall needs O(log n) lookups on the full record.
