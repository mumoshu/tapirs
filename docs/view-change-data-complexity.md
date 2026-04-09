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
| 2 | DVC addendum (always delta) | sender → leader | O(D) | O(D) |
| 3 | ~~DVC addendum (full)~~ | ~~sender → leader~~ | ~~O(S) reads~~ | ~~O(N)~~ **REMOVED** |
| 4 | latest_normal_view | sender | O(1) | O(f) |
| 5 | Merged record R | leader | **O(f · D · log M)** | O(f · D) |
| 6 | entries_by_opid | leader | O(f · D) | O(f · U) |
| 7 | d and u sets | leader | O(U · f²) | O(\|d\| + \|u\|) |
| 8 | merge() result | leader (TAPIR) | O((\|d\|+\|u\|) · log T) | O(\|d\|+\|u\|) |
| 9 | install_merged_record | leader | **O(\|R\|)** encode + persist | O(\|R\|) new |
| 10 | StartView (full) | leader → lagging | O(S) reads | O(N) |
| 11 | StartView (delta) | leader → caught-up | O(1) Arc clone | O(\|delta\|) |
| 12 | Routing decision | leader | O(f) | — |
| 13 | install_start_view | non-leader | **O(\|delta\| log \|delta\|)** + O(S) | O(\|delta\|) indexed |
| 14a | sync() leader | leader (TAPIR) | **O(f·D · log D)** | — |
| 14b | sync() non-leader | non-leader (TAPIR) | **O(\|delta\| · log N)** | — |
| 15 | CDC delta | all replicas (TAPIR) | O(\|delta\| · W) | O(\|delta\| · W) |
| 16 | flush/seal | all replicas | O(D + S) | O(D) disk |
| 17 | S3 sync | all replicas (async) | O(\|new bytes\|) net | O(\|new bytes\|) |

## Phase 1: DoViewChange (each replica → new leader)

### 1. SharedView

View number + membership struct.

- **Source**: `src/ir/replica.rs:299-337` — `broadcast_do_view_change`
- **Build**: O(1) — clone of existing struct.
- **Size**: O(f) — membership list of 2f+1 addresses.

### 2. DVC addendum payload (always delta)

Always memtable-only. DVC payloads never carry sealed segments — all peers
with matching LNV have identical sealed data (see `view-change-unneeded-doviewchange-full-payload.md`).

- **Source**: `src/unified/ir/ir_record_store.rs` — `build_view_change_payload`
- **How**: Calls `memtable_bytes()` which serializes D memtable entries to vlog-format bytes, wraps as `PersistentPayload::delta`.
- **Build**: O(D).
- **Size**: O(D) bytes.

### ~~3. DVC addendum payload (full) — REMOVED~~

Previously sent when there was a view gap or peers hadn't confirmed LNV.
Removed: three independent filters (leader rejection guard, peer LNV matching,
best_payload LNV matching) proved sealed segments in DVC were never consumed
by the leader merge. See `view-change-unneeded-doviewchange-full-payload.md`.

### 4. latest_normal_view

Piggybacked on the addendum. Clone of the replica's existing `SharedView`.

- **Source**: `src/ir/replica.rs:330-333`
- **Build**: O(1).
- **Size**: O(f).

## Phase 2: Leader Merge

### 5. Merged record R

BTreeMap union of leader's record plus peer payload entries.

- **Source**: `src/ir/replica.rs:602-658`
- **How**: The leader builds its own `memtable_record()` once (O(D): reads memtable bytes only). R is initialized from this record. Then for each of f peers' DVC payloads (always delta, memtable-only), entries are scanned via `payload_as_record()` and merged into R.
  - **Inconsistent entries**: If entry already in R, keeps the one with higher finalization view. Otherwise marks as Finalized in new view.
  - **Consensus entries**: Finalized entries go directly into R. Tentative entries are collected into `entries_by_opid` for majority voting.
- **Build**: **O(f · D · log M)** — f memtable scans merged into R.
- **Size**: O(f · D) entries (memtable-only, no sealed entries).

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

Persists the merged record as a sealed segment.

- **Source**: `src/unified/ir/ir_record_store.rs` — `install_merged_record`
- **How**: R IS the delta — it contains only merged memtable entries (none are in the VlogLsm sealed index). Encode R as vlog-format bytes, persist as sealed segment, clear memtable, advance base_view.
- **Build**: **O(|R|)** encode + persist. No segment import, no delta computation.
- **Produces**:
  - `transition`: (from_view, R) — for CDC.
  - `start_view_delta`: Optional delta payload for same-base recipients.
  - `previous_base_view`: For StartView routing decisions.
- **New disk bytes**: O(|R|).

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

### 13. prepare + complete start_view install

Split into two phases. Prepare partitions segments and builds the transition record. Complete imports segments and updates state. Between the two, the VlogLsm's existing index serves as the "previous record" for sync skip-check lookups.

- **Source**: `src/unified/ir/ir_record_store.rs` — `prepare_start_view_unified` + `complete_start_view_unified`
- **How**:
  1. **Prepare** (`&self`, no VlogLsm mutation):
     - Partition payload segments by view: skip if max(view) <= base_view.
     - Build `transition` from new segments only → `into_indexed()`: O(|delta| log |delta|).
     - Return `PreparedInstall` with transition record + payload for complete phase.
  2. **Caller performs CDC + sync** (VlogLsm still has pre-install state):
     - `sync(RecordStoreView(&record), &transition)`: iterates |delta| entries, O(log N) VlogLsm lookup per entry.
  3. **Complete** (`&mut self`):
     - Import new segments via `persist_sealed_segment`: O(|new bytes|) disk persist.
     - Clear memtable, advance base_view, update manifest.
- **Build**: **O(|delta| log |delta|)** (transition indexing) + **O(|delta| · log N)** (sync via VlogLsm).
- **Produces**: `PreparedInstall` with transition record (no previous_record or new_record).

### 14a. sync() — leader path (TAPIR upcall)

Synchronizes leader's TAPIR state with the merged memtable record.

- **Source**: `src/tapir/replica.rs:642-732`
- **Called with**: `sync(&leader_record, &R)` at `replica.rs:711`
- **How**: Iterates R (merged memtable entries, O(f·D)), looks up each in leader_record (memtable, O(log D)). Skips the leader's own entries, processes peer entries.
- **Build**: **O(f·D · log D)** — already optimized (R is memtable-only).

### 14b. sync() — non-leader path (TAPIR upcall)

Synchronizes follower's TAPIR state with the transition (delta) record.

- **Source**: `src/tapir/replica.rs:642-732`
- **Called with**: `sync(RecordStoreView(&record), &transition)` at `replica.rs:843`
- **How**: Iterates |delta| entries in `transition` (the delta record from prepare phase). For each:
  - Looks up `RecordStoreView` (backed by VlogLsm) via `get_consensus`/`get_inconsistent`: O(log N).
  - If already finalized with same result: skip (O(1)).
  - Otherwise: syncs prepared txn state or calls `exec_inconsistent`.
  - Calls `gc_stale_state()` at end.
- **Build**: **O(|delta| · log N)** — iterates only delta entries, O(log N) VlogLsm lookups.

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

### 1. Leader merge reads memtable only

The leader builds its own `memtable_record()` once (O(D)), initializes R from it, then merges only memtable entries from each peer's DVC payload. The dominant cost is **O(f · D · log M)**. Missed-view sealed entries bypass merge and go directly through sync(). All replicas in the merge share the same latest_normal_view, so their sealed segments are identical — only memtable entries differ.

### 2. DVC always delta, StartView delta on common path

DVC payloads are always O(D) (memtable-only). The full payload path was removed
after analysis proved sealed segments in DVC were never consumed (see
`view-change-unneeded-doviewchange-full-payload.md`). StartView shrinks from
O(N) to O(|delta|) on consecutive views (same-base recipients).

### 3. N grows without bound

The IR record accumulates entries across all views. There is no pruning or compaction of old entries. N increases monotonically over the cluster's lifetime, degrading every O(N) and O(N log N) term.

### 4. ~~Triple into_indexed() in install_start_view~~ — RESOLVED

Resolved by splitting install into prepare + complete. Only the transition record is indexed via `into_indexed()` — O(|delta| log |delta|). The "previous record" is the VlogLsm's existing index, accessed via `RecordStoreView` at O(log N) per lookup. `new_record` and `previous_record` construction eliminated entirely.

### 5. Quadratic majority detection

The d/u computation at lines 675-686 is O(f²) per undecided OpId due to the nested filter. With f typically 1-2 in production, this is not a practical concern but is theoretically quadratic in f.

### 6. O(N) sites — all resolved

All hot-path O(N) sites have been eliminated. The leader merge path was optimized to O(D) in earlier work. The non-leader install+sync path was the last remaining O(N) bottleneck.

**Resolution for install_start_view (was triple `into_indexed()`):**

Split into `prepare_start_view_install` (partition + build transition, `&self`) and `complete_start_view_install` (import + state update, `&mut self`). Between the two, VlogLsm still has pre-install state. Only transition is indexed: O(|delta| log |delta|). previous_record and new_record are no longer built.

**Resolution for sync() non-leader (was O(N · log N)):**

Changed to `sync(RecordStoreView(&record), &transition)`. Iterates |delta| transition entries, doing O(log N) VlogLsm lookup per entry via RecordStoreView adapter. Total: O(|delta| · log N).

**`RecordView` on Raw peer records** [WARM, bounded by O(D)]

Merge loop iterates peer DVC payload records via `as_unresolved_record()` → Raw → `scan_entries()`. Since DVC payloads are always delta (memtable-only), each scan is O(D). Total: O(f · D). Acceptable.

**Summary:**

| Site | Complexity | When | Status |
|------|-----------|------|--------|
| `memtable_record()` | O(D) | leader merge | **Optimized** |
| `install_merged_record` | O(\|R\|) | leader merge | **Optimized** (was O(M + S)) |
| DVC payload | O(D) | every VC sender | **Optimized** (was O(N) for full) |
| `install_start_view` (prepare) | O(\|delta\| log \|delta\|) | non-leader install | **Optimized** (was O(N log N) ×3) |
| `sync()` non-leader | O(\|delta\| · log N) | non-leader install | **Optimized** (was O(N · log N)) |
| `sync()` leader | O(f·D · log D) | leader merge | **Optimized** |
| `build_start_view_payload` full | O(S) reads | leader, lagging followers only | Cold path, necessary |
| `list_files_reverse` | O(manifests) | read replica refresh (async) | Cold path, documented caveat |
