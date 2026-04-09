# DoViewChange Full Payload Is Unused — Analysis

## Two distinct payload paths: DVC vs StartView

Sealed segments travel on the wire in two separate protocol messages:

| Message | Direction | When sealed segments are needed |
|---------|-----------|-------------------------------|
| **DoViewChange** | replica → new leader | **Never** (this doc's finding) |
| **StartView** | leader → all replicas | **Yes** — lagging followers need sealed segments to catch up |

StartView full payloads (sealed segments + memtable) are essential: after the
leader completes the merge, it sends StartView to all replicas. Followers whose
LNV doesn't match the leader's previous base_view receive a full StartView
containing all sealed segments via `install_start_view_payload`. This is a
completely separate code path from the leader merge and must be preserved.

This doc analyzes only the **DoViewChange** direction.

## Summary

The full DoViewChange payload (sealed segments + memtable) is never consumed
by the leader merge. Only the memtable portion and LNV metadata matter. The
sealed segments are dead weight on the wire, costing O(N) bytes per DVC message
when `peers_confirmed` is false.

## How DVC payload is chosen

`broadcast_do_view_change` at `src/ir/replica.rs:317-329`:

```
peers_confirmed = (LNV + 1 == target_view)
    && all peers reported peer_normal_view >= LNV

if peers_confirmed:
    build_view_change_payload()    → delta (memtable only, O(D))
else:
    build_full_view_change_payload()  → all sealed segments + memtable, O(N)
```

Full payload is sent when:
1. **View gap**: `LNV + 1 != target_view` (skipped views)
2. **Unconfirmed peers**: not all peers reported matching LNV via status broadcasts

## Why sealed segments in full DVC are never consumed

### Filter 1: Leader rejection guard (`replica.rs:561-571`)

A leader whose `latest_normal_view < majority_LNV` is rejected:

```rust
if sync.latest_normal_view.number < latest_normal_view.number {
    warn!("leader behind majority: ..., aborting merge");
    return None;
}
```

This guarantees the leader's LNV >= majority LNV. Combined with the majority
being the max LNV across f+1 DVCs, the leader's LNV == majority LNV.

### Filter 2: Peer LNV matching (`replica.rs:592-596`)

Only peers whose DVC `latest_normal_view == majority_LNV` contribute to merge:

```rust
let peer_payloads: Vec<_> = matching
    .filter(|(addr, r)| {
        **addr != my_address
            && r.addendum...latest_normal_view.number == latest_normal_view.number
    })
    .map(|(_, r)| &r.addendum...payload)
    .collect();
```

Peers with lower LNV are excluded entirely — their payload (full or delta) is
never read.

### Filter 3: best_payload LNV matching (`replica.rs:735-739`)

The best payload for `install_merged_record` segment import also filters by
matching LNV:

```rust
let best_payload: Option<U::Payload> = matching
    .filter(|(_, r)| r.addendum...latest_normal_view.number == latest_normal_view.number)
    .map(...)
    .next();
```

### Combined effect

All replicas that participate in the merge (leader + matching peers) have the
same LNV. Sealed segments are the output of view changes up through that LNV.
Since all participants went through the same view changes, their sealed segments
are identical. Therefore:

- **Memtable extraction** (`as_memtable_record`): only current-view entries,
  which differ across replicas — this is what needs merging.
- **Sealed segment extraction** (`as_record_since(base_view)`): always empty,
  because peers with matching LNV have the same base_view as the leader.
- **Segment import in `install_merged_record`**: `max_view > base_view` is
  always false for segments from a peer with matching LNV — nothing imported.

## Concrete scenario: A+B+C cluster, C fails during view N

1. View N starts with A+C (B was down). A and C seal view N entries.
2. C fails during view N.
3. A triggers view change to N+1.
4. Leader for N+1 = `membership[(N+1) % 3]` — deterministic round-robin.

**Case: B is leader for N+1**

- B has LNV < N (missed view N).
- B receives DVC from A (LNV=N). Majority LNV = N.
- B's LNV < majority → **guard rejects B** (filter 1).
- View change fails. Tick retries with N+2, N+3, etc.

**Case: A is leader for N+1 (or eventually via rotation)**

- A has LNV = N. Majority LNV = N. Guard passes.
- B's LNV < N → **B excluded from peer_payloads** (filter 2).
- Only A's own memtable contributes to R. No peers with matching LNV.
- B's full payload (with all its sealed segments) is **completely ignored**.
- `best_payload` also excludes B (filter 3) → segment import is a no-op.

**Case: B confirmed and has matching LNV**

This can only happen if B participated in the same view N as A — meaning B
sealed the same segments. B's sealed segments are identical to A's → no new
data to extract.

## What about `install_merged_record` segment import?

`install_merged_record` step 1 imports segments from `best_payload` where
`max_view > self.base_view`. Since best_payload comes from a peer with
matching LNV (same sealed history), all its segments have `max_view <=
self.base_view`. Import is always a no-op.

This code path was originally designed for a protocol where the leader might
reconstruct state from a more-advanced peer. The LNV matching filter and
leader rejection guard prevent that scenario.

## No durability or availability benefit from lagging replicas' DVC data

A replica that didn't participate in view N has no memtable entries from
view N — and its sealed segments are redundant. Here's why:

### Clients only send to current view membership

`ir/client.rs:395` — `invoke_inconsistent` sends `ProposeInconsistent` to
`sync.view.membership.iter()`. The client only contacts replicas in its
current view. A downed replica never receives proposals.

### Replicas reject proposals when not Normal

`ir/replica.rs:361` — `ProposeInconsistent` is only processed when
`sync.status.is_normal()`. A replica that comes back from a crash enters
`ViewChanging` status and rejects new proposals until it completes a view
change.

### Combined effect on replica B (missed view N)

1. B was down during view N → clients never sent proposals to B → **B's
   memtable has zero entries from view N**.
2. B's memtable contains entries from its last Normal view (< N). Those
   entries were already sealed by the view change that transitioned into
   view N — they exist in A's and C's sealed segments.
3. B's sealed segments are from views < N — a strict subset of what A
   already has.

**Therefore:** B's DVC payload (whether full or delta) contains no data
that isn't already available on the f+1 replicas that participated in
view N. Including B provides zero durability benefit (no unique entries)
and zero availability benefit (the quorum already has everything).

This is why the LNV filter (excluding B from `peer_payloads`) is not just
an optimization — it's semantically correct. B literally has nothing to
contribute to the merge.

### Edge case: stale client sends proposal to partitioned replica B

A client with stale view info can send proposals to B even if B didn't
participate in the current view. This is worth analyzing because B ends
up with memtable entries that A and C also have.

**Setup:** View N is active on A+C. B is stuck in Normal status at old
view V-2 (it was partitioned, not crashed — so it never entered
ViewChanging).

**What happens:**

1. Client (at view V, within 3 of B's view V-2) sends `ProposeInconsistent`
   to all of A, B, C.

2. **B accepts** — `ir/replica.rs:361-383`: the replica only checks
   `sync.status.is_normal()` (true for B) and `recent.is_recent_relative_to`
   (true if client's V is within 3 of B's V-2). There is no membership check.
   B inserts the entry into its memtable as Tentative.

3. **A and C also accept** — they're in view N, Normal. If client's V is
   within 3 of N, the proposal is accepted.

4. **Client forms quorum from view N** — `ir/client.rs:439`:
   `has_quorum(membership_size, &results, true)` requires f+1 replies from
   the **same view**. A replies with view N, C replies with view N → quorum
   of 2 from view N. B replies with view V-2 → doesn't count. The txn is
   confirmed via the view-N quorum.

5. **B's entry doesn't count** — B's reply is from a different view and is
   ignored for quorum. The entry exists in B's memtable but has no effect
   on the protocol outcome.

**During next view change (N → N+1):**

- B's DVC carries its memtable (including the stale entry). But B's LNV
  (V-2) < majority LNV (N) → **B is excluded from `peer_payloads`** → the
  entry is discarded from the merge.
- The entry already exists in A's and C's memtables (they accepted it in
  view N) → no durability loss.

**How B's stale state is corrected:**

After the leader completes the merge for view N+1, it sends StartView to
all replicas including B. B receives the full StartView payload (because
B's LNV doesn't match the leader's previous base_view) and calls
`install_start_view_payload` (`ir_record_store.rs`). This:

1. Imports all sealed segments from the payload that B is missing
   (everything from views B didn't participate in).
2. Builds `previous_record` (B's old state), `transition` (the delta),
   and `new_record` (the leader's merged state).
3. Calls `sync(previous_record, new_record)` on the TAPIR upcall —
   this iterates the leader's new_record and applies any entries B
   hasn't processed. For entries B already has (like the stale client's
   proposal), `sync` skips them if they're already finalized with the
   same result, or overwrites them with the leader's authoritative
   version.
4. Clears B's memtable and advances B's base_view to match the leader.

After this, B's state is fully synchronized with the leader's merged
record. The stale entry in B's memtable is gone — replaced by the
leader's authoritative sealed version.

**Conclusion:** Stale clients can cause B to accept proposals, but this
has no effect on correctness or durability. The quorum is formed from
view-N replicas only, and B's state is overwritten by StartView during
the next view change.

## Sealed segments ARE needed in StartView (not DVC)

As noted in the table above, sealed segments on the wire serve a real purpose
in the **StartView** direction (leader → followers), not the DoViewChange
direction (replicas → leader). After the leader completes the merge, lagging
followers (whose LNV doesn't match the leader's previous base_view) receive a
full StartView payload containing all sealed segments. This is handled by
`install_start_view_payload` on the recipient side — a completely separate
code path from the leader merge. Removing full DVC payloads does not affect
StartView payloads.

## Next Action: Remove full payload from DoViewChange only

Replace `build_full_view_change_payload()` with `build_view_change_payload()`
in `broadcast_do_view_change`. DVC payloads only need memtable entries + LNV
metadata. **StartView full payloads are unaffected and must be preserved.**

### Expected savings

- **Wire**: O(N) → O(D) per DVC message when `peers_confirmed` is false.
  With N growing without bound (observation #3 in view-change-data-complexity),
  this becomes significant over time.
- **Sender CPU**: eliminates `all_segment_bytes_with_views()` disk reads on
  the DVC sender path.
- **Code simplification**: `build_full_view_change_payload()` can be removed
  from the `IrRecordStore` trait (its only DVC caller is
  `broadcast_do_view_change`). The `peers_confirmed` check and
  `peer_normal_views` tracking in `broadcast_do_view_change` become
  unnecessary. `as_memtable_record()` filter logic for full payloads (empty
  ViewRange detection) becomes unnecessary since DVC payloads are always delta.

### What to keep (StartView path)

- `build_start_view_payload()` with full payloads — **essential** for lagging
  followers who need all sealed segments to reconstruct state.
- `PersistentPayload::full()` constructor — still used by
  `build_start_view_payload`. Only the DVC call site is removed.

Note: The StartView recipient path (`install_start_view_payload`) does NOT
use `as_record_since()` or `as_memtable_record()`. It has its own segment
partitioning logic that splits payload segments by `max_view <= base_view`
into existing (skip) vs new (import), then builds three records
(previous_record, transition, new_record) for sync. This is independent of
the IrPayload filtering methods added for DVC optimization.

### Risk assessment

Low risk. The sealed segments in DVC payloads are provably never consumed
(three independent filters prevent it). The only behavioral change is that
DVC messages become smaller. No correctness impact since the data was already
being discarded by the leader.

### Related dead code to clean up

- `missed_sealed_records` in the leader merge (`replica.rs`) — always empty.
  Can be removed along with `as_record_since` calls on DVC payloads.
- `as_record_since()` on `IrPayload` — only caller is `missed_sealed_records`
  which is always empty. Can be removed.
- `as_memtable_record()` on `IrPayload` — with DVC always delta, all DVC
  payloads are entirely memtable. `payload_as_record()` (= `as_unresolved_record()`)
  already returns the right thing for delta payloads. The full-payload
  filtering logic (empty ViewRange detection) becomes dead code.
- `install_merged_record` segment import from `best_payload` — always a no-op
  when all peers have matching LNV. Could be removed or guarded with a
  debug_assert.
- `build_full_view_change_payload()` on `IrRecordStore` trait — after removing
  the DVC full path, no DVC callers remain. StartView uses its own builder
  (`build_start_view_payload`) which calls `all_segment_bytes_with_views()`
  directly.
- `peers_confirmed` check and `peer_normal_views` tracking in
  `broadcast_do_view_change` — no longer needed if DVC is always delta.
