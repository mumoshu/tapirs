# IR/TAPIR Implementation Gotchas

A practitioner's checklist of easy-to-miss invariants when implementing or extending IR and TAPIR. Each gotcha has four parts: what the paper says, why it matters for any implementation, what breaks if violated, and how tapirs handles it correctly.

Organized by layer: [IR Protocol](#ir-protocol-gotchas) → [TAPIR Protocol](#tapir-protocol-gotchas) → [tapirs Extensions](#tapirs-extension-gotchas).

---

## IR Protocol Gotchas

### 1. Fresh op_id per retry

**What the paper says:** IR client state includes "client id" and "operation counter" (Fig. 3, §3.2.1). Each operation has a unique `id` composed of `(client_id, operation_counter)`. The paper's step 1 for both inconsistent and consensus ops says "The client sends ⟨PROPOSE, id, op⟩" — `id` identifies one logical operation attempt.

**General implication:** The `id` is the key into the replica's record (an unordered set). If a client reuses the same `id` for a retry, the replica's record already has an entry for that `id` — potentially with different state from a view-change merge. The new proposal must match the existing entry or the record becomes inconsistent. Since view-change merge can change which `op` is associated with an `id` (via IR-MERGE-RECORDS Fig. 5), a retried proposal with the same `id` but different `op` violates the record's uniqueness invariant.

**What breaks if violated:** After a view change, the merged record may contain `(id, op_A)` from a different replica's proposal. Client retries with `(id, op_B)`. The replica finds `id` occupied with `op_A ≠ op_B` → in tapirs, `debug_assert_eq!(occupied.op, op)` panics (`ir/replica.rs:418` for inconsistent, `:442` for consensus). In release builds, the client silently gets the wrong result.

**How tapirs handles it:** All three `invoke_*` methods generate a fresh `op_id` inside the retry loop by calling `sync.next_number()`, which monotonically increments `operation_counter`. Source: `ir/client.rs:355-362` (invoke_inconsistent), `:476-478` (invoke_inconsistent_with_result), `:666-669` (invoke_consensus). The assertion at `ir/replica.rs:418,442` catches violations in debug builds.

### 2. Operation counter must never reset on membership change

**What the paper says:** §3.2.3 (Client Recovery): "the recovering client requests the id for its latest operation from a majority of the replicas. This poll gets the client the largest id that the group has seen from it, so the client takes the largest returned id and increments it to use as its new operation counter." The counter must only go forward.

**General implication:** If the counter is reset to 0 (e.g., by creating a new client instance when membership changes), newly generated `(client_id, 0)`, `(client_id, 1)`, ... collide with `(client_id, 0)`, `(client_id, 1)`, ... already in the replica record from before the reset. The same collision as gotcha #1 occurs.

**What breaks if violated:** Replicas already have entries for the old op_ids. New proposals with colliding op_ids trigger `debug_assert_eq!` panics or, in release, silently return stale results from the old operations.

**How tapirs handles it:** `IrClient::reset_membership()` replaces the membership and resets the view to 0 but **does not reset** `operation_counter`. Source: `ir/client.rs:112-124`. `DnsRefreshingShardClient` calls `reset_membership()` instead of creating a new `ShardClient`. Source: `tapir/dns_shard_client.rs:85-92`.

### 3. Replicas are silent during ViewChanging

**What the paper says:** §3.2.2 step 1: "A replica that notices the need for a view change advances its view number and sets its status to VIEW-CHANGING." §3.2.2 final paragraph: "A replica can only process requests in the new view (in the NORMAL state) after it completes the view change protocol." Replicas in VIEW-CHANGING do not respond to client proposals.

**General implication:** This is correct protocol behavior, not a bug. A replica in VIEW-CHANGING cannot safely accept new proposals because its record is being merged/synced. If it accepted proposals, those entries could be lost when the view-change leader overwrites the record with the merged master record (§3.2.2 step 5).

**What breaks if "fixed":** If you add special handling to make replicas respond during ViewChanging, newly accepted entries may be silently overwritten by START-VIEW. The client believes the operation succeeded (got f+1 replies from a mix of Normal and ViewChanging replicas), but the ViewChanging replicas' entries are erased. Durability property P1 is violated.

**How tapirs handles it:** Every message handler in `ir/replica.rs:390-462` checks `sync.status.is_normal()` and returns `None` (no reply) if ViewChanging. Clients handle the silence via retry with exponential backoff in the transport layer (`ir/client.rs:421-425`), plus `update_view()` to help lagging replicas catch up (`ir/client.rs:133-160`).

### 4. All retry paths need exponential backoff

**What the paper says:** §3.2.1: "If the IR client does not receive a response to its PREPARE from f+1 replicas, it will retry until it does." The paper does not specify backoff, but it assumes an asynchronous network where "messages that are repeatedly resent are eventually delivered before the recipients time out" (§3.1.2).

**General implication:** A naive tight retry loop is correct in theory (eventually succeeds), but in practice it can starve other tasks. Under cooperative scheduling or simulated time, a tight loop prevents any other task from making progress — including the view-change protocol that would unblock the retries.

**What breaks if violated:** Under `tokio::time::start_paused` (simulated time), a tight async retry loop never yields long enough for simulated time to advance. Timeouts that trigger view changes never fire. The system deadlocks: the client retries forever waiting for replies, replicas stay ViewChanging waiting for timeout-driven view change completion.

**How tapirs handles it:** All retry paths use exponential backoff (50ms → 1s, 2x multiplier). `invoke_inconsistent`: `ir/client.rs:353,421-425`. `invoke_inconsistent_with_result` has TWO retry paths — Phase 1 (propose) and Phase 2 (finalize) — both with backoff: `ir/client.rs:566-567,605-606`. `invoke_consensus`: `ir/client.rs:659,792-793`.

### 5. FinalizeInconsistent must respond for already-Finalized entries

**What the paper says:** §3.2.1 step 4: "On FINALIZE, replicas upcall into the application protocol with ExecInconsistent(op) and mark op as FINALIZED." The paper doesn't explicitly address what happens if the entry is already FINALIZED, but the view-change merge (§3.2.2, IR-MERGE-RECORDS Fig. 5) can finalize TENTATIVE entries as part of the Sync/merge process.

**General implication:** Between a client's Phase 1 (Propose, gets f+1 TENTATIVE replies) and Phase 2 (Finalize), a view change may occur. The view-change merge finalizes the entry at all replicas. When the client's Finalize arrives, the entry is already FINALIZED. If the handler skips already-Finalized entries without replying, the client never gets the response it needs.

**What breaks if violated:** `invoke_inconsistent_with_result` (used by QuorumRead) waits for f+1 Finalize replies. If replicas skip already-Finalized entries, the client gets fewer than f+1 replies → times out → retries indefinitely.

**How tapirs handles it:** The FinalizeInconsistent handler executes and replies regardless of whether the entry was just finalized or was already finalized. `exec_inconsistent` is idempotent for QuorumRead's `commit_get`. Source: `ir/replica.rs:463-484` — the comment at line 471-475 explicitly documents this.

### 6. Finalize is async (fire-and-forget)

**What the paper says:** §3.2.1 step 3: "Once the client receives f+1 responses from replicas (retrying if necessary), it returns to the application protocol and asynchronously sends ⟨FINALIZE, id⟩ to all replicas." The paper also states: "(FINALIZE can also be piggy-backed on the client's next message.)" §5.2: "TAPIR can return the outcome of the transaction to the application as soon as Prepare returns from all shards and send the Commit operations asynchronously." §5.3.3 (Durability): "On Commit, TAPIR replicas use the transaction timestamp included in Commit to order the transaction in their log, regardless of when they execute it, thus maintaining the original linearizable ordering."

**General implication:** Durability is guaranteed at Propose time — f+1 replicas have the IO::Commit in their record as TENTATIVE, protected by Property P1. But the write is not yet *visible* in the MVCC store: that happens when Finalize triggers `ExecInconsistent(IO::Commit)`, which applies writes and removes the transaction from the prepared list. The Commit is ordered by its transaction timestamp "regardless of when they execute it" (§5.3.3), so linearizable ordering is maintained even with deferred execution.

Subsequent transactions see the committed write correctly:

**RW→RW:** Safe via OCC. If a second RW transaction reads the key before the first's Finalize arrives, the unlogged read returns the old version. But at Prepare time, TAPIR-OCC-CHECK (Fig. 9) finds the first transaction still in `prepared_writes` (the Prepare was finalized, adding the write; the IO::Commit hasn't removed it yet). The check `writes.range((Excluded(read), Excluded(commit)))` detects the conflict → `PrepareResult::Abstain` → the second transaction retries.

**RW→RO:** Safe via QuorumRead (§6.1), *provided* the Commit Finalize arrives at the overlapping replica before the QuorumRead executes. QuorumRead is itself an IR inconsistent operation that takes at least one round trip — in tapirs it uses `invoke_inconsistent_with_result`, which sends Propose (f+1 replies) then sends Finalize and *waits for f+1 Finalize replies* before returning results. The execution (`get_at` + `commit_get`) happens at Finalize time at each replica. The quorum intersection argument (any two sets of f+1 out of 2f+1 overlap by at least 1) guarantees that at least one replica in the QuorumRead quorum also received the Commit. The paper provides piggybacking ("FINALIZE can also be piggy-backed on the client's next message") as the mechanism to ensure the Commit Finalize arrives at the overlapping replica before the next operation. With piggybacking, the overlapping replica executes the Commit before the QuorumRead — the write is in the MVCC store. The paper's §6.1 correctness sketch: "the client gets the latest version of the object (because at least 1 of any f+1 replicas must have it)."

**Cross-client RO:** The same quorum intersection argument applies — the Commit Finalize is sent before the cross-client QuorumRead starts, and the overlapping replica executes the Commit before executing the QuorumRead. Separately, §6.1 notes that TAPIR's RO protocol could optionally be combined with Spanner's TrueTime algorithm for externally consistent reads (requiring TrueTime waits at the client). These are two independent options: TAPIR's §6.1 provides serializable RO transactions; the Spanner combination adds external consistency.

**Concurrent QuorumRead with a prepared-but-uncommitted write:** If a key has a prepared write at ts1 (from an RW transaction's CO::Prepare, accepted by f+1 replicas as PREPARE-OK) but the write has not been committed yet (IO::Commit not sent — either the client is slow or has crashed), an RO QuorumRead at ts2 > ts1 reads from the MVCC store and returns V_old (the pre-ts1 value). This is correct: QuorumRead reads committed versions only (`occ/store.rs:222-223`, `get_at` on the MVCC backend). The prepared write is in the `prepared_writes` set, not in the MVCC store.

The QuorumRead's `commit_get` sets read_ts = ts2 at f+1 replicas. This read_ts blocks **future** Prepares — any new CO::Prepare with a write to this key at commit_ts < ts2 will hit the OCC write-set check: `last_read (=ts2) > commit → Retry{proposed: ts2.time()}` (`occ/store.rs:380-382`). But the already-accepted Prepare at ts1 is not retroactively invalidated — it passed its OCC check before the QuorumRead set the read_ts.

**What about truly concurrent races (QuorumRead and Prepare interleaved at replicas)?** The paper's ABSTAIN/RETRY mechanism (§5.2.4) resolves this at the IR consensus level. The Prepare is an IR consensus operation: each replica runs OCC-CHECK independently and proposes a result. If a replica processes the QuorumRead before the Prepare, it sees read_ts = ts2 > ts1 → returns `Retry`. If it processes the Prepare first, OCC passes → returns `Ok`. The consensus decide function (`tapir/shard_client.rs:99-146`) aggregates: f+1 `Ok` → commit at ts1; `Retry` results → client retries at ts2+. The race is resolved by the quorum — whichever processing order dominates at f+1 replicas determines the outcome.

**Backup coordinator recovery (client crash after f+1 PREPARE-OK):** The backup coordinator (`tapir/replica.rs:137-273`) uses `CheckPrepare` — an unlogged request that checks the **prepared list and transaction log**, not OCC read timestamps (`tapir/replica.rs:340-380`). Replicas with the original Prepare return `Ok` (line 360-362: prepared + finalized → Ok); replicas without it return `TooLate` (line 367-376: `commit.time < min_prepare_time` after `raise_min_prepare_time`). With f+1 `Ok`, the backup coordinator commits. It cannot abort — TAPIR-RECOVERY-DECIDE (Fig. 13) requires f+1 NO-VOTE, but with f+1 PREPARE-OK at most f replicas can vote otherwise. The `prepared=committed` assumption is preserved: the QuorumRead's read_ts does not block the recovery path.

**Why doesn't QuorumRead read prepared values?** Because prepared ≠ must commit. A transaction prepared at f+1 replicas at one shard can still be aborted: (a) another shard in the same cross-shard transaction returns ABORT → client aborts at all shards, (b) a view-change-triggered re-prepare reveals new conflicts, (c) the client aborts for application reasons. If QuorumRead read from the prepared list, it could return data from a transaction that is later aborted — a dirty read. The paper's §5.2.4 ABSTAIN/RETRY distinction makes this explicit: ABSTAIN means conflict with a **prepared** transaction "which may later abort," while ABORT means conflict with a **committed** transaction "which will not abort." QuorumRead reads only from the MVCC store (committed versions) to guarantee the returned value is from a durably committed transaction.

**The RO transaction is not rejected** — read-only transactions have no Prepare phase. They read from the MVCC store and return. The QuorumRead correctly returned the latest committed version (V_old). The prepared write at ts1 was not committed when the QuorumRead executed.

**How Commit and QuorumRead interact (§6.1 properties (1) and (2)):** Both Commit and QuorumRead are IR inconsistent operations that execute independently at each replica (no direct IR-level conflict). The mechanism that makes them work together involves three parts:

**(1) Quorum intersection + "pick highest timestamp" (§6.1 step 6):** The Commit's Propose goes to f+1 replicas. The QuorumRead's `invoke_inconsistent_with_result` also goes to f+1 replicas. Any two sets of f+1 out of 2f+1 overlap by at least 1. At the overlapping replica, both operations execute at Finalize time: if the Commit Finalize arrives first, `exec_inconsistent(IO::Commit)` applies V_new at ts1 to MVCC, and the subsequent `exec_inconsistent(IO::QuorumRead)` reads V_new via `get_at(key, ts2)` (returns highest version at ≤ ts2, which is now V_new at ts1). The client collects f+1 QuorumRead results and picks the one with the **highest write_ts** (`shard_client.rs:241`): even if only one replica has V_new, the client returns V_new. This implements §6.1's "(1) the client gets the latest version of the object (because at least 1 of any f+1 replicas must have it)."

**(2) read_ts blocks future writes:** The QuorumRead's `commit_get(key, ts2, ts2)` sets read_ts = ts2 at ALL f+1 QuorumRead replicas (`occ/store.rs:224`). Any **new** CO::Prepare with a write to this key at commit_ts < ts2 hits the OCC write-set check: `last_read (=ts2) > commit → Retry{proposed: ts2.time()}` (`occ/store.rs:380-382`). The write must retry at ts3 > ts2. This implements §6.1's "(2) later write transactions do not invalidate the reads" — "later" means writes whose Prepare arrives after the read_ts is set.

**(3) ABSTAIN/RETRY for concurrent races:** If a Prepare and QuorumRead are truly concurrent at individual replicas (neither arrived before the other at all replicas), the paper's §5.2.4 mechanism resolves the race during the IR consensus for the Prepare: replicas where QuorumRead executed first return `Retry` (read_ts > commit_ts), replicas where Prepare executed first return `Ok`. The consensus decide function (`shard_client.rs:99-146`) aggregates: f+1 `Ok` → commit at ts1; `Retry` → client retries at ts2+.

For an already-accepted Prepare (f+1 PREPARE-OK before QuorumRead): the Prepare's OCC check passed before the read_ts existed. The IR consensus result is finalized — the read_ts does not retroactively invalidate it. The Commit executes unconditionally (`exec_inconsistent(IO::Commit)` at `tapir/replica.rs:449-468` — no OCC re-check). The write appears in MVCC, and the QuorumRead's "pick highest timestamp" mechanism sees it at the overlapping replica (if the Commit Finalize arrived first). If the Commit hasn't arrived at any QuorumRead replica (e.g., backup coordinator hasn't sent it yet), the QuorumRead correctly returns V_old — the write is not yet committed.

**What breaks if violated:** TAPIR is linearizable — if `commit()` returns (operation A completes) and a subsequent read starts (operation B), B must see A's writes. Seeing the pre-commit value IS a linearizability violation. The paper provides piggybacking as the mechanism to prevent this (§3.2.1 step 3): "FINALIZE can also be piggy-backed on the client's next message." By piggybacking the Commit Finalize on the next operation's Propose (e.g., a QuorumRead), the replica is guaranteed to receive and process the Finalize before the new operation — ensuring it sees the committed value.

Without piggybacking, the Finalize is sent as a separate fire-and-forget message. There is no ordering guarantee between this message and the client's next operation to the same replica. In practice on real networks, the Finalize typically arrives first because it was sent first and the next operation requires at least another round trip. But under adversarial scheduling (including simulated time with `start_paused = true`), the Finalize task may not be scheduled before the next operation's messages are sent, causing a linearizability violation.

For RW unlogged reads (single replica, no quorum), the read is speculative and validated at commit time — OCC catches conflicts regardless of Finalize timing (txn1 remains in `prepared_writes` until IO::Commit is executed, so ABSTAIN blocks conflicting Prepares). The linearizability concern specifically affects RO QuorumRead, where the "pick highest timestamp" mechanism depends on at least one of f+1 replicas having executed the Commit Finalize.

**How tapirs handles it:** QuorumRead/QuorumScan check `prepared_writes` before reading from MVCC. If a prepared write exists at `commit_ts <= snapshot_ts`, `quorum_read()`/`quorum_scan()` returns `Err(PrepareConflict)`, which the replica maps to `IR::PrepareConflict`. The ShardClient retries with exponential backoff (50ms→1s, 2×). During backoff, the spawned IO::Commit Finalize task delivers the Commit — removing the prepared entry and applying the write to MVCC. On retry, QuorumRead succeeds with the correct committed value. This is the RW→RO direction of OCC conflict detection, symmetric with the existing RO→RW direction where `commit_get()` sets `read_ts` to block future Prepares. Tests still use `advance(1ms)` for timestamp ordering under `start_paused=true` (ensuring `snapshot_ts > commit_ts`), not as a Finalize propagation workaround. Piggybacking (as suggested by the paper) was considered but rejected: IR Finalize is fire-and-forget, so there is no ordering guarantee from the caller's perspective. Relying on message ordering contradicts the unreliable-ordering assumption that `FaultyChannelTransport` enforces. `invoke_inconsistent_with_result` (used by QuorumRead) waits for f+1 Finalize replies, so the returned result reflects executed state — the PrepareConflict check operates on the locally visible state at each replica. Source: `occ/store.rs` (quorum_read/quorum_scan), `tapir/replica.rs` (exec_inconsistent), `tapir/shard_client.rs` (retry loop).

### 7. View staleness check (ancient views)

**What the paper says:** §3.2.1: "All IR clients require that all responses come from replicas in the same view" (implied in §3.3, invariant I4). §3.2.2: View changes advance view numbers; replicas in lower views must join higher views before processing.

**General implication:** A client stuck at view V whose replicas have moved to V+4 or higher is severely lagging. Its proposals reference an obsolete membership/view. If accepted, responses from replicas at different views would be mixed, violating the "same view" requirement for quorum counting.

**What breaks if violated:** The client counts replies without realizing that different replicas are in different views. It may reach f+1 replies from a mix of old and new views, which doesn't constitute a valid quorum in any single view. The operation may appear to succeed but isn't durably replicated in the current view's record.

**How tapirs handles it:** Replicas reject proposals from clients >3 views behind by checking `recent.is_recent_relative_to(sync.view.number)` and returning a reply with `state: None` (signal to retry). Source: `ir/view.rs:9-10` (threshold logic), `ir/replica.rs:400-407` (inconsistent check), `:432-438` (consensus check). The client's `update_view()` uses the returned view to catch up: `ir/client.rs:133-160`.

### 8. TAPIR is leaderless — view.leader() only coordinates view changes

**What the paper says:** §3.2.2: "In VR, the leader is used to process operations in the normal case, but IR uses the leader only for performing view changes." §3.2.1: clients send PROPOSE "to all replicas" — not to a leader.

**General implication:** Unlike Raft/VR/Paxos, there is no leader for normal-case operations. Every replica independently processes Propose and Finalize. The `leader` role exists only during view changes — it collects DoViewChange messages, runs IR-MERGE-RECORDS, and sends START-VIEW. After the view change, the leader has no special role.

**What breaks if you route through a "leader":** If clients route all proposals to one replica (treating it as a leader), that replica becomes a bottleneck and single point of failure. Other replicas never see Propose messages, so they have empty records. View changes produce a master record with only the "leader's" entries. Durability and visibility are compromised.

**How tapirs handles it:** All `invoke_*` methods send Propose to ALL replicas in the membership. `invoke_unlogged` randomly selects one replica — for RW transaction reads, this single-replica read is safe because the read is validated at commit time: TAPIR-OCC-CHECK (Fig. 9) checks the read-set against committed and prepared writes. Specifically, `occ_check()` at `occ/store.rs:315-350` checks (a) whether a committed write has overwritten the read version (`beginning == read && end.is_some()` → `PrepareResult::Fail`), and (b) whether a prepared write in `(read, commit)` range could invalidate it (`prepared_writes` range check → `PrepareResult::Abstain`). A stale unlogged read that conflicts with a concurrent write will be caught by one of these checks, causing the transaction to abort and retry with a fresh timestamp. Source: `ir/client.rs:365-377` (inconsistent broadcasts to all), `:672-684` (consensus broadcasts to all), `:165-167` (unlogged selects random). `view.leader()` is only called during view-change coordination: `ir/replica.rs:1065-1090`.

---

## TAPIR Protocol Gotchas

### 9. RW get()/scan() are inconsistent — OCC validates at commit

**What the paper says:** §5.2 step 2: "Read: The client reads key from the nearest replica in the key's shard by making an unlogged request through IR." §4.4: "Read and Write are not replicated." The read is from a single replica, with no IR consensus or even IR inconsistent operation.

**General implication:** The unlogged read may hit a replica that is stale (hasn't received recent Finalize/Sync, or is in a minority partition). The value returned may be an old version. Correctness relies entirely on OCC validation at Prepare time (§5.2 step 5, TAPIR-OCC-CHECK Fig. 9): if a conflicting write committed between the read and the prepare, the read-set check catches it.

**What breaks if violated:** If you treat RW `get()` as a consistent read (no commit/validate), you get stale data with no error. For standalone reads needing consistency, using `begin()` + `get()` without `commit()` silently returns potentially stale values with no OCC check.

**How tapirs handles it:** `Transaction::get()` is documented as inconsistent (comment at `tapir/client.rs:131-143`). For consistent standalone reads, `begin_read_only()` provides a `ReadOnlyTransaction` with validated-read fast path and QuorumRead fallback (§6.1). Source: `tapir/client.rs:508-519` (begin_read_only), `tapir/shard_client.rs:187-220` (read_validated).

### 10. TAPIR transaction_id must be unique per transaction

**What the paper says:** §5.2: "The transaction id must be unique, so the client uses a tuple of its client id and transaction counter, similar to IR." (Fig. 7)

**General implication:** The transaction_id is used as the key in the prepared-list, transaction-log, and OCC conflict detection. Two transactions with the same id would share prepare state — one's PREPARE-OK could be mistaken for the other's, and Commit/Abort for one would affect the other.

**What breaks if violated:** If two transactions share a transaction_id: (a) the second Prepare sees the first's entry in prepared-list and returns PREPARE-OK without running OCC check (Fig. 8, line 8-9), even if the second transaction has a different read/write set; (b) Commit for one transaction commits the other's writes. Serializability is silently violated.

**How tapirs handles it:** `TapirClient::begin()` generates a unique `OccTransactionId { client_id, number }` where `number` is from `AtomicU64::fetch_add(1)` starting from a random seed. Source: `tapir/client.rs:114-118`. The `client_id` is the IR client ID (random u64), and the number increments atomically per transaction.

### 11. Don't aggressively remove prepared entries in sync()

**What the paper says:** TAPIR-MERGE Fig. 11 (and Fig. 14 with NO-VOTE): line 3-4 "if txn.id ∈ prepared-list: DELETE(prepared-list, txn.id)" — the **leader** removes its own prepared entries before merging, to clear inconsistencies from its own out-of-order execution. TAPIR-SYNC Fig. 12: replicas reconcile their TAPIR state with the master record. Crucially, sync only processes entries that ARE in the master record — it does not specify removing local entries that are ABSENT from the master record.

**General implication:** After a view change, the IR record at each replica is **replaced** by the leader's merged master record (§3.2.2 step 5). But the TAPIR prepared-list is separate TAPIR application state, updated by `sync()`. The merged IR record is built from f+1 replicas' records (IR-MERGE-RECORDS Fig. 5): minority-only CO::Prepare entries (present at < f+1 replicas, and not included in the f+1 records selected for merge) may not appear in the merged record at all. However, these minority Prepares may represent valid in-progress cross-shard transactions — the client may have already gotten PREPARE-OK from other shards and is about to send Commit.

**What breaks if violated:** If sync() removes local prepared entries absent from the leader's merged record: (1) The replica loses its PREPARE-OK vote for the transaction. (2) When the client sends IO::Commit (an IR inconsistent op), the replica executes `exec_inconsistent(IO::Commit)` but the transaction is no longer in `prepared` — the commit is a no-op at this replica (no writes applied to MVCC store). (3) Meanwhile, other shards in the cross-shard transaction DO commit successfully. Result: atomicity violation — the transaction is partially committed across shards. This was observed as fuzz test failures.

**How tapirs handles it:** The `sync()` implementation at `tapir/replica.rs:676-761` only iterates over `leader.consensus` and `leader.inconsistent` — it processes entries IN the leader's record (adding to or removing from prepared-list based on their result), but never scans local state to remove entries absent from the leader's record. Local minority Prepares survive the view change in the TAPIR prepared-list. They are eventually cleaned up via: (a) the client sends IO::Commit (idempotent — `exec_inconsistent` applies the write even if the CO::Prepare is gone from the IR record, as long as the prepared-list entry exists), (b) the client sends IO::Abort (removes from prepared-list), or (c) a future view-change merge includes the entry and the re-run OCC check determines its fate.

### 12. Coordinator recovery requires f+1 NO-VOTE to abort

**What the paper says:** §5.2.3, TAPIR-RECOVERY-DECIDE Fig. 13: "return ABORT if count(NO-VOTE, results) >= f+1". The normal TAPIR-DECIDE (Fig. 10) can abort on a single ABORT reply, but recovery cannot — "it is not safe to return ABORT unless it is sure the original coordinator did not receive PREPARE-OK."

**General implication:** The original coordinator may have received f+1 PREPARE-OK and already sent Commit (which is async/fire-and-forget). A backup coordinator that aborts on a single NO-VOTE could abort a transaction that was already committed by the original coordinator. Since IR Commit is inconsistent (executes in any order), some replicas may have applied the Commit while the backup coordinator decides to abort.

**What breaks if violated:** Atomicity violation — the original coordinator committed at some shards, but the backup coordinator aborts at others. Durability violation — a committed transaction is retroactively aborted.

**How tapirs handles it:** The recovery `decide` function requires f+1 NO-VOTE to abort, matching Paper Fig. 13. This ensures the backup coordinator only aborts when a majority of replicas confirm they haven't voted PREPARE-OK. Source: `tapir/replica.rs` coordinator recovery implementation.

---

## tapirs Extension Gotchas

These are pitfalls specific to our implementation's extensions beyond the original paper.

### 13. DnsRefreshingShardClient must not recreate ShardClient

**What the paper says:** N/A — DNS-based dynamic membership is not in the original paper.

**General implication:** In a deployment where replica IPs change (e.g., Kubernetes pod restarts), the client needs to update its membership. The naive approach — destroy the old ShardClient and create a new one — seems clean but is incorrect because it resets internal state.

**What breaks if violated:** Creating a new ShardClient creates a new IrClient with `operation_counter: 0`. The old op_ids `(client_id, 0..N)` are already in the replicas' records from before the reset. New proposals with colliding op_ids trigger the same `debug_assert_eq!` panic as gotcha #1. Additionally, if the new ShardClient gets a new random `client_id`, the old prepared transactions become orphaned (the new client can't Commit/Abort them).

**How tapirs handles it:** `DnsRefreshingShardClient` calls `inner.reset_membership(membership)` to update the membership in-place without resetting the counter. Source: `tapir/dns_shard_client.rs:85-92`, `ir/client.rs:112-124`.

### 14. read_validated needs bounded 2s timeout

**What the paper says:** §6.1: Read-only fast path sends "Read to only one replica. If that replica has a validated version... we know that the returned object is valid." Falls back to QuorumRead if the replica "lacks a validated version." The paper doesn't specify a timeout for the fast path.

**General implication:** The fast-path read goes to a single replica via `invoke_unlogged`. If that replica is ViewChanging, it won't respond (gotcha #3). The transport retries with backoff. Without an external timeout, the fast path blocks until the replica returns to Normal — which could be arbitrarily long if the view change is slow.

**What breaks if violated:** Under simulated time, the transport's retry backoff sleeps advance the clock, consuming the caller's timeout budget. A read-only transaction that should complete in milliseconds blocks for seconds or minutes. If there's an outer timeout (e.g., in a fuzz test), the transaction fails even though a quorum of healthy replicas is available.

**How tapirs handles it:** `read_validated()` wraps `invoke_unlogged()` in a 2-second `futures::future::select` timeout. On timeout, returns `Err(Unavailable)`, and the caller falls back to `quorum_read()` which uses IR inconsistent (f+1 replicas). Source: `tapir/shard_client.rs:187-220`.

### 15. Route fallback = serializability violation

**What the paper says:** §5.2: Each shard holds a partition of the data. Clients route keys to the correct shard. The paper assumes static partitioning — no resharding during a transaction.

**General implication:** During resharding, a key's ownership transitions from shard A to shard B. If the client routes to a "fallback" shard (e.g., shard B before it has fully assumed ownership), that shard has no OCC history for the key. OCC-CHECK at the wrong shard sees no conflicts because it has no prepared-reads, no prepared-writes, and no committed versions for the key.

**What breaks if violated:** A stale read passes OCC validation because the wrong shard reports no conflicts. The transaction commits with a read that was already invalidated by a write at the correct shard. Serializability is silently violated — no error, just wrong data.

**How tapirs handles it:** Multiple layers cooperate to prevent this:

1. **Routing layer**: `ShardRouter::route()` returns `Option<ShardNumber>` — `None` when the key isn't covered by any shard in the current directory. `None` triggers `Err(OutOfRange)` which causes a retry with directory refresh. The `on_out_of_range()` callback (source: `tapir/shard_router.rs:17`) lets the router invalidate its cache on mismatch.
2. **Replica layer**: Each replica checks its `key_range` on every operation (`tapir/replica.rs:292,309,317,385,396,401,531,543,548,572`). If the key is outside the replica's assigned range, it returns `UR::OutOfRange` or `CR::Prepare(OccPrepareResult::OutOfRange)`. This is the server-side safety net — even if the client routes to the wrong shard, the replica rejects the operation.
3. **Resharding layer**: During a shard split, the source shard transitions through phases: `ReadWrite` → `ReadOnly` (blocks new Prepares, lets existing prepared txns drain) → `Decommissioning` (also blocks QuorumRead/QuorumScan to freeze OCC state). Route changes are published as atomic changesets via `publish_route_changes()` (`[RemoveRange, SetRange]` in one changeset), and `CachingShardDirectory` polls `route_changes_since()` with a monotonic high watermark. This prevents any window where both the old and new shard accept writes for the same key range. Source: `tapir/replica.rs:24-39` (ShardPhase), `tapir/shard_manager_cdc.rs` (CDC drain).

### 16. In-place IR record compaction is unsafe

**What the paper says:** §3.1: The record is an "unordered set of operations and consensus results." The paper doesn't discuss record compaction. §5.2 step 7: Commit removes the transaction from the prepared-list, but the record entry (the IR-level CO::Prepare) is not explicitly removed.

**General implication:** The IR record grows unboundedly. A natural optimization is to remove entries for committed/aborted transactions. But CO::Prepare is the sole carrier of the write set (the full transaction's read/write data). IO::Commit carries only the transaction ID and timestamp — not the write set. If a lagging replica needs to sync, it needs the CO::Prepare entry to learn what was written.

**What breaks if violated:** (a) Removing CO::Prepare after commit: a lagging replica that missed the original Prepare runs `sync()` and receives the master record without the CO::Prepare. It can execute IO::Commit but doesn't know what to write — the data is lost. (b) Removing based on IO::Abort: a backup coordinator may later send IO::Commit for the same transaction (coordinator recovery), but the CO::Prepare is already gone.

**How tapirs handles it:** The `compact_merged_record` upcall exists but is a no-op — it must remain so. Record compaction only happens via CDC-based shard replacement, where the entire shard state is transferred (not individual record entries). Source: `ir/replica.rs` (compact_merged_record), `tapir/shard_manager_cdc.rs` (CDC shard replacement).

### 17. bitcode doesn't support skip_serializing_if

**What the paper says:** N/A — serialization format is an implementation choice.

**General implication:** bitcode is a compact binary serialization format chosen for wire efficiency. Unlike serde_json, bitcode does not support serde attributes `skip_serializing_if` and `default` — they cause a `NotSupported("skip_field")` error at runtime, not at compile time.

**What breaks if violated:** Adding `#[serde(skip_serializing_if = "Option::is_none", default)]` to a field compiles successfully but panics at runtime when the field is serialized/deserialized. This is particularly insidious because it only fails when that specific code path is exercised.

**How tapirs handles it:** No `skip_serializing_if` or `default` attributes on any bitcode-serialized types. `Arc<T>` serialization uses serde's `rc` feature. BTreeMap serialization uses a custom `vectorize_btree` helper. Source: serialization layer, Cargo.toml serde features.

### 18. Deterministic simulation requires three specific fixes

**What the paper says:** N/A — deterministic simulation is not in the paper.

**General implication:** Deterministic simulation testing (same seed → same execution → same result) is critical for reproducing rare bugs. In Rust with tokio, three sources of non-determinism can break reproducibility even with seeded RNG and `start_paused=true`.

**What breaks if violated:** (a) **JoinUntil partial drain**: `FuturesUnordered` may have multiple ready futures. If you check the `until` condition after each individual future, the set of collected results depends on poll order. Different runs collect different subsets → non-deterministic behavior. (b) **HashMap iteration order**: `HashMap` iteration is randomized per process. If view-change merge iterates `outstanding_do_view_changes` in different orders, the merged record differs between runs. (c) **Shared RNG in async context**: If `FaultyChannelTransport` samples from a shared RNG inside an async block, the poll order of `FuturesUnordered` determines which message gets which fault decision → non-deterministic faults.

**How tapirs handles it:** (a) `JoinUntil::poll` drains ALL ready futures from `FuturesUnordered` before checking the `until` condition. Source: `util/join.rs`. (b) `outstanding_do_view_changes` uses `BTreeMap` for deterministic iteration. Source: `ir/replica.rs`. (c) `FaultyChannelTransport` samples a per-message seed from the shared RNG at `send()`/`do_send()` creation time (deterministic sync context), then creates a per-message `StdRng` from that seed in the async block. Source: `transport/faulty_channel.rs`.
