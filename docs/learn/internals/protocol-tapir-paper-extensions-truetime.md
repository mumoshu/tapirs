# TrueTime-Style Uncertainty Bound for RO Transaction Linearizability

## What is TrueTime?

Google Spanner's TrueTime API returns a time interval `[earliest, latest]`
that bounds the real time. Using GPS receivers and atomic clocks in every
data center, Spanner keeps the interval width (uncertainty ε) to a few
milliseconds. When a transaction commits, Spanner picks a commit timestamp
within the interval and then **commit-waits** until real time has passed
the commit timestamp. This ensures that any later reader whose timestamp
is above the interval's upper bound is guaranteed to see the write.

## How TAPIR uses a TrueTime-like mechanism

TAPIR does not assume TrueTime hardware. Instead, operators configure a
`clock_skew_uncertainty_bound` (ε) that must be ≥ the maximum clock skew
(δ) between any two nodes in the cluster. This is a static bound — if NTP
keeps clocks within 10 ms of each other, set ε = 10 ms.

When a client calls `begin_read_only(ε)`:

1. **Snapshot adjustment**: `snapshot_ts = local_time + ε`. This ensures
   `snapshot_ts ≥ commit_ts` for every write that completed before the RO
   transaction began, even if the writing node's clock was ahead by up to δ.

2. **Commit-wait**: Before issuing `quorum_read` / `quorum_scan`, the
   client sleeps for ε (minus any time already spent on the fast-path
   delay). This ensures that any concurrent write whose
   `commit_ts ≤ snapshot_ts` has either completed (visible to the quorum)
   or aborted by the time the read executes.

Together these guarantee linearizable reads: the RO transaction observes
every write that completed before it began, and no write that started
after.

Note the analogy to Spanner: Spanner applies the wait at **commit** time
(the writer waits). TAPIR applies the wait at **read** time (the reader
waits). The effect is the same — a timestamp-ordered separation between
the writer's commit and the reader's snapshot.

## Timeline: linearizability violation without the uncertainty bound

Consider two nodes A and B with clock skew δ (B's clock is ahead):

```
Real time:    t1              t2
              |               |
Node B clock: t1+δ            t2+δ
Node A clock: t1              t2

[1] Client C1 writes key X via Node B at real time t1:
    commit_ts = t1+δ   (B's clock)
    Write completes, C1 receives ack.

[2] Client C2 begins RO txn on Node A at real time t2 (after C1's ack):
    snapshot_ts = t2   (A's clock)

    If δ > t2 − t1, then: t2 < t1+δ, i.e., snapshot_ts < commit_ts

    quorum_read(X, snapshot_ts=t2) reads from MVCC at snapshot_ts,
    which is below commit_ts → returns the version BEFORE C1's write.

    → C2 misses a write that completed before it began
    → LINEARIZABILITY VIOLATION
```

The core issue: TAPIR RW transactions pick `commit_ts` from the
**proposing replica's** local clock (via `transport.time()`). Under clock
skew, different replicas assign timestamps from different clock domains.
An RO transaction on a slower-clock node may pick a `snapshot_ts` that
falls below a `commit_ts` assigned by a faster-clock node, even though
the write completed earlier in real time.

## How the uncertainty bound fixes it

```
[2'] Client C2 begins RO txn on Node A with ε ≥ δ:
     snapshot_ts = t2 + ε   (A's clock + uncertainty bound)

     Now: t2 + ε ≥ t2 + δ > t1 + δ = commit_ts

     quorum_read(X, snapshot_ts=t2+ε) sees C1's write ✓
```

But the snapshot adjustment alone is not sufficient. Consider a concurrent
write that starts at real time t2 on Node B:

```
     commit_ts_new = t2 + δ ≤ t2 + ε = snapshot_ts
```

Without waiting, `quorum_read` could observe this in-flight write — a
write that hasn't committed yet (or may abort). By sleeping for ε before
issuing the read, the client ensures that by real time `t2 + ε`:

- Any write with `commit_ts ≤ snapshot_ts = t2 + ε` was initiated at real
  time `≤ t2` (since `commit_ts = real_time + skew ≤ real_time + δ ≤
  real_time + ε`).
- A write initiated at real time `≤ t2` has had ε time to complete its
  2-phase OCC protocol. If it committed, it's visible to the quorum. If
  it aborted, it won't appear.

This is the same reasoning as Spanner's commit-wait: the wait creates a
real-time gap that separates "definitely committed" writes from
"possibly still in-flight" writes.

## What happens if ε is too low

If `ε < δ` (bound less than actual clock skew):

- `snapshot_ts = t2 + ε` may still be less than `t1 + δ` for some writes.
  The snapshot adjustment fails to cover the full skew range.
- The wait mitigates propagation delay but cannot fix the timestamp
  ordering — the fundamental issue is that `snapshot_ts` is too low to
  observe all prior writes.
- Result: linearizability violation — reads miss writes that completed
  before the RO transaction began.

Operators should set ε conservatively (e.g., 2× the observed NTP skew)
to account for transient clock drift spikes.

## Why the wait is necessary (not just the snapshot adjustment)

The snapshot adjustment ensures `snapshot_ts ≥ commit_ts` for prior
writes. But it also means `snapshot_ts` may exceed the `commit_ts` of
**concurrent** writes that are still in-flight:

- A write starting NOW on a fast-clock node gets
  `commit_ts = now + δ ≤ now + ε = snapshot_ts`.
- Without waiting, `quorum_read` might see a partially-committed write
  (prepare succeeded at some replicas but FINALIZE hasn't completed).
- Worse, the write might abort, but the read already observed its
  tentative value.

The wait ensures all writes with `commit_ts ≤ snapshot_ts` have had
enough real time to finish their protocol. After the wait, the quorum
state reflects only completed (committed or aborted) transactions.

## Relationship to the TAPIR paper

Paper Section 6.1 states:

> "this read-only protocol would provide linearizability guarantees only
> if the clock skew did not exceed the TrueTime bound."

Our implementation makes this explicit: `begin_read_only()` requires the
bound as a mandatory `Duration` parameter. Passing `Duration::ZERO`
explicitly documents the assumption that clocks are perfectly
synchronized (e.g., in-process channel transport in tests).

## RO slow path vs fast path

The uncertainty bound applies to the **slow path** (quorum read). The
**fast path** (`read_validated` / `scan_validated`) reads from a single
replica and relies on view-change sync to propagate committed values. The
fast-path delay and uncertainty wait interact:

- If both fast-path delay and uncertainty bound are set, the uncertainty
  wait subtracts any time already spent on the fast-path delay.
- The fast path is an optimization for bandwidth (1 replica vs f+1), not
  a correctness mechanism — it falls back to the slow path on cache miss.

See `set_ro_fast_path_delay()` documentation for fast-path linearizability
requirements and trade-offs.
