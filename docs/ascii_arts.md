# tapirs — ASCII Art Guide

## Table of Contents

- [What is TAPIR?](#what-is-tapir)
- [Architecture Stack](#architecture-stack)
- [IR Consensus — Leaderless!](#ir-consensus--leaderless)
- [Two Types of IR Operations](#two-types-of-ir-operations)
- [Compare: TAPIR (2PC + IR) vs 2PC + Raft](#compare-tapir-2pc--ir-vs-2pc--raft)
- [TAPIR Transaction Lifecycle](#tapir-transaction-lifecycle)
- [OCC — Optimistic Concurrency Control](#occ--optimistic-concurrency-control)
- [Storage: WiscKey on Disk](#storage-wisckey-on-disk)
- [PersistentIrRecordStore — VlogLsm-backed IR Record](#persistentirrecordstore--vloglsm-backed-ir-record-tapirs-extension)
- [View Change: Delta Payloads](#view-change-delta-payloads)
- [View Change](#view-change)
- [Sharding & Routing](#sharding--routing)
- [Backup & Restore](#backup--restore)
- [Resharding — CDC Shard Splitting](#resharding--cdc-shard-splitting)
- [Transport Options](#transport-options)
- [Read-Only vs Read-Write Transactions](#read-only-vs-read-write-transactions)
- [Project Structure](#project-structure)
- [Putting It All Together](#putting-it-all-together)

## What is TAPIR?

```
  Transactional Application Protocol for Inconsistent Replication
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Client App
        |
        |  get / put / commit
        v
  +-----------+      +-----------+      +-----------+
  |  Shard 0  |      |  Shard 1  |      |  Shard 2  |
  | [a - m)   |      | [m - t)   |      | [t - z)   |
  |  R R R    |      |  R R R    |      |  R R R    |
  +-----------+      +-----------+      +-----------+
   Replicated         Replicated         Replicated
   via IR              via IR              via IR

  A distributed transactional key-value store.
  Keys are partitioned across shards.
  Each shard is replicated for fault tolerance.
  Transactions can span multiple shards.
```

## Architecture Stack

```
         .───────────────────────────────────.
        ;    Client Application (get/put)     :
         `───────────────┬───────────────────'
                         |
         .───────────────v───────────────────.
        ;        TAPIR Transaction Layer       :
        ;   OCC validation, 2PC across shards  :
         `───────────────┬───────────────────'
                         |
         .───────────────v───────────────────.
        ;       IR Consensus (Leaderless)      :
        ;   Inconsistent + Consensus ops       :
         `───────────────┬───────────────────'
                         |
         .───────────────v───────────────────.
        ;         Transport Layer              :
        ;   Channel | TCP+Bitcode              :
         `───────────────┬───────────────────'
                         |
         .───────────────v───────────────────.
        ;          Storage Backend             :
        ;   Memory | WiscKey Disk | SurrealKV  :
         `───────────────────────────────────'
```

## IR Consensus — Leaderless!

```
  NOT Raft. No leader for normal operations.
  Leader only coordinates view changes.

       R0              R1              R2
       ..              ..              ..
       ..              ..              ..

  Client sends to ALL replicas in parallel:

       R0              R1              R2
        ^               ^               ^
         \              |              /
          `-----.   .---'----.   .----'
                 \ /          \ /
                  *            *
                  |            |
               Client A    Client B

  Quorum = f+1 matching replies  (f = tolerated failures)
  3 replicas => quorum of 2
  5 replicas => quorum of 3
```

## Two Types of IR Operations

```
  INCONSISTENT (fire-and-forget results)
  =======================================

  Client ----Propose---->  R0  R1  R2
         <---Reply--------  *   *
         (f+1 replies)          *

  Client ----Finalize---->  R0  R1  R2    (async, best-effort)


  CONSENSUS (agreement on result)
  ================================

  Client ----Propose---->  R0  R1  R2
         <---Reply--------  *   *   *
         (check f+1 match)

  If conflict:
  Client ----Finalize---->  R0  R1  R2    (with decided result)
         <---Confirm------  *   *   *
```

## Compare: TAPIR (2PC + IR) vs 2PC + Raft

```
  Traditional: 2PC + Raft (e.g. Spanner, CockroachDB)
  ====================================================

  Coordination happens TWICE:
    1. Transaction layer (2PC) coordinates across shards
    2. Raft coordinates WITHIN each shard

  Client                   Shard 0 (Raft)           Shard 1 (Raft)
    |                      Leader  F1  F2           Leader  F1  F2
    |                        |                        |
    |------- PREPARE ------->|                        |
    |                        |--- AppendEntries ----->|  Raft replicates
    |                        |<-- Ack ---------------|  the PREPARE
    |                        |--- AppendEntries ------------>|
    |                        |<-- Ack ----------------------|
    |                        |                        |
    |------- PREPARE -------------------------------->|
    |                                                 |--- AppendEntries ->|
    |                                                 |<-- Ack -----------|
    |                                                 |--- AppendEntries ---->|
    |                                                 |<-- Ack --------------|
    |                        |                        |
    |<------ PREPARE-OK -----|                        |
    |<------ PREPARE-OK ---------------------------------|
    |                        |                        |
    |------- COMMIT -------->|                        |
    |                        |--- AppendEntries ----->|  Raft replicates
    |                        |<-- Ack ---------------|  the COMMIT too!
    |                        |                        |
    |------- COMMIT ------------------------------------>|
    |                                                 |--- AppendEntries ->|
    |                                                 |<-- Ack -----------|
    |                        |                        |
    v                        v                        v

  Total hops for PREPARE:  Client -> Leader -> Followers -> Leader -> Client
                                     ~~~~~~~~~~~~~~~~~~~~~~~~
                                     Redundant! 2PC already
                                     coordinates agreement.
                                     Raft re-coordinates it.


  TAPIR: 2PC + IR (this project)
  ===============================

  Coordination happens ONCE:
    IR is "inconsistent" — no leader, no internal replication.
    2PC is the ONLY coordination layer.

  Client                   Shard 0 (IR)             Shard 1 (IR)
    |                      R0   R1   R2             R0   R1   R2
    |                       |    |    |               |    |    |
    |------- PREPARE ------>|    |    |               |    |    |
    |------- PREPARE ----------->|    |               |    |    |
    |------- PREPARE -------------->  |               |    |    |
    |------- PREPARE ---------------------------------------->  |    |
    |------- PREPARE ------------------------------------------->|    |
    |------- PREPARE ---------------------------------------------->  |
    |                       |    |    |               |    |    |
    |<------ PREPARE-OK ----|    |    |               |    |    |
    |<------ PREPARE-OK ---------|    |               |    |    |
    |<------ PREPARE-OK --------------|               |    |    |
    |<------ PREPARE-OK ----------------------------------|    |
    |<------ PREPARE-OK ---------------------------------------|
    |<------ PREPARE-OK -------------------------------------------|
    |                       |    |    |               |    |    |
    |------- COMMIT ------->|    |    |  (async,      |    |    |
    |------- COMMIT ------------>|    |   no ack      |    |    |
    |------- COMMIT --------------->  |   needed)     |    |    |
    |------- COMMIT ------------------------------------------>  |    |
    |------- COMMIT ------------------------------------------->|    |
    |------- COMMIT ------------------------------------------------>|
    |                       |    |    |               |    |    |
    v  done!                v    v    v               v    v    v

  Total hops for PREPARE:  Client -> All Replicas -> Client
                           No extra leader hop. No redundant consensus.


  Side by Side: Latency for a 2-shard PREPARE
  ============================================

  2PC + Raft (4 network hops)         TAPIR 2PC + IR (2 network hops)
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~         ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  Client ----> Leader                 Client ----> R0, R1, R2
                 |                                  |   |   |
               Leader ----> Followers               |   |   |
                              |                     |   |   |
               Leader <---- Followers               |   |   |
                 |                                  |   |   |
  Client <---- Leader                 Client <---- R0, R1, R2

  1. Client  -> Leader                1. Client  -> Replicas
  2. Leader  -> Followers             2. Replicas -> Client
  3. Followers -> Leader                 (that's it)
  4. Leader  -> Client

  The Raft layer adds hops 2 & 3 inside every shard,
  on every operation, for coordination that 2PC already provides.
```

## TAPIR Transaction Lifecycle

```
  BEGIN
    |
    v
  GET(key) ------> 1 replica (unlogged, fast)
    |                 no consensus needed
    |
  PUT(key, val) --> local write-set only
    |                 nothing sent yet
    |
    v
  COMMIT  (2-Phase Commit across shards)
    |
    |   Phase 1: PREPARE
    |   ==================
    |
    |     Shard 0          Shard 1
    |    R0 R1 R2         R0 R1 R2
    |     |  |  |          |  |  |
    |     v  v  v          v  v  v
    |    Consensus:       Consensus:
    |    PREPARE          PREPARE
    |     |  |  |          |  |  |
    |     OK OK OK        OK OK OK
    |       \  |            |  /
    |        v v            v v
    |   Phase 2: COMMIT (if all shards OK)
    |   ===================================
    |
    |     Shard 0          Shard 1
    |    R0 R1 R2         R0 R1 R2
    |     |  |  |          |  |  |
    |     v  v  v          v  v  v
    |    Inconsistent:    Inconsistent:
    |    COMMIT           COMMIT
    |    (async)          (async)
    |
    v
  DONE
```

## OCC — Optimistic Concurrency Control

```
  Transaction T1: read(x)=1, write(y=10)
  Transaction T2: read(y)=0, write(x=5)

  Time ------>

  T1: begin .... read x .... write y .... PREPARE
  T2: begin ......... read y ...... write x .... PREPARE

  At PREPARE, each replica checks:

    For each key read by T:
      Was it written by another txn with timestamp
      between T's read-time and T's commit-time?

      read_version(x)              T.timestamp
            |                          |
            v                          v
    --------[=========CHECK WINDOW=========]---------->
                  ^
                  |
            Was x written here by someone else?

    YES => ABSTAIN  (conflict detected)
    NO  => PREPARE-OK
```

## Storage: WiscKey on Disk

```
  Inspired by WiscKey: keys in LSM, values in log.

  WRITE PATH
  ==========

       put(key, value)
            |
            v
      +-----------+
      | MemTable  |  (sorted, in-memory)
      +-----------+
            |  flush when full
            v
      +-----------+       +------------------+
      |  SSTable  | ----> | Value Log (vLog) |
      | key: ptr  |       | [val1][val2][..] |
      +-----------+       +------------------+
       sorted keys         append-only values

  READ PATH
  =========

       get(key)
         |
         v
    MemTable --miss--> SSTable --found--> ptr
                                           |
                                           v
                                     +-----------+
                                     |   vLog    |
                                     | pread(ptr)|
                                     +-----------+
                                           |
                                           v
                                         value

  WHY?  LSM compaction only moves small key+ptr entries.
        Large values stay put in the append-only vLog.
```

## PersistentIrRecordStore — VlogLsm-backed IR Record (tapirs extension)

```
  tapirs extension over the original TAPIR paper.
  IR replicas store operations in PersistentIrRecordStore.
  Uses VlogLsm with per-view sealed segments.

  View 5 (sealed segments)          View 6 (memtable)
  ============================      =================

  +-------------------------+      +----------------+
  |    SEALED SEGMENTS      |      |   MEMTABLE     |
  |  (vlog + SST on disk)   |      | (current view) |
  |                         |      |                |
  |  op1: Finalized(COMMIT) |      | op5: Tentative |
  |  op2: Finalized(ABORT)  |      | op6: Finalized |
  |  op3: Finalized(COMMIT) |      | op7: Tentative |
  |  op4: Finalized(COMMIT) |      |                |
  +-------------------------+      +----------------+
              ^                           ^
              |                           |
         from previous               changes during
         view changes               current view

  On view change:
    seal memtable -> new segment (= this view's delta)
    start new empty memtable for next view

  CDC delta for a view = the sealed segment at that boundary
```

## View Change: Delta Payloads

```
  tapirs: PersistentIrRecordStore with delta payloads
  ====================================================

  Replicas with matching base_view receive only the delta
  (new entries since last seal). Others get a full payload.

  R0 ---DoViewChange(sealed + memtable bytes)-----> Leader
  R1 ---DoViewChange(sealed + memtable bytes)-----> Leader
  R2 ---DoViewChange(sealed + memtable bytes)-----> Leader
                                                      |
  Leader ---StartView(delta: 80 new ops)-----------> R0 (same base)
  Leader ---StartView(full: 10,000 ops)------------> R1 (different base)
  Leader ---StartView(delta: 80 new ops)-----------> R2 (same base)

  Followers with matching base keep their sealed segments,
  import only the delta. No re-reading of existing data.
```

## View Change

```
  Triggered when replicas suspect current view has failed.

  View N                              View N+1
  ======                              ========

  R0: "I think view N is dead"
   |
   |  StartViewChange(N+1)
   +-----------------------------> R1 (leader of N+1)
   +-----------------------------> R2
                                    |
  R2: "I agree"                     |
   |                                |
   |  DoViewChange(N+1, record)     |
   +------------------------------> R1
                                    |
                          R1 collects f+1
                          DoViewChange msgs
                                    |
                          Merge records:
                          - Finalized wins
                          - Consensus: majority
                          - Inconsistent: keep all
                                    |
                          StartView(N+1, merged_record)
                          .---------|---------.
                          v         v         v
                         R0        R1        R2

  All replicas now have consistent merged record.
  Normal operations resume in view N+1.
```

## Sharding & Routing

```
  Key space split into non-overlapping ranges.

  Full key range: [0x00 ................... 0xFF]

  Shard 0         Shard 1         Shard 2
  [0x00, 0x55)    [0x55, 0xAA)    [0xAA, 0xFF]
  aaaaa-mmmmm     mmmmm-sssss     ttttt-zzzzz


  ShardDirectory (cached, polling)
  ================================

    route("apple")  --> Shard 0
    route("nova")   --> Shard 1
    route("zebra")  --> Shard 2
    route("???")    --> None  (OutOfRange, retry after refresh)


  Resharding (CDC-based shard splitting)
  =======================================

  Before:
    Shard 0: [0x00 .............. 0xFF]    3 replicas

  After:
    Shard 0: [0x00 .... 0x80)              3 replicas
    Shard 1: [0x80 .... 0xFF]              3 replicas

  Data migrated via Change Data Capture (CDC).
  View-based cursors ensure consistency.
```

## Backup & Restore

```
  Incremental backup via CDC deltas. Restore replays to a fresh cluster.


  BACKUP
  ======

  tapictl backup cluster --output /backups/

                                       Shard 0            Shard 1
                                      R0  R1  R2         R0  R1  R2
                                       |   |   |          |   |   |
  ShardManager ----scan_changes------->|   |   |          |   |   |
  (HTTP POST       (from_view=N)       |   |   |          |   |   |
   /v1/scan-changes)                   |   |   |          |   |   |
               <--CDC deltas-----------'   |   |          |   |   |
               <--CDC deltas---------------'   |          |   |   |
               <--CDC deltas-------------------'          |   |   |
                                                          |   |   |
  ShardManager ----scan_changes-------------------------->|   |   |
               <--CDC deltas------------------------------'   |   |
               <--CDC deltas----------------------------------'   |
               <--CDC deltas--------------------------------------'
       |
       |  merge f+1 replica deltas (quorum coverage)
       v
  /backups/
    cluster.json                        (metadata + view cursors)
    shard_0_delta_0.bin                 (initial full)
    shard_0_delta_1.bin                 (incremental)
    shard_1_delta_0.bin
    ...

  Next backup resumes from last effective_end_view (incremental).


  RESTORE
  =======

  tapictl restore cluster --input /backups/ --admin-addrs n0:9000,n1:9000,...

  Phase 1: Create empty replicas on target nodes
  ===============================================

  Admin Client ---- add_replica(shard=0) ----> Node 0 :9000
  Admin Client ---- add_replica(shard=0) ----> Node 1 :9000
  Admin Client ---- add_replica(shard=0) ----> Node 2 :9000
                                                  |
                                          Fresh replicas at
                                          view 0, empty MVCC

  Phase 2: Replay CDC deltas into each shard
  ===========================================

  /backups/shard_0_delta_0.bin -.
  /backups/shard_0_delta_1.bin --+---> ShardManager
                                       /v1/apply-changes
                                            |
                                            |  ship_changes()
                                            |  (each delta -> IO::Commit)
                                            v
                                       Shard 0
                                      R0  R1  R2
                                       |   |   |
                                       v   v   v
                                      MVCC stores
                                      rebuilt!

  Phase 3: Register shards in discovery
  ======================================

  ShardManager ---- /v1/register ----> Discovery
                                       Shard 0: [0x00, 0x80) -> {R0,R1,R2}
                                       Shard 1: [0x80, 0xFF] -> {R3,R4,R5}
                                            |
                                            v
                                       Clients can now
                                       discover and query
```

## Resharding — CDC Shard Splitting

```
  All resharding (split, merge, compact) follows the same 3-phase pattern:
  bulk copy -> catch-up tailing -> freeze + drain.


  SHARD SPLIT EXAMPLE
  ====================

  Goal: Split Shard 0 [a-z) into Shard 0 [a-m) + Shard 1 [m-z)

  Phase 1: Bulk Copy
  ==================

  Shard 0 (source)                         Shard 1 (target)
  [a ................. z)                  [m ........... z)
  R0  R1  R2                               R0  R1  R2
   |   |   |                                |   |   |
   |   |   |   scan_changes(from_view=0)    |   |   |
   |   |   |   ========================    |   |   |
   |   |   |                                |   |   |
   '---+---'----> CDC deltas                |   |   |
                    |                        |   |   |
                    |  filter: key >= "m"    |   |   |
                    |                        |   |   |
                    '--- ship_changes() ---->|   |   |
                         (IO::Commit)        |   |   |


  Phase 2: Catch-up Tailing (up to 30 iterations)
  ================================================

  Clients keep writing to Shard 0 during bulk copy.
  Catch-up loop tails the CDC stream to close the gap.

  Shard 0                cursor             Shard 1
   |                       |                  |
   |  scan_changes(v=5)    |                  |
   |   new deltas -------->|                  |
   |                       |--- ship -------->|
   |  scan_changes(v=8)    |                  |
   |   new deltas -------->|                  |
   |                       |--- ship -------->|
   |  scan_changes(v=10)   |                  |
   |   (no changes,        |                  |
   |    cursor stabilized) |                  |
   |                       v                  |
   |                   done tailing           |


  Phase 3: Freeze + Drain Prepared
  =================================

  Can't switch over yet! In-flight transactions might have
  CO::Prepare on Shard 0 that haven't resolved.
  If we decommission now, their write sets are lost.

  Step 3a: Freeze source shard

  Shard 0 (FROZEN)
  R0  R1  R2
   |   |   |
   X   X   X <-- new Prepare requests rejected (Fail)
   |   |   |
   v   v   v
   Existing prepared txns continue resolving
   via tick() -> recover_coordination()

  Step 3b: Drain until pending_prepares == 0

  time ------>
  pending:  5 .... 3 .... 1 .... 0    done!
              |       |       |
              v       v       v
         scan_changes + ship to Shard 1
         (captures resolved Commit/Abort)

  Final 3s wait: tick fires (~2s), forces view change,
  seals last resolved txns into CDC delta.
  One final scan_changes() captures them.

  Step 3c: Atomic switchover

  BEFORE:
    Shard 0: [a .................. z)

  strong_atomic_update_shards([
    ActivateShard { shard=0, range=[a,m) },     narrow source
    ActivateShard { shard=1, range=[m,z) },     activate target
  ])
  ^^^ single atomic write, no transient overlap

  AFTER:
    Shard 0: [a ........ m)
    Shard 1:             [m ........ z)


  WHY VIEW-BASED CURSORS?
  ========================

  TAPIR timestamps are NOT monotonic across replicas.
  Views ARE monotonic (0, 1, 2, 3, ...).

  Timestamp-based cursor (WRONG):

  R0 commits: t=100, t=95, t=110    out of order!
                          ^
              cursor at t=100 would miss t=95

  View-based cursor (CORRECT):

  View 5: ops sealed by view change    cursor: view=5
  View 6: ops sealed by view change    cursor: view=6
                                       never misses ops


  MERGE DELTA ALGORITHM
  =====================

  f+1 replicas may have different delta granularity.
  Merge picks the finest available, falls back to spanning.

  R0:  delta(0->1)  delta(1->2)  delta(2->3)     fine-grained
  R1:  delta(0->1)  delta(1->3)                   spanning (got Full)
  R2:  delta(0->2)               delta(2->3)      spanning (got Full)

  Merged:
    view 0->1: from R0 (or R1)   finest available
    view 1->2: from R0            finest available
    view 2->3: from R0 (or R2)    finest available

  Result: complete coverage, prefer fine-grained deltas.


  COMPACTION (memory reclamation)
  ===============================

  IR record grows forever. Compaction resets it.

  Before compaction:                    After compaction:

  Shard 0                               Shard 0'
  IR record: [op1...op10000]            IR record: []  (empty!)
  CDC deltas: [v0...v500]               CDC deltas: [] (empty!)
  MVCC: {a=1, b=2, c=3, ...}           MVCC: {a=1, b=2, c=3, ...}
                                             ^
  Same data, fresh IR record.               copied via CDC
  New replicas joining via sync()
  no longer replay 10,000 ops.
```

## Transport Options

```
  Channel Transport (tests)         TCP + Bitcode (production)
  =========================         ==========================

  R0 <--channel--> R1               R0 <----TCP----> R1
  R1 <--channel--> R2               R1 <----TCP----> R2
  R0 <--channel--> R2               R0 <----TCP----> R2

  In-process, instant,              Real network, serialized
  deterministic.                    with bitcode (compact binary).

  FaultyChannel (fuzz tests)
  ==========================

  Same as Channel, but randomly:
  - drops messages
  - delays messages
  - reorders messages
  - duplicates messages

            +----[delay]----+
  R0 ------>| drop? dup?    |------> R1
            +---------------+
```

## Read-Only vs Read-Write Transactions

```
  Read-Write (begin)                 Read-Only (begin_read_only)
  ==================                 ===========================

  get(k) --> 1 replica               get(k) --> f+1 replicas
             (unlogged)                          (quorum read)
             fast but                            slower but
             may be stale                        consistent

  put(k,v) -> local buffer           put(k,v) -> NOT ALLOWED

  commit() -> 2PC                    no commit needed
    Prepare (consensus)              reads validated at
    Commit  (inconsistent)           read time by quorum
    or Abort

  Use for:                           Use for:
  - writes                           - consistent snapshots
  - read-then-write                  - reports, analytics
```

## Project Structure

```
  src/
   |
   +-- ir/                 IR consensus protocol
   |    +-- client.rs         invoke_inconsistent, invoke_consensus
   |    +-- replica.rs        view changes, record management
   |
   +-- tapir/              TAPIR transaction layer
   |    +-- client.rs         begin, get, put, commit
   |    +-- replica.rs        prepare/commit/abort handlers
   |    +-- shard_client.rs   per-shard IR client wrapper
   |
   +-- occ/                Optimistic Concurrency Control
   |    +-- store.rs          conflict detection at prepare time
   |
   +-- mvcc/               Multi-Version Concurrency Control
   |    +-- disk/             WiscKey SSD storage (DiskIo, MemoryIo, FaultyDiskIo)
   |
   +-- transport/           Network layer
   |    +-- channel.rs        in-process (tests)
   |    +-- faulty_channel.rs fault injection (fuzz)
   |    +-- tokio_bitcode_tcp/  production TCP
   |
   +-- discovery/           Shard directory & routing
   +-- tls/                 mTLS support (optional)
   +-- node/                Admin server & client
   +-- bin/tapi/            CLI binary

  kubernetes/
   +-- operator/            Go operator for K8s deployment
```

## Putting It All Together

```
                          .---------.
                          | Client  |
                          |  App    |
                          '----+----'
                               |
                     begin / get / put / commit
                               |
                    .----------v----------.
                    |   TapirClient       |
                    |  (transaction mgr)  |
                    '----+----------+-----'
                         |          |
              .----------v--. .----v----------.
              | ShardClient | | ShardClient   |
              |  (shard 0)  | |  (shard 1)    |
              '------+------' '------+--------'
                     |               |
              .------v------. .------v--------.
              |  IrClient   | |  IrClient     |
              | (consensus) | | (consensus)   |
              '------+------' '------+--------'
                     |               |
            .--------+---.   .-------+---------.
            v    v    v      v    v    v
           R0   R1   R2    R0   R1   R2

           IR Replicas      IR Replicas
           (shard 0)        (shard 1)
            |    |    |      |    |    |
            v    v    v      v    v    v
           OCC  OCC  OCC   OCC  OCC  OCC
            |    |    |      |    |    |
            v    v    v      v    v    v
          MVCC MVCC MVCC  MVCC MVCC MVCC
          Store Store Store Store Store Store
```
