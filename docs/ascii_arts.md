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
- [VersionedRecord — Base/Overlay (tapirs extension)](#versionedrecord--baseoverlay-tapirs-extension)
- [Compare: VersionedRecord vs Original IR Record](#compare-versionedrecord-vs-original-ir-record)
- [View Change](#view-change)
- [Sharding & Routing](#sharding--routing)
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
        ;   Channel | TCP+Bitcode | io_uring   :
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

## VersionedRecord — Base/Overlay (tapirs extension)

```
  tapirs extension over the original TAPIR paper.
  IR replicas track operations using a VersionedRecord.
  Avoids cloning the full record on every mutation.

  View 5 (after view change)        View 6 (current)
  ============================      =================

  +-------------------------+      +----------------+
  |         BASE            |      |    OVERLAY     |
  |  (immutable snapshot)   |      | (current view) |
  |                         |      |                |
  |  op1: Finalized(COMMIT) |      | op5: Tentative |
  |  op2: Finalized(ABORT)  |      | op6: Finalized |
  |  op3: Finalized(COMMIT) |      | op7: Tentative |
  |  op4: Finalized(COMMIT) |      |                |
  +-------------------------+      +----------------+
              ^                           ^
              |                           |
         from last                  changes during
         view change                current view

  On next view change:
    new_base = merge(base, overlay)
    overlay  = empty
```

## Compare: VersionedRecord vs Original IR Record

```
  Original TAPIR Paper: Full Record Clone
  ========================================

  Every view change, the leader merges f+1 records
  and broadcasts the FULL merged result to all replicas.

  View 5         View Change          View 6

  Record:        Leader merges        Record:
  [op1..op99]    f+1 full records     [op1..op120]
                       |
                  clone entire         Every replica
                  merged record        stores one
                  to all replicas      full copy

  Problem: Record only grows. After 10,000 ops:

  View N record:
  +-------------------------------------------------------+
  | op1 | op2 | op3 | ... | op9999 | op10000 |            |
  +-------------------------------------------------------+
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  All 10,000 entries cloned on every mutation.
  All 10,000 entries sent in every view change.


  tapirs: VersionedRecord (Base + Overlay)
  ========================================

  Only the OVERLAY (current view's changes) is cloned/sent.
  Base is immutable — shared, never copied during normal ops.

  View N record:
  +-------------------------------------------------------+   +--------+
  | op1 | op2 | op3 | ... | op9999 | op10000 |            |   | op10001|
  +-------------------------------------------------------+   | op10002|
    BASE (immutable, from last view change)                    +--------+
    Never cloned during normal operations.                      OVERLAY
                                                             (only new ops)

  Mutation cost:   O(1)              not O(record size)
  View change:     send overlay      not full record
  CDC delta:       overlay_clone()   not diff(old, new)


  View Change: What Gets Sent
  ============================

  Original paper:

  R0 ---DoViewChange(full record: 10,000 ops)---> Leader
  R1 ---DoViewChange(full record: 10,000 ops)---> Leader
  R2 ---DoViewChange(full record: 10,000 ops)---> Leader
                                                    |
  Leader ---StartView(merged: 10,000 ops)---------> R0
  Leader ---StartView(merged: 10,000 ops)---------> R1
  Leader ---StartView(merged: 10,000 ops)---------> R2

  Network: ~60,000 ops transferred


  tapirs VersionedRecord:

  R0 ---DoViewChange(overlay: 50 ops)------------> Leader
  R1 ---DoViewChange(overlay: 50 ops)------------> Leader
  R2 ---DoViewChange(overlay: 50 ops)------------> Leader
                                                    |
  Leader ---StartView(merged overlay: 80 ops)-----> R0
  Leader ---StartView(merged overlay: 80 ops)-----> R1
  Leader ---StartView(merged overlay: 80 ops)-----> R2

  Network: ~390 ops transferred  (vs 60,000)
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

## Transport Options

```
  Channel Transport (tests)         TCP + Bitcode (production)
  =========================         ==========================

  R0 <--channel--> R1               R0 <----TCP----> R1
  R1 <--channel--> R2               R1 <----TCP----> R2
  R0 <--channel--> R2               R0 <----TCP----> R2

  In-process, instant,              Real network, serialized
  deterministic.                    with bitcode (compact binary).

  FaultyChannel (fuzz tests)        io_uring (linux, optional)
  ==========================        ==========================

  Same as Channel, but randomly:    Kernel-bypassing async I/O
  - drops messages                  for disk + network.
  - delays messages                 Zero-copy, batch submissions.
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
   |    +-- store.rs          in-memory versioned KV
   |    +-- disk/             WiscKey SSD storage
   |    +-- surrealkvstore/   SurrealKV backend (optional)
   |
   +-- transport/           Network layer
   |    +-- channel.rs        in-process (tests)
   |    +-- faulty_channel.rs fault injection (fuzz)
   |    +-- tokio_bitcode_tcp/  production TCP
   |    +-- uring/            io_uring (optional)
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
