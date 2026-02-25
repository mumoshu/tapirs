# Architecture and Design

```
                          +-----------+
                          |  Client   |
                          +-----+-----+
                                |
                         +------v------+
                         |RoutingClient|
                         +------+------+
                                |
              +-----------------+-----------------+
              |                                   |
       +------v------+                     +------v------+
       | ShardClient |                     | ShardClient |
       +------+------+                     +------+------+
              |                                   |
     +--------+--------+                 +--------+--------+
     |    |    |    |   |                |    |    |    |   |
   +--+ +--+ +--+ +--+ |              +--+ +--+ +--+ +--+ |
   |R1| |R2| |R3|  ...                |R1| |R2| |R3|  ...
   +--+ +--+ +--+                     +--+ +--+ +--+
   IR Replicas (Shard 0)              IR Replicas (Shard 1)
              |                                   |
       +------v------+                     +------v------+
       |ShardManager |                     |ShardManager |
       +------+------+                     +------+------+
              |                                   |
       +------v--------------------------------------+
       |         CachingShardDirectory               |
       |    (local cache + background sync)          |
       +---------------------+-----------------------+
                             |
                    +--------v--------+
                    |RemoteShardDir   |
                    |(static/DNS/TAPIR)|
                    +-----------------+
```

**Layered architecture:** tapirs is built in layers, each depending only on the layer below it. At the bottom, **IR consensus** (`src/ir/`) provides leaderless fault-tolerant replication -- an unordered operation set, not an ordered log. On top of IR, **TAPIR transactions** (`src/tapir/`) implement 2PC with client-coordinated timestamps. The **OCC store** (`src/occ/`) validates read/write/scan conflicts at prepare time, and the **MVCC store** (`src/mvcc/`) persists versioned key-value data -- either in memory or on disk via a WiscKey engine. **Transport** (`src/transport/`) is pluggable: in-process channels for testing, Tokio TCP for production, and optional io_uring for high-throughput workloads. **Discovery** (`src/discovery/`) handles shard-to-replica mapping without external coordinators.

**Data flow:** A read-write transaction flows through these layers: the client begins a transaction, reads via IR unlogged ops (single replica, MVCC store), writes are buffered locally, then at commit time the client sends IR consensus Prepare to all shards (OCC validates at each replica), and on f+1 PREPARE-OK the client sends IR inconsistent Commit. Read-only transactions skip the prepare phase entirely -- validated reads at a single replica, with quorum fallback. Membership changes (replica joins, view changes) flow through the transport layer and update the local directory, which syncs with the remote discovery backend.

**Design principles:** Three design principles run through the codebase: (1) **leaderless** -- no single point of failure, no leader election, no failover delay; (2) **single-threaded state machine per replica** -- Mutex, not RwLock, faithful to the original paper's TLA+-verified model (sharding provides parallelism across replicas); (3) **fix at the right layer** -- transport bugs in transport, protocol bugs in protocol, never cross layers.

| You want to understand... | Start here | Deep-dive |
|--------------------------|------------|-----------|
| How transactions achieve consensus | `src/ir/replica.rs` (~700 lines, the most complex file) | [Protocol](protocol-tapir.md) |
| How the paper's extensions work (RO txns, coordinator recovery) | `src/tapir/client.rs` | [Paper Extensions](protocol-tapir-paper-extensions.md) |
| How range scan + phantom protection work | `src/occ/store.rs` | [Custom Extensions](protocol-tapir-custom-extensions.md) |
| How IR record representation and compaction work | `src/ir/record.rs`, `src/ir/replica.rs` | [IR Custom Extensions](ir-custom-extensions.md) |
| How data is persisted to disk | `src/mvcc/disk/` | [Storage](storage.md) |
| How shards split, merge, and compact online | `src/sharding/shardmanager/cdc.rs` | [Resharding](resharding.md) |
| How nodes discover each other | `src/discovery/mod.rs`, `src/discovery/tapir.rs` | [Discovery](discovery.md) |
| How tests achieve deterministic reproduction | `tests/`, `src/transport/faulty_channel.rs` | [Testing](testing.md) |
| Why specific architectural choices were made | -- | [Architecture Decisions](architecture-decisions.md) |
| How the cluster is laid out (clients, shards, discovery) | -- | [Cluster Topology](topology.md) |
