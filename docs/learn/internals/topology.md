# Cluster Topology

```
                         tapirs cluster

  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │  Your App  │  │  Your App  │  │  Your App  │
  │  (client)  │  │  (client)  │  │  (client)  │
  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
        │               │               │
        ▼               ▼               ▼
  ┌──────────┐   ┌──────────┐   ┌──────────┐
  │ Shard 1  │   │ Shard 2  │   │ Shard 3  │
  │ (a..m)   │   │ (m..t)   │   │ (t..z)   │
  │          │   │          │   │          │
  │ replica  │   │ replica  │   │ replica  │
  │ replica  │   │ replica  │   │ replica  │
  │ replica  │   │ replica  │   │ replica  │
  └──────────┘   └──────────┘   └──────────┘

  ┌──────────────┐                        ┌────────────┐
  │ ShardManager │  publishes routes to   │ Discovery  │
  │ (singleton)  │ ─────────────────────► │ (embedded) │
  └──────────────┘                        └────────────┘
                                                ▲
                                    clients pull│membership

          one binary · no external deps
```

**Clients** are transaction coordinators deployed as your application — an app or database that embeds the tapirs client library. Each client pulls shard membership from Discovery and sends Prepare/Commit messages directly to shard replicas. There is no proxy or gateway between clients and replicas.

**Shards** partition the key space into non-overlapping ranges. Each shard contains 2f+1 replicas running leaderless IR consensus. Replicas within a shard are symmetric — there is no leader in the transaction hot path.

**ShardManager** is a cluster-wide singleton that orchestrates shard lifecycle: splitting hot shards, merging cold shards, compacting to reclaim resources, and managing replica membership. It publishes route changes to Discovery so that clients learn the new shard layout. (Ref: `src/tapir/shard_manager.rs` — holds `HashMap<ShardNumber, ManagedShard>` for all shards.)

**Discovery** is a self-contained embedded membership directory — no external ZooKeeper or etcd required. Clients pull membership to learn which replicas serve which key ranges. ShardManager pushes route updates when shards split, merge, or when replicas join/leave. See [Discovery](discovery.md) for the full deep-dive.

Because clients are the transaction coordinators and the transaction hot path requires no leader election among replicas, you can scale transaction throughput horizontally in two dimensions: add more clients if coordination (mostly network latency and CPU) is the bottleneck, or add more shards and replicas if persistence (mostly network latency and I/O) is the bottleneck.
