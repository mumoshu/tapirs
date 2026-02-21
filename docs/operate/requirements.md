# Operational Requirements

```
         Cluster Sizing

  Nodes  x  Shards  x  Replicas  =  Cluster Capacity

  +-------+    +--------+    +----------+
  | 3-5   | x  | 2-100+ | x  | 3 or 5  |
  | nodes |    | shards |    | per shard|
  +-------+    +--------+    +----------+
       |            |              |
       v            v              v
    hardware    key space       fault
    budget     partitioning   tolerance
```

**P1 -- Sizing dimensions:** This document covers the hardware, network, and cluster sizing requirements for running tapirs in production. The key sizing dimension is the number of shards: each shard is an independent IR consensus group with 2f+1 replicas (typically 3 or 5), and shards are distributed across nodes. A single node can host replicas for many shards -- tapirs uses a single-threaded state machine per replica, so multi-core utilization comes from running multiple shard replicas per node rather than parallelism within a single replica.

**P2 -- Development vs production:** For a development or test cluster, the Docker testbed defaults (3 nodes, 2 shards, replication factor 3) are sufficient and run comfortably on a laptop. For production, plan based on your key space size and access patterns: each shard handles a contiguous key range, and hot shards can be split online. Memory usage scales with the number of in-flight transactions (the OCC prepared index) and the IR record size (which grows until compaction). Disk usage scales with the MVCC data volume and value log size. Network bandwidth scales with transaction throughput -- each consensus round contacts f+1 replicas per shard.

**P3 -- Related docs:** See [Monitoring](monitoring.md) for the metrics to track these resources, [Troubleshooting](troubleshooting.md) for diagnosing resource exhaustion, and [tapictl compact](cli-tapictl-compact.md) for reclaiming IR record and storage resources. For the storage engine internals, see [Storage](../learn/internals/storage.md). Back to [Operate](README.md).

| Resource | What drives it | Recommendation |
|----------|---------------|----------------|
| CPU cores | Number of shard replicas per node | 1 core per active shard replica |
| Memory | IR record size + OCC prepared index + memtable | Monitor and compact when IR record grows large |
| Disk | MVCC data + value log + SSTables | WiscKey reduces write amplification; compact to reclaim |
| Network | Transaction throughput x replicas per consensus | f+1 replicas contacted per consensus round |
| Nodes | Replication factor x fault tolerance needs | Minimum 3 (f=1), recommended 5 (f=2) |
