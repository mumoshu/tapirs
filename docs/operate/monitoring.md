# Monitoring and Observability

```
  +----------------+     scrape     +------------+     query     +---------+
  |    tapirs      |--------------->| Prometheus |-------------->| Grafana |
  |    nodes       |   /metrics     |            |               |         |
  +----------------+                +------------+               +---------+
         |                               |
         v                               v
   Transaction metrics             Alert rules
   Consensus metrics               Dashboards
   Resource metrics                Notifications
```

**Three metric categories:** Monitoring a tapirs cluster means tracking three categories of metrics: transaction health (commit rate, abort rate, OCC conflict rate), consensus health (view change frequency, quorum response times), and resource utilization (IR record size, memory, disk, network). These metrics tell you whether the cluster is healthy, whether specific shards are becoming hotspots, and when to intervene with operational actions like shard splitting or compaction.

**Key metrics to watch:** Key metrics to watch: a rising OCC abort rate indicates contention on specific key ranges -- consider splitting the hot shard. Frequent view changes suggest network instability or overloaded replicas. Growing IR record size means finalized operations are accumulating -- schedule a shard compaction. Disk usage growth rates tell you when the value log needs garbage collection. Memory pressure on a node suggests too many shard replicas or too many in-flight transactions for the available RAM.

**Thresholds and actions:** For recommended thresholds and alerting rules, see the table below. For diagnosing problems when alerts fire, see [Troubleshooting](troubleshooting.md). For operational actions to address problems (split, merge, compact), see [tapictl](cli-tapictl.md). For understanding what drives each metric, see [Requirements](requirements.md). Back to [Operate](README.md).

| Metric | What it means | Alert threshold | Action |
|--------|--------------|-----------------|--------|
| OCC abort rate | Transaction contention | > 10% of commits | Split hot shard |
| View change frequency | Consensus disruption | > 1 per minute sustained | Check network, node health |
| IR record size | Accumulated finalized ops | > configured threshold | Compact shard |
| Disk usage | MVCC data + value log growth | > 80% capacity | Compact or add storage |
| Memory usage | IR record + OCC index + memtable | > 80% of node RAM | Compact or redistribute shards |
| Quorum response p99 | Consensus latency | > 100ms sustained | Check network, replica load |
