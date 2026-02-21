# Troubleshooting

```
         Diagnosis Flow

  +----------+     +------------+     +---------+
  | Symptom  |---->| Diagnosis  |---->| Remedy  |
  +----------+     +------------+     +---------+
       |                 |                 |
       v                 v                 v
   What you see     Check metrics     Fix the cause
   in the cluster   and logs          with tapictl
                                      or config
```

**P1 -- Three problem categories:** This guide covers common symptoms you may encounter when operating a tapirs cluster, their likely causes, and the recommended remedies. Most issues fall into three categories: connectivity problems (nodes can't find each other), performance problems (high latency or abort rates), and resource problems (growing memory or disk usage). The table below provides a quick lookup from symptom to solution.

**P2 -- Diagnosis by category:** For connectivity issues, always check the discovery URL first -- nodes need to reach the discovery service to find each other. If replicas show as unreachable, verify network connectivity and that the listen addresses match what's registered in discovery. For performance issues, the OCC abort rate is the most important diagnostic: a high abort rate means transactions are conflicting, and the solution is usually to split the hot shard. For resource issues, shard compaction is the primary tool -- it reclaims both the IR record and LSM storage.

**P3 -- Related docs:** For the metrics that help diagnose these issues, see [Monitoring](monitoring.md). For the operational commands to resolve them, see [tapictl](cli-tapictl.md) (split, merge, compact) and [tapi admin](cli-tapi-admin.md) (status, membership changes). For understanding the failure modes that cause these symptoms, see [Failure Modes](../learn/internals/failure-modes.md). Back to [Operate](README.md).

| Symptom | Likely cause | Diagnosis | Remedy |
|---------|-------------|-----------|--------|
| Replicas not joining cluster | Wrong discovery URL or network issue | Check discovery URL, verify network connectivity | Fix discovery config, check firewall |
| High transaction abort rate | OCC contention on hot key range | Check OCC abort rate metric, identify hot shard | `tapictl split shard` |
| Frequent view changes | Network instability or overloaded node | Check view change frequency, node health | Stabilize network, redistribute shards |
| Growing memory usage | Large IR record or many in-flight txns | Check IR record size metric | `tapictl compact shard` |
| Growing disk usage | Value log + SSTable accumulation | Check disk usage metric | `tapictl compact shard` |
| Slow transactions | Overloaded replicas or network latency | Check quorum response p99 | Add nodes, split shards, check network |
| Resharding stuck | ShardManager crash during drain phase | Check ShardManager logs | Restart ShardManager (resumes from checkpoint) |
