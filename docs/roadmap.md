# Roadmap

```
 Current                                                       Future
────────────────────────────────────────────────────────────────────────────
  [Kubernetes Testbed] ──> [Kind Testbed] ──> [Observability]
                                                     │
                                              [Kubernetes Operator]
                                                     │
                                                [Benchmark]
────────────────────────────────────────────────────────────────────────────
```

**P1 — Current state:** tapirs has a complete protocol implementation — IR consensus, TAPIR transactions, OCC conflict detection, WiscKey storage, online resharding, and self-contained discovery are all implemented and verified by deterministic simulation testing. The roadmap below covers the next phase of work: making tapirs easy to deploy, observe, and benchmark in cloud-native environments. These items are listed in rough priority order, but the order may shift based on community feedback and use-case demand.

**P2 — Cloud-native priorities:** The Kubernetes testbed and Kind testbed are the first priorities — they extend the existing Docker testbed to Kubernetes environments, making it easy to deploy a multi-node tapirs cluster on any Kubernetes distribution or locally via Kind. Observability adds structured metrics, health endpoints, and Prometheus/Grafana integration so operators can monitor cluster health in production. The Kubernetes Operator automates cluster lifecycle management — creating, scaling, resharding, and healing tapirs clusters declaratively via Custom Resources. Benchmarks provide reproducible performance measurements (ops/sec, latency percentiles) against representative workloads, enabling objective comparison with alternatives.

**P3 — Get involved:** Contributions and feedback are welcome — see the [GitHub repository](https://github.com/mumoshu/tapirs) for issues and discussions. For the current state of the project, see [History](history.md). Back to [Overview](README.md).

| Item | Description | Status |
|------|-------------|--------|
| Kubernetes Testbed | Deploy a tapirs cluster on Kubernetes using Helm or raw manifests. Extends Docker testbed with DNS-based discovery for headless services | Planned |
| Kind (Kubernetes) Testbed | Bootstrap a local tapirs cluster on Kind (Kubernetes-in-Docker). Zero-dependency local Kubernetes experience | Planned |
| Observability | Prometheus metrics endpoint, Grafana dashboard templates, structured logging, health checks. See [Monitoring](operate/monitoring.md) for what to track | Planned |
| Kubernetes Operator | Custom Resource Definitions for TapirsCluster, automated scaling, resharding triggers, self-healing. Manages the full cluster lifecycle declaratively | Planned |
| Benchmark | Reproducible performance benchmarks: ops/sec, p50/p99/p999 latency, scalability curves. Comparison with etcd, FoundationDB, CockroachDB, TiKV. See [Comparison](evaluate/comparison.md) | Planned |
