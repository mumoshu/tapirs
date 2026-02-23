# tapirs Kubernetes Operator

A Kubernetes operator that manages TAPIR clusters via the `TAPIRCluster` custom resource.

## Cluster Architecture (Two-Tier)

1. **Discovery store** — StatefulSet + headless Service. A self-contained TAPIR cluster that stores shard topology metadata.
2. **Shard manager** — Deployment + ClusterIP Service. HTTP API for routing and resharding.
3. **Data nodes** — StatefulSet(s) + headless Service(s). Each node hosts one replica per shard. Node pools map to separate StatefulSets.

## Bootstrap Phases

```
Pending → CreatingDiscovery → BootstrappingDiscovery → CreatingDataPlane
       → BootstrappingReplicas → RegisteringShards → Running
```

| Phase | What happens |
|-------|-------------|
| `Pending` | CR accepted, validation passed |
| `CreatingDiscovery` | Creating discovery StatefulSet and headless Service |
| `BootstrappingDiscovery` | Waiting for discovery pods to be Ready, then calling `add_replica` with static membership on each |
| `CreatingDataPlane` | Creating shard-manager Deployment/Service and data-node StatefulSets/Services |
| `BootstrappingReplicas` | Waiting for data-node pods to be Ready, then calling `add_replica` with static membership per shard |
| `RegisteringShards` | Calling `POST /v1/register` on shard-manager with explicit replica addresses per shard |
| `Running` | Cluster is operational |
| `Updating` | Scaling or configuration change in progress |
| `Failed` | Unrecoverable error (see conditions) |

## Who Calls What, When

### Initial bootstrap (all static membership)

| Step | Caller | Target | API | Detail |
|------|--------|--------|-----|--------|
| 1 | Operator | node admin TCP | `add_replica` WITH membership | All discovery pod IPs known upfront. Static membership, no shard-manager. |
| 2 | Operator | node admin TCP | `add_replica` WITH membership | All data-node pod IPs known upfront. Static membership per shard. |
| 3 | Operator | shard-manager HTTP | `POST /v1/register` with replicas | Passes explicit replica addresses. Avoids 10s CachingShardDirectory push race. |

### Runtime scaling (dynamic shard-manager path)

| Operation | Caller | Target | API | Detail |
|-----------|--------|--------|-----|--------|
| Add replica | Operator | new node admin TCP | `add_replica` WITHOUT membership | Node calls `POST /v1/join` to shard-manager. Safe after initial bootstrap. |
| Remove replica | Operator | departing node admin TCP | `leave` then `remove_replica` | `leave` triggers shard-manager RemoveMember broadcast. `remove_replica` is local cleanup. |

### User-initiated resharding (out of scope for v1alpha1)

| Operation | Tool | API |
|-----------|------|-----|
| Split shard | `tapictl` | `POST /v1/split` |
| Merge shard | `tapictl` | `POST /v1/merge` |
| Compact shard | `tapictl` | `POST /v1/compact` |

> **Key insight**: Initial bootstrap MUST use static membership. The dynamic join path causes split-brain when CachingShardDirectory hasn't pushed membership to discovery yet (10s push cycle). The join path is only safe for runtime scaling after discovery is populated.

## CRD Reference

### Spec

```yaml
apiVersion: tapir.tapir.dev/v1alpha1
kind: TAPIRCluster
metadata:
  name: my-cluster
spec:
  image: ghcr.io/mumoshu/tapirs:latest
  discovery:
    replicas: 3
    resources:              # optional
      requests:
        cpu: 100m
        memory: 128Mi
  nodePools:
    - name: default
      replicas: 3
      resources:            # optional
        requests:
          cpu: 500m
          memory: 256Mi
  shards:
    - number: 0
      replicas: 3
      keyRangeEnd: "n"
      storage: memory       # "memory" (default) or "disk"
    - number: 1
      replicas: 3
      keyRangeStart: "n"
```

### Status

```yaml
status:
  phase: Running
  discoveryEndpoint: "srv://my-cluster-discovery.default.svc.cluster.local:6000"
  shardManagerURL: "http://my-cluster-shard-manager.default.svc.cluster.local:9001"
  nodes:
    - name: my-cluster-default-0
      podIP: 10.244.0.5
      ready: true
      adminAddr: 10.244.0.5:9000
      shards:
        - number: 0
          listenAddr: 10.244.0.5:6000
          storage: memory
        - number: 1
          listenAddr: 10.244.0.5:6001
          storage: memory
  shards:
    - number: 0
      readyReplicas: 3
      replicas: ["10.244.0.5:6000", "10.244.0.6:6000", "10.244.0.7:6000"]
      registered: true
  conditions:
    - type: Ready
      status: "True"
```

## Usage

### Prerequisites

- Kubernetes cluster (or Kind for local development)
- `kubectl` configured

### Install CRDs

```sh
make install
```

### Run the operator (out-of-cluster, for development)

```sh
make run
```

### Deploy to cluster

```sh
make deploy IMG=your-registry/tapirs-operator:tag
```

### Connect with tapi client

After the cluster reaches `Running` phase, use the discovery endpoint from status:

```sh
kubectl get tapircluster my-cluster -o jsonpath='{.status.discoveryEndpoint}'
# srv://my-cluster-discovery.default.svc.cluster.local:6000

# From an in-cluster pod:
tapi client --discovery-tapir-endpoint srv://my-cluster-discovery.default.svc.cluster.local:6000
```

### Development

```sh
# Install kubebuilder and kind
make install-tools

# Generate CRD manifests and code
make manifests generate

# Run tests
make test

# Run E2E tests (creates a Kind cluster)
make test-e2e
```
