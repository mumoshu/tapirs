# Getting Started: Kubernetes Operator Testbed

```
              Kind Cluster

tapirs-operator-system namespace
+-------------------+
| tapirs-operator   |  (Deployment)
| (reconciler)      |
+-------------------+
         |
         | reconciles TAPIRCluster CR
         v
tapir namespace
  tapir-discovery    (StatefulSet, 3 pods)
  tapir-shard-manager (Deployment)
  tapir-default      (StatefulSet, 3 pods)
  Services, ConfigMaps

      ^
      |
+----------+
|  Client  |
| kubectl  |
| run ...  |
+----------+
```

**Bootstrap a cluster:** This testbed deploys tapirs using the Kubernetes operator and Helm charts. The operator watches for `TAPIRCluster` custom resources and automatically creates all sub-resources (StatefulSets, Deployments, Services), bootstraps discovery, creates replicas, and registers shards. One command handles everything — building images, creating a Kind cluster, installing the operator via Helm, and creating a `TAPIRCluster`. For a shell-driven Kubernetes deployment without the operator, see [Kubernetes Testbed](getting-started-testbed-kube.md). For Docker without Kubernetes, see the [Docker testbed](getting-started-testbed.md).

**Prerequisites:** [Testbed Prerequisites](getting-started-testbed-prerequisites.md), [Docker](https://docs.docker.com/get-docker/), [kubectl](https://kubernetes.io/docs/tasks/tools/), [Helm](https://helm.sh/docs/intro/install/), [Kind](https://kind.sigs.k8s.io/). The script auto-creates the Kind cluster when `TAPIR_KIND=1`.

```
$ TAPIR_KIND=1 scripts/testbed-kube-operator.sh up
==> Pre-flight: checking required tools...
    OK: All required tools found.
==> Creating Kind cluster 'tapir-op'...
    OK: Kind cluster created.
==> Building TAPIR image 'tapir:latest'...
    OK: TAPIR image built.
==> Building operator image 'tapirs-operator:latest'...
    OK: Operator image built.
==> Installing tapirs-operator Helm chart...
    OK: Operator chart installed.
==> Installing tapirs-cluster Helm chart...
    OK: Cluster chart installed.
==> Waiting for TAPIRCluster 'tapir' to reach Running phase (timeout: 180s)...
    OK: TAPIRCluster is Running.
==> Running smoke test...
    OK: Smoke test passed: read returned 'world'.
```

**Connect and transact:** Once the cluster is running, start an interactive REPL inside the cluster:

```
$ kubectl run -n tapir tapir-client -it --rm --restart=Never \
    --image=tapir:latest --image-pull-policy=IfNotPresent -- \
    client --discovery-tapir-endpoint srv://tapir-discovery.tapir.svc.cluster.local:6000

tapi> begin
Transaction started.
tapi> put user:1 alice
OK
tapi> commit
Transaction committed (ts=1719432000.000000001).
tapi> begin ro
Transaction started (read-only).
tapi> get user:1
alice
```

**TLS mode:** Enable mTLS via cert-manager by setting `TAPIR_TLS=1`. The script installs cert-manager, creates a CA chain, and configures all components with mutual TLS:

```
$ TAPIR_KIND=1 TAPIR_TLS=1 scripts/testbed-kube-operator.sh up
```

When TLS is enabled, the smoke test automatically uses TLS client certificates.

**Inspect the cluster:** Check the operator-managed resources:

```
$ kubectl get tapircluster -n tapir -o wide
NAME    PHASE     SHARDS   NODES
tapir   Running   2        3

$ kubectl describe tapircluster tapir -n tapir

$ scripts/testbed-kube-operator.sh status
```

Watch operator reconciliation logs:

```
$ kubectl logs -n tapirs-operator-system deployment/tapirs-operator -f
```

**Apply code changes:** After modifying source code, rebuild images and rolling-restart all components without tearing down the cluster:

```
scripts/testbed-kube-operator.sh apply-code-changes
```

**Configuration:** Customize the testbed with environment variables:

| Variable | Default | Description |
|---|---|---|
| `TAPIR_KIND` | `0` | Set `1` to auto-create/destroy Kind cluster |
| `TAPIR_KIND_CLUSTER` | `tapir-op` | Kind cluster name |
| `TAPIR_NAMESPACE` | `tapir` | Namespace for TAPIRCluster |
| `TAPIR_OPERATOR_NS` | `tapirs-operator-system` | Operator namespace |
| `TAPIR_IMAGE` | `tapir:latest` | TAPIR data plane image |
| `TAPIR_OPERATOR_IMAGE` | `tapirs-operator:latest` | Operator image |
| `TAPIR_BUILD_IMAGES` | `1` | Build Docker images (set `0` to skip) |
| `TAPIR_CLUSTER_NAME` | `tapir` | TAPIRCluster resource name |
| `TAPIR_TLS` | `0` | Enable mTLS via cert-manager |

**Tear down:**

```
scripts/testbed-kube-operator.sh down
```

This uninstalls both Helm charts, deletes namespaces, cleans up cert-manager (if TLS was enabled), and deletes the Kind cluster (if `TAPIR_KIND=1`).

See the [CLI Reference](cli-reference.md) for all binary options and the [Kubernetes Testbed](getting-started-testbed-kube.md) for the shell-driven alternative.
