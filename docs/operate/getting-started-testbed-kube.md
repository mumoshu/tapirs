# Getting Started: Kubernetes Testbed

```
              Kind Cluster

StatefulSet: tapir-discovery (3 pods)
+----------+  +----------+  +----------+
| disc-0   |  | disc-1   |  | disc-2   |
+----------+  +----------+  +----------+

Deployment: tapir-shard-manager (1 pod)
+------------------+
| shard-manager    |
+------------------+

StatefulSet: tapir-node (3 pods)
+----------+  +----------+  +----------+
|  node-0  |  |  node-1  |  |  node-2  |
| shard 0a |  | shard 0b |  | shard 0c |
| shard 1a |  | shard 1b |  | shard 1c |
+----------+  +----------+  +----------+
      ^
      |
+----------+
|  Client  |
| kubectl  |
| run ...  |
+----------+
```

**Bootstrap a cluster:** This testbed deploys tapirs onto Kubernetes using raw StatefulSets and shell-driven bootstrapping — no operator, no Helm. It creates a Kind cluster, builds the Docker image, deploys a 3-node discovery store and a 3-node data cluster with 2 shards (replication factor 3), bootstraps static membership, registers shards, and runs a smoke test. The entire process takes a few minutes. If you prefer an operator-managed deployment, see [Kubernetes Operator Testbed](getting-started-testbed-kube-operator.md) instead. For Docker without Kubernetes, see the [Docker testbed](getting-started-testbed.md).

**Prerequisites:** Docker, kubectl, [Kind](https://kind.sigs.k8s.io/). The script auto-installs Kind's cluster when `TAPIR_KIND=1`.

```
$ TAPIR_KIND=1 scripts/testbed-kube.sh up
==> Creating Kind cluster 'tapir'...
    OK: Kind cluster created.
==> Building Docker image 'tapir:latest'...
    OK: Docker image built.
==> Loading image into Kind cluster...
    OK: Image loaded into Kind.
==> Deploying discovery store (3 pods)...
    OK: Discovery store bootstrapped (3 replicas).
==> Deploying shard-manager and data nodes (3 pods)...
    OK: All data node replicas bootstrapped.
==> Running smoke test...
    OK: Smoke test passed: read returned 'world'.
```

**Connect and transact:** Once the cluster is running, start an interactive REPL inside the cluster using `kubectl run`:

```
$ kubectl run -n tapir tapir-client -it --rm --restart=Never \
    --image=tapir:latest --image-pull-policy=IfNotPresent -- \
    client --discovery-tapir-endpoint srv://tapir-discovery.tapir.svc.cluster.local:6000

tapi> begin
Transaction started.
tapi> put user:1 alice
OK
tapi> put user:2 bob
OK
tapi> commit
Transaction committed (ts=1719432000.000000001).
tapi> begin ro
Transaction started (read-only).
tapi> get user:1
alice
tapi> scan user:1 user:9
user:1 = alice
user:2 = bob
tapi> abort
Transaction aborted.
```

For scripted one-liners, use `-e`:

```
$ kubectl run -n tapir tapir-txn -i --rm --restart=Never \
    --image=tapir:latest --image-pull-policy=IfNotPresent -- \
    client --discovery-tapir-endpoint srv://tapir-discovery.tapir.svc.cluster.local:6000 \
    -e "begin; put counter 42; commit"
```

**Explore further:** The `demo` command runs a full scenario suite — data seeding, cross-shard transactions, range scans, adding a 4th node, view changes, shard splitting, and cluster backup:

```
scripts/testbed-kube.sh demo
```

Check cluster health at any time:

```
scripts/testbed-kube.sh status
```

Add a new data node dynamically (via shard-manager join):

```
scripts/testbed-kube.sh add-node
```

**Configuration:** Customize the testbed with environment variables:

| Variable | Default | Description |
|---|---|---|
| `TAPIR_KIND` | `0` | Set `1` to auto-create/destroy Kind cluster |
| `TAPIR_KIND_CLUSTER` | `tapir` | Kind cluster name |
| `TAPIR_NAMESPACE` | `tapir` | Kubernetes namespace |
| `TAPIR_NODES` | `3` | Number of data nodes |
| `TAPIR_SHARDS` | `2` | Number of shards |
| `TAPIR_IMAGE` | `tapir:latest` | Container image |
| `TAPIR_BUILD_IMAGE` | `1` | Build Docker image (set `0` to skip) |

**Tear down:**

```
scripts/testbed-kube.sh down
```

This deletes the `tapir` namespace and (if `TAPIR_KIND=1`) the Kind cluster.

See the [CLI Reference](cli-reference.md) for all binary options and the [Docker testbed](getting-started-testbed.md) for the non-Kubernetes equivalent.
