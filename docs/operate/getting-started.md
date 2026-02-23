# Getting Started

Pick the setup that matches your environment. Each guide takes you from zero to a running cluster with a working transaction in under five minutes.

| Guide | What it does | Prerequisites |
|---|---|---|
| [Docker Testbed](getting-started-testbed.md) | 3-node cluster with 2 shards on Docker | Docker |
| [Single-Node Mode](getting-started-testbed-solo.md) | All replicas in one process, no network | Rust toolchain (`cargo build`) |
| [Kubernetes Testbed](getting-started-testbed-kube.md) | Shell-driven cluster on Kind with StatefulSets | Docker, kubectl, Kind |
| [Kubernetes Operator](getting-started-testbed-kube-operator.md) | Operator + Helm charts on Kind | Docker, kubectl, Helm, Kind |

**Docker Testbed** is the fastest option — `scripts/testbed.sh up` builds the image and bootstraps a full cluster in about a minute. **Single-Node Mode** needs no Docker at all — `tapi node --solo` runs everything in one process. The **Kubernetes** options deploy onto a Kind cluster; the manual testbed uses raw StatefulSets while the operator testbed uses Helm and a `TAPIRCluster` custom resource.

Once you're up and running, see the [CLI Reference](cli-reference.md) for all binary options and the [Configuration](cli-config.md) guide for TOML format and flag precedence.
