# tapiadm docker

```
          Docker Cluster Lifecycle

  +----+     +----------+     +-----------+     +------+
  | up |---->| running  |---->| add/remove|---->| down |
  +----+     |          |     |   nodes   |     +------+
   build     | 3 nodes  |     |           |      stop
   network   | 2 shards |     | elasticity|      cleanup
   bootstrap | discovery|     | testing   |
             +----------+     +-----------+
```

**P1 -- Full cluster lifecycle:** `tapiadm docker` manages the full lifecycle of a Docker-based tapirs cluster. `up` builds the Docker image, creates a bridge network, starts node containers, bootstraps the discovery service and shard manager, and wires everything together. `down` stops all containers and cleans up the network. `add node` spins up a new node container and joins it to the existing cluster. `remove node` gracefully removes a node. `get nodes` and `get replicas` list the current cluster topology.

**P2 -- Configurable testbed:** The testbed is configurable: set the number of nodes, the number of shards, and the replication factor at `up` time. Port assignments are handled automatically so multiple testbeds can coexist on the same machine. The generated configuration wires each node to the discovery service and assigns shard key ranges evenly across the cluster. After `up`, you can dynamically add and remove nodes to test elasticity, resharding, and failure recovery.

**P3 -- Related docs:** For a guided walkthrough (including REPL examples and resharding demos), see [Getting Started](getting-started-testbed.md). For shard operations against a running testbed, see [tapictl](cli-tapictl.md). Back to [tapiadm](cli-tapiadm.md). Key file: `src/bin/tapiadm/docker.rs`

| Subcommand | Purpose | Key flags |
|------------|---------|-----------|
| `up` | Build, start, and bootstrap a cluster | `--nodes`, `--shards`, `--replication-factor` |
| `down` | Stop and remove all cluster resources | -- |
| `add node` | Spin up a new node container | `--name` |
| `remove node` | Stop and remove a node container | `--name` |
| `get nodes` | List nodes with IPs and admin ports | -- |
| `get replicas` | List all replicas across nodes | -- |
