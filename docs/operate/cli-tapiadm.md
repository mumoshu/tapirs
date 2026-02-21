# tapiadm

```
                    tapiadm
                       |
                    +--------+
                    | docker |
                    +--------+
                       |
         +----+----+-----+--------+-----+
         |    |    |      |        |     |
        up  down  add   remove   get   get
                  node   node   nodes replicas
```

**P1 -- Cluster orchestration:** `tapiadm` automates cluster orchestration for development and testing. Currently it has one top-level subcommand -- [docker](cli-tapiadm-docker.md) -- which manages Docker Compose-based tapirs clusters. The Docker backend handles image building, container lifecycle, network wiring, discovery bootstrapping, and shard assignment, so you can go from zero to a running multi-node cluster in a single command.

**P2 -- Extensible design:** The design is extensible: `tapiadm` is structured to support additional orchestration backends in the future (e.g., Kubernetes, bare-metal provisioning), each as a top-level subcommand alongside `docker`. For now, Docker provides the fastest path to a local multi-node cluster for development, testing, and demos.

**P3 -- Related docs:** For a guided walkthrough of the Docker testbed, see [Getting Started](getting-started-testbed.md). Back to [CLI Reference](cli-reference.md). Key file: `src/bin/tapiadm/main.rs`

| Subcommand | Purpose | Details |
|------------|---------|---------|
| [docker](cli-tapiadm-docker.md) | Manage Docker Compose-based clusters | up, down, add node, remove node, get nodes/replicas |
