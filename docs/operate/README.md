# Operate tapirs

```
                          Ops Workflow
  +--------+   +-----------+   +---------+   +-------+   +--------+
  | Deploy |-->| Configure |-->| Monitor |-->| Scale |-->| Backup |
  +--------+   +-----------+   +---------+   +-------+   +--------+
      |              |              |             |            |
      v              v              v             v            v
   testbed        TOML +         metrics      split /      tapi admin
   tapiadm       CLI flags       alerts       merge        backup
```

**What's here:** This section covers everything you need to deploy, configure, and manage a tapirs cluster. Whether you're spinning up a local testbed for development or planning a multi-node production deployment, the guides here walk you through the process step by step.

**Getting started:** See the [Getting Started](getting-started.md) guide to pick the right setup for your environment — Docker, single-node, Kubernetes, or Kubernetes with the operator. Once you're up and running, the [CLI Reference](cli-reference.md) documents all three binaries (`tapi`, `tapictl`, `tapiadm`) with individual subcommand pages, and the [TOML configuration](cli-config.md) format.

**What's coming:** Future additions to this section will cover monitoring and observability, troubleshooting common issues, and performance tuning. For now, the testbed guides and CLI reference provide everything needed to get started and explore tapirs' capabilities.

- [Getting Started](getting-started.md) -- pick a deployment option
- [Getting Started: Docker Testbed](getting-started-testbed.md) -- multi-node cluster in under a minute
- [Getting Started: Single-Node Mode](getting-started-testbed-solo.md) -- no Docker, no network
- [Getting Started: Kubernetes Testbed](getting-started-testbed-kube.md) -- Kind + StatefulSets
- [Getting Started: Kubernetes Operator](getting-started-testbed-kube-operator.md) -- Helm + TAPIRCluster CR
- [Testbed Prerequisites](getting-started-testbed-prerequisites.md) -- tools needed for each testbed variant
- [CLI Reference](cli-reference.md) -- all three binaries and their subcommands
- [Configuration](cli-config.md) -- TOML format and CLI flag precedence
- [Requirements](requirements.md) -- hardware, cluster sizing
- [Monitoring](monitoring.md) -- metrics, health checks, alerting
- [Troubleshooting](troubleshooting.md) -- common symptoms and remedies
