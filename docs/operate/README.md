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

**P1 -- What's here:** This section covers everything you need to deploy, configure, and manage a tapirs cluster. Whether you're spinning up a local testbed for development or planning a multi-node production deployment, the guides here walk you through the process step by step.

**P2 -- Getting started:** Start with the Getting Started guides: [multi-node Docker testbed](getting-started-testbed.md) bootstraps a full cluster in under a minute, or [single-node mode](getting-started-testbed-solo.md) runs hundreds of shard replicas in a single process for local development and testing. Once you're up and running, the [CLI Reference](cli-reference.md) documents all three binaries (`tapi`, `tapictl`, `tapiadm`) with individual subcommand pages, and the [TOML configuration](cli-config.md) format.

**P3 -- What's coming:** Future additions to this section will cover Kubernetes deployment with DNS-based discovery, monitoring and observability, troubleshooting common issues, and performance tuning. For now, the testbed guides and CLI reference provide everything needed to get started and explore tapirs' capabilities.

- [Getting Started: Docker Testbed](getting-started-testbed.md) -- multi-node cluster in under a minute
- [Getting Started: Single-Node Mode](getting-started-testbed-solo.md) -- no Docker, no network
- [CLI Reference](cli-reference.md) -- all three binaries and their subcommands
- [Configuration](cli-config.md) -- TOML format and CLI flag precedence
- [Requirements](requirements.md) -- hardware, cluster sizing
- [Monitoring](monitoring.md) -- metrics, health checks, alerting
- [Troubleshooting](troubleshooting.md) -- common symptoms and remedies
