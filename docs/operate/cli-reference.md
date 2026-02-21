# CLI Reference

```
  +-----------------------------------------------------+
  |                    tapirs Cluster                    |
  |                                                     |
  |  +-----------+     +---------------+     +-------+  |
  |  | Discovery |<--->| Shard Manager |<--->| Nodes |  |
  |  +-----------+     +---------------+     +-------+  |
  |       ^                    ^                 ^       |
  +-------|--------------------|-----------------+-------+
          |                    |                 |
     tapi discovery      tapi shard-manager   tapi node
     tapi admin          tapictl              tapi client
                                              tapiadm
```

**P1 -- Three binaries:** A tapirs cluster has four logical components -- discovery service, shard manager, node replicas, and clients -- packaged into three binaries. `tapi` is the main binary that runs all server-side components (nodes, discovery, shard manager) and all client-side tools (REPL, backup/restore, admin commands). `tapictl` is the control plane CLI for shard lifecycle operations like split, merge, and compact. `tapiadm` orchestrates Docker-based test clusters for local development.

**P2 -- Configuration:** All binaries accept configuration via TOML files or CLI flags (flags override TOML values). Common flags include `--address` for listen/connect addresses, `--discovery-url` for the discovery backend endpoint, and transport selection flags. DNS-based discovery is supported natively for Kubernetes headless services. See the [Configuration reference](cli-config.md) for the full TOML format and all available options.

**P3 -- Per-binary reference pages:** Each binary has its own reference page with links to individual subcommand docs: [tapi](cli-tapi.md) (server + client, 5 subcommands), [tapictl](cli-tapictl.md) (shard operations, 4 subcommands), [tapiadm](cli-tapiadm.md) (Docker orchestration), and [Configuration](cli-config.md) (TOML format and precedence rules).

| Binary | Purpose | Key subcommands |
|--------|---------|-----------------|
| [tapi](cli-tapi.md) | Server components + client tools | `node`, `client`, `admin`, `discovery`, `shard-manager` |
| [tapictl](cli-tapictl.md) | Shard lifecycle operations | `split`, `merge`, `compact`, `solo` |
| [tapiadm](cli-tapiadm.md) | Docker cluster orchestration | `docker up`, `docker down`, `docker add`, `docker remove` |
