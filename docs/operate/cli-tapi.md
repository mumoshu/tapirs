# tapi

```
                        tapi
                         |
       +---------+-------+--------+-----------+
       |         |       |        |           |
    +------+ +--------+ +-----+ +-----------+ +--------------+
    | node | | client | |admin| | discovery | |shard-manager |
    +------+ +--------+ +-----+ +-----------+ +--------------+
    server    client     client   server        server
    (long)    (session)  (ops)    (long)         (long)
```

**Server and client in one binary:** `tapi` is the main binary that runs both server-side and client-side components. On the server side, three subcommands start long-running services: [node](cli-tapi-node.md) starts a replica that participates in IR consensus and serves TAPIR transactions, [discovery](cli-tapi-discovery.md) runs the discovery service that nodes register with and sync from, and [shard-manager](cli-tapi-shard-manager.md) runs the coordinator for online resharding operations. On the client side: [client](cli-tapi-client.md) connects to the cluster for interactive or scripted transactions, and [admin](cli-tapi-admin.md) provides operational commands like status, backup, restore, and membership changes.

**Common flags:** All subcommands share common flags: `--address` for listen or connect addresses, `--discovery-url` for the discovery backend (supports `file://`, `dns://`, and `tapirs://` schemes), and transport selection flags. For Kubernetes deployments, DNS discovery resolves headless service hostnames to discover replica endpoints automatically. The binary embeds sensible defaults -- for most development scenarios, only `--address` and `--discovery-url` are needed.

**Related docs:** For the full TOML configuration format (which mirrors the CLI flag structure), see [Configuration](cli-config.md). For how discovery backends work, see [Discovery internals](../learn/internals/discovery.md). Back to [CLI Reference](cli-reference.md). Key file: `src/bin/tapi/main.rs`

| Subcommand | Purpose | Details |
|------------|---------|---------|
| [node](cli-tapi-node.md) | Start a replica hosting one or more shards | Server-side, long-running |
| [client](cli-tapi-client.md) | Transaction shell (REPL, one-liners, scripts) | Client-side |
| [admin](cli-tapi-admin.md) | Status, backup, restore, membership changes | Client-side, operational |
| [discovery](cli-tapi-discovery.md) | Run the cluster discovery service | Server-side, long-running |
| [shard-manager](cli-tapi-shard-manager.md) | Run the resharding coordinator | Server-side, long-running |
