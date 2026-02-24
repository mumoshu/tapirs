# Getting Started: Single-Node Mode

```
  +--------------------------------------------------+
  |              Single Process (tapi node --solo)    |
  |                                                  |
  |  +----------+ +----------+ +----------+          |
  |  | Replica  | | Replica  | | Replica  |  Shard 0 |
  |  | 0a       | | 0b       | | 0c       |          |
  |  +----------+ +----------+ +----------+          |
  |  +----------+ +----------+ +----------+          |
  |  | Replica  | | Replica  | | Replica  |  Shard 1 |
  |  | 1a       | | 1b       | | 1c       |          |
  |  +----------+ +----------+ +----------+          |
  |                                                  |
  |  Discovery: in-memory    Transport: channel      |
  +--------------------------------------------------+
              ^
              |
        +----------+
        |  Client  |
        +----------+
```

**Prerequisites:** [Testbed Prerequisites](getting-started-testbed-prerequisites.md), [Rust toolchain](https://rustup.rs/) (stable 1.93.0+).

**No cluster required:** You don't need a multi-node cluster to use tapirs. Single-node mode runs hundreds of shard replicas in a single process, providing the full transaction API with strict serializability -- no Docker, no network, no external dependencies. This is ideal for local development, integration testing, or workloads that need strong transactional guarantees but don't yet require horizontal distribution. Because tapirs uses a single-threaded state machine per replica with sharding for parallelism, running many replicas in one process naturally utilizes multiple cores.

**Launch and connect:** Launch with `tapi node --solo` (or the equivalent TOML configuration). Once running, connect with the same REPL (`tapi client --repl`) or client API that you'd use against a multi-node cluster -- the interface is identical. When your workload outgrows a single node, switch to the [multi-node Docker testbed](getting-started-testbed.md) or a production deployment without changing application code.

**Configuration and architecture:** For a full list of configuration options (shard count, replication factor, data directory, discovery backend), see the [CLI Reference](cli-reference.md) and [Configuration](cli-config.md). For understanding how the single-threaded-per-replica architecture enables this single-node mode, see [Architecture Decisions](../learn/internals/architecture-decisions.md).

```
$ tapi node --solo --shards 2 --replication-factor 3
Starting single-node tapirs (2 shards, replication factor 3)...
Shard 0: replicas 0a, 0b, 0c (key range [0x00, 0x80))
Shard 1: replicas 1a, 1b, 1c (key range [0x80, 0xFF))
Discovery: in-memory
Transport: channel
Listening on 127.0.0.1:9000
Ready.

$ tapi client --repl --address 127.0.0.1:9000
tapirs> begin
Transaction started.
tapirs> put config:theme dark
OK
tapirs> get config:theme
dark
tapirs> commit
Transaction committed (ts=1719432000.000000001).
```
