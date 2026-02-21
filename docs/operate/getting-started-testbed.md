# Getting Started: Docker Testbed

```
              Docker Bridge Network
  +-------------------------------------------+
  |                                           |
  |  +----------+  +----------+  +----------+ |
  |  |  Node 1  |  |  Node 2  |  |  Node 3  | |
  |  | shard 0a |  | shard 0b |  | shard 0c | |
  |  | shard 1a |  | shard 1b |  | shard 1c | |
  |  +----------+  +----------+  +----------+ |
  |       |              |             |       |
  |       v              v             v       |
  |  +-----------+  +---------------+          |
  |  | Discovery |  | Shard Manager |          |
  |  +-----------+  +---------------+          |
  +-------------------------------------------+
              ^
              |
        +----------+
        |  Client  |
        | tapi     |
        | client   |
        | --repl   |
        +----------+
```

**P1 -- Bootstrap a cluster:** The fastest way to try tapirs is the Docker testbed. Run `scripts/testbed.sh up` to build the Docker image, bootstrap a 3-node cluster with 2 shards (replication factor 3), wire up discovery, and start all components. The entire process takes about a minute. When you're done, `scripts/testbed.sh down` tears everything down cleanly. If you don't have Docker or prefer a lighter setup, see the [single-node quick start](getting-started-testbed-solo.md) instead.

**P2 -- Connect and transact:** Once the cluster is running, connect with the interactive REPL: `tapi client --repl`. The REPL opens a transaction session where you can issue `begin`, `get`, `put`, `delete`, `scan`, `commit`, and `abort` commands -- all strict serializable by default. For scripting, use `-e` for one-liner expressions or `-s` to pipe a script file. See the [client reference](cli-tapi-client.md) for the full command list and the [CLI Reference](cli-reference.md) for all binary options.

**P3 -- Explore the architecture:** The testbed is also a good starting point for understanding tapirs' architecture in practice: you can observe view changes by stopping and restarting a node, trigger online resharding with `tapictl split`, and watch the discovery sync propagate shard membership updates across nodes. All of these operations work against the running testbed without any special configuration.

```
$ scripts/testbed.sh up
Building tapirs Docker image...
Starting 3-node cluster with 2 shards (replication factor 3)...
Discovery service started on 172.18.0.2:9100
Node 1 started on 172.18.0.3:9000 (shard 0a, shard 1a)
Node 2 started on 172.18.0.4:9000 (shard 0b, shard 1b)
Node 3 started on 172.18.0.5:9000 (shard 0c, shard 1c)
Shard manager started on 172.18.0.2:9200
Cluster ready.

$ tapi client --repl
tapirs> begin
Transaction started.
tapirs> put user:1 alice
OK
tapirs> put user:2 bob
OK
tapirs> get user:1
alice
tapirs> commit
Transaction committed (ts=1719432000.000000001).
tapirs> begin
Transaction started.
tapirs> scan user:1 user:9
user:1 = alice
user:2 = bob
tapirs> abort
Transaction aborted.
```
