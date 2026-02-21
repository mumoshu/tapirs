# tapi discovery

```
  +--------+     register      +-----------+     query      +--------+
  | Node 1 |------------------>|           |<---------------| Client |
  +--------+                   |           |                +--------+
  +--------+     register      | Discovery |     query      +--------+
  | Node 2 |------------------>|  Service  |<---------------| Client |
  +--------+                   |           |                +--------+
  +--------+     register      |           |     query      +--------+
  | Node 3 |------------------>|           |<---------------| Client |
  +--------+       sync        +-----------+                +--------+
```

**P1 -- Central registry:** `tapi discovery` runs the cluster discovery service -- the central registry where nodes register their shard replicas and clients look up where to send transactions. In production with TAPIR-backed discovery, this service itself runs on a tapirs cluster, providing linearizable writes for shard registration and eventually consistent reads for directory queries. For simpler setups, the discovery service can serve from a static JSON file or DNS resolution.

**P2 -- Key flags:** Key flags: `--address` sets the listen address, `--backend` selects the discovery backend type (static, dns, or tapir), and `--sync-interval` controls how often the caching layer refreshes from the remote backend. The discovery service is typically the first component started in a cluster -- nodes and clients need it to find each other.

**P3 -- Related docs:** For the full discovery architecture, see [Discovery internals](../learn/internals/discovery.md). For the eventual consistency model, see [Consistency](../learn/concepts/consistency.md). Back to [tapi](cli-tapi.md).

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `127.0.0.1:9100` | Listen address for the discovery service |
| `--backend` | `static` | Discovery backend type (`static`, `dns`, `tapir`) |
| `--sync-interval` | `5s` | How often the cache refreshes from the backend |
| `--config` | (none) | Path to TOML configuration file |
