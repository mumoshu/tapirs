# Configuration

```
         Config Precedence (highest to lowest)

  +------------------+
  |   CLI Flags      |  --address 0.0.0.0:9000
  +------------------+
          |
          v  overrides
  +------------------+
  |   TOML File      |  tapirs.toml
  +------------------+
          |
          v  overrides
  +------------------+
  | Embedded Defaults|  sensible dev defaults
  +------------------+
```

**Three-tier precedence:** tapirs uses a three-tier configuration system: CLI flags take highest priority, then TOML configuration files, then embedded defaults. This means you can run tapirs with zero configuration for development (defaults handle everything), add a TOML file for reproducible cluster setups, and override specific values with CLI flags for one-off testing. All three tiers use the same option names, so switching between them is straightforward.

**Per-component options:** Configuration is organized by component. Node configuration covers the listen address, data directory path, replication factor, and transport selection. Client configuration specifies the discovery URL (how to find the cluster), connection timeout, and retry policy for transient failures. Discovery configuration selects the backend type (`file://` for static JSON, `dns://` for Kubernetes headless services, `tapirs://` for TAPIR-backed self-discovery) and the sync interval that controls how often the caching layer refreshes. All paths and URLs follow standard scheme conventions.

**Related docs:** For how the discovery backends work under the hood, see [Discovery internals](../learn/internals/discovery.md). For per-binary flag documentation, see [tapi](cli-tapi.md), [tapictl](cli-tapictl.md), and [tapiadm](cli-tapiadm.md). Back to [CLI Reference](cli-reference.md). Key file: `src/bin/tapi/config.rs`

```toml
# tapirs.toml — example configuration

[node]
address = "0.0.0.0:9000"
data_dir = "/var/lib/tapirs/data"
replication_factor = 3
transport = "tcp"            # "tcp" or "uring"

[discovery]
url = "tapirs://discovery.tapirs.svc:9100"
backend = "tapir"            # "static", "dns", "tapir"
sync_interval = "5s"

[client]
discovery_url = "tapirs://discovery.tapirs.svc:9100"
connect_timeout = "5s"
retry_max_attempts = 3
retry_backoff = "50ms"       # initial backoff, doubles each retry

[shard_manager]
address = "0.0.0.0:9200"
discovery_url = "tapirs://discovery.tapirs.svc:9100"

[storage]
engine = "wisckey"           # "memory" or "wisckey"
vlog_max_size = "1GB"
memtable_size = "64MB"
l0_compaction_trigger = 4
```
