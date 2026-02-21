# Client Architecture

```
  Application
      |
      v
  +----------------+
  | RoutingClient  |  Maps keys to shards, retries OutOfRange
  +-------+--------+
          |
    +-----+------+------+
    |            |       |
    v            v       v
  +--------+ +--------+ +--------+
  | Shard  | | Shard  | | Shard  |  Per-shard RPC stubs
  | Client | | Client | | Client |
  +---+----+ +---+----+ +---+----+
      |          |           |
      v          v           v
  +--------+ +--------+ +--------+
  |   IR   | |   IR   | |   IR   |  IR protocol (propose/finalize)
  | Client | | Client | | Client |
  +---+----+ +---+----+ +---+----+
      |          |           |
      v          v           v
   Replicas   Replicas   Replicas
```

**P1 — Client as coordinator:** The client is the transaction coordinator in TAPIR — unlike leader-based systems, there is no server-side coordinator, so the client drives the entire transaction lifecycle. The client stack is organized in three layers: `RoutingClient` is the top-level interface that maps plain keys to the correct shard, `ShardClient` wraps `IrClient` to translate TAPIR operations (prepare, commit, abort) into IR consensus messages, and `IrClient` at the bottom implements the IR protocol itself (sending proposals, collecting quorum responses, running the decide function).

**P2 — Routing and recovery:** The `RoutingClient` handles two important concerns transparently. First, during resharding, a key's owning shard may change — when this happens, the replica returns `OutOfRange` and the `RoutingClient` re-reads the discovery directory and retries the operation with exponential backoff, routing to the new shard. Second, multi-shard range scans are split per shard at the routing layer and merged back into a single sorted result, so the caller sees a seamless scan across shard boundaries. If a client crashes mid-transaction, backup coordinators at replicas detect the orphaned prepare and either commit (if all shards returned PREPARE-OK) or abort the transaction — no manual intervention required.

**P3 — Related docs:** This is the last concept doc. For the full transaction lifecycle from the client's perspective, see the [Protocol deep-dive](../internals/protocol-tapir.md). For how OCC conflict detection works at the replicas the client talks to, see [OCC](occ.md). For how `OutOfRange` errors arise during resharding, see [Resharding](resharding.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| RoutingClient | High-level client — routes keys to shards, retries on OutOfRange, merges multi-shard scans | `tapir/routing_client.rs` |
| ShardClient | Per-shard RPC stub wrapping IrClient — converts TAPIR ops to IR messages | `tapir/shard_client.rs` |
| Backup Coordinator | Recovers in-flight transactions after client crash — checks prepare status at replicas, commits if all prepared | `tapir/replica.rs` |
| DynamicRouter | Maps keys to shards using current directory. Re-reads directory on OutOfRange | `tapir/dynamic_router.rs` |
