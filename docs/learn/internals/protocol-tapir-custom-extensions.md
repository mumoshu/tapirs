# Custom Extensions

```
tapirs Custom Extensions (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  Range Scan + Phantom-Write Protection                   |
|                                                          |
|  Client: scan(start_key, end_key)                        |
|       |                                                  |
|       v                                                  |
|  RoutingClient: split range per shard, merge results     |
|       |                                                  |
|       v                                                  |
|  OCC Store: record (start_key, end_key, scan_ts)         |
|             in range_reads                               |
|       |                                                  |
|       v  (at prepare time)                               |
|  Scan-Set Check: reject writes into scanned ranges       |
|                  --> prevents phantom writes              |
|                                                          |
+----------------------------------------------------------+
```

**P1 -- Phantom-write protection:** Range scan with phantom-write protection is the primary custom extension beyond the original paper. During a scan, the OCC store records a `(start_key, end_key, scan_ts)` triple in `range_reads`. At prepare time, writes into a previously-scanned range are rejected -- preventing phantom writes that could invalidate scan results. This extends the paper's read-set/write-set validation with a scan-set check, using the same OCC conflict detection framework.

**P2 -- Leaderless integration:** The phantom protection integrates naturally with TAPIR's leaderless model: each replica independently validates scan-set conflicts at prepare time, just like read-set and write-set conflicts. No coordination beyond the existing IR consensus is needed. For multi-shard scans, `RoutingClient` splits the range per shard and merges results transparently.

**P3 -- Related docs:** See [Paper Extensions](protocol-tapir-paper-extensions.md) for the original paper's extensions, [OCC concepts](../concepts/occ.md) for the full conflict detection rules, and [Resharding](resharding.md) for how scan protection interacts with shard splits (range_reads watermarks are transferred to target shards). Key files: `src/occ/store.rs` (scan-set OCC check), `src/tapir/routing_client.rs` (multi-shard scan merge).

| Extension | What it does | Key file |
|-----------|-------------|----------|
| Range scan | Multi-key ordered read within `[start_key, end_key]` | `tapir/routing_client.rs`, `tapir/shard_client.rs` |
| Phantom-write protection | `range_reads` tracking + scan-set OCC check at prepare time | `occ/store.rs` |
| Multi-shard scan merge | RoutingClient splits range per shard, merges results | `tapir/routing_client.rs` |
| Scan protection transfer | range_reads watermarks transferred during resharding | `tapir/shard_manager_cdc.rs` |
