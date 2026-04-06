# Backup and Restore

> For operator usage guide, see [Backup & Restore](../../operate/backup-restore.md).

## Overview

Backup and restore uses the same CDC (Change Data Capture) mechanism as
shard splitting and merging. Both full and incremental backups call
`scan_changes(from_view)` — the only difference is the starting view:

- **Full backup**: `scan_changes(from_view=0)` — all committed changes
- **Incremental backup**: `scan_changes(from_view=V)` — changes since last backup

## Architecture

Three components collaborate:

- **BackupManager** (`src/backup/`): Cross-cluster orchestrator library.
  Handles file I/O, multi-phase restore coordination, and talks to both
  admin servers and shard manager. Created by the CLI.

- **Shard manager server** (`src/sharding/shardmanager_server/`): Provides
  two data-plane endpoints that require a Transport + ShardClient:
  - `POST /v1/scan-changes` — scans all active shards, returns bitcode-serialized `ScanChangesResponse`
  - `POST /v1/apply-changes` — ships CDC deltas to a shard via `ship_changes()`

- **Admin server** (`src/node/node_server/`): Creates empty replicas on
  target nodes during restore via the existing `add_replica` command.

## Backup Flow

```
tapictl backup cluster --shard-manager-url URL --output DIR

BackupManager.backup_cluster(output_dir)
  |
  +-- HttpShardManagerClient.scan_changes(last_backup_views)
  |     POST /v1/scan-changes → shard manager server
  |     Server: list shards → per shard: make_shard_client → scan_changes(from_view)
  |     Returns: bitcode ScanChangesResponse (binary HTTP response)
  |
  +-- Per shard: write shard_N_delta_SEQ.bin (bitcode Vec<LeaderRecordDelta>)
  +-- Write/update cluster.json (JSON metadata with per-shard delta history)
```

The wire format is bitcode binary. Delta bytes flow directly from the shard
manager response to disk files — no re-serialization.

## Restore Flow

```
tapictl restore cluster --shard-manager-url URL --admin-addrs A1,A2,A3 --input DIR

BackupManager.restore_cluster(admin_addrs, backup_dir)
  |
  +-- Phase 1 (Prepare): Per shard, per replica:
  |     send_admin_request(admin_addr, add_replica{shard, listen_addr, membership})
  |     Creates empty replicas on target nodes
  |
  +-- Phase 2 (Apply): Per shard, per delta file:
  |     HttpShardManagerClient.apply_changes(shard, replicas, delta_bytes)
  |     POST /v1/apply-changes → shard manager → make_shard_client → ship_changes()
  |
  +-- Phase 3 (Finalize): Per shard:
        HttpShardManagerClient.register(shard, key_range, replicas)
        POST /v1/register → shard becomes visible in discovery
```

Phase ordering is strict (same pattern as split/merge/compact):
- Prepare before apply: `ship_changes` sends to ALL replicas — they must be running
- Apply before finalize: data must be fully applied before registering as Active

## Consistency Guarantees

`scan_changes(from_view)` uses `invoke_unlogged_quorum` (ALL replicas, f+1
from same view) + `merge_responses` (quorum intersection guarantees complete
history). Each delta is the diff between two view-change-merged states.

Periodic `tick()` in IR replicas triggers natural view changes (~2s interval),
sealing the current overlay into a CDC delta. No explicit `force_view_change()`
is needed in production — past committed operations are already in sealed
deltas by the time `scan_changes()` is called.

## Incremental Backup

`cluster.json` stores per-shard `ShardBackupHistory` with an ordered list of
delta files. Each delta records `from_view` and `effective_end_view`. On the
next backup, `BackupManager` reads `cluster.json` and derives
`last_backup_views` from each shard's last delta's `effective_end_view`.

## Limitations

Backup requires multi-replica shards (3+ replicas, f >= 1). See
[backup_and_restore_single_node_limitation.md](backup_and_restore_single_node_limitation.md)
for details.

## S3-Based Remote Storage

In addition to CDC-based backups, tapirs supports always-on S3 remote storage. When `--s3-bucket` is configured on a node, segments and manifests are uploaded to S3 automatically. This enables zero-copy clone, read replicas, and cross-shard consistent snapshots without explicit backup commands.

### sync_to_remote

Called after every view change flush when S3 is configured. Diffs the before/after manifests to find newly sealed segments, force-uploads them (overwriting stale S3 copies), unconditionally uploads active segments (which grow between seals), and uploads the manifest as a versioned snapshot. Best-effort: logs errors but never blocks the critical path.

### BackupDescriptor

A point-in-time per-shard backup record. `create_backup()` captures the latest manifest view number for each shard. Stored as `backups/{timestamp}.json` on S3. Used by `tapictl backup` to record what was backed up and by `tapictl restore` to know which manifest version to restore from.

### CrossShardSnapshot

A cross-shard consistent snapshot that solves the problem of shards having different `max_committed_ts` values. `create_cross_shard_snapshot()` computes `cutoff_ts = min(ceiling_ts)` across all shards. Per-shard `GhostFilter` structs hide entries in the range `(cutoff_ts, ceiling_ts]` — this ensures every shard exposes a consistent view at `cutoff_ts`.

The `GhostFilter` works by timestamp clamping, not per-item filtering: `clamp_ts(ts)` returns `cutoff_ts` when `ts` is in the ghost range, and `ts` otherwise. Applied in `CombinedStoreInner` before MVCC lookups.

### Zero-Copy Clone (cow_clone)

`clone_from_remote_lazy()` creates a copy-on-write clone of a shard from S3 by downloading only the manifest (not segments). Registers `S3CachingIo` so segments are lazily downloaded on first read. Rewrites the manifest to promote the source's active segment to sealed and creates a fresh active segment for the clone. Writes to the clone don't affect the source.

## Key Files

- [backup/mod.rs](../../../src/backup/mod.rs) — `BackupManager` struct
- [backup/backup_cluster.rs](../../../src/backup/backup_cluster.rs) — `backup_cluster()` method
- [backup/restore.rs](../../../src/backup/restore.rs) — `restore_cluster()` method
- [backup/list_backups.rs](../../../src/backup/list_backups.rs) — `list_backups()` static method
- [backup/types.rs](../../../src/backup/types.rs) — `ClusterMetadata`, `ShardDeltaInfo`, `BackupInfo`
- [shardmanager_server/handle_scan_changes.rs](../../../src/sharding/shardmanager_server/handle_scan_changes.rs) — `/v1/scan-changes` endpoint
- [shardmanager_server/handle_apply_changes.rs](../../../src/sharding/shardmanager_server/handle_apply_changes.rs) — `/v1/apply-changes` endpoint
- [shardmanager_client/scan_changes.rs](../../../src/sharding/shardmanager_client/scan_changes.rs) — Client methods
- [remote_store/sync_to_remote.rs](../../../src/remote_store/sync_to_remote.rs) — `sync_to_remote()` after flush
- [remote_store/backup_descriptor.rs](../../../src/remote_store/backup_descriptor.rs) — `BackupDescriptor`, `create_backup()`
- [remote_store/cross_shard_snapshot.rs](../../../src/remote_store/cross_shard_snapshot.rs) — `CrossShardSnapshot`, `GhostFilter`
- [remote_store/cow_clone.rs](../../../src/remote_store/cow_clone.rs) — `clone_from_remote_lazy()`
- [remote_store/read_replica.rs](../../../src/remote_store/read_replica.rs) — `ReadReplica`
- [remote_store/read_replica_refresh.rs](../../../src/remote_store/read_replica_refresh.rs) — `refresh_once()`, `spawn_refresh_task()`
