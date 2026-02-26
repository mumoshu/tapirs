# Backup & Restore

TAPIR supports full and incremental backups using CDC (Change Data Capture).
Both modes call `scan_changes(from_view)` on each shard's replicas — full
backups start from view 0, incremental backups start from the last backed-up
view.

## Storage Backends

Backups can be stored on:

- **Local filesystem** (default) — any directory path
- **Amazon S3** or S3-compatible services — paths starting with `s3://`
  (see [S3 Backup Guide](backup-restore-s3.md))

The CLI auto-detects the backend from the path format.

## Quick Start

### Full Backup

```bash
# Via shard manager (managed clusters):
tapictl backup cluster --shard-manager-url http://127.0.0.1:9001 --output /backups/2025-01-15

# Via direct node access (standalone clusters):
tapictl solo backup --admin-addrs 127.0.0.1:9000,127.0.0.2:9000 --output /backups/2025-01-15
```

### Incremental Backup

Run the same command pointing to an existing backup directory.
The CLI detects `cluster.json` and backs up only changes since the last run:

```bash
tapictl backup cluster --shard-manager-url http://127.0.0.1:9001 --output /backups/2025-01-15
```

### Restore

```bash
# Via shard manager:
tapictl restore cluster \
  --shard-manager-url http://127.0.0.1:9001 \
  --admin-addrs 10.0.1.1:9000,10.0.1.2:9000,10.0.1.3:9000 \
  --input /backups/2025-01-15

# Via direct node access:
tapictl solo restore --admin-addrs 10.0.1.1:9000,10.0.1.2:9000 --input /backups/2025-01-15
```

### List Backups

```bash
tapictl get backups --dir /backups
```

## Backup Contents

Each backup directory contains:

- `cluster.json` — metadata: shard list, delta chain, timestamps, replica addresses
- `shard_N_delta_M.bin` — bitcode-serialized CDC delta for shard N, sequence M

## Architecture

For CDC internals, delta merging, and view-cursor mechanics, see
[Backup & Restore Internals](../learn/internals/backup_and_restore.md).
