# Backup/Restore: Single-Node Limitation

## Problem

Backup and restore does not work for single-replica shards (f=0, 1 replica).

## Root Cause

1. **Periodic `tick()` triggers view changes via DoViewChange quorum** — this
   requires peers. Single-member shards skip view changes entirely
   ([ir/replica.rs:289-292](../../../src/ir/replica.rs),
   [ir/replica.rs:1078-1083](../../../src/ir/replica.rs)).

2. **CDC deltas are recorded during view changes** — no view changes means
   no deltas for `scan_changes()` to return.

This is not a new limitation introduced by the CDC-based backup. The previous
admin-level backup mechanism had the same constraint.

## Impact

The shard manager's `/v1/scan-changes` endpoint returns an HTTP 400 error
("backup requires multi-replica shards") when any shard has only one replica.

## Potential Fix

A self-directed view change mechanism where a single replica seals its own
overlay without peer coordination. This is an architectural change to the IR
protocol layer — the current IR specification assumes f >= 1 for view changes.
