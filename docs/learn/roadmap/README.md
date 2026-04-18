# Roadmap

This section tracks **proposed enhancements that are not implemented**. Each item documents a potential change, its motivation, correctness analysis, and trade-offs — but none of these are live in the codebase. Use this as a design log: start here when a piece of existing documentation says "not implemented" or "could be extended."

## Format

Each roadmap item follows the same structure:

- **Status** — `proposed` (idea captured, no design consensus), `planned` (design agreed, not yet built), or `parked` (explicitly deferred with reasons).
- **Problem** — what currently happens and why it's suboptimal.
- **Proposal** — the concrete change.
- **Correctness analysis** — crash, concurrency, and semantic implications.
- **Trade-offs** — what the proposal costs.
- **Where the change lives** — file paths and rough scope.
- **Related** — pointers to docs and code touched.

Items remain here until either landed (then removed, with a pointer from the superseded doc) or explicitly parked (then annotated).

## Current items

- [Unified manifest write — eliminate IR/TAPIR seal gap](unified-manifest-write.md) — collapse the two sequential manifest fsyncs in a combined-store flush into one, eliminating the crash window where the persisted manifest references fresh IR but stale TAPIR state.
- [Bounded-history compaction](bounded-history-compaction.md) — extend `tapictl compact shard` with `--drop-old-versions-before <TS>` so superseded MVCC versions are dropped instead of being re-shipped to fresh replicas.
