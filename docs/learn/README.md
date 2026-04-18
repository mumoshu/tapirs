# Learn How tapirs Works

```
                          Learn
                            |
          +-----------------+-----------------+
          |                 |                 |
      Concepts          Internals          Roadmap
          |                 |                 |
  +---+---+---+---+   +---+---+---+---+   +---+---+
  |   |   |   |   |   |   |   |   |   |   |   |
TAPIR IR OCC ... ...  Protocol Storage ... Proposed changes
```

**Who this is for:** This section is for readers who want to understand how tapirs works — the ideas behind it, the protocol that makes it possible, and the engineering decisions that shaped the implementation. Whether you're evaluating tapirs for a project, planning to contribute, or just curious about leaderless distributed transactions, this is the place to start.

**Organization:** The material is organized in three layers. [Concepts](concepts/) covers foundational terms and mental models: what strict serializability and linearizability mean in tapirs, how IR consensus achieves fault tolerance without ordering, how OCC detects conflicts, and how the storage, resharding, discovery, and client layers fit together. [Internals](internals/) goes deeper into architecture and design — how each subsystem is implemented, how data flows through the system, and why specific trade-offs were made. [Roadmap](roadmap/) tracks proposed enhancements that are not implemented — design sketches, correctness analyses, and trade-offs for changes being considered but not yet built.

**Suggested reading path:** If you're reading linearly, the suggested path is: [Concepts](concepts/) first (start with [TAPIR](concepts/tapir.md), then [Consistency](concepts/consistency.md), [IR](concepts/ir.md), and the rest), then [Protocol](internals/protocol-tapir.md) → [Paper Extensions](internals/protocol-tapir-paper-extensions.md) → [Custom Extensions](internals/protocol-tapir-custom-extensions.md) → [Storage](internals/storage.md) → [Resharding](internals/resharding.md) → [Discovery](internals/discovery.md) → [Testing](internals/testing.md) → [Architecture Decisions](internals/architecture-decisions.md). Or jump directly to whatever interests you — each doc is self-contained with cross-links to related material. If you're reading the [TAPIR paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) alongside the code, see [Paper Map](paper-map.md) for a section-by-section guide. If you want to read the source code directly, see [Code Tour](code-tour.md) for a recommended reading order.
