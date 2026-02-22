# Paper Map

```
  TAPIR Paper                        tapirs Source
  +-----------+                      +---------------------------+
  | +3  IR    | ------------------>  | src/ir/replica.rs         |
  | +4  TAPIR | ------------------>  | src/tapir/replica.rs      |
  | +5  Impl  | ------------------>  | src/tapir/client.rs       |
  | +6  Ext   | ------------------>  | src/tapir/shard_client.rs |
  +-----------+                      +---------------------------+
```

**What this is:** This document maps each section of the [TAPIR paper](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf) to the corresponding tapirs source files and documentation pages. If you're reading the paper and want to see how a specific algorithm or protocol step is implemented, this is your index. If you're reading the code and want to understand the theoretical justification, follow the paper references back to the original proofs.

**Paper structure:** The TAPIR paper has six main sections: &sect;3 (IR protocol), &sect;4 (TAPIR transaction protocol), &sect;5 (implementation details including coordinator recovery), &sect;6 (extensions: read-only transactions, retry timestamps), and &sect;7 (evaluation). tapirs implements all of &sect;3&ndash;&sect;6, plus custom extensions beyond the paper (range scan with phantom-write protection, online resharding). The table below covers every paper section with its implementation location.

**Related docs:** For a guided reading order through the source code itself (independent of the paper), see [Code Tour](code-tour.md). For concept definitions, see [Concepts](concepts/). For architecture deep-dives, see [Internals](internals/). Back to [Learn](README.md).

| Paper &sect; | Title | Source file(s) | Doc page |
|---------|-------|---------------|----------|
| &sect;3.1 | IR Interface | `src/ir/replica.rs`, `src/ir/record.rs` | [IR concepts](concepts/ir.md) |
| &sect;3.2 | IR Protocol | `src/ir/replica.rs`, `src/ir/client.rs` | [Protocol](internals/protocol-tapir.md) |
| &sect;3.2.2 | View Change | `src/ir/replica.rs` (handle_do_view_change) | [IR concepts](concepts/ir.md) |
| &sect;3.2.3 | Client Recovery | `src/ir/client.rs` | [IR concepts](concepts/ir.md) |
| &sect;4 | TAPIR Design | `src/tapir/replica.rs` | [TAPIR concepts](concepts/tapir.md) |
| &sect;5.2 | RW Transaction Protocol | `src/tapir/client.rs`, `src/tapir/replica.rs` | [Protocol](internals/protocol-tapir.md) |
| &sect;5.2.3 | Coordinator Recovery | `src/tapir/replica.rs` | [Paper Extensions](internals/protocol-tapir-paper-extensions.md) |
| &sect;5.3.1 | Correctness (Strict Serializability) | `src/occ/store.rs` | [Consistency](concepts/consistency.md) |
| &sect;6.1 | Read-Only Transactions | `src/tapir/shard_client.rs` | [Paper Extensions](internals/protocol-tapir-paper-extensions.md) |
| &sect;6.4 | Retry Timestamp Selection | `src/tapir/client.rs` | [Paper Extensions](internals/protocol-tapir-paper-extensions.md) |
| Fig. 9 | TAPIR-OCC-CHECK | `src/occ/store.rs` | [OCC concepts](concepts/occ.md) |
| Fig. 10 | TAPIR-DECIDE | `src/tapir/client.rs` (tapir_decide) | [Protocol](internals/protocol-tapir.md) |
| Fig. 13 | Recovery DECIDE | `src/tapir/replica.rs` | [Paper Extensions](internals/protocol-tapir-paper-extensions.md) |
| &mdash; | Range Scan (beyond paper) | `src/occ/store.rs`, `src/tapir/routing_client.rs` | [Custom Extensions](internals/protocol-tapir-custom-extensions.md) |
| &mdash; | Online Resharding (beyond paper) | `src/tapir/shard_manager_cdc.rs` | [Resharding](internals/resharding.md) |
| &mdash; | Discovery (beyond paper) | `src/discovery/mod.rs`, `src/discovery/tapir.rs` | [Discovery](internals/discovery.md) |
