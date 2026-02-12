# Migration from Nightly to Stable Rust

## Overview

This project was migrated from nightly Rust (`nightly-2023-06-08`) to stable Rust 1.93.0, along with an edition upgrade from Rust 2021 to Rust 2024.

## Why

The project previously required nightly Rust due to five unstable feature gates. By early 2026, all of these features had either been stabilized or had stable alternatives, making it possible to compile on the stable toolchain.

## Edition Upgrade: 2021 to 2024

The Rust edition was upgraded from 2021 to 2024 (stabilized in Rust 1.85.0). This was required because `let_chains` is only available in edition 2024 on stable Rust. The edition upgrade also resolved a lifetime capture error in `src/ir/client.rs`: in edition 2024, opaque types in return position (`impl Trait`) capture all in-scope lifetimes by default, which fixed an `E0700` error where a returned `impl Future` implicitly captured the `&self` lifetime.

## Nightly Features Removed

### `return_position_impl_trait_in_trait` (RPITIT)

Stabilized in Rust 1.75.0. This feature allowed using `impl Trait` in trait method return positions, which is used extensively throughout the codebase for async trait methods. The feature gate was simply removed.

### `int_roundings`

Stabilized in Rust 1.73.0. This provided the `div_ceil()` method on integer types, used in `src/ir/membership.rs` for quorum size calculations. The feature gate was simply removed.

### `let_chains`

Stabilized in Rust 1.88.0, but only available in edition 2024. This feature allows `&&`-chaining `let` statements inside `if` and `while` conditions. Used in 10 places across `src/occ/store.rs`, `src/ir/replica.rs`, `src/ir/client.rs`, `src/tapir/replica.rs`, and `src/tapir/client.rs`. The feature gate was removed and the edition was upgraded to 2024.

### `btree_cursors`

The old element-based BTreeMap cursor API (used in `src/mvcc/store.rs` and `src/occ/store.rs`) was replaced with the stable `range()`/`range_mut()` API. The old API provided methods like `upper_bound().key_value()` and `lower_bound().key()` that positioned a cursor at an element. These were replaced as follows:

| Old (nightly cursor API) | New (stable range API) |
|---|---|
| `map.upper_bound(Bound::Included(&x)).key_value()` | `map.range(..=x).next_back()` |
| `map.upper_bound(Bound::Included(&x)).value()` | `map.range(..=x).next_back().map(\|(_, v)\| v)` |
| `map.upper_bound_mut(Bound::Included(&x)).value_mut()` | `map.range_mut(..=x).next_back()` |
| `map.lower_bound(Bound::Excluded(&x)).key()` | `map.range((Bound::Excluded(&x), Bound::Unbounded)).next().map(\|(k, _)\| k)` |
| `cursor.peek_next()` (after upper_bound) | `map.range((Bound::Excluded(&key), Bound::Unbounded)).next()` |

### `never_type`

The never type (`!`) was used as a type parameter in `std::future::pending::<!>()` in `src/tapir/client.rs`. Since the never type is not stabilized as a type parameter on stable Rust, this doesn't compile without `#![feature(never_type)]`.

The original code worked because `!` coerces to any type, so the `select!` macro could unify both arms -- the `pending::<!>().await` arm (type `!`) coerced to match the other arm's `Option<Timestamp>`. The replacement uses `std::future::pending::<Option<Timestamp>>()` instead, directly matching the expected type. Since `pending()` never resolves, the type parameter has no runtime effect -- it only needs to satisfy the type checker.

## Edition 2024 Adaptations

Beyond removing the nightly feature gates, the edition 2024 upgrade required additional changes:

### `gen` reserved keyword

In edition 2024, `gen` is a reserved keyword (for future generator syntax). All calls to `rand::Rng::gen()` were changed to use the raw identifier syntax: `thread_rng().r#gen()`. This affected `src/ir/client.rs`, `src/tapir/client.rs`, `src/ir/tests/lock_server.rs`, and `src/bin/maelstrom.rs`.

### Lifetime capture in `impl Trait` return types

In edition 2024, `impl Trait` in return position captures all in-scope lifetimes by default (unlike edition 2021 where only type parameters were captured). This caused errors where methods returning `impl Future` appeared to borrow `&self` longer than intended, even though the returned futures only used `Arc`-cloned data.

The fix was to add explicit `use<>` bounds (stabilized in Rust 1.82.0) to opt out of lifetime capture on specific methods:

- `ir/client.rs`: `invoke_inconsistent` (`+ use<U, T>`), `invoke_consensus` (`+ use<U, T, D>` with named type parameter for the `decide` closure)
- `tapir/shard_client.rs`: `prepare` and `end` (`+ use<K, V, T>`)
- `tapir/client.rs`: `commit_inner` (`+ use<K, V, T>`)

### `clippy::never_loop` in `tapir/client.rs::get`

The `get` method contained a `loop` that never actually looped -- every code path through the body ended with `return`. This was pre-existing but only became a deny-by-default clippy error (`clippy::never_loop`) in newer Rust. Fixed by removing the unnecessary `loop` wrapper.

## CI Update

The GitHub Actions workflow (`.github/workflows/test.yml`) was updated:
- Toolchain changed from `nightly-2023-06-08` to `1.93.0`
- `actions-rs/toolchain@v1` (deprecated) replaced with `dtolnay/rust-toolchain@stable`
- `actions/checkout@v2` updated to `actions/checkout@v6`
