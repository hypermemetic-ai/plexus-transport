---
id: UMB-2
title: "Add `pub const VERSION` to plexus-core + plexus-macros + plexus-transport"
status: Pending
type: implementation
blocked_by: []
unlocks: [UMB-3]
severity: Low
---

## Problem

The `plexus-rpc` umbrella's `CAPABILITIES` const needs each subcrate's version available at compile time. Today there's no public `VERSION` const on any subcrate; only `Cargo.toml` knows.

## Required behavior

Each subcrate gains a single line at the top of `lib.rs`:

```rust
/// Crate version, populated at compile time from CARGO_PKG_VERSION.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
```

Three subcrates: `plexus-core`, `plexus-macros`, `plexus-transport`. (`plexus-protocol` and `plexus-derive` are out of scope per existing project memory.)

## What must NOT change

- The `Cargo.toml` `version` field is the source of truth; the const is derived from it
- No additional dependencies introduced
- No changes to existing public API

## Acceptance criteria

1. `plexus_core::VERSION` returns the literal string equal to `plexus-core/Cargo.toml`'s `version` field
2. Same for `plexus_macros::VERSION` and `plexus_transport::VERSION`
3. The const is reachable at compile time (usable in another crate's `const` initializer)
4. Each crate's existing test suite still passes

## Completion

Three one-line edits, three commits (one per repo). Trivial.
