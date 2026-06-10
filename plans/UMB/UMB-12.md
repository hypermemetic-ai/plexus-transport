---
id: UMB-12
title: "Tier 2 — facade re-exports for `futures` + `schemars`, shrink `__private` from 4 to 2"
status: Pending
type: implementation
blocked_by: [UMB-11]
unlocks: [UMB-13]
confidence: high
severity: Low
introduces:
  - "plexus_core::stream"
  - "plexus_core::schema"
imports:
  - "plexus_core::__private (owner: UMB-11)"
  - "plexus_core::PluginId (owner: UMB-11)"
---

## Problem

After UMB-11, `plexus_core::__private` re-exports four crates: `jsonrpsee`, `serde_json`, `futures`, `schemars`. Two of those (`futures`, `schemars`) are used by macro emission in narrow, easily-facadeable ways — `futures::stream::{iter, empty, StreamExt}` for streaming method dispatch, and `schemars::Schema` as the schema-generation output type. Moving those names out of `__private` and into explicit `plexus_core::{stream, schema}` modules removes them from the "borrowed runtime" mental model and makes the residual `__private` (just `jsonrpsee` and `serde_json`) honestly named: the two crates plexus-core cannot reasonably hide because their concepts are the wire protocol itself.

## Context

`futures::stream::iter`, `futures::stream::empty`, and `futures::stream::StreamExt` are three concrete items the macro emits as part of dispatch glue. They are not part of the wire format; they are just async-stream primitives the dispatch trait happens to need. Re-exporting them under `plexus_core::stream` is a one-line transformation per call site (`#crate_path::stream::iter` instead of `#crate_path::__private::futures::stream::iter`).

`schemars::Schema` is the schema type returned by `schema_for!`. The macro embeds it in a `OnceLock<serde_json::Value>` cache. Re-exporting `Schema` under `plexus_core::schema::Schema` does not move the `schema_for!` proc-macro — that proc-macro is invoked by the activation macro's expansion and emits *its own* paths into `::schemars::*`. Solving that is UMB-13 / a separate ticket; for Tier 2 the goal is narrower — the macro stops referencing `::schemars::Schema` directly.

## Evidence

The reason `futures` and `schemars` move out of `__private` while `jsonrpsee` and `serde_json` stay is that the first two have shallow surface use in macro emission (single types, single method-call sites), making a thin re-export functionally complete. `jsonrpsee` has `Methods`, `RpcResult`, and (critically) the `#[jsonrpsee::proc_macros::rpc]` proc-macro attribute — wrapping the proc-macro attribute is non-trivial (requires plexus-core to re-export the proc-macro, which proc-macro-crate machinery may complicate). `serde_json` has 31 emission sites spanning many distinct items (`Value`, `Map`, `from_value`, `to_value`, `json!`); each would need an alias in plexus-core's facade. Both belong to a later tier (UMB-13).

The `plexus_core::stream` and `plexus_core::schema` modules created here are **stable public API**, not `__private`. Consumers may use them directly (e.g. when building manual `Stream<Item = …>` return values for activation methods). This makes the facade real surface area, not a hidden detail.

## Required behavior

### Macro emission

| Trigger | Before | After |
|---|---|---|
| Streaming method with `Vec<Event>` materializer | `::futures::stream::iter(events)` | `#crate_path::stream::iter(events)` |
| Streaming method with empty result | `::futures::stream::empty::<…>()` | `#crate_path::stream::empty::<…>()` |
| Streaming method using `.then(...)` or `.map(...)` | `use ::futures::stream::StreamExt;` | `use #crate_path::stream::StreamExt;` |
| Schema cache type | `static SCHEMA_CACHE: OnceLock<::serde_json::Value>` and the assignment uses `::schemars::schema_for!(…)` to populate it | The `OnceLock` type does not change; the schema-emission site references `#crate_path::schema::Schema` instead of `::schemars::Schema` where the macro names the type. The `schema_for!` invocation itself stays for now (UMB-13 problem). |

### `plexus-core` new public surface

- `plexus_core::stream` — re-exports `iter`, `empty`, `StreamExt` from `futures::stream` and `Stream` from `futures`. Documented as "the async-stream surface activation methods return through; pinned to a `futures` major version verified compatible with the rest of the framework."
- `plexus_core::schema` — re-exports `Schema` and `JsonSchema` from `schemars`. Documented similarly.

### `__private` module surface

After UMB-12, `plexus_core::__private` contains exactly two entries: `jsonrpsee`, `serde_json`. The module doc-comment is updated to name those two and explain why they remain (wire-protocol and dispatch concerns owned by UMB-13).

### Consumer-facing change

| Setup | Builds today (post-UMB-11) | Builds after UMB-12 |
|---|---|---|
| Consumer with `plexus-core` + `plexus-macros` only, manually using `futures::Stream` in method signatures | ✓ (consumer adds `futures` to its own deps) | ✓ (consumer can use `plexus_core::stream::Stream` instead; `futures` direct dep optional) |
| Consumer with `plexus-rpc` only | ✓ | ✓ |

No consumer breaks. The facade is additive; consumers can keep importing `futures::stream` directly if they prefer.

## Risks

- **`Stream` is a re-export, not a newtype.** Consumers using both `plexus_core::stream::Stream` and `futures::Stream` get the same trait (no ambiguity in trait resolution), but two different `use` statements that resolve to the same path can confuse readers. *Mitigation:* documentation makes the re-export nature explicit ("`plexus_core::stream::Stream` IS `futures::Stream`").
- **schemars version drift.** If a future schemars major bump introduces an incompatible `Schema` type, the re-export becomes a transitive break for `plexus_core::schema::Schema` consumers. *Mitigation:* the facade lives in plexus-core; a schemars bump is itself a plexus-core SemVer event regardless of whether the type is re-exported.

## What must NOT change

- Wire protocol, schema JSON output, error envelopes: all unchanged.
- `plexus_core::__private`'s status as `#[doc(hidden)]` and unstable.
- `crate_path` default and meaning.
- The `schema_for!` proc-macro invocation (still emitted into `::schemars::schema_for!` paths; UMB-13's problem).

## Acceptance criteria

| # | Check | How to evaluate |
|---|---|---|
| 1 | A new crate with `plexus-core = "0.5.5"` + `plexus-macros = "0.6.1"` and **no `futures` or `schemars` direct dep** can compile a streaming activation that yields `Vec<EventType>`. | `cargo build` succeeds. |
| 2 | A new crate with `plexus-rpc = "0.2.1"` only can do the same. | `cargo build` succeeds. |
| 3 | `plexus_core::__private` re-exports exactly `{jsonrpsee, serde_json}`. | `cargo doc --no-deps -p plexus-core` confirms. |
| 4 | `plexus_core::stream::{iter, empty, StreamExt, Stream}` and `plexus_core::schema::{Schema, JsonSchema}` are documented public API. | `cargo doc` lists them. |
| 5 | `cargo build` and `cargo test` pass green across plexus-core, plexus-macros, plexus-rpc, plexus-transport, plexus-substrate. | CI gate green. |
| 6 | A substrate activation that previously imported `futures::stream::StreamExt` builds unchanged. | Spot-check one activation. |

## Completion

The implementor:
- Lands `plexus-core::stream` and `plexus-core::schema` modules.
- Updates `plexus-macros` codegen to emit through them.
- Ships `plexus-core 0.5.5` and `plexus-macros 0.6.1` to crates.io.
- Ships `plexus-rpc 0.2.1` (transitive pin bump).
- Records criteria 1 / 2 / 3 evidence in the PR.
- Flips this ticket to `Complete` in the same commit.
