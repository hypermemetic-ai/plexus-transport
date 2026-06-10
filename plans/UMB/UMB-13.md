---
id: UMB-13
title: "Tier 3 — eliminate `__private` by owning the wire-format and RPC surface in `plexus-core`"
status: Pending
type: implementation
blocked_by: [UMB-12]
unlocks: []
confidence: medium
severity: Low
introduces:
  - "plexus_core::value::Value"
  - "plexus_core::value::Map"
  - "plexus_core::value::json (macro)"
  - "plexus_core::rpc::Methods"
  - "plexus_core::rpc::RpcResult"
  - "plexus_core::rpc (proc-macro re-export)"
imports:
  - "plexus_core::stream (owner: UMB-12)"
  - "plexus_core::schema (owner: UMB-12)"
---

## Problem

After UMB-12, `plexus_core::__private` still re-exports `jsonrpsee` and `serde_json`. These two crates encode the *wire protocol* (JSON-RPC) and *the JSON data model*. Their presence in `__private` means: anyone reading plexus-core's surface still sees that the implementation chose jsonrpsee + serde_json, and any future swap of either crate is a breaking change for downstream tooling that reaches through `__private` (intentionally or accidentally).

Tier 3 makes plexus-core a **true facade**: the names `jsonrpsee` and `serde_json` do not appear anywhere in macro emission. `__private` is deleted. Consumers depend on `plexus-core` (or `plexus-rpc`) and the implementation details are genuinely encapsulated. The day the framework swaps jsonrpsee for an in-house transport, or moves to `simd-json`, nothing downstream notices.

## Context

The hard part isn't the data model — `Value`, `Map`, `from_value`, `to_value`, and `json!` all have clean equivalents (`serde_json` itself is a re-export-friendly crate; plexus-core would `pub use serde_json::{Value, Map};` plus `pub use serde_json::from_value;` plus a forwarding `json!` macro). 31 emission sites become 31 trivial path-prefix swaps.

The hard part is `jsonrpsee`'s proc-macro attribute `#[jsonrpsee::proc_macros::rpc(server, namespace = ...)]`. Proc-macro attributes can be re-exported through path aliases (since Rust 1.36 they resolve through path navigation), but the attribute then operates on the consumer code with `jsonrpsee::core::server::*` paths in its own expansion. Either:

- **Plexus-core owns the RPC trait surface** and translates to jsonrpsee internally — a real abstraction. The macro emits `#crate_path::rpc::*` paths; plexus-core's `rpc` module implements the RPC server by translating to jsonrpsee under the hood.
- **Plexus-core re-exports the attribute** via `pub use jsonrpsee::proc_macros::rpc;` — the attribute is reachable as `plexus_core::rpc::rpc`, but its *expanded body* still references `::jsonrpsee::*` paths in the consumer crate. The leak isn't closed.

The first approach is structurally honest and yields zero `jsonrpsee` references in any consumer-visible path. It requires plexus-core to define its own `Methods` registry, `RpcResult` type, and the small set of dispatch internals the macro currently uses from `jsonrpsee::core::server`. None of these are conceptually hard; they are mechanically substantial.

## Evidence

The reason this lands as Tier 3 (and not as part of Tier 1 / 2) is the scope of the refactor: 31 serde_json sites + 6 jsonrpsee sites + the design of plexus-core's `rpc` and `value` modules + the verification that no behavioral drift occurs. It is the natural "version 1.0 of plexus-core" boundary — once plexus-core fully owns the wire format and RPC surface, the framework's public API has a chance of stabilizing.

Confidence is `medium` (not `high`) because the rpc surface design is not fully settled: specifically, whether `plexus_core::rpc::Methods` should be a re-export of `jsonrpsee::core::server::Methods` (technically permissible but defeats the abstraction) or a newtype wrapper that translates at the edges. A short spike (UMB-13-output design ticket) is recommended before flipping this to Ready.

The reason `serde_json::json!` is in the introduces list is that the macro emits `serde_json::json!({...})` invocations; plexus-core must offer an equivalent `plexus_core::value::json!` (either a `#[macro_export]` forwarding wrapper or a `pub use serde_json::json;` re-export) so the macro can swap path prefixes without changing semantics.

## Required behavior

### Macro emission

| Trigger | Before (post UMB-12) | After UMB-13 |
|---|---|---|
| Any `serde_json::Value` reference | `::serde_json::Value` | `#crate_path::value::Value` |
| Any `serde_json::Map` reference | `::serde_json::Map<String, Value>` | `#crate_path::value::Map` |
| Any `serde_json::from_value` call | `::serde_json::from_value(...)` | `#crate_path::value::from_value(...)` |
| Any `serde_json::to_value` call | `::serde_json::to_value(...)` | `#crate_path::value::to_value(...)` |
| Any `serde_json::json!` invocation | `::serde_json::json!({...})` | `#crate_path::value::json!({...})` |
| jsonrpsee Methods registry | `::jsonrpsee::core::server::Methods::new()` | `#crate_path::rpc::Methods::new()` |
| jsonrpsee RpcResult bound | `::jsonrpsee::core::RpcResult<T>` | `#crate_path::rpc::RpcResult<T>` |
| `#[jsonrpsee::proc_macros::rpc(...)]` attribute | Hard-coded `::jsonrpsee::proc_macros::rpc` path | The macro no longer emits this attribute. Instead, the macro directly emits the trait + impl shape that jsonrpsee's macro would have expanded to — but generated against `plexus_core::rpc::*` types. Plexus-core's `rpc` module's runtime calls `jsonrpsee::core::server::*` internally to register methods. |

### `plexus-core::value` module

- `Value` — `pub use serde_json::Value;` (the re-export is fine because the type itself is permissively re-exportable; what we don't want is the *macro-emission paths* referencing serde_json).
- `Map` — `pub use serde_json::Map;`.
- `from_value`, `to_value` — re-exports.
- `json!` — a `#[macro_export] macro_rules!` that forwards to `serde_json::json!`. Plexus-core's macro carries the `__path = ::serde_json::json` token internally.

### `plexus-core::rpc` module

- `Methods` — newtype wrapper around `jsonrpsee::core::server::Methods`. Implements `register_method` / `register_subscription` / `into_rpc_module` with the small subset of the jsonrpsee API the macro needs.
- `RpcResult<T>` — type alias for `Result<T, RpcError>` where `RpcError` is plexus-core's own error type (translates to jsonrpsee error on the wire).
- Internal `register_activation_rpc` helper called by the macro instead of relying on `#[jsonrpsee::proc_macros::rpc]`.

### `__private` module

Deleted. The module no longer exists in `plexus-core`. `plexus_rpc::core::__private` also disappears.

### Consumer-facing change

| Setup | Builds today (post UMB-12) | Builds after UMB-13 |
|---|---|---|
| `plexus-core = "0.6"` only | ✓ for non-macro use | ✓ including activation macros |
| `plexus-rpc = "0.3"` only | ✓ | ✓ |
| Consumer using `serde_json::Value` directly in their own activation method return types | ✓ (consumer adds `serde_json` to its own deps) | ✓ (still works — but `plexus_core::value::Value` is the preferred path; serde_json direct dep optional) |
| Consumer using `jsonrpsee` types directly anywhere | ✓ | depends — the consumer added `jsonrpsee` to their deps explicitly, so it still works for their own code. The macro no longer requires it. |

## Risks

- **rpc trait-surface design.** `plexus_core::rpc::Methods` may need more of jsonrpsee's API than this ticket anticipates. *Mitigation:* a UMB-13-output design ticket runs first; if the spike reveals jsonrpsee's surface is wider than predicted, Tier 3 is split into UMB-13a (value module) + UMB-13b (rpc module) and 13b is rescoped.
- **`schema_for!` proc-macro residue.** `schemars::schema_for!` is still invoked by the macro and emits its own `::schemars::*` paths into the consumer crate. Tier 3 does NOT remove this — consumers still need `schemars` as a direct dep transitively through `plexus-core`'s re-export (UMB-12's `plexus_core::schema`). The proc-macro itself is unavoidable without a deep rework of schema generation. This is a deliberate scope limit, not an oversight.
- **Wire-format compatibility.** Changing `plexus_core::rpc::Methods`'s internals must not change the on-the-wire JSON-RPC envelope. *Mitigation:* the existing `plexus-transport` integration tests cover the wire format and remain unchanged.

## What must NOT change

- The on-the-wire JSON-RPC envelope: method dispatch, parameter encoding, streaming framing, error code shape.
- Existing `#[activation]` / `#[method]` / `#[child]` macro arguments and their meanings.
- The `Capabilities` manifest exposed through `plexus_rpc::CAPABILITIES`.
- The substrate startup logging line introduced in UMB-3.

## Acceptance criteria

| # | Check | How to evaluate |
|---|---|---|
| 1 | A new crate with `plexus-core = "0.6"` + `plexus-macros = "0.7"` and **no other plexus or jsonrpsee or serde_json dep** can compile a streaming activation, register it with a `Methods` registry, and call a method via JSON-RPC. | Integration test in `plexus-rpc/tests/` runs end-to-end. |
| 2 | A new crate with `plexus-rpc = "0.3"` only can do the same. | Same integration test. |
| 3 | Grepping macro-expanded output (`cargo expand` on the test activation) shows **zero** `jsonrpsee::` or `serde_json::` paths emitted by `#[plexus_macros::activation]`. | `cargo expand -p umbrella-test-tier3 2>&1 \| grep -E '(jsonrpsee\|serde_json)::' \| wc -l` is `0`. |
| 4 | `plexus_core::__private` does not exist. | `cargo doc -p plexus-core` produces no `__private` page. |
| 5 | `cargo build` and `cargo test` pass green across plexus-core, plexus-macros, plexus-rpc, plexus-transport, plexus-substrate. | CI gate green. |
| 6 | The wire-format integration tests in `plexus-transport/tests/` pass unchanged. | Test command output recorded. |
| 7 | `plexus_rpc::CAPABILITIES` still serializes (Tier 3 may have updated subcrate versions). | Existing test in `plexus-rpc/tests/` passes. |

## Completion

The implementor:
- Lands the value + rpc modules in `plexus-core` and ships `plexus-core 0.6.0`.
- Lands the codegen rewrite in `plexus-macros` and ships `plexus-macros 0.7.0`.
- Ships `plexus-rpc 0.3.0` pinning the new pair.
- Records criteria 1 / 3 evidence (the integration test output + the `cargo expand` zero-count) in the PR.
- Flips this ticket to `Complete` in the same commit.
- Files a follow-up to remove the `__private` warning notes from documentation (the umbrella's promise is now structurally honest).
