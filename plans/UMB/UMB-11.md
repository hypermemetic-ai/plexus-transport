---
id: UMB-11
title: "Tier 1 — introduce `__private`, shrink macro-emission deps from 8 to 4"
status: Pending
type: implementation
blocked_by: [UMB-1, UMB-3]
unlocks: [UMB-12]
confidence: high
severity: Medium
introduces:
  - "plexus_core::__private (module)"
  - "plexus_core::PluginId"
imports:
  - "plexus_core::plexus (owner: plexus-core)"
  - "plexus_macros::activation (owner: plexus-macros)"
---

## Problem

Today `#[plexus_macros::activation]` expands into code that references **eight external crates by absolute path** (`::async_trait::*`, `::jsonrpsee::*`, `::uuid::*`, `::serde_json::*`, `::futures::*`, `::schemars::*`, `::tokio::*`, `::serde::*`). Cargo's `--extern` rule means each one must appear in the *consumer's* `[dependencies]` for the path to resolve — even though every name is already a transitive dep through plexus-core / plexus-macros / plexus-transport.

The empirical refutation: a fresh crate depending only on `plexus-rpc = "0.1"` fails to compile with `E0463 can't find crate` for each transitive when `extern crate <name>;` is attempted. Cargo only passes `--extern` for direct deps; `extern crate` against transitives is rejected.

Net effect: the umbrella's "one dep, one version pin" promise is half-delivered. A real activation-writing consumer of `plexus-rpc` today still needs ~9 lines in `[dependencies]`, not 1.

## Context

The pattern that closes the gap is the **`__private` module re-export**, used by serde / tokio / pin-project / rocket. The proc-macro emits paths through a `__private` module hosted on a regular library crate the consumer already depends on; that module re-exports the runtime crates the macro needs. Because module lookup is composable across crate boundaries while `--extern` is not, this projects the macro-host crate's direct deps into a namespace the consumer can reach through a single `--extern` flag.

`plexus-macros` is a proc-macro crate and therefore cannot host `__private` itself (proc-macro crates can only export macros, not runtime modules). The runtime home is `plexus-core`, which is already in every macro consumer's transitive deps.

Native `async fn` in traits (AFIT) is stable since Rust 1.75 (December 2023) and is the accepted replacement for `async_trait` in the surrounding ecosystem (tokio, axum, hyper). Substrate's MSRV is comfortably above this. The `async_trait` macro's only continuing value is dynamic dispatch via `Box<dyn Trait>` — which the Plexus dispatch path does not use; it consumes `&self` and emits a concrete `impl Stream` per method.

`uuid::uuid!` is a compile-time-validated `Uuid` constructor. A `plexus_core::PluginId(Uuid)` newtype with a `const fn from_str(...)` wrapping `Uuid::from_u128` lets the macro emit a `plexus-core`-internal path instead of `::uuid::*`. The tokio sites are limited (three) and use only macros (`select!`-class joining for streamed dispatch); they can be replaced by direct `futures` joins in plexus-core helpers.

## Evidence

The decision to land `__private` in `plexus-core` rather than `plexus-rpc` is driven by Rust's `--extern` rule plus the goal of not forcing every existing plugin to take `plexus-rpc` as a direct dep. With `__private` in `plexus-core` and macro emission going through `#crate_path::__private::*` (using the existing `crate_path` macro argument), the default `crate_path = "plexus_core"` keeps every current direct-`plexus-core` consumer working unchanged; umbrella adopters opt into `crate_path = "plexus_rpc::core"` and the paths transparently resolve through `plexus-rpc`'s re-export of plexus-core.

The choice of *which four* crates to drop in Tier 1 is driven by cost / value:

- `async_trait`: 3 emission sites, fully replaceable by native AFIT (stable, settled in ecosystem since 2024).
- `tokio`: 3 emission sites, all of them macro invocations that have direct `futures`-based replacements available in plexus-core.
- `uuid`: 3 emission sites, all related to `PLUGIN_ID`. A `plexus_core::PluginId` newtype is the natural home for plugin-identifier concerns and improves type safety.
- `serde`: 1 emission site (a trait bound), trivial to re-export from plexus-core.

The remaining four (`jsonrpsee`, `serde_json`, `futures`, `schemars`) are deferred to UMB-12 / UMB-13. They have more emission sites and / or require thin facade types in plexus-core that aren't justified by Tier 1 alone.

## Required behavior

### Macro emission

| Trigger | Before | After |
|---|---|---|
| `#[activation]` on an impl block | Emits `#[::async_trait::async_trait]` on the dispatch trait impl | Emits a `Box::pin`-returning shim using native AFIT; no `async_trait` reference |
| `#[activation]` declares a `PLUGIN_ID` const | Emits `pub const PLUGIN_ID: ::uuid::Uuid = ::uuid::uuid!(<str>);` | Emits `pub const PLUGIN_ID: #crate_path::PluginId = #crate_path::PluginId::from_str_const(<str>);` |
| Streaming method dispatch | Emits a `::tokio::select!`-style join across the per-method tasks | Emits a `#crate_path::dispatch::join_methods(...)` call that uses `futures::stream::SelectAll` internally |
| Trait-bound emission | Emits `::serde::Serialize` bound on params | Emits `#crate_path::__private::serde::Serialize` |

### `__private` module surface

`plexus_core::__private` is a `#[doc(hidden)]` module containing re-exports of the runtime crates the macro emits paths through. After Tier 1 it contains exactly four entries: `jsonrpsee`, `serde_json`, `futures`, `schemars`. (Tier 2 / Tier 3 shrink this further.)

The `__private` module is documented as "not a public API; major-version-bumped at will."

### Consumer-facing change

| Consumer setup | Builds today | Builds after Tier 1 |
|---|---|---|
| `plexus-core` + `plexus-macros` + `async-trait` + `uuid` + `tokio` + `serde` in deps | ✓ | ✓ (deprecation warning on the four obsolete entries; build succeeds with or without them) |
| `plexus-core` + `plexus-macros` only | ✗ (fails to resolve `::async_trait::*` etc.) | ✓ |
| `plexus-rpc` only (with `crate_path = "plexus_rpc::core"`) | ✗ | ✓ |

The four obsolete crates can still be in consumer `[dependencies]` without breaking; the macro just no longer requires them.

### `plexus-core` new public surface

- `plexus_core::PluginId` — newtype around `Uuid`. Has `from_str_const(&'static str) -> Self` (panics on invalid at const-eval time), `as_uuid(&self) -> Uuid`, `Display`, `Serialize`, `Deserialize`, `Eq`, `Hash`. Cited by macro emission.
- `plexus_core::dispatch::join_methods` — internal helper called by the macro; not part of the documented public API but `pub` to satisfy macro hygiene.
- `plexus_core::__private` — `#[doc(hidden)]` module re-exporting the residual runtime crates.

## Risks

- **AFIT object-safety surprise.** Native AFIT does not give an object-safe trait by default. The Plexus dispatch trait is currently consumed via `Activation` and `ChildRouter` — verify neither consumer needs `dyn Activation`. *Mitigation:* if any consumer needs `dyn`, keep `async_trait` for the `Activation` trait specifically and limit Tier 1's drop to internal-only sites. The risk is a Tier 1 scope reduction, not a redesign.
- **`const fn` for `Uuid::from_u128` requires MSRV check.** `Uuid::from_u128` is `const` since uuid 1.4 (2023). Workspace pin is 1.6, so this is settled.
- **Substrate's mcp-gateway features may reference `tokio::select!` from outside the macro.** That's user code, untouched by this ticket. Only the 3 macro-emission sites move.

## What must NOT change

- Public dispatch behavior: no method, return type, or schema changes.
- Consumer wire protocol: JSON-RPC envelopes, error codes, streaming framing all unchanged.
- Existing macro arguments: `namespace`, `version`, `description`, `crate_path`, `params(...)`, `streaming` flag retain their meaning. Default `crate_path` stays `plexus_core`.
- Direct-`plexus-core` consumers continue to work without adding `plexus-rpc`.
- Substrate's `mcp-gateway`-feature build path.

## Acceptance criteria

| # | Check | How to evaluate |
|---|---|---|
| 1 | A new crate with `[dependencies] plexus-core = "0.5.4"` and `plexus-macros = "0.6.0"` and **nothing else from the eight** can compile a minimal activation declared with `#[plexus_macros::activation(namespace = "x", crate_path = "plexus_core")]`. | `cargo build` in a fresh crate succeeds. |
| 2 | A new crate with `[dependencies] plexus-rpc = "0.2.0"` and **no other plexus deps** can compile the same minimal activation when declared with `crate_path = "plexus_rpc::core"`. | `cargo build` in a fresh crate succeeds. |
| 3 | The set of names re-exported by `plexus_core::__private` is exactly `{jsonrpsee, serde_json, futures, schemars}`. | `cargo doc --no-deps -p plexus-core` lists exactly those four under `__private`. |
| 4 | `cargo build` and `cargo test` pass green across `plexus-core`, `plexus-macros`, `plexus-rpc`, `plexus-transport`, and `plexus-substrate`. | Each crate's CI gate is green. |
| 5 | Existing consumers that listed `async-trait` / `tokio` / `uuid` / `serde` purely for the macro can remove those lines and rebuild successfully. | Spot-check on `plexus-substrate` and one other plugin. |
| 6 | Substrate's startup logs still emit the `plexus-rpc umbrella v… core=… …` line (UMB integration unchanged). | Run substrate, inspect log. |

## Completion

The implementor:
- Lands the codegen changes in `plexus-macros` and ships `plexus-macros 0.6.0` to crates.io.
- Lands `__private` + `PluginId` + `dispatch::join_methods` in `plexus-core` and ships `plexus-core 0.5.4` to crates.io.
- Ships `plexus-rpc 0.2.0` pinning the new pair (purely a version bump for the umbrella).
- Records test command output for criteria 1 and 2 in the PR description.
- Flips this ticket to `Complete` in the same commit that lands the substrate verification.
