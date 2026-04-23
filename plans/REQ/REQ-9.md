---
id: REQ-9
title: "hub-codegen JSDoc emission retargeted to per-method x-plexus-source"
status: Complete
type: implementation
blocked_by: [REQ-6]
unlocks: []
severity: Medium
---

**Implemented Apr 23 2026 (autonomous run):**

- synapse `Synapse.IR.Types.ParamDef` gained `pdSource :: Maybe Value` + JSON
  round-trip (commit `775c676d` in synapse).
- `Synapse.IR.Builder.extractParamsFromObject` extracts `x-plexus-source`
  per property (commit `775c676d`).
- hub-codegen `ir::ParamDef` gained `pd_source: Option<serde_json::Value>`
  (commit `a7334eb` in hub-codegen).
- `render_method_jsdoc(method, namespace, ir)` in `namespaces.rs`
  reads per-param `pd_source` and emits `@requiresAuth` / `@reads-cookie` /
  `@reads-header` / `@reads-query` / `@server-derived` tags.
- Per-method path takes precedence over activation-level fallback, so
  methods with `request = ()` override emit no derived tags even when
  the activation has a psRequest (fixes the health.check false-positive
  from tonight's REQ-7 minimal).
- Activation-level fallback preserved for pre-REQ-6 backends (plexus-macros
  < 0.5) so existing consumers don't regress.
- 7 new acceptance tests in `req9_jsdoc_test.rs`; 89/89 hub-codegen tests pass.

End-to-end uscis verification remains in REQ-11.

## Problem

Tonight's REQ-7 minimal (commit `24c9dd6`) added JSDoc breadcrumbs (`@requiresAuth`, `@reads-cookie`, `@server-derived`) to generated TS clients, sourced from the activation-level `irPluginRequests` in the IR. The emission is correct-in-spirit but too coarse: every method in an activation with psRequest gets the same breadcrumbs, including methods that opt out via `#[plexus::method(request = ())]`.

Concrete example: `health.check` in uscis has `request = ()` but tonight's codegen emits `@server-derived origin`, `@server-derived transport`, `@server-derived client_ip` on it — misleading documentation.

Once REQ-6 lands, each method's `MethodSchema.params` carries `x-plexus-source` per param. Codegen should read from there.

## Goal

Replace `render_request_jsdoc(namespace, ir)` in `hub-codegen/src/generator/typescript/namespaces.rs` with `render_method_jsdoc(method, ir)`, which walks the method's own param schemas and emits JSDoc per param based on its `x-plexus-source`. Per-method precision; no false positives on overriding methods.

## Required behavior

For each method being rendered, walk `method.md_params` (or equivalent per-param schema data). For each param whose `x-plexus-source.from` is:

| `from` | JSDoc tag |
|---|---|
| `"auth"` | `@requiresAuth — resolver: <x-plexus-source.resolver>` |
| `"cookie"` | `@reads-cookie <x-plexus-source.key>` |
| `"header"` | `@reads-header <x-plexus-source.key>` |
| `"query"` | `@reads-query <x-plexus-source.key>` |
| `"derived"` | `@server-derived <param-name>` |
| `"rpc"` or absent | (no tag — normal RPC param) |

Methods with no non-RPC params emit no extra JSDoc tags (clean).

### Signature filter unchanged

The caller-facing TS signature already omits non-RPC params (tonight's code stripped them implicitly by reading only from the dev-written signature). REQ-6 changes what's visible in the schema but not in the dev-written signature, so the TS signature stays RPC-only automatically.

### Delete the activation-level emission

`render_request_jsdoc(namespace, ir)` and the `ir.ir_plugin_requests.get(namespace)` reads get deleted. The `ir_plugin_requests: HashMap<String, serde_json::Value>` field on the IR struct may stay (backward compat with pre-REQ-6 schemas) but it's no longer consumed by JSDoc emission.

## What must NOT change

- Methods without non-RPC params produce byte-identical TS to tonight's output (modulo the `@server-derived` tags that should now be absent because they were wrong)
- Hub-codegen file structure (one .ts per namespace, plus rpc.ts/transport.ts/types.ts/index.ts) is unchanged
- synapse-cc orchestration flow is unchanged
- The existing `hub-codegen/src/generator/typescript/transport.rs` (SAFE-7 cookie auth) is unchanged

## Acceptance criteria

1. A generated method whose schema has any param with `x-plexus-source.from == "auth"` produces TS containing `@requiresAuth — resolver: <expr>` in its JSDoc.
2. A generated method with a param annotated `x-plexus-source.from == "cookie"`, `key: "access_token"` produces TS containing `@reads-cookie access_token`.
3. A method in an activation with `request = FormVeritasRequest`, but which uses `#[plexus::method(request = ())]` override, produces TS with NO `@server-derived` / `@reads-*` / `@requiresAuth` tags (since the method's schema has no non-RPC params).
4. A method in an activation with `request = FormVeritasRequest`, NOT overriding, produces TS with `@server-derived` tags for origin / transport / client_ip (same as tonight for the non-override case).
5. The hub-codegen test suite includes a fixture method exercising all five `x-plexus-source` source types and asserts each renders correctly.
6. Against uscis: `health.check` JSDoc has no `@server-derived` tags (current output does — this is the regression fix).

## Coordination

- `blocked_by: [REQ-6]` — needs per-method `x-plexus-source` annotations to exist in the IR
- Supersedes the JSDoc piece of REQ-7. REQ-7's remaining acceptance criteria (`--expose-request-fields` flag, `-32001` typed-error mapping) stay in REQ-7.
- Tonight's minimal emission in `namespaces.rs` gets deleted or retargeted as part of this ticket's diff.

## Completion

Implementor:
- Deletes `render_request_jsdoc` and replaces with `render_method_jsdoc` reading from per-method schema
- Updates the two call sites in `namespaces.rs` (dynamic-child block + flat methods block)
- Removes or deprecates `ir.ir_plugin_requests` (keep the field for backward compat; stop reading from it)
- Runs cargo test + regenerates uscis client; verifies `health.check` no longer has misleading tags
- Flips status to Complete
