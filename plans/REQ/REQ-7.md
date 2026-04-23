---
id: REQ-7
title: "hub-codegen TS surfaces request schema as JSDoc + opt-in typed overrides"
status: Partial
type: implementation
blocked_by: [REQ-5, REQ-6, SAFE-7]
unlocks: []
---

**Partial implementation Apr 23 2026 (autonomous run):** Activation-level
JSDoc breadcrumbs landed and verified against uscis (FormVeritasV2).
`render_request_jsdoc()` in `hub-codegen/src/generator/typescript/namespaces.rs`
walks `ir.ir_plugin_requests[namespace]` and emits one of `@requiresAuth`
/ `@reads-cookie` / `@reads-header` / `@reads-query` / `@server-derived`
per property. uscis output: 234 `@server-derived` tags across 78
methods, 7 activations. tsc clean.

**JSDoc scope moved to REQ-9 (Apr 23):** The activation-level JSDoc
emission was a stepping stone; REQ-9 retargets it to per-method source
using REQ-6's `x-plexus-source` annotations. REQ-9 superseded this
ticket's JSDoc acceptance criteria.

**Typed errors landed Apr 23 (autonomous run):** Generated
`transport.ts` now declares and exports five typed error classes:

- `PlexusRpcError` (base)
- `AuthenticationError` (-32001)
- `InvalidParamsError` (-32602)
- `MethodNotFoundError` (-32601)
- `ExecutionError` (-32000)

`handleResponse` dispatches the appropriate subclass via `rpcErrorFor`
instead of wrapping in a bare `Error`. Client code can match with
`instanceof` rather than parsing error strings. Committed to
hub-codegen. Covered by 3 new tests in `req7_typed_errors_test.rs`;
92/92 hub-codegen tests pass.

**Still deferred:**

- `--expose-request-fields` flag: opt-in optional override arg on
  generated method signatures. Lower priority — most consumers rely
  on the browser's cookie store / transport-level config. Requires
  adding a second arg to each typed method plus transport-level
  per-call header injection.

## Problem

After REQ-5 (synapse reads `psRequest`) and REQ-6 (per-method `x-plexus-source`), the IR carries enough information for any tool to know exactly which auth, cookies, headers, and query params each method expects. Generated TS clients today see none of this — they expose only RPC params with no awareness of the server-side extraction contract.

A developer using a generated client today has no way to know, from the client surface alone, that calling `clients.list` requires the `access_token` cookie to be set, or that a 401 means "missing cookie" vs "invalid JWT" vs "expired session."

## Architectural correction

The previous REQ-7 draft attributed this codegen work to "synapse-cc." That was wrong: synapse-cc is the orchestrator (Haskell). The TypeScript codegen is **hub-codegen** (Rust). This ticket is a hub-codegen change. synapse-cc only orchestrates by passing one new flag through.

## Required behavior

For every generated TS client method, hub-codegen emits:

### 1. JSDoc breadcrumbs (default, always on)

For each method whose schema declares request-derived params, the JSDoc block includes one tag per source:

| Source annotation | JSDoc tag emitted |
|---|---|
| `from: "auth", resolver: <expr>` | `@requiresAuth — resolver: <expr>` |
| `from: "cookie", key: <name>` | `@reads-cookie <name>` |
| `from: "header", key: <name>` | `@reads-header <name>` |
| `from: "query", key: <name>` | `@reads-query <name>` |
| `from: "derived"` | `@server-derived <param-name>` |

Methods without request-derived params get no extra tags — clean JSDoc for the no-auth case.

### 2. Caller-facing signature omits derived params

The TS function signature exposes only RPC-sourced params (those whose `x-plexus-source.from == "rpc"` or whose annotation is absent). Auth, cookie, header, query, and derived params are stripped from the typed argument list — they come from the actual HTTP request, not function arguments.

This matches REQ-6 acceptance criterion 6 (the schema's `required` array never contains a derived param) and avoids forcing callers to pass values that the transport already provides.

### 3. Opt-in `--expose-request-fields` flag

Hub-codegen accepts a new flag `--expose-request-fields`. When set, generated methods emit a second optional argument typing the request fields with their source-aware shape. The override is fully optional — when omitted, the actual HTTP request supplies the values; when present, it's threaded through to the transport layer's request construction. Default is OFF — clean signature wins for the common case.

synapse-cc passes the flag through via a config option in `synapse.config.json` (one-line change in synapse-cc, listed under "Coordination" below).

### 4. Error mapping uses request-schema knowledge

When the transport layer surfaces a `-32001` (auth) error in a generated method's response, the client throws a typed error containing:

- The method name
- The names of the request-derived params the server expected (drawn from the schema at codegen time)
- A hint string per source (e.g. "expected cookie: access_token")

This is a transport-layer concern but is parameterized per-method by the codegen using schema knowledge it already has. Coordinates with SAFE-5 (which adds the same semantic meaning on the synapse CLI side).

## What must NOT change

- The default TS signature of methods without request-derived params is identical to today
- Existing callers that pass only RPC params continue to work without source code changes
- The hub-codegen output file structure (one .ts per namespace, plus `rpc.ts` / `transport.ts` / `types.ts` / `index.ts`) is unchanged
- synapse-cc orchestration flow is unchanged except for one new pass-through flag
- JSDoc tags for methods with no request-derived params remain blank — no `@requiresAuth: false` noise

## Acceptance criteria

1. A generated method whose schema has any param with `x-plexus-source.from == "auth"` produces TS containing the substring `@requiresAuth` in its JSDoc block
2. A generated method with a param annotated `x-plexus-source.from == "cookie"`, `key: "access_token"` produces TS containing the substring `@reads-cookie access_token` in its JSDoc
3. The TS signature of any generated method excludes parameters whose `x-plexus-source` is anything other than `"rpc"` or absent — confirmed by inspecting the generated function's typed argument list
4. With `--expose-request-fields`, the same method emits a second optional argument typed with cookies/headers/query the server expects
5. Without `--expose-request-fields`, the second argument is absent — verified by string match on the generated TS
6. A `-32001` response from a method with declared cookie sources produces a thrown error whose message names the expected cookie key
7. A method with no request-derived params (e.g. `health.check`) produces TS identical to today, modulo the absence of `@reads-*` / `@requiresAuth` tags
8. The hub-codegen test suite includes a fixture method exercising all five `x-plexus-source` source types (auth, cookie, header, query, derived) and asserts each renders correctly

## Risks

1. **Source annotation gaps in IR.** If REQ-6 hasn't shipped or schemas exist that lack `x-plexus-source` on derived params, the codegen must degrade gracefully — treat unannotated params as RPC-sourced and emit a debug-level warning. This is the fail-soft path.
2. **JSDoc tag conventions are bespoke.** The chosen tags (`@requiresAuth`, `@reads-cookie`, etc.) aren't standardized JSDoc, so IDE tooling won't surface them with rich UI. Acceptable trade-off — they're for human readers grepping source; no IDE integration is promised.
3. **`--expose-request-fields` may interact poorly with browser transports.** Browsers can't set headers on `WebSocket`, so the opt-in override for header fields is a no-op in browser mode. Document this limitation in the JSDoc emitted alongside the optional argument.

## Coordination

- `blocked_by: [REQ-5]` — needs `psRequest` in the IR
- `blocked_by: [REQ-6]` — needs per-method `x-plexus-source` for the JSDoc tag emission
- `blocked_by: [SAFE-7]` — the `@reads-cookie` tag is misleading if the transport layer can't actually use cookies on WS upgrade
- synapse-cc passes `--expose-request-fields` through via a `codegen.exposeRequestFields: bool` field in `synapse.config.json`. The synapse-cc-side change is a one-line option pass-through; out of scope for this ticket but documented here so the synapse-cc maintainer is aware.

## Completion

Implementor lands the JSDoc emission + opt-in flag + error mapping in hub-codegen, runs the test suite, and commits with status `Complete`. Updates synapse-cc to wire the config option through in a follow-up commit (or coordinates with SAFE epic implementor to bundle).
