---
id: RED-S14
title: "Spike: error response data leakage (server internals in `error.data`)"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-14]
---

## Verdict (2026-04-22)

**HOLE CONFIRMED.**

- `plexus-macros/src/codegen/activation.rs:923` and `:962` wrap method-body errors via `.map_err(|e| PlexusError::ExecutionError(e.to_string()))?`. Whatever `Display` the underlying error has is the client-visible `error.message`. For common dependencies this leaks freely:
  - `sqlx::Error::Database` — includes the raw SQL error text and often the offending query/parameter via Postgres' error payload
  - `reqwest::Error` — includes upstream hostnames and ports
  - `keycloak`/`oauth2` crate errors — include token endpoint URLs and in some variants request IDs
  - Custom anyhow chains — include `Caused by:` context strings built during development that name modules, file paths, and stack-like breadcrumbs
- `plexus-protocol/src/Plexus/Client.hs:243` — `itemError = T.pack $ "Subscription error: " <> show err` — `show` on streaming errors leaks internal type Debug output (field names, wrapped values) to the subscribing client.
- `plexus-core` `plexus_error_to_jsonrpc` does not provide any sanitization/redaction hook — it's a structural mapping only.

Concrete risk demonstrated: a typical `sqlx::query!` failure against an internal-hosted Postgres surfaces the host:port, table name, and the parameter values in `error.message` straight to any authenticated caller.

**Mitigation:** RED-14 — introduce sanitized error boundary. Options: (a) macro wraps non-`PlexusError` errors with a constant string + unique error_id (structured logged server-side only); (b) new `PlexusError::from_internal(err)` that computes a safe Display; (c) opt-in `#[plexus::method(expose_errors)]` for library-mode methods where verbose errors are intentional.

## Question

JSON-RPC error responses carry an optional `data` field that's passed through to the client. Do server-side errors (panics, database errors, internal exceptions) expose:

- Stack traces with file paths revealing server layout
- SQL query text with table names / schema details
- Internal type names / struct layouts
- Configuration values (connection strings, env var names)
- Exception messages from third-party libraries (Keycloak, Postgres) that name internal hosts

Concrete risk: a JSON-RPC `-32000` "Execution error" response where `error.message = "db connection failed"` and `error.data = "host=pg-internal.us-east-1.aws.internal:5432"` — client just learned the internal hostname.

## Setup

Trace plexus-core's error -> JSON-RPC mapping:

1. `plexus_error_to_jsonrpc` in `plexus-core/src/plexus/plexus.rs` — what gets populated into `error.data`?
2. Activation method errors — what shape reaches the client? Full Debug output of the error? Display? Redacted?
3. Streaming errors — `StreamError { itemError: Text, ... }` — does `itemError` include `show err` (Debug)?
4. `plexus-protocol/src/Plexus/Client.hs:243` — `itemError = T.pack $ "Subscription error: " <> show err` — does this include internal details?

Cross-reference with real uscis behavior: provoke a database error, observe the client-visible response.

## Pass condition

Spike **passes** (= hole confirmed) if ANY client-reachable field (`error.message`, `error.data`, streaming `itemError`) contains:
- A fully-qualified Rust type name from a non-public dependency
- A file path including server-side code organization
- Raw dependency error text (PG error, Keycloak error) that names internal hosts/credentials
- A stack trace

Spike **fails** (= safe) if errors are wrapped into sanitized `PlexusError` variants with explicit, safe display strings.

## Fail → next

Confirmed leak → mitigation: wrap dependency errors at the boundary; error.message becomes a short sanitized string; error.data populated only with fields explicitly allowed to leak. Add a Display impl that differs from Debug.

## Out of scope

- Logging (covered by RED-S13)
- Timing-based information leaks
