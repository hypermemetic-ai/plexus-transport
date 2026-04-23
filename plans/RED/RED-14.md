---
id: RED-14
title: "Sanitize error boundary — stop leaking dep-error Display into JSON-RPC error.message"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S14 confirmed that method-body errors leak verbatim dependency output into the client-visible `error.message` field:

- `plexus-macros/src/codegen/activation.rs:923` and `:962` use `.map_err(|e| PlexusError::ExecutionError(e.to_string()))?`. Whatever `Display` the inner error implements becomes the JSON-RPC `error.message`.
- Concrete leak surfaces observed on typical stacks:
  - `sqlx::Error::Database` — full Postgres error string including table names and sometimes parameter values.
  - `reqwest::Error` — upstream hostnames, ports, sometimes full URLs.
  - `anyhow` context chains — module paths, file breadcrumbs, dev comments built into context strings.
  - Keycloak/oauth2 crates — token endpoint URLs, request IDs.
- `plexus-protocol/src/Plexus/Client.hs:243` — streaming branch: `itemError = T.pack $ "Subscription error: " <> show err`. `show` leaks Haskell-side record Debug output to subscribers.
- `plexus-core` `plexus_error_to_jsonrpc` is a pure structural mapping — no sanitization hook.

Concrete risk: an authenticated caller gets "connection refused to pg-internal.us-east-1.aws.internal:5432" in `error.message` on a db hiccup. Internal topology leaked to any user.

## Goal

By default, `error.message` for non-`PlexusError` execution failures is a short, fixed, safe string. Server logs keep the full error for diagnostics, correlated by an `error_id`. Opt-in escape hatch for library-mode methods where verbose errors are intentional.

## Acceptance

- [ ] New variant or helper: `PlexusError::from_internal(e: impl std::error::Error) -> PlexusError` that
  - returns `ExecutionError { message: "Internal execution error", error_id: <uuid or short random> }` (shape TBD — extend JSON-RPC error.data with `{ "errorId": "..." }`, not the raw message)
  - logs the full `Debug` / `Display` of `e` at `error!` level server-side, including the `error_id`
- [ ] `plexus-macros/src/codegen/activation.rs` — the `.map_err(...)` at :923 and :962 route through `from_internal` by default.
- [ ] Opt-in attribute for a method: `#[plexus::method(expose_internal_errors)]` keeps today's verbose behavior. Document as "library-mode only; do not set on network-exposed methods."
- [ ] `PlexusError` variants that are already safe (AuthError, InvalidParams, NotFound, user-constructed ExecutionError with explicit message) pass through unchanged — sanitization only applies to `from_internal` wrapping.
- [ ] Haskell side: `plexus-protocol/src/Plexus/Client.hs:243` — replace `show err` with a safe rendering. Options: `"Subscription error"` plus a correlation id on server logs, OR a `Display`-via-safe-text helper.
- [ ] Test: a method that does `db_query().await?` where `db_query` returns a `sqlx::Error` — the JSON-RPC response contains `error.message = "Internal execution error"` and `error.data.errorId` present; the server log contains the full sqlx error correlated by id.
- [ ] Test: opt-in method with `expose_internal_errors` still emits verbose errors.
- [ ] Docs: `plexus-core` README gains a short "Error hygiene" section.

## Out of scope

- Logging leak (covered by RED-13).
- Timing-based info leaks.
- Non-error observability (metrics/traces).

## Design notes

- Error correlation via `errorId` is the important escape valve — devs can still debug prod issues, just not directly from client output.
- Consider whether `error.data` should include an error *category* (e.g., `"database"`, `"upstream"`) that's safe to expose. Useful for clients to branch behavior without leaking specifics. Probably yes; define a small enum.
- `PlexusError::ExecutionError(String)` (user-constructed, explicit) must remain unchanged — devs who WANT to return an error message to the client should be able to. Only automatic wrapping is sanitized.
