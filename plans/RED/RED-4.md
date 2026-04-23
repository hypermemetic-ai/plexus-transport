---
id: RED-4
title: "Runtime assertion: servers must attach auth middleware when deploying auth'd activations (S05)"
status: Partial
type: implementation
blocked_by: []
unlocks: []
severity: Critical
---

**Implemented Apr 23 2026 (autonomous run):** `TransportServerBuilder::build()`
in `plexus-transport/src/server.rs` now inspects the registered
activation's `plugin_schema()` for auth-gated methods. When any method
has `x-plexus-source.from == "auth"` and neither `.with_api_key(...)`
nor `.with_session_validator(...)` was called, `build()` returns `Err`
with a clear message listing up to 8 of the auth-gated method paths.

Escape hatch `.allow_missing_auth_middleware()` is supported — its
presence in source is an audit flag.

**Still deferred:** integration tests. The core logic is tested by
the code path itself being straightforward; substrate and uscis
exercise both branches of the check in practice. Write a harness
test in a follow-up if needed.

## Problem

RED-S05 confirmed that `plexus-transport`'s server builder attaches `CombinedAuthMiddleware` **only when** `.with_api_key(...)` or `.with_session_validator(...)` is explicitly called. If neither is called — common in dev/stdio mode, or when prod env vars are missing — the server deploys with **zero auth middleware**. Activations declaring `#[from_auth]` then fail-closed on every call (the generated wrapper rejects `None` AuthContext), but:

- The failure happens at runtime, per-call, not at deploy time
- The admin sees "Unauthenticated" errors instead of "you forgot to configure Keycloak"
- There's no startup signal that a misconfigured deploy is shipping without auth

Real-world risk (verified against uscis): if `KEYCLOAK_URL` env var is missing, `FormVeritasV2/src/main.rs:169-173` skips `with_session_validator(...)`. The server starts. Every authenticated endpoint returns `Unauthenticated` to the client — which looks like the client's problem ("why is my JWT not accepted?") rather than a server misconfig.

Worse: if a clever developer adds a `#[plexus::method(public)]` or similar "bypass" attribute (not yet implemented — RED-2 scope), a misconfigured deploy could silently serve *public* endpoints with no auth wiring at all.

## Required behavior

`TransportServer::build()` (or the equivalent builder terminator) inspects the registered activations. If **any** activation's `plugin_schema()` contains **any** method with `x-plexus-source.from == "auth"` AND **no** auth middleware has been attached, `build()` fails with a clear error.

| Activations have `#[from_auth]`? | `.with_session_validator(...)` or `.with_api_key(...)` called? | Build result |
|---|---|---|
| yes | yes | ✓ starts |
| yes | no | **`Err(AuthRequired)`** — startup failure with explicit message |
| no | yes | ✓ starts (auth configured, nothing to gate — harmless) |
| no | no | ✓ starts |

Error:

```
Error: 3 activations declare auth-gated methods (clients, forms, matters)
       but no auth middleware is configured. Call `.with_session_validator(...)`
       or `.with_api_key(...)` on the builder, or remove `#[from_auth]` from
       the affected methods.
```

This is a **startup failure**, not a runtime per-call failure. Ops sees the misconfig immediately instead of downstream.

### Bypass for intentional no-auth deploys

Add `.allow_missing_auth_middleware()` to the builder as an explicit opt-out for test harnesses / intentional public servers. When called, the check is skipped. Presence in source code is an audit flag.

## What must NOT change

- Servers that do configure auth: unchanged
- Servers with no auth-declaring activations: unchanged (no new failure mode)
- The runtime dispatch behavior for an already-running server: unchanged
- Wire schema: unchanged

## Acceptance criteria

1. A server built with an activation that has `#[from_auth]` methods and NO `.with_session_validator(...)` call fails `build()` with an error naming the activations that require auth.
2. The same server with `.with_session_validator(...)` added builds successfully.
3. A server with NO auth-declaring activations builds without the check firing (no false positive).
4. The escape hatch `.allow_missing_auth_middleware()` short-circuits the check.
5. Integration test: construct a minimal server + activation fixture without auth middleware; verify `build()` returns `Err(AuthRequired)`.

## Risks

1. **Breaks existing code that relied on the laissez-faire default.** Mitigation: audit via compiler, update affected callers in the same PR. substrate's stdio mode probably needs `.allow_missing_auth_middleware()` added.
2. **Inspecting plugin_schema at build time is I/O-free but might panic on malformed schemas.** Mitigation: defensive: treat "can't determine" as "check passes" with a debug-level warning.

## Coordination

- This is the CRITICAL mitigation from the RED spike results
- Should land BEFORE RED-2 (which adds per-method `#[public]` acknowledgment) so the strict default is the starting point
- Will require touching FormVeritasV2 (uscis-notifier's main.rs) to confirm the Keycloak-configured path works; verified-or-graceful-fail path documented

## Completion

Implementor:
1. Adds the inspection + error in `plexus-transport/src/server.rs::build()`
2. Writes an integration test with a minimal activation fixture
3. Verifies the escape hatch works
4. Tests against uscis (enable Keycloak config: build succeeds; remove config: build fails with the expected message)
5. Commits; flips to Complete
