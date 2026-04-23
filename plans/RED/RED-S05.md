---
id: RED-S05
title: "Spike: builder surface — can auth middleware be omitted?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-4]
---

## Verdict (Apr 23 2026): 🔴 **HOLE CONFIRMED — CRITICAL SEVERITY**

`plexus-transport/src/websocket.rs:23-52` attaches `CombinedAuthMiddleware` **only when** `.with_api_key(...)` OR `.with_session_validator(...)` was called on the builder. Auth is opt-IN (category C — the worst of the three). Both substrate's stdio mode and FormVeritasV2/uscis have real code paths where auth middleware is NOT attached (uscis when `KEYCLOAK_URL` env var is missing, substrate always in stdio mode).

When middleware is absent, `AuthContext` is not populated in connection Extensions. Activations' `#[from_auth]` injections then fail with `Unauthenticated` at runtime on every call — fail-closed, but:

- No startup assertion that a server deploying `#[from_auth]`-using activations has auth middleware attached
- Admins see "why is my JWT rejected?" instead of "you forgot Keycloak config"

Combined with future `#[plexus::method(public)]` support (RED-2), a misconfigured deploy could serve public endpoints from an activation that was supposed to require auth.

Mitigation tracked in **RED-4** (CRITICAL): `TransportServer::build()` refuses to start when registered activations require auth but the builder was not configured with an auth middleware.

## Question

The plexus-transport server is constructed via a builder (or similar config API). Is it possible for a developer to spin up a backend WITHOUT `CombinedAuthMiddleware` in the middleware chain? If so, all `#[from_auth]` resolvers receive a default/empty AuthContext — effectively bypassing auth globally.

## Setup

1. Read `plexus-transport/src/server.rs` (or equivalent). Enumerate the builder methods that attach middleware.
2. Identify whether `CombinedAuthMiddleware` is:
   - (a) attached by default with no opt-out
   - (b) attached by default but skippable via `.without_auth()` or similar
   - (c) only attached when the dev calls `.with_session_validator(...)` or `.with_api_key_auth(...)` — i.e., auth is opt-IN
3. Check the existing FormVeritasV2 (uscis) builder invocation to confirm which shape it uses.
4. Test: construct a minimal server WITHOUT any auth middleware call, attach an activation that has `#[from_auth]` methods, invoke without JWT.

## Pass condition

Spike **passes** (= hole confirmed) if a server can be constructed such that auth middleware is absent AND `#[from_auth]` methods succeed.

Spike **fails** (= safe) if auth middleware is always attached OR absence fails-closed (e.g., panic at startup, or resolver receives a sentinel that makes it reject).

## Fail → next

Confirmed hole → RED-6 mitigation: builder attaches auth middleware by default; opt-out is explicit and visible. Or: activation with any `#[from_auth]` method bakes a runtime assertion into `plugin_schema()` that fails-fast at server startup if middleware isn't configured.

## Out of scope

- Alternative auth mechanisms (session validator, API key) — each should be verified separately if time
- Test-harness servers that omit middleware intentionally (fine if they're clearly marked)
