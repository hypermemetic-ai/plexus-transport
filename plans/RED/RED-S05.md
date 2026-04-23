---
id: RED-S05
title: "Spike: builder surface — can auth middleware be omitted?"
status: Pending
type: spike
blocked_by: []
unlocks: []
---

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
