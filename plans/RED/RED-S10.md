---
id: RED-S10
title: "Spike: REST HTTP gateway — auth path parallels WS or diverges?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-11]
---

## Verdict (2026-04-22)

**CRITICAL HOLE CONFIRMED.**

`plexus-transport/src/http/bridge.rs:178` invokes `activation.call(&method, params)` — the two-argument legacy form. The four-argument form is `call(method, params, auth, raw_ctx)`. REST dispatch drops `auth` and `raw_ctx` at the transport boundary. Consequences:

- REST requests never populate `AuthContext` into the activation call path.
- Per-method `#[from_auth]` still fail-closes at runtime (the macro wrapper sees `auth = None` and returns `-32001`), but that's a happy accident of the macro's defensive default, not a REST-layer guarantee.
- `#[from_cookie]`, `#[from_header]`, `#[from_request]` extractors receive `None` for `raw_ctx`, so any method using them silently degrades or fails unpredictably.
- RED-4's build-time check looks at the activation schema, so it DOES refuse to start a REST-only deploy with auth-gated methods and no validator. That's the only thing preventing a public-internet deploy of this hole.

The REST transport is effectively posture-blind. Even a `required` posture activation technically works through REST — but only because the macro's `None` auth path short-circuits to an error, not because REST validates anything.

**Mitigation:** RED-11 — REST handler must run session validation and thread `AuthContext` + `RawRequestContext` into `Activation::call`.

## Question

The REST HTTP gateway (gated by the `http-gateway` feature in plexus-transport) exposes activation methods as `POST /rest/{namespace}/{method}`. Does it:

- (a) share the same `CombinedAuthMiddleware` as the WS path?
- (b) have its own auth middleware?
- (c) have NO auth middleware and rely on per-method `#[from_auth]` fail-closed?
- (d) bypass `#[from_auth]` entirely because its request path doesn't populate `AuthContext`?

(c) and (d) are vulnerabilities. (a) is safest. (b) is fine if it's equivalent to (a).

## Setup

Audit `plexus-transport/src/http/` — specifically `mod.rs`, `server.rs`, `handler.rs`, `bridge.rs`. Trace a request:

1. Client sends `POST /rest/clients/list` with `Cookie: access_token=<jwt>` header
2. What middleware runs? Does it populate `AuthContext` in Extensions?
3. When `Activation::call(method, params, auth, raw_ctx)` is invoked from the REST handler, what's `auth`? Is it the validated AuthContext, or always None?
4. Does `#[from_auth(...)]` fire-closed correctly on a REST request with no cookie? With a bad cookie?

Also verify `RED-4`'s build-time assertion catches REST deploys: does it prevent starting a REST gateway with auth-gated activations but no `with_session_validator`? (It should, since the check inspects the activation's schema regardless of transport.)

## Pass condition

Spike **passes** (= hole confirmed) if REST-routed requests bypass `#[from_auth]` gating OR if REST has its own auth path with a weaker contract than WS.

Spike **fails** (= safe) if REST auth is equivalent to WS auth (same middleware or demonstrably equivalent behavior).

## Fail → next

Confirmed hole → mitigation: REST handler must invoke the same session-validator logic as WS; `AuthContext` must be populated in Extensions before the handler calls `Activation::call`.

## Out of scope

- SSE streaming auth lifecycle (separate concern — JWT expiry mid-stream)
- CORS policies (addressed by `ValidOrigin` extractor; auditied elsewhere)
