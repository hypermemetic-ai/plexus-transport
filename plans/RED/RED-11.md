---
id: RED-11
title: "REST gateway must thread AuthContext + RawRequestContext into Activation::call"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S10 confirmed a critical hole: `plexus-transport/src/http/bridge.rs:178` calls `activation.call(&method, params)` — the legacy two-argument form. The four-arg form is `call(method, params, auth, raw_ctx)`. REST dispatch silently drops `auth` and `raw_ctx`.

Effects:
- REST requests never populate `AuthContext` in the activation call path.
- Per-method `#[from_auth]` fail-closes only because the macro wrapper defensively treats `auth = None` as `-32001`. Not a REST-layer guarantee.
- `#[from_cookie]`, `#[from_header]`, `#[from_query]`, `#[from_request]`, `ValidOrigin`, `PeerInfo` extractors all receive `None` — request-forwarding features are effectively inoperative over REST today.
- RED-4 is the only thing keeping a public-internet REST deploy with auth-gated methods from silently shipping; the transport layer itself doesn't validate anything.

## Goal

REST dispatch runs the same session-validation contract as WS. Request-forwarding extractors receive the same `RawRequestContext` shape.

## Acceptance

- [ ] REST handler invokes the configured `SessionValidator` on each request (cookie or header, matching the transport's configured source).
- [ ] Validation result populates an `AuthContext` that's passed into `activation.call(method, params, auth, raw_ctx)`.
- [ ] `RawRequestContext` built from REST carries the same fields the WS path provides: origin header, cookie jar, all headers, peer info, query string.
- [ ] A `#[from_auth]` method works end-to-end over REST with a valid session cookie/header.
- [ ] A `#[from_cookie("name")]` method extracts correctly over REST.
- [ ] A `ValidOrigin` on the request struct rejects disallowed origins over REST identically to WS.
- [ ] Integration test: REST call to `FormsClients::list` with a valid session succeeds; same call without auth returns `-32001`; same call with valid auth but disallowed Origin returns the origin rejection error.
- [ ] `http-gateway` feature-gated tests in CI if the feature isn't default.

## Design notes

- `CombinedAuthMiddleware` already knows how to validate cookies + bearer headers. Share the implementation — don't fork it per transport. A `TransportAuthChain` type with `validate(&request_parts) -> Result<AuthContext, PlexusError>` that both WS and REST call may be the cleanest shape.
- Consider whether REST auth failures should return HTTP 401 (standard REST) vs JSON-RPC `-32001` body (consistent with WS error surface). Recommendation: HTTP 401 with a JSON-RPC-shaped body; this matches REST idiom while preserving error-class parity.
- This closes the gap where REST looked posture-blind — once auth is threaded, RED-6 postures become real guarantees over REST too.

## Out of scope

- SSE streaming auth mid-stream (future ticket).
- REST-specific CORS policy beyond `ValidOrigin` (operator concern).
