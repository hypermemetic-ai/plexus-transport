---
id: DISP-3
title: "Port REST gateway + delete legacy 2-arg Activation::call"
status: Pending
type: task
blocked_by: [DISP-1, DISP-2]
unlocks: [DISP-6, DISP-7]
---

## Context

RED-S10 found the critical hole: `plexus-transport/src/http/bridge.rs:178` invokes `activation.call(&method, params)` — the legacy two-argument form that predates request-forwarding. REST drops `auth` and `raw_ctx` unconditionally. The macro wrapper defensively fail-closes on `auth = None`, but that's a happy accident, not a REST-layer guarantee.

This ticket fixes the hole AND removes the footgun: after this lands, `Activation::call` only has the 4-arg form. Any attempt to dispatch without threading auth/raw_ctx becomes a compile error.

## Goal

REST handler becomes a protocol adapter: parse POST body and URL → build `RawRequestContext` → call `Dispatcher::dispatch` → frame response as JSON or SSE. The 2-arg `Activation::call(method, params)` convenience overload is deleted from `plexus-core`.

## Acceptance

- [ ] `http/bridge.rs` uses `Dispatcher::dispatch` exclusively. No direct `activation.call(...)` calls in the REST path.
- [ ] `RawRequestContext` populated from the Axum request parts: headers, URI, peer `SocketAddr`.
- [ ] Bearer extraction in `http/server.rs` middleware replaced by — or delegated to — `AuthChain`.
- [ ] 2-arg `Activation::call(method, params)` form **removed** from the `Activation` trait in `plexus-core`. If any impl or call site still uses it, the build must fail.
- [ ] All existing REST tests pass or are updated to the new interface.
- [ ] New integration tests:
  - `#[from_auth(resolver)]` method dispatches correctly over REST with a valid session cookie
  - `#[from_auth]` fails-closed with no cookie
  - `#[from_cookie("name")]` extracts from the REST request
  - `ValidOrigin` on the request type rejects disallowed origins
- [ ] **Closes RED-11.** RED-11 ticket status flipped to Complete with a reference to this ticket.

## Out of scope

- SSE streaming lifecycle semantics (separate ticket if needed — mid-stream JWT expiry, etc.).
- MCP/stdio ports.

## Notes

Deleting the 2-arg form is the load-bearing part. Without it, a future contributor can re-introduce the same bug with a single autocomplete tap. The 4-arg form with required `auth: Option<&AuthContext>` and `raw_ctx: Option<&RawRequestContext>` makes "drop the context" require deliberate `None, None` — still possible, but visible in review.

If library-mode callers in other crates rely on the 2-arg form, they need to pass `None, None` explicitly. That's acceptable friction: DISP-6 introduces `InProcessGateway` for ergonomic library use, and that's the right shape for those callers anyway.

Streaming REST responses (SSE) continue to frame `PlexusStreamItem` as SSE events — framing logic stays in `http/handler.rs`, only the dispatch surface changes.
