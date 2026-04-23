---
id: DISP-2
title: "Port WebSocket gateway to Dispatcher (pilot)"
status: Pending
type: task
blocked_by: [DISP-1]
unlocks: [DISP-7]
---

## Context

WebSocket is the most complete existing gateway — it has session-validator auth, bearer fallback, and posture checks via the macro-generated wrapper. It's the right pilot for the Dispatcher refactor because the delta is smallest, and we discover interface problems cheaply before porting the transports that have bigger holes (REST, MCP).

## Goal

`plexus-transport/src/websocket.rs` routes every call through `Dispatcher::dispatch` instead of `activation.call(...)` directly. The jsonrpsee RpcModule wiring, subscription handling, and WS framing all stay — but the dispatch body shrinks to "parse envelope → build RawRequestContext from upgrade request → call Dispatcher → frame the response."

## Acceptance

- [ ] `websocket.rs` holds an `Arc<Dispatcher>`, not a raw `Arc<dyn Activation>`.
- [ ] `CombinedAuthMiddleware` is refactored to be a delegating adapter over `AuthChain` — OR is kept intact and fed by `AuthChain`; implementer's choice. It must not duplicate credential-extraction logic.
- [ ] The WS request handler builds a `RawRequestContext` from the HTTP upgrade parts (headers, URI, peer) and hands it to `Dispatcher::dispatch`. Existing `request.extensions_mut().insert(auth)` pattern no longer needed — auth is a parameter into dispatch.
- [ ] All existing WS integration tests pass with zero modifications. This is the pilot: preserve behavior exactly.
- [ ] New tests through the Dispatcher path:
  - Posture check (RED-6): `auth = "required"` method with no auth returns `-32001`
  - Origin policy (RED-15): disallowed origin rejected before auth runs
  - Auth validation: bearer token path works; cookie path works
- [ ] `Activation::call` is still called from within Dispatcher — this ticket does NOT delete the 2-arg legacy form (that's DISP-3).

## Out of scope

- REST, MCP, stdio ports (DISP-3, DISP-4, DISP-5).
- Deleting legacy 2-arg `Activation::call` (DISP-3).
- Deleting `CombinedAuthMiddleware` (DISP-7).

## Notes

Pilot means: if the Dispatcher interface from DISP-1 is wrong, we discover it here and fix DISP-1 before porting the other three transports. Treat interface friction as a signal to revisit DISP-1, not to work around it.

Subscription handling is the one place WS-specific code survives — jsonrpsee's `SubscriptionSink` doesn't fit Dispatcher's `PlexusStream` return directly. Expect a small adapter between `Dispatcher::dispatch` returning a stream and the WS framing layer. That adapter belongs in `websocket.rs`, not in `Dispatcher`.
