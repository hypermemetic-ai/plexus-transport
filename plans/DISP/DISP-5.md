---
id: DISP-5
title: "Port stdio gateway + allow_unauthenticated_stdio opt-out"
status: Pending
type: task
blocked_by: [DISP-1]
unlocks: [DISP-7]
---

## Context

RED-S12 verdict: stdio is safe today because substrate doesn't expose any `#[from_auth]` methods through it, but the trust model is undocumented and RED-4's build-time check would catch a future mis-deploy only because it walks the activation schema. Same shape as MCP (RED-S11): stdio invokes `activation.call(..., None, None)` and has no auth mechanism because the transport has no credential surface.

## Goal

stdio gateway becomes a protocol adapter over `Dispatcher`, feeding `RawRequestContext::empty()`. Same opt-out pattern as MCP for declaring "this transport trusts its caller."

## Acceptance

- [ ] `stdio.rs` routes calls through `Dispatcher::dispatch`, passing `RawRequestContext::empty()` (all fields `None`/empty).
- [ ] `TransportServerBuilder::allow_unauthenticated_stdio()` method exists. Starting a stdio gateway with an auth-gated method anywhere in the activation tree without the opt-in returns `ConfigError`.
- [ ] With the opt-in, stdio starts and auth-gated methods fail-closed at dispatch.
- [ ] Crate docs: stdio paragraph in the `## Trust model` section (next to MCP's).
- [ ] Tests:
  - Build stdio server with auth-gated activation + no opt-in → `ConfigError`
  - Build stdio server with opt-in → starts; auth-gated call returns `-32001`
  - Public method dispatches normally
- [ ] **Closes RED-12 stdio portion.** RED-12 status flipped to Complete once both DISP-4 and DISP-5 land.

## Out of scope

- Any stdio auth mechanism (there is no credential surface; don't invent one).
- stdio performance / framing changes.

## Notes

Trivial port relative to MCP because stdio has no HTTP parts to extract. The ticket exists for parity: without it, stdio keeps its ad-hoc dispatch path and the deletion sweep in DISP-7 can't remove all the duplicated helpers.

`RawRequestContext::empty()` is a helper worth adding during DISP-1 or this ticket — trivial constructor, makes the intent readable at call sites. Extractors that read from empty fields naturally fail-closed: `#[from_cookie("x")]` on an empty `HeaderMap` returns the "cookie not present" error, which propagates to the client as `-32001`.
