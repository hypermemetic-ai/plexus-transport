---
id: RED-S11
title: "Spike: MCP HTTP path — `hub.route(..., auth=None)` intentional or hole?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-12]
---

## Verdict (2026-04-22)

**MODERATE HOLE CONFIRMED.**

`plexus-transport/src/mcp/bridge.rs:318` explicitly passes `None` for `auth` into `activation.call(method, arguments_value, None, None)`. No MCP-layer middleware populates `AuthContext` — there is no `.with_session_validator()` equivalent on the MCP server builder.

Runtime behavior on `#[from_auth]` methods:
- MCP always fails-closed on auth-gated methods (macro wrapper returns `-32001` on `auth = None`).
- Same activation exposed through both WS and MCP gets asymmetric posture: WS authenticates; MCP returns auth errors for every call to gated methods.

This isn't a credential bypass (you can't call an auth-gated method successfully from MCP), but it IS a deployment footgun and posture asymmetry:
- Dev deploys MCP against an activation that uses `#[from_auth]`, sees everything 401, assumes MCP is "broken," ships without auth — now all non-gated methods are publicly callable.
- No `RED-4`-style build-time check refuses an MCP deploy that exposes auth-gated methods without an auth mechanism. The trust model is undocumented.

**Mitigation:** RED-12 — document MCP trust model, add build-time check refusing auth-gated activations on MCP without explicit `.allow_unauthenticated_mcp()` opt-out, OR add MCP session validator hook.

## Question

During RED-S08, the agent noted that `plexus-core/src/mcp_bridge.rs:263` invokes `self.hub.route(method_name, arguments, None)` — passing `None` for auth. The note classified this as "availability concern, not forgery" because MCP clients auth out-of-band. But: is this actually verified? Does it mean the MCP transport bypasses `#[from_auth]` on every call?

A backend exposing the same activation through WS AND MCP would have two different auth postures on the same methods: WS enforces, MCP doesn't. That's the kind of asymmetry that ships to prod and surprises an auditor.

## Setup

1. Re-read `plexus-core/src/mcp_bridge.rs`. Find every call to `hub.route(...)` or equivalent dispatch. Note the `auth` argument.
2. Read `plexus-transport/src/mcp/*`. Does any middleware populate `AuthContext` before the bridge invokes `hub.route`?
3. Simulate: construct a backend with an activation that has `#[from_auth(self.validate)]`; expose via MCP; issue a method call without any auth header. Does it succeed?
4. Check what's documented: is MCP intentionally unauthenticated (supposed to run over mTLS / behind a bouncer)? Is this documented clearly? Is there a `.with_session_validator()` equivalent for MCP?

## Pass condition

Spike **passes** (= hole confirmed) if MCP calls dispatch to `#[from_auth]`-gated methods and succeed with `AuthContext = None`.

Spike **fails** (= safe) if one of:
- MCP enforces auth equivalently to WS
- MCP explicitly fails-closed when an activation has `#[from_auth]` methods
- Documentation + builder design make clear that MCP is unauthenticated and the dev must deploy behind a mTLS/proxy bouncer

## Fail → next

Confirmed hole → mitigation options:
- Extend RED-4's build-time check to refuse MCP-bound auth-gated activations without explicit `.allow_unauthenticated_mcp()` opt-out
- Add MCP session-validator equivalent to plexus-transport
- Or document mandatory deployment topology (mTLS, proxy) and ship a `plexus-audit` check for it

## Out of scope

- MCP's SSE streaming specifics
- MCP protocol-level features (tool discovery etc.) beyond the auth surface
