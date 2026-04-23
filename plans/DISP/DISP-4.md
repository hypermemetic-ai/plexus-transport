---
id: DISP-4
title: "Port MCP gateway + allow_unauthenticated_mcp opt-out"
status: Pending
type: task
blocked_by: [DISP-1]
unlocks: [DISP-7]
---

## Context

RED-S11 confirmed that `plexus-transport/src/mcp/bridge.rs:318` explicitly passes `None` for `auth` and `raw_ctx`. No MCP-layer middleware populates `AuthContext` — there's no `.with_session_validator()` equivalent on the MCP server builder. Consequence: any `#[from_auth]` method returns 401 over MCP, same activation exposed via WS + MCP gets asymmetric posture, and the trust model is undocumented.

The DISP refactor gives this a clean resolution: MCP becomes a protocol adapter over `Dispatcher`, which threads auth if available. For the common MCP case (subprocess authenticated out-of-band by the parent process), there's an explicit opt-out that tells the posture check "this transport trusts its caller."

## Goal

MCP gateway calls `Dispatcher::dispatch`. If the operator deploys an auth-gated activation on MCP without explicit opt-out, startup refuses with a named error.

## Acceptance

- [ ] `mcp/bridge.rs` uses `Dispatcher::dispatch` exclusively; the `None, None` call at L318 is gone.
- [ ] `RawRequestContext` populated from the MCP request's HTTP parts where available (Streamable HTTP path). For session-persisted MCP calls, `RawRequestContext` carries what's available at the HTTP layer; fields missing in MCP-native calls are `None`/empty.
- [ ] Bearer extraction in `mcp/server.rs` replaced by `AuthChain`.
- [ ] `TransportServerBuilder::allow_unauthenticated_mcp()` method exists. Starting an MCP gateway with any auth-gated method anywhere in the activation tree (root or children — recursion from DISP-1's `PostureCheck::walk`) without the opt-in returns a structured `ConfigError` naming the offending method path.
- [ ] With the opt-in, MCP starts and auth-gated methods fail-closed at dispatch (this is the pre-existing behavior — the opt-in just acknowledges it).
- [ ] Crate docs: `## Trust model` section in `plexus-transport` spells out MCP's posture. One paragraph naming the opt-in and pointing at the fail-closed default.
- [ ] Tests:
  - Build MCP server with auth-gated activation + no opt-in → `ConfigError`
  - Build MCP server with opt-in → starts; call to auth-gated method returns `-32001`
  - Public method over MCP dispatches normally
- [ ] **Closes RED-12 MCP portion.** RED-12 status updated with reference to this ticket (may stay Pending until DISP-5 also lands).

## Out of scope

- Implementing a real MCP `SessionValidator` (no MCP protocol credential mechanism exists in-tree today; leave a seam for it, don't build it).
- stdio posture (DISP-5).
- Deleting `hub.route(..., auth=None)` call sites in `plexus-core/src/mcp_bridge.rs` beyond what's needed — coordinate with the core maintainer if the signature changes there.

## Notes

The asymmetric posture between WS and MCP is intentional going forward: WS authenticates per-request, MCP trusts the subprocess. The opt-in is the way we make that trust declaration explicit rather than silent. An operator who ships MCP to a context where the "trusted subprocess" assumption doesn't hold will get the ConfigError on startup and a docs pointer explaining what to do.
