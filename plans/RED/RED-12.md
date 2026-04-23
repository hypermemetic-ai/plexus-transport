---
id: RED-12
title: "MCP / stdio transports: document trust model + gate auth-gated deploys"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S11 and RED-S12 confirmed that MCP (`plexus-transport/src/mcp/bridge.rs:318`) and stdio transports invoke `activation.call(..., None, None)` — `AuthContext` is always absent. No Plexus-side mechanism to populate it; no `.with_session_validator()` equivalent.

Runtime: every `#[from_auth]` method returns `-32001` over MCP/stdio. Not a credential bypass, but:
- Posture asymmetry — same activation exposed via both WS and MCP authenticates on one, 401-on-all-methods on the other.
- Silent deployment footgun — operator sees MCP returning 401 everywhere, concludes "MCP auth is broken," ships without gating, ALL remaining (non-auth) methods become publicly callable over MCP.
- Undocumented trust model — there's no explicit "MCP is unauthenticated; deploy behind mTLS/proxy" statement in the codebase or crate docs.

## Goal

Make the trust model loud and build-time-checked:
1. **Document** that MCP and stdio transports do not run session validation; operators must either front them with a trusted bouncer or avoid exposing auth-gated activations.
2. **Refuse at build time** to register an activation with auth-gated methods onto an MCP/stdio server unless the operator explicitly opts in with `.allow_unauthenticated_mcp()` / `.allow_unauthenticated_stdio()`.
3. **Future-ready surface** for adding real MCP auth — e.g., an optional `MCPAuthValidator` hook that, if present, populates `AuthContext` from an MCP-protocol credential (currently none exists, but leave the seam).

## Acceptance

- [ ] MCP server builder: exposes `.allow_unauthenticated_mcp()` method.
- [ ] Starting an MCP server with an activation that has any auth-gated method, without the opt-in, returns a structured `ConfigError` naming the activation + method and pointing to the docs.
- [ ] stdio server builder: same shape, `.allow_unauthenticated_stdio()`.
- [ ] `plexus-transport` crate docs get a `## Trust model` section spelling out which transports validate which auth sources. One paragraph per transport.
- [ ] Tests: attempting to build an MCP server with an auth-gated activation and no opt-in fails with the expected error variant.
- [ ] Tests: opt-in path builds successfully and dispatches (methods still fail-closed per-method at runtime; that's correct).
- [ ] Recon pass to confirm there is no existing MCP auth hook we're re-inventing; if there is, wire it instead of adding an opt-out.

## Out of scope

- Building a real MCP `SessionValidator` mechanism (separate epic if ever needed).
- Changing runtime dispatch semantics on MCP/stdio — fail-closed is correct.

## Notes

This mitigation specifically addresses MCP. stdio is the same shape; the builder + doc pattern should mirror exactly so devs don't have to re-learn the trust story per transport.
