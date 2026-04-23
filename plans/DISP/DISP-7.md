---
id: DISP-7
title: "Deletion sweep: remove CombinedAuthMiddleware, duplicate collectors, ad-hoc bearer extraction"
status: Pending
type: task
blocked_by: [DISP-2, DISP-3, DISP-4, DISP-5, DISP-6]
unlocks: []
---

## Context

After DISP-2 through DISP-6 land, every gateway funnels through `Dispatcher::dispatch`. The ad-hoc infrastructure that existed to compensate for the missing shared kernel becomes dead weight. This ticket is the net-negative-LOC cleanup that realizes the DISP refactor's second promise: fewer code paths to audit.

## Goal

Every duplicated "validate credentials / walk schema / map errors" helper is removed. The only remaining copies are inside `Dispatcher` and its sub-components.

## Acceptance

- [ ] `CombinedAuthMiddleware` removed from `plexus-transport/src/websocket.rs`. WS auth flows entirely through `AuthChain`.
- [ ] Ad-hoc bearer extraction removed from `plexus-transport/src/mcp/server.rs` and `plexus-transport/src/http/server.rs`. Both delegate to `AuthChain`.
- [ ] `collect_auth_gated_methods` / `collect_from_schema` in `plexus-transport/src/server.rs` removed — replaced by `PostureCheck::walk` from DISP-1.
- [ ] `.with_session_validator()` builder semantics preserved — it now populates the `AuthChain` used by all gateways, not the WS-only middleware.
- [ ] `.allow_missing_auth_middleware()` (RED-4 opt-out) semantics preserved — flipped through to `PostureCheck`.
- [ ] Net LOC delta negative. Track in PR description.
- [ ] All RED-* tests still pass.
- [ ] No semantic change visible to users of the public API — the builder surface is unchanged, the internals are collapsed.

## Out of scope

- Changing the public `TransportServerBuilder` surface (builder methods stay where they are; they just dispatch to the shared Dispatcher internals now).
- Removing `Activation` trait features (that's DISP-3's delete of the 2-arg call).

## Notes

This ticket is a compression pass. The implementer should expect to delete ~800 LOC across transports and replace nothing (the Dispatcher already has the equivalent from DISP-1). If the delta isn't comfortably negative, something went wrong earlier in the epic and this ticket is exposing it — stop and investigate rather than accepting a wash.

Fold tests accordingly: per-transport tests that were really "does auth work?" tests move to `dispatch/` and become single-surface tests. Integration tests per transport stay, but shrink because the shared behavior is already covered once.
