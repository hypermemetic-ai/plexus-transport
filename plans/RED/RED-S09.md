---
id: RED-S09
title: "Spike: hub child activation routing — does parent posture propagate?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-10]
---

## Verdict (2026-04-22)

**PARTIAL HOLE** — dispatch is safe, RED-4 coverage is not.

- **Dispatch: SAFE.** `DynamicHub::route_with_ctx` threads `raw_ctx` into `ChildRouter::router_call(..., raw_ctx: Option<&RawRequestContext>)`. The macro-generated child wrapper re-runs `#[from_auth]` independently against the threaded `AuthContext` — the child does NOT trust the parent's posture. A call through a `none`-posture parent to a `required`-posture child still fails-closed when auth is missing.
- **RED-4 coverage: HOLE.** `plexus-transport/src/server.rs` `collect_from_schema` (around L376–402) inspects only the root activation schema. It does not recurse into `schema.children`. A deploy where the parent is `none`/`optional` but a child declares `#[from_auth]` can start with no `with_session_validator()` and no build-time error. Per-method fail-closed catches runtime calls, but the build-time safety net is incomplete.

**Mitigation:** RED-10 — recurse `collect_from_schema` into `schema.children`.

## Question

Hub-style activations route calls to children via `hub.route(method_path, params)`. If the PARENT activation has `auth = "optional"` or `auth = "none"` but a CHILD activation has `#[from_auth]` methods, does dispatch through the parent propagate `RawRequestContext.auth` correctly? Can someone call a child method through parent routing without satisfying the child's resolver?

## Setup

Audit `plexus-core/src/plexus/plexus.rs` — specifically `DynamicHub::route_with_ctx` (added recently for exactly this purpose, per earlier recon). Trace:

1. When a request lands on parent's dispatch, is `raw_ctx` threaded to `ChildRouter::router_call(..., raw_ctx: Option<&RawRequestContext>)`?
2. Does the child's macro-generated wrapper re-check `#[from_auth]` independently, or trust the parent's posture?
3. If the parent was deployed to a server with no auth middleware (RED-4 would catch this IF parent declares `#[from_auth]`, but NOT if only children do), does the child still fail-closed?

## Pass condition

Spike **passes** (= hole confirmed) if a request routed through a non-auth parent reaches a child's `#[from_auth]` resolver without a valid `AuthContext` AND the resolver succeeds.

Spike **fails** (= safe) if the child's resolver always receives the same `AuthContext` as an independently-routed call, regardless of the parent's posture.

## Fail → next

If confirmed: mitigation — RED-4's auth-middleware check walks child schemas too (today it only checks root). Or: child activations assert their own middleware expectations at registration time.

## Out of scope

- Nested hubs more than 2 deep (same logic, scalability concern only)
- Performance of per-call AuthContext cloning
