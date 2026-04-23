---
id: RED-S08
title: "Spike: `AuthContext` injection via request body / unsafe deserialization"
status: Complete
type: spike
blocked_by: []
unlocks: []
---

## Verdict (Apr 23 2026): 🟢 **SAFE**

Audit of 6 `AuthContext` construction sites across plexus-core and plexus-transport:
- All construction happens server-side via the `SessionValidator` trait contract, which takes a `&str` cookie value and returns `Option<AuthContext>`.
- `#[from_auth_context]` field semantics (plexus-macros/src/request.rs:297-309) only read from `ctx.auth` — the server-populated `RawRequestContext.auth` — never from the method's `params` body.
- `AuthContext` derives `Deserialize` but there's no code path that deserializes it from RPC params JSON.
- Query string extraction and header extraction are explicit per-field via `#[from_query]` / `#[from_header]`; neither auto-populates AuthContext.
- MCP bridge path (`plexus-core/src/mcp_bridge.rs:263`) passes `None` for auth — availability concern, not a forgery vulnerability.

No mitigation required. The architecture correctly enforces server-side-only population of AuthContext.

## Question

The `AuthContext` is populated server-side from cookie-parsed JWTs. Is there any path where a field of `AuthContext` (or the whole struct) is populated from *client-controllable* input — JSON-RPC params body, header values, query params?

If yes, a client could set `AuthContext.user_id = "admin"` directly without presenting a JWT.

## Setup

1. Grep the plexus-core codebase for construction sites of `AuthContext`. Enumerate each site.
2. For each site, trace inputs: do any come from a source the client controls without server validation?
3. Check `#[from_auth_context]` field semantics in PlexusRequest derive — does it copy from `RawRequestContext.auth` only, or does it accept override from params?
4. Check MCP transport's auth path — any difference from WS?

Grep patterns:
- `AuthContext { user_id:` — direct construction
- `AuthContext::new` or `AuthContext::default` — constructor methods
- `auth_context` in deserialize paths — fields on public types

## Pass condition

Spike **passes** (= vulnerability confirmed) if any code path constructs `AuthContext` from a field that comes from the RPC method's `params` JSON body, or from a client-set header other than `Cookie: access_token`.

Spike **fails** (= safe) if `AuthContext` is only ever built from server-validated sources (verified JWT, session store lookup).

## Fail → next

If confirmed, this is a critical hole. Mitigation RED-9: harden the construction sites to only accept trusted inputs; add `#[serde(skip)]` or equivalent to all AuthContext fields to refuse deserialization from untrusted sources.

## Out of scope

- JWT signature bypass (handled by the library)
- Cookie-fixation / session-replay — separate security concern
- Token-leakage via logs — audit concern, not the macro's problem
