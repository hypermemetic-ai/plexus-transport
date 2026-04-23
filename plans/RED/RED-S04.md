---
id: RED-S04
title: "Spike: activation without `request = ...` — does `#[from_auth]` fail-closed?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-3]
---

## Verdict (Apr 23 2026): 🟡 **PARTIAL HOLE — MEDIUM SEVERITY**

Runtime behavior is SAFE: when an activation lacks `request = ...`, the auth middleware doesn't get wired, `AuthContext` is `None` in Extensions, and the macro's generated `auth.ok_or_else(|| PlexusError::Unauthenticated(...))?` at codegen/activation.rs:910 fail-closes every call. Resolver never runs.

But the combination compiles cleanly with no compile diagnostic. Every call to the method 401s in production before anyone notices the misconfiguration. That's an ergonomics hole, not a security hole — yet it belongs at compile time.

Mitigation tracked in **RED-3**: compile error when `#[from_auth]` is used in an activation without `request = ...`.

## Question

If an activation is declared WITHOUT `request = ...` (i.e., no activation-level PlexusRequest struct), and a method inside uses `#[from_auth(resolver)]`, does the resolver:

(a) receive a populated `AuthContext` from middleware-level cookie parsing — correct behavior
(b) receive `None`/default and silently treat as authenticated — hole
(c) fail-closed at runtime — safe
(d) fail to compile — safest

## Setup

```rust
struct NoReqHub;

// Note: NO `request = ...` on the activation
#[plexus::activation(namespace = "no_req")]
impl NoReqHub {
    #[plexus::method]
    async fn sensitive(&self,
        #[from_auth(self.validate)] _u: FakeUser,
    ) -> impl Stream<Item = String> + Send + 'static { … }
}
```

Exercise:
1. Compile — does it?
2. `plugin_schema()` — what does the method's params look like?
3. Runtime — connect via WS with and without a JWT cookie; invoke `sensitive`.

## Pass condition

Spike **passes** (= hole confirmed) if the method is reachable without JWT AND the resolver runs with an empty/default AuthContext that the resolver's default logic happens to accept.

Spike **fails** (= safe) if (a) compile fails, (b) resolver gets called with a clearly-empty AuthContext and the dev's resolver rejects (fail-closed idiom), OR (c) runtime rejects before resolver runs.

## Fail → next

Confirmed hole → RED-5 mitigation: `#[from_auth]` inside an activation with no `request = ...` → compile error. The activation's request declaration is what wires AuthContext extraction; without it, auth can't work correctly.

## Out of scope

- What "default AuthContext" means in plexus-core — look it up if safety assessment needed
- Transport-level middleware always populating AuthContext regardless of activation declaration — that's RED-S05's territory
