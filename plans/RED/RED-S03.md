---
id: RED-S03
title: "Spike: `request = ()` bypass of AuthContext population"
status: Complete
type: spike
blocked_by: []
unlocks: []
---

## Verdict (Apr 23 2026): 🟢 **SAFE**

`request = ()` only affects `activation_extraction` and `activation_param_bindings` in the generated dispatch wrapper (codegen/activation.rs:918-938). `resolver_bindings` (from `#[from_auth]`) is generated unconditionally and runs independently. The auth injection at line 910 rejects missing `AuthContext` with `Unauthenticated` before any resolver runs.

A method with BOTH `request = ()` AND `#[from_auth]` compiles and fail-closes correctly at runtime. The two attributes are orthogonal.

No mitigation required.

## Question

When a method uses `#[plexus_macros::method(request = ())]` to opt out of activation-level request extraction, does it ALSO bypass the middleware that populates `AuthContext`? If so, does `#[from_auth(resolver)]` inside such a method see `None`/default and silently succeed?

## Setup

```rust
#[derive(PlexusRequest)]
struct Req {
    #[from_cookie("access_token")] auth_token: String,
}

struct Hub;

#[plexus::activation(namespace = "leak", request = Req)]
impl Hub {
    // Override skips activation-level extraction.
    // But still has #[from_auth] — what happens at runtime?
    #[plexus::method(request = ())]
    async fn sensitive(&self,
        #[from_auth(self.validate)] _u: FakeUser,
    ) -> impl Stream<Item = String> + Send + 'static { … }
}
```

Exercise paths:
1. Macro expansion — does `request = ()` strip the `from_auth` wiring?
2. Runtime — invoke `sensitive` with and without a valid JWT cookie.
3. Inspect `plugin_schema()` output — does it reflect auth requirement?

## Pass condition

Spike **passes** (= hole confirmed) if EITHER:
- `sensitive` can be invoked without a JWT and succeeds, OR
- The schema shows `x-plexus-source.from == "auth"` annotation but runtime doesn't enforce

Spike **fails** (= safe) if `sensitive` rejects unauthenticated calls at runtime AND schema matches runtime behavior.

## Fail → next

If confirmed, mitigation RED-4: `request = ()` + `#[from_auth]` on any method param → compile error. These two are inconsistent; the override shouldn't coexist with an auth-requiring method.

## Out of scope

- Interaction with REQ-10's `required(...)` locking — that's a separate defense; this spike asks about the ungated case
- Whether WS-upgrade middleware populates AuthContext — that's RED-S04's territory
