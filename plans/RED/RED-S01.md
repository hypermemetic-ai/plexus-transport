---
id: RED-S01
title: "Spike: silent auth omission in a mixed-auth activation"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-2]
---

## Verdict (Apr 23 2026): 🔴 **HOLE CONFIRMED — HIGH SEVERITY**

Fixture at `plexus-macros/tests/red_s01_silent_omission.rs`: mixed activation with `list` (has `#[from_auth]`) and `leak` (no auth) compiles cleanly, zero warnings, and `leak`'s schema shows no auth annotation. `leak` dispatches unauthenticated. Only the JSON-level comparison reveals the asymmetry — no tool does this automatically.

Mitigation tracked in **RED-2**: macro warns when mixed-auth is detected.

## Question

If an activation has SOME methods that use `#[from_auth(resolver)]` and a developer adds a new method that OMITS `#[from_auth]`, does the omission produce any warning, error, or observable signal — or does the unauthenticated method ship silently?

## Setup

Write a minimal plexus-macros test fixture:

```rust
#[derive(PlexusRequest)]
struct Req {
    #[from_cookie("access_token")] auth_token: String,
}

struct AuthedHub;

#[plexus::activation(namespace = "authed", request = Req, crate_path = "plexus_core")]
impl AuthedHub {
    // Method 1: auth'd via from_auth
    #[plexus::method]
    async fn list(&self,
        #[from_auth(self.validate)] _u: FakeUser,
    ) -> impl Stream<Item = String> + Send + 'static { … }

    // Method 2: same activation, NO from_auth
    #[plexus::method]
    async fn leak(&self) -> impl Stream<Item = String> + Send + 'static { … }
}
```

Compile. Inspect:
1. rustc output — any warning?
2. `AuthedHub::plugin_schema()` — does `leak` appear with any `x-plexus-source` indicating auth?
3. Runtime dispatch — does calling `leak()` without a valid JWT succeed?

## Pass condition

Spike **passes** (= hole confirmed) if the `leak` method is both compilable AND invokable without authentication AND not flagged in any diagnostic.

Spike **fails** (= safe) if any of: compile error, compile warning, schema annotation identifying auth inconsistency, runtime refusal.

## Fail → next

If this spike confirms the hole, mitigation ticket RED-2 drafts a compile-time diagnostic that fires when an activation has ≥1 `#[from_auth]` method and ≥1 method without.

## Out of scope

- Whether the activation-level `request = Req` extraction runs for `leak` — that's RED-S03 territory
- Whether `leak` can be reached via routing — assume yes
