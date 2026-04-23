---
id: RED-S07
title: "Spike: fake-resolver detection — can schema flag a validator that never rejects?"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-5]
---

## Verdict (Apr 23 2026): 🔴 **HOLE CONFIRMED — LOW SEVERITY**

No existing tool detects resolvers that never reject. `plexus-macros` captures the resolver expression as a string in `x-plexus-source.resolver` (per REQ-6) but nothing downstream examines resolver body semantics. Neither Clippy nor any custom lint exists. The macro invokes the resolver unconditionally; any function with signature `async fn(&AuthContext) -> Result<T, E>` passes.

Trait-bound defense is impractical (Rust can't enforce "this function inspects its argument"). Naming-pattern lint is cheap and catches obvious stubs (`accept_all`, `fake_validator`, etc.) — viable starting point.

Low severity because this is a friendly-attacker scenario (tired dev forgets to swap a stub). Adversarial bypass would require merging malicious resolver code, which has other defenses.

Mitigation tracked in **RED-5**: out-of-tree `plexus-audit` tool that walks IR schemas and flags resolver names matching known-stub patterns.

## Question

`#[from_auth(self.validate_user)]` trusts the developer's resolver to actually validate the JWT. A resolver that simply returns `Ok(FakeUser)` for any input would pass macro + runtime checks but provide no security. Can the schema surface or a lint tool catch an obviously-permissive resolver?

## Setup

```rust
impl Hub {
    // Canonical — rejects invalid JWTs
    async fn validate_user(&self, ctx: &AuthContext) -> Result<ValidUser, PlexusError> {
        self.db.lookup(&ctx.user_id).await
            .ok_or_else(|| PlexusError::Unauthenticated(...))
    }

    // Suspicious — returns Ok without checking
    async fn accept_all(&self, _ctx: &AuthContext) -> Result<ValidUser, PlexusError> {
        Ok(ValidUser { id: "placeholder".into() })
    }
}
```

Can a tool (linter, synapse audit, etc.) inspect the resolver body and flag patterns like "returns Ok without using the AuthContext argument"?

This is a static-analysis question. Macro alone can't look at function body. Options to investigate:
- Clippy lint?
- Separate `plexus-audit` binary that parses the activation impl block?
- Runtime metric: count rejections-per-method; a resolver that never rejects is suspicious

## Pass condition

Spike **passes** (= hole exists, no detection path) if: both resolvers compile, both produce identical wire schema, and no tool currently flags the fake one.

Spike **fails** (= at least some defense) if: any existing tool catches the suspicious resolver.

## Fail → next

Almost certainly confirmed — macro can't introspect resolver bodies. Mitigation RED-8: out-of-tree audit tool. Scope this as a separate ticket (not a hot path, but worth building for SOC2 evidence). Runtime metric (resolver-rejection count per method) could be a cheaper proxy.

## Out of scope

- Detecting adversarial resolvers (this is friendly-attacker scope)
- Forcing resolver shape via trait — worth considering but not in this spike
