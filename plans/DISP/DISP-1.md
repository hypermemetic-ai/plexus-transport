---
id: DISP-1
title: "Scaffolding: Dispatcher + AuthChain + OriginPolicy + PostureCheck + Gateway trait"
status: Pending
type: task
blocked_by: []
unlocks: [DISP-2, DISP-3, DISP-4, DISP-5, DISP-6]
---

## Context

Plexus currently has four ad-hoc gateway implementations (WebSocket, REST, MCP, stdio), each of which funnels requests into `Activation::call(method, params, auth, raw_ctx)` but reinvents the surrounding work — cookie/bearer extraction, origin validation, posture enforcement, error mapping. RED round 2 found this reinvention is where the leaks are: REST drops auth/raw_ctx entirely (RED-S10), MCP passes None for auth (RED-S11), and there's no single place to add CSRF/origin enforcement (RED-S15, RED-S16).

The fix is structural: a single `Dispatcher` type owns the invariant work, and gateways become thin protocol shells over it.

## Goal

Introduce the shared dispatch kernel as new types in `plexus-transport/src/dispatch/` with zero callers. No behavior change — existing gateways continue on their ad-hoc paths. This ticket makes the scaffolding compile and be unit-tested.

## Types to introduce

```rust
// plexus-transport/src/dispatch/mod.rs
pub struct Dispatcher {
    activation: Arc<dyn Activation>,
    auth_chain: AuthChain,
    origin_policy: OriginPolicy,
    posture: PostureCheck,
}

impl Dispatcher {
    pub async fn dispatch(
        &self,
        method: &str,
        params: Value,
        ctx: RawRequestContext,
    ) -> Result<PlexusStream, PlexusError> {
        self.origin_policy.check(&ctx)?;
        let auth = self.auth_chain.validate(&ctx).await?;
        self.posture.check(method, auth.as_ref())?;
        self.activation.call(method, params, auth.as_ref(), Some(&ctx)).await
    }
}

// plexus-transport/src/dispatch/auth_chain.rs
pub struct AuthChain {
    validators: Vec<Box<dyn Validator>>,  // cookie, bearer, custom in order
}

// plexus-transport/src/dispatch/origin.rs
pub struct OriginPolicy {
    allowlist: Option<HashSet<String>>,
    allow_missing_origin: bool,
}

// plexus-transport/src/dispatch/posture.rs
pub struct PostureCheck {
    // Precomputed at build time: which methods require auth, including child activations.
    // Absorbs RED-10 (recursive schema walk) and RED-6 posture enforcement.
    auth_required: HashSet<MethodPath>,
    auth_none: HashSet<MethodPath>,
}

// plexus-transport/src/gateway.rs
#[async_trait]
pub trait Gateway: Send {
    async fn serve(self, dispatcher: Arc<Dispatcher>) -> Result<(), GatewayError>;
}
```

Exact shapes may evolve during implementation — the above is the target, not a spec.

## Acceptance

- [ ] New module `plexus-transport/src/dispatch/` with `Dispatcher`, `AuthChain`, `OriginPolicy`, `PostureCheck`.
- [ ] `AuthChain` wraps the existing `SessionValidator` trait + bearer-token extraction (moved, not rewritten). Runs validators in configured order; first success wins. Returns `Option<AuthContext>`.
- [ ] `OriginPolicy::check` returns `Err(PlexusError::…)` for disallowed origin. Reads allowlist from explicit builder config OR `ALLOWED_ORIGINS` env. Logs WARN at construction time if allowlist is unset and origin validation is active (absorbs RED-16).
- [ ] `PostureCheck::walk(schema)` recursively visits `schema.children` — not just root (absorbs RED-10). `check(method, auth)` returns `Err(PlexusError::AuthenticationError)` if method is in `auth_required` and `auth.is_none()`.
- [ ] `Gateway` trait defined with a single `serve(dispatcher)` method.
- [ ] Unit tests against a mock `Activation`:
  - Dispatcher happy path: mock activation receives threaded auth + raw_ctx
  - AuthChain: bearer token validates; cookie validates; invalid returns None (not an error — posture check decides fail-closed)
  - OriginPolicy: allowlist match succeeds; mismatch errors; env-unset + no explicit config logs WARN
  - PostureCheck: auth-required method with None auth errors; auth-required with Some auth succeeds; recursive walk finds child-declared auth-gated methods
- [ ] CI green — no existing gateway code touched, no behavior change.

## Out of scope

- Wiring any gateway to use the new types (DISP-2 through DISP-5).
- Deleting the existing `CombinedAuthMiddleware` / bearer-extraction code (DISP-7).
- Deleting the legacy 2-arg `Activation::call` (DISP-3).

## Notes

This is the load-bearing ticket for the whole DISP epic. The shape of `Dispatcher::dispatch` is the contract that every subsequent ticket consumes. If the interface needs to change later, all 5 downstream tickets get dragged along — worth getting right here.

The order of checks inside `dispatch` matters: origin first (cheapest, rejects CSRF attempts before any auth work), then auth validation, then posture. This also matches "defense in depth" — an attacker who bypasses one layer hits the next.

Absorbs the following RED mitigations:
- RED-10 (recursive posture walk) — `PostureCheck::walk`
- RED-15 (upgrade-time origin rejection) — `OriginPolicy::check` runs before auth
- RED-16 (unset ALLOWED_ORIGINS WARN) — `OriginPolicy::new` logs at construction
