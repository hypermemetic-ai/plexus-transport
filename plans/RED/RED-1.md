---
id: RED-1
title: "RED — harden plexus-macros and activation against accidental auth bypass"
status: Epic
type: epic
blocked_by: []
unlocks: []
---

## Goal

Make it **structurally hard** for a developer to accidentally expose an endpoint that should require authentication. The current model relies on the developer remembering `#[from_auth(resolver)]` per method; forgetting it silently ships an unauthenticated method. This epic maps the auth-bypass attack surface, verifies each hole via spike programs, and proposes compile-time and runtime mitigations.

Threat model focuses on the *friendly* attacker: a tired developer making a three-line change, a refactor that accidentally drops an annotation, a new contributor who doesn't know the conventions. Adversarial JWT forgery and post-auth authz are out of scope.

## Attack surface (to investigate)

Each of these is a potential way a dev can ship an unauthenticated endpoint without realizing it:

1. **Silent omission**: new method added to an auth'd activation without `#[from_auth]` — compiles, dispatches unauthenticated
2. **Typo'd attribute**: `#[from_autho(...)]` — does the macro strip it (silent) or does rustc reject it (loud)?
3. **`request = ()` bypass**: method override drops activation-level extraction — does it also drop AuthContext population?
4. **Activation without `request = ...`**: `#[from_auth]` inside — does the resolver get `None` AuthContext and fail-closed, or silently succeed-open?
5. **Middleware bypass at the builder**: is there a way to construct a plexus-transport server that omits `CombinedAuthMiddleware`?
6. **Mixed-auth activation**: half the methods gate auth, half don't — is that an anti-pattern the macro should warn about?
7. **Resolver that doesn't actually validate**: `#[from_auth(self.fake_validator)]` where `fake_validator` always returns `Ok(FakeUser)` — macro can't catch this, but schema could surface it if resolver name is suspect
8. **Deserialization injection**: does `AuthContext` get populated from user-controllable JSON anywhere (e.g., in the method's request body)?

## Dependency DAG

```
RED-S01..S08  (spikes — execute in parallel)
      │
      ▼
  results → RED-2..N (mitigation tickets)
```

Spikes are all independent and can be written + run in parallel. Each returns a binary verdict: **hole confirmed** or **safe**. Confirmed holes get individual mitigation tickets.

## Phase breakdown

### Phase 1 — Map the attack surface (spike-heavy)
- RED-S01 through RED-S08: one-shot programs that exercise each attack vector
- Each spike produces a concrete yes/no + trace

### Phase 2 — Mitigate confirmed holes
- One ticket per confirmed hole, drafted after spikes run
- Typical mitigations: compile-time diagnostics, runtime fail-closed guards, explicit opt-in/opt-out markers

### Phase 3 — Strict-mode enforcement
- Propose `#[plexus::activation(auth = required)]` (or similar) that makes auth declaration explicit and mandatory
- Every method in a strict-mode activation must either consume auth (via `#[from_auth]`) or explicitly opt out (`#[no_auth]` or `#[public]`)
- Migration path for existing activations

## Tickets

| ID | Summary | Status |
|---|---|---|
| RED-S01 | Spike: silent auth omission in mixed-auth activation | Pending |
| RED-S02 | Spike: typo'd auth-related attribute handling | Pending |
| RED-S03 | Spike: `request = ()` bypass of AuthContext population | Pending |
| RED-S04 | Spike: activation without `request = ...` — does `#[from_auth]` fail-closed? | Pending |
| RED-S05 | Spike: builder surface — can auth middleware be omitted? | Pending |
| RED-S06 | Spike: mixed-auth activation — is it accepted silently? | Pending |
| RED-S07 | Spike: fake-resolver detection — can schema flag a validator that never rejects? | Pending |
| RED-S08 | Spike: `AuthContext` injection via request body / unsafe deserialization | Pending |

Mitigation tickets (RED-2..N) will be added after spikes complete. Placeholders not written yet to avoid prejudging results.

## Out of scope

- Cryptographic JWT validation (library concern)
- Authorization logic past authentication (RBAC, tenant isolation — separate SOC2 epic)
- Rate limiting / DDoS hardening
- Transport-layer TLS enforcement (handled by `SecureTransport` extractor today)
- Session hijacking / cookie fixation (outside macro surface)

## Success criteria

Epic is Complete when:
1. All 8 spikes have run and their verdicts recorded
2. Every confirmed hole has a mitigation ticket (Pending or better)
3. At least one of the mitigations is landed — the one that addresses the highest-severity confirmed hole
4. A developer adding a method to an auth'd activation cannot do so without either (a) the method requiring auth, or (b) a loud compile-time acknowledgment that this method is intentionally public
