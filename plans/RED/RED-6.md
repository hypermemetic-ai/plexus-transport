---
id: RED-6
title: "Strict-mode activation: every method must declare auth posture explicitly"
status: Idea
type: implementation
blocked_by: [RED-2, RED-3, RED-4]
unlocks: []
severity: Medium
---

## Problem

Even with RED-2 (mixed-auth warning), RED-3 (compile error on from_auth without request), and RED-4 (runtime assertion at server build), an activation author still has to *remember* to add `#[from_auth]` to a new method if the method should require auth. The consistency check catches mixed-auth but only after the dev has written something inconsistent. Strict mode goes further: the activation explicitly declares its auth posture and every method must satisfy it.

## Goal (sketch — not yet a contract)

```rust
#[plexus::activation(
    namespace = "clients",
    request = FormVeritasRequest,
    auth = required,             // NEW: every method must authenticate
)]
impl ClientsActivation {
    // Compiles: has #[from_auth]
    #[plexus::method]
    async fn list(&self, #[from_auth(...)] u: ValidUser, ...) -> ... { }

    // COMPILE ERROR: auth = required but no #[from_auth] and no #[public] acknowledgment
    #[plexus::method]
    async fn leak(&self) -> ... { }

    // Compiles: explicit public acknowledgment
    #[plexus::method(public)]
    async fn health(&self) -> ... { }
}
```

Three posture values:
- `auth = required`: every method must either use `#[from_auth]` or carry `#[plexus::method(public)]` on the method — compile error otherwise
- `auth = optional`: methods may or may not use `#[from_auth]`; macro emits RED-2-style warning on asymmetry
- `auth = none`: no method may use `#[from_auth]` (affirmative declaration of a public activation); compile error if any method has it

Default posture when `auth = ...` is not declared: `optional` (backward-compatible with today).

## Why this ticket is Idea-status

- Depends on RED-2, RED-3, RED-4 landing first (they are the foundation signals)
- Depends on the activation-attr naming convention — `auth` could conflict with future `auth_posture = "mixed"` from RED-2; the two need harmonizing
- Per-method `#[public]` attribute is new surface area; adds to the macro's attribute vocabulary
- Backward-compat migration path needs planning (existing activations default to `optional`)

## Required behavior (when promoted to Pending)

Will write full ticket after RED-2/3/4 land and the actual mitigation shape settles.

## Coordination

- `blocked_by: [RED-2, RED-3, RED-4]`
- After this lands, REQ-10's `required(...)` lock list becomes redundant for the `auth = required` case; might deprecate the lock list in favor of the posture attribute, or keep both as independent controls (fields-are-required vs auth-is-required)

## Completion

Promoted to Pending once RED-2/3/4 are Complete and the design is stable. Implementation + tests + migration notes drafted then.
