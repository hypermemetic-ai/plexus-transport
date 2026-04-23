---
id: RED-S06
title: "Spike: mixed-auth activation — is it accepted silently?"
status: Pending
type: spike
blocked_by: []
unlocks: []
---

## Question

Is there a compile-time signal when an activation has BOTH methods that use `#[from_auth]` AND methods that don't? The mixed posture is a strong indicator of human error (forgot auth on one method) or undocumented intentional asymmetry — either way, worth flagging.

## Setup

Reuse the fixture from RED-S01. Instead of just verifying the unauthenticated method works, look for:

1. Any compile warning mentioning the asymmetry
2. Any entry in `plugin_schema()` that lets a tool flag it (e.g., a plugin-level bool `mixed_auth: true`)
3. Any synapse-side rendering that warns

## Pass condition

Spike **passes** (= asymmetry is silent) if none of the above signals exist.

Spike **fails** (= safe) if any of: compile warning, schema flag, or synapse render output highlighting asymmetry.

## Fail → next

Almost certainly confirmed silent. Mitigation RED-7: macro emits a compile warning (not error — backward compat) when an activation has asymmetric auth. Opt-out via `#[plexus::activation(auth_posture = "mixed")]` to acknowledge. Strict mode (future) upgrades to error.

## Out of scope

- Defining what "correct" mixed-auth looks like (that's strict mode design)
- Schema-level flag shape (design artifact of the mitigation, not the spike)
