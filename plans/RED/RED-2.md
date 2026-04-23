---
id: RED-2
title: "Macro diagnostic on mixed-auth activation (addresses S01 + S06)"
status: Complete
type: implementation
blocked_by: []
unlocks: []
severity: High
---

**Implemented Apr 23 2026 (autonomous run):** Compile warning landed via the
dummy-deprecated-const pattern used by existing IR-5 warnings.

- `HubMethodsAttrs.auth_posture_mixed` parses `auth_posture = "mixed"` activation-level opt-out
- `HubMethodAttrs.public` (mirrored to `MethodInfo.public`) parses `#[method(public)]` per-method opt-out
- `mixed_auth_warning()` in codegen/mod.rs detects asymmetry; wires into the generate() output alongside other warnings
- 4 acceptance tests in `red2_mixed_auth_warning_tests.rs` covering all four shape combinations
- Manual smoke: warning fires on `tests/red_s01_silent_omission.rs` (the spike fixture) and `tests/req6_from_auth_tests.rs::skip` (the request = () override case)
- 90/90 plexus-macros tests pass

## Problem

An activation with some methods using `#[from_auth(resolver)]` and other methods without it ships silently (confirmed by RED-S01 + RED-S06). Zero compile warnings, no schema flag, no `cargo clippy` rule. A tired developer adding a new method to such an activation loses the auth gate without any indicator.

## Required behavior

`plexus-macros` emits a compile-time diagnostic when a `#[plexus::activation]` impl block contains at least one method using `#[from_auth]` AND at least one method that doesn't. The activation can opt out explicitly if the asymmetry is intentional.

| Activation shape | Macro behavior |
|---|---|
| All methods use `#[from_auth]` (uniformly authed) | no diagnostic |
| No methods use `#[from_auth]` (uniformly public) | no diagnostic |
| Mixed, no acknowledgment | **warning** naming the unauth'd methods |
| Mixed, with `#[plexus::activation(auth_posture = "mixed")]` explicit | no diagnostic |
| Mixed, with `#[plexus::method(public)]` on each non-auth method | no diagnostic (per-method acknowledgment) |

Warning message:

```
warning: activation `clients` has methods with `#[from_auth]` but method `leak`
  has no auth gate. If this is intentional, annotate `leak` with
  `#[plexus::method(public)]`, or set `#[plexus::activation(auth_posture = "mixed")]`
  on the impl block.
  --> src/activations/clients/activation.rs:42:14
```

Warning not error — doesn't break existing codebases. Can be upgraded to error in a future strict-mode release.

## What must NOT change

- Existing uniformly-auth'd and uniformly-public activations: zero new diagnostics
- Runtime dispatch behavior: unchanged
- Wire schema: unchanged (mitigation is macro-level only)

## Acceptance criteria

1. A test fixture with a mixed-auth activation compiles with a warning whose text includes both the activation namespace and the unauth'd method's name.
2. The same activation with `auth_posture = "mixed"` on the impl compiles without the warning.
3. The same activation with `#[plexus::method(public)]` on every non-auth method compiles without the warning.
4. A uniformly-auth'd activation (all methods have `#[from_auth]`) produces no warning.
5. A uniformly-public activation (no methods have `#[from_auth]`) produces no warning.
6. The warning is emitted at macro expansion time, not runtime.

## Risks

1. **Warning fatigue.** If many existing activations trigger this, developers disable it globally. Mitigation: ship the warning behind a feature-flag initially, enable by default after a cycle of migration.
2. **`#[plexus::method(public)]` is a new attribute.** Adds surface area. Alternative: rely only on activation-level `auth_posture = "mixed"` and skip per-method opt-in. Simpler, coarser.

## Completion

Implementor adds diagnostic + test fixtures; runs plexus-macros test suite; commits; flips status to Complete.
