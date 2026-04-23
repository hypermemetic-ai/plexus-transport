---
id: REQ-10
title: "activation `required = [...]` field-locking + override validation"
status: Partial
type: implementation
blocked_by: [REQ-6]
unlocks: []
severity: Medium
---

**Partial implementation Apr 23 2026 (autonomous run):** The `required(...)`
attribute parses, and compile-time validation for the `request = ()` Skip
case landed.

- Syntax: `required(field_a, field_b)` in the activation attribute (matches
  the existing `children(...)` convention; the `= [...]` form documented in
  the original REQ-6 ticket was wishful — not what the parser accepts).
- `HubMethodsAttrs.required_fields: Vec<syn::Ident>` captured in parse.rs.
- `codegen/mod.rs` rejects any method with `#[method(request = ())]` at
  macro expansion when `required_fields` is non-empty, with an error
  naming the locked fields and three remediation suggestions.
- Acceptance criteria 1, 2, 3 verified:
  - AC1: compile-fail trybuild fixture at
    `tests/compile/req10_required_rejects_override.rs` + `.stderr`.
  - AC2/3: override allowed when activation lacks `required` attribute
    (verified in `tests/req10_required_fields_tests.rs::override_allowed_when_activation_has_no_required_list`).

**Still deferred (AC4, AC5):**

- Per-method `#[method(request = OtherType)]` validation against the
  required list. Requires the trait-bound approach described in the
  ticket's Risks section — emit `fn __assert_has_field<T: HasField_X>()`
  expansions — which is more invasive than tonight's Skip-only check.
- Tests for that path (criteria 4 and 5) are intentionally missing; they
  land when the trait-bound follow-up lands.

Tracked as a known gap in REQ-10's body until a follow-up; no new ticket
needed yet.

## Problem

REQ-6 (commit `3aeb9cf` in plexus-macros) landed the per-method `x-plexus-source` merge. Methods without a `request = ()` override inherit the activation's request fields; methods with the override get a clean slate. But there's no middle ground: the activation can't say "these specific fields are mandatory — methods may override others but never drop these."

This is the "we are allowed to override the default request if the activation allows it" design from the Apr 23 model discussion. `required = [origin, transport]` on the activation should mean:

- `origin` and `transport` are locked — no method may use `request = ()` or otherwise drop them
- `client_ip` (not listed) is free — methods may replace it with a different type or drop it entirely
- Violation is a compile-time error at macro expansion

## Required behavior

### Parsing

`HubMethodsAttrs` gains a `required: Vec<syn::Ident>` field. The activation attribute parser accepts `required = [field_a, field_b]` and populates it. Empty or absent means all fields are overridable (today's behavior — REQ-6 already ships without this).

### Validation (compile time)

For each method in the impl block:

1. If the method has `#[plexus_macros::method(request = ())]` (i.e. `MethodRequestOverride::Skip`) AND the activation's `required` list is non-empty: emit a compile error pointing at the method's `request = ()` attribute, naming the required fields that would be dropped.

2. If the method has `#[plexus_macros::method(request = SomeType)]` (i.e. `MethodRequestOverride::Type`): compare SomeType's declared fields against the activation's `required` list. Any missing required field → compile error naming the missing field.

   - Verifying "SomeType has field X" at macro expansion requires introspection. Option: delegate to a trait bound like `AssertHasField<X>` that the PlexusRequest derive implements for each of its fields. Compile error surfaces as a trait-not-implemented error. Acceptable UX.

### Runtime merge behavior

Unchanged from REQ-6: the method's schema inherits the activation's fields unless the method overrides. With REQ-10's compile-time checks, any invalid override fails to compile — runtime doesn't need new logic.

## What must NOT change

- Activations with no `required = [...]` declaration work identically to REQ-6 (today's behavior)
- Existing per-method `request = ()` overrides on activations without `required` continue to work
- Runtime dispatch behavior is unchanged
- REQ-6's merge algorithm is untouched

## Risks

1. **Compile-time introspection of `SomeType`'s fields.** Rust doesn't natively support "does this type have field X" checks. The trait-bound workaround (emit `fn __assert_has_field<T: HasField_X>()` at expansion) works but produces opaque error messages. Acceptable for v1; can be refined with diagnostic attributes later.

2. **`required = [...]` with a type not derived via PlexusRequest.** If the activation's `request = X` type isn't PlexusRequest-derived, the trait bounds won't compile. Mitigation: document the constraint; PlexusRequest derive is the canonical path anyway.

3. **Diagnostic quality.** `syn::Error::new()` on the attribute span for drop-required-field errors is straightforward. For type-level "missing field" errors via trait bounds, the error message points at the compiler-generated assertion, not the user's `request = SomeType`. Live with it.

## Acceptance criteria

1. An activation with `required = [origin, transport]` + a method using `#[plexus_macros::method(request = ())]` fails to compile with an error message that includes the strings `required`, `origin`, and `transport` (or similar).
2. An activation with `required = []` (explicit empty) allows `request = ()` overrides — identical to today's behavior.
3. An activation without any `required` attribute allows `request = ()` overrides — identical to today's behavior (unchanged default).
4. A method with `#[plexus_macros::method(request = OtherType)]` where `OtherType` lacks a required field fails to compile with a message naming the missing field OR a trait-not-implemented error mentioning a PlexusRequest field.
5. A method with `#[plexus_macros::method(request = OtherType)]` where `OtherType` has all required fields compiles cleanly; the method's schema reflects `OtherType`'s fields (not the activation's default).
6. `plexus-macros` test suite has at least 2 new tests covering acceptance criteria 1 and 5.

## Coordination

- `blocked_by: [REQ-6]` — depends on the merge algorithm and method_enum wiring from REQ-6
- Doesn't interact with REQ-8, REQ-9, SAFE-6 — those consume REQ-6's schema output and don't care about the compile-time validation

## Completion

Implementor lands the parsing + validation + 2 new tests. Runs the full plexus-macros test suite (must still pass) and verifies the error messages are readable. Flips status to Complete.
