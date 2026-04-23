---
id: RED-S02
title: "Spike: typo'd auth-related attribute handling"
status: Pending
type: spike
blocked_by: []
unlocks: []
---

## Question

If a developer mistypes `#[from_auth(resolver)]` — as `#[from_autho(...)]`, `#[from_auth_ctx(...)]`, `#[FromAuth(...)]`, etc. — does the macro:

(a) silently strip the attribute (bad — resolver isn't wired, method runs unauth)
(b) rustc-reject the unknown attribute (good — loud failure)
(c) macro-reject with a helpful error (best)

## Setup

Test fixture in plexus-macros/tests/compile/:

```rust
// Variant 1: missing 'h'
#[from_aut(self.validate)] _u: FakeUser,

// Variant 2: extra letter
#[from_autho(self.validate)] _u: FakeUser,

// Variant 3: case variation
#[From_auth(self.validate)] _u: FakeUser,

// Variant 4: plausible-looking different name
#[from_auth_context(self.validate)] _u: FakeUser,
```

For each variant, compile and observe behavior.

Inspect the macro strip logic at `plexus-macros/src/codegen/mod.rs` (the `pat_type.attrs.retain(|attr| !attr.path().is_ident("from_auth") && …)` block). Does `is_ident` exact-match protect against typos, or does it silently accept and then strip a prefix?

## Pass condition

Spike **passes** (= hole confirmed) if ANY typo variant:
- Compiles without the resolver being invoked, AND
- The method dispatches as if authenticated

Spike **fails** (= safe) if every typo either (a) fails to compile or (b) causes the resolver to actually run.

## Fail → next

Confirmed hole → RED-3 mitigation: macro-side validation that rejects known-typo variants with a "did you mean #[from_auth]?" diagnostic. Levenshtein distance of 1 or 2 against the known attribute set.

## Out of scope

- Intentional rename of the attribute (covered by Deprecate/Alias machinery, not this spike)
- Non-auth attributes (from_cookie, from_header typos — similar problem, but separate severity)
