---
id: RED-3
title: "Compile error on `#[from_auth]` without activation-level `request = ...` (S04)"
status: Pending
type: implementation
blocked_by: []
unlocks: []
severity: Medium
---

## Problem

RED-S04 confirmed that `#[from_auth(resolver)]` inside an activation that has no `request = ...` declaration compiles. At runtime it fails-closed (the macro-generated wrapper rejects `None` AuthContext), so there's no silent-unauth hole — but the failure happens at runtime, on every call, instead of at compile time where it belongs.

## Required behavior

`plexus-macros` emits a compile error when any method in a `#[plexus::activation]` impl uses `#[from_auth(resolver)]` but the activation attribute does not declare `request = SomeType`. The message explains that auth extraction requires an activation-level request struct.

| Activation shape | Method has `#[from_auth]`? | Behavior today | Behavior after |
|---|---|---|---|
| has `request = X` | yes | ✓ works | ✓ works |
| has `request = X` | no | ✓ works | ✓ works |
| no `request = ` | yes | compiles, runtime 401 on every call | **compile error** |
| no `request = ` | no | ✓ works | ✓ works |

Error message:

```
error: method `list` uses `#[from_auth(self.validate)]` but the activation
       declares no `request = SomeType` on the impl. Auth extraction requires
       a PlexusRequest struct to wire AuthContext through. Add
       `request = MyRequest` to `#[plexus::activation(...)]`, or remove the
       `#[from_auth]` annotation.
```

## What must NOT change

- Activations with `request = ...` declared: unchanged
- Activations without `#[from_auth]` methods: unchanged regardless of request declaration
- Runtime dispatch behavior: unchanged

## Acceptance criteria

1. A fixture with `#[from_auth]` inside an activation lacking `request = ...` fails to compile with an error containing both the method name and the string "request = ".
2. A fixture with `#[from_auth]` inside an activation declaring `request = X` compiles cleanly.
3. A fixture without any `#[from_auth]` method in an activation lacking `request = ...` compiles cleanly (no false positive).
4. At least two trybuild test fixtures exist (one pass, one fail).

## Completion

Implementor adds check in `plexus-macros/src/codegen/mod.rs` post-MethodInfo-parsing, using `args.request_type.is_none() && methods.iter().any(|m| !m.auth_resolvers.is_empty())`. Writes trybuild tests; commits.
