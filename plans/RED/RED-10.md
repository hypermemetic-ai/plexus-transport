---
id: RED-10
title: "RED-4 auth-middleware check: recurse into child activations"
status: Pending
type: task
blocked_by: []
unlocks: []
---

> **Implementation vehicle: DISP-1.** Recursive schema walk is absorbed into `PostureCheck::walk`. This ticket's acceptance criteria map onto DISP-1's PostureCheck unit tests.


## Problem

RED-4 introduced a build-time assertion that refuses to start a `TransportServer` if any activation declares auth-gated methods but no `SessionValidator` is wired. RED-S09 confirmed the check only walks the root activation schema: `plexus-transport/src/server.rs` `collect_from_schema` (around L376–402) inspects `schema.methods` but not `schema.children`.

Consequence: a hub-style deploy where the parent is `auth = "none"`/`"optional"` but a child activation declares `#[from_auth]` passes the startup check. Runtime dispatch is still safe (the child's macro wrapper fail-closes independently — confirmed safe in RED-S09), but the build-time safety net is incomplete and the 401-on-every-call footgun from RED-S11 applies.

## Goal

`collect_from_schema` walks the full activation tree. Children inherit the same "has auth-gated methods?" check as the root; any auth-gated method anywhere in the tree triggers the validator requirement.

## Acceptance

- [ ] Recursive walk in `collect_from_schema` visits `schema.children` and flags auth-gated methods found anywhere.
- [ ] Error message names the child activation path (e.g. `"FormsHub::ClientsChild::create"`) so the dev knows which sub-activation triggered the check.
- [ ] Unit test: hub with no auth at root but a child with `#[from_auth]` must refuse to start without `.with_session_validator()` (and must start cleanly with one).
- [ ] Unit test: nested hub (hub-of-hubs) — check walks arbitrary depth.
- [ ] RED-4's existing test (root-only auth-gated) still passes.

## Out of scope

- Changing runtime dispatch semantics (already safe per S09).
- Auth posture declarations on children (orthogonal; covered by RED-6 if desired).

## Notes

The RED-4 opt-out `.allow_missing_auth_middleware()` remains the same; this just makes the collector see the full surface.
