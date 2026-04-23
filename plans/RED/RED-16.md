---
id: RED-16
title: "Loud unset-ALLOWED_ORIGINS fallback + RED-4 integration"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S16 verdict: the `ValidOrigin` extractor propagates rejection errors correctly (safe), but the allow-all fallback when `ALLOWED_ORIGINS` env is unset is silent:

- `origin.rs` logs `"ALLOWED_ORIGINS not set — all origins permitted."` at **INFO** on startup. INFO is easy to lose in production log volume, and shipping a dev-default to prod results in a silent allow-all posture precisely where it's thought to be locked.
- No interaction with RED-4's build-time check. An activation with a request type declaring `ValidOrigin` passes the startup check even with no allowlist configured.

## Goal

Make the unset-allowlist state impossible to miss, and surface it as a build-time concern when any activation actually uses `ValidOrigin`.

## Acceptance

- [ ] Promote the "ALLOWED_ORIGINS not set" log from INFO to **WARN**. Include the string `"all origins permitted"` verbatim so log-search catches it.
- [ ] RED-4 collector: extend `collect_from_schema` (or a sibling pass) to detect when any request type in the activation tree declares `ValidOrigin`. If so AND the process starts with no `ALLOWED_ORIGINS` env set AND no explicit builder opt-out, refuse to start with a named error naming the activations that rely on origin validation.
- [ ] New builder opt-out: `.allow_any_origin_for_dev()` — explicit, loud, documented as dev-only. Debug builds start without it (with the WARN). Release builds refuse without either the env var or the opt-out.
- [ ] Test: activation with a request declaring `ValidOrigin`, no env, no opt-in — release build refuses to start; debug build starts with WARN.
- [ ] Test: same activation with `ALLOWED_ORIGINS` set — starts cleanly.
- [ ] Test: same activation with `.allow_any_origin_for_dev()` opt-in — starts cleanly, WARN still emitted.
- [ ] Docs: `## Origin validation` section in `plexus-transport` README describes the three states (env set, env unset + opt-in, env unset + no opt-in).

## Out of scope

- Moving origin enforcement to the WS upgrade (covered by RED-15; this ticket is specifically about the fallback default).
- Changing the `ValidOrigin` extractor semantics.

## Notes

This is the smallest-possible change to make the footgun loud. The WARN alone catches most cases; the build-time refusal in release mode catches the rest. Together with RED-15 (enforcement at upgrade time) the Origin posture story becomes: "enforced at upgrade, enforced per-method, and impossible to accidentally ship with allow-all in release."

Debug/release distinction rationale: dev productivity demands the no-env-set path keep working; prod demands it fail loud. Debug assertions are the existing precedent (e.g. `debug_assert!`).
