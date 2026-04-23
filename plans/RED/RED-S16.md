---
id: RED-S16
title: "Spike: `ValidOrigin` extractor fail-open vs fail-closed"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-16]
---

## Verdict (2026-04-22)

**PARTIAL HOLE.**

- **Propagation: SAFE.** `extract_from_raw` returns `Err(PlexusError::…)` on disallowed origin; the macro-generated call wrapper `?`-propagates that error before invoking the method body. An authorized-origin call to an opted-in request type succeeds; a disallowed-origin call returns a JSON-RPC error with a recognizable message. Server log emits the rejection at info level.
- **Absent Origin header: DOCUMENTED CHOICE.** Returns `ValidOrigin("")`. This is a deliberate affordance for CLI / non-browser clients. Not a hole on its own, but a combined-attack surface with the next point.
- **HOLE: allow-all fallback is silent.** When `ALLOWED_ORIGINS` env is unset, the validator logs `"ALLOWED_ORIGINS not set — all origins permitted."` at `INFO`. INFO is easy to lose in production log volume. A dev-mode default shipped to prod without setting the env results in silent allow-all — exactly the posture one thinks is locked. No startup refusal, no WARN, no interaction with RED-4's build-time check.

**Earlier REQ-11 observation explained:** the `synapse --header origin=https://evil.example.com uscis health check` succeeding is fully explained by `health.check` declaring `request = ()` to skip activation-level extraction. On a method that opts in and has `ALLOWED_ORIGINS` set, rejection works correctly.

**Mitigation:** RED-16 — promote unset-`ALLOWED_ORIGINS` startup log to WARN and surface it in the activation build-time check when any request type declares `ValidOrigin`. Optionally refuse to start in release builds with a `Result<_, ConfigError>` unless an explicit `.allow_any_origin_for_dev()` opt-in is present.

## Question

Earlier verification (REQ-11 against uscis) observed that calling `synapse --header origin=https://evil.example.com uscis health check` succeeded. We attributed this to `health.check` using `request = ()` to skip activation-level extraction — but what about methods that DON'T opt out? Does `ValidOrigin::extract_from_raw` actually reject disallowed origins, and does that rejection propagate as a call-level error?

Edge cases:
- Origin absent entirely (CLI / non-browser client) — documented behavior: returns `ValidOrigin("")`. Acceptable, but is it safe?
- Origin present and disallowed — documented: returns `Err(PlexusError::...)`. But does the call wrapper actually propagate this?
- `ALLOWED_ORIGINS` env var not set — validator allows all origins per startup log "ALLOWED_ORIGINS not set — all origins permitted." Is this documented as a dev-mode-only behavior?

## Setup

1. Read `plexus-transport/src/request/origin.rs`. Confirm extraction logic matches documented behavior.
2. Read the macro-generated call wrapper (`plexus-macros/src/codegen/activation.rs` around the request extraction block). Does it propagate extraction errors before invoking the method body?
3. Test end-to-end against uscis with a method that does NOT have `request = ()`:
   - Restart uscis with `ALLOWED_ORIGINS="https://app.example.com"` set
   - Call a non-override method from an allowed origin → should succeed
   - Call the same method from a disallowed origin → should fail
   - Call with no Origin header → should succeed (empty-string sentinel)
4. Verify the server log reflects the rejection (operational visibility)

## Pass condition

Spike **passes** (= hole confirmed) if a disallowed Origin on a non-override method fails to be rejected — silently dispatches, or fails in a way that doesn't surface the security reason.

Spike **fails** (= safe) if rejection is consistent, loud, and propagated to the client.

Secondary check: `ALLOWED_ORIGINS` unset = allow-all is a known dev-mode footgun. Mitigation idea: log a WARN at startup (not INFO) if no allowlist is configured but the activation uses `ValidOrigin`.

## Fail → next

If disallowed origin silently passes → immediate mitigation: verify the error path in the macro wrapper. If the allowlist-unset footgun is a concern → add a WARN log at startup.

## Out of scope

- Non-Origin headers (Host, Referer) — separate hardening
- TLS termination policy
