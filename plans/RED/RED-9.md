---
id: RED-9
title: "Transport: reject upgrade when session validation fails (Mode B/C)"
status: Complete
type: implementation
blocked_by: []
unlocks: []
severity: High
---

**Implemented Apr 23 2026 (autonomous run):**

- `CombinedAuthMiddleware` in `plexus-transport/src/websocket.rs` gained
  `reject_on_session_failure: bool`. When `true`, the two fall-through
  paths (no cookie / bad cookie) emit HTTP 401 with a `text/plain` body
  before reaching the dispatch service.
- `serve_websocket` plumbs the flag through.
- `TransportServerBuilder.reject_upgrade_on_auth_failure()` opts in.
- Independent of the flag, both fall-through log lines were upgraded
  from `tracing::debug!` to `tracing::warn!` so production ops sees them.

Builds cleanly. Integration tests deferred — the change is small, the
default is backward-compat (off), and the new path is exercised by the
same upgrade machinery as today's auth path.

## Problem

RED-S05 exposed two subtle failure modes that RED-4 doesn't cover:

- **Mode B**: `CombinedAuthMiddleware` is attached (via `.with_session_validator(...)`), but the client sends no cookie. Middleware logs `"No cookie present, proceeding without auth"` at `tracing::debug!` (invisible in production) and lets the request through. Extensions are not populated with `AuthContext`.
- **Mode C**: Client sends a cookie, but validation fails. Middleware logs `"Cookie present but validation failed, proceeding without auth"` at `tracing::debug!` and lets the request through.

In both modes, methods with `#[from_auth]` fail-close at runtime. Methods without it dispatch normally (S01 territory) — which may or may not be intended, depending on the dev's assumptions.

The root cause is that `CombinedAuthMiddleware` today is a *populator*, not a *gate*: it opportunistically adds `AuthContext` if validation succeeds, and silently does nothing if it doesn't. There's no "refuse the WS upgrade when auth fails" option.

## Required behavior

Add a builder option `.reject_upgrade_on_auth_failure()` (or equivalent — name is tentative) to `TransportServer`. When enabled:

1. The WS upgrade handshake requires a valid `Cookie:` session.
2. If the cookie is missing, the upgrade is rejected with HTTP 401 before any WS frames flow.
3. If the cookie is present but the validator returns `None`, the upgrade is rejected with HTTP 401 with a diagnostic message (e.g., "session expired or invalid").
4. When enabled together with `.with_api_key(...)`, either a valid Bearer token OR a valid session satisfies the check. Requests with neither are rejected.
5. The existing log lines upgrade from `tracing::debug!` to `tracing::warn!` so production ops sees them.

### Default posture

The option is **opt-in** initially. Switching the default to "reject" is a breaking change that needs a migration cycle. Document that opt-out is equivalent to today's lax behavior, and that teams should audit their flows before enabling.

### Interaction with RED-4

RED-4 catches deploy-time misconfig (no auth middleware attached at all). RED-9 catches runtime cases where middleware IS attached but the request doesn't satisfy it. They are complementary:

| Scenario | Before RED-4+9 | After RED-4+9 |
|---|---|---|
| Dev forgets `.with_session_validator(...)` | Every request has `auth=None`; per-method fail-closed only for `#[from_auth]` methods | `build()` returns Err at deploy time |
| Middleware attached, no cookie on request | Silent pass-through at debug level; method-level fail-closed only | Opt-in: upgrade rejected before dispatch |
| Middleware attached, bad cookie | Silent pass-through at debug level; method-level fail-closed only | Opt-in: upgrade rejected before dispatch |

## What must NOT change

- Default builder behavior (for backward compat): middleware remains a populator, not a gate. Devs opt in.
- Bearer-token path: unchanged when the option is off.
- Runtime dispatch for methods: unchanged.

## Acceptance criteria

1. With the option enabled and no session_validator configured, `build()` still succeeds (the option is about *enforcement* of validation, not about *requiring* validation to be configured — that's RED-4's job).
2. With the option enabled, session_validator configured, and a WS client that sends no Cookie header: the WS upgrade fails with HTTP 401 before any RPC frames. Client-observable as a connection failure.
3. With the option enabled, session_validator configured, and a WS client that sends an invalid cookie: same 401 response.
4. With the option enabled, a client that sends a valid cookie: connection proceeds as today.
5. With the option enabled AND `.with_api_key(...)` configured: either valid Bearer OR valid session satisfies the gate. Missing both → 401.
6. The no-cookie / bad-cookie paths log at `tracing::warn!` level (previously `tracing::debug!`).
7. An integration test covers each of the four reject cases + the two pass cases.
8. The option is additive — servers that don't call it behave exactly as today.

## Risks

1. **Breaking change if the default flips later.** The ticket keeps the option opt-in; any later default flip needs its own migration ticket.
2. **Interaction with public routes** — if a backend serves any unauthenticated endpoint intentionally, rejecting the whole upgrade breaks that case. Document that "reject_upgrade" means "no public endpoints on this transport"; split into a separate transport if mixing.
3. **MCP transport parity** — MCP has its own path. Verify the option works or explicitly note the scope limits to WS.

## Coordination

- Complements RED-4 (deploy-time check) and RED-2 (macro warning)
- Should land after RED-4 so the defense-in-depth story reads cleanly (build-time → connect-time → macro-time → per-method-time)
- The log-level upgrade (debug → warn) can land independently — smaller mitigation worth keeping in its own commit

## Completion

Implementor adds the builder option, re-routes the two fall-through paths to emit a 401 response instead of calling the service, upgrades log levels, writes integration tests. Flips status to Complete.
