---
id: RED-S13
title: "Spike: log leakage of JWT / cookie / AuthContext fields"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-13]
---

## Verdict (2026-04-22)

**CRITICAL HOLE CONFIRMED.**

- `plexus-transport/src/mcp/server.rs:73` logs all incoming HTTP headers at `info!` level: `tracing::info!("    {}: {:?}", name, value)`. Same code path also at `:100` on an error branch. This unconditionally emits `Authorization: Bearer <JWT>` and `Cookie: access_token=<JWT>` strings to the log. In production-default config these reach log aggregation.
- `plexus-core/src/plexus/test_validator.rs:46` — `TestSessionValidator` logs raw cookie values at `debug!`. Acceptable for test fixtures but would burn if TestSessionValidator leaks into a prod deploy.
- No hits on raw JWT emission outside these two sites. `AuthContext` fields (user_id, session_id) are not logged directly elsewhere at info+ level.

**Classification:** critical because it's a no-auth-to-get-credentials path — anyone with log-read access gets reusable session material (JWT good until expiry, session cookie good until logout).

**Mitigation:** RED-13 — sanitize MCP header logging (list header names at debug, redact values except safelist); downgrade TestSessionValidator cookie log to `trace!` or mask.

## Question

Does ANY log line in the plexus stack — at any tracing level — emit raw JWT tokens, cookie values, or sensitive `AuthContext` fields (user_id, session_id, roles, metadata)? Production log aggregation typically captures info+ levels; warn/error definitely. If a JWT lands in a log, it's reusable credential material.

## Setup

Grep the codebase:

```
grep -rn "tracing::.*\|info!(\|debug!(\|warn!(\|error!(" \
  plexus-core/ plexus-transport/ plexus-macros/ synapse/ synapse-cc/ \
  | grep -iE "token|cookie|auth_ctx|AuthContext|jwt|session"
```

For each hit, classify:
- Safe: logs only derived metadata (e.g., `user_id` short-form, not the raw JWT)
- Risky: logs the raw Cookie header value, the full AuthContext, or unparsed token string
- Critical: emits the JWT string

Specifically check:
- `plexus-transport/src/websocket.rs` CombinedAuthMiddleware paths (both success and failure)
- SessionValidator implementations (TestSessionValidator, Keycloak integration)
- `Synapse.Transport` / transport error rendering
- Any panic handlers that dump state

## Pass condition

Spike **passes** (= hole confirmed) if ANY hit logs the raw cookie value, raw JWT, or full AuthContext at ANY level (even `debug`).

Spike **fails** (= safe) if only derived/short-form metadata appears, never the source credentials.

## Fail → next

Confirmed leaks → mitigation: redact those lines. Add a `#[serde(serialize_with = "redact")]` on sensitive AuthContext fields, or custom Display impl that masks. Grep and update any log macro calls.

## Out of scope

- External log aggregation configuration (operational concern)
- Rotation / retention policy for logs
- Client-side console.log from generated TS (separate audit)
