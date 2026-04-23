---
id: RED-S15
title: "Spike: CSRF on cookie-based WebSocket auth"
status: Complete
type: spike
blocked_by: []
unlocks: [RED-15]
---

## Verdict (2026-04-22)

**MODERATE HOLE CONFIRMED.**

- `ValidOrigin` is a per-method `PlexusRequestField` extractor in `plexus-transport/src/request/origin.rs`. It runs inside the macro-generated call wrapper, AFTER the WS upgrade completes and AFTER dispatch has begun.
- `CombinedAuthMiddleware` at WS upgrade time validates the session cookie but does NOT check `Origin`. A request with `Origin: https://evil.com` and a valid session cookie succeeds the upgrade and establishes a duplex connection. Methods that don't opt into `ValidOrigin` (which is the common case — devs would have to add it to every request type) dispatch freely.
- No Plexus-side documentation or builder hook mandating `SameSite=Strict`. Cookie issuance is delegated to the host app (Keycloak adapter etc.), so the posture depends entirely on upstream config. An operator who doesn't know to check this ships Lax by default.
- No CSRF token mechanism.

**Classification:** moderate because modern browser defaults (SameSite=Lax since Chrome 80, Firefox 96) blunt the attack for many cookie configs — Lax allows same-site navigations but NOT WebSocket-fetch cross-origin. But Plexus does not enforce or verify this at the transport layer; a SameSite=None cookie (common for SSO flows spanning domains) re-opens the hole fully.

**Mitigation:** RED-15 — transport-layer allowlist. `.require_origin_allowlist(&[..])` on the server builder rejects WS upgrades with disallowed Origin BEFORE auth middleware runs. Startup log names the allowlist (or loud WARN if unset). Defense in depth with per-method `ValidOrigin`.

## Question

Plexus cookie auth relies on the browser's same-origin cookie store. A classic cross-site-request-forgery attack: `evil.com` tricks `user.example.com`'s browser into opening a WebSocket to `api.example.com`. The browser automatically sends the session cookie. The request looks authenticated. Does plexus-transport have any CSRF protection?

Three layers that might help:
1. **Origin validation.** `ValidOrigin` in plexus-transport validates against an allowlist. Is it enforced on WS upgrade, and what happens if it fails?
2. **SameSite cookie directive.** The server sets Cookie; if `SameSite=Strict` or `SameSite=Lax`, browsers won't send it cross-origin. Does the auth flow mandate/document this?
3. **CSRF token pattern.** Separate token in a header or body, not in the cookie. Does plexus have a mechanism for this?

## Setup

1. Read `plexus-transport/src/request/origin.rs` and `src/websocket.rs`. Is `ValidOrigin` extraction wired in a way that rejects WS upgrades from disallowed origins BEFORE dispatch?
2. Actually test: with uscis running and `ALLOWED_ORIGINS="https://app.example.com"`, send a WS upgrade with `Origin: https://evil.com`. Does it connect? Does the auth'd method call succeed?
3. Check documentation: does plexus-rpc/plexus-transport documentation explicitly advise `SameSite=Strict` on the session cookie?
4. Check uscis's cookie issuance (Keycloak integration side): what SameSite value does it set?

## Pass condition

Spike **passes** (= hole confirmed) if a WS upgrade from a disallowed origin with a valid session cookie can invoke auth-gated methods.

Spike **fails** (= safe) if one of:
- Origin validation rejects the upgrade before auth runs
- Documentation + tooling enforces SameSite=Strict
- A CSRF token mechanism is wired in

## Fail → next

Confirmed hole → mitigation: `ValidOrigin` becomes mandatory at the transport layer (not just an opt-in PlexusRequest field). `.require_origin_allowlist(&[...])` builder method that rejects WS upgrades at the CombinedAuthMiddleware layer before dispatch. Defense in depth with SameSite.

## Out of scope

- Browser cookie-fixation attacks
- XSS as a CSRF delivery vector (separate class)
