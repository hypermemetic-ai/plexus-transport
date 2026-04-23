---
id: RED-15
title: "Transport-layer Origin allowlist: reject WS upgrades before auth runs"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S15 confirmed moderate CSRF exposure on cookie-auth WebSocket:

- `ValidOrigin` is a **per-method** `PlexusRequestField` extractor in `plexus-transport/src/request/origin.rs`. It runs inside the macro-generated call wrapper, AFTER the WS upgrade has completed and after dispatch has started on a method-by-method basis.
- `CombinedAuthMiddleware` at WS upgrade time validates the cookie but NOT the Origin. A request with `Origin: https://evil.com` and a valid session cookie completes the upgrade, establishes a duplex connection, and can invoke ANY method that doesn't opt into `ValidOrigin`.
- No Plexus-side mandate or check on `SameSite` cookie flags. Modern browsers default Lax, which blunts the attack for most SSO setups — but SameSite=None cookies (required for cross-subdomain SSO) reopen the hole fully, and Plexus doesn't surface this to operators.
- No CSRF-token mechanism.

## Goal

Transport-layer Origin allowlist that rejects disallowed WS upgrades before any auth or dispatch work happens. Defense in depth with the existing per-method `ValidOrigin`.

## Acceptance

- [ ] `TransportServer` builder: `.require_origin_allowlist(origins: impl IntoIterator<Item = impl Into<String>>)`. Stores the allowlist on the server struct.
- [ ] WS upgrade path (`plexus-transport/src/websocket.rs`): before invoking `CombinedAuthMiddleware`, check the upgrade request's `Origin` header. If allowlist is configured and the origin is not in it, respond with HTTP 403 and a WARN log naming the rejected origin + the requested path.
- [ ] Missing Origin header on WS upgrade from a browser context: reject. From non-browser (CLI, server-to-server), the caller must either send a matching Origin or the allowlist must be unset (dev mode). Consider an explicit `.allow_missing_origin_for_non_browser()` opt-in if this friction hurts CLI tooling.
- [ ] Startup log: INFO when allowlist is configured naming the allowed origins; WARN when unset (only one warning at startup, not per-request).
- [ ] REST gateway path: same enforcement (after RED-11 unifies the transport auth chain, this should be a single hook both transports share).
- [ ] Integration test: WS upgrade with `Origin: https://app.example.com` succeeds; with `Origin: https://evil.com` returns 403; with no Origin header returns 403 under strict mode, succeeds under `allow_missing_origin_for_non_browser`.
- [ ] Docs: `plexus-transport` gains a `## CSRF posture` section. Mentions SameSite expectations on cookies (`SameSite=Strict` strongly recommended when feasible; `SameSite=Lax` acceptable; `SameSite=None` requires this allowlist to be tight).

## Out of scope

- CSRF-token mechanism (separate epic if needed).
- Per-subdomain wildcard matching — v1 is exact-match; wildcard can come later if operators demand it.
- Cookie-fixation defenses.

## Notes

The per-method `ValidOrigin` extractor remains useful — it lets a given method tighten beyond the transport allowlist (e.g. "this method is only callable from admin.example.com even though the transport allows app.example.com"). Defense in depth, not replacement.

Relationship to RED-S16: that spike noted the `ALLOWED_ORIGINS` env-unset allow-all fallback logs at INFO. RED-16 covers promoting that to WARN and integrating with RED-4. This ticket is about moving the enforcement earlier in the request lifecycle, not about the fallback.
