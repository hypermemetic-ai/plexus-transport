---
id: RED-13
title: "Redact sensitive header values from transport logs"
status: Pending
type: task
blocked_by: []
unlocks: []
---

## Problem

RED-S13 confirmed two credential-leaking log sites:

- `plexus-transport/src/mcp/server.rs:73` — iterates all incoming HTTP headers and emits `tracing::info!("    {}: {:?}", name, value)`. Runs at INFO (captured by default in most aggregators). Leaks `Authorization: Bearer <JWT>` and `Cookie: access_token=<JWT>` strings verbatim.
- `plexus-transport/src/mcp/server.rs:100` — same pattern on an error branch at `error!`.
- `plexus-core/src/plexus/test_validator.rs:46` — `TestSessionValidator` logs raw cookie string at `debug!`. Fine for unit tests; would burn if a prod deploy accidentally used TestSessionValidator.

Classification: critical. Anyone with log-read access gets reusable session material. JWTs are valid until expiry; session cookies until logout.

## Goal

Sensitive header values never appear in logs at info+ levels. Pattern is reusable across transports so future log sites don't re-leak.

## Acceptance

- [ ] A reusable `redact_header_value(name, value) -> String` helper in `plexus-transport` that masks known-sensitive headers. Default safelist includes: `authorization`, `cookie`, `proxy-authorization`, `set-cookie`, `x-api-key`, `x-auth-token`. Case-insensitive match.
- [ ] `mcp/server.rs:73` and `:100` use the helper. Other transports audited — any similar iteration of headers uses the helper too.
- [ ] Redacted form: `"<redacted, N bytes>"` — length preserved for debugging, value not. Do NOT include a hash of the value (hash of JWT is still a correlation key that outlives rotation).
- [ ] `TestSessionValidator`: downgrade cookie-value log to `trace!` AND redact the value even at trace (defense in depth — `trace` is rarely enabled in prod but let's not rely on that).
- [ ] Audit sweep: grep `tracing::(info|debug|warn|error)!` across plexus-transport and plexus-core for occurrences near header iteration or cookie/JWT variable names. Document results as a comment on this ticket or an appendix file.
- [ ] Test: inject `Authorization: Bearer SECRET` into an MCP request, capture logs, assert `SECRET` does not appear anywhere in the captured output.

## Out of scope

- External log aggregation policy (operational).
- Log retention / rotation.
- Generated-client console.log audit (separate audit — RED-S13 out-of-scope).

## Notes

The header-names-are-fine / header-values-are-sensitive split is the right policy. Names tell the operator what was sent; values are the credentials. An allowlist of "always-safe" header names (`content-type`, `accept`, etc.) could go further but adds maintenance; the denylist of sensitive names is sufficient for this pass.
