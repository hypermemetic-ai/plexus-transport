# Changelog

All notable changes to plexus-transport are documented here. This file starts
at 0.6.0; earlier changes are in the git history.

## [0.10.1] — unreleased

KEEL-29 (security): **a WebSocket server built with the defaults is closed
to web pages.** Before, `WebSocketConfig::new` (and the builder's
`.with_websocket`) checked neither `Origin` nor `Host`, so any page a person
had open could open `ws://127.0.0.1:<port>/` and call every method of a
loopback service (cross-site WebSocket hijacking), and a page that rebound
its own name to 127.0.0.1 could do the same (DNS rebinding). The defaults
change; the API only grows (one exception, below).

### Breaking (behaviour) — for a browser client of an unconfigured server

| Change | Before (0.10.0) | After | What to do |
|---|---|---|---|
| No Origin list | any `Origin` admitted | an upgrade carrying an `Origin` is refused 403, unless it is the listener's own origin (the scheme the request came by — this listener's TLS or not, or a trusted proxy's `X-Forwarded-Proto`; none said: not its own — and the request's `Host`, that `Host` on the listener's own hosts) | name a UI on another port or site: `.with_allowed_origins(["http://127.0.0.1:5173"])`; or open on purpose: `.allow_any_origin()` |
| No Host list, bound to loopback | any `Host` answered | only `127.0.0.1:<port>`, `localhost:<port>` (and `[::1]:<port>` on IPv6) for the bound port; any other refused 421 | name other names: `.with_allowed_hosts([...])`; or `.allow_any_host()` |
| No Host list, bound beyond loopback (`0.0.0.0`) | any `Host` answered | the socket the same, and a warning logged at start; the pages on its port (`HostRoutes`) only the loopback names and the bound address (421 otherwise) | name the hosts: `.with_allowed_hosts([...])` |
| An entry `*` in an Origin or Host list (WebSocket, builder, REST bridge) | matched nothing | the listener refuses to start (`build()` / `serve_*` errs, naming `allow_any_origin()` / `allow_any_host()`) | drop it, or call the opt-out in code |

A client that sends no `Origin` — synapse, a service-to-service link, a CLI,
a test — connects exactly as before. With a list set, the check is as
before. New: `WebSocketConfig::allow_any_origin`, `allow_any_host` and
`with_allowed_origins`; `TransportServerBuilder::allow_any_origin` and
`allow_any_host`; `host::Opened`, the state those calls set — never a list
value, so no flag, environment variable or file can open a listener — kept in
a new public field `WebSocketConfig::opened`. That field is the one API
exception: a `WebSocketConfig { .. }` literal naming every field must add it
(`..WebSocketConfig::new(port)` is unaffected). `/mcp` and `serve_combined`
are unchanged (rmcp's host list has no wildcard).

## [0.10.0] — unreleased

NEST-24: on plexus-core 0.11 (NEST-15, NEST-16, DYNAUTH-5..8, NEST-21) and,
under `mcp`, plexus-transport-mcp 0.5 (plexus-client 0.11) — a new minor
because plexus-core's types are in this crate's API.

### Breaking

| Change | Before (0.9.0) | After | What to do |
|---|---|---|---|
| plexus-core | 0.10 | 0.11 | Move a service's plexus-core (or plexus-rpc) to 0.11 with it |
| plexus-transport-mcp (`mcp`) | 0.4 | 0.5 | Nothing beyond the plexus-client move |

DYNAUTH-8 (TM-2): **no credential in a log.** The stdio transport's frame
logs (`Received request`, `Sending response`, `Forwarding notification`, at
debug) go through plexus-core's redactor (`plexus::redact`): the values of
`credential`, `authorization`, `token`, `refresh_token`, `client_secret`,
`code` and their kin become `"…"`. jsonrpsee-core's own whole-request traces
(`[Methods::raw_json_request] Request: …`, at trace) are dropped around the
calls the stdio transport makes; every other record goes on to the
subscriber. The URIs, headers and arguments the REST, MCP and WebSocket
paths log are redacted the same way (an `Authorization` header, a
`?token=` or `?code=`). Needs the plexus-core after 0.10.1.

## [0.9.0] — unreleased

ASK-5 / ASK-1: on plexus-core 0.10 (ASK-2: bidir out, and nothing in its
place — Plexus has no in-call or protocol-wide confirmation) and, under
`mcp`, plexus-transport-mcp 0.4 (plexus-client 0.10) — a new minor because
plexus-core's types (`PlexusStreamItem`) are in this crate's API.

### Breaking

| Change | Before (0.8.0) | After | What to do |
|---|---|---|---|
| plexus-core | 0.9 | 0.10 | Move a service's plexus-core (or plexus-rpc) to 0.10 with it |
| plexus-transport-mcp (feature `mcp`) | 0.3 | 0.4 | Nothing beyond plexus-client 0.10 |
| `PlexusStreamItem::Request` (bidir's in-call question) | the REST bridge logged it, SSE sent `event: warning`, the MCP activation bridge sent it as a logging notification | gone: plexus-core 0.10 has no such item and no `{ns}.respond` | A method that needs its caller's yes builds it from its own parameters (a `dry_run`, a plan id) |

Spec/10's input half (`DuplexGate`, the duplex fixture) and spec/14's
symmetric connection are unchanged.

## [0.8.0] — 2026-11-06

PLEXUS-46: on plexus-core 0.9 (RETURNS-47's call context, PLEXUS-45's wasm32
core) and, under `mcp`, plexus-transport-mcp 0.3 (plexus-client 0.9) —
a new minor because plexus-core's types are in this crate's API. Carries
TRANSPORT-26 and RETURNS-47 below.

### Changed

- plexus-core 0.8 → **0.9**; plexus-transport-mcp 0.2 → **0.3** (feature
  `mcp`). A service moves its plexus-core (or plexus-rpc) to 0.9 with it.
- `scripts/check-rmcp.sh` (TRANSPORT-23's guard) holds `mcp` to one rmcp 3.x
  at or above the 3.8 the manifests ask for, not to 3.8.x: rmcp 3.9.0 is out
  and a fresh resolve takes it.

TRANSPORT-26: the transport serves https itself.

### Added

- Feature `tls` (default off): `TlsConfig { cert, key }` (PEM), and
  `TransportServerBuilder::with_tls` — every listener the builder opens (the
  WebSocket, the REST bridge, MCP HTTP, the mounted MCP bridge) serves TLS
  with rustls on the ring provider; `WebSocketConfig`, `RestHttpConfig` and
  `McpHttpConfig` take one each (`with_tls`, a `tls` field under the
  feature). The pair is read again on the next handshake after either file
  changes; one that fails to load keeps the last good pair. A plain request
  to a TLS port is refused at the handshake and logged once per peer.
  `Connection::is_tls`; `ForwardedClient::secure` and `SecureTransport` are
  true on such a connection without any forwarded header (not through a
  trusted proxy, whose word stands); `tls::is_tls_peer`.
- `HostRoutes` and `TransportServerBuilder::with_routes` /
  `with_host_routes`: a host's own axum routes on the WebSocket's port, the
  socket at `/ws` (or the path named) — one origin for a page and its
  socket, under TLS or not. `UpgradeOptions::routes`.
- `serve_combined_with` and `CombinedOptions` (allowed hosts, TLS, the
  host's routes) for the combined port.

RETURNS-47, on plexus-core's call context (`plexus_core::call`): the bridges
answer as the WebSocket does.

### Changed

- **The leaf path runs in a call context.** With one activation and no
  route, the REST bridge and the legacy `ActivationMcpBridge` call
  `plexus_core::call::ungated` instead of `Activation::call`: the bridge is
  the whole dispatch — no service, so no gate — and runs the call as a
  service with no gate runs one, its caller admitted everywhere. (Called
  alone, `Activation::call` runs in no context, where a method reading
  `reach: Reach` or `call::caller()` is refused.) `ungated` names exactly
  that: a call that passes through a gate goes through the service's route
  (`with_router_as`, `with_route_fn`, the mounted MCP bridge), never through
  it.
- The legacy `ActivationMcpBridge` names no caller: every call through it is
  anonymous, as before. The bridge that names one is the mounted bridge
  (`with_mcp`, TRANSPORT-16), through the service's route.

### Added

- `ActivationRestBridge::with_session_validator`: the REST bridge names each
  request's caller as the WebSocket's upgrade does — its `Cookie`, then its
  `Authorization: Bearer` token, to the session validator. A `Bearer` it
  refuses is a 401 (`unauthenticated`); none is the anonymous caller.
- **The api key and the validator together** (`ActivationRestBridge::with_api_key`,
  set by `serve_rest_http` / `serve_rest_http_as` from their `api_key`): a
  `Bearer` that matched the api key is the admission key, not a session
  token, and is never handed to the validator. With no cookie the caller is
  anonymous — as every REST call was before RETURNS-47, so an api-key
  deployment answers as it did after `TransportServer` hands REST the
  socket's validator — and with a valid cookie the cookie's caller. A
  session token sent as the `Bearer` with no api key is refused by the key
  (401), as any request without it is.
- **CSRF, pinned:** the REST router reads only `application/json` (a form,
  `text/plain` or multipart POST is 415 before the method runs, cookie or
  not) and answers a CORS preflight with no CORS headers, so a page on
  another origin cannot call as a cookie's caller. A CORS layer added there
  must come with a CSRF check; the router says so.
- `RouteAsFn` and `ActivationRestBridge::with_router_as`: a route with the
  caller (`hub.route_with_ctx(&method, params, auth.as_ref(), None)`), so a
  REST call over a service is admitted by its gate and runs in its context:
  an admitted caller answered within its reach, an anonymous one 401, one
  without the grant 403. `with_router` (`RouteFn`) names no caller: its calls
  are anonymous.
- `TransportServerBuilder::with_route_as_fn`, and
  `http::server::serve_rest_http_as`: the builder's REST bridge takes the
  route with the caller and the builder's session validator.

## [0.7.0]

### Breaking

- On plexus-core 0.8 (RETURNS-11): REST answers a call failure with its
  `RemoteError` and a status by kind (404 · 400 · 401 · 403 · 501 · 500, and
  429 · 503 · 504 for `too_many_calls`, `child_unavailable`,
  `child_timeout`); SSE and MCP carry typed progress and error frames.
- TRANSPORT-23: rmcp moves from 0.12 to 3.8, the version plexus-transport-mcp
  is on, so an `mcp` build compiles one rmcp stack. rmcp's types are in the
  public API under `mcp` (`ActivationMcpBridge` is an rmcp 3.8
  `ServerHandler`). Without `mcp` there is no rmcp at all: it left
  `[dev-dependencies]`, and the tests' MCP client is the `__mcp-test-client`
  feature (test support, not API).
- TRANSPORT-23: `with_mcp_http` and `serve_combined` check `Host` (rmcp 3.8):
  the loopback names, plus the literal listen address when it is neither
  loopback nor unspecified. Any other `Host` is refused 403. A service behind a
  name, or bound to `0.0.0.0`, names its hosts with
  `McpHttpConfig::with_allowed_hosts` or `serve_combined_with_allowed_hosts`.
- TRANSPORT-23: `McpHttpConfig` gains a public field, `allowed_hosts`.

### Added

- TRANSPORT-23: `McpHttpConfig::with_allowed_hosts`,
  `serve_combined_with_allowed_hosts`. `scripts/check-rmcp.sh`, run by
  `cargo test`, fails if rmcp reaches a non-`mcp` build or two versions appear.
- BIDIR-6: the WebSocket loop takes `{ns}.input` and `{ns}.end` on its reader,
  through a connection's `InputTable`, carried on the `FrameSink` every call's extensions hold. With no input
  half open that is one load; no frame of any other kind changes.
- BIDIR-17: a binary message tagged `0x01` is an input frame, routed to the
  `InputTable` with no parse; a malformed one, or a binary message neither JSON
  nor an input frame, is one `-32700` `malformed_frame` error (`id` null).
- PERF-8: a client that offers `batch` in its upgrade's URL (`?batch`, or
  `?batch=N` for at most N) is sent the items a stream has ready together in
  one notification, `results`, the metadata said once (plexus-wire spec/02).
  A client that offers nothing is sent today's frames. `Negotiated` says what
  was taken. Needs plexus-core's `FrameSink::batching`.

### Fixed

- TRANSPORT-17: `serve_stdio` has one writer. Every line, an answer or a stream's
  item, goes through one bounded queue to one task, which writes each message
  with its newline, so two live streams can no longer interleave inside a line
  (spec/11). Lines ready together go out in one write, at most 64 KiB gathered.
  `StdioConfig::subscription_buffer_size` also sizes that queue. At the end of
  stdin every call ends, as spec/11 says, and what was queued has 5 s to go out.
- TRANSPORT-22: a WebSocket frame whose 64-bit payload length has its top bit set is refused
  at its header and the socket closes with `1002`, on any frame of a message
  (RFC 6455 section 5.2). It was treated as a message over the cap: answered
  `-32007` with the socket kept, and every later byte discarded as its payload.

## [0.6.0]

Release with plexus-core 0.7.0.

### Breaking

| Change | Before | After | What to do |
|---|---|---|---|
| WebSocket server handle | jsonrpsee's `ServerHandle` | `plexus_transport::ServerHandle` (`stop`, `is_stopped`) | Import it from `plexus_transport`, and drop any direct jsonrpsee server use |
| `Data.content`, `Data.metadata` | `Value`, `StreamMetadata` | `Content`, `Arc<StreamMetadata>` | See plexus-core's changelog |
| A bound question answered from another connection | answered | `BidirError::NotThisCall` | Answer from the connection that was asked |
| A plain HTTP request on the WebSocket path | transport-defined | `426 Upgrade Required` | Probes and health checks must use another route or upgrade |
| An unknown key under `typed_params` | ignored | `-32602` invalid params | Send only declared keys |
| `with_mcp` with `with_api_key` and an `McpConfig` that admits anonymous callers | not available in 0.5.1 | `build()` fails, even with `allow_missing_auth_middleware` | Set `McpConfig.anonymous = false` or `oauth`, with `with_session_validator` to admit Bearer callers |

The mount does not read the static api_key. With `anonymous = false` and no
session validator, it refuses every request.

### Added

- `TransportServerBuilder::with_mcp`, the MCP bridge mounted in-process.
- `TransportServerBuilder::bind`, one bind address for every listener.
- `ConnectionCalls`, a count of the calls open on a connection.
- A `Host` allow-list on the WebSocket upgrade and the REST bridge.

### Fixed

- The message cap could be bypassed. The guard added a client-supplied 64-bit
  frame length without checking, so a fragment of 5 bytes followed by a
  continuation of 2^64-5 wrapped to 0 in release builds and panicked in debug
  builds. A total that overflows now counts as over the cap. Frame sizes
  computed from a header saturate, so a huge frame is never taken for a short
  one.

### Not enforced

- `ConnectionQuotas::max_calls_per_connection` sets the size of
  `Connection::calls`, but the socket loop does not count calls against it.
  A service must call `ConnectionCalls::try_open` itself.
- `answer_by` is not checked by this crate. See plexus-core's changelog.
