# Changelog

All notable changes to plexus-transport are documented here. This file starts
at 0.6.0; earlier changes are in the git history.

## [0.7.0] — unreleased (RETURNS-36)

### Breaking

- On plexus-core 0.8 (RETURNS-11): REST answers a call failure with its
  `RemoteError` and a status by kind (404 · 400 · 401 · 403 · 501 · 500, and
  429 · 503 · 504 for `too_many_calls`, `child_unavailable`,
  `child_timeout`); SSE and MCP carry typed progress and error frames.

## [Unreleased]

### Added

- BIDIR-6: the WebSocket loop takes `{ns}.input` and `{ns}.end` on its reader,
  through a connection's `InputTable` in every call's extensions. With no input
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
