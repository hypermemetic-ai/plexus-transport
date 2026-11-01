# Changelog

All notable changes to plexus-transport are documented here. This file starts
at 0.6.0; earlier changes are in the git history.

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
