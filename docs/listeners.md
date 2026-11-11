# The listeners

`TransportServer::builder(activation, rpc_converter)` takes an
`Arc<A: Activation>` and a function from that `Arc` to the `RpcModule` the
WebSocket and stdio serve. For a `DynamicHub` the function is
`DynamicHub::arc_into_rpc_module`. Each call below adds one listener.
`build()` checks the configuration, and `serve()` opens the listeners and
runs until one stops.

| call | feature | what it opens |
| -- | -- | -- |
| `with_websocket(port)` | | JSON-RPC over a WebSocket |
| `with_stdio()` | | JSON-RPC over stdin and stdout, one message a line |
| `with_rest_http(port)` | `http-gateway` | `POST /rest/{namespace}/{method}` |
| `with_mcp(McpConfig)` | `mcp` | the MCP bridge: see [MCP](mcp.md) |

Every listener binds `127.0.0.1`. `bind(addr)` gives all of them another
address, and they keep their ports.

## Features

| feature | what it adds |
| -- | -- |
| `tls` | `TlsConfig` and `with_tls`: see [TLS](tls.md) |
| `http-gateway` | the REST bridge: `with_rest_http`, `RestHttpConfig`, `serve_rest_http` |
| `mcp` | the MCP bridges, and with them `rmcp` and `plexus-transport-mcp` |
| `mcp-gateway` | `serve_combined`: the WebSocket and `/mcp` on one port, and `/rest` with `http-gateway` |
| `sqlite-sessions` | MCP sessions kept in SQLite |

No feature is on by default.

## WebSocket

One socket carries many calls. A request on the port that is not an upgrade
is answered `426 Upgrade Required`. An upgrade passes these checks in order:

| check | refused with |
| -- | -- |
| the `Host` is one the listener answers to | 421 |
| the `Origin`, when there is one, is admitted | 403 |
| the API key, when `with_api_key` set one | 401 |
| the session, under `reject_upgrade_on_auth_failure` or `anonymous_view` | 401 |
| the connection quotas | 429 |

[Origins and hosts](origins-and-hosts.md) has the first two, and
[Limits](limits.md) the last.

The API key is read from the `X-Plexus-API-Key` header;
`with_api_key_header` names another. `with_session_validator` names each
caller from the `Cookie` header, or from `Authorization: Bearer`.
`with_protected_resource` serves RFC 9728 metadata at
`/.well-known/oauth-protected-resource`, after the `Host` check and before
the others.

`build()` refuses a service with methods that take their caller from the
session when neither `with_api_key` nor `with_session_validator` was called.
`allow_missing_auth_middleware()` turns that check off.

## Stdio

`with_stdio()` reads JSON-RPC requests from stdin, one a line, and writes
answers and stream items to stdout, one a line. It speaks the same JSON-RPC
as the WebSocket. It is not the MCP stdio transport. With stdio configured,
`serve()` serves stdio alone, and returns when stdin closes.

## REST

`with_rest_http(port)` serves each method at
`POST /rest/{namespace}/{method}` with a JSON body. A method that answers
once gets one JSON body; a method that streams gets Server-Sent Events.
`with_flat_schemas` names the schemas the bridge serves, and
`with_route_as_fn` routes each call with its caller. `with_api_key` is read
here from `Authorization: Bearer`.

## One port

`serve_combined` (feature `mcp-gateway`) serves `/mcp`, `/rest` and the
WebSocket on one address. `serve_combined_with` takes `CombinedOptions`:
the hosts `/mcp` answers to, TLS, and a host's own routes.

## The registry

A server with a WebSocket announces itself to the Plexus registry when it
starts: its name, host, bound port and schema hash. The registry is at
`PLEXUS_REGISTRY_URL`, or `ws://127.0.0.1:4444`. It tries three times, logs
one warning if none succeeds, and serves either way. When it stops, SIGTERM
and ctrl-c included, it sends `registry.deregister`.

| call | what it does |
| -- | -- |
| `without_registry()` | announce nothing; `PLEXUS_NO_REGISTRY=1` does the same |
| `with_registry_url(url)` | the registry's address |
| `with_registry_name(name)` | the name announced; the default is the activation's namespace |
| `with_registry_host(host)` | the host announced; the default is `127.0.0.1` |
