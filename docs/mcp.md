# MCP

With the feature `mcp`, `with_mcp(McpConfig)` mounts the bridge of
[plexus-transport-mcp](https://github.com/hypermemetic-ai/plexus-transport-mcp)
over a `DynamicHub`, in the same process. The service's methods are an
agent's tools.

```toml
plexus-transport = { version = "0.10", features = ["mcp"] }
```

```rust
use plexus_transport::McpConfig;

TransportServer::builder(service, converter)
    .with_websocket(8080)
    .with_mcp(McpConfig::listen("127.0.0.1:8081".parse()?))
```

- The bridge has its own listener, at `McpConfig.listen`, and serves at
  `McpConfig.path`, `/mcp` by default. `listen` is required. `bind(addr)`
  applies to it.
- Each request's caller is named from its `Authorization: Bearer` by the
  validator of `with_session_validator`. A `tools/call` runs as that caller,
  and `tools/list` is that caller's view.
- A request with no credential is the anonymous caller, unless
  `McpConfig.anonymous` is `false` or `McpConfig.oauth` is set.
- `with_api_key` does not gate the bridge. With a key set, `build()` refuses
  a config that admits anonymous callers.
- With `with_tls`, the bridge serves `https://`.
- With no `allowed_hosts`, the bridge answers to the loopback names, and to
  its listen address when that is not loopback.

## `with_mcp_http`

`with_mcp_http(port)` serves MCP over HTTP on a listener of its own, on
`127.0.0.1` at that port, from the transport's own rmcp server. It needs the
feature `mcp` as well.

```rust
TransportServer::builder(service, converter)
    .with_websocket(8080)
    .with_mcp_http(8081)
```

`with_mcp_http_config(McpHttpConfig)` takes the listener's configuration:

| on `McpHttpConfig` | what it sets |
| -- | -- |
| `McpHttpConfig::new(port)` | the listener, on `127.0.0.1` |
| `with_allowed_hosts(hosts)` | the `Host` names it answers; with none, the loopback names, and any other `Host` is answered `403` |
| `with_server_name(name)`, `with_server_version(version)` | what the server calls itself; else the service's namespace and version |
| `with_tls(tls)` | `https://`, with the feature `tls` |
| `with_sqlite(path)` | MCP sessions kept in SQLite, with the feature `sqlite-sessions`; else in memory |

A key set with `with_api_key` is required on every request to it.
