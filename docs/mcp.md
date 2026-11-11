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
