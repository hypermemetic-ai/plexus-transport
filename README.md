# plexus-transport

The server of a [Plexus RPC](https://github.com/hypermemetic-ai/plexus-rpc)
service: WebSocket, stdio, REST over HTTP, and TLS.

## Most people want plexus-rpc

[plexus-rpc](https://github.com/hypermemetic-ai/plexus-rpc) is the one
dependency a service takes. It brings this crate as `plexus_rpc::transport`.

## Install

<!-- Cargo.toml -->
```toml
[dependencies]
plexus-transport = "0.10"
plexus-core = "0.11"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

## A service on a WebSocket

<!-- examples/serve.rs -->
```rust
use std::sync::Arc;

use plexus_core::activations::health::Health;
use plexus_core::plexus::DynamicHub;
use plexus_transport::TransportServer;

pub async fn serve(port: u16) -> anyhow::Result<()> {
    let service = Arc::new(DynamicHub::new("myservice").register(Health::new()));
    TransportServer::builder(service, |service| Ok(DynamicHub::arc_into_rpc_module(service)?))
        .with_websocket(port)
        .build()
        .await?
        .serve()
        .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    serve(4444).await
}
```

`cargo test --test readme` serves this file on a free port, calls
`health.check` over the socket, and holds this page to the file.

## More

- [docs.rs](https://docs.rs/plexus-transport): the API.
- [The listeners](docs/listeners.md): WebSocket, stdio, REST, one port for all, the features, the registry.
- [Origins and hosts](docs/origins-and-hosts.md): which web pages and which `Host` names a listener answers.
- [Limits](docs/limits.md): the socket's limits, the connection quotas, the connection loop.
- [Behind a proxy](docs/proxy.md): whose address a connection counts against.
- [TLS](docs/tls.md): `https://` and `wss://` from the server itself, and a page on the socket's port.
- [MCP](docs/mcp.md): the service's methods as an agent's tools.
- [Changelog](CHANGELOG.md)

MIT.
