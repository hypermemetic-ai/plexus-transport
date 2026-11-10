# plexus-transport

Transport implementations for Plexus RPC. Provides WebSocket, stdio, and HTTP/SSE backends for hosting any `Activation` (from `plexus-core`).

## Overview

`plexus-transport` provides transport layer implementations for Plexus RPC servers. It extracts the transport infrastructure from Substrate into a reusable library, allowing you to host **any** activation—single plugins, DynamicHub routers, or nested services—with WebSocket, stdio, and MCP HTTP transports using a clean builder API.

**What is Plexus RPC?** Plexus RPC is a protocol for building services with runtime schema introspection, where code IS schema. Zero drift, instant streaming.

**Key insight**: DynamicHub (formerly Plexus) is just an Activation with dynamic registration. There's nothing transport-special about it. This library works generically with `impl Activation`.

> **Note**: Examples in this README may still reference `Plexus` - this is the deprecated name for `DynamicHub` and still works but will show warnings.

## Features

- **Generic over `Activation` trait** - Works with any type implementing `plexus_core::plexus::Activation`
- **Multiple Plexus RPC transports simultaneously**:
  - WebSocket JSON-RPC server
  - Stdio line-delimited JSON-RPC (MCP-compatible)
  - MCP HTTP with SSE streaming
- **Builder pattern API** - Clean, composable configuration
- **Arc lifecycle preservation** - Callback-based RPC conversion keeps Weak references valid
- **In-memory sessions by default** - SQLite persistence opt-in via feature flag

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
plexus-transport = { path = "../plexus-transport" }

# Optional: SQLite session persistence
plexus-transport = { path = "../plexus-transport", features = ["sqlite-sessions"] }
```

## Usage

### Hosting a DynamicHub

```rust
use plexus_transport::TransportServer;
use substrate::build_dynamic_hub; // or build_plexus (deprecated)
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Build DynamicHub
    let hub = build_dynamic_hub().await;

    // Provide RPC converter (preserves Arc for Weak references)
    let rpc_converter = |arc: Arc<substrate::DynamicHub>| {
        substrate::DynamicHub::arc_into_rpc_module(arc)
            .map_err(|e| anyhow::anyhow!("{}", e))
    };

    // Configure and start transports
    TransportServer::builder(hub, rpc_converter)
        .with_websocket(4444)
        .with_mcp_http(4445)
        .build().await?
        .serve().await?;

    Ok(())
}
```

> **Note**: `Plexus` is now `DynamicHub`. The old name still works but is deprecated.

### Hosting a Single Plugin

```rust
use plexus_transport::TransportServer;
use jsexec::JsExec;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create single plugin activation
    let jsexec = Arc::new(JsExec::new(Default::default()));

    // Provide RPC converter
    let rpc_converter = |arc: Arc<JsExec>| {
        Ok(arc.into_rpc_methods())
    };

    // Host with stdio transport (MCP-compatible)
    TransportServer::builder(jsexec, rpc_converter)
        .with_stdio()
        .build().await?
        .serve().await?;

    Ok(())
}
```

### Stdio Mode (MCP-Compatible)

Perfect for Claude Desktop integration:

```rust
TransportServer::builder(activation, rpc_converter)
    .with_stdio()  // Blocks on stdin, line-delimited JSON-RPC
    .build().await?
    .serve().await?;
```

### Multiple Transports

Run WebSocket and MCP HTTP simultaneously:

```rust
TransportServer::builder(activation, rpc_converter)
    .with_websocket(8888)      // JSON-RPC WebSocket
    .with_mcp_http(8889)        // MCP HTTP with SSE
    .build().await?
    .serve().await?;
```

### Custom Server Name (Optional)

By default, MCP server reports the activation's namespace and version:

```rust
// JsExec reports as { name: "jsexec", version: "0.1.0" }
// Plexus reports as { name: "plexus", version: "0.2.6" }
```

Override with custom name/version:

```rust
let mcp_config = McpHttpConfig::new(8889)
    .with_server_name("my-custom-server".to_string())
    .with_server_version("1.0.0".to_string());

TransportServer::builder(activation, rpc_converter)
    .with_mcp_http_config(mcp_config)
    .build().await?
    .serve().await?;
```

### Allowed Hosts

`with_mcp_http` answers the loopback `Host` names, plus the listen address when
it is neither loopback nor unspecified. Any other `Host` is refused 403, which
stops DNS rebinding. A service behind a name, or bound to `0.0.0.0` for remote
clients, names its hosts:

```rust
let mcp_config = McpHttpConfig::new(8889)
    .with_allowed_hosts(["mcp.example.com"]);
```

`serve_combined_with_allowed_hosts` does the same for `serve_combined`.

### Web pages and the WebSocket (KEEL-29)

A browser sends `Origin` on every WebSocket, and sockets have no same-origin
rule, so the WebSocket listener (`WebSocketConfig::new`, `.with_websocket`) is
closed to web pages unless told otherwise:

- **Origin.** With no list, an upgrade carrying an `Origin` is refused 403,
  unless it is the listener's own origin (a page served on that very port and
  `Host`). A client that sends no `Origin` — synapse, a service, a CLI —
  connects as ever. A UI on another port or site is named:
  `.with_allowed_origins(["http://127.0.0.1:5173"])`.
- **Host.** With no list and bound to loopback, the listener answers only
  `127.0.0.1:<port>` and `localhost:<port>` (and `[::1]:<port>` on IPv6);
  any other `Host` is refused 421 (DNS rebinding). Bound beyond loopback
  (`0.0.0.0`), `Host` is not checked — the names it is reached by are not
  the transport's to know — and a warning is logged at start: name them with
  `.with_allowed_hosts([...])`.
- **Opting out**, on purpose, one line each: `.allow_any_origin()` (any site
  may call the service in its visitor's browser) and `.allow_any_host()`.

### SQLite Session Persistence (Optional)

```rust
use plexus_transport::{TransportServer, McpHttpConfig, SessionStorage};
use std::path::PathBuf;

let mcp_config = McpHttpConfig::new(8889)
    .with_sqlite(PathBuf::from("sessions.db"));

TransportServer::builder(activation, rpc_converter)
    .with_mcp_http_config(mcp_config)
    .build().await?
    .serve().await?;
```

## Architecture

### Core Components

**`TransportServer<A: Activation>`**
Main server that orchestrates multiple transports. Generic over any Activation type.

**`TransportServerBuilder<A>`**
Builder for configuring which transports to enable. Supports method chaining.

**`ActivationMcpBridge<A>`**
Generic MCP protocol handler that bridges `tools/call` requests to `Activation::call()` methods. Works with any activation type.

### Transport Modules

**`stdio`** - Line-delimited JSON-RPC over stdin/stdout (MCP-compatible)
**`websocket`** - the WebSocket server: the accept loop and the upgrade's checks
**`ws`** - the WebSocket connection loop, the transport's own (PERF-20)
**`mcp`** - MCP HTTP with SSE streaming and session management. Only with the `mcp` feature, which is the only thing that brings in `rmcp`

### RPC Conversion Pattern

The callback-based converter handles the `into_rpc_methods()` consumption issue:

```rust
// User provides: Arc<A> -> Result<RpcModule<()>>
let rpc_converter = |arc: Arc<MyActivation>| {
    // Can call activation-specific conversion logic
    MyActivation::arc_into_rpc_module(arc)
};
```

This preserves the Arc lifecycle, keeping Weak references valid throughout the server's lifetime, which is essential for Plexus RPC's cross-activation communication.

## Configuration Types

### `TransportConfig`
Container for all transport configurations.

### `WebSocketConfig`
```rust
pub struct WebSocketConfig {
    pub addr: SocketAddr,
}
```

### `StdioConfig`
```rust
pub struct StdioConfig {
    pub subscription_buffer_size: usize,  // Default: 1024
}
```

### `McpHttpConfig`
```rust
pub struct McpHttpConfig {
    pub addr: SocketAddr,
    pub session_storage: SessionStorage,
    pub server_name: Option<String>,  // Optional server name override
    pub server_version: Option<String>,
}
```

### `SessionStorage`
```rust
pub enum SessionStorage {
    InMemory,  // Default: simple, no persistence
    Sqlite { path: PathBuf },  // Optional: survives restarts
}
```

## API Reference

### `TransportServer::builder(activation, rpc_converter)`
Create a new transport server builder.

**Parameters:**
- `activation: Arc<A>` - The activation to host
- `rpc_converter: FnOnce(Arc<A>) -> Result<RpcModule<()>>` - Converter function

**Returns:** `TransportServerBuilder<A>`

### `TransportServerBuilder` Methods

#### `.with_websocket(port: u16) -> Self`
Enable WebSocket JSON-RPC transport.

#### `.with_stdio() -> Self`
Enable stdio transport (line-delimited JSON-RPC, MCP-compatible).

#### `.with_mcp_http(port: u16) -> Self`
Enable MCP HTTP transport with default configuration.

#### `.with_mcp_http_config(config: McpHttpConfig) -> Self`
Enable MCP HTTP transport with custom configuration.

#### `.bind(addr: impl Into<IpAddr>) -> Self`
TRANSPORT-7: the IP every listener binds (default `127.0.0.1`), e.g.
`0.0.0.0` in a container behind a proxy. Ports stay as given.

#### `.trusted_proxies(nets: impl IntoIterator<Item = IpNet>) -> Self` / `.with_trusted_proxies(TrustedProxies) -> Self`
TRANSPORT-7: the proxies in front. See "Behind a proxy" below.

#### `.build() -> Result<TransportServer<A>>`
Build the configured transport server.

### `TransportServer` Methods

#### `.serve() -> Result<()>`
Start all configured transports and block until shutdown.

- If stdio is configured: blocks on stdin
- Otherwise: starts WebSocket/MCP servers and waits for completion

## The WebSocket connection loop

PERF-20. A Plexus WebSocket is the transport's own (`src/ws/`): the upgrade
is answered after the checks (Host 421, the RFC 9728 metadata, Origin 403,
the key 401, the session 401, the quotas 429); the framing under it is
tokio-tungstenite's; jsonrpsee-core's method table (`Methods`, which every
service registers into) is driven from it, and jsonrpsee-server is not on the
path.

- **One task per socket** reads and writes it; a call is a task of its own
  (jsonrpsee-core's subscription task), so calls on one socket overlap, and a
  call's frames and the wakes they cause land on the socket's one task.
- **Its reader** takes every frame in arrival order — no task per frame — and
  routes it by its shape: a call to the method table, an answer to no
  request of this side's dropped (never `-32600`).
- **Its writer** takes every frame ready when the task wakes, up to its
  budget (64 KiB), and flushes once — a call's frames, or a stream's ready
  items, share one `send` — then yields to other sockets. The queue is
  bounded (1,024 frames): a peer that does not read holds its socket's
  calls back, never the server's memory.
- **A hook** (`UpgradeOptions::hooks`, `Hooks::new`) sees each inbound frame
  before dispatch and may take it; `Connection::writer()` writes this side's
  own requests on an accepted socket (BIDIR-12). `Connection::encoding()` is
  fixed at the upgrade (JSON; PERF-9 adds msgpack).
- **The limits** (`UpgradeOptions::limits`, `SocketLimits`;
  `TransportServerBuilder::with_socket_limits`): a message over 10 MiB is
  refused `-32007` with the socket kept, fragments assembled under the same
  cap; a ping every 30 s and a silent peer closed at 70 s; an idle limit
  (none by default; a service sets it) closes a socket with no call open,
  one that never called included (RPC-5); a request's headers within 10 s,
  and sockets not yet upgraded bounded per IP (RPC-4).
- **A request that is not an upgrade** is `426 Upgrade Required`: there is no
  JSON-RPC over plain HTTP on the WebSocket port.
- **Logs** name no header and no frame, at any level (ACCESS-62): the
  framing's `log` records are capped at debug once a socket is served.

## Behind a proxy

TRANSPORT-7. Behind an edge every visitor's TCP peer is the edge, so the
connection quotas (AUTH-12) would count every visitor as one. Name the edge:

```rust
TransportServer::builder(hub, converter)
    .with_websocket(4455)
    .bind([0, 0, 0, 0])
    .with_trusted_proxies(TrustedProxies::parse(["172.30.0.2/32"])?) // or ["none"]
```

- `X-Forwarded-For` / `X-Forwarded-Proto` are believed **only** from a
  trusted proxy; from any other peer they are ignored (and logged once).
- The client is the rightmost `X-Forwarded-For` hop that is not itself a
  trusted proxy — a client cannot prepend its way out of its quota.
- The connection quotas, per IP and anonymous per IP, count that client.
  Every request carries its `ForwardedClient { ip, peer, via_proxy, secure }`
  in its extensions (so every call's `ext`).
- Secure is the trusted proxy's `X-Forwarded-Proto: https|wss`. With no proxy
  in front, a loopback peer is secure; behind a proxy, loopback proves
  nothing.
- `ClientIp` and `SecureTransport` read the same set (`init_trusted_proxies`,
  which `serve()` calls); `init_trust_proxy_headers` (any peer) is
  deprecated.

## TLS: https and wss with no proxy

TRANSPORT-26, feature `tls` (default off; it compiles nothing new — the
rustls 0.23 / tokio-rustls 0.26 / ring that jsonrpsee's client already
brings). The transport terminates TLS itself, rustls on the ring provider:

```rust
use plexus_transport::TlsConfig;

TransportServer::builder(hub, converter)
    .with_websocket(4455)
    .bind([0, 0, 0, 0])
    .with_tls(TlsConfig::new("/etc/plexus/host.crt", "/etc/plexus/host.key")) // PEM chain + key
    .with_routes(shell_router)                       // the host's pages; the socket at /ws
    .with_allowed_origins(["https://host.example:4455"])
    .with_allowed_hosts(["host.example:4455"])
```

- **Every listener the builder opens** serves TLS with the pair: the
  WebSocket (`wss://`), the REST bridge and MCP HTTP (`https://…/rest`,
  `https://…/mcp`), and the mounted MCP bridge (`with_mcp`). A listener's
  own config may name its own pair (`WebSocketConfig::with_tls`,
  `RestHttpConfig::with_tls`, `McpHttpConfig::with_tls`); the combined port
  takes one in `CombinedOptions` (`serve_combined_with`).
- **The pair**: a PEM certificate chain (leaf first) and its key — RSA,
  ECDSA or Ed25519, PKCS#8/PKCS#1/SEC1. One that does not load refuses the
  start, naming the file. TLS 1.3 and 1.2; ALPN `http/1.1`.
- **Renewal** (`tailscale cert`, certbot): each handshake compares both
  files' size, mtime and inode; a change is read and the next connection
  presents it, no restart. A pair that fails to load (half written, a key
  that is not the certificate's) keeps the one before, says why once, and is
  tried again when a file changes. Open connections go on.
- **Refusals**: a plain `http://` request or a bad handshake is refused at
  the handshake, logged once per peer address; the listener goes on. The
  handshake runs in the connection's own task within the header timeout.
- **Secure**: a connection over this TLS is secure with no forwarded header
  — `Connection::is_tls()`, `ForwardedClient::secure`, and `SecureTransport`
  for its peer — unless its peer is a trusted proxy, whose word stands.
- **One origin** (`with_routes(router)` / `with_host_routes(HostRoutes::new(r).socket_at("/rpc"))`):
  the host's axum routes on the WebSocket's port, TLS or not. The socket
  (and RFC 9728's metadata) at its path, `/ws` by default, through every
  check it has; every other path to the host's router after the port's
  `Host` check, with the request's `ForwardedClient` and `Connection` in its
  extensions. Without routes the port is the socket's on every path, as
  before.
- **Origins**: a page these routes serve is the listener's own origin, and
  connects when its `Host` is on the listener's host list (the loopback
  names by default on loopback; KEEL-29). Behind a name, an `https://`
  page's origin is admitted only when named —
  `with_allowed_origins`, or plexus-login's `public_url` / `allowed_origins`
  (its `shell_origins` adds the https `public_url`'s origin, and its
  `resource_url` already names that origin's `/ws`).
- No `TlsConfig`: every listener is what it was — the same accept loop on
  the same `TcpStream`.

## Examples

See `examples/` directory:
- `jsexec_server.rs` - Hosting single JsExec plugin
- `full_plexus.rs` - Hosting complete Plexus hub

## Design Goals

1. **Activation-agnostic** - No special handling for DynamicHub vs single plugins
2. **Arc lifecycle safety** - Preserve references for cross-activation calls
3. **Clean API** - Builder pattern, composable configuration
4. **MCP-first** - Full MCP protocol support with streaming
5. **Production-ready** - Optional SQLite persistence, proper error handling
6. **Protocol-first** - Implements Plexus RPC protocol cleanly and generically

## Comparison with Substrate

**Before (substrate/src/main.rs):**
```rust
// ~200 lines of transport setup code
// Hardcoded to DynamicHub (formerly Plexus)
// Duplicated across projects
```

**After (with plexus-transport):**
```rust
// ~20 lines using builder API
// Works with any Activation
// Reusable across projects
```

## Naming Note

**Plexus RPC** is the protocol name. It's a protocol for building services with runtime schema introspection and instant streaming.

**DynamicHub** (formerly called "Plexus") is a specific hub/router implementation in `plexus-core` v0.3.0+ that dynamically registers activations. The old name `Plexus` remains as a deprecated alias.

This library (`plexus-transport`) is correctly generic over the `Activation` trait and works with any Plexus RPC activation type - whether it's a single plugin, a DynamicHub router, or a nested hub like Solar.

## License

MIT

## Contributing

This library is part of the Plexus RPC ecosystem. See the main Substrate repository for contribution guidelines.

## Learn More

- **Plexus RPC Protocol**: See the protocol specification and architecture docs in the main Substrate repository
- **Reference Server**: [Substrate](https://github.com/hypermemetic-ai/plexus-substrate) - A Plexus RPC server with conversation trees and LLM orchestration
- **Core Library**: [plexus-core](https://github.com/hypermemetic-ai/plexus-core) - Core Plexus RPC library with Activation trait
