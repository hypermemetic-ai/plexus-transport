# TLS

With the feature `tls`, the server answers `https://` and `wss://` itself,
with rustls on the ring provider.

```toml
plexus-transport = { version = "0.10", features = ["tls"] }
```

```rust
use plexus_transport::TlsConfig;

TransportServer::builder(service, converter)
    .with_websocket(8443)
    .bind([0, 0, 0, 0])
    .with_tls(TlsConfig::new("/etc/plexus/host.crt", "/etc/plexus/host.key"))
    .with_allowed_origins(["https://host.example:8443"])
    .with_allowed_hosts(["host.example:8443"])
```

- **Every listener** the builder opens serves TLS with the pair: the
  WebSocket, the REST bridge and the MCP bridge. A listener's own config may
  name its own pair (`WebSocketConfig::with_tls`, `RestHttpConfig::with_tls`).
  The combined port takes one in `CombinedOptions`.
- **The pair** is a PEM certificate chain, leaf first, and its PEM key: RSA,
  ECDSA or Ed25519, in PKCS#8, PKCS#1 or SEC1. A pair that does not load
  refuses the start and names the file. The listener speaks TLS 1.3 and 1.2,
  with ALPN `http/1.1`.
- **Renewal** needs no restart. Each handshake compares the size, mtime and
  inode of both files, and the next connection gets a changed pair. A pair
  that fails to load leaves the one in use, is reported once, and is read
  again when a file changes. Open connections go on.
- **A plain `http://` request** or a bad handshake is refused at the
  handshake and logged once per peer address. The handshake runs in the
  connection's own task, within the header timeout.
- **Secure**: a connection over this TLS is secure with no forwarded header.
  `Connection::is_tls()` and `ForwardedClient::secure` say so. When the peer
  is a trusted proxy, the proxy's word stands instead.
- Client certificates, several certificates by SNI, OCSP stapling and HTTP/2
  are not served.

## A page and its socket on one origin

`with_routes(router)` puts an axum `Router` on the WebSocket's port, with or
without TLS. The socket answers at `/ws`, and every other path goes to the
router. `with_host_routes(HostRoutes::new(router).socket_at("/rpc"))` names
another path for the socket.

The socket keeps every check it has. A request for the router passes the
port's `Host` check, and carries its `ForwardedClient` and `Connection` in
its extensions. Without routes, the socket answers on every path.

A page these routes serve is the listener's own origin: see
[Origins and hosts](origins-and-hosts.md). A page reached by a public name
connects when `with_allowed_hosts` names that host, or
`with_allowed_origins` names its origin.
