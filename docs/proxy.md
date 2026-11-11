# Behind a proxy

Behind a proxy, the TCP peer of every visitor is the proxy. Name the proxy,
and the server counts the visitor instead.

```rust
TransportServer::builder(service, converter)
    .with_websocket(8080)
    .bind([0, 0, 0, 0])
    .with_trusted_proxies(TrustedProxies::parse(["172.30.0.2/32"]).map_err(anyhow::Error::msg)?)
```

`TrustedProxies::parse` reads networks (`10.0.0.0/8`), bare addresses, or
`none` alone: no proxy is in front. `trusted_proxies(nets)` takes `IpNet`s.

- `X-Forwarded-For` and `X-Forwarded-Proto` are believed only from a trusted
  proxy. From any other peer they are ignored, and that is logged once.
- The client is the rightmost `X-Forwarded-For` hop that is not itself a
  trusted proxy. A client cannot add hops to leave its quota.
- The [connection quotas](limits.md) count that client.
- Every request carries a `ForwardedClient { ip, peer, via_proxy, secure }`
  in its extensions, and so does every call.
- A connection is secure when a trusted proxy says `X-Forwarded-Proto:
  https` or `wss`. With no proxy in front, a loopback peer is secure. Behind
  a proxy, loopback proves nothing.
- `ClientIp` and `SecureTransport` in `plexus_transport::request` read the
  same proxies. `serve()` hands them over with `init_trusted_proxies`.
