# Origins and hosts

A browser sends `Origin` on every WebSocket, and a WebSocket has no
same-origin rule. The WebSocket listener is closed to web pages unless told
otherwise.

## Origin

| the upgrade carries | then |
| -- | -- |
| no `Origin` (synapse, a service, a command line) | it connects |
| the listener's own origin | it connects |
| an origin on the list | it connects |
| any other `Origin`, `null` included | 403, before any credential is read |

The listener's own origin is a page served on the same port, scheme and
`Host`: `http://` on a plain listener, `https://` over its TLS. Behind a
trusted proxy the scheme is the proxy's `X-Forwarded-Proto`; when the proxy
sends none, no origin is the listener's own.

```rust
.with_allowed_origins(["http://127.0.0.1:5173"])
```

Origins compare without case and without a trailing slash.

## Host

| the listener | answers to |
| -- | -- |
| has a list (`with_allowed_hosts`) | the hosts on it |
| has no list and is bound to loopback | `127.0.0.1:<port>` and `localhost:<port>`, and `[::1]:<port>` on IPv6 |
| has no list and is bound beyond loopback | any `Host`, and it logs a warning at start |

Any other `Host` is refused 421. This stops DNS rebinding. An entry is a
`Host` header as a client sends it: `host:port`, or `host` alone for a
default port. Entries compare without case and exactly.
`plexus_transport::host::loopback_hosts(port)` gives a loopback port's names.

Pages on the listener's port (`with_routes`) and the listener's own origin
answer to the list when there is one. With no list they answer to the
loopback names, and to the bound address when it is neither loopback nor
unspecified.

The REST bridge has its own list, `RestHttpConfig::with_allowed_hosts`. With
no list it checks nothing.

## Opening a listener

`allow_any_origin()` lets any site call the service from its visitor's
browser. `allow_any_host()` answers any `Host`. Only these two calls open a
listener. A list entry `*` is not a wildcard: `build()` refuses a list that
holds one.
