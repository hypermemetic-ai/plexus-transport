# Limits

## The socket

`with_socket_limits(SocketLimits { .. })` sets them. The defaults:

| field | default | what it bounds |
| -- | -- | -- |
| `max_message_size` | 10 MiB | one message, assembled from its fragments. A larger one is refused `-32007` and the socket stays open |
| `ping_interval` | 30 s | how often the server pings. `None` sends no ping and closes no silent peer |
| `ping_timeout` | 70 s | a peer that sends nothing for this long is closed |
| `idle_timeout` | none | a socket with no call open for this long is closed |
| `header_timeout` | 10 s | a connection sends its request's headers within this, or is dropped |
| `max_pending_per_ip` | `max_per_ip` of the quotas | sockets from one address that have not upgraded yet |
| `queue` | 1,024 frames | frames waiting for the socket's writer. When it is full, that socket's calls wait |
| `write_budget` | 64 KiB | the bytes the writer gathers into one flush before it yields to other sockets |

## Connections

`with_connection_quotas(ConnectionQuotas { .. })` sets them. An upgrade over
a quota is refused 429.

| field | default | what it counts |
| -- | -- | -- |
| `max_connections` | 1,024 | every socket |
| `max_per_ip` | 256 | sockets from one address |
| `max_per_principal` | 64 | sockets one signed-in caller holds |
| `max_anonymous` | 128 | sockets that are no one |
| `max_anonymous_per_ip` | 32 | anonymous sockets from one address |
| `max_calls_per_connection` | 256 | calls open on one socket at once |

`max_per_principal`, `max_anonymous` and `max_anonymous_per_ip` apply under
a session validator. Without one, only the total and the per-address quota
count sockets. The address is the
client's: see [Behind a proxy](proxy.md).

A call past `max_calls_per_connection` is refused `too_many_calls`.

## The connection loop

- One task reads and writes each socket. Each call is a task of its own, so
  calls on one socket overlap.
- The reader takes frames in arrival order and routes each by its shape. A
  call goes to the method table. An answer to no request of this side is
  dropped.
- The writer takes every frame that is ready, up to its budget, flushes
  once, then yields.
- A hook (`with_hooks`, `Hooks::new`) sees each inbound frame before
  dispatch and may take it. `Connection::writer()` writes this side's own
  requests on an accepted socket.
- Every frame is JSON-RPC text: `Connection::encoding()` is `Encoding::Json`.
- A signed-in socket is closed once its session ends: the server asks the
  validator every second.
- Logs name no header and no frame, at any level.
