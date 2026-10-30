//! PERF-20: plexus-transport's own WebSocket connection loop.
//!
//! A Plexus WebSocket is the transport's own. The upgrade is answered here
//! (after [`crate::websocket`]'s checks, in their order); the framing under it
//! is tokio-tungstenite's (RFC 6455: masking, opcodes, fragments, control
//! frames, close codes); the method table above it is jsonrpsee-core's
//! [`Methods`](jsonrpsee::Methods), driven through its public API as
//! jsonrpsee-server drove it; the gate and dispatch stay plexus-core's. The
//! loop between them is this module:
//!
//! | when | then |
//! | -- | -- |
//! | a socket is upgraded | one task serves it, reading and writing: a call's frames and the wakes they cause land on it, never on another task (a call is its own task, jsonrpsee-core's) |
//! | frames arrive | the socket's reader takes them in arrival order and routes each by its shape (spec/14 rule 2); no task is spawned per frame |
//! | a call frame (`method` and `id`) | the method table's callback runs it with the upgrade's extensions (the connection, its `AuthContext`, its `ConnectionId`); a subscription is its own task (jsonrpsee-core's), so calls on one socket overlap |
//! | each inbound frame, before dispatch | the connection's [`ConnectionHook`] sees it and may take it (BIDIR-12) |
//! | a frame that answers no request of this side's | dropped, never answered `-32600` (LOCAL-7) |
//! | frames are ready to write | the socket's writer takes every ready frame, up to its budget (64 KiB), writes them and flushes once, then yields |
//! | a reader is slow | the socket's queue is bounded (1,024 frames); a full queue holds that socket's calls back |
//! | a message is over the cap (10 MiB) | refused `-32007` with `id: null`, the socket kept; fragments are assembled under the same cap |
//! | a peer goes silent, or never calls | pinged on an interval and closed past the ping timeout; under an idle limit, closed with no call open past it (RPC-5) |
//! | a TCP connection never completes its request | dropped at the header timeout; sockets not yet upgraded are bounded per IP (RPC-4) |
//! | either side closes | the close handshake is answered; every call served ends; the hook hears it |
//! | a request that is not an upgrade | `426 Upgrade Required` |
//! | anything is logged | no header, no frame (ACCESS-62) |
//!
//! [`SocketLimits`] holds the numbers; [`Hooks`] makes each connection's hook;
//! [`Connection::writer`](crate::websocket::Connection::writer) is the handle
//! this side writes its own requests through; [`Encoding`] is fixed at the
//! upgrade (JSON until PERF-9).

mod conn;
mod guard;
mod http;
mod stop;

use std::sync::Arc;
use std::time::Duration;

pub use http::{is_upgrade_request, HttpBody};
pub use stop::{AlreadyStopped, ServerHandle};

pub(crate) use http::{next_connection_id, serve_http, Shared, Upgrade};
pub(crate) use stop::{stop_channel, StopHandle};

/// RPC-6: the largest message a socket takes, as jsonrpsee's default.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// The numbers a socket is held to (PERF-20; each today's value, or its
/// gap's: RPC-4, RPC-5, RPC-6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketLimits {
    /// The largest message, in bytes, assembled from its fragments; past it
    /// the message is refused `-32007` (`id: null`) and the socket goes on.
    /// Default 10 MiB (jsonrpsee's).
    pub max_message_size: usize,
    /// A ping every this long; `None`: none sent, and no peer is closed for
    /// silence. Default 30 s (jsonrpsee's `PingConfig`).
    pub ping_interval: Option<Duration>,
    /// While pinging: a peer from which nothing has come for this long — no
    /// frame of any kind, a pong included — is closed (a dead peer, a
    /// half-open socket; RPC-5). Default 70 s: two pings missed.
    pub ping_timeout: Duration,
    /// A socket with no call open and none begun for this long is closed, a
    /// socket that never made a call included (RPC-5). Default `None`: the
    /// service's to set (menger's `--idle-timeout-secs`).
    pub idle_timeout: Option<Duration>,
    /// A connection must send its request's headers within this long, or it
    /// is dropped (RPC-4). Default 10 s.
    pub header_timeout: Duration,
    /// Sockets accepted from one client IP and not yet upgraded; past it, a
    /// new one is dropped at the accept loop (RPC-4). `None` (the default):
    /// the connection quotas' `max_per_ip`. A trusted proxy's address
    /// carries every client's sockets before their headers name the client,
    /// so it is held to the quotas' `max_connections` instead.
    pub max_pending_per_ip: Option<u32>,
    /// Frames queued for a socket's writer before its calls wait. Default
    /// 1,024 (jsonrpsee's `message_buffer_capacity`).
    pub queue: usize,
    /// The bytes the writer gathers into one flush before it yields to other
    /// sockets. Default 64 KiB (PERF-17's E2a, the better size for 1 KB and
    /// 4 KB items).
    pub write_budget: usize,
}

impl Default for SocketLimits {
    fn default() -> Self {
        Self {
            max_message_size: MAX_MESSAGE_SIZE,
            ping_interval: Some(Duration::from_secs(30)),
            ping_timeout: Duration::from_secs(70),
            idle_timeout: None,
            header_timeout: Duration::from_secs(10),
            max_pending_per_ip: None,
            queue: 1024,
            write_budget: 64 * 1024,
        }
    }
}

/// The encoding of every frame on a socket, fixed at its upgrade. JSON-RPC
/// text is the only one until PERF-9 adds msgpack, read from the upgrade's
/// `Sec-WebSocket-Protocol` where [`Negotiated`] is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Encoding {
    /// JSON-RPC 2.0 objects, one text message each (spec/01).
    #[default]
    Json,
}

/// What a socket's upgrade settled: its [`Encoding`] (PERF-9 reads the
/// offer here) and, with PERF-8, whether the client takes batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Negotiated {
    /// The encoding every frame on the socket is in.
    pub encoding: Encoding,
}

/// A connection's own reader of inbound frames (BIDIR-12's seam, in place of
/// LOCAL-3's vendored `FrameHook`): every inbound message, in arrival order,
/// before the method table sees it.
pub trait ConnectionHook: Send + Sync + 'static {
    /// One inbound message (its payload, in the connection's encoding).
    /// `true`: the hook took it, and nothing else sees it. A frame the hook
    /// leaves goes on by its shape: a call to the method table, an answer to
    /// no request of this side's dropped.
    fn on_frame(&self, frame: &[u8]) -> bool;

    /// The connection is over — either side closed, or it was lost. Every
    /// call this side opened on it ends (`connection_lost`, spec/14 rule 6).
    fn on_close(&self) {}
}

type MakeHook = dyn Fn(&Arc<crate::websocket::Connection>, &::http::Extensions) -> Option<Arc<dyn ConnectionHook>> + Send + Sync;

/// Makes each upgraded connection's [`ConnectionHook`] (`None`: that
/// connection has none). Called once a socket is upgraded, with the
/// connection — its [`writer`](crate::websocket::Connection::writer) ready —
/// and the upgrade's extensions (its `AuthContext`, …).
#[derive(Clone)]
pub struct Hooks(Arc<MakeHook>);

impl Hooks {
    /// Hooks made by `make`.
    pub fn new<F>(make: F) -> Self
    where
        F: Fn(&Arc<crate::websocket::Connection>, &::http::Extensions) -> Option<Arc<dyn ConnectionHook>> + Send + Sync + 'static,
    {
        Self(Arc::new(make))
    }

    pub(crate) fn make(&self, conn: &Arc<crate::websocket::Connection>, ext: &::http::Extensions) -> Option<Arc<dyn ConnectionHook>> {
        (self.0)(conn, ext)
    }
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hooks")
    }
}

/// A frame this side writes on a socket through its
/// [`ConnectionWriter`]: one message, in the connection's encoding.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Outbound {
    /// A JSON-RPC object as text (the JSON encoding): one text message.
    Text(String),
    /// An encoded binary message (PERF-9's msgpack): one binary message.
    Binary(bytes::Bytes),
}

/// A write on a socket that has gone: `connection_lost`. The frame is given
/// back.
#[derive(Debug)]
pub struct ConnectionLost(pub Outbound);

impl std::fmt::Display for ConnectionLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connection_lost: the socket is closed")
    }
}

impl std::error::Error for ConnectionLost {}

/// The handle this side writes its own frames through on a socket it
/// accepted (BIDIR-12's `Peer::call`): the same writer as every answer, in
/// a queue of its own (bounded as the socket's is). Cheap to clone.
#[derive(Debug, Clone)]
pub struct ConnectionWriter {
    tx: tokio::sync::mpsc::Sender<Outbound>,
}

impl ConnectionWriter {
    /// Queue `frame`; waits while the socket's queue is full.
    pub async fn send(&self, frame: Outbound) -> Result<(), ConnectionLost> {
        self.tx.send(frame).await.map_err(|e| ConnectionLost(e.0))
    }

    /// Queue `frame` now, or say why not (`Full`, `Closed`).
    pub fn try_send(&self, frame: Outbound) -> Result<(), tokio::sync::mpsc::error::TrySendError<Outbound>> {
        self.tx.try_send(frame)
    }

    /// Resolves once the socket is gone.
    pub async fn closed(&self) {
        self.tx.closed().await
    }

    /// Whether the socket is gone.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// ACCESS-62: tungstenite traces every frame it reads and writes, payload
/// included (a session token an answer carries among them), through the
/// `log` facade. Once a socket is served, nothing above `debug` from `log`
/// is kept; the transport's own records are `tracing`'s and name no header
/// and no frame.
pub(crate) fn cap_frame_logs() {
    if log::max_level() > log::LevelFilter::Debug {
        log::set_max_level(log::LevelFilter::Debug);
        tracing::debug!("`log` records capped at debug: the WebSocket framing's trace carries frame payloads (ACCESS-62)");
    }
}
