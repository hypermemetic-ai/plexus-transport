//! The HTTP side of a socket: one hyper HTTP/1.1 connection per accepted TCP
//! connection, with a header timeout (RPC-4); the request through
//! [`crate::websocket`]'s checks (Host 421, the RFC 9728 metadata, Origin
//! 403, the key 401, the session 401, the quotas 429), then [`Upgrade`]:
//! a WebSocket upgrade is answered `101` and its socket handed to the
//! connection loop; anything else is `426 Upgrade Required`.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use jsonrpsee::Methods;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use super::{Hooks, Negotiated, SocketLimits, StopHandle};
use crate::websocket::Connection;

/// Every HTTP answer the WebSocket port gives (the 101, the 426, the
/// checks' refusals).
pub type HttpBody = http_body_util::Full<Bytes>;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What every connection of one server shares.
pub(crate) struct Shared {
    pub(crate) methods: Methods,
    pub(crate) limits: SocketLimits,
    pub(crate) hooks: Option<Hooks>,
    /// AUTH-12's total (`max_connections`): a place per upgraded socket.
    connections: Arc<Semaphore>,
    max_connections: u32,
    /// jsonrpsee-server's subscription ids: random integers.
    pub(crate) ids: Arc<dyn jsonrpsee::core::traits::IdProvider>,
}

impl Shared {
    pub(crate) fn new(methods: Methods, limits: SocketLimits, hooks: Option<Hooks>, max_connections: u32) -> Arc<Self> {
        Arc::new(Self {
            methods,
            limits,
            hooks,
            connections: Arc::new(Semaphore::new(max_connections as usize)),
            max_connections,
            ids: Arc::new(jsonrpsee::core::id_providers::RandomIntegerIdProvider),
        })
    }
}

/// Connection ids, as the method table's `ConnectionId` (unique for the
/// process's life).
static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_connection_id() -> u64 {
    NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed)
}

/// Whether `request` asks for a WebSocket: `Connection: upgrade` and
/// `Upgrade: websocket`, each among its header's comma-separated tokens, in
/// any case (as soketto's check, which the quotas used).
pub fn is_upgrade_request<B>(request: &Request<B>) -> bool {
    fn has(headers: &HeaderMap, name: header::HeaderName, token: &[u8]) -> bool {
        headers.get_all(name).iter().any(|v| v.as_bytes().split(|&c| c == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(token)))
    }
    has(request.headers(), header::CONNECTION, b"upgrade") && has(request.headers(), header::UPGRADE, b"websocket")
}

fn plain(status: StatusCode, body: &'static str) -> Response<HttpBody> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(HttpBody::from(body))
        .expect("static response is valid")
}

/// `426 Upgrade Required` (RFC 9110 §15.5.22): the port speaks WebSocket only.
fn upgrade_required() -> Response<HttpBody> {
    let mut r = plain(StatusCode::UPGRADE_REQUIRED, "Upgrade Required: this port speaks JSON-RPC over WebSocket only");
    r.headers_mut().insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    r.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    r.headers_mut().insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
    r
}

/// The upgrade, the last step of the checks: `101` and the socket to the
/// connection loop, or `426`.
#[derive(Clone)]
pub(crate) struct Upgrade {
    pub(crate) shared: Arc<Shared>,
    pub(crate) stop: StopHandle,
}

impl Upgrade {
    pub(crate) fn upgrade<B>(&self, mut req: Request<B>) -> Response<HttpBody> {
        if !is_upgrade_request(&req) {
            return upgrade_required();
        }
        // RFC 6455 §4.2.1–2: version 13, and a key to accept.
        if req.headers().get(header::SEC_WEBSOCKET_VERSION).map(|v| v.as_bytes()) != Some(b"13") {
            return upgrade_required();
        }
        let Some(key) = req.headers().get(header::SEC_WEBSOCKET_KEY) else {
            return plain(StatusCode::BAD_REQUEST, "Bad Request: no Sec-WebSocket-Key");
        };
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let Some(conn) = req.extensions().get::<Arc<Connection>>().cloned() else {
            // Only the accept loop's requests reach here; a socket nobody
            // accepted has nowhere to go.
            return plain(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error: no connection");
        };
        // AUTH-12's total, held for the socket's life.
        let Ok(place) = self.shared.connections.clone().try_acquire_owned() else {
            tracing::warn!(peer = %conn.peer(), max = self.shared.max_connections, "WS upgrade refused: max connections");
            let mut r = plain(StatusCode::TOO_MANY_REQUESTS, "Too many connections. Please try again later.");
            r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            return r;
        };
        // PERF-9 reads `Sec-WebSocket-Protocol` here, and PERF-8 the batch
        // offer: what the socket speaks is settled at the upgrade.
        let negotiated = Negotiated::default();
        let on_upgrade = hyper::upgrade::on(&mut req);
        let mut ext = req.extensions().clone();
        ext.insert(jsonrpsee::ConnectionId(conn.id() as usize));
        let (shared, stop) = (self.shared.clone(), self.stop.clone());
        tokio::spawn(async move {
            match on_upgrade.await {
                Ok(upgraded) => super::conn::run(TokioIo::new(upgraded), conn, ext, negotiated, shared, stop, place).await,
                Err(e) => tracing::debug!(peer = %conn.peer(), "WS upgrade did not complete: {e}"),
            }
        });
        Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::CONNECTION, "upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_ACCEPT, accept)
            .body(HttpBody::default())
            .expect("static response is valid")
    }
}

impl<B> tower::Service<Request<B>> for Upgrade {
    type Response = Response<HttpBody>;
    type Error = BoxError;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        std::future::ready(Ok(self.upgrade(req)))
    }
}

/// Serve one accepted TCP connection: HTTP/1.1 with upgrades, its headers
/// within `header_timeout` (RPC-4), every request carrying `conn` and going
/// through `svc` (the checks, then the upgrade). Ends when the connection
/// does, or — gracefully — when the server or the connection is stopped.
pub(crate) async fn serve_http<S, B>(sock: TcpStream, conn: Arc<Connection>, svc: S, stop: StopHandle, header_timeout: Duration)
where
    S: tower::Service<Request<Incoming>, Response = Response<B>, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + From<&'static str> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let for_requests = conn.clone();
    let service = hyper::service::service_fn(move |mut req: Request<Incoming>| {
        req.extensions_mut().insert(for_requests.clone());
        let svc = svc.clone();
        async move {
            use tower::ServiceExt;
            Ok::<_, Infallible>(match svc.oneshot(req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!("websocket request failed: {e}");
                    let mut r = Response::new(B::from("Internal Server Error"));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    r
                }
            })
        }
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.timer(TokioTimer::new()).header_read_timeout(header_timeout).keep_alive(true);
    let connection = builder.serve_connection(TokioIo::new(sock), service).with_upgrades();
    tokio::pin!(connection);
    let closed = conn.closed();
    tokio::pin!(closed);
    tokio::select! {
        r = connection.as_mut() => {
            if let Err(e) = r {
                tracing::debug!(peer = %conn.peer(), "websocket connection: {e}");
            }
            return;
        }
        _ = stop.shutdown() => {}
        _ = &mut closed => {}
    }
    connection.as_mut().graceful_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(1), connection).await;
}
