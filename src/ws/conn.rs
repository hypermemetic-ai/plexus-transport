//! One socket's loop: its reader (this task) and its writer
//! ([`super::writer`], a task of its own).
//!
//! The reader takes every inbound message in the order it came, lets the
//! connection's hook see it first, and routes it by its shape (spec/14 rule
//! 2): a call (`method` and `id`) to the method table, a notification to
//! nothing (as before), an answer to no request of this side's dropped, any
//! other object `-32600`, anything else `-32700`. A call is the method
//! table's to run — jsonrpsee-core's own callbacks, through its public API,
//! as jsonrpsee-server drove them: a subscription's callback starts its own
//! task (one task a call), its acceptance is awaited here among the socket's
//! other work, never in a task of its own; a plain async method is a task of
//! its own; a sync one runs in place. No task is spawned per frame, so the
//! method table sees calls in the order they came.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, SplitStream};
use futures::StreamExt;
use http::Extensions;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use jsonrpsee::core::server::helpers::prepare_error;
use jsonrpsee::core::server::{BatchResponseBuilder, MethodCallback, MethodResponse, MethodSink, SubscriptionState};
use jsonrpsee::types::error::{reject_too_big_request, reject_too_many_subscriptions, ErrorCode};
use jsonrpsee::types::{ErrorObject, Id, Notification, Params, Request};
use jsonrpsee::ConnectionId;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::guard::{Dropped, Guard};
use super::http::Shared;
use super::writer::{self, Close};
use super::{ConnectionHook, ConnectionWriter, Negotiated, StopHandle};
use crate::websocket::Connection;

pub(crate) type Socket = WebSocketStream<Guard<TokioIo<Upgraded>>>;

/// jsonrpsee-server's: a subscription id answer never this big.
const MAX_RESPONSE: usize = super::MAX_MESSAGE_SIZE;
/// jsonrpsee-server's subscriptions on one connection.
const MAX_SUBSCRIPTIONS: u32 = 1024;
/// How long a close handshake is given before the socket is dropped.
const CLOSE_WAIT: Duration = Duration::from_secs(2);

/// Why a socket's loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// The peer sent its close; the reply is the writer's.
    PeerClosed,
    /// The socket ended without a close, or failed.
    Lost,
    /// The peer broke the protocol (tungstenite refused a frame).
    Protocol,
    /// The server is stopping.
    ServerStopped,
    /// The connection was closed (AUTH-11's sign-out watch, menger's idle sweep, …).
    Closed,
    /// Nothing from the peer past the ping timeout (RPC-5).
    Silent,
    /// No call open and none begun past the idle limit (RPC-5).
    Idle,
}

impl End {
    /// The close frame the server sends, if any.
    fn close(self) -> Close {
        let frame = |code: CloseCode, why: &'static str| Close::Send(CloseFrame { code, reason: why.into() });
        match self {
            End::PeerClosed => Close::Reply,
            End::Lost => Close::Drop,
            End::Protocol => frame(CloseCode::Protocol, "protocol error"),
            End::ServerStopped => frame(CloseCode::Away, "the server is stopping"),
            End::Closed => frame(CloseCode::Normal, "closed"),
            End::Silent => frame(CloseCode::Away, "no answer to pings"),
            End::Idle => frame(CloseCode::Normal, "idle"),
        }
    }
}

/// The method table, as one socket drives it.
#[derive(Clone)]
struct Dispatch {
    shared: Arc<Shared>,
    conn_id: ConnectionId,
    /// The socket's queue, as the method table writes to it.
    sink: MethodSink,
    /// jsonrpsee-server's 1,024 subscriptions a connection: a permit each,
    /// held for the subscription's life — so the calls open are counted.
    subscriptions: Arc<Semaphore>,
}

impl Dispatch {
    fn subscriptions_open(&self) -> usize {
        MAX_SUBSCRIPTIONS as usize - self.subscriptions.available_permits()
    }

    /// One call of a batch, answered in full (jsonrpsee-server's
    /// `RpcService::call`): a subscription's acceptance awaited here.
    async fn call(&self, req: Request<'_>, ext: Extensions) -> MethodResponse {
        let Request { id, method, params, .. } = req;
        let params = Params::new(params.as_ref().map(|p| p.get()));
        match self.shared.methods.method_with_name(&method) {
            None => MethodResponse::error(id, ErrorObject::from(ErrorCode::MethodNotFound)),
            Some((_, MethodCallback::Sync(cb))) => (cb)(id, params, MAX_RESPONSE, ext),
            Some((_, MethodCallback::Async(cb))) => {
                (cb)(id.into_owned(), params.into_owned(), self.conn_id, MAX_RESPONSE, ext).await
            }
            Some((_, MethodCallback::Subscription(cb))) => match self.subscriptions.clone().try_acquire_owned() {
                Ok(permit) => {
                    let state = SubscriptionState { conn_id: self.conn_id, id_provider: &*self.shared.ids, subscription_permit: permit };
                    (cb)(id, params, self.sink.clone(), state, ext).await
                }
                Err(_) => MethodResponse::error(id, reject_too_many_subscriptions(MAX_SUBSCRIPTIONS)),
            },
            Some((_, MethodCallback::Unsubscription(cb))) => (cb)(id, params, self.conn_id, MAX_RESPONSE, ext),
        }
    }

    /// A batch (a JSON array), as jsonrpsee-server answered one: its calls
    /// in order, one array back; notifications in it answered nothing.
    async fn batch(&self, data: &[u8], ext: Extensions) -> MethodResponse {
        let Ok(entries) = serde_json::from_slice::<Vec<&RawValue>>(data) else {
            return MethodResponse::error(Id::Null, ErrorObject::from(ErrorCode::ParseError));
        };
        let mut out = BatchResponseBuilder::new_with_limit(MAX_RESPONSE);
        let mut got_notification = false;
        for entry in entries {
            let rp = if let Ok(req) = serde_json::from_str::<Request>(entry.get()) {
                self.call(req, ext.clone()).await
            } else if serde_json::from_str::<Notification<Option<&RawValue>>>(entry.get()).is_ok() {
                got_notification = true;
                continue;
            } else {
                let id = serde_json::from_str::<jsonrpsee::types::InvalidRequest>(entry.get()).map(|e| e.id).unwrap_or(Id::Null);
                MethodResponse::error(id, ErrorObject::from(ErrorCode::InvalidRequest))
            };
            if let Err(too_big) = out.append(rp) {
                return too_big;
            }
        }
        if out.is_empty() && got_notification {
            MethodResponse::notification()
        } else {
            MethodResponse::from_batch(out.finish())
        }
    }
}

/// A frame that answers a request — no `method`, an `id`, and a `result` or
/// an `error` (spec/14 rule 2's first row).
fn is_answer(data: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct Shape<'a> {
        #[serde(borrow, default)]
        method: Option<&'a RawValue>,
        #[serde(borrow, default)]
        id: Option<&'a RawValue>,
        #[serde(borrow, default)]
        result: Option<&'a RawValue>,
        #[serde(borrow, default)]
        error: Option<&'a RawValue>,
    }
    matches!(serde_json::from_slice::<Shape>(data), Ok(s) if s.method.is_none() && s.id.is_some() && (s.result.is_some() || s.error.is_some()))
}

/// Send a response the way jsonrpsee-server did: only a method call's or a
/// batch's (a subscription answers through its sink), then tell the method
/// it went out.
async fn send_response(tx: &mpsc::Sender<Box<RawValue>>, rp: MethodResponse) {
    if rp.is_method_call() || rp.is_batch() {
        let ok = rp.is_success();
        let (json, on_close, _) = rp.into_parts();
        if tx.send(json).await.is_ok() {
            if let Some(n) = on_close {
                n.notify(ok);
            }
        }
    }
}

struct Reader {
    conn: Arc<Connection>,
    ext: Extensions,
    dispatch: Dispatch,
    /// The socket's queue (the method table's sink is a clone of it).
    tx: mpsc::Sender<Box<RawValue>>,
    hook: Option<Arc<dyn ConnectionHook>>,
    /// Subscriptions whose acceptance is awaited (their callbacks' futures).
    accepting: FuturesUnordered<BoxFuture<'static, MethodResponse>>,
    /// Plain async calls and batches running on tasks of their own.
    running: Arc<AtomicUsize>,
    dropped: Arc<Dropped>,
    answered_drops: u64,
    max_message: usize,
    ping_timeout: Option<Duration>,
    idle: Option<Duration>,
    last_in: Instant,
    last_busy: Instant,
    stop: StopHandle,
}

impl Reader {
    /// Wait for room in the queue, unless the socket ends first.
    async fn send(&mut self, json: Box<RawValue>) -> Result<(), End> {
        match self.tx.try_send(json) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(End::Lost),
            Err(mpsc::error::TrySendError::Full(json)) => {
                let deadline = self.ping_timeout.map(|t| tokio::time::Instant::from_std(self.last_in + t));
                let silent = async move {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    r = self.tx.send(json) => r.map_err(|_| End::Lost),
                    _ = self.stop.clone().shutdown() => Err(End::ServerStopped),
                    _ = self.conn.closed() => Err(End::Closed),
                    _ = silent => Err(End::Silent),
                }
            }
        }
    }

    async fn finish(&mut self, rp: MethodResponse) -> Result<(), End> {
        if rp.is_method_call() || rp.is_batch() {
            let ok = rp.is_success();
            let (json, on_close, _) = rp.into_parts();
            self.send(json).await?;
            if let Some(n) = on_close {
                n.notify(ok);
            }
        }
        Ok(())
    }

    /// `-32007` for each message the guard dropped over the cap.
    async fn answer_drops(&mut self) -> Result<(), End> {
        while self.answered_drops < self.dropped.count() {
            self.answered_drops += 1;
            let rp = MethodResponse::error(Id::Null, reject_too_big_request(self.max_message as u32));
            self.finish(rp).await?;
        }
        Ok(())
    }

    /// One inbound message, in arrival order.
    async fn message(&mut self, data: &[u8], owned: Option<&Bytes>) -> Result<(), End> {
        if let Some(hook) = &self.hook {
            if hook.on_frame(data) {
                return Ok(());
            }
        }
        let first = data.iter().take(128).position(|b| !b.is_ascii_whitespace());
        match first.map(|i| (i, data[i])) {
            Some((i, b'{')) => self.single(&data[i..]).await,
            Some((i, b'[')) => {
                // A batch, which no client sends: a task of its own, so its
                // calls run to their answers off the reader.
                self.last_busy = Instant::now();
                let bytes = owned.map(|b| b.slice(i..)).unwrap_or_else(|| Bytes::copy_from_slice(&data[i..]));
                let (d, tx, ext, running) = (self.dispatch.clone(), self.tx.clone(), self.ext.clone(), self.running.clone());
                running.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    let rp = d.batch(&bytes, ext).await;
                    send_response(&tx, rp).await;
                    running.fetch_sub(1, Ordering::AcqRel);
                });
                Ok(())
            }
            _ => self.finish(MethodResponse::error(Id::Null, ErrorObject::from(ErrorCode::ParseError))).await,
        }
    }

    async fn single(&mut self, data: &[u8]) -> Result<(), End> {
        if let Ok(req) = serde_json::from_slice::<Request>(data) {
            self.last_busy = Instant::now();
            return self.call(req).await;
        }
        if serde_json::from_slice::<Notification<Option<&RawValue>>>(data).is_ok() {
            // A notification is dispatched to nothing, as jsonrpsee did; the
            // hook is where a frame of this side's own calls is taken.
            return Ok(());
        }
        if is_answer(data) {
            // An answer to no request of this side's: dropped, never answered
            // (answering a response is how ids would collide — LOCAL-7).
            tracing::trace!(conn = self.conn.id(), "an answer to no request dropped");
            return Ok(());
        }
        let (id, code) = prepare_error(data);
        self.finish(MethodResponse::error(id, ErrorObject::from(code))).await
    }

    async fn call(&mut self, req: Request<'_>) -> Result<(), End> {
        let Request { id, method, params, .. } = req;
        let params = Params::new(params.as_ref().map(|p| p.get()));
        let d = &self.dispatch;
        match d.shared.methods.method_with_name(&method) {
            None => self.finish(MethodResponse::error(id, ErrorObject::from(ErrorCode::MethodNotFound))).await,
            Some((_, MethodCallback::Sync(cb))) => {
                let rp = (cb)(id, params, MAX_RESPONSE, self.ext.clone());
                self.finish(rp).await
            }
            Some((_, MethodCallback::Async(cb))) => {
                let fut = (cb)(id.into_owned(), params.into_owned(), d.conn_id, MAX_RESPONSE, self.ext.clone());
                let (tx, running) = (self.tx.clone(), self.running.clone());
                running.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    send_response(&tx, fut.await).await;
                    running.fetch_sub(1, Ordering::AcqRel);
                });
                Ok(())
            }
            Some((_, MethodCallback::Subscription(cb))) => match d.subscriptions.clone().try_acquire_owned() {
                Ok(permit) => {
                    let state = SubscriptionState { conn_id: d.conn_id, id_provider: &*d.shared.ids, subscription_permit: permit };
                    // The callback starts the subscription's own task; what it
                    // hands back resolves once the subscription is accepted
                    // (its id already sent through the sink) or refused.
                    let accepting = (cb)(id, params, d.sink.clone(), state, self.ext.clone());
                    self.accepting.push(accepting);
                    Ok(())
                }
                Err(_) => self.finish(MethodResponse::error(id, reject_too_many_subscriptions(MAX_SUBSCRIPTIONS))).await,
            },
            Some((_, MethodCallback::Unsubscription(cb))) => {
                let rp = (cb)(id, params, d.conn_id, MAX_RESPONSE, self.ext.clone());
                self.finish(rp).await
            }
        }
    }

    /// A call open: a subscription (its permit held), a plain call or batch
    /// running, an acceptance awaited, or a place in the connection's count.
    fn busy(&self) -> bool {
        self.dispatch.subscriptions_open() > 0
            || self.running.load(Ordering::Acquire) > 0
            || !self.accepting.is_empty()
            || self.conn.calls().open_count() > 0
    }

    /// The liveness rules (RPC-5), at each tick.
    fn liveness(&mut self) -> Option<End> {
        let now = Instant::now();
        if let Some(t) = self.ping_timeout {
            if now.duration_since(self.last_in) > t {
                return Some(End::Silent);
            }
        }
        if let Some(idle) = self.idle {
            if self.busy() {
                self.last_busy = now;
            } else if now.duration_since(self.last_busy) >= idle {
                return Some(End::Idle);
            }
        }
        None
    }

    async fn run(&mut self, stream: &mut SplitStream<Socket>) -> End {
        let shutdown = self.stop.clone().shutdown();
        tokio::pin!(shutdown);
        let tick = [self.ping_timeout, self.idle].into_iter().flatten().min().map(|d| (d / 8).clamp(Duration::from_millis(25), Duration::from_secs(5)));
        let mut ticks = tokio::time::interval(tick.unwrap_or(Duration::from_secs(3600)));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticks.reset();
        let dropped = self.dropped.clone();
        let conn = self.conn.clone();
        loop {
            let step = tokio::select! {
                biased;
                _ = &mut shutdown => Err(End::ServerStopped),
                _ = conn.closed() => Err(End::Closed),
                _ = dropped.notified() => self.answer_drops().await,
                Some(rp) = self.accepting.next(), if !self.accepting.is_empty() => self.finish(rp).await,
                msg = stream.next() => match msg {
                    Some(Ok(m)) => {
                        self.last_in = Instant::now();
                        let r = match m {
                            Message::Text(t) => {
                                let b = Bytes::from(t);
                                self.message(&b, Some(&b)).await
                            }
                            Message::Binary(b) => self.message(&b, Some(&b)).await,
                            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(()),
                            Message::Close(_) => Err(End::PeerClosed),
                        };
                        match r {
                            Ok(()) => self.answer_drops().await,
                            e => e,
                        }
                    }
                    Some(Err(e)) => {
                        use tokio_tungstenite::tungstenite::Error as E;
                        tracing::debug!(conn = conn.id(), peer = %conn.peer(), "WebSocket read: {e}");
                        Err(match e {
                            E::Protocol(_) | E::Utf8(_) | E::Capacity(_) | E::AttackAttempt => End::Protocol,
                            _ => End::Lost,
                        })
                    }
                    None => Err(End::Lost),
                },
                _ = ticks.tick(), if tick.is_some() => self.liveness().map_or(Ok(()), Err),
            };
            if let Err(end) = step {
                return end;
            }
        }
    }
}

/// Serve one upgraded socket until it ends (see the module docs).
pub(crate) async fn run(
    io: TokioIo<Upgraded>,
    conn: Arc<Connection>,
    ext: Extensions,
    negotiated: Negotiated,
    shared: Arc<Shared>,
    stop: StopHandle,
    _place: OwnedSemaphorePermit,
) {
    let limits = &shared.limits;
    let guard = Guard::new(io, limits.max_message_size);
    let dropped = guard.dropped();
    let config = WebSocketConfig::default()
        .max_message_size(Some(limits.max_message_size))
        .max_frame_size(Some(limits.max_message_size))
        // Each read clears what it may fill; small for the calls' small frames.
        .read_buffer_size(4096)
        // Above the writer's budget, so the budget's frames meet one write.
        .write_buffer_size((limits.write_budget * 2).max(128 * 1024));
    let socket: Socket = WebSocketStream::from_raw_socket(guard, Role::Server, Some(config)).await;
    let (sink, mut stream) = socket.split();

    let (tx, table_rx) = mpsc::channel::<Box<RawValue>>(limits.queue);
    let (own_tx, own_rx) = mpsc::channel(limits.queue);
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    conn.upgraded(negotiated, ConnectionWriter { tx: own_tx });
    let writer = tokio::spawn(writer::run(sink, table_rx, own_rx, close_rx, limits.ping_interval, limits.write_budget));

    let hook = shared.hooks.as_ref().and_then(|h| h.make(&conn, &ext));
    let dispatch = Dispatch {
        conn_id: ConnectionId(conn.id() as usize),
        sink: MethodSink::new_with_limit(tx.clone(), MAX_RESPONSE as u32),
        subscriptions: Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS as usize)),
        shared: shared.clone(),
    };
    let now = Instant::now();
    let mut reader = Reader {
        conn: conn.clone(),
        ext,
        dispatch,
        tx,
        hook,
        accepting: FuturesUnordered::new(),
        running: Arc::new(AtomicUsize::new(0)),
        dropped,
        answered_drops: 0,
        max_message: limits.max_message_size,
        ping_timeout: limits.ping_interval.map(|_| limits.ping_timeout),
        idle: limits.idle_timeout,
        last_in: now,
        last_busy: now,
        stop,
    };
    let end = reader.run(&mut stream).await;
    tracing::debug!(conn = conn.id(), peer = %conn.peer(), "WebSocket closing: {end:?}");

    // The writer sends the close (or its reply) and lets the queues go, so
    // every call served sees its sink closed.
    let _ = close_tx.send(end.close());
    let Reader { hook, accepting, tx, dispatch, ext, .. } = reader;
    drop((accepting, tx, dispatch, ext));
    // Read on until the peer's close comes back (or the wait runs out), so the
    // handshake completes; then the socket goes.
    if matches!(end, End::Lost | End::Protocol) {
        drop(stream);
    } else {
        let _ = tokio::time::timeout(CLOSE_WAIT, async { while let Some(Ok(_)) = stream.next().await {} }).await;
    }
    let _ = writer.await;
    if let Some(hook) = hook {
        hook.on_close();
    }
    conn.ended();
}
