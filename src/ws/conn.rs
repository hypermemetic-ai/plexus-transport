//! One socket's loop: its reader and its writer in one task (one task a
//! connection; a task a call kept).
//!
//! **Reading.** Every inbound message is taken in the order it came; the
//! connection's hook sees it first; then it is routed by its shape (spec/14
//! rule 2): a call (`method` and `id`) to the method table, a notification
//! to nothing (as before), an answer to no request of this side's dropped,
//! any other object `-32600`, anything else `-32700`. A call is the method
//! table's to run — jsonrpsee-core's own callbacks, through its public API,
//! as jsonrpsee-server drove them: a subscription's callback starts the
//! subscription's own task (the one task a call), and its acceptance is
//! awaited here among the socket's other work; a plain async method is a task
//! of its own; a sync one runs in place. No task is spawned per frame, so
//! the method table sees calls in the order they came. At most
//! [`READ_BUDGET`] frames are read a wakeup, and none while the answers wait
//! on a full socket.
//!
//! **Writing.** When it wakes the loop takes every frame that is ready — the
//! answers it made itself, the method table's queue (each call's answer,
//! and what a subscription writes through jsonrpsee's sink), the calls'
//! frames (PERF-7: each item encoded once by plexus-core's pipe into a frame
//! of its own, through the [`FrameSink`] every call's extensions carry),
//! this side's own ([`ConnectionWriter`]) — up to the budget, hands them to
//! the socket and flushes once: a call's three frames, or a stream's ready
//! items, share one `send` (PERF-17's E2a). The queues are taken by turns,
//! so none starves another; a call's first frame waits for the method
//! table's queue as it stands, where its subscription's answer was put
//! first. A queue is bounded (1,024 frames); while the socket cannot take
//! more, its calls wait and nothing more is read. A subscription's frames
//! and the wake they cause land on this one task, so a call costs no wake
//! of another.
//!
//! **Liveness** (RPC-5): a ping on the interval; a peer from which nothing
//! has come past the ping timeout is closed; under an idle limit a socket
//! with no call open and none begun past it is closed.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use http::Extensions;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use jsonrpsee::core::server::helpers::prepare_error;
use jsonrpsee::core::server::{BatchResponseBuilder, MethodCallback, MethodResponse, MethodSink, SubscriptionState};
use jsonrpsee::types::error::{reject_too_big_request, reject_too_many_subscriptions, ErrorCode};
use jsonrpsee::types::{ErrorObject, Id, Notification, Params, Request};
use jsonrpsee::ConnectionId;
use plexus_core::plexus::wire::{FrameReceiver, FrameSink, WireEncoding, WireFrame};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::WebSocketStream;

use super::guard::{Dropped, Guard};
use super::http::Shared;
use super::{ConnectionHook, ConnectionWriter, Negotiated, Outbound, StopHandle};
use crate::websocket::Connection;

type Socket = WebSocketStream<Guard<TokioIo<Upgraded>>>;

/// jsonrpsee-server's: a method's answer is capped at this.
const MAX_RESPONSE: usize = super::MAX_MESSAGE_SIZE;
/// jsonrpsee-server's subscriptions on one connection.
const MAX_SUBSCRIPTIONS: u32 = 1024;
/// Frames read in one wakeup before the socket lets the others run.
const READ_BUDGET: usize = 64;
/// Answers waiting for the socket past which no more frames are read.
const OUT_HIGH: usize = 1024;
/// How long a close handshake is given before the socket is dropped.
const CLOSE_WAIT: Duration = Duration::from_secs(2);

/// Why a socket's loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// The peer sent its close; the reply goes out.
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
    /// The close frame the server sends (`None` for the two ends that send
    /// none: the peer's close is answered, or the socket is gone).
    fn frame(self) -> Option<CloseFrame> {
        let frame = |code: CloseCode, why: &'static str| Some(CloseFrame { code, reason: why.into() });
        match self {
            End::PeerClosed | End::Lost => None,
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

/// What a task of a call's own writes back, the way jsonrpsee-server did:
/// only a method call's answer or a batch's (a subscription answers through
/// its sink), then the method is told it went out.
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

fn text(raw: Box<RawValue>) -> Message {
    Message::Text(String::from(Box::<str>::from(raw)).into())
}

fn own(frame: Outbound) -> Message {
    match frame {
        Outbound::Text(t) => Message::Text(t.into()),
        Outbound::Binary(b) => Message::Binary(b),
    }
}

/// A call's frame as the socket writes it: its bytes, as they are.
fn framed(bytes: Bytes) -> Message {
    // SAFETY: every byte of a frame is serde_json's output — the
    // notification around the item, the item's own JSON — or JSON text
    // checked when it was made (`Content::from_json`): UTF-8.
    Message::Text(unsafe { Utf8Bytes::from_bytes_unchecked(bytes) })
}

/// The queues the writer takes from, by turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Queue {
    /// This side's own frames ([`ConnectionWriter`]).
    Own,
    /// The method table's: answers, and jsonrpsee's subscription sinks.
    Table,
    /// The calls' frames (plexus-core's pipe, PERF-7).
    Frames,
}

/// One socket (see the module docs).
struct Loop {
    ws: Socket,
    conn: Arc<Connection>,
    ext: Extensions,
    dispatch: Dispatch,
    /// The socket's queue, for the tasks of a call's own (the method table's
    /// sink is a clone of it).
    tx: mpsc::Sender<Box<RawValue>>,
    table: mpsc::Receiver<Box<RawValue>>,
    own: mpsc::Receiver<Outbound>,
    own_open: bool,
    /// PERF-7: the calls' frames.
    frames: FrameReceiver,
    /// A call's first frame, waiting for the method table's queue as it
    /// stood when the frame was taken (its subscription's answer is there).
    fenced: Option<Bytes>,
    /// The queue taken from first at the next turn (by turns, so none
    /// starves another).
    turn: usize,
    hook: Option<Arc<dyn ConnectionHook>>,
    /// Subscriptions whose acceptance is awaited (their callbacks' futures),
    /// in arrival order, each polled with the socket's own waker — at most
    /// 1,024 (a subscription's permit is taken first), and short-lived: a
    /// callback answers as soon as its task runs. Not a `FuturesUnordered`:
    /// having polled every future it holds, that wakes its parent
    /// (futures-util 0.3.34, `futures_unordered/mod.rs:556`), so every call
    /// made this task yield once and wake an idle worker for it (tokio's
    /// `yield_now`: 4.8 % of the server's samples under `call.one_item`).
    accepting: Vec<BoxFuture<'static, MethodResponse>>,
    /// Plain async calls and batches running on tasks of their own.
    running: Arc<AtomicUsize>,
    dropped: Arc<Dropped>,
    answered_drops: u64,
    /// Answers the loop made itself, waiting for the socket.
    out: VecDeque<Message>,
    /// Bytes handed to the socket since its last flush.
    gathered: usize,
    unflushed: bool,
    budget: usize,
    max_message: usize,
    ping: Option<tokio::time::Interval>,
    ping_timeout: Option<Duration>,
    idle: Option<Duration>,
    ticks: Option<tokio::time::Interval>,
    last_in: Instant,
    last_busy: Instant,
    /// The server's stop, and the connection's own close.
    stopping: Pin<Box<dyn Future<Output = ()> + Send>>,
    closing: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Loop {
    /// A method call's (or a batch's) answer into the outgoing frames; a
    /// subscription answers through its sink.
    fn respond(&mut self, rp: MethodResponse) {
        if rp.is_method_call() || rp.is_batch() {
            let ok = rp.is_success();
            let (json, on_close, _) = rp.into_parts();
            self.out.push_back(text(json));
            if let Some(n) = on_close {
                n.notify(ok);
            }
        }
    }

    /// Each acceptance that has resolved, answered in arrival order (a
    /// subscription's id is already out through its sink; a refusal is
    /// answered here). `true`: one did.
    fn poll_accepting(&mut self, cx: &mut Context<'_>) -> bool {
        if self.accepting.is_empty() {
            return false;
        }
        let mut moved = false;
        let mut accepting = std::mem::take(&mut self.accepting);
        accepting.retain_mut(|f| match f.as_mut().poll(cx) {
            Poll::Ready(rp) => {
                self.respond(rp);
                moved = true;
                false
            }
            Poll::Pending => true,
        });
        self.accepting = accepting;
        moved
    }

    /// `-32007` for each message the guard dropped over the cap.
    fn answer_drops(&mut self) {
        while self.answered_drops < self.dropped.count() {
            self.answered_drops += 1;
            self.respond(MethodResponse::error(Id::Null, reject_too_big_request(self.max_message as u32)));
        }
    }

    /// One inbound message, in arrival order.
    fn message(&mut self, m: Message) -> Result<(), End> {
        self.last_in = Instant::now();
        let data = match m {
            Message::Text(t) => Bytes::from(t),
            Message::Binary(b) => b,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return Ok(()),
            Message::Close(_) => return Err(End::PeerClosed),
        };
        if let Some(hook) = &self.hook {
            if hook.on_frame(&data) {
                return Ok(());
            }
        }
        let first = data.iter().take(128).position(|b| !b.is_ascii_whitespace());
        match first.map(|i| (i, data[i])) {
            Some((i, b'{')) => self.single(&data[i..]),
            Some((i, b'[')) => {
                // A batch, which no client sends: a task of its own, so its
                // calls run to their answers off the loop.
                self.last_busy = Instant::now();
                let bytes = data.slice(i..);
                let (d, tx, ext, running) = (self.dispatch.clone(), self.tx.clone(), self.ext.clone(), self.running.clone());
                running.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    let rp = d.batch(&bytes, ext).await;
                    send_response(&tx, rp).await;
                    running.fetch_sub(1, Ordering::AcqRel);
                });
            }
            _ => self.respond(MethodResponse::error(Id::Null, ErrorObject::from(ErrorCode::ParseError))),
        }
        Ok(())
    }

    fn single(&mut self, data: &[u8]) {
        if let Ok(req) = serde_json::from_slice::<Request>(data) {
            self.last_busy = Instant::now();
            return self.call(req);
        }
        if serde_json::from_slice::<Notification<Option<&RawValue>>>(data).is_ok() {
            // A notification is dispatched to nothing, as jsonrpsee did; the
            // hook is where a frame of this side's own calls is taken.
            return;
        }
        if is_answer(data) {
            // An answer to no request of this side's: dropped, never answered
            // (answering a response is how ids would collide — LOCAL-7).
            tracing::trace!(conn = self.conn.id(), "an answer to no request dropped");
            return;
        }
        let (id, code) = prepare_error(data);
        self.respond(MethodResponse::error(id, ErrorObject::from(code)));
    }

    fn call(&mut self, req: Request<'_>) {
        let Request { id, method, params, .. } = req;
        let params = Params::new(params.as_ref().map(|p| p.get()));
        let shared = self.dispatch.shared.clone();
        let conn_id = self.dispatch.conn_id;
        match shared.methods.method_with_name(&method) {
            None => self.respond(MethodResponse::error(id, ErrorObject::from(ErrorCode::MethodNotFound))),
            Some((_, MethodCallback::Sync(cb))) => {
                let rp = (cb)(id, params, MAX_RESPONSE, self.ext.clone());
                self.respond(rp);
            }
            Some((_, MethodCallback::Async(cb))) => {
                let fut = (cb)(id.into_owned(), params.into_owned(), conn_id, MAX_RESPONSE, self.ext.clone());
                let (tx, running) = (self.tx.clone(), self.running.clone());
                running.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    send_response(&tx, fut.await).await;
                    running.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Some((_, MethodCallback::Subscription(cb))) => match self.dispatch.subscriptions.clone().try_acquire_owned() {
                Ok(permit) => {
                    let state = SubscriptionState { conn_id, id_provider: &*shared.ids, subscription_permit: permit };
                    // The callback starts the subscription's own task; what it
                    // hands back resolves once the subscription is accepted
                    // (its id already sent through the sink) or refused.
                    let accepting = (cb)(id, params, self.dispatch.sink.clone(), state, self.ext.clone());
                    self.accepting.push(accepting);
                }
                Err(_) => self.respond(MethodResponse::error(id, reject_too_many_subscriptions(MAX_SUBSCRIPTIONS))),
            },
            Some((_, MethodCallback::Unsubscription(cb))) => {
                let rp = (cb)(id, params, conn_id, MAX_RESPONSE, self.ext.clone());
                self.respond(rp);
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

    /// The next frame to hand the socket: the loop's own answers first; a
    /// call's first frame once the method table's queue holds nothing taken
    /// before it; then this side's queue, the method table's and the calls'
    /// frames, by turns.
    fn next_out(&mut self, cx: &mut Context<'_>) -> Option<Message> {
        if let Some(m) = self.out.pop_front() {
            return Some(m);
        }
        if self.fenced.is_some() {
            // A subscription's answer was queued on the method table before
            // its first frame was sent; seen that frame, the answer is seen
            // here too, so the table is emptied of what it holds first.
            // `try_recv`, not `poll_recv`: tokio's cooperative budget can
            // answer a poll `Pending` with a frame waiting.
            if let Ok(m) = self.table.try_recv() {
                return Some(text(m));
            }
            return self.fenced.take().map(framed);
        }
        const TURNS: [Queue; 3] = [Queue::Own, Queue::Table, Queue::Frames];
        for i in 0..TURNS.len() {
            let q = TURNS[(self.turn + i) % TURNS.len()];
            let got = match q {
                Queue::Own if self.own_open => match self.own.poll_recv(cx) {
                    Poll::Ready(Some(f)) => Some(own(f)),
                    Poll::Ready(None) => {
                        self.own_open = false;
                        None
                    }
                    Poll::Pending => None,
                },
                Queue::Own => None,
                Queue::Table => match self.table.poll_recv(cx) {
                    Poll::Ready(Some(m)) => Some(text(m)),
                    _ => None,
                },
                Queue::Frames => match self.frames.poll_recv(cx) {
                    Poll::Ready(Some(f)) if f.fenced() => {
                        self.turn = (self.turn + i + 1) % TURNS.len();
                        self.fenced = Some(f.into_bytes());
                        return self.next_out(cx);
                    }
                    Poll::Ready(Some(f)) => Some(framed(WireFrame::into_bytes(f))),
                    _ => None,
                },
            };
            if let Some(m) = got {
                self.turn = (self.turn + i + 1) % TURNS.len();
                return Some(m);
            }
        }
        None
    }

    /// Every ready frame, up to the budget, into the socket, then one flush.
    /// `Ok(true)`: something moved; `Err`: the socket is gone.
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<bool, ()> {
        let mut moved = false;
        while self.gathered < self.budget {
            match self.ws.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(_)) => return Err(()),
                // Its buffer is full and the socket is not taking it: wait.
                Poll::Pending => break,
            }
            let Some(m) = self.next_out(cx) else { break };
            self.gathered += m.len();
            self.ws.start_send_unpin(m).map_err(|_| ())?;
            self.unflushed = true;
            moved = true;
        }
        if self.unflushed {
            match self.ws.poll_flush_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    self.unflushed = false;
                    self.gathered = 0;
                    moved = true;
                }
                Poll::Ready(Err(_)) => return Err(()),
                Poll::Pending => {}
            }
        }
        Ok(moved)
    }

    fn poll_run(&mut self, cx: &mut Context<'_>) -> Poll<End> {
        if self.stopping.as_mut().poll(cx).is_ready() {
            return Poll::Ready(End::ServerStopped);
        }
        if self.closing.as_mut().poll(cx).is_ready() {
            return Poll::Ready(End::Closed);
        }
        let mut ticked = false;
        if let Some(t) = self.ticks.as_mut() {
            while t.poll_tick(cx).is_ready() {
                ticked = true;
            }
        }
        if ticked {
            if let Some(end) = self.liveness() {
                return Poll::Ready(end);
            }
        }
        if let Some(p) = self.ping.as_mut() {
            let mut due = false;
            while p.poll_tick(cx).is_ready() {
                due = true;
            }
            if due {
                // Ahead of anything queued: a busy socket still says it is alive.
                self.out.push_front(Message::Ping(Bytes::new()));
            }
        }
        let mut reads = 0;
        loop {
            let mut moved = false;
            if self.poll_accepting(cx) {
                moved = true;
            }
            // Frames in, while the answers are not backed up.
            if reads < READ_BUDGET && self.out.len() < OUT_HIGH {
                match self.ws.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(m))) => {
                        reads += 1;
                        moved = true;
                        if let Err(end) = self.message(m) {
                            return Poll::Ready(end);
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        use tokio_tungstenite::tungstenite::Error as E;
                        tracing::debug!(conn = self.conn.id(), peer = %self.conn.peer(), "WebSocket read: {e}");
                        return Poll::Ready(match e {
                            E::Protocol(_) | E::Utf8(_) | E::Capacity(_) | E::AttackAttempt => End::Protocol,
                            _ => End::Lost,
                        });
                    }
                    Poll::Ready(None) => return Poll::Ready(End::Lost),
                    Poll::Pending => {}
                }
                self.answer_drops();
            }
            // Frames out: every one ready, one flush.
            match self.poll_write(cx) {
                Ok(true) => moved = true,
                Ok(false) => {}
                Err(()) => return Poll::Ready(End::Lost),
            }
            if !moved {
                return Poll::Pending;
            }
            if reads >= READ_BUDGET {
                // Its share for this wakeup: the other sockets run, then it goes on.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }

    /// The socket ends: every call still writing learns it is gone, the close
    /// (or the reply to the peer's) goes out, and the peer's answer is read.
    async fn end(self, end: End) -> Option<Arc<dyn ConnectionHook>> {
        let Loop { mut ws, mut table, mut own, mut frames, accepting, tx, dispatch, ext, hook, .. } = self;
        table.close();
        own.close();
        frames.close();
        drop((table, own, frames, accepting, tx, dispatch, ext));
        let _ = tokio::time::timeout(CLOSE_WAIT, async {
            match end {
                End::Lost => {}
                End::PeerClosed => {
                    // The reply tungstenite queued when it read the close: a
                    // write after the peer's close is refused, a flush is not.
                    let _ = SinkExt::close(&mut ws).await;
                }
                End::Protocol => {
                    let _ = ws.send(Message::Close(end.frame())).await;
                }
                _ => {
                    let _ = ws.send(Message::Close(end.frame())).await;
                    while let Some(Ok(_)) = ws.next().await {}
                }
            }
        })
        .await;
        hook
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
    let limits = shared.limits.clone();
    let guard = Guard::new(io, limits.max_message_size);
    let dropped = guard.dropped();
    let config = WebSocketConfig::default()
        .max_message_size(Some(limits.max_message_size))
        .max_frame_size(Some(limits.max_message_size))
        // tungstenite zero-fills what a read may fill before each read: small,
        // for the calls' small frames (a large message is read in more steps).
        .read_buffer_size(512)
        // Above the budget, so the budget's frames meet one write.
        .write_buffer_size((limits.write_budget * 2).max(128 * 1024));
    let ws: Socket = WebSocketStream::from_raw_socket(guard, Role::Server, Some(config)).await;

    let (tx, table) = mpsc::channel::<Box<RawValue>>(limits.queue);
    let (own_tx, own_rx) = mpsc::channel(limits.queue);
    // PERF-7: the calls' frames, each item encoded once by plexus-core's pipe;
    // every call finds the sink in its extensions.
    let encoding = match negotiated.encoding {
        super::Encoding::Json => WireEncoding::Json,
    };
    let (frame_sink, frames) = FrameSink::channel(limits.queue, encoding);
    let mut ext = ext;
    ext.insert(frame_sink);
    conn.upgraded(negotiated, ConnectionWriter { tx: own_tx });
    let hook = shared.hooks.as_ref().and_then(|h| h.make(&conn, &ext));
    let dispatch = Dispatch {
        conn_id: ConnectionId(conn.id() as usize),
        sink: MethodSink::new_with_limit(tx.clone(), MAX_RESPONSE as u32),
        subscriptions: Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS as usize)),
        shared: shared.clone(),
    };
    let interval = |every: Duration| {
        let mut i = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        i
    };
    let ping_timeout = limits.ping_interval.map(|_| limits.ping_timeout);
    let tick = [ping_timeout, limits.idle_timeout].into_iter().flatten().min().map(|d| (d / 8).clamp(Duration::from_millis(25), Duration::from_secs(5)));
    let mut closed = conn.closed_watch();
    let now = Instant::now();
    let mut l = Loop {
        ws,
        conn: conn.clone(),
        ext,
        dispatch,
        tx,
        table,
        own: own_rx,
        own_open: true,
        frames,
        fenced: None,
        turn: 0,
        hook,
        accepting: Vec::new(),
        running: Arc::new(AtomicUsize::new(0)),
        dropped,
        answered_drops: 0,
        out: VecDeque::new(),
        gathered: 0,
        unflushed: false,
        budget: limits.write_budget,
        max_message: limits.max_message_size,
        ping: limits.ping_interval.map(interval),
        ping_timeout,
        idle: limits.idle_timeout,
        ticks: tick.map(interval),
        last_in: now,
        last_busy: now,
        stopping: Box::pin(stop.shutdown()),
        closing: Box::pin(async move {
            let _ = closed.wait_for(|c| *c).await;
        }),
    };
    let end = std::future::poll_fn(|cx| l.poll_run(cx)).await;
    tracing::debug!(conn = conn.id(), peer = %conn.peer(), "WebSocket closing: {end:?}");
    if let Some(hook) = l.end(end).await {
        hook.on_close();
    }
    conn.ended();
}
