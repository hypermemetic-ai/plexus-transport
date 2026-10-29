//! BIDIR-3 (spike, never merged): items INTO a live call on today's
//! jsonrpsee WebSocket — BIDIR-1's `wire` fork, "JSON-RPC methods addressed
//! to the call", built by hand so it can be measured.
//!
//! The frames are BIDIR-6's:
//!
//! ```text
//! → {"jsonrpc":"2.0","id":7,"method":"{ns}.duplex","params":{...}}          the call, as a subscription
//! ← {"jsonrpc":"2.0","id":7,"result":21}                                   its subscription id
//! ← {"method":"{ns}.duplex","params":{"subscription":21,"result":{"type":"credit","n":64}}}
//! → {"jsonrpc":"2.0","method":"{ns}.input","params":{"subscription":21,"seq":0,"items":[…]}}   a notification
//! ← {"method":"{ns}.duplex","params":{"subscription":21,"result":{"type":"ack","seq":0}}}
//! → {"jsonrpc":"2.0","method":"{ns}.end","params":{"subscription":21,"seq":1}}
//! ← … {"type":"data","content":…} … {"type":"done"}
//! ```
//!
//! What it found it had to add, each a line of the result on BIDIR-3:
//!
//! - jsonrpsee answers a notification with nothing and **dispatches it to
//!   nothing** (`RpcService::notification` is a no-op), so `{ns}.input` as a
//!   notification only reaches the call through an RPC middleware
//!   ([`DuplexLayer`]) — the transport's, beside the method table.
//! - jsonrpsee **spawns a task per frame**, so two frames sent in order may
//!   be handled out of order: order is `seq`'s, reassembled per call
//!   ([`Slot`]'s `pending`), never the socket's. `{ns}.end` races the last
//!   `{ns}.input` the same way, so it names the `seq` it follows.
//! - a call is keyed by (connection, subscription): another socket naming
//!   the same subscription id reaches nothing.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use futures::future::{BoxFuture, FutureExt};
use futures::stream::{BoxStream, Stream, StreamExt};
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::server::{ConnectionId, MethodResponse};
use jsonrpsee::types::Request;
use jsonrpsee::{PendingSubscriptionSink, RpcModule, SubscriptionMessage};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// What a duplex method is: params and its input, to a stream of output values.
pub type DuplexHandler = Arc<dyn Fn(Value, InputStream) -> BoxStream<'static, Value> + Send + Sync>;

/// How the hub orders what arrives (the spike measures both).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ordering_ {
    /// Frames delivered by `seq` (the design).
    BySeq,
    /// Frames delivered as jsonrpsee hands them over (the measurement of
    /// what the socket alone does).
    Arrival,
}

#[derive(Default, Debug)]
pub struct Stats {
    /// Frames for a (connection, subscription) that holds no call.
    pub unknown: AtomicU64,
    /// Frames dropped as at or below the delivered seq (a resend).
    pub duplicates: AtomicU64,
    /// Frames after the input half closed.
    pub after_close: AtomicU64,
    /// The most items the hub held for any one call, at once.
    pub max_held: AtomicU64,
    /// Calls open now.
    pub open: AtomicU64,
}

/// The per-server table of live duplex calls.
pub struct DuplexRegistry {
    pub ns: String,
    pub input_method: String,
    pub end_method: String,
    pub window: u64,
    pub ordering: Ordering_,
    slots: Mutex<HashMap<(usize, String), Arc<Slot>>>,
    pub stats: Stats,
}

impl std::fmt::Debug for DuplexRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplexRegistry").field("ns", &self.ns).finish()
    }
}

enum Control {
    Item(Value),
    Abort(Value),
}

struct SlotState {
    next_seq: u64,
    pending: BTreeMap<u64, Vec<Value>>,
    end_at: Option<u64>,
    input: Option<mpsc::UnboundedSender<Value>>,
    received: u64,
    consumed: u64,
    granted: u64,
    aborted: bool,
}

/// One live call's input half.
pub struct Slot {
    st: Mutex<SlotState>,
    ctl: mpsc::UnboundedSender<Control>,
    window: u64,
    ordering: Ordering_,
}

impl Slot {
    fn held(st: &SlotState) -> u64 {
        st.received - st.consumed
    }

    /// The method took an item: grant credit back in half-windows, with the ack.
    fn consumed(&self, reg: &DuplexRegistry) {
        let mut st = self.st.lock().unwrap();
        st.consumed += 1;
        let ceiling = st.consumed + self.window;
        let n = ceiling - st.granted;
        if n >= (self.window / 2).max(1) && !st.aborted {
            st.granted = ceiling;
            let ack = st.next_seq.checked_sub(1);
            let _ = self.ctl.send(Control::Item(json!({"type": "ack", "seq": ack})));
            let _ = self.ctl.send(Control::Item(json!({"type": "credit", "n": n})));
        }
        let _ = reg;
    }

    fn input(&self, reg: &DuplexRegistry, seq: u64, items: Vec<Value>) {
        let mut st = self.st.lock().unwrap();
        if st.aborted || (st.input.is_none() && st.end_at.is_none()) || st.input.is_none() {
            reg.stats.after_close.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.ordering == Ordering_::BySeq && (seq < st.next_seq || st.pending.contains_key(&seq)) {
            reg.stats.duplicates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        st.received += items.len() as u64;
        if st.received > st.granted {
            st.aborted = true;
            st.input = None;
            let _ = self.ctl.send(Control::Abort(json!({
                "type": "error", "code": "credit_exceeded",
                "message": format!("seq {seq}: {} items past a grant of {}", st.received - st.granted, st.granted),
            })));
            return;
        }
        let held = Self::held(&st);
        reg.stats.max_held.fetch_max(held, Ordering::Relaxed);
        match self.ordering {
            Ordering_::Arrival => {
                if let Some(tx) = &st.input {
                    for it in items {
                        let _ = tx.send(it);
                    }
                }
                st.next_seq = st.next_seq.max(seq + 1);
            }
            Ordering_::BySeq => {
                st.pending.insert(seq, items);
                loop {
                    let next = st.next_seq;
                    let Some(batch) = st.pending.remove(&next) else { break };
                    if let Some(tx) = &st.input {
                        for it in batch {
                            let _ = tx.send(it);
                        }
                    }
                    st.next_seq += 1;
                }
            }
        }
        if st.end_at.is_some_and(|e| st.next_seq >= e) {
            st.input = None;
        }
    }

    fn end(&self, seq: Option<u64>) {
        let mut st = self.st.lock().unwrap();
        match seq {
            Some(s) if st.next_seq < s && self.ordering == Ordering_::BySeq => st.end_at = Some(s),
            _ => {
                st.end_at = Some(st.next_seq);
                st.input = None;
            }
        }
    }

    fn input_open(&self) -> bool {
        self.st.lock().unwrap().input.is_some()
    }

    fn last_seq(&self) -> Option<u64> {
        self.st.lock().unwrap().next_seq.checked_sub(1)
    }
}

/// The method's view of its input half: the items in order, then the end.
pub struct InputStream {
    rx: mpsc::UnboundedReceiver<Value>,
    slot: Arc<Slot>,
    reg: Arc<DuplexRegistry>,
}

impl Stream for InputStream {
    type Item = Value;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Value>> {
        let p = self.rx.poll_recv(cx);
        if let Poll::Ready(Some(_)) = &p {
            let (slot, reg) = (self.slot.clone(), self.reg.clone());
            slot.consumed(&reg);
        }
        p
    }
}

fn sid_key(v: &Value) -> String {
    v.to_string()
}

impl DuplexRegistry {
    pub fn new(ns: &str, window: u64, ordering: Ordering_) -> Arc<Self> {
        Arc::new(Self {
            ns: ns.into(),
            input_method: format!("{ns}.input"),
            end_method: format!("{ns}.end"),
            window,
            ordering,
            slots: Mutex::new(HashMap::new()),
            stats: Stats::default(),
        })
    }

    pub fn open_calls(&self) -> usize {
        self.slots.lock().unwrap().len()
    }

    fn slot(&self, conn: usize, sub: &Value) -> Option<Arc<Slot>> {
        let s = self.slots.lock().unwrap().get(&(conn, sid_key(sub))).cloned();
        if s.is_none() {
            self.stats.unknown.fetch_add(1, Ordering::Relaxed);
        }
        s
    }

    /// `{ns}.input {subscription, seq, items}` or `{ns}.end {subscription, seq?}` from `conn`.
    pub fn handle(&self, method: &str, conn: usize, params: &Value) -> bool {
        let Some(sub) = params.get("subscription") else { return false };
        let Some(slot) = self.slot(conn, sub) else { return false };
        let seq = params.get("seq").and_then(Value::as_u64);
        if method == self.input_method {
            let items = params.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
            slot.input(self, seq.unwrap_or(0), items);
        } else {
            slot.end(seq);
        }
        true
    }

    /// Register a duplex method as `{ns}.<name>`: a subscription whose
    /// output carries `credit`, `ack`, `data`, `input_closed`, `error`, `done`.
    pub fn register(
        self: &Arc<Self>,
        module: &mut RpcModule<()>,
        name: &str,
        handler: DuplexHandler,
    ) -> Result<(), jsonrpsee::core::RegisterMethodError> {
        let method: &'static str = Box::leak(format!("{}.{name}", self.ns).into_boxed_str());
        let unsub: &'static str = Box::leak(format!("{}.{name}_unsub", self.ns).into_boxed_str());
        let reg = self.clone();
        module.register_subscription(method, method, unsub, move |params, pending: PendingSubscriptionSink, _, _| {
            let reg = reg.clone();
            let handler = handler.clone();
            async move {
                let p: Value = params.parse().unwrap_or(Value::Null);
                let sink = pending.accept().await?;
                let conn = sink.connection_id().0;
                let sid = serde_json::to_value(sink.subscription_id())?;
                let (in_tx, in_rx) = mpsc::unbounded_channel();
                let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel();
                let slot = Arc::new(Slot {
                    st: Mutex::new(SlotState {
                        next_seq: 0,
                        pending: BTreeMap::new(),
                        end_at: None,
                        input: Some(in_tx),
                        received: 0,
                        consumed: 0,
                        granted: reg.window,
                        aborted: false,
                    }),
                    ctl: ctl_tx,
                    window: reg.window,
                    ordering: reg.ordering,
                });
                reg.slots.lock().unwrap().insert((conn, sid_key(&sid)), slot.clone());
                reg.stats.open.fetch_add(1, Ordering::Relaxed);
                let input = InputStream { rx: in_rx, slot: slot.clone(), reg: reg.clone() };
                let mut out = handler(p, input);
                let send = |v: Value| SubscriptionMessage::from(serde_json::value::to_raw_value(&v).unwrap());
                let mut result = sink.send(send(json!({"type": "credit", "n": reg.window}))).await;
                while result.is_ok() {
                    tokio::select! {
                        biased;
                        _ = sink.closed() => break,
                        c = ctl_rx.recv() => match c {
                            Some(Control::Item(v)) => result = sink.send(send(v)).await,
                            Some(Control::Abort(v)) => { let _ = sink.send(send(v)).await; break }
                            None => break,
                        },
                        o = out.next() => match o {
                            Some(v) => result = sink.send(send(json!({"type": "data", "content": v}))).await,
                            None => {
                                // Credit and acks queued before the end go out first.
                                while let Ok(Control::Item(v)) = ctl_rx.try_recv() { let _ = sink.send(send(v)).await; }
                                if slot.input_open() {
                                    let _ = sink.send(send(json!({"type": "input_closed", "reason": "method_ended", "ack": slot.last_seq()}))).await;
                                    slot.st.lock().unwrap().input = None;
                                }
                                let _ = sink.send(send(json!({"type": "done"}))).await;
                                break;
                            }
                        },
                    }
                }
                {
                    let mut st = slot.st.lock().unwrap();
                    st.input = None;
                    st.aborted = true;
                }
                reg.slots.lock().unwrap().remove(&(conn, sid_key(&sid)));
                reg.stats.open.fetch_sub(1, Ordering::Relaxed);
                drop(out);
                Ok(())
            }
        })?;
        // The same frames as requests with an id (answered `true`), for a
        // client that cannot send a notification; jsonrpsee dispatches these.
        for m in [self.input_method.clone(), self.end_method.clone()] {
            let reg = self.clone();
            let name: &'static str = Box::leak(m.clone().into_boxed_str());
            if module.method(name).is_some() {
                continue;
            }
            module.register_method(name, move |params, _, ext| {
                let p: Value = params.parse().unwrap_or(Value::Null);
                let conn = ext.get::<ConnectionId>().map(|c| c.0).unwrap_or(usize::MAX);
                reg.handle(name, conn, &p)
            })?;
        }
        Ok(())
    }
}

/// The RPC middleware that hands `{ns}.input` / `{ns}.end` notifications to
/// the registry; everything else passes through untouched.
#[derive(Clone, Default)]
pub struct DuplexLayer(pub Option<Arc<DuplexRegistry>>);

impl<S> tower::Layer<S> for DuplexLayer {
    type Service = DuplexService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        DuplexService { inner, reg: self.0.clone() }
    }
}

#[derive(Clone)]
pub struct DuplexService<S> {
    inner: S,
    reg: Option<Arc<DuplexRegistry>>,
}

impl<S> RpcServiceT for DuplexService<S>
where
    S: RpcServiceT<MethodResponse = MethodResponse, NotificationResponse = MethodResponse, BatchResponse = MethodResponse>
        + Send
        + Sync
        + Clone
        + 'static,
{
    type MethodResponse = MethodResponse;
    type NotificationResponse = MethodResponse;
    type BatchResponse = MethodResponse;

    fn call<'a>(&self, request: Request<'a>) -> impl std::future::Future<Output = MethodResponse> + Send + 'a {
        self.inner.call(request)
    }

    fn batch<'a>(&self, requests: Batch<'a>) -> impl std::future::Future<Output = MethodResponse> + Send + 'a {
        self.inner.batch(requests)
    }

    fn notification<'a>(&self, n: Notification<'a>) -> impl std::future::Future<Output = MethodResponse> + Send + 'a {
        let fut: BoxFuture<'a, MethodResponse> = match &self.reg {
            Some(reg) if n.method == reg.input_method || n.method == reg.end_method => {
                let conn = n.extensions.get::<ConnectionId>().map(|c| c.0).unwrap_or(usize::MAX);
                let params: Value =
                    n.params.as_ref().and_then(|p| serde_json::from_str(p.get()).ok()).unwrap_or(Value::Null);
                reg.handle(&n.method, conn, &params);
                futures::future::ready(MethodResponse::notification()).boxed()
            }
            _ => self.inner.notification(n).boxed(),
        };
        fut
    }
}

/// A clock both ends of a one-process measurement share.
pub fn epoch() -> Instant {
    static E: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *E.get_or_init(Instant::now)
}
