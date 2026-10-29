//! LOCAL-3 (spike, never merged): **a connection is symmetric**. Once the
//! upgrade is done, either peer may open a call on it with the same frames.
//! This is the accepting service's half: a peer that dialed in and
//! **offered** a surface (`{ns}.offer`) is called on its own socket with
//! `{peer}.call` — a request this server writes on a connection it accepted,
//! answered by the peer's own gate.
//!
//! What it takes from jsonrpsee: nothing it has. jsonrpsee-server 0.26 can
//! neither write a request on an accepted socket (there is no raw sink) nor
//! receive the response to one (a frame with no `method` is answered
//! `-32600` before any middleware sees it). The vendored copy adds one hook,
//! `jsonrpsee_server::ws::FrameHook`: run over each inbound frame in the
//! read loop, before dispatch, handed the connection's raw `MethodSink`.
//! Everything else here is plexus-client's own [`WsTransport`] — the call
//! table, the frames, the cancellation — writing through that sink
//! ([`WsTransport::over`]).
//!
//! The rules it holds:
//!
//! | when | then |
//! |---|---|
//! | a peer offers | its offer (namespace, activations, schema hash) is recorded on its connection, admitted by [`Admit`] (LOCAL-5: a grant on the mount name), and named by its mount name |
//! | the service calls a peer that offered nothing | `no_offered_surface`, here, and no frame is sent |
//! | the connection drops | every call open on it ends with one `closed` error, the mount is gone, nothing is kept |
//! | a connection that never offers | its frames are jsonrpsee's, untouched (the hook passes every frame) |

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use futures::StreamExt;
use jsonrpsee::core::server::MethodSink;
use jsonrpsee::server::ws::FrameHook;
use jsonrpsee::RpcModule;
use plexus_client::{ItemStream, StreamItem, TransportError, WsTransport};
use plexus_core::plexus::AuthContext;
use serde_json::{json, Value};

/// Who may offer under a mount name — LOCAL-5's `AcceptPolicy`, as BIDIR-7
/// put it: `Gate::check(peer, "service.accept", <mount name>)`.
pub type Admit = Arc<dyn Fn(Option<&AuthContext>, &str) -> Result<(), String> + Send + Sync>;

/// What a peer offered, kept on its connection.
#[derive(Debug, Clone)]
pub struct OfferRecord {
    pub namespace: String,
    pub activations: Vec<String>,
    pub schema_hash: String,
    /// Kept only with [`Peers::keep_schema`]; the hash is enough to know it changed.
    pub schema: Option<Value>,
    pub principal: Option<String>,
}

/// One accepted connection, as a peer.
pub struct PeerConn {
    id: u64,
    sink: OnceLock<MethodSink>,
    writer: OnceLock<tokio::sync::mpsc::UnboundedSender<String>>,
    transport: OnceLock<WsTransport>,
    offer: Mutex<Option<OfferRecord>>,
    peers: std::sync::Weak<Peers>,
    /// Frames the hook took (responses, items of this side's calls).
    pub taken: AtomicU64,
}

impl std::fmt::Debug for PeerConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConn").field("id", &self.id).field("offer", &self.offer).finish()
    }
}

impl PeerConn {
    /// The offer, if the peer made one.
    pub fn offer(&self) -> Option<OfferRecord> {
        self.offer.lock().unwrap().clone()
    }

    /// The transport into the peer (once it offered).
    pub fn transport(&self) -> Option<&WsTransport> {
        self.transport.get()
    }

    /// Open a call into the peer's offered surface, `hop` naming whom this
    /// service acts for. A peer that offered nothing: `no_offered_surface`,
    /// and nothing is sent.
    pub fn call(&self, method: &str, params: Value, hop: Option<Value>) -> ItemStream<'_> {
        match self.transport.get() {
            Some(t) => t.call_with_hop(method, params, hop),
            None => Box::pin(futures::stream::once(async { Ok(no_offered_surface()) })),
        }
    }

    fn writer(&self) -> Option<plexus_client::ws::Writer> {
        let tx = self.writer.get()?.clone();
        Some(Arc::new(move |text: String| {
            tx.send(text).map_err(|_| TransportError::closed("connection_lost: the peer's socket is gone"))
        }))
    }

    /// The hook: the first frame hands this connection its raw sink; after
    /// an offer, responses and items of this side's calls are taken here,
    /// in arrival order.
    fn on_frame(self: &Arc<Self>, bytes: &[u8], sink: &MethodSink) -> bool {
        if self.sink.get().is_none() {
            let _ = self.sink.set(sink.clone());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            let _ = self.writer.set(tx);
            let out = sink.clone();
            let me = self.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        m = rx.recv() => match m {
                            Some(text) => {
                                let raw = match serde_json::value::RawValue::from_string(text) { Ok(r) => r, Err(_) => continue };
                                if out.send(raw).await.is_err() { break }
                            }
                            None => break,
                        },
                        _ = out.closed() => break,
                    }
                }
                me.closed();
            });
        }
        match self.transport.get() {
            Some(t) => {
                let took = t.take_frame(bytes);
                if took {
                    self.taken.fetch_add(1, Ordering::Relaxed);
                }
                took
            }
            None => false,
        }
    }

    /// The socket is gone: every call this side opened on it ends
    /// (`closed`), the mount goes.
    fn closed(&self) {
        if let Some(t) = self.transport.get() {
            t.on_close("connection_lost: the peer's socket closed");
        }
        if let Some(peers) = self.peers.upgrade() {
            peers.forget(self.id);
        }
    }
}

/// The item a call into a peer that offered nothing gets.
pub fn no_offered_surface() -> StreamItem {
    StreamItem::Error {
        metadata: None,
        message: "no_offered_surface: the peer offered nothing on this connection".into(),
        code: Some("no_offered_surface".into()),
        recoverable: false,
    }
}

/// The service's peers: every accepted connection, and those that offered,
/// by mount name.
pub struct Peers {
    ns: String,
    next: AtomicU64,
    conns: Mutex<HashMap<u64, Arc<PeerConn>>>,
    mounted: Mutex<HashMap<String, Arc<PeerConn>>>,
    admit: Option<Admit>,
    /// Keep each offer's schema (else the hash alone).
    pub keep_schema: bool,
    /// The principal this service names itself as in an offer's answer.
    pub principal: Option<String>,
}

impl std::fmt::Debug for Peers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peers").field("ns", &self.ns).finish()
    }
}

impl Peers {
    pub fn new(ns: &str) -> Arc<Self> {
        Self::with(ns, None, false, None)
    }

    pub fn with(ns: &str, admit: Option<Admit>, keep_schema: bool, principal: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            ns: ns.into(),
            next: AtomicU64::new(1),
            conns: Default::default(),
            mounted: Default::default(),
            admit,
            keep_schema,
            principal,
        })
    }

    /// One accepted connection: its hook, and the peer it is (both go in
    /// the upgrade request's extensions).
    pub fn accept(self: &Arc<Self>) -> (FrameHook, Arc<PeerConn>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let conn = Arc::new(PeerConn {
            id,
            sink: OnceLock::new(),
            writer: OnceLock::new(),
            transport: OnceLock::new(),
            offer: Mutex::new(None),
            peers: Arc::downgrade(self),
            taken: AtomicU64::new(0),
        });
        self.conns.lock().unwrap().insert(id, conn.clone());
        let c = conn.clone();
        (FrameHook(Arc::new(move |bytes, sink| c.on_frame(bytes, sink))), conn)
    }

    fn forget(&self, id: u64) {
        self.conns.lock().unwrap().remove(&id);
        self.mounted.lock().unwrap().retain(|_, c| c.id != id);
    }

    /// The peer mounted under `name`.
    pub fn get(&self, name: &str) -> Option<Arc<PeerConn>> {
        self.mounted.lock().unwrap().get(name).cloned()
    }

    /// Open a call into the peer mounted as `name` (`child_unavailable` when
    /// none is: LOCAL-5's code for a mount whose peer is gone).
    pub fn call(&self, name: &str, method: &str, params: Value, hop: Option<Value>) -> ItemStream<'static> {
        match self.get(name) {
            Some(peer) => {
                let method = method.to_string();
                Box::pin(async_stream::stream! {
                    let mut s = peer.call(&method, params, hop);
                    while let Some(i) = s.next().await { yield i; }
                })
            }
            None => {
                let name = name.to_string();
                Box::pin(futures::stream::once(async move {
                    Ok(StreamItem::Error {
                        metadata: None,
                        message: format!("child_unavailable: no peer is mounted as `{name}`"),
                        code: Some("child_unavailable".into()),
                        recoverable: false,
                    })
                }))
            }
        }
    }

    /// (accepted connections, mounted peers) — 0 and 0 once all are gone.
    pub fn counts(&self) -> (usize, usize) {
        (self.conns.lock().unwrap().len(), self.mounted.lock().unwrap().len())
    }

    /// Register `{ns}.offer {namespace, activations, schema_hash, schema}`.
    pub fn register(self: &Arc<Self>, module: &mut RpcModule<()>) -> Result<(), jsonrpsee::core::RegisterMethodError> {
        let name: &'static str = Box::leak(format!("{}.offer", self.ns).into_boxed_str());
        let me = self.clone();
        module.register_method(name, move |params, _, ext| {
            let p: Value = params.parse().unwrap_or(Value::Null);
            let Some(conn) = ext.get::<Arc<PeerConn>>().cloned() else {
                return Err(jsonrpsee::types::ErrorObject::owned(-32601, "offers are not taken on this server", None::<()>));
            };
            let auth = ext.get::<Arc<AuthContext>>().map(|a| a.as_ref().clone());
            me.offer(&conn, auth.as_ref(), &p)
        })?;
        Ok(())
    }

    fn offer(&self, conn: &Arc<PeerConn>, auth: Option<&AuthContext>, p: &Value) -> Result<Value, jsonrpsee::types::ErrorObjectOwned> {
        let refuse = |kind: &str, msg: String| {
            jsonrpsee::types::ErrorObject::owned(-32003, msg, Some(json!({"error_kind": kind})))
        };
        let namespace = p.get("namespace").and_then(Value::as_str).unwrap_or_default().to_string();
        if namespace.is_empty() {
            return Err(refuse("invalid_params", "an offer names its namespace".into()));
        }
        // The mount name is the offered namespace (LOCAL-5 may map it).
        let mount = namespace.clone();
        if let Some(admit) = &self.admit {
            admit(auth, &mount).map_err(|why| refuse("forbidden", format!("forbidden: may not offer as `{mount}`: {why}")))?;
        }
        let principal = auth.and_then(|a| a.principal().map(|p| p.to_string()));
        if let Some(other) = self.get(&mount) {
            let same = other.offer().and_then(|o| o.principal) == principal && principal.is_some();
            if other.id != conn.id && !same {
                return Err(refuse("name_taken", format!("name_taken: `{mount}` is offered on another connection")));
            }
        }
        let Some(writer) = conn.writer() else {
            return Err(refuse("internal", "the connection has no sink".into()));
        };
        let schema_hash = p.get("schema_hash").and_then(Value::as_str).unwrap_or_default().to_string();
        let record = OfferRecord {
            namespace: namespace.clone(),
            activations: p.get("activations").and_then(Value::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default(),
            schema_hash: schema_hash.clone(),
            schema: self.keep_schema.then(|| p.get("schema").cloned().unwrap_or(Value::Null)),
            principal,
        };
        let _ = conn.transport.set(WsTransport::over(&namespace, writer));
        *conn.offer.lock().unwrap() = Some(record);
        // A reconnect by the same principal takes the name over.
        self.mounted.lock().unwrap().insert(mount.clone(), conn.clone());
        Ok(json!({"offered": namespace, "mount": mount, "schema_hash": schema_hash, "service": {"namespace": self.ns, "principal": self.principal}}))
    }
}
