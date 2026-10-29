//! LOCAL-3 — a connection is symmetric. Service A (`svc`, plexus-transport
//! + plexus-core) accepts; peer B (`box2`, its own DynamicHub behind its own
//! gate) dials in with plexus-client's `WsTransport`, proves a credential,
//! and **offers** its `ci` activation. Then A calls B on B's socket with the
//! frames B uses to call A, and B's gate decides — as A, for whom A acts.
//!
//! Every test runs over a real WebSocket on 127.0.0.1 with an OS-handed port.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use futures::stream::{BoxStream, Stream, StreamExt};
use plexus_client::offer::{Offered, ServedCall};
use plexus_client::{ClientError, ItemStream, MethodRef, StreamItem, Transport, WsTransport};
use plexus_core::plexus::{AuthContext, Gate, Membership, Memberships, Resolve, RoleTable, ScopeRef, ServiceOffer, SessionValidator};
use plexus_core::DynamicHub;
use plexus_transport::duplex_spike::{DuplexHandler, DuplexRegistry, Ordering_};
use plexus_transport::symmetric_spike::{Admit, Peers};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const A_KEY: &str = "key:aaaa000000000000000000000000000000000000000000000000000000000000";
const B_KEY: &str = "key:bbbb000000000000000000000000000000000000000000000000000000000000";
const C_KEY: &str = "key:cccc000000000000000000000000000000000000000000000000000000000000";
const ADA: &str = "key:ada0000000000000000000000000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// The two services' activations.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelayEvent {
    Item { content: Value },
    Refused { code: Option<String>, message: String },
    Tick { n: u64 },
    Error { message: String },
}
impl plexus_client::FromClientError for RelayEvent {
    fn from_client_error(error: ClientError) -> Self {
        RelayEvent::Error { message: error.to_string() }
    }
}

/// A — calls a peer that dialed in, for its own caller.
#[derive(Clone)]
pub struct RelayActivation {
    peers: Arc<Peers>,
}

#[plexus_macros::activation(version = "1.0.0", description = "LOCAL-3 A: calls into a peer that dialed in")]
impl RelayActivation {
    /// Call `method` on the peer mounted as `peer`, for the caller.
    #[plexus_macros::method(streaming)]
    pub async fn ask(&self, auth: Option<&AuthContext>, peer: String, method: String, n: Option<u64>) -> impl Stream<Item = RelayEvent> + Send + 'static {
        let for_: Vec<String> = auth.and_then(|a| a.principal()).map(|p| vec![p.to_string()]).unwrap_or_default();
        let hop = json!({"principal": A_KEY, "for": for_, "limit": null});
        let params = n.map(|n| json!({"n": n})).unwrap_or(json!({}));
        let s = self.peers.call(&peer, &method, params, Some(hop));
        stream! {
            for await i in s {
                match i {
                    Ok(StreamItem::Data { content, .. }) => yield RelayEvent::Item { content },
                    Ok(StreamItem::Error { message, code, .. }) => { yield RelayEvent::Refused { code, message }; break }
                    Ok(StreamItem::Done { .. }) => break,
                    Ok(_) => {}
                    Err(e) => { yield RelayEvent::Refused { code: Some("transport".into()), message: e.to_string() }; break }
                }
            }
        }
    }

    /// `n` ticks — the forward direction's baseline.
    #[plexus_macros::method(streaming)]
    pub async fn count(&self, n: u64) -> impl Stream<Item = RelayEvent> + Send + 'static {
        stream! { for i in 0..n { yield RelayEvent::Tick { n: i }; } }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CiEvent {
    Who { principal: Option<String>, acting_for: Vec<String> },
    Tick { n: u64 },
    Secret { value: String },
    Error { message: String },
}
impl plexus_client::FromClientError for CiEvent {
    fn from_client_error(error: ClientError) -> Self {
        CiEvent::Error { message: error.to_string() }
    }
}

struct DropCount(Arc<AtomicU64>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// B — what the peer offers.
#[derive(Clone)]
pub struct CiActivation {
    dropped: Arc<AtomicU64>,
}

#[plexus_macros::activation(
    version = "1.0.0",
    description = "LOCAL-3 B: the offered surface",
    roles(viewer = ["whoami", "count", "ticks"], editor = ["secret"]),
)]
impl CiActivation {
    /// Who the gate let in, and for whom.
    #[plexus_macros::method]
    pub async fn whoami(&self, auth: Option<&AuthContext>) -> impl Stream<Item = CiEvent> + Send + 'static {
        let principal = auth.and_then(|a| a.principal()).map(|p| p.to_string());
        let acting_for = auth
            .and_then(|a| a.metadata.get("for").and_then(Value::as_array).cloned())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        stream! { yield CiEvent::Who { principal, acting_for }; }
    }

    /// `n` ticks.
    #[plexus_macros::method(streaming)]
    pub async fn count(&self, n: u64) -> impl Stream<Item = CiEvent> + Send + 'static {
        stream! { for i in 0..n { yield CiEvent::Tick { n: i }; } }
    }

    /// Ticks every 2 ms until dropped; counts its drops.
    #[plexus_macros::method(streaming)]
    pub async fn ticks(&self) -> impl Stream<Item = CiEvent> + Send + 'static {
        let dropped = self.dropped.clone();
        stream! {
            let _guard = DropCount(dropped);
            let mut n = 0;
            loop { yield CiEvent::Tick { n }; n += 1; tokio::time::sleep(Duration::from_millis(2)).await; }
        }
    }

    /// An editor's method: the peer A holds viewer only.
    #[plexus_macros::method]
    pub async fn secret(&self) -> impl Stream<Item = CiEvent> + Send + 'static {
        stream! { yield CiEvent::Secret { value: "s3cret".into() }; }
    }
}

/// B's second activation — callable by B's gate, but not offered.
#[derive(Clone)]
pub struct AdminActivation;

#[plexus_macros::activation(version = "1.0.0", description = "LOCAL-3 B: not offered", roles(viewer = ["wipe"]))]
impl AdminActivation {
    /// Never reachable through the offer.
    #[plexus_macros::method]
    pub async fn wipe(&self) -> impl Stream<Item = CiEvent> + Send + 'static {
        stream! { yield CiEvent::Secret { value: "wiped".into() }; }
    }
}

// ---------------------------------------------------------------------------
// Gates, validators, the two sides.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Members(std::sync::Mutex<Vec<Membership>>);
impl Members {
    fn add(&self, who: &str, role: &str, scope: &str) {
        self.0.lock().unwrap().push(Membership { principal: who.parse().unwrap(), role: plexus_auth_core::RoleName::new(role), scope: ScopeRef::parse(scope) });
    }
}
impl Memberships for Members {
    fn held(&self, who: &plexus_auth_core::identity::Principal) -> Vec<Membership> {
        self.0.lock().unwrap().iter().filter(|m| &m.principal == who).cloned().collect()
    }
}

/// Any target → [that name, *].
struct Named;
impl Resolve for Named {
    fn chain(&self, target: &str) -> Option<Vec<ScopeRef>> {
        Some(vec![ScopeRef::named(target), ScopeRef::Everywhere])
    }
}

fn who(p: &str) -> AuthContext {
    AuthContext::for_principal(&p.parse().unwrap(), "s1", vec![], json!({}))
}

/// B's own service, behind B's own gate: A holds `viewer` on `*`.
fn box2_hub(dropped: Arc<AtomicU64>) -> Arc<DynamicHub> {
    let plain = || DynamicHub::new("box2").register(CiActivation { dropped: dropped.clone() }).register(AdminActivation);
    let members = Arc::new(Members::default());
    members.add(A_KEY, "viewer", "*");
    let declared = plain().declared_roles();
    let roles = RoleTable::from_lists(declared.iter().map(|(r, g)| (r.as_str(), g.iter().map(String::as_str))));
    Arc::new(plain().with_gate(Arc::new(Gate::new(members, Arc::new(roles), Arc::new(Named)))))
}

/// B offers `ci`, serving calls as `peer` (what B pinned for the address it
/// dialed; `None`: anonymous).
fn box2_offer(peer: Option<&str>, dropped: Arc<AtomicU64>) -> Arc<dyn Offered> {
    Arc::new(ServiceOffer::new(box2_hub(dropped), &["ci"], peer.map(who)))
}

/// A peer whose `ci.sum` takes an input half — the reverse duplex call.
struct SumOffer;
impl Offered for SumOffer {
    fn namespace(&self) -> &str {
        "box3"
    }
    fn activations(&self) -> Vec<String> {
        vec!["ci".into()]
    }
    fn has_input(&self, method: &str) -> bool {
        method == "ci.sum"
    }
    fn serve(&self, call: ServedCall) -> BoxStream<'static, Value> {
        let mut input = call.input.expect("ci.sum takes input");
        stream! {
            let (mut count, mut in_order, mut last) = (0u64, true, None::<u64>);
            while let Some(v) = input.next().await {
                let i = v["i"].as_u64().unwrap_or(u64::MAX);
                if last.is_some_and(|l| i != l + 1) || (last.is_none() && i != 0) { in_order = false; }
                last = Some(i);
                count += 1;
            }
            yield json!({"type": "data", "content": {"count": count, "in_order": in_order}});
            yield json!({"type": "done"});
        }
        .boxed()
    }
}

/// The forward duplex method (BIDIR-3's route): the same counting, on A.
fn sum_handler() -> DuplexHandler {
    Arc::new(|_p, input| {
        stream! {
            let mut input = input;
            let (mut count, mut in_order, mut last) = (0u64, true, None::<u64>);
            while let Some(v) = input.next().await {
                let i = v["i"].as_u64().unwrap_or(u64::MAX);
                if last.is_some_and(|l| i != l + 1) || (last.is_none() && i != 0) { in_order = false; }
                last = Some(i);
                count += 1;
            }
            yield json!({"count": count, "in_order": in_order});
        }
        .boxed()
    })
}

#[derive(Clone)]
struct Tokens;
#[async_trait::async_trait]
impl SessionValidator for Tokens {
    async fn validate(&self, input: &str) -> Option<AuthContext> {
        let p = match input {
            "tok-b" => B_KEY,
            "tok-c" => C_KEY,
            "tok-ada" => ADA,
            _ => return None,
        };
        Some(who(p))
    }
}

struct Svc {
    url: String,
    peers: Arc<Peers>,
    _h: jsonrpsee::server::ServerHandle,
}

#[derive(Default)]
struct SvcOpts {
    admit: Option<Admit>,
    keep_schema: bool,
    sequential: bool,
    no_peers: bool,
}

async fn svc(o: SvcOpts) -> Svc {
    let peers = Peers::with("svc", o.admit, o.keep_schema, Some(A_KEY.into()));
    let hub = Arc::new(DynamicHub::new("svc").register(RelayActivation { peers: peers.clone() }));
    let mut module = DynamicHub::arc_into_rpc_module(hub).unwrap();
    peers.register(&mut module).unwrap();
    let duplex = DuplexRegistry::new("svc", 64, Ordering_::BySeq);
    duplex.register(&mut module, "sum", sum_handler()).unwrap();
    let opts = UpgradeOptions {
        anonymous_view: true,
        peers: (!o.no_peers).then(|| peers.clone()),
        duplex: Some(duplex),
        sequential_ids: o.sequential,
        ..UpgradeOptions::default()
    };
    let mut config = WebSocketConfig::new(0);
    // 1,000 sockets from one principal for the memory measure.
    config.quotas.max_connections = 4096;
    config.quotas.max_per_ip = 4096;
    config.quotas.max_per_principal = 4096;
    let (h, addr) = serve_websocket_with(module, config, Some(Arc::new(Tokens)), opts).await.unwrap();
    Svc { url: format!("ws://127.0.0.1:{}", addr.port()), peers, _h: h }
}

async fn dial(url: &str, token: Option<&str>) -> WsTransport {
    match token {
        Some(t) => WsTransport::connect_with_headers(url, "svc", &[("Authorization".into(), format!("Bearer {t}"))]).await.unwrap(),
        None => WsTransport::connect(url, "svc").await.unwrap(),
    }
}

async fn collect(mut s: ItemStream<'_>) -> Vec<Result<StreamItem, plexus_client::TransportError>> {
    let mut out = vec![];
    while let Some(i) = tokio::time::timeout(Duration::from_secs(10), s.next()).await.expect("an item within 10 s") {
        let end = matches!(&i, Ok(x) if x.is_terminal()) || i.is_err();
        out.push(i);
        if end {
            break;
        }
    }
    out
}

fn data(items: &[Result<StreamItem, plexus_client::TransportError>]) -> Vec<Value> {
    items.iter().filter_map(|i| match i { Ok(StreamItem::Data { content, .. }) => Some(content.clone()), _ => None }).collect()
}

fn error_code(items: &[Result<StreamItem, plexus_client::TransportError>]) -> Option<String> {
    items.iter().find_map(|i| match i {
        Ok(StreamItem::Error { code, message, .. }) => Some(code.clone().unwrap_or_else(|| message.clone())),
        Err(e) => Some(format!("transport: {e}")),
        _ => None,
    })
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) -> Duration {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(5), "{what}: not within 5 s");
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    t.elapsed()
}

fn pct(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

// ---------------------------------------------------------------------------
// (1) the offer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn offer_is_recorded_and_reoffered() {
    let s = svc(SvcOpts::default()).await;
    let b = dial(&s.url, Some("tok-b")).await;
    let ans = b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    assert_eq!(ans["offered"], "box2");
    assert_eq!(ans["mount"], "box2");
    assert_eq!(ans["service"]["principal"], A_KEY, "the service names itself");
    let rec = s.peers.get("box2").expect("mounted").offer().unwrap();
    assert_eq!(rec.activations, vec!["ci".to_string()]);
    assert_eq!(rec.principal.as_deref(), Some(B_KEY), "the offer is the proven principal's");
    assert_eq!(rec.schema_hash.len(), 16);
    assert!(rec.schema.is_none(), "the schema is not kept by default: the hash is");

    drop(b);
    eventually("the mount goes with the socket", || s.peers.counts() == (0, 0)).await;
    assert!(s.peers.get("box2").is_none());

    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    assert!(s.peers.get("box2").is_some(), "offered again after the reconnect");
    let items = collect(s.peers.call("box2", "ci.count", json!({"n": 2}), None)).await;
    assert_eq!(data(&items).len(), 2);
}

// ---------------------------------------------------------------------------
// (2) the reverse call — the same frames, verbatim, both ways
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_service_calls_an_offered_client_method() {
    let s = svc(SvcOpts::default()).await;
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    let items = collect(s.peers.call("box2", "ci.count", json!({"n": 5}), None)).await;
    let ticks: Vec<u64> = data(&items).iter().map(|c| c["n"].as_u64().unwrap()).collect();
    assert_eq!(ticks, vec![0, 1, 2, 3, 4]);
    assert!(matches!(items.last(), Some(Ok(StreamItem::Done { .. }))));
    // And the forward direction, on the same socket, at the same time.
    let fwd = collect(b.call(MethodRef::new("relay", "count"), json!({"n": 3}))).await;
    assert_eq!(data(&fwd).len(), 3);
}

/// The bytes on the wire: a raw socket plays B, so nothing reads the frames
/// for us. Printed for the result.
#[tokio::test]
async fn frames_are_the_same_in_both_directions() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let s = svc(SvcOpts { sequential: true, ..Default::default() }).await;
    let mut req = s.url.as_str().into_client_request().unwrap();
    req.headers_mut().insert("Authorization", "Bearer tok-b".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let mut log = vec![];
    macro_rules! send { ($v:expr) => {{ let t = $v.to_string(); log.push(format!("B → A  {t}")); ws.send(Message::text(t)).await.unwrap(); }} }
    macro_rules! recv { () => {{ let m = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap(); let t = m.into_text().unwrap().to_string(); log.push(format!("A → B  {t}")); serde_json::from_str::<Value>(&t).unwrap() }} }
    use futures::SinkExt;

    send!(json!({"jsonrpc":"2.0","id":1,"method":"svc.offer","params":{"namespace":"box2","activations":["ci"],"schema_hash":"5e1f0a2b9c3d4e6f","schema":{"namespace":"box2"}}}));
    let ans = recv!();
    assert_eq!(ans["id"], 1);
    assert_eq!(ans["result"]["mount"], "box2");

    // Forward: B calls A.
    send!(json!({"jsonrpc":"2.0","id":2,"method":"svc.call","params":{"method":"relay.count","params":{"n":1}}}));
    let fwd_ack = recv!();
    let fsub = fwd_ack["result"].clone();
    let fwd_item = recv!();
    let fwd_done = recv!();
    assert_eq!(fwd_item["method"], "svc.call");
    assert_eq!(fwd_item["params"]["subscription"], fsub);
    assert_eq!(fwd_item["params"]["result"]["type"], "data");
    assert_eq!(fwd_done["params"]["result"]["type"], "done");

    // Reverse: A calls B, on B's socket.
    let peers = s.peers.clone();
    let call = tokio::spawn(async move {
        let s = peers.call("box2", "ci.count", json!({"n": 1}), Some(json!({"principal": A_KEY, "for": [ADA], "limit": null})));
        collect(s).await
    });
    let rq = recv!();
    assert_eq!(rq["jsonrpc"], "2.0");
    assert_eq!(rq["method"], "box2.call", "the call names the callee's namespace, as the forward call names A's");
    assert_eq!(rq["params"]["method"], "ci.count");
    assert_eq!(rq["params"]["params"], json!({"n": 1}));
    assert_eq!(rq["params"]["hop"]["for"], json!([ADA]));
    let id = rq["id"].clone();
    assert!(id.is_u64(), "A mints its own request ids: {id}");
    send!(json!({"jsonrpc":"2.0","id":id,"result":1}));
    send!(json!({"jsonrpc":"2.0","method":"box2.call","params":{"subscription":1,"result":{"type":"data","content_type":"ci.count","content":{"type":"tick","n":0}}}}));
    send!(json!({"jsonrpc":"2.0","method":"box2.call","params":{"subscription":1,"result":{"type":"done"}}}));
    let got = call.await.unwrap();
    assert_eq!(data(&got), vec![json!({"type":"tick","n":0})]);
    assert!(matches!(got.last(), Some(Ok(StreamItem::Done { .. }))));

    // Reverse cancel: A lets go, B is told with A's own request id.
    let peers = s.peers.clone();
    let held = tokio::spawn(async move {
        let mut s = peers.call("box2", "ci.ticks", json!({}), None);
        let first = s.next().await;
        drop(s);
        first
    });
    let rq = recv!();
    send!(json!({"jsonrpc":"2.0","id":rq["id"],"result":2}));
    send!(json!({"jsonrpc":"2.0","method":"box2.call","params":{"subscription":2,"result":{"type":"data","content":{"type":"tick","n":0}}}}));
    held.await.unwrap();
    let unsub = recv!();
    assert_eq!(unsub["method"], "box2.call_unsub");
    assert_eq!(unsub["params"], json!([2]));
    send!(json!({"jsonrpc":"2.0","id":unsub["id"],"result":true}));
    // The answer to A's unsub is taken by A's hook: nothing comes back.
    assert!(tokio::time::timeout(Duration::from_millis(200), ws.next()).await.is_err(), "a response frame to A is never answered -32600 under the hook");

    eprintln!("\n--- LOCAL-3 frames (raw socket as B) ---");
    for l in &log {
        eprintln!("{l}");
    }
}

/// What jsonrpsee does with a response frame when nothing takes it.
#[tokio::test]
async fn jsonrpsee_answers_a_response_frame_invalid_request() {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    let s = svc(SvcOpts { no_peers: true, ..Default::default() }).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(s.url.as_str()).await.unwrap();
    ws.send(Message::text(r#"{"jsonrpc":"2.0","id":"s1","result":3}"#)).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), ws.next()).await.unwrap().unwrap().unwrap();
    let v: Value = serde_json::from_str(m.into_text().unwrap().as_str()).unwrap();
    eprintln!("jsonrpsee 0.26 alone, a response frame in → {v}");
    assert_eq!(v["id"], "s1");
    assert_eq!(v["error"]["code"], -32600, "the response is refused as an invalid request, and dropped");
    // An item of a call the server opened, without the hook: dispatched to nothing.
    ws.send(Message::text(r#"{"jsonrpc":"2.0","method":"box2.call","params":{"subscription":1,"result":{"type":"done"}}}"#)).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(200), ws.next()).await.is_err(), "a notification is answered with nothing and reaches nothing");
}

// ---------------------------------------------------------------------------
// (3) + (7) the callee's gate decides — as the peer, for whom it acts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_hop_names_the_service_for_its_caller() {
    let s = svc(SvcOpts::default()).await;
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    let ada = dial(&s.url, Some("tok-ada")).await;
    let items = collect(ada.call(MethodRef::new("relay", "ask"), json!({"peer": "box2", "method": "ci.whoami"}))).await;
    let got = data(&items);
    assert_eq!(got.len(), 1, "{items:?}");
    let who = &got[0]["content"];
    assert_eq!(who["principal"], A_KEY, "B's gate sees A — the peer on that connection");
    assert_eq!(who["acting_for"], json!([ADA]), "A's caller is named `for`");
}

#[tokio::test]
async fn the_clients_gate_refuses_what_the_peer_may_not() {
    let s = svc(SvcOpts::default()).await;
    let dropped = Arc::new(AtomicU64::new(0));
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), dropped), 64).await.unwrap();
    // B's gate: A is a viewer; `secret` is an editor's.
    let items = collect(s.peers.call("box2", "ci.secret", json!({}), None)).await;
    assert_eq!(error_code(&items).as_deref(), Some("-32003"), "forbidden by B's own gate: {items:?}");
    assert!(data(&items).is_empty());
    // Not offered: refused before B's service is reached at all.
    let items = collect(s.peers.call("box2", "admin.wipe", json!({}), None)).await;
    assert_eq!(error_code(&items).as_deref(), Some("not_offered"));
    let (_, _, not_offered, _) = b.served_stats().unwrap();
    assert_eq!(not_offered, 1);
    // A peer that offered nothing: refused at A, no frame sent.
    let c = dial(&s.url, Some("tok-c")).await;
    let _ = c; // dialed, offered nothing
    let items = collect(s.peers.call("box9", "ci.whoami", json!({}), None)).await;
    assert_eq!(error_code(&items).as_deref(), Some("child_unavailable"));
}

#[tokio::test]
async fn an_anonymous_peer_is_anonymous() {
    let s = svc(SvcOpts::default()).await;
    let b = dial(&s.url, Some("tok-b")).await;
    // B has no identity for the address it dialed: it serves A as anonymous.
    b.offer(box2_offer(None, Default::default()), 64).await.unwrap();
    let items = collect(s.peers.call("box2", "ci.whoami", json!({}), Some(json!({"principal": A_KEY, "for": [ADA]})))).await;
    let code = error_code(&items);
    assert!(matches!(code.as_deref(), Some("-32001") | Some("-32003")), "the anonymous view refuses: {items:?}");
}

// ---------------------------------------------------------------------------
// (4) cancel from either side
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_from_either_side() {
    let s = svc(SvcOpts::default()).await;
    let dropped = Arc::new(AtomicU64::new(0));
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), dropped.clone()), 64).await.unwrap();

    // The caller (A) lets go: B's handler is dropped.
    let mut lat = vec![];
    for i in 0..50u64 {
        let mut st = s.peers.call("box2", "ci.ticks", json!({}), None);
        st.next().await.unwrap().unwrap();
        let t = Instant::now();
        drop(st);
        let d = eventually("B's handler dropped", || dropped.load(Ordering::SeqCst) == i + 1).await;
        lat.push(d.max(t.elapsed().min(d)));
    }
    let (open, _, _, cancelled) = b.served_stats().unwrap();
    assert_eq!(open, 0);
    assert_eq!(cancelled, 50);
    eprintln!("cancel A → B's handler dropped: p50 {:?} p99 {:?} max {:?} (50 calls)", pct(&mut lat, 0.5), pct(&mut lat, 0.99), lat.iter().max().unwrap());

    // The callee (B) ends it: every call it serves gets one terminal error.
    let st = s.peers.call("box2", "ci.ticks", json!({}), None);
    let mut st = st;
    st.next().await.unwrap().unwrap();
    let t = Instant::now();
    b.withdraw_offer();
    let rest = collect(st).await;
    eprintln!("B withdraws → A's stream ended in {:?}", t.elapsed());
    assert_eq!(error_code(&rest).as_deref(), Some("offer_withdrawn"));
    eventually("B's handler dropped", || dropped.load(Ordering::SeqCst) == 51).await;
    let (held_req, held_subs) = s.peers.get("box2").unwrap().transport().unwrap().held();
    assert_eq!((held_req, held_subs), (0, 0), "A holds nothing for the ended calls");
}

// ---------------------------------------------------------------------------
// (5) items in, both directions — the same frames, the same driver
// ---------------------------------------------------------------------------

async fn drive(mut items: ItemStream<'_>, h: plexus_client::ws::DuplexHandle, n: u64) -> (Value, Duration, u64) {
    let t = Instant::now();
    let (mut credit, mut sent, mut seq, mut ended, mut most) = (0u64, 0u64, 0u64, false, 0u64);
    let mut result = Value::Null;
    loop {
        while credit > 0 && sent < n {
            h.input(seq, vec![json!({"i": sent})]).unwrap();
            seq += 1;
            sent += 1;
            credit -= 1;
        }
        if sent == n && !ended {
            h.end(seq).unwrap();
            ended = true;
        }
        match tokio::time::timeout(Duration::from_secs(10), items.next()).await.expect("an item").expect("not ended") {
            Ok(StreamItem::Credit { n }) => {
                credit += n;
                most = most.max(credit);
            }
            Ok(StreamItem::Ack { .. }) | Ok(StreamItem::InputClosed { .. }) => {}
            Ok(StreamItem::Data { content, .. }) => result = content,
            Ok(StreamItem::Done { .. }) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    (result, t.elapsed(), most)
}

#[tokio::test]
async fn items_in_cross_both_ways() {
    let s = svc(SvcOpts::default()).await;
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(Arc::new(SumOffer), 64).await.unwrap();
    const N: u64 = 10_000;

    // Forward: B → A (A's `svc.sum`, BIDIR-3's route).
    let (st, h) = b.open_duplex("svc.sum", "sum", json!({}), "svc");
    let (fwd, fwd_t, fwd_credit) = drive(st, h, N).await;
    assert_eq!(fwd, json!({"count": N, "in_order": true}), "forward");

    // Reverse: A → B (B's `ci.sum`), on B's socket.
    let peer = s.peers.get("box3").unwrap();
    let t = peer.transport().unwrap();
    let (st, h) = t.open_duplex("box3.call", "ci.sum", json!({}), "box3");
    let (rev, rev_t, rev_credit) = drive(st, h, N).await;
    assert_eq!(rev, json!({"count": N, "in_order": true}), "reverse");
    assert!(fwd_credit <= 64 && rev_credit <= 64, "neither side was ever granted past its window");
    eprintln!(
        "items in, 10,000 × one per frame, credit 64: forward {:?} ({:.0}/s) · reverse {:?} ({:.0}/s)",
        fwd_t,
        N as f64 / fwd_t.as_secs_f64(),
        rev_t,
        N as f64 / rev_t.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// (6) a reconnect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_reconnect_ends_open_calls_and_leaks_nothing() {
    let s = svc(SvcOpts::default()).await;
    let dropped = Arc::new(AtomicU64::new(0));
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), dropped.clone()), 64).await.unwrap();
    let peer = s.peers.get("box2").unwrap();
    let mut calls = vec![];
    for _ in 0..10 {
        let mut st = peer.call("ci.ticks", json!({}), None);
        st.next().await.unwrap().unwrap();
        calls.push(st);
    }
    assert_eq!(b.served_stats().unwrap().0, 10);
    let t = Instant::now();
    drop(b); // B's socket goes
    for st in calls.iter_mut() {
        let rest = collect(Box::pin(st)).await;
        let e = rest.last().unwrap().as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(e.contains("connection_lost"), "a typed end, not a hang: {rest:?}");
    }
    eprintln!("socket lost → 10 open calls ended at A in {:?}", t.elapsed());
    eventually("B's handlers dropped", || dropped.load(Ordering::SeqCst) == 10).await;
    eventually("the peer and its mount are gone", || s.peers.counts() == (0, 0)).await;
    assert_eq!(peer.transport().unwrap().held(), (0, 0), "nothing of the old connection is held");
    drop(calls);
    drop(peer);

    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), dropped.clone()), 64).await.unwrap();
    let items = collect(s.peers.call("box2", "ci.whoami", json!({}), None)).await;
    assert_eq!(data(&items)[0]["principal"], A_KEY, "called again after the reconnect; nothing replayed");

    // A second connection may not take a name another principal holds.
    let c = dial(&s.url, Some("tok-c")).await;
    let refused = c.offer(box2_offer(Some(A_KEY), Default::default()), 64).await;
    assert!(format!("{refused:?}").contains("name_taken"), "{refused:?}");
}

// ---------------------------------------------------------------------------
// (8) ids: each side mints its own; the same numbers at once collide nowhere
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ids_never_collide() {
    let s = svc(SvcOpts { sequential: true, ..Default::default() }).await;
    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    let t = s.peers.get("box2").unwrap();
    let t = t.transport().unwrap();
    // B → A: A mints subscription 1. A → B: B mints subscription 1. Both
    // sides' first request ids were 1 too (B's offer, A's call).
    let (fwd, fh) = b.open_duplex("svc.call", "relay.count", json!({"n": 200}), "svc");
    let (rev, rh) = t.open_duplex("box2.call", "ci.count", json!({"n": 200}), "box2");
    let (f, r) = tokio::join!(collect(fwd), collect(rev));
    assert_eq!(fh.subscription(), Some(json!(1)));
    assert_eq!(rh.subscription(), Some(json!(1)));
    let fticks: Vec<u64> = data(&f).iter().map(|c| c["n"].as_u64().unwrap()).collect();
    let rticks: Vec<u64> = data(&r).iter().map(|c| c["n"].as_u64().unwrap()).collect();
    assert_eq!(fticks, (0..200).collect::<Vec<_>>());
    assert_eq!(rticks, (0..200).collect::<Vec<_>>());
    assert!(f.iter().all(|i| !matches!(i, Ok(StreamItem::Data { content, .. }) if content["type"] != "tick")));
    // Each side's content type proves whose stream it read.
    assert!(r.iter().any(|i| matches!(i, Ok(StreamItem::Data { content_type: Some(ct), .. }) if ct.starts_with("ci."))), "{r:?}");
    assert!(f.iter().any(|i| matches!(i, Ok(StreamItem::Data { content_type: Some(ct), .. }) if ct.starts_with("relay."))), "{f:?}");
}

// ---------------------------------------------------------------------------
// (9) LOCAL-5: a dialed-in service is admitted by a grant on its mount name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_dialed_in_service_is_admitted_by_a_grant_on_its_mount_name() {
    let members = Arc::new(Members::default());
    members.add(B_KEY, "acceptor", "box2");
    let roles = RoleTable::from_lists([("acceptor", ["service.accept"].into_iter())]);
    let gate = Gate::new(members, Arc::new(roles), Arc::new(Named));
    let admit: Admit = Arc::new(move |auth, mount| {
        gate.check(auth, &plexus_auth_core::grants::Grant::new("service.accept"), Some(mount)).map(|_| ()).map_err(|r| format!("{r:?}"))
    });
    let s = svc(SvcOpts { admit: Some(admit), ..Default::default() }).await;

    let c = dial(&s.url, Some("tok-c")).await;
    let r = c.offer(box2_offer(Some(A_KEY), Default::default()), 64).await;
    assert!(format!("{r:?}").contains("forbidden"), "C holds no grant on box2: {r:?}");
    let anon = dial(&s.url, None).await;
    let r = anon.offer(box2_offer(Some(A_KEY), Default::default()), 64).await;
    assert!(r.is_err(), "an anonymous peer is admitted nowhere: {r:?}");
    assert!(s.peers.get("box2").is_none());

    let b = dial(&s.url, Some("tok-b")).await;
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    let items = collect(s.peers.call("box2", "ci.whoami", json!({}), None)).await;
    assert_eq!(data(&items)[0]["principal"], A_KEY);
}

// ---------------------------------------------------------------------------
// Measures
// ---------------------------------------------------------------------------

async fn time_calls(n: usize, mut f: impl FnMut() -> ItemStream<'static>) -> Vec<Duration> {
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let items = collect(f()).await;
        assert!(matches!(items.last(), Some(Ok(StreamItem::Done { .. }))), "{items:?}");
        v.push(t.elapsed());
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_vs_forward_cost() {
    const N: usize = 1000;
    let s = svc(SvcOpts::default()).await;
    let b = Arc::new(dial(&s.url, Some("tok-b")).await);
    b.offer(box2_offer(Some(A_KEY), Default::default()), 64).await.unwrap();
    let plain = svc(SvcOpts { no_peers: true, ..Default::default() }).await;
    let p = Arc::new(dial(&plain.url, Some("tok-b")).await);

    // warm
    for _ in 0..100 {
        collect(b.call(MethodRef::new("relay", "count"), json!({"n": 1}))).await;
        collect(s.peers.call("box2", "ci.count", json!({"n": 1}), None)).await;
        collect(p.call(MethodRef::new("relay", "count"), json!({"n": 1}))).await;
    }
    let bb = b.clone();
    let mut fwd = time_calls(N, move || {
        let b = bb.clone();
        Box::pin(stream! { let mut s = b.call(MethodRef::new("relay", "count"), json!({"n": 1})); while let Some(i) = s.next().await { yield i; } })
    })
    .await;
    let peers = s.peers.clone();
    let mut rev = time_calls(N, move || peers.call("box2", "ci.count", json!({"n": 1}), None)).await;
    let pp = p.clone();
    let mut nohook = time_calls(N, move || {
        let p = pp.clone();
        Box::pin(stream! { let mut s = p.call(MethodRef::new("relay", "count"), json!({"n": 1})); while let Some(i) = s.next().await { yield i; } })
    })
    .await;
    let row = |name: &str, v: &mut Vec<Duration>| {
        eprintln!("{name:<44} p50 {:>9?}  p99 {:>9?}", pct(v, 0.5), pct(v, 0.99));
    };
    eprintln!("\n--- 1,000 sequential calls, one item + done, loopback ---");
    row("forward  B → A (hook installed)", &mut fwd);
    row("forward  B → A (no hook: a server without offers)", &mut nohook);
    row("reverse  A → B (on B's socket, B's gate)", &mut rev);
    assert!(pct(&mut rev, 0.5) < pct(&mut fwd, 0.5) * 3, "the reverse call within 3× the forward");
}

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = s.split_whitespace().nth(1).unwrap().parse().unwrap();
    pages * 4
}

async fn offer_memory(keep_schema: bool) -> (f64, usize) {
    const N: usize = 1000;
    let s = svc(SvcOpts { keep_schema, ..Default::default() }).await;
    let mut conns = vec![];
    for _ in 0..N {
        conns.push(dial(&s.url, Some("tok-b")).await);
    }
    // One distinct mount per connection: a namespace each.
    struct Named2(String, Arc<dyn Offered>);
    impl Offered for Named2 {
        fn namespace(&self) -> &str { &self.0 }
        fn activations(&self) -> Vec<String> { self.1.activations() }
        fn schema(&self) -> Value { self.1.schema() }
        fn serve(&self, c: ServedCall) -> BoxStream<'static, Value> { self.1.serve(c) }
    }
    let inner = box2_offer(Some(A_KEY), Default::default());
    let schema_len = inner.schema().to_string().len();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = rss_kb();
    for (i, c) in conns.iter().enumerate() {
        c.offer(Arc::new(Named2(format!("peer{i}"), inner.clone())), 64).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = rss_kb();
    assert_eq!(s.peers.counts().1, N);
    ((after.saturating_sub(before)) as f64 * 1024.0 / N as f64, schema_len)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offer_memory_per_connection() {
    let (hash_only, schema_len) = offer_memory(false).await;
    let (kept, _) = offer_memory(true).await;
    eprintln!(
        "an offer held, per connection (both ends in one process, 1,000 connections): hash only {:.0} B · schema kept {:.0} B (schema {} B of JSON)",
        hash_only, kept, schema_len
    );
}

// ---------------------------------------------------------------------------
// (1) the kit's Python client offers, and is called
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_kits_python_client_offers_and_is_called() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let s = svc(SvcOpts::default()).await;
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../plexus-wire/conformance/examples/offering_client.py");
    let mut child = tokio::process::Command::new("python3")
        .args(["-I", script, &s.url, "svc"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("python3");
    child.stdin.take().unwrap().write_all(b"tok-b\n").await.unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(10), out.next_line()).await.unwrap().unwrap().unwrap();
    assert!(line.starts_with("offered "), "{line}");
    eprintln!("python: {line}");

    let items = collect(s.peers.call("pybox", "echo.echo", json!({"text": "hi", "times": 2}), None)).await;
    assert_eq!(data(&items), vec![json!({"type": "said", "text": "hi"}); 2]);
    let items = collect(s.peers.call("pybox", "echo.whoami", json!({}), Some(json!({"principal": A_KEY, "for": [ADA], "limit": null})))).await;
    assert_eq!(data(&items)[0]["hop"]["for"], json!([ADA]));
    let items = collect(s.peers.call("pybox", "admin.wipe", json!({}), None)).await;
    assert_eq!(error_code(&items).as_deref(), Some("not_offered"));
    child.kill().await.ok();
    eventually("the Python peer's mount goes with it", || s.peers.get("pybox").is_none()).await;
}
