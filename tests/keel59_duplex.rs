//! KEEL-59 (review H1) · a call with an input half is held for its whole
//! life: a revoke ends it whether its input half is open, ended by the
//! client (`end`), or let go by its handler — on a real socket, the handler
//! dropped within [`BOUND`] of the revoke, and nothing it would have said
//! after goes out.
//!
//! A call with an input half reaches a service only over a WebSocket: a hop
//! to a remote child refuses one ("does not forward one yet") and the MCP
//! bridges carry none, so these rows are the whole of it.

mod ws_support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use duplex_fixture::{IngestActivation, Probe};
use futures::{SinkExt, StreamExt};
use plexus_core::plexus::duplex_gate::GrantsMove;
use plexus_core::plexus::{AuthContext, DynamicHub, Gate, Membership, Memberships, Resolve, RoleTable, ScopeRef, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use ws_support::Ws;

const BOUND: Duration = Duration::from_millis(100);
const WAIT: Duration = Duration::from_secs(5);
const ADA: &str = "key:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Default)]
struct Members(Mutex<Vec<Membership>>);

impl Memberships for Members {
    fn held(&self, who: &plexus_auth_core::identity::Principal) -> Vec<Membership> {
        self.0.lock().unwrap().iter().filter(|m| &m.principal == who).cloned().collect()
    }
}

struct Everywhere;

impl Resolve for Everywhere {
    fn chain(&self, target: &str) -> Option<Vec<ScopeRef>> {
        Some(vec![ScopeRef::named(target), ScopeRef::Everywhere])
    }
}

struct Ada;

#[async_trait]
impl SessionValidator for Ada {
    async fn validate(&self, _token: &str) -> Option<AuthContext> {
        Some(AuthContext::for_principal(&ADA.parse().unwrap(), "s", vec![], Value::Null))
    }
}

struct Served {
    port: u16,
    gate: Gate,
    members: Arc<Members>,
    probe: Probe,
}

async fn served() -> Served {
    let probe = Probe::default();
    let members = Arc::new(Members::default());
    members.0.lock().unwrap().push(Membership { principal: ADA.parse().unwrap(), role: plexus_auth_core::RoleName::new("writer"), scope: ScopeRef::Everywhere });
    let roles = RoleTable::new().define("writer", ["ingest.gated", "ingest.letgo", "ingest.stuck", "ingest.plain"]).unwrap();
    let gate = Gate::new(members.clone(), Arc::new(roles), Arc::new(Everywhere));
    let service = DynamicHub::new("menger").register(IngestActivation { probe: probe.clone() }).with_gate(Arc::new(gate.clone()));
    let validator: Arc<dyn SessionValidator> = Arc::new(Ada);
    let (handle, addr) = serve_websocket_with(service.into_rpc_module().unwrap(), WebSocketConfig::new(0), Some(validator), UpgradeOptions::default()).await.unwrap();
    std::mem::forget(handle);
    Served { port: addr.port(), gate, members, probe }
}

async fn signed_in(port: u16) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    req.headers_mut().insert("authorization", "Bearer ada".parse().unwrap());
    tokio_tungstenite::client_async(req, tcp).await.unwrap().0
}

async fn send(ws: &mut Ws, frame: Value) {
    ws.send(Message::text(frame.to_string())).await.unwrap();
}

/// The next notification's item.
async fn next_item(ws: &mut Ws) -> Value {
    let v = ws_support::next_text(ws, WAIT).await.expect("a frame");
    v["params"]["result"].clone()
}

/// What comes within `d`: every item.
async fn items_for(ws: &mut Ws, d: Duration) -> Vec<Value> {
    let mut out = Vec::new();
    let until = tokio::time::Instant::now() + d;
    while let Ok(Some(Ok(m))) = tokio::time::timeout_at(until, ws.next()).await {
        if let Message::Text(t) = m {
            let v: Value = serde_json::from_str(t.as_str()).unwrap();
            out.push(v["params"]["result"].clone());
        }
    }
    out
}

/// Open `method`: its subscription, once its first credit came.
async fn open(ws: &mut Ws, method: &str) -> u64 {
    send(ws, json!({"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":method,"params":{}}})).await;
    let answer = ws_support::next_text(ws, WAIT).await.expect("answered");
    let sub = answer["result"].as_u64().unwrap_or_else(|| panic!("{method}: {answer}"));
    let credit = next_item(ws).await;
    assert_eq!(credit["type"], "credit", "{credit}");
    sub
}

/// Ada's grant revoked: the cause's instant.
fn revoke(s: &Served) -> Instant {
    s.members.0.lock().unwrap().clear();
    let at = Instant::now();
    s.gate.moved(GrantsMove::Revoke);
    at
}

/// The handler was dropped within [`BOUND`] of `cause`.
async fn dropped_within(probe: &Probe, cause: Instant) -> f64 {
    tokio::time::timeout(WAIT, async {
        while probe.dropped() < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the handler was dropped");
    let ms = cause.elapsed().as_secs_f64() * 1e3;
    assert!(cause.elapsed() <= BOUND + Duration::from_millis(5), "the handler ran {ms:.1} ms past the revoke (bound {BOUND:?})");
    ms
}

/// Its last item: an error of `kind`; the frames before it, if any, an
/// `input_closed` with the same reason.
fn ends_with(items: &[Value], kind: &str) {
    let last = items.last().unwrap_or_else(|| panic!("nothing came: the call was not ended"));
    assert_eq!((last["type"].as_str(), last["error_kind"].as_str()), (Some("error"), Some(kind)), "{items:?}");
    for i in &items[..items.len() - 1] {
        assert_eq!((i["type"].as_str(), i["reason"].as_str()), (Some("input_closed"), Some(kind)), "{items:?}");
    }
}

/// Control: the input half still open (the handler holds it, never reads).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_input_half_ends_at_the_revoke() {
    let s = served().await;
    let mut ws = signed_in(s.port).await;
    open(&mut ws, "ingest.stuck").await;
    assert_eq!(next_item(&mut ws).await["content"]["type"], "ok");
    let cause = revoke(&s);
    let ms = dropped_within(&s.probe, cause).await;
    let items = items_for(&mut ws, Duration::from_millis(300)).await;
    ends_with(&items, "forbidden");
    println!("    stuck: handler dropped {ms:.1} ms after the revoke");
}

/// The handler let its input go and went on: the revoke still ends it, and
/// what it would have said after never goes out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_whose_handler_let_its_input_go_ends_at_the_revoke() {
    let s = served().await;
    let mut ws = signed_in(s.port).await;
    open(&mut ws, "ingest.letgo").await;
    let closed = next_item(&mut ws).await;
    assert_eq!((closed["type"].as_str(), closed["reason"].as_str()), (Some("input_closed"), Some("method_ended")), "{closed}");
    let cause = revoke(&s);
    let ms = dropped_within(&s.probe, cause).await;
    // Let the handler go on: it is gone, so nothing comes.
    s.probe.permits.add_permits(4);
    let items = items_for(&mut ws, Duration::from_millis(300)).await;
    assert_eq!(items.len(), 1, "one item, the end: {items:?}");
    ends_with(&items, "forbidden");
    println!("    letgo: handler dropped {ms:.1} ms after the revoke");
}

/// The client sent its items and `end` at once: the revoke still ends the
/// call, and the handler never reads what was sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_whose_client_ended_its_input_ends_at_the_revoke() {
    let s = served().await;
    let mut ws = signed_in(s.port).await;
    let sub = open(&mut ws, "ingest.gated").await;
    send(&mut ws, json!({"jsonrpc":"2.0","method":"menger.input","params":{"subscription":sub,"seq":0,"items":[{"t":"a"},{"t":"b"}]}})).await;
    send(&mut ws, json!({"jsonrpc":"2.0","method":"menger.end","params":{"subscription":sub,"seq":1}})).await;
    // The input half is over before the revoke.
    let _ = items_for(&mut ws, Duration::from_millis(100)).await;
    let cause = revoke(&s);
    let ms = dropped_within(&s.probe, cause).await;
    s.probe.permits.add_permits(4);
    let items = items_for(&mut ws, Duration::from_millis(300)).await;
    let last = items.last().unwrap_or_else(|| panic!("nothing came: the call was not ended"));
    assert_eq!((last["type"].as_str(), last["error_kind"].as_str()), (Some("error"), Some("forbidden")), "{items:?}");
    assert!(items.iter().all(|i| i["type"] != "data" && i["type"] != "done"), "it went on: {items:?}");
    assert!(s.probe.seen().is_empty(), "the handler read after the revoke: {:?}", s.probe.seen());
    println!("    client-ended: handler dropped {ms:.1} ms after the revoke");
}
