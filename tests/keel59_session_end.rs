//! KEEL-59 · on the socket, the server ends a call when its right to run
//! ends, and says so.
//!
//! - A sign-in that ends (its `expires_at`; a capped session's the same)
//!   closes the socket — and before the close, every call open on it is
//!   told, one `unauthenticated` item each, its work stopped.
//! - A grant revoked mid-stream ends the call that stood on it with one
//!   `forbidden` item, and its work stops; the socket and its other calls
//!   go on.

mod ws_support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use keel59_fixture::{Counter, WorkActivation};
use plexus_auth_core::grants::{GrantsMove, CEILING_KEY};
use plexus_core::plexus::{AuthContext, DynamicHub, Gate, Membership, Memberships, Resolve, RoleTable, ScopeRef, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use ws_support::Ws;

const BOUND: Duration = Duration::from_millis(100);
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
    fn chain(&self, _target: &str) -> Option<Vec<ScopeRef>> {
        Some(vec![ScopeRef::Everywhere])
    }
}

/// Every token signs in as Ada, in a session with `meta` (its end, its
/// ceiling).
struct Ada(Value);

#[async_trait]
impl SessionValidator for Ada {
    async fn validate(&self, _token: &str) -> Option<AuthContext> {
        Some(AuthContext::for_principal(&ADA.parse().unwrap(), "s", vec![], self.0.clone()))
    }
}

/// Three counters: one for each call a test opens.
struct Served {
    port: u16,
    gate: Gate,
    members: Arc<Members>,
    work: [Counter; 3],
}

async fn served(session: Value) -> Served {
    let members = Arc::new(Members::default());
    members.0.lock().unwrap().push(Membership { principal: ADA.parse().unwrap(), role: plexus_auth_core::RoleName::new("reader"), scope: ScopeRef::Everywhere });
    let roles = RoleTable::new().define("reader", ["work.quiet", "work.chatty"]).unwrap();
    let gate = Gate::new(members.clone(), Arc::new(roles), Arc::new(Everywhere));
    let activation = WorkActivation::default();
    let work = [activation.lanes[0].clone(), activation.lanes[1].clone(), activation.lanes[2].clone()];
    let service = DynamicHub::new("svc").register(activation).with_gate(Arc::new(gate.clone()));
    let validator: Arc<dyn SessionValidator> = Arc::new(Ada(session));
    let (handle, addr) = serve_websocket_with(service.into_rpc_module().unwrap(), WebSocketConfig::new(0), Some(validator), UpgradeOptions::default()).await.unwrap();
    std::mem::forget(handle);
    Served { port: addr.port(), gate, members, work }
}

async fn signed_in(port: u16) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    req.headers_mut().insert("authorization", "Bearer ada".parse().unwrap());
    tokio_tungstenite::client_async(req, tcp).await.unwrap().0
}

/// Open `method` on `lane` (call id `id`): its subscription, once it said
/// it started.
async fn open(ws: &mut Ws, id: u64, method: &str, lane: u8) -> Value {
    let f = json!({"jsonrpc":"2.0","id":id,"method":"svc.call","params":{"method":method,"params":{"lane":lane}}});
    ws.send(Message::text(f.to_string())).await.unwrap();
    let mut sub = Value::Null;
    loop {
        let v = ws_support::next_text(ws, Duration::from_secs(5)).await.expect("opened");
        if v["id"] == json!(id) {
            sub = v["result"].clone();
        }
        let r = &v["params"]["result"];
        if r["type"] == "error" {
            panic!("{method}: refused at open: {v}");
        }
        if r["type"] == "data" && v["params"]["subscription"] == sub {
            return sub;
        }
    }
}

/// What the client reads until the socket ends: each subscription's
/// non-data items in order, and when the close came.
#[derive(Debug, Default)]
struct Seen {
    items: Vec<(Value, Value, Instant)>,
    close: Option<(Instant, String)>,
}

async fn read_until_close(ws: &mut Ws, within: Duration) -> Seen {
    let mut seen = Seen::default();
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) | Ok(None) | Ok(Some(Err(_))) => return seen,
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                let r = v["params"]["result"].clone();
                if r["type"] != "data" {
                    seen.items.push((v["params"]["subscription"].clone(), r, Instant::now()));
                }
            }
            Ok(Some(Ok(Message::Close(f)))) => {
                seen.close = Some((Instant::now(), f.map(|f| f.reason.to_string()).unwrap_or_default()));
                return seen;
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_that_ends_says_so_before_the_socket_closes() {
    // A capped session (KEEL-51: a ceiling and an end) ending in 1–2 s.
    let at = now_secs() + 2;
    let s = served(json!({ "expires_at": at, CEILING_KEY: ["reader@*"] })).await;
    let mut ws = signed_in(s.port).await;
    let subs = [open(&mut ws, 1, "work.quiet", 0).await, open(&mut ws, 2, "work.chatty", 1).await, open(&mut ws, 3, "work.open", 2).await];
    for c in &s.work {
        c.going().await;
    }
    let seen = read_until_close(&mut ws, Duration::from_secs(5)).await;
    let ended = UNIX_EPOCH + Duration::from_secs(at as u64);
    let (closed, why) = seen.close.clone().unwrap_or_else(|| panic!("no close: {seen:?}"));
    assert_eq!(why, "closed", "{seen:?}");
    // Each call: one item, `unauthenticated`, saying why, before the close.
    for sub in &subs {
        let mine: Vec<_> = seen.items.iter().filter(|(s, _, _)| s == sub).collect();
        assert_eq!(mine.len(), 1, "one item for {sub}: {seen:?}");
        let (_, item, when) = mine[0];
        assert_eq!(item["type"], json!("error"), "{item}");
        assert_eq!(item["error_kind"], json!("unauthenticated"), "{item}");
        assert!(item["message"].as_str().unwrap().contains("expired"), "{item}");
        assert!(*when <= closed, "told after the close");
    }
    // Told as the sign-in ended, not a tick later.
    let late = SystemTime::now().duration_since(ended).unwrap_or_default();
    let told = Instant::now() - late;
    let first = seen.items.iter().map(|(_, _, w)| *w).min().unwrap();
    assert!(first <= told + BOUND, "told {:?} after the sign-in ended", first.saturating_duration_since(told));
    for c in &s.work {
        let ms = c.stopped(closed, BOUND).await;
        println!("    work stopped {ms:+.1} ms from the close");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_viewer_whose_grant_is_revoked_mid_stream_is_told_forbidden_and_the_work_stops() {
    let s = served(Value::Null).await;
    let mut ws = signed_in(s.port).await;
    let quiet = open(&mut ws, 1, "work.quiet", 0).await;
    let public = open(&mut ws, 2, "work.open", 2).await;
    s.work[0].going().await;
    s.members.0.lock().unwrap().clear();
    let cause = Instant::now();
    s.gate.moved(GrantsMove::Revoke);
    let item = loop {
        let v = ws_support::next_text(&mut ws, Duration::from_secs(5)).await.expect("told");
        if v["params"]["subscription"] == quiet {
            break v["params"]["result"].clone();
        }
        assert_ne!(v["params"]["subscription"], public, "the public call was touched: {v}");
    };
    assert_eq!(item["error_kind"], json!("forbidden"), "{item}");
    assert_eq!(item["grant"], json!("work.quiet"), "{item}");
    let ms = s.work[0].stopped(cause, BOUND).await;
    println!("    revoked call's work stopped {ms:+.1} ms from the revoke");
    // The public call, and the socket, go on.
    let before = s.work[2].ticks();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(s.work[2].ticks() > before, "the public call stopped");
}
