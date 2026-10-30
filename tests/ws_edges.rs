//! PERF-20: the WebSocket edge cases the loop owns, each with its test —
//! the close handshake both ways (every call served ends, nothing is held),
//! fragments assembled under the message cap, a message over the cap
//! refused `-32007` with the socket kept (RPC-6), and a stray continuation
//! refused.

mod ws_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonrpsee::{PendingSubscriptionSink, RpcModule};
use plexus_transport::websocket::{Connection, UpgradeOptions};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

const CAP: usize = 10 * 1024 * 1024;

/// `hold` (a call that runs until its socket lets go, counted in `open`),
/// `conn` (keeps a weak handle on the caller's connection), `bye` (the server
/// closes the caller's socket) and `ping`.
fn module(open: Arc<AtomicUsize>, kept: Arc<Mutex<Option<Weak<Connection>>>>) -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |_, _, _| "pong").unwrap();
    m.register_method("conn", move |_, _, ext| {
        let c = ext.get::<Arc<Connection>>().expect("every call carries its connection");
        *kept.lock().unwrap() = Some(Arc::downgrade(c));
        true
    })
    .unwrap();
    m.register_method("bye", |_, _, ext| {
        ext.get::<Arc<Connection>>().unwrap().close();
        true
    })
    .unwrap();
    m.register_subscription("hold", "hold", "unhold", move |_, pending: PendingSubscriptionSink, _, _| {
        let open = open.clone();
        async move {
            let sink = pending.accept().await?;
            open.fetch_add(1, Ordering::SeqCst);
            sink.send(serde_json::value::to_raw_value(&"held")?).await?;
            sink.closed().await;
            open.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    m
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{what}: not within 2 s");
}

/// The client closes: its close is answered with a close, every call it had
/// open ends, and the connection is let go of (no handle on it is left).
#[tokio::test]
async fn a_client_close_is_answered_and_nothing_is_held() {
    let open = Arc::new(AtomicUsize::new(0));
    let kept = Arc::new(Mutex::new(None));
    let port = serve(module(open.clone(), kept.clone()), UpgradeOptions::default()).await;
    let mut ws = connect(port).await;
    for id in [1, 2] {
        request(&mut ws, id, "hold", json!([])).await;
    }
    assert_eq!(request(&mut ws, 3, "conn", json!([])).await["result"], true);
    eventually("two calls open", || open.load(Ordering::SeqCst) == 2).await;

    ws.send(Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason: "bye".into() }))).await.unwrap();
    let mut answered = false;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
        if let Ok(Message::Close(_)) = m {
            answered = true;
        }
    }
    assert!(answered, "the close is answered with a close");
    eventually("every call served ends", || open.load(Ordering::SeqCst) == 0).await;
    let weak = kept.lock().unwrap().clone().unwrap();
    eventually("the connection is let go of", || weak.upgrade().is_none()).await;
}

/// The server closes (the connection's own `close`, as AUTH-11's sign-out
/// watch does): a close frame goes out, the client's answer ends it, and the
/// calls it served end.
#[tokio::test]
async fn a_server_close_is_sent_and_its_calls_end() {
    let open = Arc::new(AtomicUsize::new(0));
    let kept = Arc::new(Mutex::new(None));
    let port = serve(module(open.clone(), kept.clone()), UpgradeOptions::default()).await;
    let mut ws = connect(port).await;
    request(&mut ws, 1, "hold", json!([])).await;
    eventually("a call open", || open.load(Ordering::SeqCst) == 1).await;
    let frame = json!({"jsonrpc": "2.0", "id": 2, "method": "bye", "params": []});
    ws.send(Message::text(frame.to_string())).await.unwrap();
    let mut closed = false;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
        if let Ok(Message::Close(_)) = m {
            closed = true;
        }
    }
    assert!(closed, "the server sends a close frame");
    eventually("the call served ends", || open.load(Ordering::SeqCst) == 0).await;
}

/// A message in three fragments, a ping between two of them, is assembled
/// and answered.
#[tokio::test]
async fn a_fragmented_message_is_assembled() {
    let port = serve(module(Default::default(), Default::default()), UpgradeOptions::default()).await;
    let mut s = raw_upgrade(port).await;
    let msg = br#"{"jsonrpc":"2.0","id":1,"method":"ping","params":[]}"#;
    s.write_all(&client_frame(0x1, false, &msg[..10])).await.unwrap();
    s.write_all(&client_frame(0x9, true, b"between")).await.unwrap();
    s.write_all(&client_frame(0x0, false, &msg[10..30])).await.unwrap();
    s.write_all(&client_frame(0x0, true, &msg[30..])).await.unwrap();
    let got = raw_next_text(&mut s, Duration::from_secs(5)).await.expect("an answer");
    let v: serde_json::Value = serde_json::from_str(&got).unwrap();
    assert_eq!((v["id"].clone(), v["result"].clone()), (json!(1), json!("pong")), "{v}");
}

/// RPC-6: a message over the cap (10 MiB) — one frame — is refused `-32007`
/// with `id: null`, and the socket goes on.
#[tokio::test]
async fn a_message_over_the_cap_is_refused_and_the_socket_kept() {
    let port = serve(module(Default::default(), Default::default()), UpgradeOptions::default()).await;
    let mut s = raw_upgrade(port).await;
    let mut big = br#"{"jsonrpc":"2.0","id":31,"method":"ping","params":{"pad":""#.to_vec();
    big.resize(CAP + 1024, b'x');
    big.extend_from_slice(br#""}}"#);
    s.write_all(&client_frame(0x1, true, &big)).await.unwrap();
    s.write_all(&client_frame(0x1, true, br#"{"jsonrpc":"2.0","id":32,"method":"ping","params":[]}"#)).await.unwrap();
    let first: serde_json::Value = serde_json::from_str(&raw_next_text(&mut s, Duration::from_secs(5)).await.expect("the refusal")).unwrap();
    assert_eq!(first["error"]["code"], -32007, "{first}");
    assert!(first["id"].is_null(), "{first}");
    assert_eq!(first["error"]["data"], format!("Exceeded max limit of {CAP}"));
    let second: serde_json::Value = serde_json::from_str(&raw_next_text(&mut s, Duration::from_secs(5)).await.expect("the socket kept")).unwrap();
    assert_eq!(second["result"], "pong", "{second}");
}

/// RPC-6: fragments are assembled under the same cap — past it the message is
/// refused `-32007`, its remaining fragments dropped, and the socket goes on.
#[tokio::test]
async fn fragments_past_the_cap_are_refused_and_the_socket_kept() {
    let port = serve(module(Default::default(), Default::default()), UpgradeOptions::default()).await;
    let mut s = raw_upgrade(port).await;
    let mut first = br#"{"jsonrpc":"2.0","id":41,"method":"ping","params":{"pad":""#.to_vec();
    first.resize(6 * 1024 * 1024, b'x');
    s.write_all(&client_frame(0x1, false, &first)).await.unwrap();
    s.write_all(&client_frame(0x0, false, &vec![b'x'; 5 * 1024 * 1024])).await.unwrap();
    s.write_all(&client_frame(0x0, true, br#""}}"#)).await.unwrap();
    s.write_all(&client_frame(0x1, true, br#"{"jsonrpc":"2.0","id":42,"method":"ping","params":[]}"#)).await.unwrap();
    let refusal: serde_json::Value =
        serde_json::from_str(&raw_next_text(&mut s, Duration::from_secs(5)).await.expect("the refusal")).unwrap();
    assert_eq!(refusal["error"]["code"], -32007, "{refusal}");
    let next: serde_json::Value = serde_json::from_str(&raw_next_text(&mut s, Duration::from_secs(5)).await.expect("the socket kept")).unwrap();
    assert_eq!(next["id"], 42, "{next}");
}

/// A continuation frame with no message begun is a protocol error: the
/// socket is closed.
#[tokio::test]
async fn a_stray_continuation_is_refused() {
    let port = serve(module(Default::default(), Default::default()), UpgradeOptions::default()).await;
    let mut s = raw_upgrade(port).await;
    s.write_all(&client_frame(0x0, true, b"stray")).await.unwrap();
    assert!(closed_within(&mut s, Duration::from_secs(3)).await, "a stray continuation closes the socket");
}
