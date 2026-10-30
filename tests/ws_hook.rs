//! PERF-20 → BIDIR-12: the connection's hook and its writer handle, in place
//! of LOCAL-3's vendored `FrameHook`. The hook sees every inbound frame in
//! arrival order, before the method table, and may take one; the writer
//! handle lets this side write a request of its own on a socket it accepted,
//! and the hook takes its answer; when the socket goes, the hook hears it
//! (every call this side opened ends `connection_lost`). LOCAL-3's 15
//! symmetric tests move to BIDIR-12; this is the smoke test of the seam.

mod ws_support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonrpsee::RpcModule;
use plexus_transport::websocket::UpgradeOptions;
use plexus_transport::ws::{ConnectionHook, Encoding, Hooks, Outbound};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

/// Records every frame's `params[0]` in order; takes the `hooked` calls.
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl ConnectionHook for Recorder {
    fn on_frame(&self, frame: &[u8]) -> bool {
        let v: Value = serde_json::from_slice(frame).unwrap();
        self.seen.lock().unwrap().push(v["params"][0].as_u64().unwrap());
        v["method"] == "hooked"
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hook_sees_every_frame_in_order_and_can_take_one() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let table = Arc::new(Mutex::new(Vec::new()));
    let hooked_calls = Arc::new(AtomicUsize::new(0));
    let mut module = RpcModule::new(());
    let t = table.clone();
    module
        .register_method("rec", move |p, _, _| {
            let n: u64 = p.one().unwrap();
            t.lock().unwrap().push(n);
            n
        })
        .unwrap();
    let h = hooked_calls.clone();
    module.register_method("hooked", move |_, _, _| h.fetch_add(1, Ordering::SeqCst)).unwrap();
    let s = seen.clone();
    let hooks = Hooks::new(move |_conn, _ext| Some(Arc::new(Recorder { seen: s.clone() }) as Arc<dyn ConnectionHook>));
    let port = serve(module, UpgradeOptions { hooks: Some(hooks), ..UpgradeOptions::default() }).await;

    let (mut tx, mut rx) = connect(port).await.split();
    for n in 0..1000u64 {
        let method = if n % 2 == 0 { "rec" } else { "hooked" };
        tx.feed(Message::text(json!({"jsonrpc": "2.0", "id": n, "method": method, "params": [n]}).to_string())).await.unwrap();
    }
    tx.flush().await.unwrap();
    for _ in 0..500 {
        let v = next_text(&mut rx, Duration::from_secs(10)).await.unwrap();
        assert_eq!(v["result"].as_u64().unwrap() % 2, 0, "only the calls the hook left are answered: {v}");
    }
    tx.send(Message::text(json!({"jsonrpc": "2.0", "id": 5000, "method": "rec", "params": [5000]}).to_string())).await.unwrap();
    assert_eq!(next_text(&mut rx, Duration::from_secs(5)).await.unwrap()["id"], 5000, "nothing else was answered");

    let seen = seen.lock().unwrap();
    assert_eq!(&seen[..1000], &(0..1000).collect::<Vec<u64>>()[..], "the hook saw every frame, in order");
    let table = table.lock().unwrap();
    assert_eq!(&table[..500], &(0..1000).step_by(2).collect::<Vec<u64>>()[..], "the method table saw the rest, in order");
    assert_eq!(hooked_calls.load(Ordering::SeqCst), 0, "a frame the hook took never reaches the method table");
}

/// Takes the answer to this side's request `id: 1`.
struct Answers {
    got: tokio::sync::mpsc::UnboundedSender<Value>,
    closed: Arc<AtomicBool>,
}

impl ConnectionHook for Answers {
    fn on_frame(&self, frame: &[u8]) -> bool {
        let v: Value = serde_json::from_slice(frame).unwrap();
        let mine = v.get("method").is_none() && v["id"] == 1 && v.get("result").is_some();
        if mine {
            let _ = self.got.send(v);
        }
        mine
    }
    fn on_close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn this_side_writes_a_request_on_an_accepted_socket_and_gets_its_answer() {
    let (got_tx, mut got_rx) = tokio::sync::mpsc::unbounded_channel();
    let closed = Arc::new(AtomicBool::new(false));
    let writers = Arc::new(Mutex::new(Vec::new()));
    let (c, w) = (closed.clone(), writers.clone());
    let hooks = Hooks::new(move |conn, _ext| {
        assert_eq!(conn.encoding(), Some(Encoding::Json), "the encoding is fixed at the upgrade");
        let writer = conn.writer().expect("an upgraded connection has a writer");
        w.lock().unwrap().push(writer.clone());
        tokio::spawn(async move {
            let request = json!({"jsonrpc": "2.0", "id": 1, "method": "client.ping", "params": {}}).to_string();
            writer.send(Outbound::Text(request)).await.unwrap();
        });
        Some(Arc::new(Answers { got: got_tx.clone(), closed: c.clone() }) as Arc<dyn ConnectionHook>)
    });
    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    let port = serve(module, UpgradeOptions { hooks: Some(hooks), ..UpgradeOptions::default() }).await;

    let mut ws = connect(port).await;
    let asked = next_text(&mut ws, Duration::from_secs(5)).await.expect("the server's request");
    assert_eq!((asked["id"].clone(), asked["method"].clone()), (json!(1), json!("client.ping")), "{asked}");
    ws.send(Message::text(json!({"jsonrpc": "2.0", "id": 1, "result": "pong"}).to_string())).await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(5), got_rx.recv()).await.unwrap().unwrap();
    assert_eq!(answer["result"], "pong");
    assert_eq!(request(&mut ws, 1, "ping", json!([])).await["result"], "pong", "the client's own call, same id, both ways");

    drop(ws);
    for _ in 0..100 {
        if closed.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(closed.load(Ordering::SeqCst), "the hook hears the socket go");
    let writer = writers.lock().unwrap().pop().unwrap();
    tokio::time::timeout(Duration::from_secs(2), writer.closed()).await.expect("the writer handle sees the socket gone");
    assert!(writer.send(Outbound::Text("{}".into())).await.is_err(), "a write after the socket went is connection_lost");
}
