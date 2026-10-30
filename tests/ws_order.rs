//! PERF-20: one reader per socket — frames reach the method table in the
//! order they came, and a frame that answers no request of the server's is
//! dropped, never answered `-32600` (spec/14 rule 2, LOCAL-7).
//!
//! Today jsonrpsee-server spawns a task for every inbound frame, so a frame
//! can be handled after a later one (BIDIR-14 measured ~9 %), and it answers
//! a response frame `{"error":{"code":-32600}}`.

mod ws_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::SinkExt;
use jsonrpsee::RpcModule;
use plexus_transport::websocket::UpgradeOptions;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

/// 10,000 calls written back to back, one write: the method table sees them
/// in the order they were sent, and every one is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_thousand_frames_reach_the_method_table_in_arrival_order() {
    const N: u64 = 10_000;
    let seen = Arc::new(Mutex::new(Vec::with_capacity(N as usize)));
    let mut module = RpcModule::new(());
    let s = seen.clone();
    module
        .register_method("rec", move |params, _, _| {
            let n: u64 = params.one().unwrap();
            s.lock().unwrap().push(n);
            n
        })
        .unwrap();
    let port = serve(module, UpgradeOptions::default()).await;
    // The writes on a task of their own, so the answers are read meanwhile.
    let (mut tx, mut rx) = futures::StreamExt::split(connect(port).await);
    let writing = tokio::spawn(async move {
        for n in 0..N {
            let f = json!({"jsonrpc": "2.0", "id": n, "method": "rec", "params": [n]});
            tx.feed(Message::text(f.to_string())).await.unwrap();
        }
        tx.flush().await.unwrap();
        tx
    });
    let mut answered = 0;
    while answered < N {
        let v = next_text(&mut rx, Duration::from_secs(20)).await.expect("every call answered");
        assert!(v.get("result").is_some(), "{v}");
        answered += 1;
    }
    drop(writing.await.unwrap());
    let seen = seen.lock().unwrap();
    let late = seen.windows(2).filter(|w| w[1] < w[0]).count();
    println!("{N} frames: {late} handled after a later one");
    assert_eq!(seen.len() as u64, N);
    assert_eq!(late, 0, "every frame in arrival order");
}

/// A response frame — no `method`, an `id` with `result` or `error` — answers
/// a request of the receiver's; the server has none open, so it is dropped:
/// nothing is answered, and the socket goes on. (An object with no `method`
/// and neither is still `-32600`: spec/01, the kit's `envelope.parse_error`.)
#[tokio::test]
async fn an_answer_to_no_request_is_dropped_not_answered() {
    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    let port = serve(module, UpgradeOptions::default()).await;
    let mut ws = connect(port).await;
    for f in [
        json!({"jsonrpc": "2.0", "id": 5, "result": 1}),
        json!({"jsonrpc": "2.0", "id": 6, "error": {"code": -32000, "message": "no"}}),
        json!({"jsonrpc": "2.0", "id": "s1", "result": {"subscription": 7}}),
    ] {
        ws.send(Message::text(f.to_string())).await.unwrap();
    }
    ws.send(Message::text(json!({"jsonrpc": "2.0", "id": 77}).to_string())).await.unwrap();
    ws.send(Message::text(json!({"jsonrpc": "2.0", "id": 9, "method": "ping"}).to_string())).await.unwrap();
    let mut got: Vec<Value> = Vec::new();
    while let Some(v) = next_text(&mut ws, Duration::from_millis(500)).await.filter(|_| got.len() < 10) {
        let done = v["id"] == 9;
        got.push(v);
        if done {
            break;
        }
    }
    let answered: Vec<&Value> = got.iter().filter(|v| v["id"] != 9).collect();
    assert_eq!(answered.len(), 1, "only the object with no method and no result is refused: {got:?}");
    assert_eq!(answered[0]["id"], 77);
    assert_eq!(answered[0]["error"]["code"], -32600);
    assert_eq!(got.last().unwrap()["result"], "pong", "the socket goes on: {got:?}");
}
