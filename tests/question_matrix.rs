//! BIDIR-3 (spike): today's in-stream question — `Request { requestId }` out,
//! `{ns}.respond { request_id, response_data }` back — over the real
//! WebSocket (two principals) and the way `serve_stdio` drives the module.
//! The hub's own `{ns}.respond` (plexus-core `arc_into_rpc_module`) answers;
//! the asking method pipes its stream exactly as `pipe_stream_to_subscription`
//! does today. Run: `cargo test --test question_matrix -- --nocapture --test-threads=1`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use jsonrpsee::core::client::{ClientT, Subscription, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{HeaderMap, HeaderValue, WsClient, WsClientBuilder};
use jsonrpsee::RpcModule;
use plexus_core::plexus::bidirectional::registry::pending_count;
use plexus_core::plexus::streaming::create_bidir_stream;
use plexus_core::plexus::types::{PlexusStreamItem, StreamMetadata};
use plexus_core::plexus::{DynamicHub, StandardRequest, StandardResponse, TestSessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};

/// `pending_count` is process-wide: one question test at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn module() -> RpcModule<()> {
    let hub = Arc::new(DynamicHub::new("q"));
    let mut m = DynamicHub::arc_into_rpc_module(hub).unwrap();
    // `q.ask {n, timeout_ms}`: asks n confirms in turn, says each answer.
    m.register_subscription("q.ask", "q.ask", "q.ask_unsub", |params, pending, _, _| async move {
        let p: Value = params.parse().unwrap_or(json!({}));
        let p = if let Value::Array(a) = p { a.into_iter().next().unwrap_or(json!({})) } else { p };
        let n = p["n"].as_u64().unwrap_or(1);
        let t = Duration::from_millis(p["timeout_ms"].as_u64().unwrap_or(30_000));
        let (chan, wrap) = create_bidir_stream::<StandardRequest, StandardResponse>("q.ask", vec!["q".into()]);
        let user = async_stream::stream! {
            for i in 0..n {
                let r = chan.request_with_timeout(StandardRequest::Confirm { message: format!("q{i}?"), default: None }, t).await;
                let answer = match r { Ok(StandardResponse::Confirmed { value }) => json!(value), other => json!(format!("{other:?}")) };
                yield PlexusStreamItem::data(StreamMetadata::new(vec!["q".into()], String::new()), "q.ask".into(), json!({"i": i, "answer": answer}));
            }
            yield PlexusStreamItem::Done { metadata: StreamMetadata::new(vec!["q".into()], String::new()) };
        };
        let mut stream = wrap(Box::pin(user));
        // As `pipe_stream_to_subscription` does today: no watch on the sink's close.
        let sink = pending.accept().await?;
        while let Some(item) = stream.next().await {
            sink.send(serde_json::value::to_raw_value(&item)?).await?;
        }
        Ok(())
    })
    .unwrap();
    m
}

async fn ws_server() -> String {
    let opts = UpgradeOptions { reject_on_session_failure: true, ..UpgradeOptions::default() };
    let (h, addr) = serve_websocket_with(module(), WebSocketConfig::new(0), Some(Arc::new(TestSessionValidator::new())), opts)
        .await
        .unwrap();
    std::mem::forget(h);
    format!("ws://127.0.0.1:{}", addr.port())
}

async fn as_principal(url: &str, who: &str) -> WsClient {
    let mut h = HeaderMap::new();
    h.insert("cookie", HeaderValue::from_str(&format!("session={who}")).unwrap());
    WsClientBuilder::default().set_headers(h).build(url).await.unwrap()
}

fn yes() -> Value {
    serde_json::to_value(StandardResponse::<Value>::Confirmed { value: true }).unwrap()
}

fn respond(id: &str) -> ObjectParams {
    let mut p = ObjectParams::new();
    p.insert("request_id", id).unwrap();
    p.insert("response_data", yes()).unwrap();
    p
}

async fn next(sub: &mut Subscription<Value>) -> Value {
    tokio::time::timeout(Duration::from_secs(5), sub.next()).await.unwrap().unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_question_round_trip() {
    let _s = SERIAL.lock().await;
    let url = ws_server().await;
    let alice = as_principal(&url, "alice").await;
    let mut sub: Subscription<Value> = alice.subscribe("q.ask", rpc_params![json!({"n": 200})], "q.ask_unsub").await.unwrap();
    let mut rt = Vec::new();
    for i in 0..200 {
        let q = next(&mut sub).await;
        assert_eq!(q["type"], "request", "{q}");
        assert!(q["requestId"].is_string() && q["timeoutMs"] == 30_000);
        let t0 = Instant::now();
        let ok: Value = alice.request("q.respond", respond(q["requestId"].as_str().unwrap())).await.unwrap();
        assert_eq!(ok, json!({"success": true}));
        let a = next(&mut sub).await;
        rt.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(a["content"], json!({"i": i, "answer": true}));
    }
    assert_eq!(next(&mut sub).await["type"], "done");
    rt.sort();
    println!(
        "WS question round trip (respond sent → next item), 200: p50 {:.3} ms p99 {:.3} ms",
        rt[100] as f64 / 1e6,
        rt[198] as f64 / 1e6
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_second_connection_answers() {
    let _s = SERIAL.lock().await;
    let url = ws_server().await;
    let alice = as_principal(&url, "alice").await;
    let bob = as_principal(&url, "bob").await;
    let mut sub: Subscription<Value> = alice.subscribe("q.ask", rpc_params![json!({"n": 1})], "q.ask_unsub").await.unwrap();
    let q = next(&mut sub).await;
    // bob's socket, bob's principal, alice's question.
    let ok: Value = bob.request("q.respond", respond(q["requestId"].as_str().unwrap())).await.unwrap();
    let a = next(&mut sub).await;
    println!("WS wrong peer: bob answered alice's question → {ok}; alice's method got {}", a["content"]);
    assert_eq!(ok, json!({"success": true}), "today: accepted (the hole)");
    assert_eq!(a["content"]["answer"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_dropped_stream_leaves_entry() {
    let _s = SERIAL.lock().await;
    let url = ws_server().await;
    let alice = as_principal(&url, "alice").await;
    let base = pending_count();
    let mut sub: Subscription<Value> =
        alice.subscribe("q.ask", rpc_params![json!({"n": 1, "timeout_ms": 1500})], "q.ask_unsub").await.unwrap();
    let q = next(&mut sub).await;
    assert_eq!(q["type"], "request");
    let t0 = Instant::now();
    sub.unsubscribe().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_cancel = pending_count() - base;
    let gone = loop {
        if pending_count() == base { break t0.elapsed(); }
        assert!(t0.elapsed() < Duration::from_secs(5), "never cleaned");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!("WS dropped mid-question: {after_cancel} entry left 300 ms after the unsubscribe; gone after {gone:?} (its timeout 1.5 s)");
    assert_eq!(after_cancel, 1, "today: the entry outlives the call");
    assert!(gone >= Duration::from_millis(1000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_question_round_trip() {
    let _s = SERIAL.lock().await;
    // `serve_stdio` is this loop: one line → `raw_json_request` → the
    // response line, then the subscription's notifications as lines.
    let m = module();
    let (resp, mut rx) = m
        .raw_json_request(r#"{"jsonrpc":"2.0","id":1,"method":"q.ask","params":[{"n":1}]}"#, 64)
        .await
        .unwrap();
    assert!(resp.get().contains("\"result\""), "{}", resp.get());
    let line: Value = serde_json::from_str(rx.recv().await.unwrap().get()).unwrap();
    let q = &line["params"]["result"];
    assert_eq!(q["type"], "request");
    let t0 = Instant::now();
    let answer = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"q.respond","params":{{"request_id":"{}","response_data":{}}}}}"#,
        q["requestId"].as_str().unwrap(),
        yes()
    );
    let (r, _) = m.raw_json_request(&answer, 64).await.unwrap();
    assert!(r.get().contains("success"), "{}", r.get());
    let a: Value = serde_json::from_str(rx.recv().await.unwrap().get()).unwrap();
    println!("STDIO question round trip: {:?}; answer {}", t0.elapsed(), a["params"]["result"]["content"]);
    assert_eq!(a["params"]["result"]["content"]["answer"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_notification_line() {
    let _s = SERIAL.lock().await;
    // A notification line (no id), as `{ns}.input` would be: what does the
    // module — and so `serve_stdio`'s `?` — make of it?
    let m = module();
    let r = m.raw_json_request(r#"{"jsonrpc":"2.0","method":"q.respond","params":{"request_id":"x","response_data":null}}"#, 64).await;
    println!("STDIO notification line: {:?}", r.as_ref().map(|(v, _)| v.get().to_string()).map_err(|e| e.to_string()));
    // `serve_stdio` does `.map_err(..)?` on this result: an Err ends the loop.
    assert!(r.is_err(), "raw_json_request accepted a notification");
}
