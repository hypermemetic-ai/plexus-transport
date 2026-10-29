//! BIDIR-3 (spike): items into a live call over the real WebSocket — order,
//! credit, half-close, cancel, ack — with the frames BIDIR-6 builds on.
//! Run: `cargo test --test duplex_spike -- --nocapture --test-threads=1`
//! (one at a time: the numbers are timings).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use jsonrpsee::core::client::{ClientT, Subscription, SubscriptionClientT, SubscriptionKind};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use jsonrpsee::RpcModule;
use plexus_transport::duplex_spike::{epoch, DuplexHandler, DuplexRegistry, InputStream, Ordering_};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};

const NS: &str = "spike";

async fn server(window: u64, ordering: Ordering_, middleware: bool, h: DuplexHandler) -> (jsonrpsee::server::ServerHandle, String, Arc<DuplexRegistry>) {
    let reg = DuplexRegistry::new(NS, window, ordering);
    let mut m = RpcModule::new(());
    reg.register(&mut m, "duplex", h).unwrap();
    let opts = UpgradeOptions { duplex: middleware.then(|| reg.clone()), ..UpgradeOptions::default() };
    let (handle, addr) = serve_websocket_with(m, WebSocketConfig::new(0), None, opts).await.unwrap();
    (handle, format!("ws://127.0.0.1:{}", addr.port()), reg)
}

async fn client(url: &str) -> WsClient {
    WsClientBuilder::default()
        .max_buffer_capacity_per_subscription(16_384)
        .build(url)
        .await
        .unwrap()
}

async fn open(c: &WsClient) -> (Subscription<Value>, Value) {
    let sub: Subscription<Value> = c.subscribe("spike.duplex", rpc_params![], "spike.duplex_unsub").await.unwrap();
    let sid = match sub.kind() {
        SubscriptionKind::Subscription(id) => serde_json::to_value(id).unwrap(),
        _ => unreachable!(),
    };
    (sub, sid)
}

fn input(sid: &Value, seq: u64, items: Vec<Value>) -> ObjectParams {
    let mut p = ObjectParams::new();
    p.insert("subscription", sid).unwrap();
    p.insert("seq", seq).unwrap();
    p.insert("items", items).unwrap();
    p
}

fn end(sid: &Value, seq: Option<u64>) -> ObjectParams {
    let mut p = ObjectParams::new();
    p.insert("subscription", sid).unwrap();
    if let Some(s) = seq {
        p.insert("seq", s).unwrap();
    }
    p
}

async fn next(sub: &mut Subscription<Value>, ms: u64) -> Option<Value> {
    tokio::time::timeout(Duration::from_millis(ms), sub.next()).await.ok().flatten().map(|r| r.unwrap())
}

fn item(i: u64) -> Value {
    json!({"i": i, "t": epoch().elapsed().as_nanos() as u64, "pad": "x".repeat(1000)})
}

/// The method of the load tests: records (i, latency) per item, says the
/// running count every 1,000, and the total at the end.
fn recorder(log: Arc<Mutex<Vec<(u64, u64)>>>) -> DuplexHandler {
    Arc::new(move |_p, input: InputStream| {
        let log = log.clone();
        async_stream::stream! {
            let mut input = input;
            let mut n = 0u64;
            while let Some(v) = input.next().await {
                let now = epoch().elapsed().as_nanos() as u64;
                log.lock().unwrap().push((v["i"].as_u64().unwrap(), now - v["t"].as_u64().unwrap()));
                n += 1;
                if n % 1000 == 0 { yield json!({"count": n}); }
            }
            yield json!({"received": n});
        }
        .boxed()
    })
}

/// Sends `total` items one per frame, never past the credit it holds,
/// draining the output as it goes; then `{ns}.end` naming the seq it follows.
/// Returns the output items.
async fn send_under_credit(c: &WsClient, sub: &mut Subscription<Value>, sid: &Value, total: u64) -> Vec<Value> {
    let mut credit = 0u64;
    let mut out = Vec::new();
    let mut seq = 0u64;
    while seq < total {
        while credit == 0 {
            let v = next(sub, 10_000).await.expect("credit");
            if v["type"] == "credit" { credit += v["n"].as_u64().unwrap(); }
            out.push(v);
        }
        c.notification("spike.input", input(sid, seq, vec![item(seq)])).await.unwrap();
        seq += 1;
        credit -= 1;
        // Take whatever has come back without waiting.
        while let Some(Some(Ok(v))) = sub.next().now_or_never() {
            if v["type"] == "credit" { credit += v["n"].as_u64().unwrap(); }
            out.push(v);
        }
    }
    c.notification("spike.end", end(sid, Some(total))).await.unwrap();
    while let Some(v) = next(sub, 10_000).await {
        let done = v["type"] == "done";
        out.push(v);
        if done { break; }
    }
    out
}

use futures::FutureExt;

fn pct(sorted: &[u64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 - 1.0) * p) as usize] as f64 / 1e6
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_under_load_10k_credit_64() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    let began = Instant::now();
    let out = send_under_credit(&c, &mut sub, &sid, 10_000).await;
    let wall = began.elapsed();
    let log = log.lock().unwrap().clone();
    assert_eq!(log.len(), 10_000);
    assert!(log.iter().enumerate().all(|(k, (i, _))| *i == k as u64), "out of order");
    assert_eq!(out.iter().rev().nth(1).unwrap()["content"]["received"], 10_000);
    assert_eq!(out.last().unwrap()["type"], "done");
    let mut lat: Vec<u64> = log.iter().map(|(_, l)| *l).collect();
    lat.sort();
    let acks = out.iter().filter(|v| v["type"] == "ack").count();
    println!(
        "ORDER 10k×1KB credit 64: in order; p50 {:.3} ms p99 {:.3} ms max {:.3} ms; {:.0} items/s; wall {:?}; acks {acks}; hub max held {}",
        pct(&lat, 0.5), pct(&lat, 0.99), pct(&lat, 1.0), 10_000.0 / wall.as_secs_f64(), wall,
        reg.stats.max_held.load(std::sync::atomic::Ordering::Relaxed)
    );
    assert!(reg.stats.max_held.load(std::sync::atomic::Ordering::Relaxed) <= 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonrpsee_alone_reorders() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut inversions_total = 0;
    for _ in 0..3 {
        log.lock().unwrap().clear();
        let (_h, url, _reg) = server(64, Ordering_::Arrival, true, recorder(log.clone())).await;
        let c = client(&url).await;
        let (mut sub, sid) = open(&c).await;
        send_under_credit(&c, &mut sub, &sid, 10_000).await;
        let log = log.lock().unwrap().clone();
        let inversions = log.windows(2).filter(|w| w[1].0 < w[0].0).count();
        println!("ARRIVAL order, 10k one per frame: {} received, {inversions} arrivals behind the one before", log.len());
        inversions_total += inversions;
    }
    println!("ARRIVAL inversions over 3 runs: {inversions_total}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notification_reaches_the_call() {
    // Without the middleware: the notification is dropped by jsonrpsee; the
    // same frame as a request with an id is dispatched.
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, reg) = server(64, Ordering_::BySeq, false, recorder(log.clone())).await;
    let c = client(&url).await;
    let (_sub, sid) = open(&c).await;
    c.notification("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.lock().unwrap().len(), 0, "jsonrpsee dispatched a notification");
    let took: bool = c.request("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    assert!(took);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(log.lock().unwrap().len(), 1);
    drop(reg);

    // With it: the notification reaches the call.
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, _reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    let (_sub, sid) = open(&c).await;
    c.notification("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_honoured_bounds_the_hub() {
    let slow: DuplexHandler = Arc::new(|_p, input: InputStream| {
        async_stream::stream! {
            let mut input = input;
            let mut n = 0u64;
            while let Some(_) = input.next().await {
                tokio::time::sleep(Duration::from_micros(500)).await;
                n += 1;
            }
            yield json!({"received": n});
        }
        .boxed()
    });
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, slow).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    let out = send_under_credit(&c, &mut sub, &sid, 2_000).await;
    assert_eq!(out.iter().rev().nth(1).unwrap()["content"]["received"], 2_000);
    let held = reg.stats.max_held.load(std::sync::atomic::Ordering::Relaxed);
    println!("CREDIT honoured: a 0.5 ms/item method, 2,000 items: the hub held at most {held} items (window 64)");
    assert!(held <= 64);
}

/// A method that never reads its input, until it is dropped.
fn stalled(dropped: Arc<Mutex<Option<Instant>>>) -> DuplexHandler {
    Arc::new(move |_p, input: InputStream| {
        struct Guard(Arc<Mutex<Option<Instant>>>);
        impl Drop for Guard {
            fn drop(&mut self) { *self.0.lock().unwrap() = Some(Instant::now()); }
        }
        let g = Guard(dropped.clone());
        async_stream::stream! {
            let _g = g;
            let _input = input;
            futures::future::pending::<()>().await;
            yield json!(null);
        }
        .boxed()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_exceeded_ends_the_call() {
    let dropped = Arc::new(Mutex::new(None));
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, stalled(dropped.clone())).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    assert_eq!(next(&mut sub, 1000).await.unwrap(), json!({"type": "credit", "n": 64}));
    for seq in 0..65 {
        c.notification("spike.input", input(&sid, seq, vec![item(seq)])).await.unwrap();
    }
    let e = next(&mut sub, 2000).await.expect("an error item");
    println!("CREDIT exceeded: {e}");
    assert_eq!(e["type"], "error");
    assert_eq!(e["code"], "credit_exceeded");
    assert_eq!(next(&mut sub, 300).await, None, "nothing after the error");
    assert!(dropped.lock().unwrap().is_some(), "the method was dropped");
    assert_eq!(reg.open_calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_half_closes_input() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, _reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    // Many short calls: `end` sent right behind the last input, naming its seq.
    for _ in 0..50 {
        log.lock().unwrap().clear();
        let (mut sub, sid) = open(&c).await;
        let out = send_under_credit(&c, &mut sub, &sid, 50).await;
        assert_eq!(log.lock().unwrap().len(), 50);
        let tail: Vec<_> = out.iter().filter(|v| v["type"] != "credit" && v["type"] != "ack").cloned().collect();
        assert_eq!(tail, vec![json!({"type": "data", "content": {"received": 50}}), json!({"type": "done"})]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_without_seq_races() {
    // Measurement: `{ns}.end {subscription}` without the seq it follows,
    // straight after 50 one-item frames. jsonrpsee spawns a task per frame,
    // so the end can be handled before the frames it follows.
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, _reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    let mut short = 0;
    for _ in 0..200 {
        log.lock().unwrap().clear();
        let (mut sub, sid) = open(&c).await;
        next(&mut sub, 1000).await.unwrap();
        for seq in 0..50 {
            c.notification("spike.input", input(&sid, seq, vec![item(seq)])).await.unwrap();
        }
        c.notification("spike.end", end(&sid, None)).await.unwrap();
        while let Some(v) = next(&mut sub, 2000).await {
            if v["type"] == "done" { break; }
        }
        if log.lock().unwrap().len() < 50 { short += 1; }
    }
    println!("END without seq: {short} of 200 calls lost items to an end that overtook them");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn output_ends_first_input_closed_once() {
    let three: DuplexHandler = Arc::new(|_p, input: InputStream| {
        async_stream::stream! {
            let mut input = input;
            for _ in 0..3 { input.next().await; }
            yield json!({"took": 3});
        }
        .boxed()
    });
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, three).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    next(&mut sub, 1000).await.unwrap();
    for seq in 0..3 {
        c.notification("spike.input", input(&sid, seq, vec![item(seq)])).await.unwrap();
    }
    let mut got = Vec::new();
    while let Some(v) = next(&mut sub, 2000).await {
        let d = v["type"] == "done";
        got.push(v);
        if d { break; }
    }
    println!("OUTPUT first: {got:?}");
    let closed: Vec<_> = got.iter().filter(|v| v["type"] == "input_closed").collect();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0]["ack"], 2);
    assert_eq!(got.last().unwrap()["type"], "done");
    let before = reg.stats.unknown.load(std::sync::atomic::Ordering::Relaxed);
    c.notification("spike.input", input(&sid, 3, vec![item(3)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(reg.stats.unknown.load(std::sync::atomic::Ordering::Relaxed), before + 1, "a late frame reaches nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_ends_both_halves() {
    let mut lat = Vec::new();
    for _ in 0..50 {
        let dropped = Arc::new(Mutex::new(None));
        let (_h, url, reg) = server(64, Ordering_::BySeq, true, stalled(dropped.clone())).await;
        let c = client(&url).await;
        let (mut sub, sid) = open(&c).await;
        next(&mut sub, 1000).await.unwrap();
        c.notification("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
        let t0 = Instant::now();
        sub.unsubscribe().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let t1 = loop {
            if let Some(t) = *dropped.lock().unwrap() { break t; }
            assert!(Instant::now() < deadline, "the method outlived its cancel");
            tokio::time::sleep(Duration::from_micros(200)).await;
        };
        lat.push(t1.saturating_duration_since(t0).as_nanos() as u64);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(reg.open_calls(), 0);
    }
    lat.sort();
    println!("CANCEL → method dropped: p50 {:.3} ms p99 {:.3} ms max {:.3} ms (50 calls)", pct(&lat, 0.5), pct(&lat, 0.99), pct(&lat, 1.0));
    assert!(pct(&lat, 1.0) <= 100.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_ack() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    next(&mut sub, 1000).await.unwrap();
    for seq in 0..40 {
        c.notification("spike.input", input(&sid, seq, vec![item(seq)])).await.unwrap();
    }
    // The ack that comes with the first re-grant.
    let ack = loop {
        let v = next(&mut sub, 2000).await.expect("an ack");
        if v["type"] == "ack" { break v["seq"].as_u64().unwrap(); }
    };
    println!("ACK after 40 frames: seq {ack}");
    assert!(ack >= 31 && ack <= 39);
    // A client unsure what landed resends from 0: everything at or below
    // the delivered seq is dropped, the rest is new.
    for seq in 0..45 {
        c.notification("spike.input", input(&sid, seq, vec![item(seq)])).await.unwrap();
    }
    c.notification("spike.end", end(&sid, Some(45))).await.unwrap();
    while let Some(v) = next(&mut sub, 2000).await {
        if v["type"] == "done" { break; }
    }
    let log = log.lock().unwrap();
    assert_eq!(log.len(), 45);
    assert!(log.iter().enumerate().all(|(k, (i, _))| *i == k as u64));
    assert_eq!(reg.stats.duplicates.load(std::sync::atomic::Ordering::Relaxed), 40);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_from_another_connection_refused() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, recorder(log.clone())).await;
    let a = client(&url).await;
    let b = client(&url).await;
    let (_sub, sid) = open(&a).await;
    b.notification("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    let took: bool = b.request("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!took);
    assert_eq!(log.lock().unwrap().len(), 0);
    assert_eq!(reg.stats.unknown.load(std::sync::atomic::Ordering::Relaxed), 2);
    a.notification("spike.input", input(&sid, 0, vec![item(0)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(log.lock().unwrap().len(), 1);
}

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    s.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 4
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_per_open_call() {
    let dropped = Arc::new(Mutex::new(None));
    let (_h, url, reg) = server(64, Ordering_::BySeq, true, stalled(dropped)).await;
    let c = client(&url).await;
    // Warm: one call opened and dropped, so allocator pools exist.
    { let (s, _) = open(&c).await; s.unsubscribe().await.unwrap(); }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before = rss_kb();
    let mut subs = Vec::new();
    for _ in 0..1000 {
        subs.push(open(&c).await);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = rss_kb();
    assert_eq!(reg.open_calls(), 1000);
    println!("MEMORY: 1,000 open duplex calls, client and server in one process: {} KB RSS, {:.1} KB per call (an upper bound for the hub)", after - before, (after - before) as f64 / 1000.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throughput_batched_frames() {
    // The same 10,000 × 1 KB under credit 64, sent 16 items to a frame.
  for window in [64u64, 256, 1024] {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (_h, url, reg) = server(window, Ordering_::BySeq, true, recorder(log.clone())).await;
    let c = client(&url).await;
    let (mut sub, sid) = open(&c).await;
    let began = Instant::now();
    let (mut credit, mut seq, mut sent) = (0u64, 0u64, 0u64);
    while sent < 10_000 {
        while credit < 16 {
            let v = next(&mut sub, 10_000).await.expect("credit");
            if v["type"] == "credit" { credit += v["n"].as_u64().unwrap(); }
        }
        let batch: Vec<Value> = (sent..(sent + 16).min(10_000)).map(item).collect();
        let n = batch.len() as u64;
        c.notification("spike.input", input(&sid, seq, batch)).await.unwrap();
        seq += 1;
        sent += n;
        credit -= n;
        while let Some(Some(Ok(v))) = sub.next().now_or_never() {
            if v["type"] == "credit" { credit += v["n"].as_u64().unwrap(); }
        }
    }
    c.notification("spike.end", end(&sid, Some(seq))).await.unwrap();
    while let Some(v) = next(&mut sub, 10_000).await {
        if v["type"] == "done" { break; }
    }
    let wall = began.elapsed();
    let log = log.lock().unwrap().clone();
    assert_eq!(log.len(), 10_000);
    assert!(log.iter().enumerate().all(|(k, (i, _))| *i == k as u64));
    let mut lat: Vec<u64> = log.iter().map(|(_, l)| *l).collect();
    lat.sort();
    println!(
        "BATCHED 16/frame, 10k×1KB credit {window}: in order; p50 {:.3} ms p99 {:.3} ms; {:.0} items/s; hub max held {}",
        pct(&lat, 0.5), pct(&lat, 0.99), 10_000.0 / wall.as_secs_f64(),
        reg.stats.max_held.load(std::sync::atomic::Ordering::Relaxed)
    );
  }
}
