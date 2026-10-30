//! PERF-20: one busy socket cannot starve another. The writer gathers at
//! most its budget (64 KiB) a wakeup, then yields; each socket has its own
//! reader and writer. While one socket streams 10,000 × 4 KB items, another
//! socket's calls stay near their unloaded p50 (PLEXUS-12: 0.6 ms with a list
//! streaming on another socket). A test binary of its own: it times.

mod ws_support;

use std::time::{Duration, Instant};

use futures::SinkExt;
use jsonrpsee::{PendingSubscriptionSink, RpcModule};
use plexus_transport::websocket::UpgradeOptions;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

async fn pings(ws: &mut Ws, n: u64) -> Vec<Duration> {
    let mut out = Vec::with_capacity(n as usize);
    for id in 0..n {
        let t = Instant::now();
        let v = request(ws, 1000 + id, "ping", json!([])).await;
        assert_eq!(v["result"], "pong");
        out.push(t.elapsed());
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streaming_socket_does_not_starve_another() {
    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    module
        .register_subscription("flood", "flood", "unflood", |_, pending: PendingSubscriptionSink, _, _| async move {
            let sink = pending.accept().await?;
            let item = serde_json::value::to_raw_value(&"x".repeat(4096))?;
            for _ in 0..10_000 {
                sink.send(item.clone()).await?;
            }
            Ok(())
        })
        .unwrap();
    let port = serve(module, UpgradeOptions::default()).await;
    let mut b = connect(port).await;
    pings(&mut b, 50).await;
    let alone = p50(pings(&mut b, 300).await);

    // Socket A streams 10,000 × 4 KB items, again and again, for as long as
    // socket B measures.
    let mut a = connect(port).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flooding = stop.clone();
    let reading = tokio::spawn(async move {
        let (mut streams, mut id) = (0u32, 1u64);
        while !flooding.load(std::sync::atomic::Ordering::Relaxed) {
            a.send(Message::text(json!({"jsonrpc": "2.0", "id": id, "method": "flood", "params": []}).to_string())).await.unwrap();
            id += 1;
            let mut items = 0u32;
            while items < 10_000 {
                match next_text(&mut a, Duration::from_secs(30)).await {
                    Some(v) if v.get("method").is_some() => items += 1,
                    Some(_) => {}
                    None => return streams,
                }
            }
            streams += 1;
        }
        streams
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let loaded = p50(pings(&mut b, 300).await);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let streams = reading.await.unwrap();
    println!("ping p50: alone {alone:?}, beside 10,000 × 4 KB streams {loaded:?}; {streams} whole streams read meanwhile");
    assert!(streams >= 1, "the stream ran beside the calls");
    assert!(
        loaded <= Duration::from_millis(2).max(alone * 4),
        "a call beside a streaming socket took {loaded:?} at p50 (alone {alone:?})"
    );
}
