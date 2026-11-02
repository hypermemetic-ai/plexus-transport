//! PERF-8: a client that offers `batch` at its upgrade (spec/02) is sent the
//! items a stream has ready together in one notification, `results`; a client
//! that offers nothing, or an offer the upgrade cannot read, is sent exactly
//! today's frames, one an item.

mod ws_support;

use std::time::Duration;

use futures::SinkExt;
use jsonrpsee::RpcModule;
use plexus_transport::websocket::UpgradeOptions;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

/// `count`: `n` items, every one ready at once, through plexus-core's pipe.
fn counting(n: u64) -> RpcModule<()> {
    let mut module = RpcModule::new(());
    module
        .register_subscription("count", "count", "count_unsub", move |_, pending, _, ext| async move {
            let sink = pending.accept().await?;
            let items = futures::stream::iter((0..n).map(|n| json!({"type": "item", "n": n})));
            let stream = plexus_core::plexus::wrap_stream(items, "test.count", vec!["test".into()]);
            plexus_core::plexus::wire::pipe(sink, stream, &ext).await
        })
        .unwrap();
    module
}

/// A client upgraded at `path`, which calls `count` once: the items of each
/// notification it is sent, until the terminal one, and whether each came
/// as a batch.
async fn notes(port: u16, path: &str) -> Vec<(bool, usize)> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let (mut ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{port}{path}"), tcp).await.expect("upgrade");
    ws.send(Message::text(json!({"jsonrpc": "2.0", "id": 1, "method": "count", "params": []}).to_string())).await.unwrap();
    let mut out = Vec::new();
    loop {
        let v: Value = next_text(&mut ws, Duration::from_secs(10)).await.expect("a frame");
        let Some(params) = v.get("params") else { continue };
        let (batched, items) = match params.get("results") {
            Some(items) => (true, items.as_array().unwrap().clone()),
            None => (false, vec![params["result"].clone()]),
        };
        if batched {
            assert!(params["metadata"]["provenance"].is_array(), "the batch's metadata, once: {params}");
            assert!(items.iter().filter(|i| i["type"] == "data").all(|i| i.get("metadata").is_none()), "a data item of the batch's leaves its own out");
        }
        let terminal = items.iter().any(|i| i["type"] == "done" || i["type"] == "error");
        out.push((batched, items.len()));
        if terminal {
            ws.close(None).await.ok();
            return out;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_offers_batch_is_sent_what_is_ready_together() {
    let port = serve(counting(200), UpgradeOptions::default()).await;
    let got = notes(port, "/?batch").await;
    assert_eq!(got, vec![(true, 64), (true, 64), (true, 64), (true, 9)], "200 items and `done`, 64 to a notification");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_offers_nothing_is_sent_todays_frames() {
    let port = serve(counting(200), UpgradeOptions::default()).await;
    let got = notes(port, "/").await;
    assert_eq!(got.len(), 201);
    assert!(got.iter().all(|&(batched, n)| !batched && n == 1), "one an item, `result`");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_may_ask_for_fewer_and_never_gets_more_than_64() {
    let port = serve(counting(20), UpgradeOptions::default()).await;
    assert_eq!(notes(port, "/?batch=8").await, vec![(true, 8), (true, 8), (true, 5)]);
    let port = serve(counting(200), UpgradeOptions::default()).await;
    assert!(notes(port, "/?x=1&batch=1000").await.iter().all(|&(_, n)| n <= 64), "the service's cap");
}

/// An offer the upgrade cannot read is no offer, as an unknown query is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offer_that_does_not_read_is_no_offer() {
    let port = serve(counting(20), UpgradeOptions::default()).await;
    for path in ["/?batch=lots", "/?batch=1", "/?batch=0", "/?batches", "/?nobatch"] {
        let got = notes(port, path).await;
        assert!(got.iter().all(|&(batched, n)| !batched && n == 1), "{path}: one an item: {got:?}");
    }
}
