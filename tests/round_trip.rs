//! The accept loop AUTH-11 took over from jsonrpsee's server must set
//! TCP_NODELAY as jsonrpsee did: without it every small reply waits on
//! Nagle and the peer's delayed ACK, and a call costs ~40 ms (menger's
//! diagrams wire test caught it at a 41 ms median).

use std::time::{Duration, Instant};

use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::PendingSubscriptionSink;
use jsonrpsee::ws_client::WsClientBuilder;
use jsonrpsee::RpcModule;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;

#[tokio::test]
async fn a_warm_call_is_not_held_by_nagle() {
    let mut module = RpcModule::new(());
    // A call as Plexus makes one: a subscription that streams items, so
    // several small frames go out back to back (the second is what Nagle
    // holds until the peer's delayed ACK).
    module
        .register_subscription("pings", "ping", "unping", |_, pending: PendingSubscriptionSink, _, _| async move {
            let sink = pending.accept().await?;
            for i in 0..3 {
                sink.send(serde_json::value::to_raw_value(&i)?).await?;
            }
            Ok(())
        })
        .unwrap();
    let (_handle, addr) =
        serve_websocket_with(module, WebSocketConfig::new(0), None, UpgradeOptions::default()).await.unwrap();
    let c = WsClientBuilder::default().build(format!("ws://127.0.0.1:{}", addr.port())).await.unwrap();
    let mut times = Vec::new();
    for _ in 0..60 {
        let began = Instant::now();
        let mut sub: Subscription<u32> = c.subscribe("pings", rpc_params![], "unping").await.unwrap();
        for want in 0..3 {
            assert_eq!(sub.next().await.unwrap().unwrap(), want);
        }
        times.push(began.elapsed());
    }
    times.sort();
    let median = times[30];
    assert!(median < Duration::from_millis(15), "median {median:?}");
}
