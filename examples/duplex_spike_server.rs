//! BIDIR-3 (spike): a server for the JS client (`spike/browser.mjs`) — the
//! duplex frames and today's question, on one WebSocket, for a client that
//! speaks the browser's WebSocket API. `cargo run --example duplex_spike_server -- 4482`

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use jsonrpsee::RpcModule;
use plexus_core::plexus::streaming::create_bidir_stream;
use plexus_core::plexus::types::{PlexusStreamItem, StreamMetadata};
use plexus_core::plexus::{DynamicHub, StandardRequest, StandardResponse};
use plexus_transport::duplex_spike::{DuplexHandler, DuplexRegistry, InputStream, Ordering_};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};

#[tokio::main]
async fn main() {
    let port: u16 = std::env::args().nth(1).and_then(|p| p.parse().ok()).unwrap_or(4482);
    let hub = Arc::new(DynamicHub::new("spike"));
    let mut m: RpcModule<()> = DynamicHub::arc_into_rpc_module(hub).unwrap();
    let reg = DuplexRegistry::new("spike", 64, Ordering_::BySeq);
    // `spike.duplex`: checks order itself; says the count and whether in order.
    let order: DuplexHandler = Arc::new(|_p, input: InputStream| {
        async_stream::stream! {
            let mut input = input;
            let (mut n, mut ok) = (0u64, true);
            while let Some(v) = input.next().await {
                ok &= v["i"].as_u64() == Some(n);
                n += 1;
            }
            yield json!({"received": n, "in_order": ok});
        }
        .boxed()
    });
    reg.register(&mut m, "duplex", order).unwrap();
    // `spike.hold`: never reads; held open for the memory count.
    let hold: DuplexHandler = Arc::new(|_p, input: InputStream| {
        async_stream::stream! {
            let _i = input;
            futures::future::pending::<()>().await;
            yield Value::Null;
        }
        .boxed()
    });
    reg.register(&mut m, "hold", hold).unwrap();
    m.register_subscription("spike.ask", "spike.ask", "spike.ask_unsub", |params, pending, _, _| async move {
        let p: Value = params.parse().unwrap_or(json!({}));
        let p = if let Value::Array(a) = p { a.into_iter().next().unwrap_or(json!({})) } else { p };
        let n = p["n"].as_u64().unwrap_or(1);
        let (chan, wrap) = create_bidir_stream::<StandardRequest, StandardResponse>("spike.ask", vec!["spike".into()]);
        let user = async_stream::stream! {
            for i in 0..n {
                let r = chan.request_with_timeout(StandardRequest::Confirm { message: format!("q{i}?"), default: None }, Duration::from_secs(30)).await;
                let answer = match r { Ok(StandardResponse::Confirmed { value }) => json!(value), other => json!(format!("{other:?}")) };
                yield PlexusStreamItem::data(StreamMetadata::new(vec![], String::new()), "spike.ask".into(), json!({"i": i, "answer": answer}));
            }
            yield PlexusStreamItem::Done { metadata: StreamMetadata::new(vec![], String::new()) };
        };
        let mut stream = wrap(Box::pin(user));
        let sink = pending.accept().await?;
        while let Some(item) = stream.next().await {
            sink.send(serde_json::value::to_raw_value(&item)?).await?;
        }
        Ok(())
    })
    .unwrap();
    let opts = UpgradeOptions { duplex: Some(reg.clone()), ..UpgradeOptions::default() };
    let (_h, addr) = serve_websocket_with(m, WebSocketConfig::new(port), None, opts).await.unwrap();
    println!("listening {addr} pid {}", std::process::id());
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
