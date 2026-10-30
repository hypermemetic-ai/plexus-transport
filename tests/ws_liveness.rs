//! PERF-20 / RPC-5: a socket's liveness is the transport's. It pings on an
//! interval; a peer silent past the ping timeout (no frame of any kind, a
//! pong included) is closed — a dead peer found by ping; and under an idle
//! limit, a socket with no call open and none begun for that long is closed,
//! one that never made a call included (RPC-5's probe: at `--idle-timeout-secs
//! 5` a socket that never called stayed open indefinitely). A socket with a
//! call open is never idle.

mod ws_support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use jsonrpsee::{PendingSubscriptionSink, RpcModule};
use plexus_transport::websocket::UpgradeOptions;
use plexus_transport::ws::SocketLimits;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use ws_support::*;

fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |_, _, _| "pong").unwrap();
    m.register_subscription("hold", "hold", "unhold", |_, pending: PendingSubscriptionSink, _, _| async move {
        let sink = pending.accept().await?;
        sink.closed().await;
        Ok(())
    })
    .unwrap();
    m
}

fn limits(f: impl FnOnce(&mut SocketLimits)) -> UpgradeOptions {
    let mut limits = SocketLimits::default();
    f(&mut limits);
    UpgradeOptions { limits, ..UpgradeOptions::default() }
}

/// The defaults: today's message cap, a ping every 30 s, no idle limit.
#[test]
fn the_defaults_are_todays_or_the_gaps() {
    let d = SocketLimits::default();
    assert_eq!(d.max_message_size, 10 * 1024 * 1024);
    assert_eq!(d.ping_interval, Some(Duration::from_secs(30)));
    assert!(d.ping_timeout > d.ping_interval.unwrap(), "a ping has time to be answered");
    assert_eq!(d.idle_timeout, None, "idle is the service's to set (menger: --idle-timeout-secs)");
    assert!(d.header_timeout <= Duration::from_secs(10), "RPC-4: a few seconds");
    assert_eq!(d.queue, 1024);
}

#[tokio::test]
async fn the_server_pings_on_its_interval() {
    let port = serve(module(), limits(|l| l.ping_interval = Some(Duration::from_millis(100)))).await;
    let mut ws = connect(port).await;
    let mut pings = 0;
    let until = tokio::time::Instant::now() + Duration::from_millis(550);
    while let Ok(Some(Ok(m))) = tokio::time::timeout_at(until, ws.next()).await {
        if let Message::Ping(_) = m {
            pings += 1;
        }
    }
    assert!(pings >= 3, "{pings} pings in 550 ms at a 100 ms interval");
}

/// A dead peer: upgraded, then nothing — no pong, no frame. Closed past the
/// ping timeout.
#[tokio::test]
async fn a_silent_peer_is_closed_past_the_ping_timeout() {
    let port = serve(
        module(),
        limits(|l| {
            l.ping_interval = Some(Duration::from_millis(100));
            l.ping_timeout = Duration::from_millis(600);
        }),
    )
    .await;
    let mut s = raw_upgrade(port).await;
    let began = Instant::now();
    assert!(closed_within(&mut s, Duration::from_secs(4)).await, "a silent peer is closed");
    assert!(began.elapsed() >= Duration::from_millis(500), "closed at {:?}, before the timeout", began.elapsed());
}

/// A live peer that answers pings is not closed for silence.
#[tokio::test]
async fn a_peer_that_answers_pings_stays() {
    let port = serve(
        module(),
        limits(|l| {
            l.ping_interval = Some(Duration::from_millis(100));
            l.ping_timeout = Duration::from_millis(400);
        }),
    )
    .await;
    let mut ws = connect(port).await;
    // Reading is what answers the pings (tokio-tungstenite pongs as it reads).
    let until = tokio::time::Instant::now() + Duration::from_millis(1500);
    while let Ok(Some(Ok(_))) = tokio::time::timeout_at(until, ws.next()).await {}
    assert_eq!(request(&mut ws, 1, "ping", json!([])).await["result"], "pong");
}

/// RPC-5's probe: a socket that upgrades and never calls is closed past the
/// idle limit, though it answers every ping.
#[tokio::test]
async fn a_socket_that_never_calls_is_closed_past_the_idle_limit() {
    let port = serve(module(), limits(|l| l.idle_timeout = Some(Duration::from_millis(500)))).await;
    let mut ws = connect(port).await;
    let began = Instant::now();
    let mut closed = false;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(4), ws.next()).await {
        if let Ok(Message::Close(_)) | Err(_) = m {
            closed = true;
            break;
        }
    }
    assert!(closed, "the idle socket was closed");
    assert!(began.elapsed() >= Duration::from_millis(450), "closed at {:?}, before the idle limit", began.elapsed());
}

/// A call open (a watch) keeps a socket from being idle; once it ends, the
/// idle clock runs.
#[tokio::test]
async fn a_socket_with_a_call_open_is_not_idle() {
    let port = serve(module(), limits(|l| l.idle_timeout = Some(Duration::from_millis(300)))).await;
    let mut ws = connect(port).await;
    let sub = request(&mut ws, 1, "hold", json!([])).await["result"].clone();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(request(&mut ws, 2, "ping", json!([])).await["result"], "pong", "a call open: not idle");
    assert_eq!(request(&mut ws, 3, "unhold", json!([sub])).await["result"], true);
    let mut closed = false;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
        if let Ok(Message::Close(_)) | Err(_) = m {
            closed = true;
            break;
        }
    }
    assert!(closed, "idle once its last call ended");
}
