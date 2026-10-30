//! PERF-20: the upgrade is plexus-transport's own.
//!
//! - A request that is not a WebSocket upgrade, once past the checks, is
//!   answered `426 Upgrade Required` — jsonrpsee-server's plain-HTTP
//!   JSON-RPC on the WebSocket port goes (it answered every Plexus call
//!   `-32603` and reached `{ns}.respond` from outside any connection).
//! - RPC-4: a TCP connection that never completes its request is dropped at
//!   the header timeout, and the sockets accepted from one IP but not yet
//!   upgraded are bounded at the accept loop (by the quotas' per-IP ceiling
//!   unless the limits name another) — today all 300 of a dribbling client's
//!   sockets are held, and one left mid-headers stays for as long as it likes.

mod ws_support;

use std::time::{Duration, Instant};

use jsonrpsee::RpcModule;
use plexus_transport::websocket::UpgradeOptions;
use plexus_transport::{ConnectionQuotas, WebSocketConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use ws_support::*;

fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |_, _, _| "pong").unwrap();
    m
}

/// The status code and the head of the answer to a raw HTTP/1.1 request.
async fn http(port: u16, request: String) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(500), s.read(&mut buf)).await {
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&out).to_string();
    let code = text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    (code, text)
}

/// A JSON-RPC call POSTed to the WebSocket port, and a plain GET: both 426,
/// each naming the upgrade it wants. The socket's own clients are untouched.
#[tokio::test]
async fn plain_http_on_the_websocket_port_is_426() {
    let port = serve(module(), UpgradeOptions::default()).await;
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping","params":[]}"#;
    let post = format!(
        "POST / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (code, text) = http(port, post).await;
    assert_eq!(code, 426, "{text}");
    assert!(text.to_ascii_lowercase().contains("upgrade: websocket"), "a 426 names the upgrade: {text}");
    assert!(!text.contains("\"jsonrpc\""), "no JSON-RPC answer over plain HTTP: {text}");

    let get = format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    assert_eq!(http(port, get).await.0, 426);

    let mut ws = connect(port).await;
    assert_eq!(request(&mut ws, 1, "ping", serde_json::json!([])).await["result"], "pong");
}

/// RPC-4: 300 sockets from one IP, each dribbling a request's first header
/// line and never finishing it: at most the per-IP ceiling are held, the rest
/// are dropped at once; closing the held ones gives their places back.
#[tokio::test]
async fn sockets_not_yet_upgraded_are_bounded_per_ip() {
    let quotas = ConnectionQuotas { max_per_ip: 8, ..ConnectionQuotas::default() };
    let port = serve_with(module(), WebSocketConfig { quotas, ..WebSocketConfig::new(0) }, UpgradeOptions::default()).await;
    let mut socks = Vec::new();
    for _ in 0..300 {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let _ = s.write_all(format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n").as_bytes()).await;
        socks.push(s);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut held = Vec::new();
    for mut s in socks {
        if !closed_within(&mut s, Duration::from_millis(20)).await {
            held.push(s);
        }
    }
    println!("300 dribbling sockets from one IP: {} held", held.len());
    assert!(held.len() <= 8, "{} sockets held past the per-IP ceiling of 8", held.len());
    assert!(!held.is_empty(), "the ceiling's worth are held, not refused");
    drop(held);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut ws = connect(port).await;
    assert_eq!(request(&mut ws, 1, "ping", serde_json::json!([])).await["result"], "pong", "places given back");
}

/// RPC-4: a connection left mid-headers is dropped at the header timeout
/// (10 s by default), not held for as long as the peer likes.
#[tokio::test]
async fn a_connection_left_mid_headers_is_dropped_at_the_timeout() {
    let port = serve(module(), UpgradeOptions::default()).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n").as_bytes()).await.unwrap();
    let began = Instant::now();
    let dropped = closed_within(&mut s, Duration::from_secs(14)).await;
    println!("mid-headers: dropped {dropped} after {:?}", began.elapsed());
    assert!(dropped, "a connection left mid-headers for 14 s was never dropped");
    assert!(began.elapsed() >= Duration::from_secs(5), "dropped too soon: {:?}", began.elapsed());
}

/// AUTH-12's total (`max_connections`) holds on the new loop: past it, 429.
#[tokio::test]
async fn past_max_connections_the_upgrade_is_429() {
    let quotas = ConnectionQuotas { max_connections: 2, ..ConnectionQuotas::default() };
    let port = serve_with(module(), WebSocketConfig { quotas, ..WebSocketConfig::new(0) }, UpgradeOptions::default()).await;
    let a = connect(port).await;
    let b = connect(port).await;
    let up = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    assert_eq!(http(port, up.clone()).await.0, 429);
    drop((a, b));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut ws = connect(port).await;
    assert_eq!(request(&mut ws, 1, "ping", serde_json::json!([])).await["result"], "pong");
}

/// RPC-4 behind a proxy (TRANSPORT-7): a trusted proxy's address carries
/// every client's sockets before their headers name the client, so it is
/// held to the total (`max_connections`), not to the per-IP ceiling that its
/// forwarded clients are each held to. Another address keeps the per-IP one.
#[tokio::test]
async fn a_trusted_proxy_is_held_to_the_total_not_to_the_per_ip_ceiling() {
    let quotas = ConnectionQuotas { max_per_ip: 2, max_connections: 50, ..ConnectionQuotas::default() };
    let proxies = plexus_transport::TrustedProxies::parse(["127.0.0.1"]).unwrap();
    let config = WebSocketConfig { quotas, ..WebSocketConfig::new(0) }.with_trusted_proxies(proxies);
    let port = serve_with(module(), config, UpgradeOptions::default()).await;
    let mut socks = Vec::new();
    for _ in 0..10 {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n").as_bytes()).await.unwrap();
        socks.push(s);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut held = 0;
    for s in socks.iter_mut() {
        if !closed_within(s, Duration::from_millis(20)).await {
            held += 1;
        }
    }
    assert_eq!(held, 10, "a trusted proxy's sockets not yet upgraded are held to the total (50), not to 2");
}
