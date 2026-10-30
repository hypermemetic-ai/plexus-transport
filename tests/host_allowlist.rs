//! ACCESS-25: a `Host` allow-list on the WebSocket upgrade and the REST
//! bridge. A page that rebinds its own name to 127.0.0.1 names its own host:
//! refused 421 before anything routes it. No list: nothing is refused.

use std::sync::Arc;

use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use jsonrpsee::RpcModule;
use plexus_transport::host::loopback_hosts;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The status line's code and the body of a raw HTTP/1.1 request to
/// 127.0.0.1:`port` naming `host` (or none).
async fn raw(port: u16, path: &str, host: Option<&str>, extra: &str) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let host = host.map(|h| format!("Host: {h}\r\n")).unwrap_or_default();
    let req = format!("GET {path} HTTP/1.1\r\n{host}{extra}Connection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    // An upgrade answered 101 stays open: read what comes until it pauses.
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(Ok(n)) = tokio::time::timeout(std::time::Duration::from_millis(300), s.read(&mut buf)).await {
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&out).to_string();
    let code = text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    (code, text)
}

fn free() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

const UPGRADE: &str = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n";

fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |_, _, _| "pong").unwrap();
    m
}

async fn ws_anywhere() -> u16 {
    let cfg = WebSocketConfig::new(0);
    let (handle, addr) = serve_websocket_with(module(), cfg, None, UpgradeOptions::default()).await.unwrap();
    std::mem::forget(handle);
    addr.port()
}

/// The upgrade, and a plain HTTP request, naming another host are refused
/// 421; the loopback names with the port proceed, and a real client calls.
#[tokio::test]
async fn the_websocket_answers_only_its_hosts() {
    // The list names the port, so the port is chosen before binding.
    let port = free();
    let cfg = WebSocketConfig::new(port).with_allowed_hosts(loopback_hosts(port));
    let (handle, _) = serve_websocket_with(module(), cfg, None, UpgradeOptions::default()).await.unwrap();

    let (code, text) = raw(port, "/", Some("evil.example"), UPGRADE).await;
    assert_eq!(code, 421, "{text}");
    assert!(text.ends_with(plexus_transport::host::REFUSAL), "{text}");
    assert_eq!(raw(port, "/", Some("evil.example"), "").await.0, 421, "plain HTTP too");
    assert_eq!(raw(port, "/", Some(&format!("evil.example:{port}")), UPGRADE).await.0, 421);
    assert_eq!(raw(port, "/", Some("127.0.0.1:1"), UPGRADE).await.0, 421, "another port");
    assert_eq!(raw(port, "/", None, UPGRADE).await.0, 421, "no host at all");
    assert_eq!(raw(port, "/", Some(&format!("localhost:{port}")), UPGRADE).await.0, 101);
    assert_eq!(raw(port, "/", Some(&format!("127.0.0.1:{port}")), UPGRADE).await.0, 101);
    // PERF-20: past the host check, a request that is not an upgrade is 426
    // (jsonrpsee-server's plain-HTTP JSON-RPC on this port is gone).
    assert_eq!(raw(port, "/", Some(&format!("127.0.0.1:{port}")), "").await.0, 426, "plain HTTP, the right host");

    let c = WsClientBuilder::default().build(format!("ws://127.0.0.1:{port}")).await.unwrap();
    let pong: String = c.request("ping", rpc_params![]).await.unwrap();
    assert_eq!(pong, "pong");
    handle.stop().unwrap();
}

/// No list (the default): any host is answered, as before ACCESS-25.
#[tokio::test]
async fn without_a_list_the_websocket_answers_any_host() {
    let port = ws_anywhere().await;
    assert_eq!(raw(port, "/", Some("evil.example"), UPGRADE).await.0, 101);
}

/// The builder's list reaches the WebSocket config.
#[tokio::test]
async fn the_builder_s_list_reaches_the_websocket() {
    let port = free();
    let hub = Arc::new(plexus_core::plexus::DynamicHub::new("t"));
    let server = plexus_transport::TransportServer::builder(hub, |h| {
        plexus_core::plexus::DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!("{e}"))
    })
    .without_registry()
    .with_websocket(port)
    .with_allowed_hosts(loopback_hosts(port))
    .build()
    .await
    .unwrap();
    tokio::spawn(server.serve());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(raw(port, "/", Some("evil.example"), UPGRADE).await.0, 421);
    assert_eq!(raw(port, "/", Some(&format!("localhost:{port}")), UPGRADE).await.0, 101);
}

#[cfg(feature = "http-gateway")]
mod rest {
    use super::*;
    use plexus_transport::RestHttpConfig;

    async fn rest(hosts: Option<Vec<String>>, port: u16) {
        let mut cfg = RestHttpConfig::new(port);
        if let Some(h) = hosts {
            cfg = cfg.with_allowed_hosts(h);
        }
        let hub = Arc::new(plexus_core::plexus::DynamicHub::new("t"));
        plexus_transport::serve_rest_http(hub, None, None, cfg, None).await.unwrap();
    }

    /// Another host is refused 421 on every path — a route, the fallback,
    /// `/debug` — before the api key is even asked for.
    #[tokio::test]
    async fn the_rest_bridge_answers_only_its_hosts() {
        let port = free();
        rest(Some(loopback_hosts(port)), port).await;
        for path in ["/debug", "/rest/t/schema", "/nowhere"] {
            let (code, text) = raw(port, path, Some("evil.example"), "").await;
            assert_eq!(code, 421, "{path}: {text}");
            assert!(text.ends_with(plexus_transport::host::REFUSAL), "{text}");
        }
        assert_eq!(raw(port, "/debug", Some(&format!("127.0.0.1:{port}")), "").await.0, 200);
        assert_eq!(raw(port, "/debug", Some(&format!("LOCALHOST:{port}")), "").await.0, 200);
        assert_eq!(raw(port, "/debug", Some("127.0.0.1"), "").await.0, 421, "no port: not ours");
    }

    #[tokio::test]
    async fn without_a_list_the_rest_bridge_answers_any_host() {
        let port = free();
        rest(None, port).await;
        assert_eq!(raw(port, "/debug", Some("evil.example"), "").await.0, 200);
    }
}
