//! KEEL-29: a WebSocket server built with the defaults keeps web pages out.
//!
//! A browser sends `Origin` on every WebSocket and there is no same-origin
//! rule for sockets, so before KEEL-29 any page a developer had open could
//! open `ws://127.0.0.1:<port>/` to a default server and call its methods
//! (cross-site WebSocket hijacking), and a page that rebinds its own name to
//! 127.0.0.1 got the same (DNS rebinding). Now, with no lists set:
//!
//! - an upgrade carrying an `Origin` is refused 403, unless it is the
//!   listener's own origin (a page this port serves); one carrying none —
//!   synapse, a service, a CLI — connects as before;
//! - bound to loopback, the `Host` list is the loopback names for the port,
//!   and any other `Host` is refused 421;
//! - bound beyond loopback, `Host` is unchecked as before (a warning says so);
//! - `allow_any_origin()` / `allow_any_host()` are the opt-outs.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonrpsee::RpcModule;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

const EVIL: &str = "https://evil.example";

fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("ping", |_, _, _| "pong").unwrap();
    m
}

async fn serve(cfg: WebSocketConfig) -> u16 {
    let (handle, addr) = serve_websocket_with(module(), cfg, None, UpgradeOptions::default()).await.expect("serve");
    std::mem::forget(handle);
    addr.port()
}

/// A real client on 127.0.0.1:`port` naming `host` (default: the address)
/// and carrying `origin` if any: `Ok(what ping answered)` when the upgrade
/// was accepted, `Err(status)` when it was refused.
async fn call_ping(port: u16, host: Option<&str>, origin: Option<&str>) -> Result<String, u16> {
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    if let Some(h) = host {
        req.headers_mut().insert(http::header::HOST, h.parse().unwrap());
    }
    if let Some(o) = origin {
        req.headers_mut().insert(http::header::ORIGIN, o.parse().unwrap());
    }
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (mut ws, _) = match tokio_tungstenite::client_async(req, tcp).await {
        Ok(ok) => ok,
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => return Err(resp.status().as_u16()),
        Err(e) => panic!("upgrade: {e}"),
    };
    let frame = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "ping", "params": []});
    ws.send(Message::text(frame.to_string())).await.unwrap();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.expect("an answer").expect("open").unwrap();
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
            return Ok(v["result"].as_str().unwrap_or_default().to_string());
        }
    }
}

/// The status code of a raw upgrade naming `host` and carrying `origin`.
async fn upgrade_status(port: u16, host: &str, origin: Option<&str>) -> u16 {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let origin = origin.map(|o| format!("Origin: {o}\r\n")).unwrap_or_default();
    let req = format!(
        "GET / HTTP/1.1\r\nHost: {host}\r\n{origin}Upgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 1024];
    while !out.windows(4).any(|w| w == b"\r\n\r\n") {
        match tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
        }
    }
    let text = String::from_utf8_lossy(&out).to_string();
    text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0)
}

/// The hole, and its closing: a page on another site opens a socket to a
/// default server and calls a method.
#[tokio::test]
async fn a_page_from_another_site_cannot_call_a_default_server() {
    let port = serve(WebSocketConfig::new(0)).await;
    match call_ping(port, None, Some(EVIL)).await {
        Ok(answer) => panic!(
            "the hole: an upgrade with Origin {EVIL} to a default server was accepted, and `ping` answered {answer:?}"
        ),
        Err(status) => assert_eq!(status, 403),
    }
    assert_eq!(upgrade_status(port, &format!("127.0.0.1:{port}"), Some("null")).await, 403, "a sandboxed frame");
}

/// synapse, a service, a test: no `Origin`, connects and calls as before.
#[tokio::test]
async fn a_client_that_sends_no_origin_connects_as_before() {
    let port = serve(WebSocketConfig::new(0)).await;
    assert_eq!(call_ping(port, None, None).await, Ok("pong".into()));
    assert_eq!(call_ping(port, Some(&format!("localhost:{port}")), None).await, Ok("pong".into()));
}

/// A page this very listener serves (TRANSPORT-26's routes) is its own
/// origin, and connects with no list.
#[tokio::test]
async fn the_listener_s_own_origin_connects() {
    let port = serve(WebSocketConfig::new(0)).await;
    assert_eq!(call_ping(port, None, Some(&format!("http://127.0.0.1:{port}"))).await, Ok("pong".into()));
    assert_eq!(
        call_ping(port, Some(&format!("localhost:{port}")), Some(&format!("http://localhost:{port}"))).await,
        Ok("pong".into())
    );
    // Another port of the same host is another origin.
    assert_eq!(call_ping(port, None, Some("http://127.0.0.1:3000")).await, Err(403));
    // An origin naming one loopback name over a request naming the other is not this page.
    assert_eq!(call_ping(port, None, Some(&format!("http://localhost:{port}"))).await, Err(403));
}

/// With a list, as before KEEL-29: the listed origin and none connect.
#[tokio::test]
async fn with_an_origin_list_it_is_as_before() {
    let port = serve(WebSocketConfig::new(0).with_allowed_origins(["http://app.example:5173"])).await;
    assert_eq!(call_ping(port, None, Some("http://app.example:5173")).await, Ok("pong".into()));
    assert_eq!(call_ping(port, None, None).await, Ok("pong".into()));
    assert_eq!(call_ping(port, None, Some(EVIL)).await, Err(403));
}

/// The opt-out, one visible line.
#[tokio::test]
async fn allow_any_origin_is_the_opt_out() {
    let port = serve(WebSocketConfig::new(0).allow_any_origin()).await;
    assert_eq!(call_ping(port, None, Some(EVIL)).await, Ok("pong".into()));
}

/// DNS rebinding: bound to loopback with no list, the `Host` list is the
/// loopback names for the port; a page under its own name gets 421.
#[tokio::test]
async fn a_loopback_default_server_answers_only_its_loopback_names() {
    let port = serve(WebSocketConfig::new(0)).await;
    let rebound = format!("evil.example:{port}");
    assert_eq!(upgrade_status(port, &rebound, Some(&format!("http://{rebound}"))).await, 421, "a rebound page");
    assert_eq!(upgrade_status(port, &rebound, None).await, 421);
    assert_eq!(upgrade_status(port, "127.0.0.1:1", None).await, 421, "another port");
    assert_eq!(upgrade_status(port, &format!("127.0.0.1:{port}"), None).await, 101);
    assert_eq!(upgrade_status(port, &format!("LOCALHOST:{port}"), None).await, 101);
}

/// The host opt-out.
#[tokio::test]
async fn allow_any_host_is_the_opt_out() {
    let port = serve(WebSocketConfig::new(0).allow_any_host()).await;
    assert_eq!(upgrade_status(port, "evil.example", None).await, 101);
    assert_eq!(upgrade_status(port, "evil.example", Some(EVIL)).await, 403, "the origin is still checked");
}

/// Bound beyond loopback with no list: `Host` is unchecked as before (a
/// server on `0.0.0.0` is reached by names the transport cannot know), but
/// a foreign `Origin` is still refused.
#[tokio::test]
async fn a_server_bound_beyond_loopback_keeps_answering_any_host() {
    let cfg = WebSocketConfig::new(0).with_addr("0.0.0.0:0".parse().unwrap());
    let port = serve(cfg).await;
    assert_eq!(upgrade_status(port, "box.lan:1234", None).await, 101);
    assert_eq!(upgrade_status(port, "box.lan:1234", Some(EVIL)).await, 403);
}

/// The builder's server is as safe as `WebSocketConfig::new`, and its own
/// opt-out reaches the socket.
#[tokio::test]
async fn the_builder_s_defaults_are_the_same() {
    async fn start(open: bool) -> u16 {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let service = Arc::new(plexus_core::plexus::DynamicHub::new("t"));
        let mut b = plexus_transport::TransportServer::builder(service, |h| {
            plexus_core::plexus::DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!("{e}"))
        })
        .without_registry()
        .with_websocket(port);
        if open {
            b = b.allow_any_origin().allow_any_host();
        }
        tokio::spawn(b.build().await.unwrap().serve());
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        port
    }
    let port = start(false).await;
    assert_eq!(upgrade_status(port, &format!("127.0.0.1:{port}"), Some(EVIL)).await, 403);
    assert_eq!(upgrade_status(port, "evil.example", None).await, 421);
    assert_eq!(upgrade_status(port, &format!("127.0.0.1:{port}"), None).await, 101);
    let port = start(true).await;
    assert_eq!(upgrade_status(port, "evil.example", Some(EVIL)).await, 101);
}

// ── KEEL-29 follow-up ───────────────────────────────────────────────────────

/// `*` is not a wildcard anyone can write: an operator's `--allowed-origin
/// '*'` or `--allowed-host '*'` arriving as a list entry is refused when the
/// listener starts, naming the call that does open it.
#[tokio::test]
async fn a_star_in_a_list_is_refused_at_start() {
    for cfg in [
        WebSocketConfig::new(0).with_allowed_origins([" * "]),
        WebSocketConfig::new(0).with_allowed_origins(["http://ok.example", "*"]),
    ] {
        match serve_websocket_with(module(), cfg, None, UpgradeOptions::default()).await {
            Ok((handle, addr)) => {
                let opened = upgrade_status(addr.port(), &format!("127.0.0.1:{}", addr.port()), Some(EVIL)).await;
                std::mem::forget(handle);
                panic!("a `*` origin entry started a server; {EVIL} got {opened}");
            }
            Err(e) => assert!(e.to_string().contains("allow_any_origin()"), "{e}"),
        }
    }
    match serve_websocket_with(module(), WebSocketConfig::new(0).with_allowed_hosts(["*"]), None, UpgradeOptions::default()).await {
        Ok((handle, addr)) => {
            let opened = upgrade_status(addr.port(), "evil.example", None).await;
            std::mem::forget(handle);
            panic!("a `*` host entry started a server; Host evil.example got {opened}");
        }
        Err(e) => assert!(e.to_string().contains("allow_any_host()"), "{e}"),
    }
}

/// The builder refuses the same at `build()`.
#[tokio::test]
async fn the_builder_refuses_a_star_at_build() {
    for (origins, hosts, call) in [(vec!["*"], vec![], "allow_any_origin()"), (vec![], vec!["*"], "allow_any_host()")] {
        let service = Arc::new(plexus_core::plexus::DynamicHub::new("t"));
        let built = plexus_transport::TransportServer::builder(service, |h| {
            plexus_core::plexus::DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!("{e}"))
        })
        .without_registry()
        .with_websocket(0)
        .with_allowed_origins(origins)
        .with_allowed_hosts(hosts)
        .build()
        .await;
        match built {
            Ok(_) => panic!("a `*` entry built a server"),
            Err(e) => assert!(e.to_string().contains(call), "{e}"),
        }
    }
}

/// The REST bridge's host list refuses `*` at start too (before KEEL-29 it
/// matched no host; it never meant every host).
#[cfg(feature = "http-gateway")]
#[tokio::test]
async fn the_rest_bridge_refuses_a_star() {
    let cfg = plexus_transport::RestHttpConfig::new(0).with_allowed_hosts(["*"]);
    let service = Arc::new(plexus_core::plexus::DynamicHub::new("t"));
    let started = plexus_transport::serve_rest_http(service, None, None, cfg, None).await;
    assert!(started.as_ref().is_err_and(|e| e.to_string().contains("`*`")), "{:?}", started.map(|_| ()));
}

/// The listener's own origin has its scheme: a plain listener's page is
/// `http://`, never `https://`.
#[tokio::test]
async fn the_own_origin_has_the_listener_s_scheme() {
    let port = serve(WebSocketConfig::new(0)).await;
    let host = format!("127.0.0.1:{port}");
    assert_eq!(upgrade_status(port, &host, Some(&format!("http://{host}"))).await, 101);
    assert_eq!(upgrade_status(port, &host, Some(&format!("https://{host}"))).await, 403);
}

/// Behind a trusted proxy the scheme is the proxy's `X-Forwarded-Proto`;
/// with none, it is not known and no origin is the listener's own.
#[tokio::test]
async fn behind_a_proxy_the_own_origin_s_scheme_is_the_proxy_s_word() {
    let mut cfg = WebSocketConfig::new(0).with_allowed_hosts(["app.example"]);
    cfg.trusted_proxies = plexus_transport::proxy::TrustedProxies::new(["127.0.0.1/32".parse().unwrap()]);
    let port = serve(cfg).await;
    let with = |proto: &'static str| async move {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let req = format!(
            "GET / HTTP/1.1\r\nHost: app.example\r\nOrigin: https://app.example\r\nX-Forwarded-For: 203.0.113.9\r\n{proto}\
             Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf)).await.unwrap().unwrap();
        String::from_utf8_lossy(&buf[..n]).split(' ').nth(1).and_then(|c| c.parse::<u16>().ok()).unwrap_or(0)
    };
    assert_eq!(with("X-Forwarded-Proto: https\r\n").await, 101);
    assert_eq!(with("X-Forwarded-Proto: http\r\n").await, 403, "the page came over http: another origin");
    assert_eq!(with("").await, 403, "no word on the scheme: not known, so not its own");
}

/// Bound beyond loopback with no Host list, the page routes answer only the
/// loopback names and the bound address's literal — a rebound name gets 421
/// — while the socket answers any `Host` for clients that send no Origin.
#[tokio::test]
async fn beyond_loopback_the_routes_answer_only_names_the_listener_knows() {
    let router = axum::Router::new().route("/page", axum::routing::get(|| async { "page" }));
    let options = UpgradeOptions { routes: Some(plexus_transport::websocket::HostRoutes::new(router)), ..UpgradeOptions::default() };
    let cfg = WebSocketConfig::new(0).with_addr("0.0.0.0:0".parse().unwrap());
    let (handle, addr) = serve_websocket_with(module(), cfg, None, options).await.unwrap();
    std::mem::forget(handle);
    let port = addr.port();
    let get = |host: String| async move {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(format!("GET /page HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        let mut out = String::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_string(&mut out)).await;
        out.split(' ').nth(1).and_then(|c| c.parse::<u16>().ok()).unwrap_or(0)
    };
    assert_eq!(get(format!("evil.example:{port}")).await, 421, "a rebound page");
    assert_eq!(get(format!("127.0.0.1:{port}")).await, 200);
    assert_eq!(get(format!("localhost:{port}")).await, 200);
    let socket = |host: String, origin: Option<String>| async move {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let origin = origin.map(|o| format!("Origin: {o}\r\n")).unwrap_or_default();
        s.write_all(format!("GET /ws HTTP/1.1\r\nHost: {host}\r\n{origin}Upgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf)).await.unwrap().unwrap();
        String::from_utf8_lossy(&buf[..n]).split(' ').nth(1).and_then(|c| c.parse::<u16>().ok()).unwrap_or(0)
    };
    assert_eq!(socket("box.lan:1234".into(), None).await, 101, "a client with no Origin, any name: as before");
    assert_eq!(socket(format!("127.0.0.1:{port}"), Some(format!("http://127.0.0.1:{port}"))).await, 101, "the page's own socket");
    assert_eq!(socket(format!("evil.example:{port}"), Some(format!("http://evil.example:{port}"))).await, 403, "a rebound page's socket");
}
