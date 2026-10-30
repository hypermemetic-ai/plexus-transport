//! ACCESS-62 on PERF-20's loop: at `trace`, for every target, the server
//! logs no credential — not the upgrade's `Authorization` header, not a
//! session token a call answers with, plain or as hex. Today jsonrpsee-server
//! traces the upgrade request whole, headers included. A test binary of its
//! own: it installs the process's subscriber (and the `log` bridge, so the
//! WebSocket library's own records are caught too). The client here is
//! written by hand, so nothing of the client's side is logged.

mod ws_support;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jsonrpsee::{PendingSubscriptionSink, RpcModule};
use plexus_core::plexus::{AuthContext, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use ws_support::*;

const TOKEN: &str = "mgs_secretSessionToken0123456789";
const MINTED: &str = "mgs_mintedByTheCall9876543210";

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct Tokens;

#[async_trait]
impl SessionValidator for Tokens {
    async fn validate(&self, token: &str) -> Option<AuthContext> {
        (token.trim() == TOKEN).then(|| AuthContext { user_id: "alice".into(), session_id: "s1".into(), roles: vec![], metadata: json!({}) })
    }
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trace_run_logs_no_credential() {
    let log = Captured::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(log.clone())
        .with_ansi(false)
        .init();

    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    // As `session.login` does: a call whose answer carries a credential.
    module
        .register_subscription("mint", "mint", "unmint", |_, pending: PendingSubscriptionSink, _, _| async move {
            let sink = pending.accept().await?;
            sink.send(serde_json::value::to_raw_value(&json!({"token": MINTED}))?).await?;
            Ok(())
        })
        .unwrap();
    let options = UpgradeOptions { reject_on_session_failure: true, ..UpgradeOptions::default() };
    let (handle, addr) = serve_websocket_with(module, WebSocketConfig::new(0), Some(Arc::new(Tokens)), options).await.unwrap();
    let port = addr.port();

    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer {TOKEN}\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        s.read_exact(&mut b).await.unwrap();
        head.push(b[0]);
    }
    assert!(head.starts_with(b"HTTP/1.1 101"), "{}", String::from_utf8_lossy(&head));
    for (id, method) in [(1, "ping"), (2, "mint")] {
        let call = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": []}).to_string();
        s.write_all(&client_frame(0x1, true, call.as_bytes())).await.unwrap();
    }
    let mut minted = false;
    for _ in 0..4 {
        match raw_next_text(&mut s, Duration::from_secs(5)).await {
            Some(t) if t.contains(MINTED) => {
                minted = true;
                break;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(minted, "the call answered with its credential");
    drop(s);
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.stop().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let text = String::from_utf8_lossy(&log.0.lock().unwrap()).to_string();
    assert!(!text.is_empty(), "trace is on: something is logged");
    let leaks: Vec<&str> = ["Bearer", "mgs_", TOKEN, MINTED]
        .into_iter()
        .filter(|w| text.contains(w))
        .chain([hex(TOKEN), hex(MINTED), hex("mgs_")].iter().filter(|h| text.contains(h.as_str())).map(|_| "a credential as hex"))
        .collect();
    if !leaks.is_empty() {
        let line = text.lines().find(|l| l.contains("Bearer") || l.contains("mgs_") || l.contains(&hex("mgs_"))).unwrap_or("");
        panic!("a trace run logged {leaks:?}; e.g. {}", &line[..line.len().min(400)]);
    }
    println!("{} bytes of trace logged, no credential among them", text.len());
}
