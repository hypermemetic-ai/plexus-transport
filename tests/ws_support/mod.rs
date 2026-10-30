//! PERF-20: what the socket-edge tests share — a server on a free port, a
//! client that speaks frames (tokio-tungstenite's, so a test can send what a
//! well-behaved client would not), a raw upgrade for a peer that never reads,
//! and the process's resident memory.
#![allow(dead_code)]

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonrpsee::RpcModule;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub type Ws = WebSocketStream<TcpStream>;

/// `module` served on a free loopback port under `options`; the handle is
/// kept alive for the test (dropping it stops the server).
pub async fn serve(module: RpcModule<()>, options: UpgradeOptions) -> u16 {
    serve_with(module, WebSocketConfig::new(0), options).await
}

pub async fn serve_with(module: RpcModule<()>, config: WebSocketConfig, options: UpgradeOptions) -> u16 {
    let (handle, addr) = serve_websocket_with(module, config, None, options).await.expect("serve");
    std::mem::forget(handle);
    addr.port()
}

/// A WebSocket client on `port` (tokio-tungstenite: it answers pings).
pub async fn connect(port: u16) -> Ws {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
    tcp.set_nodelay(true).unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{port}/"), tcp).await.expect("upgrade");
    ws
}

/// The next text message, within `within`, skipping pings and pongs;
/// `None`: the socket closed (or a close frame came). Past `within`: a panic.
pub async fn next_text<S>(ws: &mut S, within: Duration) -> Option<Value>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next()).await.expect("a frame within the time")?;
        match msg {
            Ok(Message::Text(t)) => return Some(serde_json::from_str(t.as_str()).expect("JSON")),
            Ok(Message::Binary(b)) => return Some(serde_json::from_slice(&b).expect("JSON")),
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(Message::Frame(_)) => continue,
        }
    }
}

/// Send one request and read until its answer (other frames are dropped).
pub async fn request(ws: &mut Ws, id: u64, method: &str, params: Value) -> Value {
    let frame = serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    ws.send(Message::text(frame.to_string())).await.expect("send");
    loop {
        let v = next_text(ws, Duration::from_secs(10)).await.expect("an answer, the socket open");
        if v.get("id") == Some(&Value::from(id)) && v.get("method").is_none() {
            return v;
        }
    }
}

/// A raw upgrade: the request written by hand, the 101 read, the socket
/// handed back — a peer that then does whatever the test says (or nothing).
pub async fn raw_upgrade(port: u16) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
    let req = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut b)).await.expect("the 101 in time").unwrap();
        assert!(n == 1, "the server closed before answering the upgrade: {}", String::from_utf8_lossy(&head));
        head.push(b[0]);
    }
    let head = String::from_utf8_lossy(&head);
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    s
}

/// One masked client frame, written by hand (`op`: 0x1 text, 0x0 continuation, …).
pub fn client_frame(op: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
    let mut f = vec![if fin { 0x80 } else { 0 } | op];
    let len = payload.len();
    if len < 126 {
        f.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        f.push(0x80 | 126);
        f.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        f.push(0x80 | 127);
        f.extend_from_slice(&(len as u64).to_be_bytes());
    }
    let key = [0x12u8, 0x34, 0x56, 0x78];
    f.extend_from_slice(&key);
    f.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
    f
}

/// Read server frames off a raw socket until one is a text frame; its
/// payload. `None`: the socket closed, or a close frame came first.
pub async fn raw_next_text(s: &mut TcpStream, within: Duration) -> Option<String> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let mut h = [0u8; 2];
        match tokio::time::timeout_at(deadline, s.read_exact(&mut h)).await {
            Ok(Ok(_)) => {}
            _ => return None,
        }
        let op = h[0] & 0x0f;
        let mut len = (h[1] & 0x7f) as u64;
        if len == 126 {
            let mut e = [0u8; 2];
            s.read_exact(&mut e).await.ok()?;
            len = u16::from_be_bytes(e) as u64;
        } else if len == 127 {
            let mut e = [0u8; 8];
            s.read_exact(&mut e).await.ok()?;
            len = u64::from_be_bytes(e);
        }
        let mut p = vec![0u8; len as usize];
        s.read_exact(&mut p).await.ok()?;
        match op {
            0x1 => return Some(String::from_utf8(p).unwrap()),
            0x8 => return None,
            _ => continue,
        }
    }
}

/// This process's resident set, in bytes (Linux).
pub fn rss() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    pages * 4096
}

/// The socket reads EOF (or a reset) within `within`: the server dropped it.
pub async fn closed_within(s: &mut TcpStream, within: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    let mut buf = [0u8; 4096];
    loop {
        match tokio::time::timeout_at(deadline, s.read(&mut buf)).await {
            Err(_) => return false,
            Ok(Ok(0)) | Ok(Err(_)) => return true,
            Ok(Ok(_)) => continue,
        }
    }
}
