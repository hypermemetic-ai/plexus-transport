//! TRANSPORT-26's perf row: what TLS on the socket costs, plain against TLS,
//! in one process and interleaved. plexus-bench's raw client speaks plain
//! TCP only (a blocking socket split in two), so the row is measured here:
//!
//! - `stream.10k` — one subscription sending 10,000 items (64 B and 1 KiB),
//!   read to the last by a tokio-tungstenite client: items a second, the
//!   19th of 21 runs (plexus-bench's reading of a short rate) and the median;
//! - `call.one_item` — sequential calls, one answer each: p50 and p99.
//!
//! Server and client share the runtime and are not pinned: the ratio is the
//! number, not the absolutes. Run it in release:
//!
//! ```sh
//! cargo test --release --features tls --test tls_cost -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use jsonrpsee::RpcModule;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{TlsConfig, WebSocketConfig};
use rcgen::{CertificateParams, KeyPair};
use serde_json::value::RawValue;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

fn module() -> RpcModule<()> {
    let mut m = RpcModule::new(());
    m.register_method("one", |_, _, _| "the item").unwrap();
    m.register_subscription("stream", "item", "unstream", |params, pending, _, _| async move {
        let (n, size): (u64, usize) = params.parse()?;
        let sink = pending.accept().await?;
        let item: Box<RawValue> = RawValue::from_string(format!("\"{}\"", "x".repeat(size))).unwrap();
        for _ in 0..n {
            sink.send(item.clone()).await?;
        }
        Ok(())
    })
    .unwrap();
    m
}

struct Tls {
    _dir: tempdir::Dir,
    config: TlsConfig,
    client: Arc<rustls::ClientConfig>,
}

mod tempdir {
    pub struct Dir(pub std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn tls() -> Tls {
    let dir = std::env::temp_dir().join(format!("plexus-transport-tls-cost-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap().self_signed(&key).unwrap();
    let config = TlsConfig::new(dir.join("c.pem"), dir.join("k.pem"));
    std::fs::write(&config.cert, cert.pem()).unwrap();
    std::fs::write(&config.key, key.serialize_pem()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from_pem_slice(cert.pem().as_bytes()).unwrap()).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Tls { _dir: tempdir::Dir(dir), config, client: Arc::new(client) }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

async fn connect(port: u16, tls: Option<&Tls>) -> WebSocketStream<Box<dyn Io>> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let io: Box<dyn Io> = match tls {
        None => Box::new(tcp),
        Some(t) => {
            let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            Box::new(tokio_rustls::TlsConnector::from(t.client.clone()).connect(name, tcp).await.unwrap())
        }
    };
    let scheme = if tls.is_some() { "wss" } else { "ws" };
    tokio_tungstenite::client_async(format!("{scheme}://127.0.0.1:{port}/"), io).await.unwrap().0
}

async fn next_text(ws: &mut WebSocketStream<Box<dyn Io>>) -> String {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return t.as_str().to_string(),
            _ => continue,
        }
    }
}

/// One stream of `n` items of `size` bytes: items a second.
async fn stream(ws: &mut WebSocketStream<Box<dyn Io>>, id: u64, n: u64, size: usize) -> f64 {
    let start = Instant::now();
    ws.send(Message::text(format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"stream","params":[{n},{size}]}}"#))).await.unwrap();
    let mut seen = 0u64;
    while seen < n {
        let t = next_text(ws).await;
        if t.contains(r#""method":"item""#) {
            seen += 1;
        }
    }
    n as f64 / start.elapsed().as_secs_f64()
}

async fn call(ws: &mut WebSocketStream<Box<dyn Io>>, id: u64) -> Duration {
    let start = Instant::now();
    ws.send(Message::text(format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"one","params":[]}}"#))).await.unwrap();
    next_text(ws).await;
    start.elapsed()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
async fn tls_cost_on_the_socket() {
    let t = tls();
    let (_hp, plain) = serve_websocket_with(module(), WebSocketConfig::new(0), None, UpgradeOptions::default()).await.unwrap();
    let (_ht, secure) = serve_websocket_with(module(), WebSocketConfig::new(0).with_tls(t.config.clone()), None, UpgradeOptions::default()).await.unwrap();
    let mut p = connect(plain.port(), None).await;
    let mut s = connect(secure.port(), Some(&t)).await;
    let mut id = 0u64;

    for size in [64usize, 1024] {
        // Warm both.
        for _ in 0..5 {
            id += 1;
            stream(&mut p, id, 10_000, size).await;
            stream(&mut s, id, 10_000, size).await;
        }
        let (mut rp, mut rs) = (Vec::new(), Vec::new());
        for _ in 0..21 {
            id += 1;
            rp.push(stream(&mut p, id, 10_000, size).await);
            rs.push(stream(&mut s, id, 10_000, size).await);
        }
        rp.sort_by(f64::total_cmp);
        rs.sort_by(f64::total_cmp);
        let (top_p, top_s, med_p, med_s) = (rp[18], rs[18], rp[10], rs[10]);
        println!(
            "stream.10k.{size}B  plain {:>9.0} items/s (median {:>9.0})   tls {:>9.0} items/s (median {:>9.0})   tls/plain {:.3} (median {:.3})",
            top_p, med_p, top_s, med_s, top_s / top_p, med_s / med_p
        );
    }

    let (mut cp, mut cs) = (Vec::new(), Vec::new());
    for _ in 0..500 {
        id += 1;
        call(&mut p, id).await;
        call(&mut s, id).await;
    }
    for _ in 0..5_000 {
        id += 1;
        cp.push(call(&mut p, id).await.as_secs_f64() * 1e6);
        cs.push(call(&mut s, id).await.as_secs_f64() * 1e6);
    }
    cp.sort_by(f64::total_cmp);
    cs.sort_by(f64::total_cmp);
    println!(
        "call.one_item      plain p50 {:>6.1} µs p99 {:>6.1} µs   tls p50 {:>6.1} µs p99 {:>6.1} µs   tls/plain p50 {:.3}",
        pct(&cp, 0.5),
        pct(&cp, 0.99),
        pct(&cs, 0.5),
        pct(&cs, 0.99),
        pct(&cs, 0.5) / pct(&cp, 0.5)
    );
}
