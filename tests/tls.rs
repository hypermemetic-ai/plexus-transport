//! TRANSPORT-26: plexus-transport serves TLS itself. A test CA is made in
//! each test (rcgen); a client trusting it calls the WebSocket over `wss://`,
//! the REST bridge and the MCP bridges over `https://` — on their own
//! listeners and on the combined port — and a duplex call with binary
//! frames; a host's routes share the socket's TLS port; a plain-HTTP request
//! is refused and the listener goes on; a certificate replaced on disk is
//! the next connection's.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use plexus_core::activations::echo::EchoActivation;
use plexus_core::plexus::DynamicHub;
use plexus_transport::request::{RawRequestContext, SecureTransport};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{ForwardedClient, Hooks, HostRoutes, TlsConfig, TransportServer, TrustedProxies, WebSocketConfig};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, SignatureAlgorithm};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const WAIT: Duration = Duration::from_secs(10);

// ------------------------------------------------------------------ the CA

/// A test CA, its leaves written to a directory of the test's own.
struct Pki {
    dir: PathBuf,
    ca_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

/// One leaf: its PEM, its key's PEM, its DER (what a handshake presents).
struct Leaf {
    cert_pem: String,
    key_pem: String,
    der: Vec<u8>,
}

impl Pki {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("plexus-transport-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, "TRANSPORT-26 test CA");
        let ca = params.self_signed(&key).unwrap();
        Self { dir, ca_pem: ca.pem(), issuer: Issuer::new(params, key) }
    }

    fn leaf(&self, alg: &'static SignatureAlgorithm) -> Leaf {
        let key = KeyPair::generate_for(alg).unwrap();
        let params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf { cert_pem: cert.pem(), key_pem: key.serialize_pem(), der: cert.der().to_vec() }
    }

    /// The pair's paths, the files written as a renewal writes them: each
    /// to a temporary name, then renamed over the old.
    fn write(&self, leaf: &Leaf) -> TlsConfig {
        let config = TlsConfig::new(self.dir.join("server.crt"), self.dir.join("server.key"));
        self.write_raw(&config, &leaf.cert_pem, &leaf.key_pem);
        config
    }

    fn write_raw(&self, config: &TlsConfig, cert: &str, key: &str) {
        for (path, text) in [(&config.cert, cert), (&config.key, key)] {
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, text).unwrap();
            std::fs::rename(&tmp, path).unwrap();
        }
    }

    fn client(&self) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        let ca = rustls::pki_types::CertificateDer::from_pem_slice(self.ca_pem.as_bytes()).unwrap();
        roots.add(ca).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        Arc::new(
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }

    /// A TLS connection to `port` that trusts this CA, and its local address.
    async fn connect(&self, port: u16) -> (TlsStream<TcpStream>, SocketAddr) {
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let local = tcp.local_addr().unwrap();
        let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
        let tls = tokio::time::timeout(WAIT, tokio_rustls::TlsConnector::from(self.client()).connect(name, tcp))
            .await
            .expect("a handshake within the time")
            .expect("the handshake");
        (tls, local)
    }

    /// The leaf a new connection to `port` is shown.
    async fn presented(&self, port: u16) -> Vec<u8> {
        let (tls, _) = self.connect(port).await;
        tls.get_ref().1.peer_certificates().unwrap()[0].to_vec()
    }

    async fn wss(&self, port: u16, path: &str) -> (WebSocketStream<TlsStream<TcpStream>>, SocketAddr) {
        let (tls, local) = self.connect(port).await;
        let (ws, _) = tokio_tungstenite::client_async(format!("wss://127.0.0.1:{port}{path}"), tls).await.expect("upgrade");
        (ws, local)
    }

    /// One HTTPS request: its status and body.
    async fn https(&self, port: u16, method: &str, path: &str, body: Option<Value>) -> (http::StatusCode, String) {
        let (tls, _) = self.connect(port).await;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.unwrap();
        tokio::spawn(conn);
        let mut req = http::Request::builder().method(method).uri(path).header("host", format!("127.0.0.1:{port}"));
        if body.is_some() {
            req = req.header("content-type", "application/json");
        }
        let body = body.map(|b| Bytes::from(b.to_string())).unwrap_or_default();
        let resp = sender.send_request(req.body(Full::new(body)).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }
}

impl Drop for Pki {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn up(port: u16) {
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing listening on {port}");
}

// ------------------------------------------------------------ the service

fn echo_hub() -> Arc<DynamicHub> {
    Arc::new(DynamicHub::new("x").register(EchoActivation::new()))
}

fn echo_module() -> jsonrpsee::RpcModule<()> {
    DynamicHub::arc_into_rpc_module(echo_hub()).unwrap()
}

/// `echo.echo` over a socket: the data item's content.
async fn echo_over<S>(ws: &mut WebSocketStream<S>, message: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let call = json!({"jsonrpc":"2.0","id":1,"method":"x.call","params":{"method":"echo.echo","params":{"message":message,"count":1}}});
    ws.send(Message::text(call.to_string())).await.unwrap();
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next()).await.expect("a frame within the time").expect("open").expect("a frame");
        let Message::Text(t) = msg else { continue };
        let frame: Value = serde_json::from_str(t.as_str()).unwrap();
        let item = &frame["params"]["result"];
        match item["type"].as_str() {
            Some("data") => return item["content"].clone(),
            Some("error") => panic!("{frame}"),
            _ => {}
        }
    }
}

/// Proxies named elsewhere: a loopback peer is not secure by itself, so only
/// TLS can make it so.
fn proxy_elsewhere() -> TrustedProxies {
    TrustedProxies::parse(["10.9.9.9"]).unwrap()
}

/// What each upgraded connection was: its `ForwardedClient`, and whether
/// the connection says TLS.
type Seen = Arc<Mutex<Vec<(ForwardedClient, bool)>>>;

fn seeing(seen: &Seen) -> Hooks {
    let seen = seen.clone();
    Hooks::new(move |conn, ext| {
        seen.lock().unwrap().push((*ext.get::<ForwardedClient>().expect("every upgrade carries its client"), conn.is_tls()));
        None
    })
}

fn raw(peer: SocketAddr) -> RawRequestContext {
    RawRequestContext { headers: http::HeaderMap::new(), uri: "/".parse().unwrap(), auth: None, peer: Some(peer) }
}

// ------------------------------------------------------------- the socket

/// A client trusting the CA calls over `wss://`; the connection is secure
/// with no forwarded header — `ForwardedClient::secure`, `is_tls`, and
/// `SecureTransport` for its peer while it lasts — where a plain one with
/// the same proxies is not.
#[tokio::test(flavor = "multi_thread")]
async fn wss_calls_the_socket_and_the_connection_is_secure() {
    let pki = Pki::new();
    let leaf = pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256);
    let tls = pki.write(&leaf);
    let seen = Seen::default();
    let options = UpgradeOptions { hooks: Some(seeing(&seen)), ..Default::default() };

    let config = WebSocketConfig::new(0).with_trusted_proxies(proxy_elsewhere()).with_tls(tls);
    let (_h, addr) = serve_websocket_with(echo_module(), config, None, options.clone()).await.unwrap();
    let (mut ws, local) = pki.wss(addr.port(), "/").await;
    assert_eq!(echo_over(&mut ws, "over wss").await["message"], "over wss");
    let (client, is_tls) = seen.lock().unwrap()[0];
    assert!(is_tls && client.secure && !client.via_proxy, "{client:?}");
    assert!(SecureTransport::check(&raw(local), &proxy_elsewhere()).is_ok(), "the TLS peer is secure");

    // The same, plain: not secure.
    let plain = WebSocketConfig::new(0).with_trusted_proxies(proxy_elsewhere());
    let (_h2, plain_addr) = serve_websocket_with(echo_module(), plain, None, options).await.unwrap();
    let tcp = TcpStream::connect(plain_addr).await.unwrap();
    let plain_local = tcp.local_addr().unwrap();
    let (mut pws, _) = tokio_tungstenite::client_async(format!("ws://{plain_addr}/"), tcp).await.unwrap();
    assert_eq!(echo_over(&mut pws, "plain").await["message"], "plain");
    let (client, is_tls) = seen.lock().unwrap()[1];
    assert!(!is_tls && !client.secure, "{client:?}");
    assert!(SecureTransport::check(&raw(plain_local), &proxy_elsewhere()).is_err());

    // Once the TLS connection is gone, its peer is no longer secure.
    drop(ws);
    tokio::time::timeout(WAIT, async {
        while SecureTransport::check(&raw(local), &proxy_elsewhere()).is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the peer's place goes with its connection");
}

/// An Ed25519 leaf serves as an ECDSA one does.
#[tokio::test(flavor = "multi_thread")]
async fn an_ed25519_pair_serves() {
    let pki = Pki::new();
    let leaf = pki.leaf(&rcgen::PKCS_ED25519);
    let config = WebSocketConfig::new(0).with_tls(pki.write(&leaf));
    let (_h, addr) = serve_websocket_with(echo_module(), config, None, UpgradeOptions::default()).await.unwrap();
    assert_eq!(pki.presented(addr.port()).await, leaf.der);
    let (mut ws, _) = pki.wss(addr.port(), "/").await;
    assert_eq!(echo_over(&mut ws, "ed").await["message"], "ed");
}

/// A pair that does not load refuses the start, naming the file.
#[tokio::test]
async fn a_pair_that_does_not_load_refuses_the_start() {
    let pki = Pki::new();
    let missing = TlsConfig::new(pki.dir.join("nope.crt"), pki.dir.join("nope.key"));
    let err = serve_websocket_with(echo_module(), WebSocketConfig::new(0).with_tls(missing), None, UpgradeOptions::default())
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("nope.crt"), "{err}");

    // A key that is not the certificate's.
    let (a, b) = (pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256), pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256));
    let config = pki.write(&a);
    pki.write_raw(&config, &a.cert_pem, &b.key_pem);
    let err = serve_websocket_with(echo_module(), WebSocketConfig::new(0).with_tls(config), None, UpgradeOptions::default())
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("server.crt"), "{err}");
}

/// A duplex call (`Input<Bytes>`, binary frames) over `wss://`: items in,
/// events out, as over `ws://` (tests/duplex_wire.rs).
#[tokio::test(flavor = "multi_thread")]
async fn a_duplex_call_with_binary_frames_over_wss() {
    use duplex_fixture::{IngestActivation, Probe};
    let pki = Pki::new();
    let config = WebSocketConfig::new(0).with_tls(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256)));
    let probe = Probe::default();
    probe.permits.add_permits(1_000_000);
    let module = DynamicHub::new("menger").register(IngestActivation { probe: probe.clone() }).into_rpc_module().unwrap();
    let (_h, addr) = serve_websocket_with(module, config, None, UpgradeOptions::default()).await.unwrap();
    let (mut ws, _) = pki.wss(addr.port(), "/").await;

    async fn next<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(ws: &mut WebSocketStream<S>) -> Value {
        loop {
            match tokio::time::timeout(WAIT, ws.next()).await.expect("a frame").unwrap().unwrap() {
                Message::Text(t) => return serde_json::from_str(t.as_str()).unwrap(),
                _ => continue,
            }
        }
    }
    ws.send(Message::text(json!({"jsonrpc":"2.0","id":1,"method":"menger.call","params":{"method":"ingest.chunks","params":{"run":"r"}}}).to_string()))
        .await
        .unwrap();
    let sub = next(&mut ws).await["result"].as_u64().expect("a subscription");
    assert_eq!(next(&mut ws).await["params"]["result"]["type"], "credit");
    let payloads: Vec<Vec<u8>> = vec![vec![0, 1, 2, 255], b"{\"not\":\"json\"}".to_vec(), vec![], (0..=255u8).collect()];
    for (seq, p) in payloads.iter().enumerate() {
        let mut frame = vec![0x01];
        frame.extend_from_slice(&sub.to_be_bytes());
        frame.extend_from_slice(&(seq as u64).to_be_bytes());
        frame.extend_from_slice(p);
        ws.send(Message::binary(frame)).await.unwrap();
    }
    ws.send(Message::text(json!({"jsonrpc":"2.0","method":"menger.end","params":{"subscription":sub,"seq":payloads.len()}}).to_string()))
        .await
        .unwrap();
    let content = loop {
        let item = next(&mut ws).await["params"]["result"].clone();
        match item["type"].as_str() {
            Some("ack" | "credit") => {}
            Some("data") => break item["content"].clone(),
            other => panic!("{other:?}: {item}"),
        }
    };
    assert_eq!(content, json!({"type":"ingested","count":4}));
    assert_eq!(next(&mut ws).await["params"]["result"]["type"], "done");
    let got: Vec<Vec<u8>> = probe.chunks.lock().unwrap().iter().map(|c| c.to_vec()).collect();
    assert_eq!(got, payloads);
}

// ----------------------------------------------------- refusals, renewals

/// A plain HTTP request to a TLS port is refused at the handshake (never
/// answered in HTTP); so is a peer that says nothing, or garbage. The next
/// TLS client is served, and one that never finishes its handshake holds up
/// no other.
#[tokio::test(flavor = "multi_thread")]
async fn plain_http_to_a_tls_port_is_refused_and_the_next_client_served() {
    let pki = Pki::new();
    let config = WebSocketConfig::new(0).with_tls(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256)));
    let (_h, addr) = serve_websocket_with(echo_module(), config, None, UpgradeOptions::default()).await.unwrap();

    // A silent peer, held open the whole test.
    let _silent = TcpStream::connect(addr).await.unwrap();

    for attempt in [&b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n\r\n"[..], &[0x16, 0x03, 0x01, 0xff, 0xff, 1, 2, 3][..]] {
        let mut tcp = TcpStream::connect(addr).await.unwrap();
        tcp.write_all(attempt).await.unwrap();
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(WAIT, tcp.read_to_end(&mut answer)).await.expect("closed within the time");
        assert!(!answer.starts_with(b"HTTP/"), "never an HTTP answer: {:?}", String::from_utf8_lossy(&answer));
    }

    let (mut ws, _) = pki.wss(addr.port(), "/").await;
    assert_eq!(echo_over(&mut ws, "still serving").await["message"], "still serving");
}

/// A certificate replaced on disk is the next connection's, with no restart;
/// an open connection goes on; a replacement that does not load (a key that
/// is not the certificate's) keeps the last good pair, and a good one after
/// it is taken.
#[tokio::test(flavor = "multi_thread")]
async fn a_certificate_replaced_on_disk_is_the_next_connections() {
    let pki = Pki::new();
    let (a, b, c) = (pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256), pki.leaf(&rcgen::PKCS_ED25519), pki.leaf(&rcgen::PKCS_ECDSA_P384_SHA384));
    let tls = pki.write(&a);
    let config = WebSocketConfig::new(0).with_tls(tls.clone());
    let (_h, addr) = serve_websocket_with(echo_module(), config, None, UpgradeOptions::default()).await.unwrap();
    let port = addr.port();
    assert_eq!(pki.presented(port).await, a.der);
    let (mut open, _) = pki.wss(port, "/").await;

    pki.write(&b);
    assert_eq!(pki.presented(port).await, b.der, "the renewal is the next connection's");
    assert_eq!(echo_over(&mut open, "kept").await["message"], "kept", "an open connection goes on");

    // A half-done renewal: C's certificate with B's key.
    pki.write_raw(&tls, &c.cert_pem, &b.key_pem);
    assert_eq!(pki.presented(port).await, b.der, "a pair that does not load keeps the last good one");
    assert_eq!(pki.presented(port).await, b.der);

    pki.write(&c);
    assert_eq!(pki.presented(port).await, c.der, "the good pair after it is taken");
    let (mut ws, _) = pki.wss(port, "/").await;
    assert_eq!(echo_over(&mut ws, "renewed").await["message"], "renewed");
}

// --------------------------------------------------- the host's routes

fn page() -> axum::Router {
    use axum::routing::get;
    axum::Router::new().route(
        "/",
        get(|axum::Extension(client): axum::Extension<ForwardedClient>| async move {
            format!("the host's page (secure: {})", client.secure)
        }),
    )
}

/// `with_routes`: `GET /` is the host's page and `/ws` the socket, on one
/// TLS port — one https origin; an upgrade elsewhere is not the socket.
#[tokio::test(flavor = "multi_thread")]
async fn the_hosts_routes_and_the_socket_share_one_tls_port() {
    let pki = Pki::new();
    let port = free_port();
    let server = TransportServer::builder(echo_hub(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(port)
        .without_registry()
        .allow_missing_auth_middleware()
        .with_trusted_proxies(proxy_elsewhere())
        .with_tls(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256)))
        .with_routes(page())
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    up(port).await;

    assert_eq!(pki.https(port, "GET", "/", None).await, (http::StatusCode::OK, "the host's page (secure: true)".to_string()));
    let (mut ws, _) = pki.wss(port, "/ws").await;
    assert_eq!(echo_over(&mut ws, "one origin").await["message"], "one origin");
    let (tls, _) = pki.connect(port).await;
    assert!(tokio_tungstenite::client_async(format!("wss://127.0.0.1:{port}/elsewhere"), tls).await.is_err(), "the socket is at /ws");
}

/// The same with no TLS: one origin over `http://` and `ws://`, the socket at
/// the path the host names.
#[tokio::test(flavor = "multi_thread")]
async fn the_hosts_routes_without_tls() {
    let port = free_port();
    let server = TransportServer::builder(echo_hub(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(port)
        .without_registry()
        .allow_missing_auth_middleware()
        .with_host_routes(HostRoutes::new(page()).socket_at("/rpc"))
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    up(port).await;

    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp)).await.unwrap();
    tokio::spawn(conn);
    let req = http::Request::builder().uri("/").header("host", format!("127.0.0.1:{port}")).body(Full::new(Bytes::new())).unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), http::StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"the host's page (secure: true)", "a loopback peer with no proxy, as ever");

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/rpc")).await.unwrap();
    assert_eq!(echo_over(&mut ws, "plain origin").await["message"], "plain origin");
}

// ------------------------------------------------------------ the bridges

#[cfg(feature = "http-gateway")]
fn route_fn(hub: Arc<DynamicHub>) -> plexus_transport::RouteFn {
    Arc::new(move |method, params| {
        let hub = hub.clone();
        Box::pin(async move { hub.route(&method, params, None).await })
    })
}

/// The REST bridge on its own listener over `https://`, beside `wss://`, one
/// pair from the builder.
#[cfg(feature = "http-gateway")]
#[tokio::test(flavor = "multi_thread")]
async fn the_rest_bridge_over_https_on_its_own_listener() {
    let pki = Pki::new();
    let (ws_port, rest_port) = (free_port(), free_port());
    let hub = echo_hub();
    let server = TransportServer::builder(hub.clone(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(ws_port)
        .with_rest_http(rest_port)
        .with_flat_schemas(hub.list_plugin_schemas())
        .with_route_fn(route_fn(hub))
        .without_registry()
        .allow_missing_auth_middleware()
        .with_tls(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256)))
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    up(ws_port).await;
    up(rest_port).await;

    let (status, body) = pki.https(rest_port, "POST", "/rest/echo/echo", Some(json!({"message":"over https","count":1}))).await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    assert!(body.contains("over https"), "{body}");
    let (mut ws, _) = pki.wss(ws_port, "/").await;
    assert_eq!(echo_over(&mut ws, "beside it").await["message"], "beside it");

    // Plain HTTP to the REST bridge's TLS port: refused, and it goes on.
    let mut tcp = TcpStream::connect(("127.0.0.1", rest_port)).await.unwrap();
    tcp.write_all(b"POST /rest/echo/echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\n\r\n{}").await.unwrap();
    let mut answer = Vec::new();
    let _ = tokio::time::timeout(WAIT, tcp.read_to_end(&mut answer)).await.expect("closed within the time");
    assert!(!answer.starts_with(b"HTTP/"), "{:?}", String::from_utf8_lossy(&answer));
    let (status, _) = pki.https(rest_port, "POST", "/rest/echo/echo", Some(json!({"message":"again","count":1}))).await;
    assert_eq!(status, http::StatusCode::OK);
}

#[cfg(feature = "__mcp-test-client")]
async fn mcp_lists_and_calls_echo(pki: &Pki, url: String) {
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::ServiceExt;

    let http = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(pki.ca_pem.as_bytes()).unwrap()])
        .build()
        .unwrap();
    let transport = StreamableHttpClientTransport::with_client(http, StreamableHttpClientTransportConfig::with_uri(url));
    let client = ().serve(transport).await.expect("initialize over https");
    let names: Vec<String> = client.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    assert!(names.iter().any(|n| n == "echo.echo"), "{names:?}");
    let r = client
        .peer()
        .call_tool(CallToolRequestParams::new("echo.echo").with_arguments(json!({"message": "mcp over https", "count": 1}).as_object().cloned().unwrap()))
        .await
        .unwrap();
    let text: String = r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect();
    assert_ne!(r.is_error, Some(true), "{text}");
    assert!(text.contains("mcp over https"), "{text}");
}

/// The MCP bridges on their own listeners over `https://`: the mounted one
/// (`with_mcp`, TRANSPORT-16) and `with_mcp_http`, an rmcp client trusting
/// the CA.
#[cfg(feature = "__mcp-test-client")]
#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_bridges_over_https_on_their_own_listeners() {
    let pki = Pki::new();
    let (ws_port, mounted, legacy) = (free_port(), free_port(), free_port());
    let hub = echo_hub();
    let server = TransportServer::builder(hub.clone(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(ws_port)
        .with_mcp(plexus_transport::McpConfig::listen(([127, 0, 0, 1], mounted).into()))
        .with_mcp_http(legacy)
        .with_flat_schemas(hub.list_plugin_schemas())
        .with_route_fn(route_fn(hub))
        .without_registry()
        .allow_missing_auth_middleware()
        .with_tls(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256)))
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    for p in [ws_port, mounted, legacy] {
        up(p).await;
    }
    mcp_lists_and_calls_echo(&pki, format!("https://127.0.0.1:{mounted}/mcp")).await;
    mcp_lists_and_calls_echo(&pki, format!("https://127.0.0.1:{legacy}/mcp")).await;
}

/// The combined port over TLS: `https://…/mcp`, `https://…/rest`, the
/// socket at `wss://…/ws` and the host's page at `/`, one origin.
#[cfg(all(feature = "mcp-gateway", feature = "http-gateway", feature = "__mcp-test-client"))]
#[tokio::test(flavor = "multi_thread")]
async fn the_combined_port_over_tls_with_the_hosts_routes() {
    use plexus_transport::{serve_combined_with, CombinedOptions};
    let pki = Pki::new();
    let port = free_port();
    let hub = echo_hub();
    let module = DynamicHub::arc_into_rpc_module(hub.clone()).unwrap();
    let options = CombinedOptions {
        tls: Some(pki.write(&pki.leaf(&rcgen::PKCS_ECDSA_P256_SHA256))),
        routes: Some(HostRoutes::new(page())),
        ..CombinedOptions::default()
    };
    let schemas = hub.list_plugin_schemas();
    let _handle = serve_combined_with(module, hub.clone(), Some(schemas), Some(route_fn(hub)), ([127, 0, 0, 1], port).into(), None, true, options)
        .await
        .unwrap();
    up(port).await;

    mcp_lists_and_calls_echo(&pki, format!("https://127.0.0.1:{port}/mcp")).await;
    let (status, body) = pki.https(port, "POST", "/rest/echo/echo", Some(json!({"message":"combined rest","count":1}))).await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    assert!(body.contains("combined rest"), "{body}");
    let (mut ws, _) = pki.wss(port, "/ws").await;
    assert_eq!(echo_over(&mut ws, "combined socket").await["message"], "combined socket");
    let (status, body) = pki.https(port, "GET", "/", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(body.starts_with("the host's page"), "{body}");
}
