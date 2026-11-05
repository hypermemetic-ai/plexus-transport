//! TRANSPORT-26: the transport serves TLS itself (feature `tls`).
//!
//! Every listener a [`TransportServerBuilder`](crate::TransportServerBuilder)
//! opens — the WebSocket, the REST bridge, the MCP bridges — and the combined
//! port take a [`TlsConfig`]: a PEM certificate chain and its private key
//! (RSA, ECDSA or Ed25519), served with rustls on the ring provider. No proxy
//! in front is needed for `https://` and `wss://`.
//!
//! | when | then |
//! | -- | -- |
//! | a listener is given a [`TlsConfig`] | it answers TLS only (TLS 1.3 and 1.2, ALPN `http/1.1`); the pair is read once at start, and a pair that does not load refuses the start |
//! | a TLS handshake | done in the connection's own task, within the listener's header timeout (RPC-4), never on the accept loop |
//! | a plain-HTTP request or a bad handshake | refused at the handshake; logged once per peer address (warn), never a panic; the listener keeps serving |
//! | the certificate or the key changes on disk | the next handshake reads the new pair (each handshake compares both files' size, mtime and inode — a renewal by rename or by symlink, as certbot and `tailscale cert` do, included); a pair that fails to load (a half-written renewal, a key that does not match) keeps the old one, says why once, and is retried when either file changes again |
//! | a request on a TLS connection | the connection is secure: [`Connection::is_tls`](crate::websocket::Connection::is_tls) and the request's [`ForwardedClient::secure`](crate::ForwardedClient) on the WebSocket, and [`SecureTransport`](crate::request::SecureTransport) for its peer address while the connection lasts — without any forwarded header |
//! | no [`TlsConfig`] | the listener is what it was, byte for byte: the same accept loop, the same `TcpStream` |
//!
//! Not here: client certificates (mutual TLS), SNI with several
//! certificates, OCSP stapling, HTTP/2.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::TlsAcceptor;

/// A listener's TLS: a PEM certificate chain (the leaf first, then its
/// intermediates) and the PEM private key for it (PKCS#8, PKCS#1 or SEC1;
/// RSA, ECDSA or Ed25519). Both files are read again whenever either
/// changes (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// The certificate chain, PEM.
    pub cert: PathBuf,
    /// The private key, PEM.
    pub key: PathBuf,
}

impl TlsConfig {
    /// The chain at `cert` and the key at `key`.
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self { cert: cert.into(), key: key.into() }
    }
}

/// How long a TLS handshake may take on a listener with no header timeout
/// of its own (the REST bridge, MCP): the WebSocket's default (RPC-4).
#[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The ring provider, always: a graph that also carries aws-lc-rs (reqwest's
/// default) leaves rustls no process default to pick. Its suites with
/// AES-128-GCM first, chosen by this side ([`Tls::load`]): what browsers
/// ask for anyway, and a quarter fewer AES rounds a byte than AES-256 for a
/// rustls client, which asks for AES-256 first (the perf row).
fn provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            let mut ring = rustls::crypto::ring::default_provider();
            ring.cipher_suites.sort_by_key(|s| match s.suite() {
                rustls::CipherSuite::TLS13_AES_128_GCM_SHA256 => 0,
                rustls::CipherSuite::TLS13_AES_256_GCM_SHA384 => 1,
                rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256 => 2,
                _ => 3,
            });
            Arc::new(ring)
        })
        .clone()
}

/// What a file is now: size, mtime, inode — read through symlinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

fn stamp(path: &Path) -> Option<Stamp> {
    let m = std::fs::metadata(path).ok()?;
    #[cfg(unix)]
    let ino = std::os::unix::fs::MetadataExt::ino(&m);
    #[cfg(not(unix))]
    let ino = 0;
    Some(Stamp { len: m.len(), modified: m.modified().ok(), ino })
}

type Stamps = (Option<Stamp>, Option<Stamp>);

/// Read and check one pair.
fn load(config: &TlsConfig, provider: &CryptoProvider) -> Result<CertifiedKey, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&config.cert)
        .map_err(|e| format!("certificate {}: {e}", config.cert.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("certificate {}: {e}", config.cert.display()))?;
    if certs.is_empty() {
        return Err(format!("certificate {}: no CERTIFICATE in it", config.cert.display()));
    }
    let key = PrivateKeyDer::from_pem_file(&config.key).map_err(|e| format!("key {}: {e}", config.key.display()))?;
    CertifiedKey::from_der(certs, key, provider)
        .map_err(|e| format!("{} with {}: {e}", config.cert.display(), config.key.display()))
}

/// The pair a handshake presents, read again when its files change.
#[derive(Debug)]
struct Reloading {
    config: TlsConfig,
    provider: Arc<CryptoProvider>,
    current: RwLock<(Stamps, Arc<CertifiedKey>)>,
    /// The files as they were when a load last failed: not tried again (nor
    /// said again) until one of them changes.
    failed: Mutex<Option<Stamps>>,
}

impl Reloading {
    fn new(config: TlsConfig) -> Result<Self, String> {
        let provider = provider();
        let stamps = (stamp(&config.cert), stamp(&config.key));
        let key = load(&config, &provider)?;
        Ok(Self { config, provider, current: RwLock::new((stamps, Arc::new(key))), failed: Mutex::new(None) })
    }

    fn current(&self) -> Arc<CertifiedKey> {
        let stamps = (stamp(&self.config.cert), stamp(&self.config.key));
        {
            let current = self.current.read().unwrap_or_else(|p| p.into_inner());
            if current.0 == stamps {
                return current.1.clone();
            }
        }
        let mut failed = self.failed.lock().unwrap_or_else(|p| p.into_inner());
        let mut current = self.current.write().unwrap_or_else(|p| p.into_inner());
        if current.0 == stamps || *failed == Some(stamps) {
            return current.1.clone();
        }
        match load(&self.config, &self.provider) {
            Ok(key) => {
                tracing::info!(cert = %self.config.cert.display(), "TLS: the certificate changed on disk; new connections present the new one");
                *current = (stamps, Arc::new(key));
                *failed = None;
            }
            Err(why) => {
                tracing::warn!("TLS: the changed certificate or key does not load, so the one loaded before is still served: {why}");
                *failed = Some(stamps);
            }
        }
        current.1.clone()
    }
}

impl ResolvesServerCert for Reloading {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

/// A listener's TLS, loaded: the acceptor, and the peers whose refusal has
/// been said.
#[derive(Clone)]
pub(crate) struct Tls {
    acceptor: TlsAcceptor,
    said: Arc<Mutex<HashSet<IpAddr>>>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Tls")
    }
}

impl Tls {
    /// Read the pair (an error names the file and why) and make the acceptor.
    pub(crate) fn load(config: &TlsConfig) -> anyhow::Result<Self> {
        let resolver = Reloading::new(config.clone()).map_err(|e| anyhow::anyhow!("TLS: {e}"))?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| anyhow::anyhow!("TLS: {e}"))?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        server.ignore_client_order = true;
        Ok(Self { acceptor: TlsAcceptor::from(Arc::new(server)), said: Arc::default() })
    }

    /// The handshake on `sock`, within `timeout`: the stream, or `None` (the
    /// refusal said once for `peer`'s address).
    pub(crate) async fn accept(&self, sock: TcpStream, peer: SocketAddr, timeout: Duration) -> Option<TlsIo> {
        let why = match tokio::time::timeout(timeout, self.acceptor.accept(sock)).await {
            Ok(Ok(stream)) => return Some(TlsIo { inner: stream, _peer: SecurePeer::new(peer) }),
            Ok(Err(e)) => e.to_string(),
            Err(_) => "no handshake within the time".to_string(),
        };
        let first = {
            let mut said = self.said.lock().unwrap_or_else(|p| p.into_inner());
            if said.len() >= 4096 {
                said.clear();
            }
            said.insert(peer.ip())
        };
        if first {
            tracing::warn!(%peer, "TLS handshake refused (a plain http:// request to an https:// port?): {why}");
        } else {
            tracing::debug!(%peer, "TLS handshake refused: {why}");
        }
        None
    }
}

// ----------------------------------------------------------- secure peers

/// The peers of every open TLS connection in the process, by address: what
/// [`SecureTransport`](crate::request::SecureTransport) reads, since a
/// request's raw context carries its peer and nothing of its connection.
fn secure_peers() -> &'static Mutex<HashMap<SocketAddr, u32>> {
    static PEERS: OnceLock<Mutex<HashMap<SocketAddr, u32>>> = OnceLock::new();
    PEERS.get_or_init(Mutex::default)
}

/// Whether `peer` is the far end of a TLS connection open on one of this
/// process's listeners.
pub fn is_tls_peer(peer: SocketAddr) -> bool {
    secure_peers().lock().unwrap_or_else(|p| p.into_inner()).contains_key(&peer)
}

/// A peer's place among the secure ones, for as long as its connection lives.
#[derive(Debug)]
struct SecurePeer(SocketAddr);

impl SecurePeer {
    fn new(peer: SocketAddr) -> Self {
        *secure_peers().lock().unwrap_or_else(|p| p.into_inner()).entry(peer).or_insert(0) += 1;
        Self(peer)
    }
}

impl Drop for SecurePeer {
    fn drop(&mut self) {
        let mut peers = secure_peers().lock().unwrap_or_else(|p| p.into_inner());
        if let Some(n) = peers.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                peers.remove(&self.0);
            }
        }
    }
}

/// A TLS connection's stream, holding its peer's place among the secure ones
/// until it is dropped — after an upgrade too, since the WebSocket keeps it.
pub(crate) struct TlsIo {
    inner: tokio_rustls::server::TlsStream<TcpStream>,
    _peer: SecurePeer,
}

impl AsyncRead for TlsIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[io::IoSlice<'_>]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ------------------------------------------------- axum's listeners, in TLS

/// An axum [`Listener`](axum::serve::Listener) that hands axum only
/// connections whose handshake is done: each handshake runs in its own task
/// (so a slow one holds up no other), and a refused one never reaches axum.
#[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
pub(crate) struct TlsListener {
    ready: tokio::sync::mpsc::Receiver<(TlsIo, SocketAddr)>,
    local: SocketAddr,
}

#[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
impl TlsListener {
    pub(crate) fn new(listener: TcpListener, tls: Tls) -> io::Result<Self> {
        let local = listener.local_addr()?;
        let (tx, ready) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                let (sock, peer) = tokio::select! {
                    res = listener.accept() => match res {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::error!("TLS accept: {e}");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    },
                    _ = tx.closed() => break,
                };
                let _ = sock.set_nodelay(true);
                let (tls, tx) = (tls.clone(), tx.clone());
                tokio::spawn(async move {
                    if let Some(io) = tls.accept(sock, peer, HANDSHAKE_TIMEOUT).await {
                        let _ = tx.send((io, peer)).await;
                    }
                });
            }
        });
        Ok(Self { ready, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.ready.recv().await {
            Some(x) => x,
            // The accept loop ended (only once this listener is dropped).
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}

/// Serve `router` on `listener`, in TLS when `tls` names a pair: what the
/// REST bridge's and MCP's listeners run. With no TLS this is
/// `axum::serve(listener, router)`, as before.
#[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
pub(crate) async fn serve_axum(listener: TcpListener, router: axum::Router, tls: Option<&TlsConfig>) -> anyhow::Result<tokio::task::JoinHandle<io::Result<()>>> {
    match tls {
        None => Ok(tokio::spawn(async move { axum::serve(listener, router).await })),
        Some(config) => {
            let listener = TlsListener::new(listener, Tls::load(config)?)?;
            Ok(tokio::spawn(async move { axum::serve(listener, router).await }))
        }
    }
}
