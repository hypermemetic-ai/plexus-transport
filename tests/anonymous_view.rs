//! AUTH-6: an anonymous upgrade enters the anonymous view; RFC 9728
//! metadata and `WWW-Authenticate` for HTTP clients.
//!
//! Under `anonymous_view`, an upgrade that presents no credential is
//! admitted without an `AuthContext`; one that presents a credential the
//! validator rejects is refused 401 with `error="invalid_token"` and, when
//! metadata is configured, `resource_metadata`. Presented means an
//! `Authorization` header or a declared credential cookie with a value —
//! `theme=dark` presents nothing. Without `anonymous_view`, nothing changes.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use plexus_core::plexus::{AuthContext, SessionValidator};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::Service;

use plexus_transport::websocket::testing::{combined_auth_middleware_with_options, CombinedAuthMiddleware};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{ProtectedResource, WebSocketConfig};

/// Admits `access_token=good` in a Cookie header, and the bearer `good`.
#[derive(Clone, Default)]
struct Validator {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SessionValidator for Validator {
    async fn validate(&self, input: &str) -> Option<AuthContext> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let good = input == "good" || input.split(';').any(|c| c.trim() == "access_token=good");
        good.then(|| AuthContext {
            user_id: "alice".into(),
            session_id: "s1".into(),
            roles: vec![],
            metadata: json!({}),
        })
    }
}

/// The inner service: 200, and whether the request reached it with an
/// `AuthContext` or without.
#[derive(Clone, Default)]
struct Inner {
    reached: Arc<AtomicUsize>,
    with_auth: Arc<AtomicUsize>,
}

impl Service<http::Request<Empty<Bytes>>> for Inner {
    type Response = http::Response<jsonrpsee::server::HttpBody>;
    type Error = Infallible;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Empty<Bytes>>) -> Self::Future {
        self.reached.fetch_add(1, Ordering::SeqCst);
        if req.extensions().get::<Arc<AuthContext>>().is_some() {
            self.with_auth.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async { Ok(http::Response::builder().status(200).body(jsonrpsee::server::HttpBody::from("ok")).unwrap()) })
    }
}

const RESOURCE: &str = "http://127.0.0.1:4455";
const METADATA: &str = "http://127.0.0.1:4455/.well-known/oauth-protected-resource";

fn resource() -> ProtectedResource {
    ProtectedResource::new(RESOURCE, ["https://as.example/client_01"]).with_scopes(["tickets.get", "tickets.set_status"])
}

fn anonymous() -> UpgradeOptions {
    UpgradeOptions {
        anonymous_view: true,
        credential_cookies: vec!["menger_session_4456".into(), "access_token".into()],
        protected_resource: Some(resource()),
        ..UpgradeOptions::default()
    }
}

fn middleware(options: &UpgradeOptions, v: &Validator, inner: &Inner) -> CombinedAuthMiddleware<Inner> {
    combined_auth_middleware_with_options(inner.clone(), Some(Arc::new(v.clone())), &[], options)
}

struct Reply {
    status: u16,
    challenge: Option<String>,
    content_type: Option<String>,
    body: String,
}

async fn send(svc: &mut CombinedAuthMiddleware<Inner>, uri: &str, headers: &[(&str, &str)]) -> Reply {
    let mut b = http::Request::builder().method("GET").uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let resp = Service::call(svc, b.body(Empty::<Bytes>::new()).unwrap()).await.expect("call");
    let header = |name| resp.headers().get(name).map(|h: &http::HeaderValue| h.to_str().unwrap().to_string());
    let (status, challenge, content_type) =
        (resp.status().as_u16(), header(http::header::WWW_AUTHENTICATE), header(http::header::CONTENT_TYPE));
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    Reply { status, challenge, content_type, body: String::from_utf8_lossy(&body).into_owned() }
}

async fn upgrade(svc: &mut CombinedAuthMiddleware<Inner>, headers: &[(&str, &str)]) -> Reply {
    send(svc, "/", headers).await
}

#[tokio::test]
async fn no_credential_is_admitted_into_the_anonymous_view() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    let r = upgrade(&mut svc, &[("origin", "http://127.0.0.1:4456")]).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(inner.reached.load(Ordering::SeqCst), 1);
    assert_eq!(inner.with_auth.load(Ordering::SeqCst), 0, "admitted with no AuthContext");
}

#[tokio::test]
async fn unrelated_cookies_present_nothing() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    let r = upgrade(&mut svc, &[("cookie", "theme=dark; _ga=GA1.2.3")]).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(inner.with_auth.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_empty_credential_cookie_presents_nothing() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("cookie", "access_token=; theme=dark")]).await.status, 200);
}

#[tokio::test]
async fn a_bad_bearer_is_refused_with_invalid_token_and_the_metadata_url() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    let r = upgrade(&mut svc, &[("authorization", "Bearer mgs_expired")]).await;
    assert_eq!(r.status, 401);
    assert_eq!(
        r.challenge.as_deref(),
        Some(format!(r#"Bearer realm="plexus", error="invalid_token", resource_metadata="{METADATA}""#).as_str())
    );
    assert_eq!(inner.reached.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn any_authorization_scheme_is_a_presentation() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("authorization", "Basic YTpi")]).await.status, 401);
}

#[tokio::test]
async fn a_recognised_credential_cookie_with_a_bad_value_is_refused() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    let r = upgrade(&mut svc, &[("cookie", "theme=dark; menger_session_4456=stale")]).await;
    assert_eq!(r.status, 401, "{}", r.body);
    assert!(r.challenge.unwrap().contains(r#"error="invalid_token""#));
    assert_eq!(inner.reached.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_good_credential_signs_the_caller_in() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("cookie", "theme=dark; access_token=good")]).await.status, 200);
    assert_eq!(upgrade(&mut svc, &[("authorization", "Bearer good")]).await.status, 200);
    assert_eq!(inner.with_auth.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn without_declared_cookies_a_stale_cookie_falls_back_to_the_anonymous_view() {
    let (v, inner) = (Validator::default(), Inner::default());
    let options = UpgradeOptions { credential_cookies: vec![], ..anonymous() };
    let mut svc = middleware(&options, &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("cookie", "access_token=stale")]).await.status, 200);
    assert_eq!(inner.with_auth.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn anonymous_view_wins_over_strict_mode_only_for_the_absent_credential() {
    let (v, inner) = (Validator::default(), Inner::default());
    let options = UpgradeOptions { reject_on_session_failure: true, ..anonymous() };
    let mut svc = middleware(&options, &v, &inner);
    assert_eq!(upgrade(&mut svc, &[]).await.status, 200);
    assert_eq!(upgrade(&mut svc, &[("authorization", "Bearer bad")]).await.status, 401);
}

#[tokio::test]
async fn the_metadata_document_is_served_before_any_check() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&anonymous(), &v, &inner);
    let r = send(&mut svc, "/.well-known/oauth-protected-resource", &[("authorization", "Bearer bad")]).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.content_type.as_deref(), Some("application/json"));
    let doc: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(
        doc,
        json!({
            "resource": RESOURCE,
            "authorization_servers": ["https://as.example/client_01"],
            "scopes_supported": ["tickets.get", "tickets.set_status"],
            "bearer_methods_supported": ["header"],
        })
    );
    assert_eq!(v.calls.load(Ordering::SeqCst), 0, "no credential was read");
    assert_eq!(inner.reached.load(Ordering::SeqCst), 0);
}

// ── Without anonymous_view: today's behavior ─────────────────────────────

#[tokio::test]
async fn without_anonymous_view_strict_mode_still_refuses_no_credential() {
    let (v, inner) = (Validator::default(), Inner::default());
    let options = UpgradeOptions { reject_on_session_failure: true, ..UpgradeOptions::default() };
    let mut svc = middleware(&options, &v, &inner);
    let r = upgrade(&mut svc, &[]).await;
    assert_eq!(r.status, 401);
    assert_eq!(r.body, "Unauthorized: session cookie or bearer required");
    assert_eq!(r.challenge.as_deref(), Some(r#"Bearer realm="plexus""#), "no error code for an absent credential");
    assert_eq!(upgrade(&mut svc, &[("cookie", "theme=dark")]).await.status, 401);
}

#[tokio::test]
async fn without_anonymous_view_strict_mode_names_the_metadata_when_configured() {
    let (v, inner) = (Validator::default(), Inner::default());
    let options = UpgradeOptions {
        reject_on_session_failure: true,
        protected_resource: Some(resource()),
        ..UpgradeOptions::default()
    };
    let mut svc = middleware(&options, &v, &inner);
    let r = upgrade(&mut svc, &[]).await;
    assert_eq!(r.status, 401);
    assert_eq!(r.challenge.as_deref(), Some(format!(r#"Bearer realm="plexus", resource_metadata="{METADATA}""#).as_str()));
}

#[tokio::test]
async fn without_anonymous_view_or_strict_mode_a_bad_credential_passes_through() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&UpgradeOptions::default(), &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("authorization", "Bearer bad")]).await.status, 200);
    assert_eq!(inner.with_auth.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn without_metadata_the_well_known_path_is_not_answered() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&UpgradeOptions::default(), &v, &inner);
    send(&mut svc, "/.well-known/oauth-protected-resource", &[]).await;
    assert_eq!(inner.reached.load(Ordering::SeqCst), 1, "passed through to jsonrpsee");
}

// ── On a real socket ─────────────────────────────────────────────────────

async fn raw(addr: std::net::SocketAddr, request: &str) -> String {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut buf)).await.unwrap().unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn upgrade_request(extra: &str) -> String {
    format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{extra}\r\n"
    )
}

#[tokio::test]
async fn on_the_wire() {
    let mut config = WebSocketConfig::new(0);
    config.addr = "127.0.0.1:0".parse().unwrap();
    let (handle, addr) = serve_websocket_with(
        jsonrpsee::RpcModule::new(()),
        config,
        Some(Arc::new(Validator::default())),
        anonymous(),
    )
    .await
    .unwrap();

    let r = raw(addr, &upgrade_request("Origin: http://127.0.0.1:4456\r\nCookie: theme=dark\r\n")).await;
    assert!(r.starts_with("HTTP/1.1 101"), "{r}");

    let r = raw(addr, &upgrade_request("Authorization: Bearer mgs_expired\r\n")).await;
    assert!(r.starts_with("HTTP/1.1 401"), "{r}");
    assert!(
        r.to_ascii_lowercase().contains(&format!(
            r#"www-authenticate: bearer realm="plexus", error="invalid_token", resource_metadata="{METADATA}""#
        )),
        "{r}"
    );

    let r = raw(addr, "GET /.well-known/oauth-protected-resource HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    assert!(r.to_ascii_lowercase().contains("content-type: application/json"), "{r}");
    assert!(r.contains(r#""authorization_servers":["https://as.example/client_01"]"#), "{r}");

    handle.stop().unwrap();
}
