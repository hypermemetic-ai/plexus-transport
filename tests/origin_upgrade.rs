//! IDENTITY-20: an Origin allow-list at the WebSocket upgrade.
//!
//! Cookies are scoped to a host, not a port, so a page served from any port
//! of the host can open a socket that carries a person's session cookie.
//! With an allow-list, an upgrade from an unlisted origin — `null`, from a
//! sandboxed frame, included — is refused 403 before any credential is read;
//! one without an Origin (synapse, a CLI) proceeds to the session validator.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use plexus_core::plexus::{AuthContext, SessionValidator};
use serde_json::json;
use tower::Service;

use plexus_transport::websocket::testing::{combined_auth_middleware_with_origins, CombinedAuthMiddleware};

/// Admits the cookie `session=good`; counts every call.
#[derive(Clone, Default)]
struct Validator {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SessionValidator for Validator {
    async fn validate(&self, input: &str) -> Option<AuthContext> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (input.trim() == "session=good").then(|| AuthContext {
            user_id: "alice".into(),
            session_id: "s1".into(),
            roles: vec![],
            metadata: json!({}),
        })
    }
}

/// The inner service: 200, and whether the request reached it.
#[derive(Clone, Default)]
struct Inner {
    reached: Arc<AtomicUsize>,
}

impl Service<http::Request<Empty<Bytes>>> for Inner {
    type Response = http::Response<jsonrpsee::server::HttpBody>;
    type Error = Infallible;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<Empty<Bytes>>) -> Self::Future {
        self.reached.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(http::Response::builder().status(200).body(jsonrpsee::server::HttpBody::from("ok")).unwrap()) })
    }
}

const SHELL: &str = "http://127.0.0.1:4451";

fn middleware(origins: &[&str], v: &Validator, inner: &Inner) -> CombinedAuthMiddleware<Inner> {
    let origins: Vec<String> = origins.iter().map(|s| s.to_string()).collect();
    combined_auth_middleware_with_origins(inner.clone(), Some(Arc::new(v.clone())), true, &origins)
}

async fn upgrade(svc: &mut CombinedAuthMiddleware<Inner>, headers: &[(&str, &str)]) -> (u16, String) {
    let mut b = http::Request::builder().method("GET").uri("/");
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let resp = Service::call(svc, b.body(Empty::<Bytes>::new()).unwrap()).await.expect("call");
    let status = resp.status().as_u16();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn a_page_from_another_port_is_refused_before_its_cookie_is_read() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&[SHELL], &v, &inner);
    let (status, body) = upgrade(&mut svc, &[("origin", "http://127.0.0.1:3000"), ("cookie", "session=good")]).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(v.calls.load(Ordering::SeqCst), 0, "the validator never saw the cookie");
    assert_eq!(inner.reached.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_sandboxed_frame_s_null_origin_is_refused() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&[SHELL], &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("origin", "null"), ("cookie", "session=good")]).await.0, 403);
}

#[tokio::test]
async fn the_listed_origin_proceeds_to_the_validator_in_any_case_and_with_a_slash() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&["HTTP://127.0.0.1:4451/"], &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("origin", SHELL), ("cookie", "session=good")]).await.0, 200);
    assert_eq!(upgrade(&mut svc, &[("origin", SHELL), ("cookie", "session=bad")]).await.0, 401, "then the session decides");
    assert_eq!(inner.reached.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn no_origin_is_a_cli_and_proceeds() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&[SHELL], &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("cookie", "session=good")]).await.0, 200);
}

#[tokio::test]
async fn without_an_allow_list_any_origin_proceeds() {
    let (v, inner) = (Validator::default(), Inner::default());
    let mut svc = middleware(&[], &v, &inner);
    assert_eq!(upgrade(&mut svc, &[("origin", "http://evil.example"), ("cookie", "session=good")]).await.0, 200);
}
