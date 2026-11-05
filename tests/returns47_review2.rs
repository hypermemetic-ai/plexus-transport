//! RETURNS-47 · the re-review's REST cases.
//!
//! - **The api key and the session validator together.** A `Bearer` that
//!   matched the api key is the admission key, not a session token: with no
//!   cookie the caller is anonymous (as every REST call was before
//!   RETURNS-47), with a valid cookie the cookie's caller; a session token
//!   sent as the `Bearer` with no api key is refused by the key.
//! - **CSRF pinned.** A cookie-carrying POST that is not
//!   `application/json` (a form, `text/plain` — what a page on another
//!   origin may send without a preflight) is refused 415 and runs nothing;
//!   the REST router answers a CORS preflight with no CORS headers.
#![cfg(feature = "http-gateway")]

use std::sync::atomic::Ordering;
use std::sync::Arc;

use plexus_transport::config::RestHttpConfig;
use plexus_transport::http::server::serve_rest_http_as;
use plexus_transport::ActivationRestBridge;
use returns47_fixture::{ProbeActivation, Users, ADA, TOUCHED};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn post(port: u16, path: &str, auth: Option<&str>, cookie: Option<&str>) -> (u16, String) {
    let mut r = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}{path}"))
        .header("content-type", "application/json")
        .body("{}");
    if let Some(a) = auth {
        r = r.header("authorization", a);
    }
    if let Some(k) = cookie {
        r = r.header("cookie", k);
    }
    let resp = r.send().await.unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// A REST server behind the api key `K`, its callers named by `Users`.
async fn served() -> (u16, tokio::task::JoinHandle<std::io::Result<()>>) {
    let port = free_port();
    let cfg = RestHttpConfig::new(port).with_allowed_hosts([format!("127.0.0.1:{port}")]);
    let h = serve_rest_http_as(Arc::new(ProbeActivation::default()), None, None, None, Some(Arc::new(Users)), cfg, Some("K".into()))
        .await
        .unwrap();
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    (port, h)
}

#[tokio::test]
async fn the_api_key_alone_is_the_anonymous_caller() {
    let (port, h) = served().await;
    let (status, body) = post(port, "/rest/probe/open", Some("Bearer K"), None).await;
    assert_eq!(status, 200, "a public method, the api key alone: {body}");
    assert!(body.contains("anyone *"), "{body}");
    let (status, body) = post(port, "/rest/probe/touch", Some("Bearer K"), None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("anonymous *"), "the key names no one: {body}");
    let (status, body) = post(port, "/rest/probe/reach", Some("Bearer K"), None).await;
    assert_eq!(status, 401, "a method that needs a caller, the api key alone: {body}");
    h.abort();
}

#[tokio::test]
async fn the_api_key_and_a_cookie_is_the_cookies_caller() {
    let (port, h) = served().await;
    let (status, body) = post(port, "/rest/probe/reach", Some("Bearer K"), Some("ada")).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(&format!("{ADA} *")), "{body}");
    h.abort();
}

#[tokio::test]
async fn a_session_bearer_without_the_api_key_is_refused_by_the_key() {
    let (port, h) = served().await;
    let (status, body) = post(port, "/rest/probe/open", Some("Bearer ada"), None).await;
    assert_eq!(status, 401, "{body}");
    h.abort();
}

// ------------------------------------------------------------------- CSRF

mod csrf {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use tower::ServiceExt;

    fn router() -> axum::Router {
        ActivationRestBridge::new(Arc::new(ProbeActivation::default())).with_session_validator(Arc::new(Users)).into_router()
    }

    #[tokio::test]
    async fn a_cookie_carrying_post_that_is_not_json_is_refused_and_runs_nothing() {
        let before = TOUCHED.load(Ordering::SeqCst);
        for (content_type, body) in [("text/plain", "{}"), ("application/x-www-form-urlencoded", "a=1"), ("multipart/form-data; boundary=x", "--x--")] {
            let request = Request::builder()
                .method("POST")
                .uri("/probe/touch")
                .header(header::CONTENT_TYPE, content_type)
                .header(header::COOKIE, "ada")
                .header(header::ORIGIN, "https://evil.example")
                .body(Body::from(body))
                .unwrap();
            let response = router().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE, "{content_type}");
        }
        assert_eq!(TOUCHED.load(Ordering::SeqCst), before, "nothing ran");
    }

    #[tokio::test]
    async fn a_cors_preflight_gets_no_cors_headers() {
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/probe/touch")
            .header(header::ORIGIN, "https://evil.example")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
            .body(Body::empty())
            .unwrap();
        let response = router().oneshot(request).await.unwrap();
        assert!(!response.status().is_success(), "{}", response.status());
        let cors: Vec<_> = response.headers().keys().filter(|k| k.as_str().starts_with("access-control-")).collect();
        assert!(cors.is_empty(), "no CORS headers: {cors:?}");
    }
}
