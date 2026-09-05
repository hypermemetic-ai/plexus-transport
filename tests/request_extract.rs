//! REQ-1 acceptance tests: PlexusRequest derive — extraction logic.
//!
//! These tests will compile-fail until `PlexusRequest` derive and `RawRequestContext`
//! are implemented in plexus-transport. Run with:
//!
//!   cargo test --test request_extract
//!
//! All tests must pass for REQ-1 to be considered complete.

use plexus_transport::request::{PlexusRequest, RawRequestContext};
use plexus_core::plexus::AuthContext;
use schemars::PlexusRequest;
use std::net::SocketAddr;

/// Helper: build a RawRequestContext from header pairs and optional peer addr.
fn make_raw(headers: &[(&str, &str)], peer: Option<&str>) -> RawRequestContext {
    let mut h = http::HeaderMap::new();
    for &(k, v) in headers {
        h.insert(
            http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    RawRequestContext {
        headers: h,
        uri: "/".parse().unwrap(),
        auth: None,
        peer: peer.map(|p| p.parse().unwrap()),
    }
}

// ---------------------------------------------------------------------------
// Struct used across multiple tests
// ---------------------------------------------------------------------------

#[derive(PlexusRequest, schemars::JsonSchema)]
struct TestRequest {
    /// JWT from Keycloak
    #[from_cookie("access_token")]
    auth_token: String,

    /// Request origin header
    #[from_header("origin")]
    origin: Option<String>,

    /// Caller's IP
    #[from_peer]
    peer_addr: Option<SocketAddr>,
}

// ---------------------------------------------------------------------------
// Required field (cookie)
// ---------------------------------------------------------------------------

#[test]
fn required_cookie_present() {
    let ctx = make_raw(&[("cookie", "access_token=jwt123")], None);
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.auth_token, "jwt123");
}

#[test]
fn required_cookie_absent_is_err() {
    let ctx = make_raw(&[], None);
    assert!(
        TestRequest::extract(&ctx).is_err(),
        "missing required cookie must return Err"
    );
}

#[test]
fn cookie_header_with_multiple_values() {
    let ctx = make_raw(
        &[("cookie", "session=abc; access_token=tok; other=xyz")],
        None,
    );
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.auth_token, "tok");
}

// ---------------------------------------------------------------------------
// Optional field (header)
// ---------------------------------------------------------------------------

#[test]
fn optional_header_present() {
    let ctx = make_raw(
        &[
            ("cookie", "access_token=x"),
            ("origin", "https://app.example.com"),
        ],
        None,
    );
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.origin.as_deref(), Some("https://app.example.com"));
}

#[test]
fn optional_header_absent() {
    let ctx = make_raw(&[("cookie", "access_token=x")], None);
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.origin, None);
}

// ---------------------------------------------------------------------------
// Derived field (peer)
// ---------------------------------------------------------------------------

#[test]
fn peer_addr_present() {
    let ctx = make_raw(&[("cookie", "access_token=x")], Some("1.2.3.4:5678"));
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.peer_addr, Some("1.2.3.4:5678".parse::<SocketAddr>().unwrap()));
}

#[test]
fn peer_addr_absent() {
    let ctx = make_raw(&[("cookie", "access_token=x")], None);
    let req = TestRequest::extract(&ctx).unwrap();
    assert_eq!(req.peer_addr, None);
}

// ---------------------------------------------------------------------------
// Auth context forwarding
// ---------------------------------------------------------------------------

#[derive(PlexusRequest, schemars::JsonSchema)]
struct AuthedRequest {
    #[from_cookie("access_token")]
    auth_token: String,
    #[from_auth_context]
    auth: Option<AuthContext>,
}

#[test]
fn auth_context_carried_through() {
    let mut ctx = make_raw(&[("cookie", "access_token=x")], None);
    ctx.auth = Some(AuthContext {
        user_id: "u1".into(),
        session_id: "s1".into(),
        roles: vec![],
        metadata: Default::default(),
    });
    let req = AuthedRequest::extract(&ctx).unwrap();
    assert_eq!(req.auth.as_ref().unwrap().user_id, "u1");
}

#[test]
fn auth_context_none_when_unauthenticated() {
    let ctx = make_raw(&[("cookie", "access_token=x")], None);
    let req = AuthedRequest::extract(&ctx).unwrap();
    assert!(req.auth.is_none());
}
