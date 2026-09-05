//! REQ-3 acceptance tests: ValidOrigin extraction and allowlist.
//!
//! Run with: cargo test --test origin
//!
//! The `uninitialised_allows_all` test lives in `origin_uninitialised.rs`
//! (separate binary = fresh OnceLock state) so it doesn't race with
//! the `configured::*` tests here that call `init_allowed_origins`.

use plexus_transport::request::{PlexusRequest, RawRequestContext};
use plexus_transport::request::origin::{ValidOrigin, init_allowed_origins};
use plexus_core::plexus::{PlexusError, PlexusRequestField};
use schemars::PlexusRequest;

fn make_raw(headers: &[(&str, &str)]) -> RawRequestContext {
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
        peer: None,
    }
}

// ---------------------------------------------------------------------------
// ValidOrigin::extract_from_raw unit tests
// ---------------------------------------------------------------------------

#[test]
fn no_origin_passes_always() {
    // Non-browser clients (synapse, CLI) send no Origin header — must always pass
    let ctx = make_raw(&[]);
    let result = ValidOrigin::extract_from_raw(&ctx).unwrap();
    assert_eq!(result.0, "", "absent origin yields empty-string sentinel");
}

// Tests below require the allowlist to be initialised.
// Because OnceLock only sets once, we use a separate test module
// with its own binary to avoid state bleed.
//
// Run: cargo test --test origin -- configured
mod configured {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            init_allowed_origins(vec![
                "https://app.example.com".into(),
                "http://localhost:5173".into(),
            ]);
        });
    }

    #[test]
    fn allowed_origin_passes() {
        setup();
        let ctx = make_raw(&[("origin", "https://app.example.com")]);
        assert!(ValidOrigin::extract_from_raw(&ctx).is_ok());
    }

    #[test]
    fn second_allowed_origin_passes() {
        setup();
        let ctx = make_raw(&[("origin", "http://localhost:5173")]);
        assert!(ValidOrigin::extract_from_raw(&ctx).is_ok());
    }

    #[test]
    fn disallowed_origin_is_err() {
        setup();
        let ctx = make_raw(&[("origin", "https://evil.com")]);
        let err = ValidOrigin::extract_from_raw(&ctx).unwrap_err();
        assert!(
            matches!(err, PlexusError::Unauthenticated(_)),
            "disallowed origin must be Unauthenticated error"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("evil.com"),
            "error message must name the disallowed origin: {msg}"
        );
    }

    #[test]
    fn disallowed_origin_in_struct_extraction_fails() {
        setup();

        #[derive(PlexusRequest, schemars::JsonSchema)]
        struct TestReq {
            #[from_cookie("access_token")]
            auth_token: String,
            origin: ValidOrigin,
        }

        let ctx_bad = make_raw(&[
            ("cookie", "access_token=tok"),
            ("origin", "https://evil.com"),
        ]);
        // Struct extraction must propagate the ValidOrigin error
        let ctx_bad = {
            let mut raw = ctx_bad;
            raw.peer = None;
            raw
        };
        // Need cookie too for the raw context
        let mut h = http::HeaderMap::new();
        h.insert(http::header::COOKIE, "access_token=tok".parse().unwrap());
        h.insert(
            http::header::HeaderName::from_static("origin"),
            "https://evil.com".parse().unwrap(),
        );
        let bad_ctx = RawRequestContext { headers: h, uri: "/".parse().unwrap(), auth: None, peer: None };
        assert!(TestReq::extract(&bad_ctx).is_err());
    }

    #[test]
    fn no_origin_passes_even_with_allowlist() {
        setup();
        let ctx = make_raw(&[("cookie", "access_token=tok")]);
        // Build a ctx with just the cookie, no origin
        let mut h = http::HeaderMap::new();
        h.insert(http::header::COOKIE, "access_token=tok".parse().unwrap());
        let no_origin_ctx = RawRequestContext { headers: h, uri: "/".parse().unwrap(), auth: None, peer: None };

        #[derive(PlexusRequest, schemars::JsonSchema)]
        struct TestReq2 {
            #[from_cookie("access_token")]
            auth_token: String,
            origin: ValidOrigin,
        }

        let req = TestReq2::extract(&no_origin_ctx).unwrap();
        assert_eq!(req.origin.0, "");
    }
}
