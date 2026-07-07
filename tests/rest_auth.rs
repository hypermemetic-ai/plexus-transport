//! PLX-16 acceptance: the REST/HTTP gateway threads a per-user `AuthContext`
//! into dispatch (4-arg `Activation::call`) instead of dropping it via the
//! legacy 2-arg call, and its error mapper maps auth failures to 401/403.
//!
//! These drive the *exact production wiring* — the REST bridge router plus the
//! `serve_rest_http` middleware stack (api_key gate + session-auth) — via
//! `tower::oneshot`, so a green assertion means auth genuinely reached the
//! handler through the gateway.
//!
//! Run: `cargo test -p plexus-transport --features http-gateway --test rest_auth`
#![cfg(feature = "http-gateway")]

use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use axum::body::Body;
use http::Request;
use jsonrpsee::core::server::Methods;
use plexus_core::plexus::{
    types::{PlexusStreamItem, StreamMetadata},
    Activation, AuthContext, PlexusError, PlexusStream, SessionValidator,
};
use plexus_core::request::RawRequestContext;
use serde_json::Value;
use tower::ServiceExt; // for `oneshot`

use plexus_transport::http::server::testing::build_rest_app;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A hand-rolled test activation with one optional-auth (public-read) method
/// and one force-auth method. Each echoes the identity it *observed* so the
/// assertion can prove exactly what the gateway threaded into `call`.
///
/// `type Methods` reuses `HealthMethod` purely to satisfy the associated-type
/// bound — this crate aliases `schemars` to a no-op shim, so the `#[activation]`
/// macro (which needs the real derive) can't be used here.
struct SecureApi;

fn one_string(namespace: &str, s: String) -> PlexusStream {
    let ns = namespace.to_string();
    Box::pin(stream! {
        yield PlexusStreamItem::Data {
            metadata: StreamMetadata::new(vec![ns.clone().into()], "h".into()),
            content_type: "text".into(),
            content: Value::String(s),
        };
        yield PlexusStreamItem::Done {
            metadata: StreamMetadata::new(vec![ns.into()], "h".into()),
        };
    })
}

#[async_trait]
impl Activation for SecureApi {
    type Methods = plexus_core::activations::health::HealthMethod;

    fn namespace(&self) -> &str {
        "secure"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn methods(&self) -> Vec<&str> {
        vec!["whoami", "admin"]
    }

    async fn call(
        &self,
        method: &str,
        _params: Value,
        auth: Option<&AuthContext>,
        _raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        match method {
            // Optional-auth: observe Some(user_id) or None.
            "whoami" => {
                let label = match auth {
                    Some(ctx) => format!("some:{}", ctx.user_id),
                    None => "none".to_string(),
                };
                Ok(one_string("secure", label))
            }
            // Force-auth: fail closed with Unauthenticated when no identity
            // reached dispatch (mirrors the macro's force-auth wrapper).
            "admin" => match auth {
                Some(ctx) => Ok(one_string("secure", ctx.user_id.clone())),
                None => Err(PlexusError::Unauthenticated(
                    "admin requires authentication".to_string(),
                )),
            },
            other => Err(PlexusError::MethodNotFound {
                activation: "secure".to_string(),
                method: other.to_string(),
            }),
        }
    }

    fn into_rpc_methods(self) -> Methods {
        // REST dispatch does not use the jsonrpsee method registry.
        Methods::new()
    }
}

/// Validator that accepts any credential input containing the token `valid`
/// and mints `alice`. Anything else → `None` (anonymous).
#[derive(Clone, Default)]
struct TestValidator;

#[async_trait]
impl SessionValidator for TestValidator {
    async fn validate(&self, input: &str) -> Option<AuthContext> {
        if input.contains("valid") {
            Some(AuthContext::new(
                "alice".into(),
                "sess-1".into(),
                vec!["user".into()],
                Value::Null,
            ))
        } else {
            None
        }
    }
}

fn app_with_validator() -> axum::Router {
    build_rest_app(
        Arc::new(SecureApi),
        None, // flat_schemas
        None, // route_fn (leaf activation)
        None, // api_key (static admission gate off)
        Some(Arc::new(TestValidator) as Arc<dyn SessionValidator>),
    )
}

async fn post(
    app: axum::Router,
    path: &str,
    cookie: Option<&str>,
) -> (http::StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(c) = cookie {
        builder = builder.header("cookie", c);
    }
    let req = builder.body(Body::from("{}")).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The core PLX-16 claim: a validated cookie identity reaches the handler.
#[tokio::test]
async fn authed_request_threads_identity_to_handler() {
    let (status, body) =
        post(app_with_validator(), "/rest/secure/whoami", Some("access_token=valid")).await;
    assert_eq!(status, http::StatusCode::OK, "authed whoami should be 200; body={body}");
    assert_eq!(
        body["data"][0], "some:alice",
        "PLX-16: the REST gateway must thread the validated per-user AuthContext \
         into Activation::call — handler observed {:?}",
        body["data"]
    );
}

/// No credential → dispatch proceeds anonymously (None), NOT a hard failure,
/// for an optional-auth (public-read) method.
#[tokio::test]
async fn anonymous_request_reaches_public_method_as_none() {
    let (status, body) = post(app_with_validator(), "/rest/secure/whoami", None).await;
    assert_eq!(status, http::StatusCode::OK, "anonymous whoami should be 200; body={body}");
    assert_eq!(
        body["data"][0], "none",
        "public-read method should observe None when unauthenticated; got {:?}",
        body["data"]
    );
}

/// Force-auth method without a credential → `Unauthenticated`, mapped to 401
/// by the extended error mapper (previously an E0004 non-exhaustive match).
#[tokio::test]
async fn force_auth_method_without_credential_maps_to_401() {
    let (status, _body) = post(app_with_validator(), "/rest/secure/admin", None).await;
    assert_eq!(
        status,
        http::StatusCode::UNAUTHORIZED,
        "PLX-16: Unauthenticated must map to HTTP 401 in the REST error mapper",
    );
}

/// Force-auth method WITH a valid credential → 200 and the threaded identity.
#[tokio::test]
async fn force_auth_method_with_credential_succeeds() {
    let (status, body) =
        post(app_with_validator(), "/rest/secure/admin", Some("access_token=valid")).await;
    assert_eq!(status, http::StatusCode::OK, "authed admin should be 200; body={body}");
    assert_eq!(body["data"][0], "alice");
}
