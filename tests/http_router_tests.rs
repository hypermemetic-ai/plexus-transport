//! Router-level integration tests for the REST HTTP bridge (`http-gateway`).
//!
//! These tests spin up the real axum router produced by
//! `ActivationRestBridge::into_router()` and drive it through tower's
//! `oneshot`, covering the full dispatch path that unit tests of
//! `handle_method_call` cannot reach:
//!
//! - unary method → 200 with the documented `{"data": [...]}` JSON structure
//! - streaming method → `text/event-stream`, events delivered promptly while
//!   the stream is still open (not buffered until close)
//! - unknown method/namespace → 404
//! - malformed params → 400
//! - HTTP verb mismatch → 405 (with `Allow` header)
//! - `PlexusError::Unauthenticated` → 401

#![cfg(feature = "http-gateway")]

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use futures::StreamExt;
use jsonrpsee::core::server::Methods;
use plexus_core::plexus::{
    auth::AuthContext,
    schema::{HttpMethod, MethodSchema},
    types::{PlexusStreamItem, StreamMetadata},
    Activation, MethodEnumSchema, PlexusError, PlexusStream, PluginSchema,
};
use plexus_core::request::RawRequestContext;
use serde_json::{json, Value};
use tower::ServiceExt;

use plexus_transport::http::ActivationRestBridge;

// =============================================================================
// Test activation
// =============================================================================

struct TestMethods;

impl plexus_core::__schemars::JsonSchema for TestMethods {
    fn schema_name() -> Cow<'static, str> {
        "TestMethods".into()
    }

    fn json_schema(
        _generator: &mut plexus_core::__schemars::SchemaGenerator,
    ) -> plexus_core::__schemars::Schema {
        plexus_core::__schemars::Schema::try_from(json!({"type": "object"})).unwrap()
    }
}

impl MethodEnumSchema for TestMethods {
    fn method_names() -> &'static [&'static str] {
        &["echo", "watch", "stats", "locked"]
    }

    fn schema_with_consts() -> Value {
        json!({})
    }
}

fn metadata() -> StreamMetadata {
    StreamMetadata::new(vec!["test".into()], "hash".into())
}

/// Leaf activation exposing:
/// - `echo`   (POST, unary): echoes `params.msg`, InvalidParams when missing
/// - `watch`  (POST, streaming): 2 data events promptly, then stays open ~forever
/// - `stats`  (GET, unary): fixed payload
/// - `locked` (POST, unary): always PlexusError::Unauthenticated
struct TestActivation;

#[async_trait]
impl Activation for TestActivation {
    type Methods = TestMethods;

    fn namespace(&self) -> &str {
        "test"
    }

    fn version(&self) -> &str {
        "0.0.1"
    }

    fn methods(&self) -> Vec<&str> {
        vec!["echo", "watch", "stats", "locked"]
    }

    async fn call(
        &self,
        method: &str,
        params: Value,
        _auth: Option<&AuthContext>,
        _raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        match method {
            "echo" => {
                let msg = params
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        PlexusError::InvalidParams("expected string field 'msg'".into())
                    })?;
                Ok(Box::pin(stream! {
                    yield PlexusStreamItem::Data {
                        metadata: metadata().into(),
                        content_type: "test".into(),
                        content: json!({"echo": msg}).into(),
                    };
                    yield PlexusStreamItem::Done { metadata: metadata() };
                }))
            }
            "watch" => Ok(Box::pin(stream! {
                // Two events up front, then the stream stays open for a long
                // time. A buffering (collect-at-close) implementation would
                // never deliver these events to the client.
                yield PlexusStreamItem::Data {
                    metadata: metadata().into(),
                    content_type: "test".into(),
                    content: json!({"n": 1}).into(),
                };
                tokio::time::sleep(Duration::from_millis(50)).await;
                yield PlexusStreamItem::Data {
                    metadata: metadata().into(),
                    content_type: "test".into(),
                    content: json!({"n": 2}).into(),
                };
                tokio::time::sleep(Duration::from_secs(3600)).await;
                yield PlexusStreamItem::Done { metadata: metadata() };
            })),
            "stats" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::Data {
                    metadata: metadata().into(),
                    content_type: "test".into(),
                    content: json!({"count": 42}).into(),
                };
                yield PlexusStreamItem::Done { metadata: metadata() };
            })),
            "locked" => Err(PlexusError::Unauthenticated("token required".into())),
            other => Err(PlexusError::MethodNotFound {
                activation: "test".into(),
                method: other.into(),
            }),
        }
    }

    fn into_rpc_methods(self) -> Methods {
        Methods::new()
    }

    fn plugin_schema(&self) -> PluginSchema {
        PluginSchema::leaf(
            "test",
            "0.0.1",
            "Router test activation",
            vec![
                MethodSchema::new("echo", "Echo a message", "h-echo"),
                MethodSchema::new("watch", "Stream events", "h-watch").with_streaming(true),
                MethodSchema::new("stats", "Read stats", "h-stats")
                    .with_http_method(HttpMethod::Get),
                MethodSchema::new("locked", "Always unauthenticated", "h-locked"),
            ],
        )
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn router() -> axum::Router {
    ActivationRestBridge::new(Arc::new(TestActivation)).into_router()
}

fn json_request(method: &str, uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// =============================================================================
// Unary dispatch
// =============================================================================

#[tokio::test]
async fn unary_method_returns_json_200() {
    let response = router()
        .oneshot(json_request("POST", "/test/echo", r#"{"msg":"hi"}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        content_type.starts_with("application/json"),
        "expected JSON content-type, got {content_type}"
    );

    let body = body_json(response).await;
    let data = body["data"]
        .as_array()
        .expect("documented response structure has a 'data' array");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["echo"], "hi");
}

#[tokio::test]
async fn get_mapped_method_dispatches_on_get() {
    let response = router()
        .oneshot(json_request("GET", "/test/stats", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    assert_eq!(body["data"][0]["count"], 42);
}

// =============================================================================
// Streaming dispatch (SSE)
// =============================================================================

#[tokio::test]
async fn streaming_method_returns_sse_and_delivers_events_promptly() {
    let response = router()
        .oneshot(json_request("POST", "/test/watch", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        content_type.starts_with("text/event-stream"),
        "streaming method must respond with SSE, got {content_type}"
    );

    // The activation stream never completes (sleeps for an hour after the
    // second event). If the bridge buffered events until stream close, this
    // read loop would time out. Receiving both events promptly proves live
    // delivery.
    let mut body_stream = response.into_body().into_data_stream();
    let mut received = String::new();
    let deadline = Duration::from_secs(5);

    while received.matches("event: data").count() < 2 {
        let chunk = tokio::time::timeout(deadline, body_stream.next())
            .await
            .expect("SSE events must be delivered promptly, not buffered at stream close")
            .expect("stream ended before delivering both events")
            .expect("body error");
        received.push_str(std::str::from_utf8(&chunk).unwrap());
    }

    assert!(received.contains(r#"{"content":{"n":1}}"#), "raw SSE: {received}");
    assert!(received.contains(r#"{"content":{"n":2}}"#), "raw SSE: {received}");
}

// =============================================================================
// Error paths
// =============================================================================

#[tokio::test]
async fn unknown_method_returns_404() {
    let response = router()
        .oneshot(json_request("POST", "/test/nope", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_json(response).await;
    assert!(
        body["error"].as_str().unwrap().contains("Method not found"),
        "body: {body}"
    );
}

#[tokio::test]
async fn unknown_namespace_returns_404() {
    let response = router()
        .oneshot(json_request("POST", "/ghosts/echo", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn malformed_json_body_returns_400() {
    let response = router()
        .oneshot(json_request("POST", "/test/echo", "{not valid json"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn invalid_params_returns_400() {
    // Well-formed JSON that the activation rejects (msg is not a string)
    let response = router()
        .oneshot(json_request("POST", "/test/echo", r#"{"msg": 42}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_json(response).await;
    assert!(
        body["error"].as_str().unwrap().contains("Invalid parameters"),
        "body: {body}"
    );
}

#[tokio::test]
async fn verb_mismatch_returns_405_with_allow_header() {
    // echo is POST-mapped; call it with GET (no body needed — the verb check
    // runs before body extraction)
    let response = router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test/echo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        response.headers().get(header::ALLOW).unwrap(),
        "POST",
        "405 must advertise the allowed verb"
    );

    // stats is GET-mapped; POST must be rejected the same way
    let response = router()
        .oneshot(json_request("POST", "/test/stats", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers().get(header::ALLOW).unwrap(), "GET");
}

#[tokio::test]
async fn unauthenticated_error_maps_to_401() {
    let response = router()
        .oneshot(json_request("POST", "/test/locked", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = body_json(response).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("Authentication required"),
        "body: {body}"
    );
}
