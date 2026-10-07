//! Router-level integration tests for the REST HTTP bridge (`http-gateway`).
//!
//! These tests spin up the real axum router produced by
//! `ActivationRestBridge::into_router()` and drive it through tower's
//! `oneshot`, covering the full dispatch path that unit tests of
//! `handle_method_call` cannot reach:
//!
//! - cardinality-one method → 200 with the documented `{"data": [...]}` JSON
//!   structure
//! - cardinality-many method → `text/event-stream`, events delivered promptly
//!   while the stream is still open (not buffered until close)
//! - unknown method/namespace → 404
//! - malformed params → 400
//! - HTTP verb mismatch → 405 (with `Allow` header)
//! - `PlexusError::Unauthenticated` → 401
//!
//! RETURNS-11 (plexus 0.7 frames): every `RemoteError` kind → its status, the
//! typed error in the body, whether the call is rejected before its stream
//! starts or fails on an error frame; typed progress in the unary body; an
//! `{"err": …}` item is data and stays 200; SSE `progress` / `error` events
//! byte-compared against the documented JSON.

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
    Activation, AuthzDenyReason, Cardinality, MethodEnumSchema, ParamProblem, PlexusError,
    PlexusStream, PluginSchema, RemoteError,
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
        &[
            "echo", "watch", "stats", "locked", "reject", "fail", "progress", "refuse", "import",
        ]
    }

    fn schema_with_consts() -> Value {
        json!({})
    }
}

fn metadata() -> StreamMetadata {
    StreamMetadata::new(vec!["test".into()], "hash".into())
}

/// The `PlexusError` the `reject` / `fail` fixture methods provoke, by the
/// `RemoteError` kind it maps to — one per row of RETURNS-11's status table.
fn provoke(kind: &str) -> PlexusError {
    match kind {
        "activation_not_found" => PlexusError::ActivationNotFound("ghosts".into()),
        "method_not_found" => PlexusError::MethodNotFound {
            activation: "test".into(),
            method: "frob".into(),
        },
        "invalid_params" => PlexusError::InvalidParams {
            param: Some("id".into()),
            problem: ParamProblem::Missing,
        },
        "unauthenticated" => PlexusError::Unauthenticated("token required".into()),
        "forbidden" => PlexusError::Forbidden {
            reason: AuthzDenyReason::TenantBoundary,
        },
        "handle_not_supported" => PlexusError::HandleNotSupported("test".into()),
        "execution" => PlexusError::ExecutionError("disk full".into()),
        other => panic!("fixture has no kind {other}"),
    }
}

fn kind_param(params: &Value) -> String {
    params["kind"].as_str().expect("fixture needs a 'kind'").to_owned()
}

/// Leaf activation exposing:
/// - `echo`     (POST, one): echoes `params.msg`, InvalidParams when missing
/// - `watch`    (POST, many): 2 data events promptly, then stays open ~forever
/// - `stats`    (GET, one): fixed payload
/// - `locked`   (POST, one): always PlexusError::Unauthenticated
/// - `reject`   (POST, one): rejected before its stream starts with the
///   `PlexusError` for `params.kind`
/// - `fail`     (POST, one): one data item, then an error frame carrying the
///   `RemoteError` for `params.kind`
/// - `progress` (POST, one): a `Note` progress frame, a typed progress frame,
///   then an `{"ok": …}` item
/// - `refuse`   (POST, one): an `{"err": …}` item — the activation said no
/// - `import`   (POST, many): a progress frame, a data item, then a
///   non-recoverable error frame; rejected up front when `params.reject`
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
        vec![
            "echo", "watch", "stats", "locked", "reject", "fail", "progress", "refuse", "import",
        ]
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
                    .ok_or_else(|| PlexusError::InvalidParams {
                        param: Some("msg".into()),
                        problem: ParamProblem::Malformed {
                            detail: "expected a string".into(),
                        },
                    })?;
                Ok(Box::pin(stream! {
                    yield PlexusStreamItem::Data {
                        metadata: metadata(),
                        content_type: "test".into(),
                        content: json!({"echo": msg}),
                    };
                    yield PlexusStreamItem::Done { metadata: metadata() };
                }))
            }
            "watch" => Ok(Box::pin(stream! {
                // Two events up front, then the stream stays open for a long
                // time. A buffering (collect-at-close) implementation would
                // never deliver these events to the client.
                yield PlexusStreamItem::Data {
                    metadata: metadata(),
                    content_type: "test".into(),
                    content: json!({"n": 1}),
                };
                tokio::time::sleep(Duration::from_millis(50)).await;
                yield PlexusStreamItem::Data {
                    metadata: metadata(),
                    content_type: "test".into(),
                    content: json!({"n": 2}),
                };
                tokio::time::sleep(Duration::from_secs(3600)).await;
                yield PlexusStreamItem::Done { metadata: metadata() };
            })),
            "stats" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::Data {
                    metadata: metadata(),
                    content_type: "test".into(),
                    content: json!({"count": 42}),
                };
                yield PlexusStreamItem::Done { metadata: metadata() };
            })),
            "locked" => Err(PlexusError::Unauthenticated("token required".into())),
            "reject" => Err(provoke(&kind_param(&params))),
            "fail" => {
                let error = RemoteError::from(&provoke(&kind_param(&params)));
                Ok(Box::pin(stream! {
                    yield PlexusStreamItem::data(metadata(), "test.fail".into(), json!({"n": 1}));
                    yield PlexusStreamItem::error(metadata(), error, false);
                }))
            }
            "progress" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::progress(
                    metadata(),
                    "test.progress".into(),
                    json!({"message": "halfway", "percentage": 50.0}),
                );
                yield PlexusStreamItem::progress(
                    metadata(),
                    "test.progress".into(),
                    json!({"type": "chunk", "done": 1, "of": 2}),
                );
                yield PlexusStreamItem::data(
                    metadata(),
                    "test.progress".into(),
                    json!({"ok": {"loaded": 2}}),
                );
                yield PlexusStreamItem::done(metadata());
            })),
            "refuse" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::data(
                    metadata(),
                    "test.refuse".into(),
                    json!({"err": {"type": "not_found", "id": "MNG-999"}}),
                );
                yield PlexusStreamItem::done(metadata());
            })),
            "import" => {
                if params.get("reject").is_some() {
                    return Err(provoke("invalid_params"));
                }
                Ok(Box::pin(stream! {
                    yield PlexusStreamItem::progress(
                        metadata(),
                        "test.import".into(),
                        json!({"message": "halfway", "percentage": 50.0}),
                    );
                    yield PlexusStreamItem::data(
                        metadata(),
                        "test.import".into(),
                        json!({"ok": {"row": 1}}),
                    );
                    yield PlexusStreamItem::error(
                        metadata(),
                        RemoteError::Execution { message: "disk full".into() },
                        false,
                    );
                }))
            }
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
                MethodSchema::new("watch", "Stream events", "h-watch")
                    .with_cardinality(Cardinality::Many),
                MethodSchema::new("stats", "Read stats", "h-stats")
                    .with_http_method(HttpMethod::Get),
                MethodSchema::new("locked", "Always unauthenticated", "h-locked"),
                MethodSchema::new("reject", "Rejected up front", "h-reject"),
                MethodSchema::new("fail", "Fails on an error frame", "h-fail"),
                MethodSchema::new("progress", "Reports progress", "h-progress"),
                MethodSchema::new("refuse", "Says no", "h-refuse"),
                MethodSchema::new("import", "Streams with progress", "h-import")
                    .with_cardinality(Cardinality::Many),
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

fn content_type(response: &axum::response::Response) -> String {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned()
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
async fn cardinality_many_method_returns_sse_and_delivers_events_promptly() {
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
        "cardinality-many method must respond with SSE, got {content_type}"
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
    assert_eq!(
        body["error"],
        json!({"kind": "method_not_found", "activation": "test", "method": "nope"}),
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
    let body = body_json(response).await;
    assert_eq!(
        body["error"],
        json!({"kind": "activation_not_found", "activation": "ghosts"}),
        "body: {body}"
    );
}

#[tokio::test]
async fn malformed_json_body_returns_400() {
    let response = router()
        .oneshot(json_request("POST", "/test/echo", "{not valid json"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_json(response).await;
    assert_eq!(body["error"]["kind"], "invalid_params", "body: {body}");
    assert_eq!(body["error"]["problem"], "malformed", "body: {body}");
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
    assert_eq!(
        body["error"],
        json!({
            "kind": "invalid_params",
            "param": "msg",
            "problem": "malformed",
            "detail": "expected a string",
        }),
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
    assert_eq!(body["error"], json!({"kind": "unauthenticated"}), "body: {body}");
}

// =============================================================================
// RETURNS-11: RemoteError kind → HTTP status (one test per row)
// =============================================================================

/// Provoke `kind` both ways a call fails and assert the status and the typed
/// error each time:
/// - `reject`: the call is refused before its stream starts →
///   `{"error": <RemoteError>}`;
/// - `fail`: the stream yields one item, then an error frame →
///   `{"data": [<item>], "error": <RemoteError>}`.
async fn assert_kind_maps_to(kind: &str, status: StatusCode) {
    let body = format!(r#"{{"kind":"{kind}"}}"#);
    let expected = serde_json::to_value(RemoteError::from(&provoke(kind))).unwrap();

    let response = router()
        .oneshot(json_request("POST", "/test/reject", &body))
        .await
        .unwrap();
    assert_eq!(response.status(), status, "reject {kind}");
    assert!(content_type(&response).starts_with("application/json"));
    let json = body_json(response).await;
    assert_eq!(json["error"]["kind"], kind, "reject {kind}: {json}");
    assert_eq!(json["error"], expected, "reject {kind}: {json}");

    let response = router()
        .oneshot(json_request("POST", "/test/fail", &body))
        .await
        .unwrap();
    assert_eq!(response.status(), status, "fail {kind}");
    let json = body_json(response).await;
    assert_eq!(json["error"]["kind"], kind, "fail {kind}: {json}");
    assert_eq!(json["error"], expected, "fail {kind}: {json}");
    assert_eq!(json["data"], json!([{"n": 1}]), "fail {kind}: {json}");
}

#[tokio::test]
async fn status_activation_not_found_and_method_not_found_are_404() {
    assert_kind_maps_to("activation_not_found", StatusCode::NOT_FOUND).await;
    assert_kind_maps_to("method_not_found", StatusCode::NOT_FOUND).await;
}

#[tokio::test]
async fn status_invalid_params_is_400() {
    assert_kind_maps_to("invalid_params", StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn status_unauthenticated_is_401() {
    assert_kind_maps_to("unauthenticated", StatusCode::UNAUTHORIZED).await;
}

#[tokio::test]
async fn status_forbidden_is_403() {
    assert_kind_maps_to("forbidden", StatusCode::FORBIDDEN).await;
}

#[tokio::test]
async fn status_handle_not_supported_is_501() {
    assert_kind_maps_to("handle_not_supported", StatusCode::NOT_IMPLEMENTED).await;
}

#[tokio::test]
async fn status_execution_is_500() {
    assert_kind_maps_to("execution", StatusCode::INTERNAL_SERVER_ERROR).await;
}

// =============================================================================
// RETURNS-11: unary bodies carry typed progress; an err item is data
// =============================================================================

#[tokio::test]
async fn unary_body_carries_typed_progress_whole() {
    let response = router()
        .oneshot(json_request("POST", "/test/progress", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    assert_eq!(
        body,
        json!({
            "data": [{"ok": {"loaded": 2}}],
            "progress": [
                {"message": "halfway", "percentage": 50.0},
                {"type": "chunk", "done": 1, "of": 2},
            ],
        })
    );
}

#[tokio::test]
async fn err_item_is_data_and_stays_200() {
    let response = router()
        .oneshot(json_request("POST", "/test/refuse", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    assert_eq!(
        body,
        json!({"data": [{"err": {"type": "not_found", "id": "MNG-999"}}]}),
        "an err item is data, not a failed call"
    );
}

// =============================================================================
// RETURNS-11: SSE progress / error events, byte-compared
// =============================================================================

#[tokio::test]
async fn sse_carries_typed_progress_and_error_events() {
    let response = router()
        .oneshot(json_request("POST", "/test/import", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(content_type(&response).starts_with("text/event-stream"));

    // The stream ends after the error frame, so the body completes.
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("SSE body completes when the stream ends")
    .unwrap();
    let sse = std::str::from_utf8(&bytes).unwrap();

    assert_eq!(
        sse,
        concat!(
            "event: progress\n",
            r#"data: {"content_type":"test.import","content":{"message":"halfway","percentage":50.0}}"#,
            "\n\n",
            "event: data\n",
            r#"data: {"content":{"ok":{"row":1}}}"#,
            "\n\n",
            "event: error\n",
            r#"data: {"recoverable":false,"error":{"kind":"execution","message":"disk full"}}"#,
            "\n\n",
        )
    );
}

#[tokio::test]
async fn cardinality_many_rejected_up_front_answers_json_with_status() {
    let response = router()
        .oneshot(json_request("POST", "/test/import", r#"{"reject":true}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(content_type(&response).starts_with("application/json"));
    let body = body_json(response).await;
    assert_eq!(
        body,
        json!({"error": {"kind": "invalid_params", "param": "id", "problem": "missing"}})
    );
}

#[tokio::test]
async fn cardinality_one_method_answers_one_json_body() {
    // Progress frames and a data item, as `import` sends them, but on a
    // cardinality-one method: one JSON body, not SSE.
    let response = router()
        .oneshot(json_request("POST", "/test/progress", "{}"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        content_type(&response).starts_with("application/json"),
        "cardinality-one method must answer one JSON body"
    );
}
