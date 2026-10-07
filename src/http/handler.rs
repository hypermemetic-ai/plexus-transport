//! Request/response handlers for REST HTTP transport
//!
//! Handles PlexusStream responses, converting them to either:
//! - one JSON body for a method of cardinality `one`
//! - Server-Sent Events (SSE) for a method of cardinality `many`
//!
//! ## Frames (plexus 0.7, RETURNS-11)
//!
//! A transport never inspects `content`; it forwards `content` and `error`
//! whole. The only interpretation it does is the status mapping
//! ([`remote_error_status`]). An `{"ok": …}` / `{"err": …}` item is data:
//! it travels like any other item and a unary response stays `200`.
//!
//! | frame | unary JSON body | SSE event |
//! | -- | -- | -- |
//! | data | `"data": [<content>…]` | `event: data`, `{"content": <content>}` |
//! | progress | `"progress": [<P>…]` | `event: progress`, `{"content_type": …, "content": <P>}` |
//! | error | status per kind; `"error": <RemoteError>` beside `data` | `event: error`, `{"recoverable": …, "error": <RemoteError>}` |
//! | done | ends the body | `event: done`, `{}` |
//!
//! A call rejected before its stream starts (unknown method, bad params,
//! auth) is answered `{"error": <RemoteError>}` with the same status mapping,
//! whatever the method's cardinality.
//!
//! Answers that no `RemoteError` kind describes — they come from this
//! transport, not the call — keep a human-readable string under `"error"`:
//! 405 (wrong verb), 413 (the unary buffer limits), 504 (the method timed
//! out). A request without a JSON content-type gets axum's own 415.
//!
//! ## Protection Mechanisms
//!
//! - **Timeouts**: Methods timeout after 5 minutes to prevent infinite streams
//! - **Buffer Limits**: Unary (cardinality `one`) methods enforce max item count and byte limits
//! - **Memory Safety**: Prevents unbounded buffering and OOM crashes

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response, sse::{Event, Sse}},
    Json,
};
use futures::StreamExt;
use plexus_core::plexus::{types::PlexusStreamItem, Cardinality, PlexusStream, RemoteError};
use serde::Serialize;
use serde_json::{json, Value};
use std::time::Duration;

// =============================================================================
// Configuration Constants
// =============================================================================

/// Maximum time to wait for a method to complete (5 minutes)
const METHOD_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum number of items to buffer for a unary (cardinality `one`) method
const MAX_BUFFERED_ITEMS: usize = 10_000;

/// Maximum total bytes to buffer for a unary (cardinality `one`) method (100MB)
const MAX_BUFFER_BYTES: usize = 100 * 1024 * 1024;

// =============================================================================
// Method Info
// =============================================================================

/// Metadata about a method for response handling
#[derive(Clone, Debug)]
pub struct MethodInfo {
    pub namespace: String,
    pub method: String,
    /// The schema's cardinality (RETURNS-5): `one` → one JSON body,
    /// `many` → SSE.
    pub cardinality: Cardinality,
}

// =============================================================================
// Error Mapping
// =============================================================================

/// The HTTP status a [`RemoteError`] is answered with, so curl users and
/// proxies see the failure class without parsing the body.
///
/// Exhaustive on purpose: a new `RemoteError` kind fails this build until it
/// has a status.
pub fn remote_error_status(error: &RemoteError) -> StatusCode {
    match error {
        RemoteError::ActivationNotFound { .. } | RemoteError::MethodNotFound { .. } => {
            StatusCode::NOT_FOUND
        }
        RemoteError::InvalidParams { .. } => StatusCode::BAD_REQUEST,
        RemoteError::Unauthenticated => StatusCode::UNAUTHORIZED,
        RemoteError::Forbidden { .. } => StatusCode::FORBIDDEN,
        RemoteError::HandleNotSupported { .. } => StatusCode::NOT_IMPLEMENTED,
        RemoteError::Execution { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Answer a call rejected before its stream started:
/// [`remote_error_status`] and `{"error": <RemoteError>}`.
pub fn remote_error_response(error: RemoteError) -> Response {
    (remote_error_status(&error), Json(json!({ "error": error }))).into_response()
}

// =============================================================================
// Response Handling
// =============================================================================

/// Handle a method call, routing to JSON or SSE response by the method's
/// cardinality
///
/// This function applies timeout protection to prevent infinite streams from hanging forever.
pub async fn handle_method_call(stream: PlexusStream, method_info: MethodInfo) -> Response {
    // Apply timeout to prevent infinite streams
    let result = match method_info.cardinality {
        Cardinality::Many => {
            tokio::time::timeout(METHOD_TIMEOUT, stream_sse_response(stream)).await
        }
        Cardinality::One => {
            tokio::time::timeout(METHOD_TIMEOUT, collect_and_respond(stream)).await
        }
    };

    match result {
        Ok(response) => response,
        Err(_elapsed) => {
            tracing::error!(
                "Method {}.{} timed out after {:?}",
                method_info.namespace,
                method_info.method,
                METHOD_TIMEOUT
            );
            (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "error": format!(
                        "Method execution timed out after {} seconds",
                        METHOD_TIMEOUT.as_secs()
                    ),
                    "timeout_seconds": METHOD_TIMEOUT.as_secs(),
                })),
            )
                .into_response()
        }
    }
}

/// Collect all stream items and return structured JSON response
///
/// This function enforces buffer limits to prevent memory exhaustion:
/// - Maximum item count: 10,000 items
/// - Maximum total bytes: 100MB
async fn collect_and_respond(mut stream: PlexusStream) -> Response {
    let mut data_items = Vec::new();
    let mut progress_items: Vec<Value> = Vec::new();
    let mut error: Option<RemoteError> = None;
    let mut total_bytes: usize = 0;

    while let Some(item) = stream.next().await {
        match item {
            PlexusStreamItem::Data { content, .. } => {
                // Check item count limit
                if data_items.len() >= MAX_BUFFERED_ITEMS {
                    tracing::error!(
                        "Stream exceeded max buffered items: {} items",
                        MAX_BUFFERED_ITEMS
                    );
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(json!({
                            "error": format!(
                                "Stream exceeded maximum item count of {}",
                                MAX_BUFFERED_ITEMS
                            ),
                            "max_items": MAX_BUFFERED_ITEMS,
                        })),
                    )
                        .into_response();
                }

                // Check total byte limit
                let item_bytes = serde_json::to_vec(&content).unwrap_or_default().len();
                total_bytes += item_bytes;

                if total_bytes > MAX_BUFFER_BYTES {
                    tracing::error!(
                        "Stream exceeded max buffer size: {} bytes (limit: {} bytes)",
                        total_bytes,
                        MAX_BUFFER_BYTES
                    );
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(json!({
                            "error": format!(
                                "Stream exceeded maximum buffer size of {} bytes",
                                MAX_BUFFER_BYTES
                            ),
                            "max_bytes": MAX_BUFFER_BYTES,
                            "bytes_buffered": total_bytes,
                        })),
                    )
                        .into_response();
                }

                data_items.push(content);
            }
            PlexusStreamItem::Progress { content, .. } => {
                // The method's progress value `P`, forwarded whole.
                progress_items.push(content);
            }
            PlexusStreamItem::Error { error: e, .. } => {
                error = Some(e);
                break;
            }
            PlexusStreamItem::Done { .. } => {
                break;
            }
            PlexusStreamItem::Request { .. } => {
                // Bidirectional requests not supported in REST HTTP
                // This would require WebSocket or long-polling
                tracing::warn!("Ignoring bidirectional request in REST HTTP transport");
            }
        }
    }

    // Build response
    let mut response_obj = json!({
        "data": data_items,
    });

    if !progress_items.is_empty() {
        response_obj["progress"] = json!(progress_items);
    }

    if let Some(error) = error {
        let status = remote_error_status(&error);
        response_obj["error"] = json!(error);
        return (status, Json(response_obj)).into_response();
    }

    (StatusCode::OK, Json(response_obj)).into_response()
}

/// An SSE `progress` event's data. A struct (not `json!`) so the key order
/// is the documented one: `{"content_type": …, "content": <P>}`.
#[derive(Serialize)]
struct SseProgress<'a> {
    content_type: &'a str,
    content: &'a Value,
}

/// An SSE `error` event's data: `{"recoverable": …, "error": <RemoteError>}`.
#[derive(Serialize)]
struct SseError<'a> {
    recoverable: bool,
    error: &'a RemoteError,
}

/// An SSE `error` event. Every `event: error` the bridge sends has this
/// shape, so a client decodes it as one type.
fn sse_error_event(recoverable: bool, error: &RemoteError) -> Event {
    Event::default()
        .event("error")
        .json_data(SseError { recoverable, error })
        // Unreachable (a `RemoteError` always serializes); last resort only.
        .unwrap_or_else(|_| Event::default().event("error").data(error.to_string()))
}

/// A frame whose JSON could not be written: a typed, recoverable error event
/// in its place (the stream goes on). Not reached in practice — the frame's
/// payload is already a `serde_json::Value`.
fn sse_unserializable(frame: &str, e: axum::Error) -> Event {
    sse_error_event(
        true,
        &RemoteError::Execution {
            message: format!("failed to serialize {frame}: {e}"),
        },
    )
}

/// Stream PlexusStream items as Server-Sent Events
async fn stream_sse_response(stream: PlexusStream) -> Response {
    let event_stream = stream.map(|item| -> Result<Event, std::convert::Infallible> {
        match item {
            PlexusStreamItem::Data { content, .. } => {
                Ok(Event::default()
                    .event("data")
                    .json_data(json!({ "content": content }))
                    .unwrap_or_else(|e| sse_unserializable("data", e)))
            }
            PlexusStreamItem::Progress { content_type, content, .. } => {
                Ok(Event::default()
                    .event("progress")
                    .json_data(SseProgress { content_type: &content_type, content: &content })
                    .unwrap_or_else(|e| sse_unserializable("progress", e)))
            }
            PlexusStreamItem::Error { recoverable, error, .. } => {
                Ok(sse_error_event(recoverable, &error))
            }
            PlexusStreamItem::Done { .. } => {
                Ok(Event::default()
                    .event("done")
                    .data("{}"))
            }
            PlexusStreamItem::Request { request_id, .. } => {
                // Bidirectional requests not supported in REST HTTP
                tracing::warn!(
                    "Ignoring bidirectional request in REST HTTP transport (id: {})",
                    request_id
                );
                // Send a warning event
                Ok(Event::default()
                    .event("warning")
                    .data("Bidirectional requests not supported in REST HTTP transport"))
            }
        }
    });

    Sse::new(event_stream).into_response()
}
