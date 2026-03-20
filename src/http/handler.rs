//! Request/response handlers for REST HTTP transport
//!
//! Handles PlexusStream responses, converting them to either:
//! - JSON responses for non-streaming methods
//! - Server-Sent Events (SSE) for streaming methods
//!
//! ## Protection Mechanisms
//!
//! - **Timeouts**: Methods timeout after 5 minutes to prevent infinite streams
//! - **Buffer Limits**: Non-streaming methods enforce max item count and byte limits
//! - **Memory Safety**: Prevents unbounded buffering and OOM crashes

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response, sse::{Event, Sse}},
    Json,
};
use futures::StreamExt;
use plexus_core::plexus::{types::PlexusStreamItem, PlexusStream};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Configuration Constants
// =============================================================================

/// Maximum time to wait for a method to complete (5 minutes)
const METHOD_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum number of items to buffer for non-streaming methods
const MAX_BUFFERED_ITEMS: usize = 10_000;

/// Maximum total bytes to buffer for non-streaming methods (100MB)
const MAX_BUFFER_BYTES: usize = 100 * 1024 * 1024;

// =============================================================================
// Method Info
// =============================================================================

/// Metadata about a method for response handling
#[derive(Clone, Debug)]
pub struct MethodInfo {
    pub namespace: String,
    pub method: String,
    pub streaming: bool,
}

// =============================================================================
// Response Handling
// =============================================================================

/// Handle a method call, routing to JSON or SSE response based on streaming flag
///
/// This function applies timeout protection to prevent infinite streams from hanging forever.
pub async fn handle_method_call(stream: PlexusStream, method_info: MethodInfo) -> Response {
    // Apply timeout to prevent infinite streams
    let result = if method_info.streaming {
        tokio::time::timeout(METHOD_TIMEOUT, stream_sse_response(stream)).await
    } else {
        tokio::time::timeout(METHOD_TIMEOUT, collect_and_respond(stream)).await
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
    let mut progress_items = Vec::new();
    let mut error_msg: Option<String> = None;
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
            PlexusStreamItem::Progress { message, percentage, .. } => {
                let mut progress_obj = json!({
                    "message": message,
                });
                if let Some(pct) = percentage {
                    progress_obj["percentage"] = json!(pct);
                }
                progress_items.push(progress_obj);
            }
            PlexusStreamItem::Error { message, .. } => {
                error_msg = Some(message);
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

    if let Some(error) = error_msg {
        response_obj["error"] = json!(error);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(response_obj),
        ).into_response();
    }

    (StatusCode::OK, Json(response_obj)).into_response()
}

/// Stream PlexusStream items as Server-Sent Events
async fn stream_sse_response(stream: PlexusStream) -> Response {
    let event_stream = stream.map(|item| -> Result<Event, std::convert::Infallible> {
        match item {
            PlexusStreamItem::Data { content, .. } => {
                Ok(Event::default()
                    .event("data")
                    .json_data(json!({ "content": content }))
                    .unwrap_or_else(|e| {
                        Event::default()
                            .event("error")
                            .data(format!("Failed to serialize data: {}", e))
                    }))
            }
            PlexusStreamItem::Progress { message, percentage, .. } => {
                let mut progress_obj = json!({
                    "message": message,
                });
                if let Some(pct) = percentage {
                    progress_obj["percentage"] = json!(pct);
                }
                Ok(Event::default()
                    .event("progress")
                    .json_data(progress_obj)
                    .unwrap_or_else(|e| {
                        Event::default()
                            .event("error")
                            .data(format!("Failed to serialize progress: {}", e))
                    }))
            }
            PlexusStreamItem::Error { message, code, .. } => {
                let mut error_obj = json!({ "message": message });
                if let Some(c) = code {
                    error_obj["code"] = json!(c);
                }
                Ok(Event::default()
                    .event("error")
                    .json_data(error_obj)
                    .unwrap_or_else(|_| {
                        Event::default()
                            .event("error")
                            .data(message)
                    }))
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
