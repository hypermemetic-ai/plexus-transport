//! Generic MCP server bridge for any Activation
//!
//! This module implements the MCP protocol using the rmcp crate,
//! bridging MCP tool calls to activation methods.
//!
//! ## Frames (plexus 0.7, RETURNS-11)
//!
//! Every frame of a call's stream goes out as a logging notification (logger
//! = the tool name); the tool result is built when the stream ends. The bridge
//! never inspects `content` — it forwards `content` and `error` whole.
//!
//! | frame | logging notification `data` | tool result |
//! | -- | -- | -- |
//! | data | `{"type": "data", "content_type", "data": <content>}` | buffered into the success result |
//! | progress | `{"type": "progress", "content_type", "content": <P>}` (plus `notifications/progress` when the caller sent a progress token) | — |
//! | error | `{"type": "error", "error": <RemoteError>, "recoverable"}` | a non-recoverable one makes it an error result: text = `RemoteError`'s `Display`, structured content = the `RemoteError` JSON |
//!
//! A call rejected before its stream starts answers a JSON-RPC error built
//! by `plexus_error_to_jsonrpc`: `{code, message: <Display>, data:
//! <RemoteError>}`.

// TRANSPORT-23: this bridge streams items as MCP logging notifications, which
// rmcp 3.x deprecates (SEP-2577). The port keeps that behaviour;
// plexus-transport-mcp's bridge does not use them.
#![allow(deprecated)]

use std::sync::Arc;

use futures::StreamExt;
use plexus_core::plexus::{
    plexus_error_to_jsonrpc, types::PlexusStreamItem, Activation, PlexusError, PluginSchema,
    RemoteError,
};
use rmcp::{
    model::*,
    service::{RequestContext, RoleServer},
    ErrorData as McpError, ServerHandler,
};
use serde_json::json;
use form_urlencoded;

/// A function that routes a namespaced method call (e.g., "loopback.permit") to the
/// correct activation. Used by hub activations to dispatch child calls via `hub.route()`.
///
/// Canonical definition lives in [`crate::server`] so it is available with or
/// without the `mcp` feature.
pub use crate::server::RouteFn;

// =============================================================================
// Schema Transformation
// =============================================================================

/// Convert activation schemas to rmcp Tool format
///
/// MCP requires all tool inputSchema to have "type": "object" at root.
/// schemars may produce schemas without this (e.g., for unit types).
fn schemas_to_rmcp_tools(schemas: Vec<PluginSchema>) -> Vec<Tool> {
    schemas
        .into_iter()
        .flat_map(|activation| {
            let namespace = activation.namespace.clone();
            activation.methods.into_iter().map(move |method| {
                let name = format!("{}.{}", namespace, method.name);
                let description = method.description.clone();

                // Convert schemars::Schema to JSON, ensure "type": "object" exists
                let input_schema = method
                    .params
                    .and_then(|s| serde_json::to_value(s).ok())
                    .and_then(|v| v.as_object().cloned())
                    .map(|mut obj| {
                        // MCP requires "type": "object" at schema root
                        if !obj.contains_key("type") {
                            obj.insert("type".to_string(), json!("object"));
                        }
                        Arc::new(obj)
                    })
                    .unwrap_or_else(|| {
                        // Empty params = empty object schema
                        Arc::new(serde_json::Map::from_iter([(
                            "type".to_string(),
                            json!("object"),
                        )]))
                    });

                Tool::new(name, description, input_schema)
            })
        })
        .collect()
}

// =============================================================================
// Error Mapping
// =============================================================================

/// Convert a `PlexusError` (the call was rejected before its stream started)
/// to an MCP JSON-RPC error: plexus-core's semantic `code`, the
/// `RemoteError`'s `Display` as `message`, the typed `RemoteError` in `data`.
fn plexus_to_mcp_error(e: PlexusError) -> McpError {
    let rejection = plexus_error_to_jsonrpc(&e);
    let data = rejection
        .data()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw.get()).ok());
    McpError::new(
        ErrorCode(rejection.code()),
        rejection.message().to_owned(),
        data,
    )
}

/// An MCP progress notification's `(message, percentage)` from a progress
/// frame's `content`: the default `Note` (`{message, percentage?}`) maps field
/// for field; any other progress type travels as its JSON text, no
/// percentage. (The typed `content` itself goes out whole on the logging
/// notification.)
fn progress_note(content: &serde_json::Value) -> (String, Option<f64>) {
    match content.get("message").and_then(|m| m.as_str()) {
        Some(message) => (
            message.to_owned(),
            content.get("percentage").and_then(|p| p.as_f64()),
        ),
        None => (content.to_string(), None),
    }
}

/// The tool result of a call whose stream carried a non-recoverable error
/// frame: `RemoteError`'s `Display` as its text, the `RemoteError` JSON as its
/// structured content.
fn remote_error_tool_result(error: &RemoteError) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(error.to_string())]);
    result.structured_content = serde_json::to_value(error).ok();
    result
}

/// A logging notification from `logger`.
fn log_param(
    level: LoggingLevel,
    logger: &str,
    data: serde_json::Value,
) -> LoggingMessageNotificationParam {
    let mut param = LoggingMessageNotificationParam::new(level, data);
    param.logger = Some(logger.to_string());
    param
}

// =============================================================================
// Generic Activation MCP Bridge
// =============================================================================

/// MCP handler that bridges to any Activation
///
/// Generic over any type implementing the Activation trait from hub-core.
/// This allows hosting single plugins, Plexus hubs, or nested hubs with
/// the same MCP transport infrastructure.
pub struct ActivationMcpBridge<A: Activation> {
    activation: Arc<A>,
    /// Pre-computed flat list of all schemas to expose as MCP tools.
    /// When set, this is used instead of deriving schemas from `plugin_schema()`.
    /// Allows hubs to expose all child activation schemas (e.g., loopback, claudecode).
    flat_schemas: Option<Arc<Vec<PluginSchema>>>,
    server_name_override: Option<String>,
    server_version_override: Option<String>,
    /// Optional routing function for hub activations.
    /// When set, `call_tool` uses this to dispatch namespaced calls (e.g., "loopback.permit")
    /// via `hub.route()` instead of stripping the namespace and calling `activation.call()`.
    router: Option<RouteFn>,
}

impl<A: Activation> ActivationMcpBridge<A> {
    pub fn new(activation: Arc<A>) -> Self {
        Self {
            activation,
            flat_schemas: None,
            server_name_override: None,
            server_version_override: None,
            router: None,
        }
    }

    /// Create bridge with a pre-computed flat schema list.
    /// Use this for hub activations to expose all child schemas as MCP tools.
    pub fn with_flat_schemas(activation: Arc<A>, schemas: Vec<PluginSchema>) -> Self {
        Self {
            activation,
            flat_schemas: Some(Arc::new(schemas)),
            server_name_override: None,
            server_version_override: None,
            router: None,
        }
    }

    /// Create bridge with custom server name/version
    pub fn with_server_info(
        activation: Arc<A>,
        name: Option<String>,
        version: Option<String>,
    ) -> Self {
        Self {
            activation,
            flat_schemas: None,
            server_name_override: name,
            server_version_override: version,
            router: None,
        }
    }

    /// Create bridge with custom server name/version and pre-computed schemas
    pub fn with_server_info_and_schemas(
        activation: Arc<A>,
        name: Option<String>,
        version: Option<String>,
        schemas: Option<Vec<PluginSchema>>,
    ) -> Self {
        Self {
            activation,
            flat_schemas: schemas.map(|s| Arc::new(s)),
            server_name_override: name,
            server_version_override: version,
            router: None,
        }
    }

    /// Set the routing function used in `call_tool` for dispatching namespaced method calls.
    ///
    /// Hub activations should provide a function that wraps `hub.route()` so that
    /// calls like "loopback.permit" are dispatched to the correct child activation.
    pub fn with_router(mut self, router: RouteFn) -> Self {
        self.router = Some(router);
        self
    }
}

impl<A: Activation> Clone for ActivationMcpBridge<A> {
    fn clone(&self) -> Self {
        Self {
            activation: self.activation.clone(),
            flat_schemas: self.flat_schemas.clone(),
            server_name_override: self.server_name_override.clone(),
            server_version_override: self.server_version_override.clone(),
            router: self.router.clone(),
        }
    }
}

impl<A: Activation> ServerHandler for ActivationMcpBridge<A> {
    fn get_info(&self) -> ServerConfig {
        // Use activation's namespace and version for server identity
        // Allow override via config
        let name = self
            .server_name_override
            .clone()
            .unwrap_or_else(|| self.activation.namespace().to_string());
        let version = self
            .server_version_override
            .clone()
            .unwrap_or_else(|| self.activation.version().to_string());

        let mut info = ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_logging()
                .build(),
        );
        info.server_info = Implementation::new(name, version);
        info.instructions = Some(self.activation.description().to_string());
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        // Use pre-computed flat schemas if available (set for hub activations).
        // Otherwise fall back to single activation schema.
        let schemas = if let Some(ref flat) = self.flat_schemas {
            flat.as_ref().clone()
        } else {
            vec![self.activation.plugin_schema()]
        };

        let tools = schemas_to_rmcp_tools(schemas);
        tracing::debug!("Listing {} tools", tools.len());

        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        // KEEL-59: a cancel (`notifications/cancelled`) or a client gone
        // away ends the call at once — the call's future, and the method's
        // stream inside it, dropped here — not at the method's next item;
        // a unary method still being routed, and a send to a gone peer, are
        // cut off with it.
        let work = async {
            let method_name = &request.name;
            let mut arguments_map = request
                .arguments
                .unwrap_or_else(|| serde_json::Map::new());

            tracing::debug!("Calling tool: {} with args: {}", method_name, plexus_core::plexus::redact::redact_str(&format!("{arguments_map:?}")));

            // Extract HTTP connection metadata from extensions and inject into arguments.
            // This makes the gateway transparent: all HTTP-level metadata (query params, headers)
            // is forwarded to the backend without the gateway needing to know what it means.
            tracing::debug!("[MCP BRIDGE] call_tool: method={}, checking for HTTP metadata...", method_name);
            if let Some(parts) = ctx.extensions.get::<http::request::Parts>() {
                tracing::debug!("[MCP BRIDGE] Found HTTP Parts! URI: {}", plexus_core::plexus::redact::redact_str(&parts.uri.to_string()));
                let mut connection_meta = serde_json::Map::new();

                // Forward ALL query parameters
                if let Some(query) = parts.uri.query() {
                    tracing::debug!("[MCP BRIDGE] Query string: {}", plexus_core::plexus::redact::redact_str(query));
                    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
                        tracing::debug!("[MCP BRIDGE] Query param: {}", plexus_core::plexus::redact::redact_str(&format!("{key}={value}")));
                        connection_meta.insert(
                            format!("query.{}", key),
                            json!(value.to_string()),
                        );
                    }
                } else {
                    tracing::debug!("[MCP BRIDGE] No query string in URI");
                }

                // If we extracted any connection metadata, inject it
                if !connection_meta.is_empty() {
                    arguments_map.insert("_connection".to_string(), json!(connection_meta));
                    tracing::debug!("[MCP BRIDGE] Injected connection metadata: {}", plexus_core::plexus::redact::redact_str(&format!("{connection_meta:?}")));
                    tracing::debug!("Injected connection metadata: {}", plexus_core::plexus::redact::redact_str(&format!("{connection_meta:?}")));
                } else {
                    tracing::debug!("[MCP BRIDGE] No connection metadata to inject");
                }
            } else {
                tracing::debug!("[MCP BRIDGE] No HTTP Parts in extensions!");
            }

            let arguments_value = serde_json::Value::Object(arguments_map);

            // Get progress token if provided
            let progress_token = ctx.meta.get_progress_token();

            // Logger name: namespace.method (e.g., bash.execute)
            let logger = method_name.to_string();

            // Call activation and get stream.
            // If a router is available (hub activations), use it to dispatch the full
            // namespaced method name (e.g., "loopback.permit") to the correct child.
            // Otherwise strip the namespace prefix and call activation directly.
            let stream = if let Some(ref router) = self.router {
                router(method_name.to_string(), arguments_value)
                    .await
                    .map_err(plexus_to_mcp_error)?
            } else {
                let method = if method_name.contains('.') {
                    method_name.split('.').nth(1).unwrap_or(method_name)
                } else {
                    method_name
                };
                // RETURNS-47: the leaf path. This bridge names no caller, and
                // with no service there is no gate: the call runs as a service
                // with no gate runs one, anonymous and admitted everywhere
                // (`call::ungated`). `Activation::call` alone runs in no context,
                // where a method reading `reach` or `caller()` is refused.
                plexus_core::call::ungated(&*self.activation, method, arguments_value, None, None)
                    .await
                    .map_err(plexus_to_mcp_error)?
            };

            // Stream events via notifications AND buffer for final result
            let mut fatal: Option<RemoteError> = None;
            let mut buffered_data: Vec<serde_json::Value> = Vec::new();

            tokio::pin!(stream);
            while let Some(item) = stream.next().await {
                match &item {
                    PlexusStreamItem::Progress {
                        content_type,
                        content,
                        ..
                    } => {
                        // The typed progress value, whole, for every consumer
                        let _ = ctx
                            .peer
                            .notify_logging_message(log_param(LoggingLevel::Info, &logger, json!({
                                    "type": "progress",
                                    "content_type": content_type,
                                    "content": content,
                                })))
                            .await;

                        // MCP-native progress only if the client provided a token
                        if let Some(ref token) = progress_token {
                            let (message, percentage) = progress_note(content);
                            let mut progress =
                                ProgressNotificationParam::new(token.clone(), percentage.unwrap_or(0.0));
                            progress.message = Some(message);
                            let _ = ctx.peer.notify_progress(progress).await;
                        }
                    }

                    PlexusStreamItem::Data {
                        content,
                        content_type,
                        ..
                    } => {
                        // Buffer data for final result
                        buffered_data.push(content.to_value());

                        // Also stream via notifications for real-time consumers
                        let _ = ctx
                            .peer
                            .notify_logging_message(log_param(LoggingLevel::Info, &logger, json!({
                                    "type": "data",
                                    "content_type": content_type,
                                    "data": content,
                                })))
                            .await;
                    }

                    PlexusStreamItem::Error {
                        error,
                        recoverable,
                        ..
                    } => {
                        let _ = ctx
                            .peer
                            .notify_logging_message(log_param(LoggingLevel::Error, &logger, json!({
                                    "type": "error",
                                    "error": error,
                                    "recoverable": recoverable,
                                })))
                            .await;

                        // The first non-recoverable error is why the call failed
                        if !recoverable && fatal.is_none() {
                            fatal = Some(error.clone());
                        }
                    }

                    PlexusStreamItem::Done { .. } => {
                        break;
                    }
                }
            }

            // Return buffered data in the final result
            if let Some(error) = fatal {
                Ok(remote_error_tool_result(&error).into())
            } else {
                // Convert buffered data to content
                let text_content = if buffered_data.is_empty() {
                    "(no output)".to_string()
                } else if buffered_data.len() == 1 {
                    // Single value - return as text if string, otherwise JSON
                    match &buffered_data[0] {
                        serde_json::Value::String(s) => s.clone(),
                        other => serde_json::to_string_pretty(other).unwrap_or_default(),
                    }
                } else {
                    // Multiple values - join strings or return as JSON array
                    let all_strings = buffered_data.iter().all(|v| v.is_string());
                    if all_strings {
                        buffered_data
                            .iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join("")
                    } else {
                        serde_json::to_string_pretty(&buffered_data).unwrap_or_default()
                    }
                };

                Ok(CallToolResult::success(vec![ContentBlock::text(text_content)]).into())
            }
        };
        tokio::select! {
            biased;
            _ = ctx.ct.cancelled() => Err(McpError::internal_error("Cancelled", None)),
            _ = plexus_transport_mcp::gone::client_gone(&ctx) => Err(McpError::internal_error("Cancelled: the client went away", None)),
            answer = work => answer,
        }
    }
}
