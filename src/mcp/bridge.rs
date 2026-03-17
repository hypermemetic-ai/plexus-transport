//! Generic MCP server bridge for any Activation
//!
//! This module implements the MCP protocol using the rmcp crate,
//! bridging MCP tool calls to activation methods.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use plexus_core::plexus::{types::PlexusStreamItem, Activation, PlexusError, PlexusStream, PluginSchema};
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
/// Signature: `fn(method: String, params: Value) -> Future<Output = Result<PlexusStream, PlexusError>>`
pub type RouteFn = Arc<
    dyn Fn(String, serde_json::Value) -> Pin<Box<dyn Future<Output = Result<PlexusStream, PlexusError>> + Send>>
        + Send
        + Sync,
>;

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

/// Convert PlexusError to McpError
fn plexus_to_mcp_error(e: PlexusError) -> McpError {
    match e {
        PlexusError::ActivationNotFound(name) => {
            McpError::invalid_params(format!("Unknown activation: {}", name), None)
        }
        PlexusError::MethodNotFound { activation, method } => McpError::invalid_params(
            format!("Unknown method: {}.{}", activation, method),
            None,
        ),
        PlexusError::InvalidParams(reason) => McpError::invalid_params(reason, None),
        PlexusError::ExecutionError(error) => McpError::internal_error(error, None),
        PlexusError::HandleNotSupported(activation) => McpError::invalid_params(
            format!("Handle resolution not supported: {}", activation),
            None,
        ),
        PlexusError::TransportError(kind) => {
            McpError::internal_error(format!("Transport error: {:?}", kind), None)
        }
    }
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
    fn get_info(&self) -> ServerInfo {
        // Use activation's namespace and version for server identity
        // Allow override via config
        let mut server_info = Implementation::from_build_env();
        server_info.name = self
            .server_name_override
            .clone()
            .unwrap_or_else(|| self.activation.namespace().to_string());
        server_info.version = self
            .server_version_override
            .clone()
            .unwrap_or_else(|| self.activation.version().to_string());

        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_logging()
                .build(),
            server_info,
            instructions: Some(self.activation.description().to_string()),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
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

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let method_name = &request.name;
        let mut arguments_map = request
            .arguments
            .unwrap_or_else(|| serde_json::Map::new());

        tracing::debug!("Calling tool: {} with args: {:?}", method_name, arguments_map);

        // Extract HTTP connection metadata from extensions and inject into arguments.
        // This makes the gateway transparent: all HTTP-level metadata (query params, headers)
        // is forwarded to the backend without the gateway needing to know what it means.
        tracing::debug!("[MCP BRIDGE] call_tool: method={}, checking for HTTP metadata...", method_name);
        if let Some(parts) = ctx.extensions.get::<http::request::Parts>() {
            tracing::debug!("[MCP BRIDGE] Found HTTP Parts! URI: {}", parts.uri);
            let mut connection_meta = serde_json::Map::new();

            // Forward ALL query parameters
            if let Some(query) = parts.uri.query() {
                tracing::debug!("[MCP BRIDGE] Query string: {}", query);
                for (key, value) in form_urlencoded::parse(query.as_bytes()) {
                    tracing::debug!("[MCP BRIDGE] Query param: {}={}", key, value);
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
                tracing::debug!("[MCP BRIDGE] Injected connection metadata: {:?}", connection_meta);
                tracing::debug!("Injected connection metadata: {:?}", connection_meta);
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
            self.activation
                .call(method, arguments_value)
                .await
                .map_err(plexus_to_mcp_error)?
        };

        // Stream events via notifications AND buffer for final result
        let mut had_error = false;
        let mut buffered_data: Vec<serde_json::Value> = Vec::new();
        let mut error_messages: Vec<String> = Vec::new();

        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            // Check cancellation on each iteration
            if ctx.ct.is_cancelled() {
                return Err(McpError::internal_error("Cancelled", None));
            }

            match &item {
                PlexusStreamItem::Progress {
                    message,
                    percentage,
                    ..
                } => {
                    // Only send progress if client provided token
                    if let Some(ref token) = progress_token {
                        let _ = ctx
                            .peer
                            .notify_progress(ProgressNotificationParam {
                                progress_token: token.clone(),
                                progress: percentage.unwrap_or(0.0) as f64,
                                total: None,
                                message: Some(message.clone()),
                            })
                            .await;
                    }
                }

                PlexusStreamItem::Data {
                    content,
                    content_type,
                    ..
                } => {
                    // Buffer data for final result
                    buffered_data.push(content.clone());

                    // Also stream via notifications for real-time consumers
                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Info,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "data",
                                "content_type": content_type,
                                "data": content,
                            }),
                        })
                        .await;
                }

                PlexusStreamItem::Error {
                    message,
                    recoverable,
                    ..
                } => {
                    // Buffer errors for final result
                    error_messages.push(message.clone());

                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Error,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "error",
                                "error": message,
                                "recoverable": recoverable,
                            }),
                        })
                        .await;

                    if !recoverable {
                        had_error = true;
                    }
                }

                PlexusStreamItem::Done { .. } => {
                    break;
                }

                PlexusStreamItem::Request {
                    request_id,
                    request_data,
                    timeout_ms,
                } => {
                    // Send bidirectional request to client via logging notification
                    // Client should respond via _plexus_respond tool
                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Info,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "request",
                                "request_id": request_id,
                                "request_data": request_data,
                                "timeout_ms": timeout_ms,
                            }),
                        })
                        .await;
                }
            }
        }

        // Return buffered data in the final result
        if had_error {
            let error_content = if error_messages.is_empty() {
                "Stream completed with errors".to_string()
            } else {
                error_messages.join("\n")
            };
            Ok(CallToolResult::error(vec![Content::text(error_content)]))
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

            Ok(CallToolResult::success(vec![Content::text(
                text_content,
            )]))
        }
    }
}
