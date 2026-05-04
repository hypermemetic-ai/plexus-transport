//! Transport server builder and orchestration

use anyhow::Result;
use plexus_core::plexus::{Activation, PluginSchema, SessionValidator};
use jsonrpsee::server::ServerHandle;
use jsonrpsee::RpcModule;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::config::{McpHttpConfig, StdioConfig, TransportConfig, WebSocketConfig};
use crate::mcp::bridge::RouteFn;
use crate::mcp::server::serve_mcp_http;
use crate::stdio::serve_stdio;
use crate::websocket::serve_websocket;

/// Function type for converting Arc<Activation> to RpcModule
///
/// This allows each activation type to provide its own conversion logic,
/// which is critical for preserving Arc lifecycle and Weak references.
pub type RpcConverter<A> = Box<dyn FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send>;

/// Transport server that can host any Activation
///
/// Supports multiple transports simultaneously:
/// - WebSocket (JSON-RPC)
/// - Stdio (line-delimited JSON-RPC, MCP-compatible)
/// - MCP HTTP (with SSE streaming)
pub struct TransportServer<A: Activation> {
    activation: Arc<A>,
    config: TransportConfig,
    rpc_converter: Option<RpcConverter<A>>,
    /// Pre-computed flat schema list for MCP tool exposure.
    /// When set, the MCP bridge exposes all listed schemas as tools.
    mcp_flat_schemas: Option<Vec<PluginSchema>>,
    /// Optional routing function for hub activations.
    /// When set, MCP call_tool uses it to dispatch namespaced calls via hub.route().
    mcp_route_fn: Option<RouteFn>,
    /// Optional session validator for cookie-based authentication.
    /// When set, validates cookies from HTTP upgrade requests.
    session_validator: Option<Arc<dyn SessionValidator>>,
    /// RED-9: opt-in strict mode for the session-validator path. When `true`,
    /// the WS upgrade is rejected with HTTP 401 if the cookie is absent OR
    /// the validator returns `None`. When `false` (default, backward-compat),
    /// the middleware logs and proceeds without `AuthContext`; per-method
    /// `#[from_auth]` checks fire-closed at dispatch instead.
    reject_upgrade_on_auth_failure: bool,
}

impl<A: Activation> TransportServer<A> {
    /// Create a builder for configuring transports
    pub fn builder<F>(activation: Arc<A>, rpc_converter: F) -> TransportServerBuilder<A>
    where
        F: FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send + 'static,
    {
        TransportServerBuilder::new(activation, rpc_converter)
    }

    /// Start all configured transports
    ///
    /// If stdio is configured, this will block on stdio (as it's the primary transport).
    /// Otherwise, it will start WebSocket/MCP servers and wait for them to complete.
    pub async fn serve(mut self) -> Result<()> {
        // Convert activation to RPC module for WebSocket/stdio
        let needs_rpc = self.config.websocket.is_some() || self.config.stdio.is_some();
        let module = if needs_rpc {
            let converter = self
                .rpc_converter
                .take()
                .ok_or_else(|| anyhow::anyhow!("RPC converter required for WebSocket/stdio"))?;
            Some(converter(self.activation.clone())?)
        } else {
            None
        };

        // Start stdio transport (blocking)
        if let Some(stdio_config) = self.config.stdio {
            let module = module.expect("RPC module should be created for stdio");
            return serve_stdio(module, stdio_config).await;
        }

        // Start WebSocket transport
        let ws_handle: Option<ServerHandle> = if let Some(mut ws_config) = self.config.websocket {
            // Propagate the global api_key + header config to the WebSocket
            // config if not already set on the WS-specific config.
            if ws_config.api_key.is_none() {
                ws_config.api_key = self.config.api_key.clone();
            }
            // Always propagate the configured api_key_header from the global
            // config — the WS-specific default is set in WebSocketConfig::new
            // and we want the global builder method to win.
            ws_config.api_key_header = self.config.api_key_header.clone();
            let module = module.expect("RPC module should be created for WebSocket");
            Some(serve_websocket(module, ws_config, self.session_validator.clone(), self.reject_upgrade_on_auth_failure).await?)
        } else {
            None
        };

        // Start MCP HTTP transport
        let mcp_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> =
            if let Some(mcp_config) = self.config.mcp_http {
                let api_key = self.config.api_key.clone();
                Some(serve_mcp_http(self.activation.clone(), self.mcp_flat_schemas.clone(), self.mcp_route_fn.clone(), mcp_config, api_key).await?)
            } else {
                None
            };

        // Start REST HTTP transport
        #[cfg(feature = "http-gateway")]
        let rest_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> =
            if let Some(rest_config) = self.config.rest_http {
                let api_key = self.config.api_key.clone();
                Some(crate::http::serve_rest_http(self.activation.clone(), self.mcp_flat_schemas.clone(), self.mcp_route_fn.clone(), rest_config, api_key).await?)
            } else {
                None
            };

        #[cfg(not(feature = "http-gateway"))]
        let rest_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> = None;

        // Wait for any server to complete
        if ws_handle.is_none() && mcp_handle.is_none() && rest_handle.is_none() {
            tracing::warn!("No transports configured, nothing to serve");
            return Ok(());
        }

        // Wait for first server to stop
        tokio::select! {
            _ = async {
                if let Some(ws) = ws_handle {
                    ws.stopped().await;
                    tracing::info!("WebSocket server stopped");
                }
            }, if ws_handle.is_some() => {}

            _ = async {
                if let Some(mcp) = mcp_handle {
                    match mcp.await {
                        Ok(Ok(())) => tracing::info!("MCP server stopped"),
                        Ok(Err(e)) => tracing::error!("MCP server error: {}", e),
                        Err(e) => tracing::error!("MCP server task failed: {}", e),
                    }
                }
            }, if mcp_handle.is_some() => {}

            _ = async {
                if let Some(rest) = rest_handle {
                    match rest.await {
                        Ok(Ok(())) => tracing::info!("REST server stopped"),
                        Ok(Err(e)) => tracing::error!("REST server error: {}", e),
                        Err(e) => tracing::error!("REST server task failed: {}", e),
                    }
                }
            }, if rest_handle.is_some() => {}
        }

        Ok(())
    }

}

/// Builder for configuring transport servers
pub struct TransportServerBuilder<A: Activation> {
    activation: Arc<A>,
    config: TransportConfig,
    rpc_converter: Option<RpcConverter<A>>,
    mcp_flat_schemas: Option<Vec<PluginSchema>>,
    mcp_route_fn: Option<RouteFn>,
    session_validator: Option<Arc<dyn SessionValidator>>,
    /// RED-4: opt-out of the build-time check that refuses to start the
    /// server when activations declare auth-gated methods but no auth
    /// middleware has been configured. Set by
    /// `.allow_missing_auth_middleware()`.
    allow_missing_auth: bool,
    /// RED-9: opt-in strict-mode for the WS upgrade. Set by
    /// `.reject_upgrade_on_auth_failure()`.
    reject_upgrade_on_auth_failure: bool,
}

impl<A: Activation> TransportServerBuilder<A> {
    pub fn new<F>(activation: Arc<A>, rpc_converter: F) -> Self
    where
        F: FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send + 'static,
    {
        Self {
            activation,
            config: TransportConfig::default(),
            rpc_converter: Some(Box::new(rpc_converter)),
            mcp_flat_schemas: None,
            mcp_route_fn: None,
            session_validator: None,
            allow_missing_auth: false,
            reject_upgrade_on_auth_failure: false,
        }
    }

    /// Enable WebSocket transport on the specified port
    pub fn with_websocket(mut self, port: u16) -> Self {
        self.config.websocket = Some(WebSocketConfig::new(port));
        self
    }

    /// Enable stdio transport (MCP-compatible)
    pub fn with_stdio(mut self) -> Self {
        self.config.stdio = Some(StdioConfig::default());
        self
    }

    /// Enable MCP HTTP transport on the specified port
    pub fn with_mcp_http(mut self, port: u16) -> Self {
        self.config.mcp_http = Some(McpHttpConfig::new(port));
        self
    }

    /// Enable MCP HTTP transport with custom configuration
    pub fn with_mcp_http_config(mut self, config: McpHttpConfig) -> Self {
        self.config.mcp_http = Some(config);
        self
    }

    /// Enable REST HTTP transport on the specified port
    #[cfg(feature = "http-gateway")]
    pub fn with_rest_http(mut self, port: u16) -> Self {
        self.config.rest_http = Some(crate::config::RestHttpConfig::new(port));
        self
    }

    /// Enable REST HTTP transport with custom configuration
    #[cfg(feature = "http-gateway")]
    pub fn with_rest_http_config(mut self, config: crate::config::RestHttpConfig) -> Self {
        self.config.rest_http = Some(config);
        self
    }

    /// Set pre-computed flat schemas for MCP tool exposure.
    /// For hub activations, pass `hub.list_plugin_schemas()` to expose all child schemas.
    pub fn with_mcp_flat_schemas(mut self, schemas: Vec<PluginSchema>) -> Self {
        self.mcp_flat_schemas = Some(schemas);
        self
    }

    /// Set routing function for MCP call_tool dispatch.
    /// For hub activations, provide a closure wrapping `hub.route()` so that
    /// namespaced tool calls (e.g., "loopback.permit") reach the correct child.
    pub fn with_mcp_route_fn(mut self, route_fn: RouteFn) -> Self {
        self.mcp_route_fn = Some(route_fn);
        self
    }

    /// Configure a static admission key required on all WebSocket connections.
    ///
    /// When set, the WebSocket upgrade is rejected with HTTP 401 unless the
    /// configured api_key header (default `X-Plexus-API-Key`, see
    /// [`Self::with_api_key_header`]) carries the matching value. Passing
    /// `None` disables the static admission gate (default behaviour).
    ///
    /// Per AUTHZ-BEARER-1, the static api_key gate is independent of the
    /// `SessionValidator` path. The `Authorization: Bearer` header is reserved
    /// for `SessionValidator` user-identity tokens and is not consulted by the
    /// static admission gate. (One exception: the deprecated v1 compat shim
    /// — see the WS middleware docs.)
    ///
    /// MCP HTTP and REST HTTP gateways continue to enforce api_key against
    /// `Authorization: Bearer` until a follow-up ticket aligns them.
    pub fn with_api_key(mut self, key: Option<String>) -> Self {
        self.config.api_key = key;
        self
    }

    /// Override the header that carries the static api_key on WebSocket upgrades.
    ///
    /// Default: `X-Plexus-API-Key`. Header lookup is case-insensitive per the
    /// HTTP spec; the stored `HeaderName` is normalized to lowercase. Has no
    /// effect when `with_api_key(None)` is configured.
    ///
    /// AUTHZ-BEARER-1: separates the static-admission header from the
    /// `Authorization` header that carries `SessionValidator`-consumed Bearer
    /// tokens.
    pub fn with_api_key_header(mut self, header: http::HeaderName) -> Self {
        self.config.api_key_header = header;
        self
    }

    /// Set session validator for cookie-based authentication.
    ///
    /// When set, the WebSocket transport will extract cookies from HTTP upgrade
    /// requests and validate them using the provided SessionValidator. The resulting
    /// AuthContext is stored in request Extensions and passed to RPC methods.
    ///
    /// This is useful for browser-based authentication where cookies are preferred
    /// over Authorization headers.
    pub fn with_session_validator(mut self, validator: Arc<dyn SessionValidator>) -> Self {
        self.session_validator = Some(validator);
        self
    }

    /// RED-9: opt in to strict-mode session validation at the WebSocket
    /// upgrade.
    ///
    /// By default (backward-compatible), the session-validator middleware
    /// populates `AuthContext` when a valid cookie is present and silently
    /// passes through with no auth context when the cookie is missing or
    /// invalid. Methods with `#[from_auth(...)]` then fail-closed at
    /// dispatch with `Unauthenticated` — but methods without it dispatch
    /// normally.
    ///
    /// When this option is enabled:
    ///
    /// - Missing `Cookie:` header on the WS upgrade → HTTP 401 before any
    ///   RPC frames flow.
    /// - Cookie present but `SessionValidator::validate()` returns `None`
    ///   → HTTP 401 before any RPC frames flow.
    ///
    /// This is the right posture for backends where ANY public endpoint
    /// would be a problem (e.g., FormVeritas-style apps where every method
    /// requires a valid session). For backends that intentionally mix
    /// authenticated and public endpoints on the same transport, leave
    /// this off.
    ///
    /// Has no effect when no `SessionValidator` is configured. To force
    /// auth middleware to be configured at build time, see
    /// [`Self::allow_missing_auth_middleware`] (RED-4).
    pub fn reject_upgrade_on_auth_failure(mut self) -> Self {
        self.reject_upgrade_on_auth_failure = true;
        self
    }

    /// RED-4: opt out of the build-time auth-configuration check.
    ///
    /// Normally, [`Self::build`] inspects the registered activation's
    /// `plugin_schema()` for methods carrying an `x-plexus-source.from == "auth"`
    /// annotation (emitted by `#[from_auth(...)]`). If any such method exists
    /// AND neither [`Self::with_api_key`] nor [`Self::with_session_validator`]
    /// was called, `build()` returns `Err` to prevent deploying an auth-gated
    /// activation to a server without auth middleware.
    ///
    /// Call this method to bypass the check when you intentionally want a
    /// server with no auth (e.g., local test harness, fully public backend).
    /// The presence of this call in source code is an audit signal.
    pub fn allow_missing_auth_middleware(mut self) -> Self {
        self.allow_missing_auth = true;
        self
    }

    /// Build the transport server.
    ///
    /// RED-4: fails with a clear error when the activation declares auth-gated
    /// methods but no auth middleware has been configured. Opt out via
    /// [`Self::allow_missing_auth_middleware`].
    pub async fn build(self) -> Result<TransportServer<A>> {
        // RED-4: verify auth configuration before starting the server.
        let has_api_key = self.config.api_key.is_some();
        let has_session_validator = self.session_validator.is_some();
        let auth_configured = has_api_key || has_session_validator;

        if !auth_configured && !self.allow_missing_auth {
            let auth_gated = collect_auth_gated_methods(&*self.activation);
            if !auth_gated.is_empty() {
                let summary = auth_gated
                    .iter()
                    .take(8)
                    .map(|m| format!("  - {}", m))
                    .collect::<Vec<_>>()
                    .join("\n");
                let extra = if auth_gated.len() > 8 {
                    format!("\n  ... ({} more)", auth_gated.len() - 8)
                } else {
                    String::new()
                };
                return Err(anyhow::anyhow!(
                    "RED-4: {} auth-gated method(s) declared but no auth middleware \
                     configured. Call `.with_session_validator(...)` or \
                     `.with_api_key(...)` on the builder, or \
                     `.allow_missing_auth_middleware()` to intentionally opt out.\n\
                     \n\
                     Auth-gated methods:\n{}{}",
                    auth_gated.len(),
                    summary,
                    extra,
                ));
            }
        }

        Ok(TransportServer {
            activation: self.activation,
            config: self.config,
            rpc_converter: self.rpc_converter,
            mcp_flat_schemas: self.mcp_flat_schemas,
            mcp_route_fn: self.mcp_route_fn,
            session_validator: self.session_validator,
            reject_upgrade_on_auth_failure: self.reject_upgrade_on_auth_failure,
        })
    }
}

/// RED-4: inspect an activation's plugin schema for methods with
/// `x-plexus-source.from == "auth"`.
///
/// Returns a flat list of `"namespace.method"` paths for error reporting.
/// Activations that declare no auth-gated methods return an empty vec —
/// the caller uses emptiness as a signal that no auth config is required.
///
/// Only inspects the root activation's schema. Standalone child activations
/// register their own `TransportServerBuilder`s and hit this check
/// independently. Hub-style children whose methods appear in the parent
/// schema are covered through the root traversal.
fn collect_auth_gated_methods<A: Activation>(activation: &A) -> Vec<String> {
    let schema = Activation::plugin_schema(activation);
    let mut out = Vec::new();
    collect_from_schema(&schema, &mut out);
    out
}

fn collect_from_schema(schema: &PluginSchema, out: &mut Vec<String>) {
    for method in &schema.methods {
        let params_schema = match &method.params {
            Some(p) => p,
            None => continue,
        };
        let props = match params_schema.get("properties").and_then(|v| v.as_object()) {
            Some(p) => p,
            None => continue,
        };
        for (_name, prop) in props {
            let src = prop.get("x-plexus-source").and_then(|v| v.as_object());
            let from = src.and_then(|s| s.get("from")).and_then(|v| v.as_str());
            if from == Some("auth") {
                out.push(format!("{}.{}", schema.namespace, method.name));
                break;
            }
        }
    }
}
