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
            // Propagate the global api_key to the WebSocket config if not already set.
            if ws_config.api_key.is_none() {
                ws_config.api_key = self.config.api_key.clone();
            }
            let module = module.expect("RPC module should be created for WebSocket");
            Some(serve_websocket(module, ws_config, self.session_validator.clone()).await?)
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

    /// Require `Authorization: Bearer <key>` on all WebSocket and MCP HTTP connections.
    ///
    /// When set, connections missing or supplying the wrong token are rejected with
    /// HTTP 401. Passing `None` disables authentication (default behaviour).
    pub fn with_api_key(mut self, key: Option<String>) -> Self {
        self.config.api_key = key;
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

    /// Build the transport server
    pub async fn build(self) -> Result<TransportServer<A>> {
        Ok(TransportServer {
            activation: self.activation,
            config: self.config,
            rpc_converter: self.rpc_converter,
            mcp_flat_schemas: self.mcp_flat_schemas,
            mcp_route_fn: self.mcp_route_fn,
            session_validator: self.session_validator,
        })
    }
}
