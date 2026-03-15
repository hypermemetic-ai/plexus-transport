//! MCP HTTP server setup

use anyhow::Result;
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::any, Router,
};
use plexus_core::plexus::Activation;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::config::McpHttpConfig;
use crate::mcp::bridge::{ActivationMcpBridge, RouteFn};

#[cfg(feature = "sqlite-sessions")]
use crate::mcp::session::{SqliteSessionConfig, SqliteSessionManager};

/// Middleware to enforce `Authorization: Bearer <key>` on all MCP HTTP requests.
///
/// When the `api_key` state is `Some(key)`, requests missing or supplying the
/// wrong token are rejected with HTTP 401. When state is `None`, all requests
/// pass through unchanged (preserving the no-auth default).
async fn auth_middleware(
    axum::extract::State(api_key): axum::extract::State<Option<String>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(ref key) = api_key {
        let expected = format!("Bearer {}", key);
        let ok = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == expected)
            .unwrap_or(false);

        if !ok {
            tracing::warn!(
                "MCP HTTP auth rejected: missing or invalid Authorization header (uri={})",
                request.uri()
            );
            return (
                StatusCode::UNAUTHORIZED,
                [(
                    http::header::WWW_AUTHENTICATE,
                    http::HeaderValue::from_static("Bearer realm=\"plexus\""),
                )],
                "Unauthorized",
            )
                .into_response();
        }
    }
    next.run(request).await
}

/// Middleware to log all incoming HTTP requests
async fn log_request_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();

    tracing::info!("▶▶▶ MCP HTTP REQUEST ▶▶▶");
    tracing::info!("  Method: {}", method);
    tracing::info!("  URI: {}", uri);
    tracing::info!("  Headers:");
    for (name, value) in headers.iter() {
        tracing::info!("    {}: {:?}", name, value);
    }

    let response = next.run(request).await;

    let status = response.status();
    tracing::info!("◀◀◀ MCP HTTP RESPONSE ◀◀◀");
    tracing::info!("  Status: {}", status);

    response
}

/// Fallback handler for unmatched routes - logs and returns debug info
async fn fallback_handler(request: Request) -> impl IntoResponse {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();

    tracing::error!("╔══════════════════════════════════════════════════════════╗");
    tracing::error!("║  UNMATCHED REQUEST - NO ROUTE FOUND                      ║");
    tracing::error!("╚══════════════════════════════════════════════════════════╝");
    tracing::error!("  Method: {}", method);
    tracing::error!("  URI: {}", uri);
    tracing::error!("  Path: {}", uri.path());
    tracing::error!("  Query: {:?}", uri.query());
    tracing::error!("  Headers:");
    for (name, value) in headers.iter() {
        tracing::error!("    {}: {:?}", name, value);
    }

    let body_hint = if method == axum::http::Method::POST {
        "(body not captured in fallback - check /mcp endpoint)"
    } else {
        "(no body expected)"
    };
    tracing::error!("  Body: {}", body_hint);
    tracing::error!("");
    tracing::error!("  HINT: MCP endpoint is at /mcp");
    tracing::error!("  HINT: Make sure to send 'initialize' request first!");

    let debug_response = format!(
        r#"{{
  "error": "Route not found",
  "received": {{
    "method": "{}",
    "uri": "{}",
    "path": "{}"
  }},
  "hint": "MCP endpoint is at /mcp. Send 'initialize' request first.",
  "available_endpoints": ["/mcp", "/debug"]
}}"#,
        method,
        uri,
        uri.path()
    );

    (
        StatusCode::NOT_FOUND,
        [("content-type", "application/json")],
        debug_response,
    )
}

/// Debug endpoint that returns server info
async fn debug_handler() -> impl IntoResponse {
    tracing::info!("Debug endpoint hit");

    let info = r#"{
  "server": "plexus-transport",
  "mcp_endpoint": "/mcp",
  "mcp_protocol": "MCP Streamable HTTP (2025-03-26)",
  "notes": [
    "MCP requires 'initialize' request before 'tools/list'",
    "Accept header must include 'application/json, text/event-stream'",
    "Tool names use format: namespace.method (e.g., 'echo.echo')"
  ]
}"#;

    (StatusCode::OK, [("content-type", "application/json")], info)
}

/// Serve MCP HTTP endpoint for any Activation
///
/// Returns a JoinHandle to the server task. The server will run until
/// the task is cancelled or encounters an error.
///
/// `flat_schemas` provides a pre-computed list of all schemas to expose as MCP tools.
/// Pass `Some(hub.list_plugin_schemas())` for hub activations to expose all child schemas.
/// Pass `None` to derive schemas from the activation's own `plugin_schema()`.
///
/// `route_fn` is an optional routing function for hub activations. When provided,
/// `call_tool` uses it to dispatch namespaced method calls (e.g., "loopback.permit")
/// to the correct child activation via `hub.route()`.
pub async fn serve_mcp_http<A: Activation>(
    activation: Arc<A>,
    flat_schemas: Option<Vec<plexus_core::plexus::PluginSchema>>,
    route_fn: Option<RouteFn>,
    config: McpHttpConfig,
    api_key: Option<String>,
) -> Result<JoinHandle<std::result::Result<(), std::io::Error>>> {
    tracing::info!("Starting MCP HTTP transport at http://{}/mcp", config.addr);

    let mut bridge = ActivationMcpBridge::with_server_info_and_schemas(
        activation,
        config.server_name.clone(),
        config.server_version.clone(),
        flat_schemas,
    );
    if let Some(router) = route_fn {
        bridge = bridge.with_router(router);
    }

    // Create session manager based on configuration
    #[cfg(feature = "sqlite-sessions")]
    let mcp_service = match config.session_storage {
        crate::config::SessionStorage::InMemory => {
            let session_manager = LocalSessionManager::new();
            let server_config = StreamableHttpServerConfig::default();
            let bridge_clone = bridge.clone();
            StreamableHttpService::new(
                move || Ok(bridge_clone.clone()),
                session_manager.into(),
                server_config,
            )
        }
        crate::config::SessionStorage::Sqlite { path } => {
            let sqlite_config = SqliteSessionConfig {
                db_path: path,
                ..Default::default()
            };
            let session_manager = SqliteSessionManager::new(sqlite_config)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialize SQLite session manager: {}", e))?;
            let server_config = StreamableHttpServerConfig::default();
            let bridge_clone = bridge.clone();
            StreamableHttpService::new(
                move || Ok(bridge_clone.clone()),
                session_manager.into(),
                server_config,
            )
        }
    };

    #[cfg(not(feature = "sqlite-sessions"))]
    let mcp_service = {
        let session_manager = LocalSessionManager::default();
        let server_config = StreamableHttpServerConfig::default();
        let bridge_clone = bridge.clone();
        StreamableHttpService::new(
            move || Ok(bridge_clone.clone()),
            session_manager.into(),
            server_config,
        )
    };

    // Build axum router with MCP at /mcp, debug endpoint, request logging, and auth
    let mcp_app = Router::new()
        .nest_service("/mcp", mcp_service)
        .route("/debug", any(debug_handler))
        .fallback(fallback_handler)
        .layer(middleware::from_fn(log_request_middleware))
        .layer(middleware::from_fn_with_state(api_key, auth_middleware));

    // Start MCP HTTP server
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    let handle = tokio::spawn(async move { axum::serve(listener, mcp_app).await });

    Ok(handle)
}
