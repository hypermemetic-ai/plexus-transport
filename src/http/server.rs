//! REST HTTP server setup

use anyhow::Result;
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::any, Router,
};
use plexus_core::plexus::{Activation, PluginSchema};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::config::RestHttpConfig;
use crate::http::bridge::{ActivationRestBridge, RouteFn};

/// Middleware to enforce `Authorization: Bearer <key>` on all REST HTTP requests.
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
                "REST HTTP auth rejected: missing or invalid Authorization header (uri={})",
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

    tracing::debug!("REST HTTP {} {}", method, uri);

    let response = next.run(request).await;
    let status = response.status();

    tracing::debug!("REST HTTP {} {} -> {}", method, uri, status);

    response
}

/// Fallback handler for unmatched routes
async fn fallback_handler(request: Request) -> impl IntoResponse {
    let method = request.method().clone();
    let uri = request.uri().clone();

    tracing::warn!(
        "REST HTTP: Unmatched route {} {} - Expected format: POST /rest/{{namespace}}/{{method}}",
        method,
        uri.path()
    );

    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({
            "error": format!("Not found: {} {}", method, uri.path()),
            "hint": "Expected format: POST /rest/{namespace}/{method}"
        }))
    )
}

/// Debug handler - returns server info
async fn debug_handler() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "server": "plexus-rest-http",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "REST HTTP",
        "endpoint_format": "POST /rest/{namespace}/{method}"
    }))
}

/// Start a standalone REST HTTP server
///
/// This function creates an Axum HTTP server that exposes activation methods
/// as REST endpoints at `POST /{namespace}/{method}`.
///
/// ## Arguments
///
/// - `activation`: The activation to expose via REST API
/// - `flat_schemas`: For hub activations, pass `hub.list_plugin_schemas()` to expose all children
/// - `route_fn`: For hub activations, provide a routing function for child dispatch
/// - `config`: REST HTTP server configuration (port, server name/version)
/// - `api_key`: Optional Bearer token for authentication
///
/// ## Returns
///
/// A `JoinHandle` for the server task. Await it to run until shutdown, or detach to run in background.
///
/// ## Example
///
/// ```rust,no_run
/// # use plexus_transport::http::serve_rest_http;
/// # use plexus_transport::config::RestHttpConfig;
/// # use std::sync::Arc;
/// # async fn example() -> anyhow::Result<()> {
/// # let activation = Arc::new(());
/// let config = RestHttpConfig::new(8888);
/// let handle = serve_rest_http(
///     activation,
///     None,  // flat_schemas
///     None,  // route_fn
///     config,
///     None,  // api_key
/// ).await?;
///
/// // Server runs in background
/// # Ok(())
/// # }
/// ```
pub async fn serve_rest_http<A: Activation>(
    activation: Arc<A>,
    flat_schemas: Option<Vec<PluginSchema>>,
    route_fn: Option<RouteFn>,
    config: RestHttpConfig,
    api_key: Option<String>,
) -> Result<JoinHandle<std::result::Result<(), std::io::Error>>> {
    tracing::info!(
        "Starting REST HTTP server at http://{} (server: {}, version: {})",
        config.addr,
        config.server_name,
        config.server_version
    );

    // Create REST bridge
    let bridge = ActivationRestBridge::with_server_info_and_schemas(
        activation,
        Some(config.server_name.clone()),
        Some(config.server_version.clone()),
        flat_schemas,
    );

    // Apply routing function if provided
    let bridge = if let Some(rf) = route_fn {
        bridge.with_router(rf)
    } else {
        bridge
    };

    // Build router
    let rest_router = bridge.into_router();

    // Build main app with middleware
    let app = Router::new()
        .nest("/rest", rest_router)
        .route("/debug", any(debug_handler))
        .fallback(fallback_handler)
        .layer(middleware::from_fn(log_request_middleware))
        .layer(middleware::from_fn_with_state(api_key.clone(), auth_middleware));

    // Start server
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!("REST HTTP server listening on {}", config.addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
    });

    Ok(handle)
}
