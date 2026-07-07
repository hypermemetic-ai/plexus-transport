//! REST HTTP server setup

use anyhow::Result;
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::any, Router,
};
use plexus_core::plexus::{Activation, AuthContext, PluginSchema, SessionValidator};
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

/// Strip the RFC-6750 `Bearer ` prefix (case-sensitive) from an
/// `Authorization` header value. Returns `None` for any value that does not
/// begin with the literal `Bearer ` (note the trailing space).
fn strip_bearer(value: &str) -> Option<&str> {
    value.strip_prefix("Bearer ")
}

/// PLX-16: REST session-auth middleware.
///
/// Resolves a per-user [`AuthContext`] from the request and stashes it in the
/// request extensions as `Arc<AuthContext>`, where `rest_method_handler`
/// picks it up and threads it into the 4-arg `Activation::call`. Mirrors the
/// WebSocket `CombinedAuthMiddleware` dynamic-identity step: **Cookie wins,
/// then `Authorization: Bearer`** — the first input that produces
/// `Some(AuthContext)` is used.
///
/// When no `SessionValidator` is configured, or no credential validates, no
/// extension is inserted and dispatch proceeds anonymously (`auth = None`) —
/// `#[from_auth]` methods fail-close at the macro boundary. The static
/// `Authorization: Bearer <api_key>` admission gate (`auth_middleware`) is a
/// separate concern and is unaffected.
async fn rest_session_auth(
    axum::extract::State(validator): axum::extract::State<Option<Arc<dyn SessionValidator>>>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(validator) = validator {
        let cookie_value = request
            .headers()
            .get(http::header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bearer_token = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(strip_bearer)
            .map(|s| s.to_string());

        // Cookie wins, then Bearer (AUTHZ-BEARER-S01-output §4.3 parity).
        let mut auth_ctx: Option<AuthContext> = None;
        if let Some(ref cookies) = cookie_value {
            auth_ctx = validator.validate(cookies).await;
        }
        if auth_ctx.is_none() {
            if let Some(ref bearer) = bearer_token {
                auth_ctx = validator.validate(bearer).await;
            }
        }

        if let Some(ctx) = auth_ctx {
            tracing::debug!(user_id = %ctx.user_id, "REST: SessionValidator produced AuthContext");
            request.extensions_mut().insert(Arc::new(ctx));
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

/// Assemble the REST app: the bridge router under `/rest`, the `/debug`
/// route, and the middleware stack.
///
/// Layer order (axum applies `.layer` outermost-last, so the last one added
/// runs first on the way in): the static api_key admission gate runs first,
/// then request logging, then PLX-16 session-auth resolves the per-user
/// identity into request extensions before the handler dispatches.
///
/// Extracted from [`serve_rest_http`] so PLX-16 gateway tests can drive the
/// exact production wiring via `tower::ServiceExt::oneshot` without binding a
/// socket.
pub(crate) fn assemble_rest_app(
    rest_router: Router,
    api_key: Option<String>,
    session_validator: Option<Arc<dyn SessionValidator>>,
) -> Router {
    Router::new()
        .nest("/rest", rest_router)
        .route("/debug", any(debug_handler))
        .fallback(fallback_handler)
        .layer(middleware::from_fn_with_state(session_validator, rest_session_auth))
        .layer(middleware::from_fn(log_request_middleware))
        .layer(middleware::from_fn_with_state(api_key, auth_middleware))
}

/// Test-only hooks for PLX-16 REST gateway auth-threading tests.
///
/// `#[doc(hidden)]` — not part of the public API. Lets integration tests
/// build the production REST app (bridge + middleware stack) and drive it
/// directly, so they assert that a validated identity reaches the handler.
#[doc(hidden)]
pub mod testing {
    use super::*;
    use crate::http::bridge::ActivationRestBridge;

    /// Build the full REST app for an activation, wiring the api_key gate and
    /// (optionally) a `SessionValidator`. Mirrors [`serve_rest_http`] minus
    /// the socket bind.
    pub fn build_rest_app<A: Activation>(
        activation: Arc<A>,
        flat_schemas: Option<Vec<PluginSchema>>,
        route_fn: Option<RouteFn>,
        api_key: Option<String>,
        session_validator: Option<Arc<dyn SessionValidator>>,
    ) -> Router {
        let bridge = ActivationRestBridge::with_server_info_and_schemas(
            activation,
            Some("test".to_string()),
            Some("0.0.0".to_string()),
            flat_schemas,
        );
        let bridge = if let Some(rf) = route_fn {
            bridge.with_router(rf)
        } else {
            bridge
        };
        assemble_rest_app(bridge.into_router(), api_key, session_validator)
    }
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
/// # let activation = Arc::new(plexus_core::plexus::DynamicHub::new("example"));
/// let config = RestHttpConfig::new(8888);
/// let handle = serve_rest_http(
///     activation,
///     None,  // flat_schemas
///     None,  // route_fn
///     config,
///     None,  // api_key
///     None,  // session_validator
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
    session_validator: Option<Arc<dyn SessionValidator>>,
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

    // Assemble the full app (middleware stack). Extracted so PLX-16 gateway
    // tests can drive the exact production wiring via `tower::oneshot`.
    let app = assemble_rest_app(rest_router, api_key.clone(), session_validator);

    // Start server
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!("REST HTTP server listening on {}", config.addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
    });

    Ok(handle)
}
