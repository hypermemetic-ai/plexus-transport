//! Combined WebSocket + MCP HTTP server on a single port.
//!
//! Routes incoming connections by path:
//! - `/mcp/*`  → rmcp `StreamableHttpService` (MCP Streamable HTTP)
//! - `/rest/*` → the REST bridge (feature `http-gateway`)
//! - everything else → the WebSocket (PERF-20: plexus-transport's own loop,
//!   [`crate::ws`]); a request there that is not an upgrade is `426` —
//!   jsonrpsee-server's plain-HTTP JSON-RPC catch-all is gone.
//!
//! Both transports share one `TcpListener` bound to the configured address.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{body::Body as AxumBody, Router};
use hyper::body::Incoming;
use jsonrpsee::RpcModule;
use plexus_core::plexus::{Activation, PluginSchema};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};
use futures::FutureExt;
use tokio::net::TcpListener;
use tower::ServiceExt;

use crate::ws::ServerHandle;

use axum::extract::State;
use axum::middleware::{self as axum_middleware, Next as AxumNext};
use axum::response::IntoResponse as AxumIntoResponse;

use crate::mcp::bridge::{ActivationMcpBridge, RouteFn};

/// Serve WebSocket JSON-RPC and MCP HTTP on the **same** port.
///
/// - `GET /` (with `Upgrade: websocket`) and all other non-`/mcp` paths →
///   the WebSocket (anything there but an upgrade is `426`)
/// - `POST /mcp`, `GET /mcp` → rmcp Streamable HTTP (MCP 2025-03-26)
///
/// The returned `ServerHandle` can be awaited via `handle.stopped()` and
/// stopped via `handle.stop()`.
/// Axum middleware that validates the `Authorization: Bearer <key>` header.
///
/// State is `Option<String>` — when `None`, all requests pass through.
async fn combined_auth_middleware(
    State(api_key): State<Option<String>>,
    request: http::Request<axum::body::Body>,
    next: AxumNext,
) -> impl AxumIntoResponse {
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
                "Combined server auth rejected: missing or invalid Authorization header (uri={})",
                request.uri()
            );
            return (
                http::StatusCode::UNAUTHORIZED,
                [(
                    http::header::WWW_AUTHENTICATE,
                    http::HeaderValue::from_static("Bearer realm=\"plexus\""),
                )],
                "Unauthorized",
            )
                .into_response();
        }
    }
    next.run(request).await.into_response()
}

pub async fn serve_combined<A>(
    module: RpcModule<()>,
    activation: Arc<A>,
    flat_schemas: Option<Vec<PluginSchema>>,
    route_fn: Option<RouteFn>,
    addr: SocketAddr,
    api_key: Option<String>,
    enable_rest: bool,
) -> Result<ServerHandle>
where
    A: Activation + Send + Sync + 'static,
{
    // ── MCP side ────────────────────────────────────────────────────────────
    let mut bridge = ActivationMcpBridge::with_server_info_and_schemas(
        activation.clone(),
        None,
        None,
        flat_schemas.clone(),
    );
    if let Some(rf) = route_fn.clone() {
        bridge = bridge.with_router(rf);
    }

    let session_manager = LocalSessionManager::default();
    let bridge_clone = bridge.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(bridge_clone.clone()),
        session_manager.into(),
        StreamableHttpServerConfig::default(),
    );

    // ── REST side (optional) ───────────────────────────────────────────────────
    #[cfg(feature = "http-gateway")]
    let rest_router_opt = if enable_rest {
        let mut rest_bridge = crate::http::bridge::ActivationRestBridge::with_server_info_and_schemas(
            activation.clone(),
            None,
            None,
            flat_schemas.clone(),
        );
        if let Some(rf) = route_fn {
            rest_bridge = rest_bridge.with_router(rf);
        }
        Some(rest_bridge.into_router())
    } else {
        None
    };

    #[cfg(not(feature = "http-gateway"))]
    let rest_router_opt: Option<Router> = None;

    // Axum router — intercepts /mcp and optionally /rest; all other requests fall through to
    // the WebSocket upgrade via the else branch in the per-connection service.
    let mut mcp_router: Router = Router::new()
        .nest_service("/mcp", mcp_service);

    #[cfg(feature = "http-gateway")]
    if let Some(rest_router) = rest_router_opt {
        mcp_router = mcp_router.nest("/rest", rest_router);
    }

    let mcp_router = mcp_router.layer(axum_middleware::from_fn_with_state(
        api_key.clone(),
        combined_auth_middleware,
    ));

    // ── JSON-RPC / WebSocket side ────────────────────────────────────────────
    crate::ws::cap_frame_logs();
    let (stop_handle, server_handle) = crate::ws::stop_channel();
    let limits = crate::ws::SocketLimits::default();
    let shared = crate::ws::Shared::new(
        jsonrpsee::Methods::from(module),
        limits.clone(),
        None,
        crate::ConnectionQuotas::default().max_connections,
    );
    let upgrade = crate::ws::Upgrade { shared, stop: stop_handle.clone() };

    // ── Shared TCP listener ──────────────────────────────────────────────────
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Starting WebSocket transport at ws://{}", addr);
    tracing::info!("Starting MCP HTTP transport at http://{}/mcp", addr);
    #[cfg(feature = "http-gateway")]
    if enable_rest {
        tracing::info!("Starting REST HTTP transport at http://{}/rest", addr);
    }

    // Pre-compute the expected Authorization header value for WebSocket path checks.
    let ws_auth_header: Option<String> = api_key.map(|k| format!("Bearer {}", k));

    tokio::spawn(async move {
        let shutdown = stop_handle.clone().shutdown();
        tokio::pin!(shutdown);
        loop {
            let (sock, peer) = tokio::select! {
                res = listener.accept() => match res {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!("combined accept: {}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                },
                _ = &mut shutdown => break,
            };
            if let Err(e) = sock.set_nodelay(true) {
                tracing::debug!("combined set_nodelay: {e}");
            }
            let conn = Arc::new(crate::websocket::Connection::new(peer, crate::CALLS_PER_CONNECTION, None));
            let (mcp, upgrade, ws_auth) = (mcp_router.clone(), upgrade.clone(), ws_auth_header.clone());

            // Per-connection service — routes each request by path.
            let svc = tower::service_fn(move |req: http::Request<Incoming>| {
                let (mcp, upgrade, ws_auth) = (mcp.clone(), upgrade.clone(), ws_auth.clone());
                async move {
                    if req.uri().path().starts_with("/mcp") || req.uri().path().starts_with("/rest") {
                        // Axum expects Request<AxumBody>; wrap Incoming.
                        let (parts, body) = req.into_parts();
                        let axum_req = http::Request::from_parts(parts, AxumBody::new(body));
                        // Router<()> returns Response<AxumBody> with Infallible error.
                        return mcp.oneshot(axum_req).await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) });
                    }
                    // Everything else is the WebSocket: the auth header
                    // checked first, then the upgrade (or 426).
                    if let Some(ref expected) = ws_auth {
                        let ok = req
                            .headers()
                            .get(http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == expected)
                            .unwrap_or(false);
                        if !ok {
                            tracing::warn!(
                                "WebSocket auth rejected: missing or invalid Authorization header (uri={})",
                                req.uri()
                            );
                            let resp = http::Response::builder()
                                .status(http::StatusCode::UNAUTHORIZED)
                                .header(http::header::WWW_AUTHENTICATE, "Bearer realm=\"plexus\"")
                                .header(http::header::CONTENT_TYPE, "text/plain")
                                .body(AxumBody::from("Unauthorized"))
                                .expect("static response is valid");
                            return Ok(resp);
                        }
                    }
                    Ok(upgrade.upgrade(req).map(AxumBody::new))
                }
                .boxed()
            });
            tokio::spawn(crate::ws::serve_http(sock, conn, svc, stop_handle.clone(), limits.header_timeout));
        }
    });

    Ok(server_handle)
}
