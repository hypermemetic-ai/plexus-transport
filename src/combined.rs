//! Combined WebSocket + MCP HTTP server on a single port.
//!
//! Routes incoming connections by path:
//! - `/mcp/*`  → rmcp `StreamableHttpService` (MCP Streamable HTTP)
//! - everything else → jsonrpsee `TowerService` (WebSocket JSON-RPC + HTTP batch)
//!
//! Both transports share one `TcpListener` bound to the configured address.
//! WebSocket upgrades work because jsonrpsee's `TowerService` is served via
//! `serve_with_graceful_shutdown`, which uses `hyper_util`'s
//! `serve_connection_with_upgrades` under the hood.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{body::Body as AxumBody, Router};
use hyper::body::Incoming;
use jsonrpsee::{
    RpcModule,
    server::{Server, ServerHandle, serve_with_graceful_shutdown, stop_channel},
};
use plexus_core::plexus::{Activation, PluginSchema};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};
use futures::FutureExt;
use tokio::net::TcpListener;
use tower::{Service, ServiceExt};

use axum::extract::State;
use axum::middleware::{self as axum_middleware, Next as AxumNext};
use axum::response::IntoResponse as AxumIntoResponse;

use crate::mcp::bridge::{ActivationMcpBridge, RouteFn};

/// Serve WebSocket JSON-RPC and MCP HTTP on the **same** port.
///
/// - `GET /` (with `Upgrade: websocket`) and all other non-`/mcp` paths →
///   jsonrpsee WebSocket + HTTP JSON-RPC
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
) -> Result<ServerHandle>
where
    A: Activation + Send + Sync + 'static,
{
    // ── MCP side ────────────────────────────────────────────────────────────
    let mut bridge = ActivationMcpBridge::with_server_info_and_schemas(
        activation,
        None,
        None,
        flat_schemas,
    );
    if let Some(rf) = route_fn {
        bridge = bridge.with_router(rf);
    }

    let session_manager = LocalSessionManager::default();
    let bridge_clone = bridge.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(bridge_clone.clone()),
        session_manager.into(),
        StreamableHttpServerConfig::default(),
    );

    // Axum router — only intercepts /mcp; all other requests fall through to
    // jsonrpsee via the else branch in the per-connection service_fn.
    // Auth middleware is applied here so that WebSocket upgrades (handled by
    // jsonrpsee below) are also protected by the check inside serve_websocket.
    let mcp_router: Router = Router::new()
        .nest_service("/mcp", mcp_service)
        .layer(axum_middleware::from_fn_with_state(
            api_key.clone(),
            combined_auth_middleware,
        ));

    // ── JSON-RPC / WebSocket side ────────────────────────────────────────────
    let (stop_handle, server_handle) = stop_channel();
    let svc_builder = Server::builder().to_service_builder();
    let methods = jsonrpsee::Methods::from(module);

    // ── Shared TCP listener ──────────────────────────────────────────────────
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Starting WebSocket transport at ws://{}", addr);
    tracing::info!("Starting MCP HTTP transport at http://{}/mcp", addr);

    let stop = stop_handle.clone();
    // Pre-compute the expected Authorization header value for WebSocket path checks.
    let ws_auth_header: Option<String> = api_key.map(|k| format!("Bearer {}", k));

    tokio::spawn(async move {
        loop {
            let (sock, _peer) = tokio::select! {
                res = listener.accept() => match res {
                    Ok(x) => x,
                    Err(e) => { tracing::error!("combined accept: {}", e); continue; }
                },
                _ = stop.clone().shutdown() => break,
            };

            let svc_b = svc_builder.clone();
            let m = methods.clone();
            let stop2 = stop.clone();
            let mcp = mcp_router.clone();
            let ws_auth = ws_auth_header.clone();

            tokio::spawn(async move {
                // Clone stop2 before moving into the closure, for the shutdown future.
                let stop_for_serve = stop2.clone();

                // Per-connection service — routes each request by path.
                let svc = tower::service_fn(move |req: http::Request<Incoming>| {
                    let mcp = mcp.clone();
                    let svc_b = svc_b.clone();
                    let m = m.clone();
                    let stop2 = stop2.clone();
                    let ws_auth = ws_auth.clone();

                    async move {
                        if req.uri().path().starts_with("/mcp") {
                            // Axum expects Request<AxumBody>; wrap Incoming.
                            let (parts, body) = req.into_parts();
                            let axum_req =
                                http::Request::from_parts(parts, AxumBody::new(body));

                            // Router<()> returns Response<AxumBody> with Infallible error.
                            mcp.oneshot(axum_req)
                                .await
                                .map_err(|e| anyhow::anyhow!("{e}"))
                        } else {
                            // For non-MCP requests (WebSocket upgrades and JSON-RPC HTTP):
                            // validate the auth header before handing off to jsonrpsee.
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

                            // TowerService is generic over RequestBody; pass Incoming directly.
                            let mut rpc_svc = svc_b.build(m, stop2);

                            rpc_svc
                                .call(req)
                                .await
                                .map(|resp: http::Response<_>| resp.map(AxumBody::new))
                                .map_err(anyhow::Error::from_boxed)
                        }
                    }
                    .boxed()
                });

                if let Err(e) =
                    serve_with_graceful_shutdown(sock, svc, stop_for_serve.shutdown()).await
                {
                    tracing::debug!("combined connection closed: {}", e);
                }
            });
        }
    });

    Ok(server_handle)
}
