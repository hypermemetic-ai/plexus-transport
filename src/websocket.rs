//! WebSocket transport - JSON-RPC over WebSocket

use anyhow::Result;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::RpcModule;
use std::sync::Arc;

use crate::config::WebSocketConfig;

/// Serve RPC module over WebSocket
///
/// Starts a WebSocket server that accepts JSON-RPC requests.
/// When `config.api_key` is set and the `mcp-gateway` feature is enabled,
/// the HTTP upgrade request must carry `Authorization: Bearer <key>` or the
/// connection is rejected with 401.
///
/// When `session_validator` is provided, the server will:
/// - Extract cookies from the HTTP upgrade request
/// - Validate them using the SessionValidator
/// - Store the resulting AuthContext in request Extensions for use by RPC methods
///
/// Returns a handle that can be used to stop the server.
pub async fn serve_websocket(
    module: RpcModule<()>,
    config: WebSocketConfig,
    session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
) -> Result<ServerHandle> {
    tracing::info!("Starting WebSocket transport at ws://{}", config.addr);

    #[cfg(feature = "mcp-gateway")]
    {
        let has_bearer = config.api_key.is_some();
        let has_cookies = session_validator.is_some();

        if has_bearer || has_cookies {
            let expected_bearer = config.api_key.map(|key| format!("Bearer {}", key));
            let middleware = tower::ServiceBuilder::new().layer_fn(move |service| {
                CombinedAuthMiddleware {
                    service,
                    expected_bearer: expected_bearer.clone(),
                    session_validator: session_validator.clone(),
                }
            });
            let server = Server::builder()
                .set_http_middleware(middleware)
                .build(config.addr)
                .await?;
            let handle = server.start(module);
            return Ok(handle);
        }
    }

    let server = Server::builder().build(config.addr).await?;
    let handle = server.start(module);
    Ok(handle)
}

// ---------------------------------------------------------------------------
// Combined auth middleware for jsonrpsee's HTTP upgrade path
// Supports both Bearer tokens (for API keys) and Cookies (for session auth)
// (only compiled when the mcp-gateway feature is active)
// ---------------------------------------------------------------------------

#[cfg(feature = "mcp-gateway")]
mod auth {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http_body::Body as HttpBody;
    use tower::Service;

    type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type HttpRequest<B> = http::Request<B>;
    type HttpResponse = http::Response<jsonrpsee::server::HttpBody>;

    /// Tower middleware layer that handles both Bearer token and Cookie authentication.
    ///
    /// - If `expected_bearer` is set: validates Authorization header
    /// - If `session_validator` is set: validates Cookie header and stores AuthContext in Extensions
    /// - Both can be enabled simultaneously (Bearer for API access, Cookies for browser sessions)
    #[derive(Clone)]
    pub(super) struct CombinedAuthMiddleware<S> {
        pub(super) service: S,
        pub(super) expected_bearer: Option<String>,
        pub(super) session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
    }

    impl<S, B> Service<HttpRequest<B>> for CombinedAuthMiddleware<S>
    where
        S: Service<HttpRequest<B>, Response = HttpResponse> + Clone + Send + 'static,
        S::Error: Into<BoxError> + Send + 'static,
        S::Future: Send + 'static,
        B: HttpBody<Data = Bytes> + Send + std::fmt::Debug + 'static,
        B::Data: Send,
        B::Error: Into<BoxError>,
    {
        type Response = HttpResponse;
        type Error = BoxError;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, mut request: HttpRequest<B>) -> Self::Future {
            // Check Bearer token if configured
            if let Some(ref expected) = self.expected_bearer {
                let auth_ok = request
                    .headers()
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v == expected)
                    .unwrap_or(false);

                if !auth_ok {
                    tracing::warn!(
                        "WebSocket auth rejected: missing or invalid Authorization header (uri={})",
                        request.uri()
                    );
                    let resp = http::Response::builder()
                        .status(http::StatusCode::UNAUTHORIZED)
                        .header(http::header::WWW_AUTHENTICATE, "Bearer realm=\"plexus\"")
                        .header(http::header::CONTENT_TYPE, "text/plain")
                        .body(jsonrpsee::server::HttpBody::from("Unauthorized"))
                        .expect("static response is valid");
                    return Box::pin(async move { Ok(resp) });
                }
            }

            // Extract and validate cookies if validator is configured
            let session_validator = self.session_validator.clone();
            if let Some(validator) = session_validator.clone() {
                if let Some(cookie_header) = request.headers().get(http::header::COOKIE) {
                    if let Ok(cookie_str) = cookie_header.to_str() {
                        // Spawn async validation task
                        let cookie_str_owned = cookie_str.to_string();
                        return Box::pin(async move {
                            // Validate cookie and get auth context
                            if let Some(auth_ctx) = validator.validate(&cookie_str_owned).await {
                                tracing::debug!("Cookie validation successful for user: {}", auth_ctx.user_id);
                                // Store AuthContext in request Extensions for RPC handlers
                                // Extensions are propagated through jsonrpsee to the RPC methods
                                request.extensions_mut().insert(Arc::new(auth_ctx));
                            } else {
                                tracing::debug!("Cookie validation failed, proceeding without auth");
                                // Cookie invalid - proceed without auth (methods can handle anonymous access)
                            }

                            // Forward the request with modified extensions
                            let mut service = self.service.clone();
                            service.call(request).await.map_err(Into::into)
                        });
                    }
                }
                tracing::debug!("No cookie header present, proceeding without auth");
            }

            // No auth required or no cookie present - pass through
            let fut = self.service.call(request);
            Box::pin(async move { fut.await.map_err(Into::into) })
        }
    }
}

#[cfg(feature = "mcp-gateway")]
use auth::CombinedAuthMiddleware;
