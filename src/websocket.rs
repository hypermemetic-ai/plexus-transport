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
    reject_on_session_failure: bool,
) -> Result<ServerHandle> {
    tracing::info!("Starting WebSocket transport at ws://{}", config.addr);

    let has_bearer = config.api_key.is_some();
    let has_session = session_validator.is_some();

    if has_bearer || has_session {
        let expected_bearer = config.api_key.map(|key| format!("Bearer {}", key));
        let middleware = tower::ServiceBuilder::new().layer_fn(move |service| {
            CombinedAuthMiddleware {
                service,
                expected_bearer: expected_bearer.clone(),
                session_validator: session_validator.clone(),
                reject_on_session_failure,
            }
        });
        let server = Server::builder()
            .set_http_middleware(middleware)
            .build(config.addr)
            .await?;
        let handle = server.start(module);
        return Ok(handle);
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
        /// RED-9: when `true`, reject the WS upgrade with HTTP 401 if the
        /// session validator returns `None` OR no cookie is present.
        /// When `false` (default, backward-compat), the middleware logs and
        /// passes through with no AuthContext — methods with `#[from_auth]`
        /// will fail-close at runtime, but methods without it dispatch.
        pub(super) reject_on_session_failure: bool,
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
            let service = self.service.clone();

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

            // Extract and validate session if validator is configured.
            // Tokens MUST be carried in the Cookie header — query parameters are
            // rejected because they leak to logs, browser history, and Referer headers.
            let session_validator = self.session_validator.clone();
            let reject_on_failure = self.reject_on_session_failure;
            if let Some(validator) = session_validator {
                let cookie_str = request.headers()
                    .get(http::header::COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                if let Some(cookies) = cookie_str {
                    let mut service = service;
                    return Box::pin(async move {
                        let auth_ctx = validator.validate(&cookies).await;

                        match auth_ctx {
                            Some(ctx) => {
                                tracing::debug!("Auth resolved for user: {}", ctx.user_id);
                                request.extensions_mut().insert(Arc::new(ctx));
                                service.call(request).await.map_err(Into::into)
                            }
                            None if reject_on_failure => {
                                // RED-9: cookie present but validation failed AND opt-in
                                // strict mode is enabled — refuse the upgrade now instead
                                // of letting the request reach the dispatch layer with
                                // auth=None.
                                tracing::warn!(
                                    "WS upgrade rejected: session validation failed (uri={})",
                                    request.uri()
                                );
                                let resp = http::Response::builder()
                                    .status(http::StatusCode::UNAUTHORIZED)
                                    .header(http::header::CONTENT_TYPE, "text/plain")
                                    .body(jsonrpsee::server::HttpBody::from(
                                        "Unauthorized: session invalid or expired"
                                    ))
                                    .expect("static response is valid");
                                Ok(resp)
                            }
                            None => {
                                // Backward-compat path: log and proceed; per-method
                                // #[from_auth] still fail-closes at dispatch.
                                tracing::warn!(
                                    "Cookie present but validation failed, proceeding without auth (uri={})",
                                    request.uri()
                                );
                                service.call(request).await.map_err(Into::into)
                            }
                        }
                    });
                }
                if reject_on_failure {
                    // RED-9: no cookie at all + opt-in strict mode → reject upgrade.
                    tracing::warn!(
                        "WS upgrade rejected: no session cookie present (uri={})",
                        request.uri()
                    );
                    let resp = http::Response::builder()
                        .status(http::StatusCode::UNAUTHORIZED)
                        .header(http::header::CONTENT_TYPE, "text/plain")
                        .body(jsonrpsee::server::HttpBody::from(
                            "Unauthorized: session cookie required"
                        ))
                        .expect("static response is valid");
                    return Box::pin(async move { Ok(resp) });
                }
                tracing::warn!(
                    "No cookie present, proceeding without auth (uri={})",
                    request.uri()
                );
            }

            // No auth configured - pass through
            let mut service = service;
            Box::pin(async move { service.call(request).await.map_err(Into::into) })
        }
    }
}

use auth::CombinedAuthMiddleware;
