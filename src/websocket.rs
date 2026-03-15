//! WebSocket transport - JSON-RPC over WebSocket

use anyhow::Result;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::RpcModule;

use crate::config::WebSocketConfig;

/// Serve RPC module over WebSocket
///
/// Starts a WebSocket server that accepts JSON-RPC requests.
/// When `config.api_key` is set and the `mcp-gateway` feature is enabled,
/// the HTTP upgrade request must carry `Authorization: Bearer <key>` or the
/// connection is rejected with 401.
/// Returns a handle that can be used to stop the server.
pub async fn serve_websocket(
    module: RpcModule<()>,
    config: WebSocketConfig,
) -> Result<ServerHandle> {
    tracing::info!("Starting WebSocket transport at ws://{}", config.addr);

    #[cfg(feature = "mcp-gateway")]
    if let Some(key) = config.api_key {
        let expected = format!("Bearer {}", key);
        let middleware = tower::ServiceBuilder::new().layer_fn(move |service| {
            AuthMiddleware { service, expected: expected.clone() }
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
// Bearer-token middleware for jsonrpsee's HTTP upgrade path
// (only compiled when the mcp-gateway feature is active)
// ---------------------------------------------------------------------------

#[cfg(feature = "mcp-gateway")]
mod auth {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http_body::Body as HttpBody;
    use tower::Service;

    type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type HttpRequest<B> = http::Request<B>;
    type HttpResponse = http::Response<jsonrpsee::server::HttpBody>;

    /// Tower middleware layer that checks `Authorization: Bearer <key>` on every
    /// incoming HTTP request (including WebSocket upgrade requests).
    #[derive(Clone)]
    pub(super) struct AuthMiddleware<S> {
        pub(super) service: S,
        pub(super) expected: String,
    }

    impl<S, B> Service<HttpRequest<B>> for AuthMiddleware<S>
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

        fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
            // Validate the Authorization header before forwarding the request.
            let auth_ok = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(|v| v == self.expected)
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

            let fut = self.service.call(request);
            Box::pin(async move { fut.await.map_err(Into::into) })
        }
    }
}

#[cfg(feature = "mcp-gateway")]
use auth::AuthMiddleware;
