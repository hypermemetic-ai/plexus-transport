//! WebSocket transport - JSON-RPC over WebSocket
//!
//! ## Authentication (AUTHZ-BEARER-1)
//!
//! The WS upgrade middleware enforces two independent auth concerns on
//! separate headers:
//!
//! 1. **Static admission gate** (`api_key`): when configured, the upgrade
//!    request MUST carry the configured api_key header (default
//!    `X-Plexus-API-Key`) with the matching value, or the upgrade is
//!    rejected with HTTP 401. The `Authorization` header is NOT consulted by
//!    the static gate (one exception: a deprecated v1 compat shim, see
//!    below).
//! 2. **Dynamic identity** (`SessionValidator`): when configured, an
//!    `AuthContext` is produced from one of two inputs (cookie wins, then
//!    Bearer):
//!    - `Cookie` header → `SessionValidator::validate(cookie_str)`
//!    - `Authorization: Bearer <token>` → `SessionValidator::validate(token)`
//!
//! The two layers compose: when both `api_key` and `SessionValidator` are
//! configured, the static gate fires first; only after it passes is the
//! `SessionValidator` consulted.
//!
//! ### v1 compat shim (deprecated, single-release window)
//!
//! When `api_key` is configured AND the configured api_key header is absent
//! AND `Authorization: Bearer <value>` matches the configured api_key
//! exactly AND `SessionValidator` is `None`, the request is accepted and a
//! `tracing::warn!` deprecation notice is emitted. The compat shim is OFF
//! whenever `SessionValidator` is configured, to prevent re-entry of the
//! header-conflation defect this ticket exists to remove.

use anyhow::Result;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::RpcModule;
use std::sync::Arc;

use crate::config::WebSocketConfig;

/// Serve RPC module over WebSocket
///
/// Starts a WebSocket server that accepts JSON-RPC requests.
///
/// Authentication, when configured, follows the AUTHZ-BEARER-1 contract:
/// - `config.api_key` + `config.api_key_header` enforce a static admission
///   gate on the configured header (default `X-Plexus-API-Key`).
/// - `session_validator`, when provided, produces an `AuthContext` from the
///   `Cookie` header (preferred) or the `Authorization: Bearer` header
///   (fallback).
///
/// The two layers compose; see the module docs for the full behavior table
/// (also documented in AUTHZ-BEARER-S01-output §4).
///
/// Returns a handle that can be used to stop the server.
pub async fn serve_websocket(
    module: RpcModule<()>,
    config: WebSocketConfig,
    session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
    reject_on_session_failure: bool,
) -> Result<ServerHandle> {
    tracing::info!("Starting WebSocket transport at ws://{}", config.addr);

    let has_api_key = config.api_key.is_some();
    let has_session = session_validator.is_some();

    if has_api_key || has_session {
        let api_key = config.api_key.clone();
        let api_key_header = config.api_key_header.clone();
        let middleware = tower::ServiceBuilder::new().layer_fn(move |service| {
            CombinedAuthMiddleware {
                service,
                api_key: api_key.clone(),
                api_key_header: api_key_header.clone(),
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
// AUTHZ-BEARER-1: separates the static api_key admission gate (configurable
// header, default X-Plexus-API-Key) from the SessionValidator path which
// consumes Cookie and/or Authorization: Bearer.
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

    /// Tower middleware layer for AUTHZ-BEARER-1 WS upgrade authentication.
    ///
    /// Layers, in order:
    /// 1. Static api_key admission gate on the configured header (default
    ///    `X-Plexus-API-Key`). When `api_key` is `Some`, the upgrade is
    ///    rejected unless the header carries the matching value.
    /// 2. SessionValidator dynamic identity, when configured. Tries cookie
    ///    first, then `Authorization: Bearer`. The first input that
    ///    produces `Some(AuthContext)` wins.
    ///
    /// See module docs for full behavior table.
    #[derive(Clone)]
    pub(super) struct CombinedAuthMiddleware<S> {
        pub(super) service: S,
        /// AUTHZ-BEARER-1: static admission key. When `Some`, every upgrade
        /// must carry the configured header with this value.
        pub(super) api_key: Option<String>,
        /// AUTHZ-BEARER-1: header that carries the api_key
        /// (default `X-Plexus-API-Key`).
        pub(super) api_key_header: http::HeaderName,
        pub(super) session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
        /// RED-9: when `true`, reject the WS upgrade with HTTP 401 if the
        /// session validator returns `None` OR no credential input was
        /// present. When `false` (default, backward-compat), the middleware
        /// logs and passes through with no AuthContext — methods with
        /// `#[from_auth]` will fail-close at runtime, but methods without it
        /// dispatch.
        pub(super) reject_on_session_failure: bool,
    }

    /// Helper: build a 401 response with the substrate-wire-protocol
    /// `WWW-Authenticate: Bearer realm="plexus"` header preserved.
    fn unauthorized(body: &'static str) -> HttpResponse {
        http::Response::builder()
            .status(http::StatusCode::UNAUTHORIZED)
            .header(http::header::WWW_AUTHENTICATE, "Bearer realm=\"plexus\"")
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body(jsonrpsee::server::HttpBody::from(body))
            .expect("static response is valid")
    }

    /// Strip `Bearer ` prefix per RFC 6750 (case-sensitive prefix).
    /// Returns `None` for any value that does not begin with the literal
    /// ASCII `Bearer ` (note the trailing space).
    fn strip_bearer(value: &str) -> Option<&str> {
        value.strip_prefix("Bearer ")
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

            // ── Step 1: static api_key admission gate ──────────────────────
            // AUTHZ-BEARER-1: read from the configured api_key header, NOT
            // from `Authorization`. Compat shim (deprecated, off when
            // SessionValidator is configured) accepts a Bearer-as-api-key
            // fallback to support the v0 wire shape during the migration
            // window.
            if let Some(ref expected) = self.api_key {
                let header_value = request
                    .headers()
                    .get(&self.api_key_header)
                    .and_then(|v| v.to_str().ok());

                match header_value {
                    Some(v) if v == expected => {
                        // api_key gate passed — fall through to SessionValidator
                        tracing::debug!(
                            header = %self.api_key_header,
                            "WS upgrade: static api_key gate matched"
                        );
                    }
                    Some(_) => {
                        // Header present but value mismatched. Per AUTHZ-BEARER-1
                        // §"Required behavior" step 1: reject with "api key invalid".
                        tracing::warn!(
                            uri = %request.uri(),
                            header = %self.api_key_header,
                            "WS upgrade rejected: api key invalid"
                        );
                        return Box::pin(async move {
                            Ok(unauthorized("Unauthorized: api key invalid"))
                        });
                    }
                    None => {
                        // Configured header is absent. Try the v1 compat shim
                        // ONLY when SessionValidator is unconfigured — per
                        // AUTHZ-BEARER-S01-output §5.2, the compat shim is
                        // OFF whenever a SessionValidator is wired, to
                        // prevent re-entry of the header-conflation defect.
                        let compat_attempted = self.session_validator.is_none()
                            && request
                                .headers()
                                .get(http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .and_then(strip_bearer)
                                .map(|v| v == expected)
                                .unwrap_or(false);

                        if compat_attempted {
                            tracing::warn!(
                                target: "plexus_transport::auth_compat_shim",
                                configured_header = %self.api_key_header,
                                auth_compat_shim_fires_total = 1u64,
                                "deprecated: Bearer-as-api-key compatibility shim fired; \
                                 migrate to {} header",
                                self.api_key_header,
                            );
                            // Compat-shim accepted the request as if the
                            // api_key header had matched. Fall through.
                        } else {
                            tracing::warn!(
                                uri = %request.uri(),
                                header = %self.api_key_header,
                                "WS upgrade rejected: api key required"
                            );
                            return Box::pin(async move {
                                Ok(unauthorized("Unauthorized: api key required"))
                            });
                        }
                    }
                }
            }

            // ── Step 2: SessionValidator (when configured) ─────────────────
            // AUTHZ-BEARER-1: try Cookie first; on no-Some, try
            // Authorization: Bearer. The first to return Some(AuthContext)
            // wins. Per the contract, SessionValidator is NEVER fed the
            // api_key (the Authorization header is read here exclusively for
            // user-identity tokens).
            let session_validator = self.session_validator.clone();
            let reject_on_failure = self.reject_on_session_failure;

            if let Some(validator) = session_validator {
                // Snapshot the two candidate inputs before moving the request
                // into the async block. We pass the Cookie header value
                // verbatim (validator disambiguates) and the bare token from
                // `Authorization: Bearer <token>` (RFC-6750 prefix stripped).
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

                let attempted_input =
                    cookie_value.is_some() || bearer_token.is_some();

                let mut service = service;
                return Box::pin(async move {
                    let mut auth_ctx: Option<plexus_core::plexus::AuthContext> = None;

                    // Cookie wins per AUTHZ-BEARER-S01-output §4.3.
                    if let Some(ref cookies) = cookie_value {
                        auth_ctx = validator.validate(cookies).await;
                    }

                    if auth_ctx.is_none() {
                        if let Some(ref bearer) = bearer_token {
                            auth_ctx = validator.validate(bearer).await;
                            if auth_ctx.is_some() {
                                tracing::debug!(
                                    "WS upgrade: SessionValidator accepted Bearer token"
                                );
                            }
                        }
                    } else {
                        tracing::debug!("WS upgrade: SessionValidator accepted Cookie input");
                    }

                    match auth_ctx {
                        Some(ctx) => {
                            tracing::debug!(
                                user_id = %ctx.user_id,
                                "WS upgrade: SessionValidator produced AuthContext"
                            );
                            request.extensions_mut().insert(Arc::new(ctx));
                            service.call(request).await.map_err(Into::into)
                        }
                        None if reject_on_failure => {
                            // RED-9 strict-mode: distinguish "credentials
                            // were supplied but invalid" from "no credentials
                            // present at all" so the operator can debug.
                            let body = if attempted_input {
                                "Unauthorized: session invalid or expired"
                            } else {
                                "Unauthorized: session cookie or bearer required"
                            };
                            tracing::warn!(
                                uri = %request.uri(),
                                attempted_input,
                                "WS upgrade rejected: SessionValidator produced no AuthContext"
                            );
                            Ok(unauthorized(body))
                        }
                        None => {
                            // Backward-compat path: log and proceed; per-method
                            // #[from_auth] still fail-closes at dispatch.
                            if attempted_input {
                                tracing::warn!(
                                    uri = %request.uri(),
                                    "WS upgrade: credentials present but \
                                     SessionValidator returned None; proceeding without auth"
                                );
                            } else {
                                tracing::debug!(
                                    uri = %request.uri(),
                                    "WS upgrade: no Cookie or Bearer present; \
                                     proceeding without auth"
                                );
                            }
                            service.call(request).await.map_err(Into::into)
                        }
                    }
                });
            }

            // No SessionValidator → either api_key gate already passed (or
            // nothing was configured). Pass through.
            let mut service = service;
            Box::pin(async move { service.call(request).await.map_err(Into::into) })
        }
    }
}

use auth::CombinedAuthMiddleware;
