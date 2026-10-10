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
//!
//! ## The anonymous view and RFC 9728 (AUTH-6)
//!
//! [`UpgradeOptions::anonymous_view`]: with a `SessionValidator`, an upgrade
//! that presents no credential is admitted without an `AuthContext` (the
//! hub shows it the anonymous view); one that presents a credential the
//! validator rejects is refused 401 with `error="invalid_token"`. Presented
//! = an `Authorization` header, or a cookie named in
//! [`UpgradeOptions::credential_cookies`] with a value.
//! [`UpgradeOptions::protected_resource`] serves RFC 9728 metadata at
//! `/.well-known/oauth-protected-resource` on this port, ahead of every
//! check, and adds `resource_metadata="…"` to each session 401.
//!
//! ## A sign-in that ends closes its socket (AUTH-11)
//!
//! Each connection has its own stop handle ([`Connection`], in every
//! request's extensions). When the upgrade signs a caller in, a watcher
//! asks the validator ([`SessionValidator::alive`]) every
//! [`LIVENESS_EVERY`] while the socket is open, and closes it once the
//! session is signed out, revoked or expired — rather than leaving it to
//! the next call (which the hub's gate refuses anyway).
//!
//! [`SessionValidator::alive`]: plexus_core::plexus::SessionValidator::alive

use anyhow::Result;
use futures::FutureExt;
use jsonrpsee::server::{serve_with_graceful_shutdown, stop_channel, Server, ServerHandle};
use jsonrpsee::RpcModule;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tower::Service;

use crate::config::WebSocketConfig;

/// AUTH-11: how often an open, signed-in socket asks whether its sign-in
/// still stands.
pub const LIVENESS_EVERY: Duration = Duration::from_secs(1);

/// One accepted TCP connection, in the extensions of every request it
/// carries (and so of the WebSocket it upgrades to, for as long as that
/// lives): who is at the other end, and how to close it.
#[derive(Debug)]
pub struct Connection {
    peer: SocketAddr,
    handle: ServerHandle,
}

impl Connection {
    /// The peer's address, as accepted (no proxy headers).
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Close this connection (its WebSocket, after in-flight calls).
    pub fn close(&self) {
        let _ = self.handle.stop();
    }

    /// Whether it has been closed.
    pub fn is_closed(&self) -> bool {
        self.handle.is_stopped()
    }
}

impl Drop for Connection {
    /// Gone from every request: the connection is over; release its stop
    /// handle's watchers.
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

/// AUTH-11: watch a signed-in socket; close it once its sign-in no longer
/// stands. Holds the connection weakly: when the socket ends on its own,
/// the watch ends at its next tick.
fn watch_liveness(
    conn: std::sync::Weak<Connection>,
    validator: Arc<dyn plexus_core::plexus::SessionValidator>,
    ctx: plexus_core::plexus::AuthContext,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(LIVENESS_EVERY).await;
            let Some(conn) = conn.upgrade() else { return };
            if conn.is_closed() {
                return;
            }
            if let Err(why) = validator.alive(&ctx) {
                tracing::info!(user_id = %ctx.user_id, peer = %conn.peer, reason = %why, "closing socket: its sign-in ended");
                conn.close();
                return;
            }
        }
    });
}

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
/// Returns a handle that can be used to stop the server, plus the ACTUAL
/// bound address (differs from `config.addr` when binding port 0 — Z2H-6
/// registration reports the OS-assigned port).
pub async fn serve_websocket(
    module: RpcModule<()>,
    config: WebSocketConfig,
    session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
    reject_on_session_failure: bool,
) -> Result<(ServerHandle, std::net::SocketAddr)> {
    serve_websocket_with(
        module,
        config,
        session_validator,
        UpgradeOptions { reject_on_session_failure, ..UpgradeOptions::default() },
    )
    .await
}

/// AUTH-6: how the upgrade treats a caller's credential, beyond the
/// [`WebSocketConfig`]. `Default` is today's behavior: no strict mode, no
/// anonymous view, no protected-resource metadata.
#[derive(Debug, Clone, Default)]
pub struct UpgradeOptions {
    /// RED-9: with a `SessionValidator`, refuse (401) an upgrade that yields
    /// no `AuthContext`, credential or not. Under [`Self::anonymous_view`]
    /// only a presented credential is refused.
    pub reject_on_session_failure: bool,
    /// AUTH-6: with a `SessionValidator`, an upgrade that presents NO
    /// credential is admitted without an `AuthContext` (the hub's gate shows
    /// it the anonymous view), and one that presents a credential the
    /// validator rejects is refused 401. What counts as presented: an
    /// `Authorization` header (any scheme), or a cookie named in
    /// [`Self::credential_cookies`] with a non-empty value. Unrelated
    /// cookies (`theme=dark`) present nothing.
    pub anonymous_view: bool,
    /// AUTH-6: the names of the cookies that carry a credential (e.g.
    /// `access_token`). Only these, under [`Self::anonymous_view`], make a
    /// `Cookie` header a presentation; the validator still sees the whole
    /// header, so a valid credential in any cookie still signs the caller
    /// in. Empty: no cookie is a presentation — a stale cookie falls back
    /// to the anonymous view rather than a 401.
    pub credential_cookies: Vec<String>,
    /// AUTH-6: RFC 9728 metadata, served at its well-known path on this
    /// port and named in every session 401's `WWW-Authenticate`.
    pub protected_resource: Option<crate::protected_resource::ProtectedResource>,
}

/// [`serve_websocket`] with the full [`UpgradeOptions`] (AUTH-6).
pub async fn serve_websocket_with(
    module: RpcModule<()>,
    config: WebSocketConfig,
    session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
    options: UpgradeOptions,
) -> Result<(ServerHandle, std::net::SocketAddr)> {
    tracing::info!("Starting WebSocket transport at ws://{}", config.addr);

    // The middleware is always in: with nothing configured it passes every
    // request through, as the bare server did.
    let api_key = config.api_key.clone();
    let api_key_header = config.api_key_header.clone();
    let allowed_origins = auth::normalized_origins(&config.allowed_origins);
    let policy: Arc<auth::Policy> = (&options).into();
    let middleware = tower::ServiceBuilder::new().layer_fn(move |service| CombinedAuthMiddleware {
        service,
        api_key: api_key.clone(),
        api_key_header: api_key_header.clone(),
        session_validator: session_validator.clone(),
        reject_on_session_failure: options.reject_on_session_failure,
        allowed_origins: allowed_origins.clone(),
        policy: policy.clone(),
    });
    let svc_builder = Server::builder().set_http_middleware(middleware).to_service_builder();
    let methods = jsonrpsee::Methods::from(module);

    let listener = TcpListener::bind(config.addr).await?;
    let local_addr = listener.local_addr()?;
    let (stop_handle, server_handle) = stop_channel();

    // AUTH-11: jsonrpsee's low-level accept loop, so each connection gets
    // its own stop handle (a socket can be closed alone) and its peer
    // address rides into the middleware.
    tokio::spawn(async move {
        loop {
            let (sock, peer) = tokio::select! {
                res = listener.accept() => match res {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!("websocket accept: {e}");
                        continue;
                    }
                },
                _ = stop_handle.clone().shutdown() => break,
            };
            let (conn_stop, conn_handle) = stop_channel();
            // The server's stop closes every connection.
            let server_stop = stop_handle.clone();
            let forward = conn_handle.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = server_stop.shutdown() => { let _ = forward.stop(); }
                    _ = forward.clone().stopped() => {}
                }
            });
            let conn = Arc::new(Connection { peer, handle: conn_handle });
            let svc_builder = svc_builder.clone();
            let methods = methods.clone();
            let conn_stop_for_serve = conn_stop.clone();
            let svc = tower::service_fn(move |mut req: http::Request<hyper::body::Incoming>| {
                req.extensions_mut().insert(conn.clone());
                let mut svc = svc_builder.clone().build(methods.clone(), conn_stop.clone());
                async move { svc.call(req).await.map_err(anyhow::Error::from_boxed) }.boxed()
            });
            tokio::spawn(async move {
                if let Err(e) = serve_with_graceful_shutdown(sock, svc, conn_stop_for_serve.shutdown()).await {
                    tracing::debug!("websocket connection closed: {e}");
                }
            });
        }
    });

    Ok((server_handle, local_addr))
}

// ---------------------------------------------------------------------------
// Combined auth middleware for jsonrpsee's HTTP upgrade path
// AUTHZ-BEARER-1: separates the static api_key admission gate (configurable
// header, default X-Plexus-API-Key) from the SessionValidator path which
// consumes Cookie and/or Authorization: Bearer.
// ---------------------------------------------------------------------------

pub(crate) mod auth {
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
    pub struct CombinedAuthMiddleware<S> {
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
        /// IDENTITY-20: normalized allowed origins; empty checks nothing.
        pub(super) allowed_origins: Arc<Vec<String>>,
        /// AUTH-6: the anonymous view, credential cookies, RFC 9728.
        pub(super) policy: Arc<Policy>,
    }

    /// AUTH-6: [`super::UpgradeOptions`], prepared once.
    #[derive(Debug, Default)]
    pub struct Policy {
        pub(super) anonymous_view: bool,
        pub(super) credential_cookies: Vec<String>,
        pub(super) metadata: Option<crate::protected_resource::Prepared>,
    }

    impl From<&super::UpgradeOptions> for Arc<Policy> {
        fn from(o: &super::UpgradeOptions) -> Self {
            Arc::new(Policy {
                anonymous_view: o.anonymous_view,
                credential_cookies: o.credential_cookies.clone(),
                metadata: o.protected_resource.as_ref().map(Into::into),
            })
        }
    }

    impl Policy {
        /// AUTH-6: does the upgrade present a credential? An `Authorization`
        /// header of any scheme does; so does a declared credential cookie
        /// with a non-empty value. Nothing else — `theme=dark` is not one.
        fn presents_credential(&self, headers: &http::HeaderMap) -> bool {
            if headers.contains_key(http::header::AUTHORIZATION) {
                return true;
            }
            headers
                .get_all(http::header::COOKIE)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|v| v.split(';'))
                .filter_map(|pair| pair.split_once('='))
                .any(|(name, value)| {
                    !value.trim().is_empty()
                        && self.credential_cookies.iter().any(|c| c == name.trim())
                })
        }

        /// AUTH-6: the metadata document, when this request asks for it.
        fn metadata_response<B>(&self, request: &HttpRequest<B>) -> Option<HttpResponse> {
            let m = self.metadata.as_ref()?;
            let path = request.uri().path().trim_end_matches('/');
            if request.method() != http::Method::GET || path != m.path {
                return None;
            }
            Some(
                http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(http::header::CACHE_CONTROL, "max-age=3600")
                    .body(jsonrpsee::server::HttpBody::from(m.body.clone()))
                    .expect("static response is valid"),
            )
        }

        /// The 401 for a session refusal: `invalid_token` when a credential
        /// was presented (RFC 6750 §3.1 — none when it was absent), and the
        /// RFC 9728 `resource_metadata` when configured.
        fn session_unauthorized(&self, body: &'static str, presented: bool) -> HttpResponse {
            let mut challenge = String::from("Bearer realm=\"plexus\"");
            if presented {
                challenge.push_str(", error=\"invalid_token\"");
            }
            if let Some(m) = &self.metadata {
                challenge.push_str(&format!(", resource_metadata=\"{}\"", m.url));
            }
            unauthorized_with(body, &challenge)
        }
    }

    /// IDENTITY-20: an origin as compared: lowercase, no trailing slash.
    fn normalize_origin(origin: &str) -> String {
        origin.trim().trim_end_matches('/').to_ascii_lowercase()
    }

    /// IDENTITY-20: the allow-list, normalized once.
    pub(crate) fn normalized_origins(origins: &[String]) -> Arc<Vec<String>> {
        Arc::new(origins.iter().map(|o| normalize_origin(o)).collect())
    }

    /// IDENTITY-20: `Some(the refusal)` when an allow-list is set and the
    /// request's `Origin` is present and not on it.
    fn origin_refused(allowed: &[String], headers: &http::HeaderMap) -> Option<String> {
        if allowed.is_empty() {
            return None;
        }
        let origin = headers.get(http::header::ORIGIN)?;
        let Ok(origin) = origin.to_str() else {
            return Some("<not text>".to_string());
        };
        let normalized = normalize_origin(origin);
        (!allowed.iter().any(|a| *a == normalized)).then(|| origin.to_string())
    }

    fn forbidden(body: &'static str) -> HttpResponse {
        http::Response::builder()
            .status(http::StatusCode::FORBIDDEN)
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body(jsonrpsee::server::HttpBody::from(body))
            .expect("static response is valid")
    }

    /// Helper: build a 401 response with the substrate-wire-protocol
    /// `WWW-Authenticate: Bearer realm="plexus"` header preserved.
    fn unauthorized(body: &'static str) -> HttpResponse {
        unauthorized_with(body, "Bearer realm=\"plexus\"")
    }

    fn unauthorized_with(body: &'static str, challenge: &str) -> HttpResponse {
        let challenge = http::HeaderValue::from_str(challenge)
            .unwrap_or_else(|_| http::HeaderValue::from_static("Bearer realm=\"plexus\""));
        http::Response::builder()
            .status(http::StatusCode::UNAUTHORIZED)
            .header(http::header::WWW_AUTHENTICATE, challenge)
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

            // ── AUTH-6: RFC 9728 metadata — public, before anything else ───
            if let Some(resp) = self.policy.metadata_response(&request) {
                return Box::pin(async move { Ok(resp) });
            }

            // ── Step 0: the origin (IDENTITY-20) ───────────────────────────
            // Before any credential is read: a page from another origin on
            // the same host would otherwise carry the person's cookies.
            if let Some(origin) = origin_refused(&self.allowed_origins, request.headers()) {
                tracing::warn!(origin = %origin, uri = %request.uri(), "WS upgrade rejected: origin not allowed");
                return Box::pin(async move { Ok(forbidden("Forbidden: origin not allowed")) });
            }

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
                // AUTH-6: under the anonymous view, only a credential the
                // server recognises as one counts as presented.
                let policy = self.policy.clone();
                let presented = policy.presents_credential(request.headers());

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
                            // AUTH-11: a socket signed in is watched, and
                            // closed once its sign-in ends.
                            if jsonrpsee::server::ws::is_upgrade_request(&request) {
                                if let Some(conn) = request.extensions().get::<Arc<super::Connection>>() {
                                    super::watch_liveness(Arc::downgrade(conn), validator.clone(), ctx.clone());
                                }
                            }
                            request.extensions_mut().insert(Arc::new(ctx));
                            service.call(request).await.map_err(Into::into)
                        }
                        None if policy.anonymous_view && presented => {
                            // AUTH-6: a credential was presented and is bad:
                            // only its absence is the anonymous view.
                            tracing::warn!(
                                uri = %request.uri(),
                                "WS upgrade rejected: presented credential invalid"
                            );
                            Ok(policy.session_unauthorized(
                                "Unauthorized: session invalid or expired",
                                true,
                            ))
                        }
                        None if policy.anonymous_view => {
                            // AUTH-6: no credential — admitted without an
                            // AuthContext; the hub's gate shows the
                            // anonymous view.
                            tracing::debug!(
                                uri = %request.uri(),
                                "WS upgrade: no credential presented; admitted anonymous"
                            );
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
                            Ok(policy.session_unauthorized(body, attempted_input))
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

/// Test-only hooks for AUTHZ-BEARER-1 integration tests.
///
/// `#[doc(hidden)]` because this is not part of the public API contract; it
/// exists to let integration tests in `tests/` drive the middleware
/// directly without spinning up a real WS server.
#[doc(hidden)]
pub mod testing {
    use std::sync::Arc;

    /// Build the AUTHZ-BEARER-1 auth middleware, wrapping the supplied
    /// inner service. See `serve_websocket` for the production wiring.
    pub fn combined_auth_middleware<S>(
        service: S,
        api_key: Option<String>,
        api_key_header: http::HeaderName,
        session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
        reject_on_session_failure: bool,
    ) -> super::auth::CombinedAuthMiddleware<S> {
        super::auth::CombinedAuthMiddleware {
            service,
            api_key,
            api_key_header,
            session_validator,
            reject_on_session_failure,
            allowed_origins: Arc::new(Vec::new()),
            policy: Default::default(),
        }
    }

    /// AUTH-6: the middleware with full [`super::UpgradeOptions`] and an
    /// optional origin allow-list.
    pub fn combined_auth_middleware_with_options<S>(
        service: S,
        session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
        allowed_origins: &[String],
        options: &super::UpgradeOptions,
    ) -> super::auth::CombinedAuthMiddleware<S> {
        super::auth::CombinedAuthMiddleware {
            service,
            api_key: None,
            api_key_header: http::HeaderName::from_static("x-plexus-api-key"),
            session_validator,
            reject_on_session_failure: options.reject_on_session_failure,
            allowed_origins: super::auth::normalized_origins(allowed_origins),
            policy: options.into(),
        }
    }

    /// IDENTITY-20: [`combined_auth_middleware`] with an origin allow-list.
    pub fn combined_auth_middleware_with_origins<S>(
        service: S,
        session_validator: Option<Arc<dyn plexus_core::plexus::SessionValidator>>,
        reject_on_session_failure: bool,
        allowed_origins: &[String],
    ) -> super::auth::CombinedAuthMiddleware<S> {
        super::auth::CombinedAuthMiddleware {
            service,
            api_key: None,
            api_key_header: http::HeaderName::from_static("x-plexus-api-key"),
            session_validator,
            reject_on_session_failure,
            allowed_origins: super::auth::normalized_origins(allowed_origins),
            policy: Default::default(),
        }
    }

    pub use super::auth::CombinedAuthMiddleware;
}
