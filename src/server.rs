//! Transport server builder and orchestration

use anyhow::Result;
use plexus_core::plexus::{Activation, PlexusError, PlexusStream, PluginSchema, SessionValidator};
use crate::ws::ServerHandle;
use jsonrpsee::RpcModule;
use serde_json::Value;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::config::{ConnectionQuotas, StdioConfig, TransportConfig, WebSocketConfig};
#[cfg(feature = "mcp")]
use crate::config::McpHttpConfig;
#[cfg(feature = "mcp")]
use crate::mcp::server::serve_mcp_http;
use crate::registry::{self, RegistryRegistration};
use crate::stdio::serve_stdio;
use crate::protected_resource::ProtectedResource;
use crate::proxy::{IpNet, TrustedProxies};
use crate::websocket::{serve_websocket_with, UpgradeOptions};

/// A function that routes a namespaced method call (e.g., "loopback.permit") to the
/// correct activation. Used by hub activations to dispatch child calls via `hub.route()`.
///
/// Signature: `fn(method: String, params: Value) -> Future<Output = Result<PlexusStream, PlexusError>>`
///
/// Shared by the MCP and REST HTTP bridges; defined here so it is available
/// regardless of which transport features are enabled.
pub type RouteFn = Arc<
    dyn Fn(String, Value) -> Pin<Box<dyn Future<Output = Result<PlexusStream, PlexusError>> + Send>>
        + Send
        + Sync,
>;

/// Z2H-6: builder-level configuration for boot-time registry
/// self-registration. All fields are overrides; `Default` means
/// "registration ON, endpoint from `PLEXUS_REGISTRY_URL` (default
/// `ws://127.0.0.1:4444`), name from the activation's schema namespace".
#[derive(Debug, Clone, Default)]
pub struct RegistryOptions {
    /// `None` = default (enabled when a WebSocket transport is configured).
    /// `PLEXUS_NO_REGISTRY=1` wins over any builder setting.
    pub enabled: Option<bool>,
    /// Override the registry endpoint URL.
    pub url: Option<String>,
    /// Override the registered backend name.
    pub name: Option<String>,
    /// Override the advertised host (default `127.0.0.1`).
    pub host: Option<String>,
}

/// TRANSPORT-16: the bridge mounted in-process, built once the validator
/// is known (at `build`), served at `McpConfig.listen` until TRANSPORT-10
/// puts it on the WebSocket's own router.
#[cfg(feature = "mcp")]
pub(crate) struct McpMount {
    config: plexus_transport_mcp::McpConfig,
    make: Box<dyn FnOnce(Option<Arc<dyn SessionValidator>>, plexus_transport_mcp::McpConfig) -> axum::Router + Send>,
}

/// Function type for converting Arc<Activation> to RpcModule
///
/// This allows each activation type to provide its own conversion logic,
/// which is critical for preserving Arc lifecycle and Weak references.
pub type RpcConverter<A> = Box<dyn FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send>;

/// Transport server that can host any Activation
///
/// Supports multiple transports simultaneously:
/// - WebSocket (JSON-RPC)
/// - Stdio (line-delimited JSON-RPC, MCP-compatible)
/// - MCP HTTP (with SSE streaming)
pub struct TransportServer<A: Activation> {
    activation: Arc<A>,
    config: TransportConfig,
    rpc_converter: Option<RpcConverter<A>>,
    /// Pre-computed flat schema list for gateway exposure (MCP tools and/or
    /// REST routes). When set, the bridges expose all listed schemas.
    #[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
    flat_schemas: Option<Vec<PluginSchema>>,
    /// Optional routing function for hub activations.
    /// When set, MCP call_tool and REST dispatch use it to route namespaced
    /// calls via hub.route().
    #[cfg_attr(not(any(feature = "mcp", feature = "http-gateway")), allow(dead_code))]
    route_fn: Option<RouteFn>,
    /// Optional session validator for cookie-based authentication.
    /// When set, validates cookies from HTTP upgrade requests.
    session_validator: Option<Arc<dyn SessionValidator>>,
    /// RED-9: opt-in strict mode for the session-validator path. When `true`,
    /// the WS upgrade is rejected with HTTP 401 if the cookie is absent OR
    /// the validator returns `None`. When `false` (default, backward-compat),
    /// the middleware logs and proceeds without `AuthContext`; per-method
    /// `#[from_auth]` checks fire-closed at dispatch instead.
    reject_upgrade_on_auth_failure: bool,
    /// IDENTITY-20: the origins a browser page may open the socket from.
    allowed_origins: Vec<String>,
    /// ACCESS-25: the `Host` values the WebSocket answers to.
    allowed_hosts: Vec<String>,
    /// AUTH-12: the WebSocket's connection quotas, when not the default.
    connection_quotas: Option<ConnectionQuotas>,
    /// TRANSPORT-7: the proxies in front, when named.
    trusted_proxies: Option<TrustedProxies>,
    /// AUTH-6: the anonymous view, credential cookies, RFC 9728 metadata.
    upgrade: UpgradeOptions,
    /// Z2H-6: boot-time registry self-registration configuration.
    registry: RegistryOptions,
    /// TRANSPORT-16: the mounted bridge, when `with_mcp` was called.
    #[cfg(feature = "mcp")]
    mcp: Option<McpMount>,
}

impl<A: Activation> TransportServer<A> {
    /// Create a builder for configuring transports
    pub fn builder<F>(activation: Arc<A>, rpc_converter: F) -> TransportServerBuilder<A>
    where
        F: FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send + 'static,
    {
        TransportServerBuilder::new(activation, rpc_converter)
    }

    /// Start all configured transports
    ///
    /// If stdio is configured, this will block on stdio (as it's the primary transport).
    /// Otherwise, it will start WebSocket/MCP servers and wait for them to complete.
    ///
    /// ## Z2H-6: registry self-registration
    ///
    /// When a WebSocket transport is configured, the server announces
    /// itself to the Plexus registry after binding (name, host, ACTUAL
    /// bound port, schema identity) — a first-class, observable act.
    /// Registration is fail-open (≤3 attempts, one warn, serve anyway).
    /// On graceful shutdown (server stop, SIGTERM, or ctrl-c) the
    /// registration is invalidated via `registry.deregister`. Opt out
    /// with [`TransportServerBuilder::without_registry`] or
    /// `PLEXUS_NO_REGISTRY=1`; point elsewhere with
    /// `PLEXUS_REGISTRY_URL` / [`TransportServerBuilder::with_registry_url`].
    pub async fn serve(mut self) -> Result<()> {
        // Convert activation to RPC module for WebSocket/stdio
        let needs_rpc = self.config.websocket.is_some() || self.config.stdio.is_some();
        let module = if needs_rpc {
            let converter = self
                .rpc_converter
                .take()
                .ok_or_else(|| anyhow::anyhow!("RPC converter required for WebSocket/stdio"))?;
            Some(converter(self.activation.clone())?)
        } else {
            None
        };

        // TRANSPORT-7: ClientIp and SecureTransport believe the same
        // proxies the upgrade does.
        if let Some(p) = self.trusted_proxies.clone() {
            crate::proxy::init_trusted_proxies(p);
        }

        // Start stdio transport (blocking)
        if let Some(stdio_config) = self.config.stdio {
            let module = module.expect("RPC module should be created for stdio");
            return serve_stdio(module, stdio_config).await;
        }

        // Start WebSocket transport
        let ws_started: Option<(ServerHandle, std::net::SocketAddr)> = if let Some(mut ws_config) = self.config.websocket {
            // Propagate the global api_key + header config to the WebSocket
            // config if not already set on the WS-specific config.
            if ws_config.api_key.is_none() {
                ws_config.api_key = self.config.api_key.clone();
            }
            // Always propagate the configured api_key_header from the global
            // config — the WS-specific default is set in WebSocketConfig::new
            // and we want the global builder method to win.
            ws_config.api_key_header = self.config.api_key_header.clone();
            // IDENTITY-20: the builder's allow-list, unless the WS config
            // already names one.
            if ws_config.allowed_origins.is_empty() {
                ws_config.allowed_origins = self.allowed_origins.clone();
            }
            // ACCESS-25: the builder's host list, unless the WS config
            // already names one.
            if ws_config.allowed_hosts.is_empty() {
                ws_config.allowed_hosts = self.allowed_hosts.clone();
            }
            // AUTH-12: the builder's quotas, when set.
            if let Some(q) = self.connection_quotas.clone() {
                ws_config.quotas = q;
            }
            // TRANSPORT-7: the builder's proxies, when named.
            if let Some(p) = self.trusted_proxies.clone() {
                ws_config.trusted_proxies = p;
            }
            let module = module.expect("RPC module should be created for WebSocket");
            let upgrade = UpgradeOptions {
                reject_on_session_failure: self.reject_upgrade_on_auth_failure,
                ..self.upgrade.clone()
            };
            Some(serve_websocket_with(module, ws_config, self.session_validator.clone(), upgrade).await?)
        } else {
            None
        };

        // Z2H-6: announce ourselves to the registry (fail-open, spawned so
        // a slow/absent registry never delays serving).
        let registry_reg: Option<RegistryRegistration> = match &ws_started {
            Some((_, bound_addr))
                if self.registry.enabled.unwrap_or(true)
                    && !registry::registry_disabled_by_env() =>
            {
                let schema = Activation::plugin_schema(self.activation.as_ref());
                Some(RegistryRegistration {
                    url: self
                        .registry
                        .url
                        .clone()
                        .unwrap_or_else(registry::registry_url_from_env),
                    name: self
                        .registry
                        .name
                        .clone()
                        .unwrap_or_else(|| schema.namespace.clone()),
                    host: self
                        .registry
                        .host
                        .clone()
                        .unwrap_or_else(|| "127.0.0.1".to_string()),
                    port: bound_addr.port(),
                    version: schema.version.clone(),
                    schema_hash: schema.hash.clone(),
                })
            }
            _ => None,
        };

        let registered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(reg) = registry_reg.clone() {
            let registered = registered.clone();
            tokio::spawn(async move {
                if registry::register_with_retry(&reg).await {
                    registered.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }

        let ws_handle: Option<ServerHandle> = ws_started.map(|(h, _)| h);
        // Clone for the shutdown-signal branch (ServerHandle is a cheap
        // shared handle); the original moves into the wait branch below.
        let ws_handle_for_signal = ws_handle.clone();

        // Start MCP HTTP transport (requires the `mcp` feature)
        #[cfg(feature = "mcp")]
        let mcp_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> =
            if let Some(mcp_config) = self.config.mcp_http {
                let api_key = self.config.api_key.clone();
                Some(serve_mcp_http(self.activation.clone(), self.flat_schemas.clone(), self.route_fn.clone(), mcp_config, api_key).await?)
            } else {
                None
            };

        #[cfg(not(feature = "mcp"))]
        let mcp_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> = {
            if self.config.mcp_http.is_some() {
                tracing::warn!(
                    "MCP HTTP transport configured but the 'mcp' feature is disabled; ignoring"
                );
            }
            None
        };

        // Start REST HTTP transport
        #[cfg(feature = "http-gateway")]
        let rest_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> =
            if let Some(rest_config) = self.config.rest_http {
                let api_key = self.config.api_key.clone();
                Some(crate::http::serve_rest_http(self.activation.clone(), self.flat_schemas.clone(), self.route_fn.clone(), rest_config, api_key).await?)
            } else {
                None
            };

        #[cfg(not(feature = "http-gateway"))]
        let rest_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> = None;

        // TRANSPORT-16: the bridge mounted over the service, as the caller
        // the WebSocket's validator names.
        #[cfg(feature = "mcp")]
        let mounted_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> = match self.mcp.take() {
            Some(mount) => {
                let listen = mount.config.listen.ok_or_else(|| {
                    anyhow::anyhow!("with_mcp: McpConfig.listen is required — the mounted bridge has its own listener until TRANSPORT-10 puts /mcp on the WebSocket's router")
                })?;
                let router = (mount.make)(self.session_validator.clone(), mount.config.clone());
                let listener = tokio::net::TcpListener::bind(listen).await?;
                tracing::info!("Starting MCP at http://{}{}", listener.local_addr()?, mount.config.path);
                Some(tokio::spawn(async move { axum::serve(listener, router).await }))
            }
            None => None,
        };
        #[cfg(not(feature = "mcp"))]
        let mounted_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>> = None;

        // Wait for any server to complete
        if ws_handle.is_none() && mcp_handle.is_none() && rest_handle.is_none() && mounted_handle.is_none() {
            tracing::warn!("No transports configured, nothing to serve");
            return Ok(());
        }

        // Wait for first server to stop. When registry registration is
        // active, also listen for shutdown signals (SIGTERM / ctrl-c) so
        // the registration can be invalidated before exit (Z2H-6
        // stop-invalidation; SIGKILL/crash is covered by resolver-side
        // lazy eviction instead).
        tokio::select! {
            _ = async {
                if let Some(ws) = ws_handle {
                    ws.stopped().await;
                    tracing::info!("WebSocket server stopped");
                }
            }, if ws_handle.is_some() => {}

            _ = async {
                if let Some(mcp) = mcp_handle {
                    match mcp.await {
                        Ok(Ok(())) => tracing::info!("MCP server stopped"),
                        Ok(Err(e)) => tracing::error!("MCP server error: {}", e),
                        Err(e) => tracing::error!("MCP server task failed: {}", e),
                    }
                }
            }, if mcp_handle.is_some() => {}

            _ = async {
                if let Some(rest) = rest_handle {
                    match rest.await {
                        Ok(Ok(())) => tracing::info!("REST server stopped"),
                        Ok(Err(e)) => tracing::error!("REST server error: {}", e),
                        Err(e) => tracing::error!("REST server task failed: {}", e),
                    }
                }
            }, if rest_handle.is_some() => {}

            _ = async {
                if let Some(mounted) = mounted_handle {
                    match mounted.await {
                        Ok(Ok(())) => tracing::info!("MCP (mounted) stopped"),
                        Ok(Err(e)) => tracing::error!("MCP (mounted) error: {}", e),
                        Err(e) => tracing::error!("MCP (mounted) task failed: {}", e),
                    }
                }
            }, if mounted_handle.is_some() => {}

            _ = wait_shutdown_signal(), if registry_reg.is_some() => {
                tracing::info!("shutdown signal received — stopping transports");
                if let Some(ws) = &ws_handle_for_signal {
                    let _ = ws.stop();
                }
            }
        }

        // Z2H-6: invalidate our registration on the way out (best-effort).
        if let Some(reg) = &registry_reg {
            if registered.load(std::sync::atomic::Ordering::SeqCst) {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    registry::deregister(reg),
                )
                .await
                {
                    Ok(Ok(())) => tracing::info!(
                        "deregistered '{}' from registry at {}",
                        reg.name, reg.url
                    ),
                    Ok(Err(e)) => tracing::warn!(
                        "could not deregister '{}' from registry (entry will be \
                         lazily evicted on next resolve): {e:#}",
                        reg.name
                    ),
                    Err(_) => tracing::warn!(
                        "timed out deregistering '{}' from registry (entry will \
                         be lazily evicted on next resolve)",
                        reg.name
                    ),
                }
            }
        }

        Ok(())
    }

}

/// Resolve when a shutdown signal (ctrl-c, or SIGTERM on unix) arrives.
/// Only awaited when registry registration is active — signal handlers are
/// not installed otherwise, preserving default process behavior.
async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Builder for configuring transport servers
pub struct TransportServerBuilder<A: Activation> {
    activation: Arc<A>,
    config: TransportConfig,
    rpc_converter: Option<RpcConverter<A>>,
    flat_schemas: Option<Vec<PluginSchema>>,
    route_fn: Option<RouteFn>,
    session_validator: Option<Arc<dyn SessionValidator>>,
    /// RED-4: opt-out of the build-time check that refuses to start the
    /// server when activations declare auth-gated methods but no auth
    /// middleware has been configured. Set by
    /// `.allow_missing_auth_middleware()`.
    allow_missing_auth: bool,
    /// RED-9: opt-in strict-mode for the WS upgrade. Set by
    /// `.reject_upgrade_on_auth_failure()`.
    reject_upgrade_on_auth_failure: bool,
    /// IDENTITY-20: set by `.with_allowed_origins()`.
    allowed_origins: Vec<String>,
    /// ACCESS-25: set by `.with_allowed_hosts()`.
    allowed_hosts: Vec<String>,
    /// AUTH-12: set by `.with_connection_quotas()`.
    connection_quotas: Option<ConnectionQuotas>,
    /// TRANSPORT-7: set by `.trusted_proxies()` / `.with_trusted_proxies()`.
    trusted_proxies: Option<TrustedProxies>,
    /// TRANSPORT-7: set by `.bind()`.
    bind: Option<IpAddr>,
    /// AUTH-6: set by `.anonymous_view()`, `.credential_cookies()`,
    /// `.with_protected_resource()`.
    upgrade: UpgradeOptions,
    /// Z2H-6: registry self-registration overrides.
    registry: RegistryOptions,
    /// TRANSPORT-16: set by `.with_mcp()`.
    #[cfg(feature = "mcp")]
    mcp: Option<McpMount>,
}

#[cfg(feature = "mcp")]
impl TransportServerBuilder<plexus_core::plexus::DynamicHub> {
    /// TRANSPORT-16: mount the MCP bridge over this service. The same bridge
    /// a sidecar runs (`plexus-transport-mcp`), over the service in memory: every
    /// `tools/call` is `route_with_ctx` as the caller the session validator
    /// names from the request's `Bearer` (the WebSocket's own validator,
    /// [`Self::with_session_validator`]); no credential is the anonymous
    /// caller; `tools/list` is that caller's view. Served at
    /// `config.listen` (required; `bind` applies to it) until TRANSPORT-10
    /// puts `config.path` on the WebSocket's router. Replaces
    /// `with_mcp_http`, which TRANSPORT-9 removes.
    ///
    /// The mount does not read the static api_key. With
    /// [`Self::with_api_key`] set, [`Self::build`] refuses a config that
    /// admits anonymous callers: set `McpConfig.anonymous = false`. Without a
    /// validator such a mount refuses every request, since a Bearer has
    /// nothing to name it.
    pub fn with_mcp(mut self, config: plexus_transport_mcp::McpConfig) -> Self {
        let hub = self.activation.clone();
        self.mcp = Some(McpMount {
            config,
            make: Box::new(move |validator, config| {
                let bridge = plexus_transport_mcp::McpBridge::new(crate::mcp::ServiceUpstream::new(hub, validator), config);
                plexus_transport_mcp::router(bridge)
            }),
        });
        self
    }
}

impl<A: Activation> TransportServerBuilder<A> {
    pub fn new<F>(activation: Arc<A>, rpc_converter: F) -> Self
    where
        F: FnOnce(Arc<A>) -> Result<RpcModule<()>> + Send + 'static,
    {
        Self {
            activation,
            config: TransportConfig::default(),
            rpc_converter: Some(Box::new(rpc_converter)),
            flat_schemas: None,
            route_fn: None,
            session_validator: None,
            allow_missing_auth: false,
            reject_upgrade_on_auth_failure: false,
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
            connection_quotas: None,
            trusted_proxies: None,
            bind: None,
            upgrade: UpgradeOptions::default(),
            registry: RegistryOptions::default(),
            #[cfg(feature = "mcp")]
            mcp: None,
        }
    }

    /// Z2H-6: opt out of boot-time registry self-registration (the
    /// `--no-register` convention). Also reachable at runtime via
    /// `PLEXUS_NO_REGISTRY=1` without touching code.
    pub fn without_registry(mut self) -> Self {
        self.registry.enabled = Some(false);
        self
    }

    /// Z2H-6: override the name this backend registers under. Default:
    /// the activation's schema namespace (the hub root for `DynamicHub`).
    pub fn with_registry_name(mut self, name: impl Into<String>) -> Self {
        self.registry.name = Some(name.into());
        self
    }

    /// Z2H-6: override the registry endpoint. Default:
    /// `PLEXUS_REGISTRY_URL`, falling back to `ws://127.0.0.1:4444`.
    pub fn with_registry_url(mut self, url: impl Into<String>) -> Self {
        self.registry.url = Some(url.into());
        self
    }

    /// Z2H-6: override the host advertised in the registration (default
    /// `127.0.0.1`; multi-host deployments are out of Z2H-6 scope).
    pub fn with_registry_host(mut self, host: impl Into<String>) -> Self {
        self.registry.host = Some(host.into());
        self
    }

    /// Enable WebSocket transport on the specified port
    pub fn with_websocket(mut self, port: u16) -> Self {
        self.config.websocket = Some(WebSocketConfig::new(port));
        self
    }

    /// Enable stdio transport (MCP-compatible)
    pub fn with_stdio(mut self) -> Self {
        self.config.stdio = Some(StdioConfig::default());
        self
    }

    /// Enable MCP HTTP transport on the specified port
    #[cfg(feature = "mcp")]
    pub fn with_mcp_http(mut self, port: u16) -> Self {
        self.config.mcp_http = Some(McpHttpConfig::new(port));
        self
    }

    /// Enable MCP HTTP transport with custom configuration
    #[cfg(feature = "mcp")]
    pub fn with_mcp_http_config(mut self, config: McpHttpConfig) -> Self {
        self.config.mcp_http = Some(config);
        self
    }

    /// Enable REST HTTP transport on the specified port
    #[cfg(feature = "http-gateway")]
    pub fn with_rest_http(mut self, port: u16) -> Self {
        self.config.rest_http = Some(crate::config::RestHttpConfig::new(port));
        self
    }

    /// Enable REST HTTP transport with custom configuration
    #[cfg(feature = "http-gateway")]
    pub fn with_rest_http_config(mut self, config: crate::config::RestHttpConfig) -> Self {
        self.config.rest_http = Some(config);
        self
    }

    /// Set pre-computed flat schemas for gateway exposure (MCP tools and/or
    /// REST routes — this hook feeds both bridges and works with or without
    /// the `mcp` feature).
    /// For hub activations, pass `hub.list_plugin_schemas()` to expose all child schemas.
    pub fn with_flat_schemas(mut self, schemas: Vec<PluginSchema>) -> Self {
        self.flat_schemas = Some(schemas);
        self
    }

    /// Set the routing function for gateway dispatch (MCP call_tool and REST
    /// method calls — this hook feeds both bridges and works with or without
    /// the `mcp` feature).
    /// For hub activations, provide a closure wrapping `hub.route()` so that
    /// namespaced tool calls (e.g., "loopback.permit") reach the correct child.
    pub fn with_route_fn(mut self, route_fn: RouteFn) -> Self {
        self.route_fn = Some(route_fn);
        self
    }

    /// Deprecated alias of [`Self::with_flat_schemas`]. The `mcp_` prefix was
    /// misleading: these schemas also feed the REST HTTP bridge
    /// (`http-gateway` feature) and do not require the `mcp` feature.
    #[deprecated(since = "0.5.0", note = "renamed to `with_flat_schemas` — the schemas feed the REST bridge too, not just MCP")]
    pub fn with_mcp_flat_schemas(self, schemas: Vec<PluginSchema>) -> Self {
        self.with_flat_schemas(schemas)
    }

    /// Deprecated alias of [`Self::with_route_fn`]. The `mcp_` prefix was
    /// misleading: the routing function also feeds the REST HTTP bridge
    /// (`http-gateway` feature) and does not require the `mcp` feature.
    #[deprecated(since = "0.5.0", note = "renamed to `with_route_fn` — the routing function feeds the REST bridge too, not just MCP")]
    pub fn with_mcp_route_fn(self, route_fn: RouteFn) -> Self {
        self.with_route_fn(route_fn)
    }

    /// Configure a static admission key required on all WebSocket connections.
    ///
    /// When set, the WebSocket upgrade is rejected with HTTP 401 unless the
    /// configured api_key header (default `X-Plexus-API-Key`, see
    /// [`Self::with_api_key_header`]) carries the matching value. Passing
    /// `None` disables the static admission gate (default behaviour).
    ///
    /// Per AUTHZ-BEARER-1, the static api_key gate is independent of the
    /// `SessionValidator` path. The `Authorization: Bearer` header is reserved
    /// for `SessionValidator` user-identity tokens and is not consulted by the
    /// static admission gate. (One exception: the deprecated v1 compat shim
    /// — see the WS middleware docs.)
    ///
    /// MCP HTTP and REST HTTP gateways continue to enforce api_key against
    /// `Authorization: Bearer` until a follow-up ticket aligns them.
    pub fn with_api_key(mut self, key: Option<String>) -> Self {
        self.config.api_key = key;
        self
    }

    /// Override the header that carries the static api_key on WebSocket upgrades.
    ///
    /// Default: `X-Plexus-API-Key`. Header lookup is case-insensitive per the
    /// HTTP spec; the stored `HeaderName` is normalized to lowercase. Has no
    /// effect when `with_api_key(None)` is configured.
    ///
    /// AUTHZ-BEARER-1: separates the static-admission header from the
    /// `Authorization` header that carries `SessionValidator`-consumed Bearer
    /// tokens.
    pub fn with_api_key_header(mut self, header: http::HeaderName) -> Self {
        self.config.api_key_header = header;
        self
    }

    /// Set session validator for cookie-based authentication.
    ///
    /// When set, the WebSocket transport will extract cookies from HTTP upgrade
    /// requests and validate them using the provided SessionValidator. The resulting
    /// AuthContext is stored in request Extensions and passed to RPC methods.
    ///
    /// This is useful for browser-based authentication where cookies are preferred
    /// over Authorization headers.
    pub fn with_session_validator(mut self, validator: Arc<dyn SessionValidator>) -> Self {
        self.session_validator = Some(validator);
        self
    }

    /// RED-9: opt in to strict-mode session validation at the WebSocket
    /// upgrade.
    ///
    /// By default (backward-compatible), the session-validator middleware
    /// populates `AuthContext` when a valid cookie is present and silently
    /// passes through with no auth context when the cookie is missing or
    /// invalid. Methods with `#[from_auth(...)]` then fail-closed at
    /// dispatch with `Unauthenticated` — but methods without it dispatch
    /// normally.
    ///
    /// When this option is enabled:
    ///
    /// - Missing `Cookie:` header on the WS upgrade → HTTP 401 before any
    ///   RPC frames flow.
    /// - Cookie present but `SessionValidator::validate()` returns `None`
    ///   → HTTP 401 before any RPC frames flow.
    ///
    /// This is the right posture for backends where ANY public endpoint
    /// would be a problem (e.g., FormVeritas-style apps where every method
    /// requires a valid session). For backends that intentionally mix
    /// authenticated and public endpoints on the same transport, leave
    /// this off.
    ///
    /// Has no effect when no `SessionValidator` is configured. To force
    /// auth middleware to be configured at build time, see
    /// [`Self::allow_missing_auth_middleware`] (RED-4).
    pub fn reject_upgrade_on_auth_failure(mut self) -> Self {
        self.reject_upgrade_on_auth_failure = true;
        self
    }

    /// IDENTITY-20: the origins a browser page may open the WebSocket from,
    /// e.g. `http://127.0.0.1:4456` for an app served there.
    ///
    /// Cookies are scoped to a host, not a port, so any page on the same
    /// host would otherwise open a socket that carries a person's session.
    /// With an allow-list, an upgrade whose `Origin` header is not on it —
    /// `null`, from a sandboxed frame, included — is refused with 403 before
    /// any credential is read. An upgrade with no `Origin` (synapse, a
    /// server, a CLI: browsers always send one) proceeds. Origins compare
    /// case-insensitively, without a trailing slash. Empty: no check.
    pub fn with_allowed_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_origins = origins.into_iter().map(Into::into).collect();
        self
    }

    /// ACCESS-25: the `Host` values the WebSocket answers to, e.g.
    /// [`crate::host::loopback_hosts`]`(4455)` — a page that rebinds its
    /// own name to this address (DNS rebinding) is refused 421 before
    /// anything else is read, the RFC 9728 metadata included. Entries are
    /// `host:port`, or `host` for a default port; they compare
    /// case-insensitively and exactly. Empty (the default): no check. The
    /// REST bridge's list is [`crate::config::RestHttpConfig::with_allowed_hosts`],
    /// since its port differs.
    pub fn with_allowed_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// AUTH-12: the WebSocket port's connection quotas — in all, per IP,
    /// per signed-in principal, and for anonymous sockets (see
    /// [`ConnectionQuotas`]). Default: [`ConnectionQuotas::default`].
    pub fn with_connection_quotas(mut self, quotas: ConnectionQuotas) -> Self {
        self.connection_quotas = Some(quotas);
        self
    }

    /// TRANSPORT-7: the address every listener of this builder binds —
    /// the WebSocket, the REST bridge and MCP HTTP keep their ports and take
    /// this IP (`0.0.0.0` / `::` in a container behind a proxy). Default:
    /// `127.0.0.1`. The registry is then told this address, unless it is
    /// unspecified or loopback (`127.0.0.1`) or
    /// [`Self::with_registry_host`] says otherwise.
    pub fn bind(mut self, addr: impl Into<IpAddr>) -> Self {
        self.bind = Some(addr.into());
        self
    }

    /// TRANSPORT-7: the proxies in front of the WebSocket, as networks
    /// (`172.30.0.2/32`). `X-Forwarded-For` / `X-Forwarded-Proto` are
    /// believed only from these peers — the client is the rightmost
    /// forwarded hop that is not itself one — and never from any other:
    /// the connection quotas count that client, every request carries its
    /// [`ForwardedClient`](crate::proxy::ForwardedClient), and
    /// [`ClientIp`](crate::request::ClientIp) /
    /// [`SecureTransport`](crate::request::SecureTransport) read the same
    /// set. Behind a proxy, loopback is not assumed secure. See
    /// [`crate::proxy`].
    pub fn trusted_proxies<I: IntoIterator<Item = IpNet>>(self, nets: I) -> Self {
        self.with_trusted_proxies(TrustedProxies::new(nets))
    }

    /// TRANSPORT-7: [`Self::trusted_proxies`] as a [`TrustedProxies`] —
    /// e.g. [`TrustedProxies::parse`] of a `--trusted-proxy` flag, or
    /// [`TrustedProxies::direct`] (`none`: no proxy in front).
    pub fn with_trusted_proxies(mut self, proxies: TrustedProxies) -> Self {
        self.trusted_proxies = Some(proxies);
        self
    }

    /// AUTH-6: admit an upgrade that presents no credential into the
    /// anonymous view, instead of refusing it.
    ///
    /// With a [`SessionValidator`] configured:
    ///
    /// - no credential presented → admitted with no `AuthContext`; the hub's
    ///   gate answers it with the anonymous view (how to sign in);
    /// - a credential presented that the validator rejects → HTTP 401, with
    ///   `WWW-Authenticate: Bearer realm="plexus", error="invalid_token"`
    ///   (and `resource_metadata="…"` under
    ///   [`Self::with_protected_resource`]);
    /// - a credential the validator accepts → admitted with its
    ///   `AuthContext`, as always.
    ///
    /// Presented means an `Authorization` header (any scheme) or a cookie
    /// named by [`Self::credential_cookies`] with a non-empty value; other
    /// cookies (`theme=dark`) present nothing. This supersedes
    /// [`Self::reject_upgrade_on_auth_failure`] for the no-credential case
    /// when both are set. Has no effect without a `SessionValidator`.
    pub fn anonymous_view(mut self) -> Self {
        self.upgrade.anonymous_view = true;
        self
    }

    /// AUTH-6: the cookies that carry a credential (e.g.
    /// `["menger_session_4456", "access_token"]`). Under
    /// [`Self::anonymous_view`], an upgrade whose `Cookie` header holds one
    /// of these with a value presents a credential, so a bad value is a 401
    /// rather than the anonymous view. The validator still receives the
    /// whole `Cookie` header. Names compare exactly (RFC 6265).
    pub fn credential_cookies<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.upgrade.credential_cookies = names.into_iter().map(Into::into).collect();
        self
    }

    /// AUTH-6: publish RFC 9728 protected-resource metadata.
    ///
    /// The WebSocket port answers `GET /.well-known/oauth-protected-resource`
    /// (followed by the resource's path, if it has one — RFC 9728 §3.1) with
    /// the document as `application/json`, before any origin or credential
    /// check; and every session 401 at the upgrade carries
    /// `resource_metadata="<that URL>"` in its `WWW-Authenticate`.
    pub fn with_protected_resource(mut self, resource: ProtectedResource) -> Self {
        self.upgrade.protected_resource = Some(resource);
        self
    }

    /// PERF-20: the WebSocket's limits — the message cap, ping and idle
    /// (RPC-5), the header timeout and the sockets not yet upgraded per IP
    /// (RPC-4), the writer's queue and budget. Unset: today's values, or
    /// their gaps' ([`crate::ws::SocketLimits`]).
    pub fn with_socket_limits(mut self, limits: crate::ws::SocketLimits) -> Self {
        self.upgrade.limits = limits;
        self
    }

    /// PERF-20 → BIDIR-12: a hook over each upgraded connection's inbound
    /// frames, made per connection by `hooks`.
    pub fn with_hooks(mut self, hooks: crate::ws::Hooks) -> Self {
        self.upgrade.hooks = Some(hooks);
        self
    }

    /// RED-4: opt out of the build-time auth-configuration check.
    ///
    /// Normally, [`Self::build`] inspects the registered activation's
    /// `plugin_schema()` for methods carrying an `x-plexus-source.from == "auth"`
    /// annotation (emitted by `#[from_auth(...)]`). If any such method exists
    /// AND neither [`Self::with_api_key`] nor [`Self::with_session_validator`]
    /// was called, `build()` returns `Err` to prevent deploying an auth-gated
    /// activation to a server without auth middleware.
    ///
    /// Call this method to bypass the check when you intentionally want a
    /// server with no auth (e.g., local test harness, fully public backend).
    /// The presence of this call in source code is an audit signal.
    pub fn allow_missing_auth_middleware(mut self) -> Self {
        self.allow_missing_auth = true;
        self
    }

    /// Build the transport server.
    ///
    /// RED-4: fails with a clear error when the activation declares auth-gated
    /// methods but no auth middleware has been configured. Opt out via
    /// [`Self::allow_missing_auth_middleware`]. Also fails when a static
    /// api_key is set and the [`Self::with_mcp`] mount admits anonymous
    /// callers, which that opt-out does not excuse.
    pub async fn build(self) -> Result<TransportServer<A>> {
        // RED-4: verify auth configuration before starting the server.
        let has_api_key = self.config.api_key.is_some();
        let has_session_validator = self.session_validator.is_some();
        let auth_configured = has_api_key || has_session_validator;

        // The MCP mount has no key check of its own, so a key gates it only
        // if it refuses callers with no credential. Not excused by
        // `allow_missing_auth_middleware`: a key says a gate was wanted.
        #[cfg(feature = "mcp")]
        if has_api_key && self.mcp.as_ref().is_some_and(|m| m.config.admits_anonymous()) {
            return Err(anyhow::anyhow!(
                "with_mcp: `.with_api_key(...)` does not gate the MCP mount, and this \
                 McpConfig admits anonymous callers, who would reach the hub without \
                 the key. Set `McpConfig.anonymous = false` (or `oauth`), with \
                 `.with_session_validator(...)` to admit Bearer callers."
            ));
        }

        if !auth_configured && !self.allow_missing_auth {
            let auth_gated = collect_auth_gated_methods(&*self.activation);
            if !auth_gated.is_empty() {
                let summary = auth_gated
                    .iter()
                    .take(8)
                    .map(|m| format!("  - {}", m))
                    .collect::<Vec<_>>()
                    .join("\n");
                let extra = if auth_gated.len() > 8 {
                    format!("\n  ... ({} more)", auth_gated.len() - 8)
                } else {
                    String::new()
                };
                return Err(anyhow::anyhow!(
                    "RED-4: {} auth-gated method(s) declared but no auth middleware \
                     configured. Call `.with_session_validator(...)` or \
                     `.with_api_key(...)` on the builder, or \
                     `.allow_missing_auth_middleware()` to intentionally opt out.\n\
                     \n\
                     Auth-gated methods:\n{}{}",
                    auth_gated.len(),
                    summary,
                    extra,
                ));
            }
        }

        // TRANSPORT-7: one bind address for every listener.
        let mut config = self.config;
        let mut registry = self.registry;
        if let Some(ip) = self.bind {
            if let Some(ws) = config.websocket.as_mut() {
                ws.addr.set_ip(ip);
            }
            if let Some(mcp) = config.mcp_http.as_mut() {
                mcp.addr.set_ip(ip);
            }
            #[cfg(feature = "http-gateway")]
            if let Some(rest) = config.rest_http.as_mut() {
                rest.addr.set_ip(ip);
            }
            if registry.host.is_none() && !ip.is_unspecified() && !ip.is_loopback() {
                registry.host = Some(ip.to_string());
            }
        }
        // TRANSPORT-16: the mounted bridge takes the bind address too.
        #[cfg(feature = "mcp")]
        let mcp = self.mcp.map(|mut m| {
            if let (Some(ip), Some(listen)) = (self.bind, m.config.listen.as_mut()) {
                listen.set_ip(ip);
            }
            m
        });

        Ok(TransportServer {
            activation: self.activation,
            config,
            rpc_converter: self.rpc_converter,
            flat_schemas: self.flat_schemas,
            route_fn: self.route_fn,
            session_validator: self.session_validator,
            reject_upgrade_on_auth_failure: self.reject_upgrade_on_auth_failure,
            allowed_origins: self.allowed_origins,
            allowed_hosts: self.allowed_hosts,
            connection_quotas: self.connection_quotas,
            trusted_proxies: self.trusted_proxies,
            upgrade: self.upgrade,
            registry,
            #[cfg(feature = "mcp")]
            mcp,
        })
    }
}

/// RED-4: inspect an activation's plugin schema for methods with
/// `x-plexus-source.from == "auth"`.
///
/// Returns a flat list of `"namespace.method"` paths for error reporting.
/// Activations that declare no auth-gated methods return an empty vec —
/// the caller uses emptiness as a signal that no auth config is required.
///
/// Only inspects the root activation's schema. Standalone child activations
/// register their own `TransportServerBuilder`s and hit this check
/// independently. Hub-style children whose methods appear in the parent
/// schema are covered through the root traversal.
fn collect_auth_gated_methods<A: Activation>(activation: &A) -> Vec<String> {
    let schema = Activation::plugin_schema(activation);
    let mut out = Vec::new();
    collect_from_schema(&schema, &mut out);
    out
}

fn collect_from_schema(schema: &PluginSchema, out: &mut Vec<String>) {
    for method in &schema.methods {
        let params_schema = match &method.params {
            Some(p) => p,
            None => continue,
        };
        let props = match params_schema.get("properties").and_then(|v| v.as_object()) {
            Some(p) => p,
            None => continue,
        };
        for (_name, prop) in props {
            let src = prop.get("x-plexus-source").and_then(|v| v.as_object());
            let from = src.and_then(|s| s.get("from")).and_then(|v| v.as_str());
            if from == Some("auth") {
                out.push(format!("{}.{}", schema.namespace, method.name));
                break;
            }
        }
    }
}
