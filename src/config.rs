//! Configuration types for transport servers

use std::net::SocketAddr;

#[cfg(feature = "sqlite-sessions")]
use std::path::PathBuf;

/// Default header name carrying the static api_key (AUTHZ-BEARER-1).
///
/// Per AUTHZ-BEARER-S01-output §3 and AUTHZ-0 principles 4 and 6, the static
/// admission key MUST live on a dedicated header so the `Authorization` header
/// is reserved exclusively for `SessionValidator` user-identity tokens.
pub const DEFAULT_API_KEY_HEADER: &str = "X-Plexus-API-Key";

/// Complete transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub websocket: Option<WebSocketConfig>,
    pub stdio: Option<StdioConfig>,
    pub mcp_http: Option<McpHttpConfig>,
    pub rest_http: Option<RestHttpConfig>,
    /// Optional admission key required on all WebSocket, MCP HTTP, and REST HTTP connections.
    /// When `None`, no admission gate is enforced (current default).
    ///
    /// Per AUTHZ-BEARER-1, this is checked against the configured `api_key_header`
    /// (default `X-Plexus-API-Key`), NOT against `Authorization: Bearer`.
    pub api_key: Option<String>,
    /// Header name carrying the api_key on incoming requests.
    ///
    /// Defaults to `X-Plexus-API-Key`. Header lookup is case-insensitive per the
    /// HTTP spec; this value is used to construct an `http::HeaderName`.
    pub api_key_header: http::HeaderName,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            websocket: None,
            stdio: None,
            mcp_http: None,
            rest_http: None,
            api_key: None,
            api_key_header: http::HeaderName::from_static("x-plexus-api-key"),
        }
    }
}

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub addr: SocketAddr,
    /// Optional admission key required on the HTTP upgrade request.
    ///
    /// Per AUTHZ-BEARER-1, checked against the configured `api_key_header`
    /// (default `X-Plexus-API-Key`), NOT against `Authorization: Bearer`.
    pub api_key: Option<String>,
    /// Header name carrying the api_key on the upgrade request.
    /// Defaults to `X-Plexus-API-Key`.
    pub api_key_header: http::HeaderName,
    /// IDENTITY-20: the origins a browser page may open a socket from. Empty
    /// (the default) checks nothing. Otherwise an upgrade whose `Origin` is
    /// anything else — `null` included — is refused 403 before any
    /// credential is read; one with no `Origin` (a CLI, a server) proceeds.
    pub allowed_origins: Vec<String>,
    /// ACCESS-25: the `Host` values the upgrade answers to (see
    /// [`crate::host`]). Empty (the default) checks nothing; otherwise any
    /// other host is refused 421 before anything else is read.
    pub allowed_hosts: Vec<String>,
    /// AUTH-12: how many sockets may be open, in all and per caller.
    pub quotas: ConnectionQuotas,
    /// TRANSPORT-7: the proxies in front, whose `X-Forwarded-For` /
    /// `X-Forwarded-Proto` are believed — so the quotas count the client
    /// they forward, not the proxy (see [`crate::proxy`]). Default: none;
    /// no forwarded header is believed.
    pub trusted_proxies: crate::proxy::TrustedProxies,
}

/// AUTH-12: connection quotas on the WebSocket port, so no one caller —
/// least of all an anonymous one — can take every connection. A socket
/// counts against [`Self::max_connections`] and its IP's
/// [`Self::max_per_ip`] — the client's, when a trusted proxy forwards it
/// (TRANSPORT-7, [`WebSocketConfig::trusted_proxies`]), else the peer's; then, under a session validator, against its
/// principal's [`Self::max_per_principal`] if the upgrade signed someone
/// in, else against [`Self::max_anonymous`] and its IP's
/// [`Self::max_anonymous_per_ip`]. (Without a validator no socket is anyone,
/// so only the total and the per-IP quota apply.) Over a quota, the upgrade
/// is refused 429. A plain HTTP JSON-RPC request counts only against the
/// total, while it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionQuotas {
    /// Every connection, set on jsonrpsee explicitly (its default is 100).
    pub max_connections: u32,
    /// Sockets from one IP address, signed in or not.
    pub max_per_ip: u32,
    /// Sockets one signed-in principal holds.
    pub max_per_principal: u32,
    /// Sockets that proved no one — held well below the total, so they can
    /// never lock a signed-in caller out.
    pub max_anonymous: u32,
    /// Anonymous sockets from one IP address.
    pub max_anonymous_per_ip: u32,
}

impl Default for ConnectionQuotas {
    fn default() -> Self {
        Self { max_connections: 1024, max_per_ip: 256, max_per_principal: 64, max_anonymous: 128, max_anonymous_per_ip: 32 }
    }
}

impl WebSocketConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            api_key: None,
            api_key_header: http::HeaderName::from_static("x-plexus-api-key"),
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
            quotas: ConnectionQuotas::default(),
            trusted_proxies: Default::default(),
        }
    }

    /// TRANSPORT-7: listen on `addr` (e.g. `0.0.0.0:4455` in a container
    /// behind a proxy) rather than `127.0.0.1:<port>`.
    pub fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    /// TRANSPORT-7: the proxies in front (see [`Self::trusted_proxies`]).
    pub fn with_trusted_proxies(mut self, proxies: crate::proxy::TrustedProxies) -> Self {
        self.trusted_proxies = proxies;
        self
    }

    /// ACCESS-25: the `Host` values this listener answers to; any other is
    /// refused 421. [`crate::host::loopback_hosts`] gives a loopback port's.
    pub fn with_allowed_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// Override the header name carrying the api_key.
    ///
    /// Default: `X-Plexus-API-Key`. Header lookup is case-insensitive per the
    /// HTTP spec; the stored `HeaderName` is normalized to lowercase.
    pub fn with_api_key_header(mut self, header: http::HeaderName) -> Self {
        self.api_key_header = header;
        self
    }
}

/// Stdio (line-delimited JSON-RPC) configuration
#[derive(Debug, Clone)]
pub struct StdioConfig {
    /// Buffer size for subscription notifications
    pub subscription_buffer_size: usize,
}

impl Default for StdioConfig {
    fn default() -> Self {
        Self {
            subscription_buffer_size: 1024,
        }
    }
}

/// MCP HTTP server configuration
#[derive(Debug, Clone)]
pub struct McpHttpConfig {
    pub addr: SocketAddr,
    pub session_storage: SessionStorage,
    /// Optional override for server name (defaults to activation namespace)
    pub server_name: Option<String>,
    /// Optional override for server version (defaults to activation version)
    pub server_version: Option<String>,
    /// Optional bearer token required on all MCP HTTP requests.
    pub api_key: Option<String>,
}

impl McpHttpConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            session_storage: SessionStorage::default(),
            server_name: None,
            server_version: None,
            api_key: None,
        }
    }

    /// Override the server name reported in MCP server info
    pub fn with_server_name(mut self, name: String) -> Self {
        self.server_name = Some(name);
        self
    }

    /// Override the server version reported in MCP server info
    pub fn with_server_version(mut self, version: String) -> Self {
        self.server_version = Some(version);
        self
    }

    #[cfg(feature = "sqlite-sessions")]
    pub fn with_sqlite(mut self, path: PathBuf) -> Self {
        self.session_storage = SessionStorage::Sqlite { path };
        self
    }
}

/// Session storage backend for MCP
#[derive(Debug, Clone)]
pub enum SessionStorage {
    /// In-memory sessions (lost on restart, simpler)
    InMemory,
    /// SQLite persistent sessions (survive restarts)
    #[cfg(feature = "sqlite-sessions")]
    Sqlite { path: PathBuf },
}

impl Default for SessionStorage {
    fn default() -> Self {
        Self::InMemory
    }
}

/// REST HTTP server configuration
#[derive(Debug, Clone)]
pub struct RestHttpConfig {
    pub addr: SocketAddr,
    pub server_name: String,
    pub server_version: String,
    /// ACCESS-25: the `Host` values the bridge answers to (see
    /// [`crate::host`]). Empty (the default) checks nothing; otherwise any
    /// other host is refused 421 before routing.
    pub allowed_hosts: Vec<String>,
}

impl RestHttpConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            server_name: "plexus-rest".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            allowed_hosts: Vec::new(),
        }
    }

    /// ACCESS-25: the `Host` values this listener answers to; any other is
    /// refused 421. [`crate::host::loopback_hosts`] gives a loopback port's.
    pub fn with_allowed_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// Override the server name
    pub fn with_server_name(mut self, name: String) -> Self {
        self.server_name = name;
        self
    }

    /// Override the server version
    pub fn with_server_version(mut self, version: String) -> Self {
        self.server_version = version;
        self
    }
}
