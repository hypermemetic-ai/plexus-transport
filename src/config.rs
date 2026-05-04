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
}

impl WebSocketConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            api_key: None,
            api_key_header: http::HeaderName::from_static("x-plexus-api-key"),
        }
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
}

impl RestHttpConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            server_name: "plexus-rest".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
        }
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
