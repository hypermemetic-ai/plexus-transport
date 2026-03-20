//! Configuration types for transport servers

use std::net::SocketAddr;

#[cfg(feature = "sqlite-sessions")]
use std::path::PathBuf;

/// Complete transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub websocket: Option<WebSocketConfig>,
    pub stdio: Option<StdioConfig>,
    pub mcp_http: Option<McpHttpConfig>,
    pub rest_http: Option<RestHttpConfig>,
    /// Optional bearer token required on all WebSocket, MCP HTTP, and REST HTTP connections.
    /// When `None`, no authentication is required (current behaviour).
    pub api_key: Option<String>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            websocket: None,
            stdio: None,
            mcp_http: None,
            rest_http: None,
            api_key: None,
        }
    }
}

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub addr: SocketAddr,
    /// Optional bearer token required on the HTTP upgrade request.
    pub api_key: Option<String>,
}

impl WebSocketConfig {
    pub fn new(port: u16) -> Self {
        Self {
            addr: format!("127.0.0.1:{}", port)
                .parse()
                .expect("Valid socket address"),
            api_key: None,
        }
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
