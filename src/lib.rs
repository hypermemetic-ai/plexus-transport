//! Hub Transport - Generic transport layer for Plexus activations
//!
//! This library provides transport adapters (WebSocket, stdio, MCP HTTP) that work
//! with any type implementing the `Activation` trait from `plexus-core`.
//!
//! ## Example
//!
//! ```rust,ignore
//! use plexus_transport::TransportServer;
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Create your activation (single plugin or DynamicHub router)
//! let activation = Arc::new(MyActivation::new());
//!
//! // Provide a converter function that handles Arc -> RpcModule conversion
//! // For DynamicHub, this preserves the Arc for Weak references
//! let rpc_converter = |arc: Arc<MyActivation>| {
//!     MyActivation::arc_into_rpc_module(arc)
//! };
//!
//! // Configure and start transport servers
//! TransportServer::builder(activation, rpc_converter)
//!     .with_websocket(8888)
//!     .with_mcp_http(8889)
//!     .build().await?
//!     .serve().await?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "mcp-gateway")]
pub mod combined;
pub mod config;
pub mod server;
pub mod stdio;
pub mod websocket;

#[cfg(feature = "sqlite-sessions")]
pub mod mcp;

#[cfg(not(feature = "sqlite-sessions"))]
pub mod mcp;

// Re-export main API
#[cfg(feature = "mcp-gateway")]
pub use combined::serve_combined;
pub use config::{McpHttpConfig, SessionStorage, StdioConfig, TransportConfig, WebSocketConfig};
pub use server::{TransportServer, TransportServerBuilder};

// Re-export MCP bridge for advanced usage
#[cfg(feature = "sqlite-sessions")]
pub use mcp::{bridge::ActivationMcpBridge, session::SqliteSessionManager};

#[cfg(not(feature = "sqlite-sessions"))]
pub use mcp::bridge::ActivationMcpBridge;

pub use mcp::bridge::RouteFn;
