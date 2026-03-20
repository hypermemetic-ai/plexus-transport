//! Hub Transport - Generic transport layer for Plexus activations
//!
//! This library provides transport adapters (WebSocket, stdio, MCP HTTP, REST HTTP) that work
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
//!     .with_rest_http(8890)  // Feature: http-gateway
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

#[cfg(feature = "http-gateway")]
pub mod http;

// Re-export main API
#[cfg(feature = "mcp-gateway")]
pub use combined::serve_combined;
pub use config::{McpHttpConfig, SessionStorage, StdioConfig, TransportConfig, WebSocketConfig};

#[cfg(feature = "http-gateway")]
pub use config::RestHttpConfig;

pub use server::{TransportServer, TransportServerBuilder};

// Re-export MCP bridge for advanced usage
#[cfg(feature = "sqlite-sessions")]
pub use mcp::{bridge::ActivationMcpBridge, session::SqliteSessionManager};

#[cfg(not(feature = "sqlite-sessions"))]
pub use mcp::bridge::ActivationMcpBridge;

pub use mcp::bridge::RouteFn;

// Re-export REST HTTP bridge for advanced usage
#[cfg(feature = "http-gateway")]
pub use http::{ActivationRestBridge, serve_rest_http};
