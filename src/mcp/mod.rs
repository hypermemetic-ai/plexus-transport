//! MCP (Model Context Protocol) transport
//!
//! Provides HTTP-based MCP server with SSE streaming support.

pub mod bridge;
pub mod service_upstream;
pub mod server;

#[cfg(feature = "sqlite-sessions")]
pub mod session;

pub use bridge::ActivationMcpBridge;
pub use service_upstream::ServiceUpstream;
pub use server::serve_mcp_http;

#[cfg(feature = "sqlite-sessions")]
pub use session::{SqliteSessionConfig, SqliteSessionManager};
