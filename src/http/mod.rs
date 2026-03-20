//! RESTful HTTP API transport for Plexus activations
//!
//! This module provides a REST HTTP transport layer that exposes Plexus activation
//! methods as HTTP endpoints. Methods are exposed as `POST /rest/{namespace}/{method}`
//! with JSON request/response bodies.
//!
//! ## Response Formats
//!
//! - **Non-streaming methods**: Return structured JSON with `data`, `logs`, and `progress` fields
//! - **Streaming methods**: Use Server-Sent Events (SSE) for real-time streaming
//!
//! ## Example
//!
//! ```rust,no_run
//! use plexus_transport::http::serve_rest_http;
//! use plexus_transport::config::RestHttpConfig;
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! # let activation = Arc::new(());  // Your Activation implementation
//! let config = RestHttpConfig::new(8888);
//! let handle = serve_rest_http(
//!     activation,
//!     None,  // flat_schemas for hub activations
//!     None,  // route_fn for hub routing
//!     config,
//!     None,  // api_key for auth
//! ).await?;
//!
//! handle.await??;
//! # Ok(())
//! # }
//! ```

pub mod bridge;
pub mod handler;
pub mod server;

pub use bridge::ActivationRestBridge;
pub use handler::{handle_method_call, MethodInfo};
pub use server::serve_rest_http;
