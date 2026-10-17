//! Request extraction infrastructure for Plexus HTTP/WebSocket handlers.
//!
//! The [`PlexusRequest`] trait and [`RawRequestContext`] struct form the
//! foundation for typed extraction of inbound HTTP request data.
//!
//! These types are now defined in `plexus_core::request` and re-exported here
//! for backward compatibility.

pub mod client_ip;
pub mod derive;
pub mod origin;
pub mod raw;
pub mod transport;

#[allow(deprecated)]
pub use client_ip::{ClientIp, init_trust_proxy_headers};
pub use derive::PlexusRequest;
pub use origin::{ValidOrigin, init_allowed_origins};
pub use raw::RawRequestContext;
pub use transport::{SecureTransport, init_require_secure_transport};
// Re-export parse_cookie from plexus-core for backward compatibility
pub use plexus_core::request::parse_cookie;
