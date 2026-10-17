//! Secure transport validation behind a TLS-terminating proxy.
//!
//! In production, TLS is terminated by the edge (Caddy, nginx), which says
//! how the client came in with `X-Forwarded-Proto`. TRANSPORT-7: that header
//! is believed only from a trusted proxy ([`crate::proxy::init_trusted_proxies`]);
//! from any other peer it is ignored. With no proxy in front, a loopback
//! peer is secure (local development); behind a proxy, loopback proves
//! nothing — the proxy may itself be on loopback — and only the proxy's
//! word counts. See [`crate::proxy`].

use std::sync::OnceLock;

use plexus_core::{
    plexus::PlexusError,
    request::{PlexusRequestField, RawRequestContext},
};

static REQUIRE_SECURE: OnceLock<bool> = OnceLock::new();

/// Enable secure transport enforcement: a request is refused unless a
/// trusted proxy says it came over `https`/`wss`, or — with no proxy in
/// front — it comes from loopback.
pub fn init_require_secure_transport(require: bool) {
    let _ = REQUIRE_SECURE.set(require);
}

/// Validated secure transport marker.
///
/// Extraction succeeds when:
/// - Enforcement is disabled (dev mode), OR
/// - the peer is a trusted proxy whose `X-Forwarded-Proto` is `https` or
///   `wss`, OR
/// - no proxy is in front and the peer is loopback (local dev).
#[derive(Debug, Clone)]
pub struct SecureTransport;

impl SecureTransport {
    /// Whether `ctx` came securely under `proxies` (enforcement aside).
    pub fn check(ctx: &RawRequestContext, proxies: &crate::proxy::TrustedProxies) -> Result<Self, PlexusError> {
        let Some(peer) = ctx.peer else {
            return Err(PlexusError::Unauthenticated("Secure transport required (no peer address)".into()));
        };
        let client = proxies.resolve(peer.ip(), &ctx.headers);
        if client.secure {
            return Ok(SecureTransport);
        }
        let why = if client.via_proxy {
            match ctx.headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok()) {
                Some(p) => format!("Secure transport required, got X-Forwarded-Proto: {p}"),
                None => "Secure transport required (X-Forwarded-Proto header missing)".to_string(),
            }
        } else {
            "Secure transport required (not through a trusted proxy)".to_string()
        };
        Err(PlexusError::Unauthenticated(why))
    }
}

impl PlexusRequestField for SecureTransport {
    fn extract_from_raw(ctx: &RawRequestContext) -> Result<Self, PlexusError> {
        if !REQUIRE_SECURE.get().copied().unwrap_or(false) {
            return Ok(SecureTransport);
        }
        Self::check(ctx, crate::proxy::trusted_proxies())
    }
}
