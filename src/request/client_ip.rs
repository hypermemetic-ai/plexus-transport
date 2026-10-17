//! Client IP extraction behind a reverse proxy.
//!
//! In production behind an edge, `ctx.peer` is the proxy. TRANSPORT-7: with
//! trusted proxies configured ([`crate::proxy::init_trusted_proxies`]), the
//! client is the address a trusted proxy forwarded in `X-Forwarded-For`
//! (the rightmost hop that is not itself a proxy) — believed only when the
//! peer is one of them, never from anyone else. See [`crate::proxy`].

use std::net::IpAddr;
use std::sync::OnceLock;

use plexus_core::{
    plexus::PlexusError,
    request::{PlexusRequestField, RawRequestContext},
};

static TRUST_PROXY: OnceLock<bool> = OnceLock::new();

/// Trust `X-Forwarded-For` (first entry) / `X-Real-IP` from **any** peer.
///
/// Deprecated (TRANSPORT-7): any client can send those headers, so this lets
/// a caller choose its own address. Name the proxies instead —
/// [`crate::proxy::init_trusted_proxies`], or
/// `TransportServerBuilder::trusted_proxies`, which wins over this when set.
#[deprecated(note = "trusts forwarded headers from any peer; use init_trusted_proxies (TRANSPORT-7)")]
pub fn init_trust_proxy_headers(trust: bool) {
    let _ = TRUST_PROXY.set(trust);
}

/// The real client IP address: forwarded by a trusted proxy, else the peer.
#[derive(Debug, Clone)]
pub struct ClientIp(pub IpAddr);

impl ClientIp {
    /// The client of `ctx` under `proxies` (what extraction does with the
    /// set [`crate::proxy::init_trusted_proxies`] made).
    pub fn resolve(ctx: &RawRequestContext, proxies: &crate::proxy::TrustedProxies) -> Self {
        match ctx.peer {
            Some(addr) => ClientIp(proxies.client_ip(addr.ip(), &ctx.headers)),
            None => ClientIp(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
        }
    }
}

impl PlexusRequestField for ClientIp {
    fn extract_from_raw(ctx: &RawRequestContext) -> Result<Self, PlexusError> {
        let proxies = crate::proxy::trusted_proxies();
        if proxies.configured() {
            return Ok(Self::resolve(ctx, proxies));
        }

        // Legacy (deprecated): forwarded headers from any peer, when asked.
        if TRUST_PROXY.get().copied().unwrap_or(false) {
            if let Some(xff) = ctx.headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
                if let Some(first) = xff.split(',').next().map(|s| s.trim()) {
                    if let Ok(ip) = first.parse::<IpAddr>() {
                        return Ok(ClientIp(ip));
                    }
                }
            }
            if let Some(xri) = ctx.headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
                if let Ok(ip) = xri.trim().parse::<IpAddr>() {
                    return Ok(ClientIp(ip));
                }
            }
        }

        Ok(Self::resolve(ctx, proxies))
    }
}
