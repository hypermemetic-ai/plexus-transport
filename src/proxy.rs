//! TRANSPORT-7: who a request comes from, behind a reverse proxy.
//!
//! Behind an edge (Caddy, nginx) every visitor's TCP peer is the edge, so
//! anything keyed by the peer — AUTH-12's connection quotas, [`ClientIp`],
//! [`SecureTransport`] — sees one caller. The edge says who the visitor is
//! in `X-Forwarded-For` and how they came in `X-Forwarded-Proto`; those
//! headers are believed only from the peers named as proxies
//! ([`TrustedProxies`]), never from anyone else, since any client can send
//! them. The rule is the standard one (and plexus-login's, AUTH-22):
//!
//! - the TCP peer is not a trusted proxy → the client is the peer; whatever
//!   forwarded headers it sent are ignored (and said once, loudly: a proxy
//!   may be in front and not named);
//! - the peer is a trusted proxy → walk `X-Forwarded-For` from the right
//!   (the hop nearest us) leftwards, skipping addresses that are themselves
//!   trusted proxies; the first that is not is the client. A hop that does
//!   not parse ends the walk at the last trusted hop (what is left of it the
//!   client wrote, and proves nothing). If every hop is trusted, the
//!   leftmost is the client.
//!
//! **Secure transport.** A connection is secure when a trusted proxy says
//! its client came over `https`/`wss` (`X-Forwarded-Proto`, the entry
//! matching the client's hop, else the rightmost — the one the nearest
//! proxy wrote), or, with no proxy in front ([`TrustedProxies::behind_proxy`]
//! false), when the peer is loopback. Behind a proxy loopback proves
//! nothing — the proxy itself may be on loopback, relaying plain `http` —
//! so only the proxy's word counts.
//!
//! The WebSocket's upgrade resolves a [`ForwardedClient`] for every request
//! ([`crate::WebSocketConfig::trusted_proxies`],
//! [`crate::TransportServerBuilder::trusted_proxies`]), counts its
//! connection quotas by [`ForwardedClient::ip`], and leaves it in the
//! request's extensions (so in every call's) for the application.
//! [`ClientIp`] and [`SecureTransport`] read the same set through
//! [`init_trusted_proxies`] (which `TransportServer::serve` calls).
//!
//! [`ClientIp`]: crate::request::ClientIp
//! [`SecureTransport`]: crate::request::SecureTransport

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

pub use ipnet::IpNet;

/// The proxies whose `X-Forwarded-For` / `X-Forwarded-Proto` are believed,
/// or [`Self::direct`]: no proxy is in front, said so. The default names
/// none and is not [`Self::configured`]; it believes no forwarded header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrustedProxies {
    /// The proxies' networks (a bare address is a /32 or /128).
    pub nets: Vec<IpNet>,
    /// People connect directly: no proxy is in front (`none`).
    pub direct: bool,
}

/// Who a request comes from, as the proxies in front say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardedClient {
    /// The client: the peer, or the address a trusted proxy forwarded.
    pub ip: IpAddr,
    /// The TCP peer, as accepted.
    pub peer: IpAddr,
    /// Whether the peer is a trusted proxy (so `ip` came from its header).
    pub via_proxy: bool,
    /// Whether the client's connection is secure (see the module docs).
    pub secure: bool,
}

impl TrustedProxies {
    /// These networks are the proxies in front.
    pub fn new<I: IntoIterator<Item = IpNet>>(nets: I) -> Self {
        Self { nets: nets.into_iter().collect(), direct: false }
    }

    /// No proxy is in front: people connect directly.
    pub fn direct() -> Self {
        Self { nets: Vec::new(), direct: true }
    }

    /// Parses CIDRs (`10.0.0.0/8`, `fd00::/8`), bare addresses, or `none`
    /// alone ([`Self::direct`]) — the spelling of a `--trusted-proxy` flag
    /// (plexus-login's `TrustedProxies::parse` reads the same).
    pub fn parse<I, S>(specs: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let specs: Vec<String> =
            specs.into_iter().map(|s| s.as_ref().trim().to_string()).filter(|s| !s.is_empty()).collect();
        if specs.iter().any(|s| s == "none") {
            if specs.len() > 1 {
                return Err("trusted proxy `none` stands alone: no proxy, or name them".into());
            }
            return Ok(Self::direct());
        }
        Ok(Self::new(specs.iter().map(|s| parse_net(s)).collect::<Result<Vec<_>, _>>()?))
    }

    /// Whether anything was said: a proxy named, or `none`.
    pub fn configured(&self) -> bool {
        self.direct || !self.nets.is_empty()
    }

    /// Whether a proxy is in front (one is named).
    pub fn behind_proxy(&self) -> bool {
        !self.nets.is_empty()
    }

    /// Whether `ip` is one of the trusted proxies.
    pub fn contains(&self, ip: &IpAddr) -> bool {
        let ip = canonical(*ip);
        self.nets.iter().any(|n| n.contains(&ip))
    }

    /// The client's address for a request from `peer` carrying `headers`.
    pub fn client_ip(&self, peer: IpAddr, headers: &http::HeaderMap) -> IpAddr {
        self.resolve(peer, headers).ip
    }

    /// Who a request from `peer` carrying `headers` comes from, and whether
    /// it came securely. See the module docs.
    pub fn resolve(&self, peer: IpAddr, headers: &http::HeaderMap) -> ForwardedClient {
        let peer = canonical(peer);
        if !self.contains(&peer) {
            if headers.contains_key("x-forwarded-for") || headers.contains_key("x-forwarded-proto") {
                warn_once(
                    &UNTRUSTED_FORWARDED,
                    "X-Forwarded-For/-Proto arrives from a peer that is not a trusted proxy and is ignored: \
                     if a proxy is in front, name it (trusted proxies), or every visitor counts as that proxy",
                );
            }
            let secure = !self.behind_proxy() && peer.is_loopback();
            return ForwardedClient { ip: peer, peer, via_proxy: false, secure };
        }
        let hops = list(headers, "x-forwarded-for");
        // Walk from the hop nearest us; `at` is the client's index in `hops`
        // (None: the peer itself).
        let mut nearest_trusted = (peer, None);
        let mut client = None;
        for (i, hop) in hops.iter().enumerate().rev() {
            match parse_hop(hop) {
                Some(ip) if self.contains(&ip) => nearest_trusted = (ip, Some(i)),
                Some(ip) => {
                    client = Some((ip, Some(i)));
                    break;
                }
                None => break,
            }
        }
        let (ip, at) = client.unwrap_or(nearest_trusted);
        let protos = list(headers, "x-forwarded-proto");
        let proto = match at {
            Some(i) if protos.len() == hops.len() => protos.get(i),
            _ => protos.last(),
        };
        let secure = proto.is_some_and(|p| p.eq_ignore_ascii_case("https") || p.eq_ignore_ascii_case("wss"));
        ForwardedClient { ip, peer, via_proxy: true, secure }
    }
}

impl From<Vec<IpNet>> for TrustedProxies {
    fn from(nets: Vec<IpNet>) -> Self {
        Self::new(nets)
    }
}

/// Every header named `name`, in order, as one comma-separated list.
fn list<'a>(headers: &'a http::HeaderMap, name: &str) -> Vec<&'a str> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect()
}

static UNTRUSTED_FORWARDED: AtomicBool = AtomicBool::new(false);

fn warn_once(flag: &AtomicBool, words: &str) {
    if !flag.swap(true, Ordering::Relaxed) {
        tracing::error!("{words}");
    }
}

/// A CIDR or a bare address.
pub fn parse_net(s: &str) -> Result<IpNet, String> {
    let s = s.trim();
    s.parse::<IpNet>()
        .or_else(|_| s.parse::<IpAddr>().map(IpNet::from))
        .map_err(|_| format!("`{s}` is not a CIDR (10.0.0.0/8, fd00::/8) or an address"))
}

/// One `X-Forwarded-For` entry: an address, maybe with a port
/// (`1.2.3.4:5678`, `[::1]:5678`).
fn parse_hop(hop: &str) -> Option<IpAddr> {
    if let Ok(ip) = hop.parse::<IpAddr>() {
        return Some(canonical(ip));
    }
    hop.parse::<SocketAddr>().ok().map(|s| canonical(s.ip()))
}

/// An IPv4-mapped IPv6 address as the IPv4 address it is.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        v4 => v4,
    }
}

static TRUSTED: OnceLock<TrustedProxies> = OnceLock::new();

/// The proxies [`ClientIp`] and [`SecureTransport`] believe (they extract
/// from a [`RawRequestContext`], which carries no configuration). The first
/// call wins; `TransportServer::serve` makes it with the builder's set when
/// one was configured. Not called: no proxy is trusted.
///
/// [`ClientIp`]: crate::request::ClientIp
/// [`SecureTransport`]: crate::request::SecureTransport
/// [`RawRequestContext`]: plexus_core::request::RawRequestContext
pub fn init_trusted_proxies(proxies: TrustedProxies) {
    let _ = TRUSTED.set(proxies);
}

/// The set [`init_trusted_proxies`] made, or none.
pub fn trusted_proxies() -> &'static TrustedProxies {
    static NONE: TrustedProxies = TrustedProxies { nets: Vec::new(), direct: false };
    TRUSTED.get().unwrap_or(&NONE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn an_untrusted_peer_is_the_client_whatever_it_forwards() {
        let forged = headers(&[("x-forwarded-for", "1.1.1.1"), ("x-forwarded-proto", "https")]);
        for proxies in [TrustedProxies::default(), TrustedProxies::direct(), TrustedProxies::parse(["10.0.0.0/8"]).unwrap()] {
            let c = proxies.resolve(ip("203.0.113.9"), &forged);
            assert_eq!(c, ForwardedClient { ip: ip("203.0.113.9"), peer: ip("203.0.113.9"), via_proxy: false, secure: false }, "{proxies:?}");
        }
    }

    #[test]
    fn behind_a_trusted_proxy_the_rightmost_untrusted_hop_is_the_client() {
        let edge = TrustedProxies::parse(["10.0.0.0/8", "192.0.2.7"]).unwrap();
        let peer = ip("10.1.2.3");
        // The client claimed 6.6.6.6; the edge appended the real address.
        assert_eq!(edge.client_ip(peer, &headers(&[("x-forwarded-for", "6.6.6.6, 198.51.100.4")])), ip("198.51.100.4"));
        // Two trusted hops before the real one.
        assert_eq!(edge.client_ip(peer, &headers(&[("x-forwarded-for", "6.6.6.6, 198.51.100.4, 192.0.2.7, 10.9.9.9")])), ip("198.51.100.4"));
        // Several headers are one list; a port is dropped.
        assert_eq!(edge.client_ip(peer, &headers(&[("x-forwarded-for", "6.6.6.6"), ("x-forwarded-for", "198.51.100.4:4431")])), ip("198.51.100.4"));
        // No header: the proxy itself.
        assert_eq!(edge.client_ip(peer, &HeaderMap::new()), peer);
        // Garbage nearest us ends the walk at the last trusted hop.
        assert_eq!(edge.client_ip(peer, &headers(&[("x-forwarded-for", "6.6.6.6, not-an-ip")])), peer);
        // All trusted: the leftmost.
        assert_eq!(edge.client_ip(peer, &headers(&[("x-forwarded-for", "10.0.0.5, 10.0.0.6")])), ip("10.0.0.5"));
        // IPv4-mapped peers and bracketed v6 hops.
        let lo = TrustedProxies::parse(["127.0.0.1"]).unwrap();
        assert_eq!(lo.client_ip(ip("::ffff:127.0.0.1"), &headers(&[("x-forwarded-for", "[2001:db8::1]:443")])), ip("2001:db8::1"));
    }

    #[test]
    fn secure_only_on_a_trusted_proxys_word_and_loopback_only_with_no_proxy() {
        let edge = TrustedProxies::parse(["127.0.0.1/32"]).unwrap();
        let lo = ip("127.0.0.1");
        let https = headers(&[("x-forwarded-for", "198.51.100.4"), ("x-forwarded-proto", "https")]);
        let http = headers(&[("x-forwarded-for", "198.51.100.4"), ("x-forwarded-proto", "http")]);
        assert!(edge.resolve(lo, &https).secure);
        assert!(!edge.resolve(lo, &http).secure);
        // Behind a proxy, loopback is not secure by itself.
        assert!(!edge.resolve(lo, &HeaderMap::new()).secure);
        // With no proxy in front, a loopback peer is; a forwarded header is not.
        assert!(TrustedProxies::direct().resolve(lo, &HeaderMap::new()).secure);
        assert!(TrustedProxies::default().resolve(lo, &HeaderMap::new()).secure);
        assert!(!TrustedProxies::default().resolve(ip("203.0.113.9"), &https).secure);
        // A chain: the proto aligned with the client's hop, else the rightmost.
        let two = TrustedProxies::parse(["10.0.0.0/8"]).unwrap();
        let chain = headers(&[("x-forwarded-for", "198.51.100.4, 10.0.0.9"), ("x-forwarded-proto", "https, http")]);
        assert!(two.resolve(ip("10.0.0.1"), &chain).secure);
        let forged = headers(&[("x-forwarded-for", "198.51.100.4"), ("x-forwarded-proto", "https, http")]);
        assert!(!two.resolve(ip("10.0.0.1"), &forged).secure, "the client's own https is not the proxy's word");
    }

    #[test]
    fn parse_reads_cidrs_addresses_and_none() {
        let p = TrustedProxies::parse(["172.30.0.2/32", "fd00::/8", "10.0.0.1"]).unwrap();
        assert_eq!(p.nets.len(), 3);
        assert!(p.contains(&ip("10.0.0.1")) && p.behind_proxy() && p.configured());
        assert_eq!(TrustedProxies::parse(["none"]).unwrap(), TrustedProxies::direct());
        assert!(TrustedProxies::parse(["none", "10.0.0.0/8"]).is_err());
        assert!(TrustedProxies::parse(["edge"]).is_err());
        assert!(!TrustedProxies::parse(Vec::<String>::new()).unwrap().configured());
    }
}
