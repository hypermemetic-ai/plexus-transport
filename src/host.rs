//! ACCESS-25: a `Host` allow-list on every HTTP listener — the REST bridge
//! and the WebSocket upgrade — so a page that rebinds its own name to
//! 127.0.0.1 (DNS rebinding) gets nothing: its requests name its host, not
//! one of ours.
//!
//! An entry is a `Host` header value as a client sends it: `host:port`, or
//! `host` alone for a default port (`menger.example` behind https). Entries
//! compare case-insensitively and exactly — `127.0.0.1:4455` does not admit
//! `127.0.0.1:4456`. A request whose `Host` (or, with none, its URI's
//! authority) is not listed is refused **421 Misdirected Request** before
//! anything routes it. On the REST bridge an empty list — its default —
//! checks nothing. On the WebSocket (KEEL-29) an empty list is resolved when
//! the listener binds ([`listener_hosts`]): bound to loopback, the loopback
//! names for its port; beyond loopback, nothing for the socket (with a
//! warning), and for the pages on its port the loopback names and the bound
//! address ([`page_hosts`]). Every host is answered only after the explicit
//! `allow_any_host()`; a list entry `*` is no wildcard — a listener given one
//! refuses to start ([`refuse_star`]), so no configured value opens it.

use std::sync::Arc;

/// KEEL-29: what `allow_any_origin()` / `allow_any_host()` opened — a state
/// only those calls set, never a value a list (and so a flag, an environment
/// variable or a file) can carry. [`Default`]: neither.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Opened {
    pub(crate) any_origin: bool,
    pub(crate) any_host: bool,
}

impl Opened {
    /// Whether `allow_any_origin()` was called.
    pub fn any_origin(&self) -> bool {
        self.any_origin
    }

    /// Whether `allow_any_host()` was called.
    pub fn any_host(&self) -> bool {
        self.any_host
    }
}

/// KEEL-29: `Err` when a list holds `*` — no wildcard, and before KEEL-29 no
/// match for anything — naming the call that does open the listener, so an
/// operator's `--allowed-host '*'` fails the start instead of opening it.
/// `what` is `origin` or `host`.
pub(crate) fn refuse_star(list: &[String], what: &str) -> anyhow::Result<()> {
    if list.iter().any(|e| e.trim() == "*") {
        anyhow::bail!(
            "an allowed {what} `*` is not a wildcard and is refused: to answer every {what} on purpose, \
             call allow_any_{what}() in code; otherwise name each {what}"
        );
    }
    Ok(())
}

/// The body every refusal carries.
pub const REFUSAL: &str = "this server does not answer to that name";

/// The names a listener on loopback `port` is reached by:
/// `127.0.0.1:<port>` and `localhost:<port>`.
pub fn loopback_hosts(port: u16) -> Vec<String> {
    vec![format!("127.0.0.1:{port}"), format!("localhost:{port}")]
}

fn is_loopback(bound: std::net::SocketAddr) -> bool {
    plexus_core::plexus::remote_child::is_loopback_host(&bound.ip().to_string())
}

/// The loopback names for `bound`'s port (`[::1]:<port>` too on IPv6).
fn loopback_names(bound: std::net::SocketAddr) -> Vec<String> {
    let mut hosts = loopback_hosts(bound.port());
    if bound.is_ipv6() {
        hosts.push(format!("[::1]:{}", bound.port()));
    }
    hosts
}

/// KEEL-29: the `Host` list a WebSocket listener bound at `bound` answers
/// to: `named` when it names any; else, bound to loopback, the loopback names
/// for the bound port; else empty — no check for the socket, as before,
/// since a server on `0.0.0.0` is reached by names it cannot know (a client
/// with no `Origin` is not a page) — and a warning says so once, at start.
pub(crate) fn listener_hosts(named: &[String], bound: std::net::SocketAddr) -> Vec<String> {
    if !named.is_empty() {
        return named.to_vec();
    }
    if is_loopback(bound) {
        return loopback_names(bound);
    }
    tracing::warn!(
        addr = %bound,
        "WebSocket bound beyond loopback with no Host list: the socket answers any Host (DNS rebinding is not checked), \
         and the pages on its port only the loopback names and this address; name this listener's hosts with \
         with_allowed_hosts, or say allow_any_host()"
    );
    Vec::new()
}

/// KEEL-29: the `Host` list for what a browser reaches on the listener — its
/// pages ([`crate::websocket::HostRoutes`]) and its own origin: the socket's
/// list when it names any, and with none, the loopback names plus `bound`'s
/// literal when it is neither loopback nor unspecified (as `/mcp` does), so
/// a rebound name gets no page beyond loopback either.
pub(crate) fn page_hosts(named: &[String], bound: std::net::SocketAddr) -> Vec<String> {
    if !named.is_empty() {
        return named.to_vec();
    }
    let mut hosts = loopback_names(bound);
    if !is_loopback(bound) && !bound.ip().is_unspecified() {
        hosts.push(bound.to_string());
    }
    hosts
}

/// The host a request names: its `Host`, or with none its URI's authority,
/// normalized; `Err` with what to report when it names none or not as text.
pub(crate) fn named<B>(request: &http::Request<B>) -> Result<String, String> {
    match request.headers().get(http::header::HOST) {
        Some(v) => v.to_str().map(normalize).map_err(|_| "<not text>".into()),
        None => request.uri().authority().map(|a| normalize(a.as_str())).ok_or_else(|| "<none>".into()),
    }
}

/// A host as compared: trimmed, lowercase.
fn normalize(host: &str) -> String {
    host.trim().to_ascii_lowercase()
}

/// The allow-list, normalized once.
pub(crate) fn normalized(hosts: &[String]) -> Arc<Vec<String>> {
    Arc::new(hosts.iter().map(|h| normalize(h)).filter(|h| !h.is_empty()).collect())
}

/// `Some(what was named)` when an allow-list is set and the request's host
/// is not on it; a request that names no host at all is refused too. An
/// empty list refuses nothing.
pub(crate) fn refused<B>(allowed: &[String], request: &http::Request<B>) -> Option<String> {
    if allowed.is_empty() {
        return None;
    }
    let named = match named(request) {
        Ok(n) => n,
        Err(what) => return Some(what),
    };
    (!allowed.iter().any(|a| *a == named)).then_some(named)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(host: Option<&str>) -> http::Request<()> {
        let mut b = http::Request::builder().uri("/x");
        if let Some(h) = host {
            b = b.header(http::header::HOST, h);
        }
        b.body(()).unwrap()
    }

    #[test]
    fn no_list_checks_nothing() {
        assert_eq!(refused(&normalized(&[]), &req(Some("evil.example"))), None);
        assert_eq!(refused(&normalized(&[]), &req(None)), None);
    }

    #[test]
    fn only_a_listed_host_passes() {
        let list = normalized(&[loopback_hosts(4455), vec!["Menger.Example".into()]].concat());
        for ok in ["127.0.0.1:4455", "localhost:4455", "LOCALHOST:4455", "menger.example"] {
            assert_eq!(refused(&list, &req(Some(ok))), None, "{ok}");
        }
        for bad in ["evil.example", "127.0.0.1:4456", "127.0.0.1", "menger.example:4455", "evil.example:4455"] {
            assert!(refused(&list, &req(Some(bad))).is_some(), "{bad}");
        }
        assert!(refused(&list, &req(None)).is_some(), "no host at all");
    }

    #[test]
    fn a_star_is_no_wildcard() {
        let list = normalized(&["*".to_string()]);
        assert!(refused(&list, &req(Some("evil.example"))).is_some(), "it matches no host");
        assert!(refuse_star(&[" * ".into()], "host").unwrap_err().to_string().contains("allow_any_host()"));
        assert!(refuse_star(&["*.example".into(), "a:1".into()], "host").is_ok());
    }

    #[test]
    fn a_listener_s_page_hosts() {
        let at = |a: &str| a.parse::<std::net::SocketAddr>().unwrap();
        assert_eq!(page_hosts(&[], at("127.0.0.1:4455")), loopback_hosts(4455));
        assert_eq!(page_hosts(&[], at("0.0.0.0:4455")), loopback_hosts(4455));
        assert_eq!(page_hosts(&[], at("10.0.0.5:4455")), [loopback_hosts(4455), vec!["10.0.0.5:4455".into()]].concat());
        assert_eq!(page_hosts(&["a.example".into()], at("0.0.0.0:4455")), vec!["a.example".to_string()]);
    }

    #[test]
    fn a_listener_s_default_hosts() {
        let at = |a: &str| a.parse::<std::net::SocketAddr>().unwrap();
        assert_eq!(listener_hosts(&[], at("127.0.0.1:4455")), loopback_hosts(4455));
        assert_eq!(listener_hosts(&[], at("[::1]:4455")), [loopback_hosts(4455), vec!["[::1]:4455".into()]].concat());
        assert!(listener_hosts(&[], at("0.0.0.0:4455")).is_empty(), "beyond loopback: unchecked, as before");
        assert!(listener_hosts(&[], at("10.0.0.5:4455")).is_empty());
        assert_eq!(listener_hosts(&["menger.example".into()], at("127.0.0.1:4455")), vec!["menger.example".to_string()]);
    }

    #[test]
    fn the_authority_stands_in_for_a_missing_host() {
        let list = normalized(&loopback_hosts(1));
        let r = http::Request::builder().uri("http://127.0.0.1:1/x").body(()).unwrap();
        assert_eq!(refused(&list, &r), None);
        let r = http::Request::builder().uri("http://evil.example/x").body(()).unwrap();
        assert_eq!(refused(&list, &r).as_deref(), Some("evil.example"));
    }
}
