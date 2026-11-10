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
//! names for its port; beyond loopback, nothing (with a warning). [`ANY`]
//! as an entry is the explicit opt-out: every host is answered.

use std::sync::Arc;

/// KEEL-29: the entry that admits anything — in a `Host` list every host,
/// in an Origin list every origin. Written by `allow_any_host()` /
/// `allow_any_origin()`, so an open server says so in one line.
pub const ANY: &str = "*";

/// The body every refusal carries.
pub const REFUSAL: &str = "this server does not answer to that name";

/// The names a listener on loopback `port` is reached by:
/// `127.0.0.1:<port>` and `localhost:<port>`.
pub fn loopback_hosts(port: u16) -> Vec<String> {
    vec![format!("127.0.0.1:{port}"), format!("localhost:{port}")]
}

/// KEEL-29: the `Host` list a WebSocket listener bound at `bound` answers
/// to: `named` when it names any; else, bound to loopback, the loopback names
/// for the bound port (`[::1]:<port>` too on IPv6); else empty — no check, as
/// before, since a server on `0.0.0.0` is reached by names it cannot know —
/// and a warning says so once, at start.
pub(crate) fn listener_hosts(named: &[String], bound: std::net::SocketAddr) -> Vec<String> {
    if !named.is_empty() {
        return named.to_vec();
    }
    if plexus_core::plexus::remote_child::is_loopback_host(&bound.ip().to_string()) {
        let mut hosts = loopback_hosts(bound.port());
        if bound.is_ipv6() {
            hosts.push(format!("[::1]:{}", bound.port()));
        }
        return hosts;
    }
    tracing::warn!(
        addr = %bound,
        "WebSocket bound beyond loopback with no Host list: any Host is answered (DNS rebinding is not checked); \
         name this listener's hosts with with_allowed_hosts, or say allow_any_host()"
    );
    Vec::new()
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
/// empty list, or one holding [`ANY`], refuses nothing.
pub(crate) fn refused<B>(allowed: &[String], request: &http::Request<B>) -> Option<String> {
    if allowed.is_empty() || allowed.iter().any(|a| a == ANY) {
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
    fn any_refuses_nothing() {
        let list = normalized(&[ANY.to_string()]);
        assert_eq!(refused(&list, &req(Some("evil.example"))), None);
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
