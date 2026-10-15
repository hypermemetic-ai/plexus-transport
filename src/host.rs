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
//! anything routes it. An empty list — the default — checks nothing, so a
//! server that names no hosts behaves as it always has.

use std::sync::Arc;

/// The body every refusal carries.
pub const REFUSAL: &str = "this server does not answer to that name";

/// The names a listener on loopback `port` is reached by:
/// `127.0.0.1:<port>` and `localhost:<port>`.
pub fn loopback_hosts(port: u16) -> Vec<String> {
    vec![format!("127.0.0.1:{port}"), format!("localhost:{port}")]
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
/// is not on it; a request that names no host at all is refused too.
pub(crate) fn refused<B>(allowed: &[String], request: &http::Request<B>) -> Option<String> {
    if allowed.is_empty() {
        return None;
    }
    let named = match request.headers().get(http::header::HOST) {
        Some(v) => match v.to_str() {
            Ok(s) => normalize(s),
            Err(_) => return Some("<not text>".into()),
        },
        None => match request.uri().authority() {
            Some(a) => normalize(a.as_str()),
            None => return Some("<none>".into()),
        },
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
    fn the_authority_stands_in_for_a_missing_host() {
        let list = normalized(&loopback_hosts(1));
        let r = http::Request::builder().uri("http://127.0.0.1:1/x").body(()).unwrap();
        assert_eq!(refused(&list, &r), None);
        let r = http::Request::builder().uri("http://evil.example/x").body(()).unwrap();
        assert_eq!(refused(&list, &r).as_deref(), Some("evil.example"));
    }
}
