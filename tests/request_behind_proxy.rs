//! TRANSPORT-7: `ClientIp` and `SecureTransport` extracted behind a trusted
//! proxy (the process-wide set `init_trusted_proxies` makes — this binary
//! sets it once: the edge is 172.30.0.2, secure transport required).

use plexus_core::request::PlexusRequestField;
use plexus_transport::request::{ClientIp, RawRequestContext, SecureTransport};
use plexus_transport::{init_trusted_proxies, TrustedProxies};

fn raw(headers: &[(&'static str, &str)], peer: &str) -> RawRequestContext {
    let mut h = http::HeaderMap::new();
    for (k, v) in headers {
        h.append(*k, v.parse().unwrap());
    }
    RawRequestContext { headers: h, uri: "/".parse().unwrap(), auth: None, peer: Some(peer.parse().unwrap()) }
}

fn setup() {
    init_trusted_proxies(TrustedProxies::parse(["172.30.0.2/32"]).unwrap());
    plexus_transport::request::init_require_secure_transport(true);
}

#[test]
fn client_ip_is_the_forwarded_client_only_through_the_proxy() {
    setup();
    let via_edge = raw(&[("x-forwarded-for", "6.6.6.6, 198.51.100.7")], "172.30.0.2:5000");
    assert_eq!(ClientIp::extract_from_raw(&via_edge).unwrap().0.to_string(), "198.51.100.7");
    let forged = raw(&[("x-forwarded-for", "198.51.100.7"), ("x-real-ip", "198.51.100.7")], "203.0.113.5:5000");
    assert_eq!(ClientIp::extract_from_raw(&forged).unwrap().0.to_string(), "203.0.113.5", "an untrusted peer's headers are ignored");
}

#[test]
fn secure_transport_only_on_the_proxys_word() {
    setup();
    let https = raw(&[("x-forwarded-for", "198.51.100.7"), ("x-forwarded-proto", "https")], "172.30.0.2:5000");
    assert!(SecureTransport::extract_from_raw(&https).is_ok());
    let http = raw(&[("x-forwarded-for", "198.51.100.7"), ("x-forwarded-proto", "http")], "172.30.0.2:5000");
    assert!(SecureTransport::extract_from_raw(&http).is_err());
    let forged = raw(&[("x-forwarded-proto", "https")], "203.0.113.5:5000");
    assert!(SecureTransport::extract_from_raw(&forged).is_err(), "an untrusted peer's https is not believed");
    let loopback = raw(&[], "127.0.0.1:5000");
    assert!(SecureTransport::extract_from_raw(&loopback).is_err(), "behind a proxy, loopback is not assumed secure");
}

#[test]
fn with_no_proxy_in_front_loopback_is_secure_and_forwarded_proto_is_not() {
    let direct = TrustedProxies::direct();
    assert!(SecureTransport::check(&raw(&[], "127.0.0.1:5000"), &direct).is_ok());
    assert!(SecureTransport::check(&raw(&[("x-forwarded-proto", "https")], "203.0.113.5:5000"), &direct).is_err());
    assert_eq!(ClientIp::resolve(&raw(&[("x-forwarded-for", "1.1.1.1")], "203.0.113.5:5000"), &direct).0.to_string(), "203.0.113.5");
}
