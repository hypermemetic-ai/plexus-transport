//! TRANSPORT-7: plexus-transport behind a proxy. The test's own sockets come
//! from 127.0.0.1, so a server that trusts 127.0.0.1/32 sees them as a fake
//! proxy peer (its X-Forwarded-For is who the visitor is), and one that
//! trusts only 10.0.0.0/8 sees an untrusted peer forging the same headers.
//! Connection quotas count the forwarded client; forged headers are
//! ignored; secure comes only from a trusted proxy's X-Forwarded-Proto, and
//! behind a proxy loopback is not assumed secure.

use std::sync::Arc;

use async_trait::async_trait;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use jsonrpsee::RpcModule;
use plexus_core::plexus::{AuthContext, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{ConnectionQuotas, ForwardedClient, TrustedProxies, WebSocketConfig};
use serde_json::{json, Value};

/// A bearer token is the user's name.
struct Names;

#[async_trait]
impl SessionValidator for Names {
    async fn validate(&self, token: &str) -> Option<AuthContext> {
        let name = token.trim();
        (!name.is_empty()).then(|| AuthContext { user_id: name.into(), session_id: "s".into(), roles: vec![], metadata: json!({}) })
    }
}

const SMALL: ConnectionQuotas =
    ConnectionQuotas { max_connections: 100, max_per_ip: 5, max_per_principal: 50, max_anonymous: 50, max_anonymous_per_ip: 2 };

async fn server(trusted: &[&str]) -> (jsonrpsee::server::ServerHandle, u16) {
    let mut module = RpcModule::new(());
    // What the application sees of its caller: the upgrade's ForwardedClient.
    module
        .register_method("who", |_, _, ext| {
            let c = ext.get::<ForwardedClient>().copied().expect("every upgrade carries its client");
            json!({ "ip": c.ip.to_string(), "peer": c.peer.to_string(), "via_proxy": c.via_proxy, "secure": c.secure })
        })
        .unwrap();
    let config = WebSocketConfig::new(0)
        .with_trusted_proxies(TrustedProxies::parse(trusted).unwrap());
    let config = WebSocketConfig { quotas: SMALL, ..config };
    let options = UpgradeOptions { anonymous_view: true, ..Default::default() };
    let (handle, addr) = serve_websocket_with(module, config, Some(Arc::new(Names)), options).await.unwrap();
    (handle, addr.port())
}

async fn open(port: u16, headers: &[(&'static str, &str)]) -> Result<(WsClient, Value), String> {
    let mut map = http::HeaderMap::new();
    for (k, v) in headers {
        map.append(*k, v.parse().unwrap());
    }
    let c = WsClientBuilder::default()
        .set_headers(map)
        .build(format!("ws://127.0.0.1:{port}"))
        .await
        .map_err(|e| e.to_string())?;
    let who: Value = c.request("who", rpc_params![]).await.map_err(|e| e.to_string())?;
    Ok((c, who))
}

fn from(ip: &str) -> Vec<(&'static str, &str)> {
    vec![("x-forwarded-for", ip)]
}

#[tokio::test]
async fn behind_a_trusted_proxy_the_quotas_count_each_forwarded_client() {
    let (handle, port) = server(&["127.0.0.1/32"]).await;
    let mut held = Vec::new();

    // Two anonymous visitors from one address fill its anonymous quota…
    for _ in 0..2 {
        let (c, who) = open(port, &from("198.51.100.1")).await.expect("within the visitor's quota");
        assert_eq!(who["ip"], "198.51.100.1");
        assert_eq!(who["peer"], "127.0.0.1");
        assert_eq!(who["via_proxy"], true);
        held.push(c);
    }
    let refused = open(port, &from("198.51.100.1")).await.unwrap_err();
    assert!(refused.contains("429"), "the 3rd anonymous socket from one visitor: {refused}");

    // …and the next visitor through the same proxy is not locked out.
    held.push(open(port, &from("198.51.100.2")).await.expect("another visitor").0);
    held.push(open(port, &from("198.51.100.2")).await.expect("another visitor, twice").0);

    // A visitor cannot step out of its quota by prepending an address: the
    // client is the rightmost hop the proxy appended.
    let (c, who) = open(port, &from("6.6.6.6, 198.51.100.3")).await.unwrap();
    assert_eq!(who["ip"], "198.51.100.3");
    held.push(c);
    assert!(open(port, &from("6.6.6.6, 198.51.100.1")).await.unwrap_err().contains("429"));

    // The per-IP quota (signed in or not) is per visitor too.
    for i in 0..5 {
        let me = format!("u{i}");
        let bearer = format!("Bearer {me}");
        held.push(open(port, &[("x-forwarded-for", "198.51.100.9"), ("authorization", &bearer)]).await.expect("per-IP quota").0);
    }
    let over = open(port, &[("x-forwarded-for", "198.51.100.9"), ("authorization", "Bearer u9")]).await.unwrap_err();
    assert!(over.contains("429"), "the 6th socket from one visitor: {over}");
    // Nine sockets are open from the one peer: per-IP (5) would have
    // refused the 6th had it counted the proxy.
    assert!(held.len() > 5);

    // The proxy itself (no X-Forwarded-For) is a client of its own.
    let (_c, who) = open(port, &[]).await.unwrap();
    assert_eq!(who["ip"], "127.0.0.1");
    handle.stop().unwrap();
}

#[tokio::test]
async fn forged_forwarded_headers_from_an_untrusted_peer_are_ignored() {
    // 10.0.0.0/8 is the proxy; the test's 127.0.0.1 is just a client.
    let (handle, port) = server(&["10.0.0.0/8"]).await;
    let (a, who) = open(port, &[("x-forwarded-for", "198.51.100.1"), ("x-forwarded-proto", "https")]).await.unwrap();
    assert_eq!(who["ip"], "127.0.0.1", "the peer, not what it forwarded");
    assert_eq!(who["via_proxy"], false);
    assert_eq!(who["secure"], false, "its https is not believed, and loopback is not secure behind a proxy");
    // Rotating forged addresses does not buy more anonymous sockets.
    let (b, _) = open(port, &from("198.51.100.2")).await.unwrap();
    let refused = open(port, &from("198.51.100.3")).await.unwrap_err();
    assert!(refused.contains("429"), "every forged address is the one peer: {refused}");
    drop((a, b));
    handle.stop().unwrap();
}

#[tokio::test]
async fn secure_transport_comes_from_a_trusted_proxys_forwarded_proto_only() {
    let (handle, port) = server(&["127.0.0.1/32"]).await;
    let (_a, who) = open(port, &[("x-forwarded-for", "198.51.100.1"), ("x-forwarded-proto", "https")]).await.unwrap();
    assert_eq!(who["secure"], true, "the proxy says https");
    let (_b, who) = open(port, &[("x-forwarded-for", "198.51.100.2"), ("x-forwarded-proto", "http")]).await.unwrap();
    assert_eq!(who["secure"], false, "the proxy says http");
    let (_c, who) = open(port, &from("198.51.100.3")).await.unwrap();
    assert_eq!(who["secure"], false, "behind a proxy, a loopback peer with no word is not secure");
    handle.stop().unwrap();

    // With no proxy in front (`none`), a loopback peer is, and a forwarded
    // header is still not believed.
    let (handle, port) = server(&["none"]).await;
    let (_d, who) = open(port, &[("x-forwarded-for", "198.51.100.4"), ("x-forwarded-proto", "http")]).await.unwrap();
    assert_eq!(who["secure"], true);
    assert_eq!(who["ip"], "127.0.0.1");
    handle.stop().unwrap();
}
