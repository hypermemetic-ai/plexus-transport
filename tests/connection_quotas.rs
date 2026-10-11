//! AUTH-12: connection quotas at the WebSocket upgrade — per IP, per
//! signed-in principal, and for anonymous sockets, held below the total, so
//! anonymous sockets can never lock a signed-in caller out (AUTH-10 #2: 100
//! anonymous sockets took jsonrpsee's default 100 and the admin's 101st was
//! refused).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use jsonrpsee::RpcModule;
use plexus_core::plexus::{AuthContext, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::{ConnectionQuotas, WebSocketConfig};
use serde_json::json;

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
    ConnectionQuotas { max_connections: 100, max_per_ip: 7, max_per_principal: 2, max_anonymous: 4, max_anonymous_per_ip: 3 };

async fn server(validator: Option<Arc<dyn SessionValidator>>) -> (jsonrpsee::server::ServerHandle, u16) {
    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    let mut config = WebSocketConfig::new(0);
    config.quotas = SMALL;
    let options = UpgradeOptions { anonymous_view: true, ..Default::default() };
    let (handle, addr) = serve_websocket_with(module, config, validator, options).await.unwrap();
    (handle, addr.port())
}

async fn open(port: u16, as_who: Option<&str>) -> Result<WsClient, String> {
    let mut headers = http::HeaderMap::new();
    if let Some(t) = as_who {
        headers.insert(http::header::AUTHORIZATION, format!("Bearer {t}").parse().unwrap());
    }
    let c = WsClientBuilder::default()
        .set_headers(headers)
        .build(format!("ws://127.0.0.1:{port}"))
        .await
        .map_err(|e| e.to_string())?;
    let pong: String = c.request("ping", rpc_params![]).await.map_err(|e| e.to_string())?;
    assert_eq!(pong, "pong");
    Ok(c)
}

#[tokio::test]
async fn anonymous_sockets_cannot_lock_a_signed_in_caller_out() {
    let (handle, port) = server(Some(Arc::new(Names))).await;
    let mut anon = Vec::new();
    for _ in 0..3 {
        anon.push(open(port, None).await.expect("within the anonymous quota"));
    }
    let refused = open(port, None).await.unwrap_err();
    println!("the 4th anonymous socket: {refused}");
    assert!(refused.contains("429"), "{refused}");

    // The flood does not reach the signed-in.
    let admin = open(port, Some("admin")).await.expect("a signed-in caller still gets in");
    let admin2 = open(port, Some("admin")).await.expect("twice");
    let third = open(port, Some("admin")).await.unwrap_err();
    assert!(third.contains("429"), "per principal: {third}");
    let bob = open(port, Some("bob")).await.expect("another principal");

    // 3 anonymous + 3 signed in from this IP; the 7th of anyone is the last.
    let carol = open(port, Some("carol")).await.expect("the 7th from this IP");
    let over = open(port, Some("dave")).await.unwrap_err();
    assert!(over.contains("429"), "per IP: {over}");

    // A socket closed gives its place back.
    drop(anon.pop());
    drop(carol);
    tokio::time::sleep(Duration::from_millis(300)).await;
    anon.push(open(port, None).await.expect("a freed anonymous place"));
    drop((admin, admin2, bob));
    handle.stop().unwrap();
}

#[tokio::test]
async fn without_a_validator_no_socket_is_anonymous_only_the_ip_counts() {
    let (handle, port) = server(None).await;
    let mut socks = Vec::new();
    for _ in 0..7 {
        socks.push(open(port, None).await.expect("within the per-IP quota"));
    }
    assert!(open(port, None).await.unwrap_err().contains("429"));
    handle.stop().unwrap();
}

#[test]
fn the_defaults_hold_anonymous_well_below_the_total() {
    let q = ConnectionQuotas::default();
    assert!(q.max_anonymous * 4 <= q.max_connections, "{q:?}");
    assert!(q.max_per_ip < q.max_connections && q.max_anonymous_per_ip < q.max_anonymous);
    assert!(q.max_connections > 100, "set explicitly, above jsonrpsee's default");
}
