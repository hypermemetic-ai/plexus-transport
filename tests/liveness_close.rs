//! AUTH-11: a socket whose sign-in ends is closed by the server — not left
//! open until its next call. The upgrade's validator is asked
//! ([`SessionValidator::alive`]) while the socket stays open.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jsonrpsee::core::client::{ClientT, Error as ClientError};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use jsonrpsee::RpcModule;
use plexus_core::plexus::{AuthContext, SessionValidator};
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions, LIVENESS_EVERY};
use plexus_transport::WebSocketConfig;
use serde_json::json;

/// `good` signs in as alice (session s1) and `other` as bob (session s2);
/// `ended` signs s1 out.
#[derive(Default)]
struct Sessions {
    ended: AtomicBool,
}

#[async_trait]
impl SessionValidator for Sessions {
    async fn validate(&self, token: &str) -> Option<AuthContext> {
        let (user, session) = match token.trim() {
            "good" => ("alice", "s1"),
            "other" => ("bob", "s2"),
            _ => return None,
        };
        Some(AuthContext { user_id: user.into(), session_id: session.into(), roles: vec![], metadata: json!({}) })
    }

    fn alive(&self, ctx: &AuthContext) -> Result<(), String> {
        if ctx.session_id == "s1" && self.ended.load(Ordering::SeqCst) {
            return Err("you have signed out: sign in again".into());
        }
        Ok(())
    }
}

async fn server(sessions: Arc<Sessions>) -> (jsonrpsee::server::ServerHandle, u16) {
    let mut module = RpcModule::new(());
    module.register_method("ping", |_, _, _| "pong").unwrap();
    let config = WebSocketConfig::new(0);
    let (handle, addr) = serve_websocket_with(module, config, Some(sessions), UpgradeOptions { anonymous_view: true, ..Default::default() })
        .await
        .unwrap();
    (handle, addr.port())
}

async fn client(port: u16, token: Option<&str>) -> jsonrpsee::ws_client::WsClient {
    let mut headers = http::HeaderMap::new();
    if let Some(t) = token {
        headers.insert(http::header::AUTHORIZATION, format!("Bearer {t}").parse().unwrap());
    }
    WsClientBuilder::default().set_headers(headers).build(format!("ws://127.0.0.1:{port}")).await.unwrap()
}

#[tokio::test]
async fn a_signed_out_socket_is_closed_and_the_others_stay() {
    let sessions = Arc::new(Sessions::default());
    let (handle, port) = server(sessions.clone()).await;
    let alice = client(port, Some("good")).await;
    let bob = client(port, Some("other")).await;
    let anon = client(port, None).await;
    let pong: String = alice.request("ping", rpc_params![]).await.unwrap();
    assert_eq!(pong, "pong");

    sessions.ended.store(true, Ordering::SeqCst);
    tokio::time::sleep(LIVENESS_EVERY * 3).await;

    assert!(!alice.is_connected(), "the signed-out socket is closed");
    assert!(matches!(alice.request::<String, _>("ping", rpc_params![]).await, Err(ClientError::RestartNeeded(_))));
    // Another session's socket, and an anonymous one, are untouched.
    assert_eq!(bob.request::<String, _>("ping", rpc_params![]).await.unwrap(), "pong");
    assert_eq!(anon.request::<String, _>("ping", rpc_params![]).await.unwrap(), "pong");
    handle.stop().unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.stopped()).await.expect("the server stops");
}

#[tokio::test]
async fn stopping_the_server_closes_every_socket() {
    let (handle, port) = server(Arc::new(Sessions::default())).await;
    let alice = client(port, Some("good")).await;
    let anon = client(port, None).await;
    handle.stop().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!alice.is_connected());
    assert!(!anon.is_connected());
}
