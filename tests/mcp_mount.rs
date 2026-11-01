//! TRANSPORT-16: the bridge mounted in-process — `with_mcp(McpConfig)` over
//! the service, driven by an rmcp 3.x client; the same bridge the sidecar runs.
#![cfg(feature = "mcp")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use plexus_core::activations::echo::EchoActivation;
use plexus_core::plexus::DynamicHub;
use plexus_transport::{McpConfig, TransportServer};
use rmcp3::model::CallToolRequestParams;
use rmcp3::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp3::transport::StreamableHttpClientTransport;
use rmcp3::ServiceExt;
use serde_json::json;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mounted_bridge_lists_the_hubs_tools_and_calls_them() {
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let mcp: SocketAddr = ([127, 0, 0, 1], free_port()).into();
    let server = TransportServer::builder(hub, |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(free_port())
        .without_registry()
        .allow_missing_auth_middleware()
        .with_mcp(McpConfig::listen(mcp))
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(mcp).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let transport = StreamableHttpClientTransport::with_client(
        reqwest::Client::default(),
        StreamableHttpClientTransportConfig::with_uri(format!("http://{mcp}/mcp")),
    );
    let client = ().serve(transport).await.expect("initialize");
    let names: Vec<String> = client.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    assert!(names.iter().any(|n| n.starts_with("echo.")), "the service's activations are the tools: {names:?}");

    let echo = names.iter().find(|n| *n == "echo.echo").expect("echo.echo");
    let r = client
        .peer()
        .call_tool(CallToolRequestParams::new(echo.clone()).with_arguments(json!({"message": "hi", "count": 1}).as_object().cloned().unwrap()))
        .await
        .unwrap();
    let text: String = r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect();
    assert_ne!(r.is_error, Some(true), "{text}");
    assert!(text.contains("hi"), "{text}");
}

/// A key is a gate the mount never reads: admitting a caller with no
/// credential would walk round it.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_with_a_mount_that_admits_anonymous_callers_refuses_to_start() {
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let mcp: SocketAddr = ([127, 0, 0, 1], free_port()).into();
    let err = TransportServer::builder(hub, |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(free_port())
        .without_registry()
        .with_api_key(Some("k".to_string()))
        .allow_missing_auth_middleware()
        .with_mcp(McpConfig::listen(mcp))
        .build()
        .await
        .err()
        .expect("the mount is not gated by the key");
    assert!(err.to_string().contains("anonymous"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_with_a_mount_closed_to_anonymous_callers_starts_and_refuses_them() {
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let mcp: SocketAddr = ([127, 0, 0, 1], free_port()).into();
    let config = McpConfig { anonymous: false, ..McpConfig::listen(mcp) };
    let server = TransportServer::builder(hub, |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .with_websocket(free_port())
        .without_registry()
        .with_api_key(Some("k".to_string()))
        .with_mcp(config)
        .build()
        .await
        .unwrap();
    tokio::spawn(server.serve());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(mcp).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let r = reqwest::Client::new().post(format!("http://{mcp}/mcp")).json(&body).send().await.unwrap();
    assert_eq!(r.status(), reqwest::StatusCode::UNAUTHORIZED);
}
