//! TRANSPORT-23: the old path, `with_mcp_http`, on rmcp 3.8: served, gated by
//! the key, and answering only the hosts it allows.
#![cfg(feature = "mcp")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use plexus_core::activations::echo::EchoActivation;
use plexus_core::plexus::DynamicHub;
#[cfg(feature = "__mcp-test-client")]
use plexus_transport::RouteFn;
use plexus_transport::{McpHttpConfig, TransportServer, TransportServerBuilder};
use reqwest::StatusCode;
use serde_json::json;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn builder() -> (Arc<DynamicHub>, TransportServerBuilder<DynamicHub>) {
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let builder = TransportServer::builder(hub.clone(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .without_registry()
        .allow_missing_auth_middleware();
    (hub, builder)
}

#[cfg(feature = "__mcp-test-client")]
fn route_fn(hub: Arc<DynamicHub>) -> RouteFn {
    Arc::new(move |method, params| {
        let hub = hub.clone();
        Box::pin(async move { hub.route(&method, params, None).await })
    })
}

async fn up(port: u16) {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing listening on {addr}");
}

/// A raw `initialize`, for the HTTP-level answers.
async fn initialize(port: u16, host: Option<&str>, bearer: Option<&str>) -> StatusCode {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
    });
    let mut request = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .json(&body);
    if let Some(host) = host {
        request = request.header("host", host);
    }
    if let Some(bearer) = bearer {
        request = request.bearer_auth(bearer);
    }
    request.send().await.unwrap().status()
}

#[cfg(feature = "__mcp-test-client")]
async fn lists_and_calls_echo(port: u16) {
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::ServiceExt;

    let transport = StreamableHttpClientTransport::with_client(
        reqwest::Client::default(),
        StreamableHttpClientTransportConfig::with_uri(format!("http://127.0.0.1:{port}/mcp")),
    );
    let client = ().serve(transport).await.expect("initialize");
    let names: Vec<String> = client.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    assert!(names.iter().any(|n| n == "echo.echo"), "the hub's activations are the tools: {names:?}");

    let r = client
        .peer()
        .call_tool(CallToolRequestParams::new("echo.echo").with_arguments(json!({"message": "hi", "count": 1}).as_object().cloned().unwrap()))
        .await
        .unwrap();
    let text: String = r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect();
    assert_ne!(r.is_error, Some(true), "{text}");
    assert!(text.contains("hi"), "{text}");
}

#[cfg(feature = "__mcp-test-client")]
#[tokio::test(flavor = "multi_thread")]
async fn with_mcp_http_lists_a_hubs_tools_and_calls_them_from_an_rmcp_3_client() {
    let port = free_port();
    let (hub, builder) = builder();
    let schemas = hub.list_plugin_schemas();
    let server = builder.with_flat_schemas(schemas).with_route_fn(route_fn(hub)).with_mcp_http(port).build().await.unwrap();
    tokio::spawn(server.serve());
    up(port).await;
    lists_and_calls_echo(port).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn with_mcp_http_and_a_key_refuses_all_but_that_key() {
    let port = free_port();
    let (_, builder) = builder();
    let server = builder.with_api_key(Some("k".to_string())).with_mcp_http(port).build().await.unwrap();
    tokio::spawn(server.serve());
    up(port).await;

    assert_eq!(initialize(port, None, None).await, StatusCode::UNAUTHORIZED);
    assert_eq!(initialize(port, None, Some("wrong")).await, StatusCode::UNAUTHORIZED);
    assert_eq!(initialize(port, None, Some("k")).await, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn with_mcp_http_answers_loopback_names_and_refuses_any_other_host() {
    let port = free_port();
    let (_, builder) = builder();
    let server = builder.with_mcp_http(port).build().await.unwrap();
    tokio::spawn(server.serve());
    up(port).await;

    assert_eq!(initialize(port, None, None).await, StatusCode::OK);
    assert_eq!(initialize(port, Some(&format!("localhost:{port}")), None).await, StatusCode::OK);
    assert_eq!(initialize(port, Some("evil.example"), None).await, StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_http_config_names_the_hosts_it_answers_to() {
    let port = free_port();
    let (_, builder) = builder();
    let config = McpHttpConfig::new(port).with_allowed_hosts(["mcp.example"]);
    let server = builder.with_mcp_http_config(config).build().await.unwrap();
    tokio::spawn(server.serve());
    up(port).await;

    assert_eq!(initialize(port, Some("mcp.example"), None).await, StatusCode::OK);
    assert_eq!(initialize(port, Some("evil.example"), None).await, StatusCode::FORBIDDEN);
    assert_eq!(initialize(port, None, None).await, StatusCode::FORBIDDEN, "a named list replaces the loopback names");
}

#[cfg(all(feature = "sqlite-sessions", feature = "__mcp-test-client"))]
#[tokio::test(flavor = "multi_thread")]
async fn sqlite_sessions_serve_an_rmcp_3_client() {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("plexus-transport-mcp-http-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (hub, builder) = builder();
    let schemas = hub.list_plugin_schemas();
    let config = McpHttpConfig::new(port).with_sqlite(dir.join("sessions.db"));
    let server = builder.with_flat_schemas(schemas).with_route_fn(route_fn(hub)).with_mcp_http_config(config).build().await.unwrap();
    tokio::spawn(server.serve());
    up(port).await;
    lists_and_calls_echo(port).await;
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(all(feature = "mcp-gateway", feature = "__mcp-test-client"))]
#[tokio::test(flavor = "multi_thread")]
async fn serve_combined_serves_an_rmcp_3_client_and_refuses_any_other_host() {
    let port = free_port();
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let module = DynamicHub::arc_into_rpc_module(hub.clone()).unwrap();
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let schemas = hub.list_plugin_schemas();
    let _handle = plexus_transport::serve_combined(module, hub.clone(), Some(schemas), Some(route_fn(hub)), addr, None, false).await.unwrap();
    up(port).await;

    lists_and_calls_echo(port).await;
    assert_eq!(initialize(port, Some("evil.example"), None).await, StatusCode::FORBIDDEN);
}

#[cfg(feature = "mcp-gateway")]
#[tokio::test(flavor = "multi_thread")]
async fn serve_combined_with_allowed_hosts_names_the_hosts_it_answers_to() {
    let port = free_port();
    let hub = Arc::new(DynamicHub::new("x").register(EchoActivation::new()));
    let module = DynamicHub::arc_into_rpc_module(hub.clone()).unwrap();
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let _handle = plexus_transport::serve_combined_with_allowed_hosts(module, hub, None, None, addr, None, false, vec!["mcp.example".to_string()]).await.unwrap();
    up(port).await;

    assert_eq!(initialize(port, Some("mcp.example"), None).await, StatusCode::OK);
    assert_eq!(initialize(port, Some("evil.example"), None).await, StatusCode::FORBIDDEN);
}
