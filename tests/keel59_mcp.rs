//! KEEL-59 · an MCP client that goes away stops its call, on both bridges;
//! on the legacy bridge a cancel takes effect at once, not at the method's
//! next item.
//!
//! The method's work ticks a counter where it runs; each row measures that
//! the counter stopped within [`BOUND`] of the let-go and stays stopped.
#![cfg(feature = "__mcp-test-client")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use keel59_fixture::{Counter, WorkActivation};
use plexus_core::plexus::{Activation, DynamicHub};
use plexus_transport::{McpConfig, RouteFn, TransportServer};
use rmcp::model::{CallToolRequest, CallToolRequestParams, ClientRequest};
use rmcp::service::PeerRequestOptions;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::json;
use tokio::net::TcpStream;

const BOUND: Duration = Duration::from_millis(100);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn up(addr: SocketAddr) {
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing on {addr}");
}

#[derive(Clone, Copy, Debug)]
enum Bridge {
    /// `with_mcp`: plexus-transport-mcp, over the service in memory.
    Mounted,
    /// `with_mcp_http`: the activation bridge.
    Legacy,
}

/// The service behind `bridge`; its work's counter.
async fn served(bridge: Bridge) -> (SocketAddr, Counter) {
    let activation = WorkActivation::default();
    let work = activation.lanes[0].clone();
    let hub = Arc::new(DynamicHub::new("svc").register(activation));
    let port = free_port();
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let b = TransportServer::builder(hub.clone(), |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!(e)))
        .without_registry()
        .allow_missing_auth_middleware();
    let server = match bridge {
        Bridge::Mounted => b.with_websocket(free_port()).with_mcp(McpConfig::listen(addr)).build().await.unwrap(),
        Bridge::Legacy => {
            let route: RouteFn = {
                let hub = hub.clone();
                Arc::new(move |method, params| {
                    let hub = hub.clone();
                    Box::pin(async move { hub.route(&method, params, None).await })
                })
            };
            let schemas = vec![WorkActivation::default().plugin_schema()];
            b.with_flat_schemas(schemas).with_route_fn(route).with_mcp_http(port).build().await.unwrap()
        }
    };
    tokio::spawn(server.serve());
    up(addr).await;
    (addr, work)
}

/// How the client lets go of its call.
#[derive(Clone, Copy, Debug)]
enum LetGo {
    /// `notifications/cancelled`.
    Cancels,
    /// The client closes its transport (its session is deleted).
    GoesAway,
}

/// Call `tool` through `bridge`, let go `how` once its work is under way:
/// the work stopped within [`BOUND`].
async fn row(bridge: Bridge, tool: &str, how: LetGo) {
    let (addr, work) = served(bridge).await;
    let transport = StreamableHttpClientTransport::with_client(reqwest::Client::default(), StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")));
    let client = ().serve(transport).await.expect("initialize");
    let names: Vec<String> = client.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    let name = names.iter().find(|n| n.ends_with(tool)).unwrap_or_else(|| panic!("{tool} in {names:?}")).clone();
    let args = json!({ "lane": 0 }).as_object().cloned().unwrap();
    let req = ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(name).with_arguments(args)));
    let handle = client.peer().send_cancellable_request(req, PeerRequestOptions::no_options()).await.unwrap();
    work.going().await;
    let cause = Instant::now();
    match how {
        LetGo::Cancels => {
            handle.cancel(Some("the test".into())).await.unwrap();
        }
        LetGo::GoesAway => {
            drop(handle);
            let _ = tokio::time::timeout(Duration::from_secs(3), client.cancel()).await;
        }
    }
    let ms = work.stopped(cause, BOUND).await;
    println!("    {bridge:?} {tool} {how:?}: the work stopped {ms:+.1} ms from the let-go");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_mcp_client_that_goes_away_stops_its_call() {
    for bridge in [Bridge::Mounted, Bridge::Legacy] {
        for tool in ["quiet", "chatty", "slow"] {
            row(bridge, tool, LetGo::GoesAway).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_stops_the_call_at_once_on_either_bridge() {
    for bridge in [Bridge::Mounted, Bridge::Legacy] {
        // A stream that says nothing, and a unary method still running:
        // neither has a next item to notice the cancel at.
        for tool in ["quiet", "slow"] {
            row(bridge, tool, LetGo::Cancels).await;
        }
    }
}
