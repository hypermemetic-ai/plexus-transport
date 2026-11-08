//! ASK-5 · a method that asks (`#[plexus_macros::method(asks)]`, plexus-wire
//! spec/08), proposing then confirming, over every way this crate serves it:
//!
//! | path | the proposal | the answer |
//! | -- | -- | -- |
//! | the WebSocket (`svc.call`) | a `data` item `{"type":"needs_input",…}` whose `content_type` is `ledger.bump`, then `done` | the same call with `confirm`: `bumped`; the same token again: a fresh proposal |
//! | MCP, mounted over the service (`ServiceUpstream`, what `with_mcp` serves) | a successful tool result saying what would happen and how to call again | the same tool, the same arguments, `confirm` |
//! | MCP, the legacy activation bridge | the same tool result | the same |
//!
//! No server-to-client question and no `{ns}.respond`: two ordinary calls.

use std::sync::Arc;
use std::time::Duration;

use ask_fixture::{gated, LedgerActivation, Users};
use futures::{SinkExt, StreamExt};
use plexus_core::plexus::SessionValidator;
use plexus_transport::websocket::{serve_websocket_with, UpgradeOptions};
use plexus_transport::WebSocketConfig;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

async fn serve(ledger: LedgerActivation) -> u16 {
    let module = plexus_core::DynamicHub::arc_into_rpc_module(gated(ledger)).unwrap();
    let validator: Arc<dyn SessionValidator> = Arc::new(Users);
    let (handle, addr) = serve_websocket_with(module, WebSocketConfig::new(0), Some(validator), UpgradeOptions::default()).await.unwrap();
    std::mem::forget(handle);
    addr.port()
}

async fn signed_in(port: u16, who: &str) -> Ws {
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut req = format!("ws://127.0.0.1:{port}/").into_client_request().unwrap();
    req.headers_mut().insert("authorization", format!("Bearer {who}").parse().unwrap());
    tokio_tungstenite::client_async(req, tcp).await.unwrap().0
}

async fn next(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next()).await.expect("a frame in time").expect("open").unwrap();
        match msg {
            Message::Text(t) => return serde_json::from_str(t.as_str()).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame {other:?}"),
        }
    }
}

/// One `svc.call` of `ledger.bump`: each item of its stream, to `done`.
async fn bump(ws: &mut Ws, id: u64, params: Value) -> Vec<Value> {
    let frame = json!({"jsonrpc": "2.0", "id": id, "method": "svc.call", "params": {"method": "ledger.bump", "params": params}});
    ws.send(Message::text(frame.to_string())).await.unwrap();
    let ack = next(ws).await;
    assert_eq!(ack["id"], id, "the subscription's id first: {ack}");
    let sub = ack["result"].clone();
    let mut items = Vec::new();
    loop {
        let f = next(ws).await;
        assert_eq!(f["params"]["subscription"], sub, "{f}");
        let item = f["params"]["result"].clone();
        let done = item["type"] == "done";
        items.push(item);
        if done {
            return items;
        }
    }
}

/// The proposal a call answered: one `needs_input` data item, then `done`.
fn proposal(items: &[Value]) -> Value {
    assert_eq!(items.len(), 2, "a proposal, then done: {items:?}");
    assert_eq!(items[0]["type"], "data", "{items:?}");
    assert_eq!(items[0]["content_type"], "ledger.bump", "{items:?}");
    assert_eq!(items[0]["content"]["type"], "needs_input", "{items:?}");
    assert_eq!(items[1]["type"], "done", "{items:?}");
    items[0]["content"].clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn over_the_websocket_a_method_proposes_then_the_same_call_with_confirm_carries_it_out() {
    let ledger = LedgerActivation::default();
    let port = serve(ledger.clone()).await;
    let mut ada = signed_in(port, "ada").await;

    let p = proposal(&bump(&mut ada, 1, json!({"n": 2})).await);
    assert_eq!(p["summary"], "add 2 to 0");
    assert_eq!(p["effects"], json!([{"from": 0}]));
    assert_eq!(p["fields"], json!([]), "a plain confirmation has no fields");
    let token = p["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("v1."), "{token}");
    assert!(p["expires"].as_str().unwrap().ends_with('Z'), "RFC 3339: {p}");
    assert_eq!(*ledger.value.lock().unwrap(), 0, "a proposal changes nothing");

    // The answer: the same method, the same params, and `confirm`.
    let got = bump(&mut ada, 2, json!({"n": 2, "confirm": token})).await;
    assert_eq!(got[0]["content"], json!({"type": "bumped", "from": 0, "to": 2}), "{got:?}");
    assert_eq!(got[1]["type"], "done");

    // The same token again meets a moved plan: a fresh proposal, nothing done.
    let again = proposal(&bump(&mut ada, 3, json!({"n": 2, "confirm": token})).await);
    assert_eq!(again["summary"], "add 2 to 2");
    assert_ne!(again["token"], json!(token));
    assert_eq!(*ledger.value.lock().unwrap(), 2);

    // A field: the value under its own parameter's name on the answer.
    let p = proposal(&bump(&mut ada, 4, json!({})).await);
    assert_eq!(p["fields"], json!([{"name": "n", "default": 1, "help": "how much to add"}]));
    // Another caller cannot spend ADA's token.
    let mut bob = signed_in(port, "bob").await;
    proposal(&bump(&mut bob, 1, json!({"n": 5, "confirm": p["token"]})).await);
    let got = bump(&mut ada, 5, json!({"n": 5, "confirm": p["token"]})).await;
    assert_eq!(got[0]["content"], json!({"type": "bumped", "from": 2, "to": 7}), "{got:?}");

    // `{ns}.respond` is gone from the wire.
    let frame = json!({"jsonrpc": "2.0", "id": 9, "method": "svc.respond", "params": {"request_id": "x", "response": {}}});
    ada.send(Message::text(frame.to_string())).await.unwrap();
    let answer = next(&mut ada).await;
    assert_eq!(answer["error"]["code"], -32601, "method not found: {answer}");
}

/// The tool result of a proposal, read as an MCP client reads it.
#[cfg(feature = "__mcp-test-client")]
fn says_the_proposal(r: &rmcp::model::CallToolResult, tool: &str) -> Value {
    assert_ne!(r.is_error, Some(true), "a proposal is a finished call, not an error: {r:?}");
    let text: String = r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect();
    let s = r.structured_content.clone().expect("structured content");
    let token = s["needs_input"]["token"].as_str().expect("the proposal's token").to_string();
    for line in [
        format!("Nothing was done yet: {tool} proposes this and waits for a confirmation."),
        "Would: add 2 to 0".to_string(),
        "- {\"from\":0}".to_string(),
        format!("call {tool} again with the same arguments plus \"confirm\":"),
        token,
    ] {
        assert!(text.contains(&line), "{line:?} in:\n{text}");
    }
    s
}

#[cfg(feature = "__mcp-test-client")]
fn items(r: &rmcp::model::CallToolResult) -> Value {
    serde_json::to_value(r).unwrap()
}

#[cfg(feature = "__mcp-test-client")]
#[tokio::test(flavor = "multi_thread")]
async fn over_mcp_mounted_a_proposal_is_a_tool_result_and_the_same_tool_with_confirm_carries_it_out() {
    use plexus_transport::ServiceUpstream;
    use plexus_transport_mcp::{router, McpBridge, McpConfig};
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::ServiceExt;

    let ledger = LedgerActivation::default();
    let upstream = ServiceUpstream::new(gated(ledger.clone()), Some(Arc::new(Users)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bridge = McpBridge::new(upstream, McpConfig::default());
    tokio::spawn(async move { axum::serve(listener, router(bridge)).await.unwrap() });

    let config = StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")).auth_header("ada");
    let client = ().serve(StreamableHttpClientTransport::with_client(reqwest::Client::default(), config)).await.expect("initialize");
    let tools = client.peer().list_all_tools().await.unwrap();
    let tool = tools.iter().find(|t| t.name == "ledger.bump").expect("ledger.bump is a tool");
    assert!(tool.input_schema["properties"]["confirm"].is_object(), "confirm is a parameter: {:?}", tool.input_schema);

    let call = |args: Value| CallToolRequestParams::new("ledger.bump").with_arguments(args.as_object().cloned().unwrap());
    let r = client.peer().call_tool(call(json!({"n": 2}))).await.unwrap();
    let s = says_the_proposal(&r, "ledger.bump");
    assert_eq!(*ledger.value.lock().unwrap(), 0, "a proposal changes nothing");
    assert_eq!(s["confirm_with"]["n"], 2);

    let r = client.peer().call_tool(call(s["confirm_with"].clone())).await.unwrap();
    assert_ne!(r.is_error, Some(true), "{r:?}");
    assert_eq!(r.structured_content.as_ref().unwrap()["items"], json!([{"type": "bumped", "from": 0, "to": 2}]), "{}", items(&r));
    assert_eq!(*ledger.value.lock().unwrap(), 2);
}

#[cfg(feature = "__mcp-test-client")]
#[tokio::test(flavor = "multi_thread")]
async fn over_the_legacy_activation_bridge_the_same_tool_result_and_the_same_answer() {
    #![allow(deprecated)]
    use plexus_transport::ActivationMcpBridge;
    use rmcp::model::CallToolRequestParams;
    use rmcp::ServiceExt;

    let ledger = LedgerActivation::default();
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let activation = ledger.clone();
    tokio::spawn(async move {
        let server = ActivationMcpBridge::new(Arc::new(activation)).serve(server_io).await.expect("server initializes");
        let _ = server.waiting().await;
    });
    let client = ().serve(client_io).await.expect("client initializes");
    let call = |args: Value| CallToolRequestParams::new("ledger.bump").with_arguments(args.as_object().cloned().unwrap());

    let r = client.peer().call_tool(call(json!({"n": 2}))).await.unwrap();
    let s = says_the_proposal(&r, "ledger.bump");
    assert_eq!(*ledger.value.lock().unwrap(), 0, "a proposal changes nothing");

    let r = client.peer().call_tool(call(s["confirm_with"].clone())).await.unwrap();
    assert_ne!(r.is_error, Some(true), "{r:?}");
    let text: String = r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect();
    assert!(text.contains("bumped") && !text.contains("Nothing was done"), "{}", items(&r));
    assert_eq!(*ledger.value.lock().unwrap(), 2);
    client.cancel().await.unwrap();
}
