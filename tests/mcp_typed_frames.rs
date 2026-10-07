//! MCP bridge carries plexus 0.7's typed frames (RETURNS-11).
//!
//! Drives `ActivationMcpBridge` end to end over an in-memory duplex with a
//! real rmcp client:
//!
//! - a failed call (a non-recoverable error frame) yields an error tool
//!   result: text = `RemoteError`'s `Display`, structured content = the
//!   `RemoteError` JSON;
//! - progress frames go out as logging notifications `{"type": "progress",
//!   "content_type", "content"}`; error frames as `{"type": "error", "error":
//!   <RemoteError>, "recoverable"}`;
//! - an `{"err": …}` item is data: the tool result is a success;
//! - a call rejected before its stream starts is a JSON-RPC error whose
//!   `data` is the `RemoteError`.

#![cfg(feature = "mcp")]

use std::borrow::Cow;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use jsonrpsee::core::server::Methods;
use plexus_core::plexus::{
    auth::AuthContext,
    schema::MethodSchema,
    types::{PlexusStreamItem, StreamMetadata},
    Activation, MethodEnumSchema, PlexusError, PlexusStream, PluginSchema, RemoteError,
};
use plexus_core::request::RawRequestContext;
use plexus_transport::ActivationMcpBridge;
use rmcp::model::{CallToolRequestParam, LoggingMessageNotificationParam};
use rmcp::service::{NotificationContext, RoleClient, RunningService};
use rmcp::{ClientHandler, ServiceError, ServiceExt};
use serde_json::{json, Value};

// =============================================================================
// Fixture activation
// =============================================================================

struct FixtureMethods;

impl plexus_core::__schemars::JsonSchema for FixtureMethods {
    fn schema_name() -> Cow<'static, str> {
        "FixtureMethods".into()
    }

    fn json_schema(
        _generator: &mut plexus_core::__schemars::SchemaGenerator,
    ) -> plexus_core::__schemars::Schema {
        plexus_core::__schemars::Schema::try_from(json!({"type": "object"})).unwrap()
    }
}

impl MethodEnumSchema for FixtureMethods {
    fn method_names() -> &'static [&'static str] {
        &["fail", "refuse", "reject"]
    }

    fn schema_with_consts() -> Value {
        json!({})
    }
}

fn metadata() -> StreamMetadata {
    StreamMetadata::new(vec!["test".into()], "hash".into())
}

fn disk_full() -> RemoteError {
    RemoteError::Execution {
        message: "disk full".into(),
    }
}

/// - `fail`: a progress frame, a data item, a recoverable error frame, then a
///   non-recoverable one (`execution: disk full`)
/// - `refuse`: an `{"err": …}` item
/// - `reject`: refused before its stream starts (`method_not_found`)
struct Fixture;

#[async_trait]
impl Activation for Fixture {
    type Methods = FixtureMethods;

    fn namespace(&self) -> &str {
        "test"
    }

    fn version(&self) -> &str {
        "0.0.1"
    }

    fn methods(&self) -> Vec<&str> {
        vec!["fail", "refuse", "reject"]
    }

    async fn call(
        &self,
        method: &str,
        _params: Value,
        _auth: Option<&AuthContext>,
        _raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        match method {
            "fail" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::progress(
                    metadata(),
                    "test.fail".into(),
                    json!({"type": "chunk", "done": 1, "of": 2}),
                );
                yield PlexusStreamItem::data(metadata(), "test.fail".into(), json!({"n": 1}));
                yield PlexusStreamItem::error(
                    metadata(),
                    RemoteError::Execution { message: "retrying".into() },
                    true,
                );
                yield PlexusStreamItem::error(metadata(), disk_full(), false);
            })),
            "refuse" => Ok(Box::pin(stream! {
                yield PlexusStreamItem::data(
                    metadata(),
                    "test.refuse".into(),
                    json!({"err": {"type": "not_found", "id": "MNG-999"}}),
                );
                yield PlexusStreamItem::done(metadata());
            })),
            other => Err(PlexusError::MethodNotFound {
                activation: "test".into(),
                method: other.into(),
            }),
        }
    }

    fn into_rpc_methods(self) -> Methods {
        Methods::new()
    }

    fn plugin_schema(&self) -> PluginSchema {
        PluginSchema::leaf(
            "test",
            "0.0.1",
            "MCP typed-frame fixture",
            vec![
                MethodSchema::new("fail", "Fails on an error frame", "h-fail"),
                MethodSchema::new("refuse", "Says no", "h-refuse"),
            ],
        )
    }
}

// =============================================================================
// Client harness
// =============================================================================

/// An rmcp client that records every logging notification it receives.
#[derive(Clone, Default)]
struct Recorder {
    logs: Arc<Mutex<Vec<LoggingMessageNotificationParam>>>,
}

impl ClientHandler for Recorder {
    fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        let logs = self.logs.clone();
        async move { logs.lock().unwrap().push(params) }
    }
}

impl Recorder {
    /// The logging notifications' `data`, once `n` have arrived.
    async fn wait_for(&self, n: usize) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let logs = self.logs.lock().unwrap();
                    if logs.len() >= n {
                        return logs.iter().map(|l| l.data.clone()).collect::<Vec<_>>();
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("logging notifications arrive")
    }
}

/// Serve the bridge on one end of a duplex and connect a client to the other.
async fn connect() -> (RunningService<RoleClient, Recorder>, Recorder) {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let server = ActivationMcpBridge::new(Arc::new(Fixture))
            .serve(server_io)
            .await
            .expect("server initializes");
        let _ = server.waiting().await;
    });
    let recorder = Recorder::default();
    let client = recorder
        .clone()
        .serve(client_io)
        .await
        .expect("client initializes");
    (client, recorder)
}

fn call(name: &'static str) -> CallToolRequestParam {
    CallToolRequestParam {
        name: name.into(),
        arguments: Some(serde_json::Map::new()),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn failed_call_yields_error_result_with_remote_error_as_structured_content() {
    let (client, _recorder) = connect().await;

    let result = client.peer().call_tool(call("test.fail")).await.unwrap();

    assert_eq!(result.is_error, Some(true), "a failed call is an error result");
    let structured = result
        .structured_content
        .clone()
        .expect("error result carries structured content");
    let error: RemoteError =
        serde_json::from_value(structured).expect("structured content deserializes as RemoteError");
    assert_eq!(error, disk_full(), "the non-recoverable error is why the call failed");

    let text = &result.content[0].as_text().expect("text content").text;
    assert_eq!(text, &disk_full().to_string(), "text is RemoteError's Display");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn progress_and_error_frames_go_out_typed_as_logging_notifications() {
    let (client, recorder) = connect().await;

    client.peer().call_tool(call("test.fail")).await.unwrap();

    let logs = recorder.wait_for(4).await;
    assert_eq!(
        logs,
        vec![
            json!({
                "type": "progress",
                "content_type": "test.fail",
                "content": {"type": "chunk", "done": 1, "of": 2},
            }),
            json!({"type": "data", "content_type": "test.fail", "data": {"n": 1}}),
            json!({
                "type": "error",
                "error": {"kind": "execution", "message": "retrying"},
                "recoverable": true,
            }),
            json!({
                "type": "error",
                "error": {"kind": "execution", "message": "disk full"},
                "recoverable": false,
            }),
        ]
    );
    let error: RemoteError = serde_json::from_value(logs[3]["error"].clone()).unwrap();
    assert_eq!(error, disk_full());

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn err_item_is_data_and_the_result_is_a_success() {
    let (client, _recorder) = connect().await;

    let result = client.peer().call_tool(call("test.refuse")).await.unwrap();

    assert_eq!(result.is_error, Some(false), "an err item is data, not a failed call");
    let text = &result.content[0].as_text().expect("text content").text;
    let item: Value = serde_json::from_str(text).unwrap();
    assert_eq!(item, json!({"err": {"type": "not_found", "id": "MNG-999"}}));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn call_rejected_up_front_is_a_jsonrpc_error_carrying_remote_error() {
    let (client, _recorder) = connect().await;

    let err = client
        .peer()
        .call_tool(call("test.nope"))
        .await
        .expect_err("a rejected call is a JSON-RPC error");

    let ServiceError::McpError(data) = err else {
        panic!("expected an MCP error, got {err:?}");
    };
    let expected = RemoteError::MethodNotFound {
        activation: "test".into(),
        method: "nope".into(),
    };
    assert_eq!(data.code.0, -32601);
    assert_eq!(data.message, expected.to_string());
    let error: RemoteError =
        serde_json::from_value(data.data.expect("rejection carries data")).unwrap();
    assert_eq!(error, expected);

    client.cancel().await.unwrap();
}
