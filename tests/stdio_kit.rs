//! TRANSPORT-17: the conformance kit against plexus-transport's own stdio.
//!
//! The kit's child role has run `echo-rs --stdio`, which is a loop of its own
//! with a writer of its own, so nothing has run the kit against `serve_stdio`.
//! This file is that server, a real service through the pipe and jsonrpsee's
//! sink: run with `--serve` it serves the echo's `echo`, `count` and `whoami` on
//! its own stdin and stdout (`harness = false` keeps libtest's lines off them).
//! Run without it, it runs the kit's stdio tests against itself. A child's call
//! is `echo.call`, which a service's root would shadow, so `module` adds it by
//! hand over the same route and the same pipe. The kit's other child tests are
//! about the hop and the child's schema, which are a child's and not this
//! transport's. A missing kit fails, naming it, as in plexus-core's
//! tests/conformance.rs.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use jsonrpsee::core::server::Methods;
use jsonrpsee::RpcModule;
use plexus_core::plexus::{
    auth::AuthContext, error_stream, schema::MethodSchema, wire, wrap_stream, Activation, ChildRouter, DynamicHub,
    MethodEnumSchema, PlexusError, PlexusStream, PluginSchema, PLEXUS_NOTIF_METHOD,
};
use plexus_core::request::RawRequestContext;
use plexus_transport::{stdio::serve_stdio, StdioConfig};
use serde_json::{json, Value};

const TESTS: &str = "stdio.framing,stdio.one_write,child.stdout_only_frames,child.stdin_eof_ends_calls,child.crash_is_eof";

#[derive(Clone)]
struct Echo;

struct EchoMethods;

impl plexus_core::__schemars::JsonSchema for EchoMethods {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "EchoMethods".into()
    }

    fn json_schema(_: &mut plexus_core::__schemars::SchemaGenerator) -> plexus_core::__schemars::Schema {
        plexus_core::__schemars::Schema::try_from(json!({"type": "object"})).unwrap()
    }
}

impl MethodEnumSchema for EchoMethods {
    fn method_names() -> &'static [&'static str] {
        &["echo", "count", "whoami"]
    }

    fn schema_with_consts() -> Value {
        json!({})
    }
}

#[async_trait]
impl ChildRouter for Echo {
    fn router_namespace(&self) -> &str {
        "echo"
    }

    async fn router_call(
        &self,
        method: &str,
        params: Value,
        auth: Option<&AuthContext>,
        raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        Activation::call(self, method, params, auth, raw_ctx).await
    }

    async fn get_child(&self, _: &str) -> Option<Box<dyn ChildRouter>> {
        None
    }
}

#[async_trait]
impl Activation for Echo {
    type Methods = EchoMethods;

    fn namespace(&self) -> &str {
        "echo"
    }

    fn version(&self) -> &str {
        "0.0.1"
    }

    fn methods(&self) -> Vec<&str> {
        vec!["echo", "count", "whoami"]
    }

    async fn call(
        &self,
        method: &str,
        params: Value,
        _auth: Option<&AuthContext>,
        _raw_ctx: Option<&RawRequestContext>,
    ) -> Result<PlexusStream, PlexusError> {
        let provenance = vec!["echo".to_string()];
        Ok(match method {
            "echo" => {
                let text = params["text"].as_str().unwrap_or_default().to_string();
                let times = params["times"].as_u64().unwrap_or(1);
                wrap_stream(
                    stream! {
                        for _ in 0..times {
                            yield json!({"type": "said", "text": text});
                        }
                    },
                    "echo.echo",
                    provenance,
                )
            }
            "count" => {
                let to = params["to"].as_u64().unwrap_or(0);
                let delay = params["delay_ms"].as_u64().unwrap_or(0);
                wrap_stream(
                    stream! {
                        for n in 1..=to {
                            if delay > 0 {
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                            yield json!({"type": "counted", "n": n});
                        }
                    },
                    "echo.count",
                    provenance,
                )
            }
            "whoami" => wrap_stream(futures::stream::iter([json!({"type": "seen", "principal": null})]), "echo.whoami", provenance),
            other => return Err(PlexusError::MethodNotFound { activation: "echo".into(), method: other.into() }),
        })
    }

    fn into_rpc_methods(self) -> Methods {
        Methods::new()
    }

    fn plugin_schema(&self) -> PluginSchema {
        let streaming = |name: &str| MethodSchema::new(name, name, name).with_streaming(true);
        PluginSchema::leaf("echo", "0.0.1", "The kit's echo", vec![streaming("echo"), streaming("count"), streaming("whoami")])
    }
}

/// The service's module, with a child's `echo.call`.
fn module() -> RpcModule<()> {
    let hub = Arc::new(DynamicHub::new("kit").register(Echo));
    let mut module = DynamicHub::arc_into_rpc_module(hub.clone()).unwrap();
    module
        .register_subscription("echo.call", PLEXUS_NOTIF_METHOD, "echo.call_unsub", move |params, pending, _, ext| {
            let hub = hub.clone();
            async move {
                let p: Value = params.parse()?;
                let method = p["method"].as_str().unwrap_or_default();
                let stream = match hub.route(method, p["params"].clone(), None).await {
                    Ok(stream) => stream,
                    Err(e) => error_stream(e.to_string(), vec!["echo".to_string()], false),
                };
                let sink = pending.accept().await?;
                wire::pipe(sink, stream, &ext).await
            }
        })
        .unwrap();
    module
}

fn serve() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    runtime.block_on(serve_stdio(module(), StdioConfig::default())).unwrap();
}

fn main() {
    if std::env::args().any(|a| a == "--serve") {
        return serve();
    }
    let wire = std::env::var_os("PLEXUS_WIRE").map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../plexus-wire"), PathBuf::from);
    let kit = wire.join("conformance/run.py");
    assert!(kit.exists(), "the conformance kit is not at {} (a plexus-wire checkout beside plexus-transport, or PLEXUS_WIRE)", kit.display());
    let me = std::env::current_exe().unwrap();
    let status = Command::new("python3")
        .arg("-I")
        .arg(&kit)
        .args(["run", "--role", "child", "--stdio", &format!("{} --serve", me.display()), "--budget-scale", "4", "--only", TESTS])
        .current_dir(&wire)
        .status()
        .unwrap_or_else(|e| panic!("python3 (to run the conformance kit) did not start: {e}"));
    std::process::exit(status.code().unwrap_or(1));
}
