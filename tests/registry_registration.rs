//! Z2H-6 integration tests: boot-time registry self-registration.
//!
//! Drives `TransportServer::serve()` against an in-process FAKE registry
//! that speaks the same wire surface as plexus-registry's hub
//! (`_info` + `<hub>.call` → `registry.register` / `registry.deregister`)
//! and records what it receives.
//!
//! Stop-invalidation via SIGTERM is exercised by the Z2H-6 end-to-end
//! walkthrough (signals are process-global — not unit-testable in-process);
//! the deregister wire call itself is covered here at function level.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jsonrpsee::server::Server;
use jsonrpsee::RpcModule;
use plexus_core::plexus::DynamicHub;
use plexus_transport::registry::{self, RegistryRegistration};
use plexus_transport::TransportServer;
use serde_json::{json, Value};

// ===========================================================================
// Fake registry fixture
// ===========================================================================

#[derive(Clone, Default)]
struct Recorded {
    registers: Arc<Mutex<Vec<Value>>>,
    deregisters: Arc<Mutex<Vec<Value>>>,
}

fn meta() -> Value {
    json!({"provenance": ["registry-hub"], "plexus_hash": "fake", "timestamp": 0})
}

fn data_item(content: Value) -> Value {
    json!({"type": "data", "metadata": meta(), "content_type": "registry", "content": content})
}

fn done_item() -> Value {
    json!({"type": "done", "metadata": meta()})
}

/// Start a fake registry hub on an OS-assigned port. Mirrors the wire
/// surface synapse + plexus-transport actually speak: a namespace-free
/// `_info` subscription and a `registry-hub.call` subscription routing
/// `registry.*` methods.
async fn start_fake_registry(recorded: Recorded) -> (SocketAddr, jsonrpsee::server::ServerHandle) {
    let mut module = RpcModule::new(recorded);

    module
        .register_subscription("_info", "result", "_info_unsub", |_p, pending, _ctx, _ext| {
            Box::pin(async move {
                let sink = pending.accept().await?;
                let info = json!({
                    "type": "data", "metadata": meta(), "content_type": "_info",
                    "content": {"backend": "registry-hub"}
                });
                sink.send(serde_json::value::to_raw_value(&info)?).await?;
                sink.send(serde_json::value::to_raw_value(&done_item())?).await?;
                Ok(())
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = jsonrpsee::core::SubscriptionResult> + Send>>
        })
        .unwrap();

    module
        .register_subscription(
            "registry-hub.call",
            "result",
            "registry-hub.call_unsub",
            |params, pending, ctx, _ext| {
                let recorded: Recorded = (*ctx).clone();
                Box::pin(async move {
                    let call: Value = params.parse()?;
                    let sink = pending.accept().await?;
                    let method = call.get("method").and_then(Value::as_str).unwrap_or("");
                    let p = call.get("params").cloned().unwrap_or(Value::Null);
                    let reply = match method {
                        "registry.register" => {
                            recorded.registers.lock().unwrap().push(p.clone());
                            data_item(json!({"type": "backend_registered", "backend": p}))
                        }
                        "registry.deregister" => {
                            recorded.deregisters.lock().unwrap().push(p.clone());
                            data_item(json!({
                                "type": "backend_deregistered",
                                "name": p.get("name").cloned().unwrap_or(Value::Null)
                            }))
                        }
                        other => data_item(json!({
                            "type": "error",
                            "message": format!("unknown method {other}")
                        })),
                    };
                    sink.send(serde_json::value::to_raw_value(&reply)?).await?;
                    sink.send(serde_json::value::to_raw_value(&done_item())?).await?;
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = jsonrpsee::core::SubscriptionResult> + Send>>
            },
        )
        .unwrap();

    let server = Server::builder()
        .build("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let handle = server.start(module);
    (addr, handle)
}

async fn wait_for<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

fn hub(name: &str) -> Arc<DynamicHub> {
    Arc::new(DynamicHub::new(name))
}

fn converter(arc: Arc<DynamicHub>) -> anyhow::Result<RpcModule<()>> {
    DynamicHub::arc_into_rpc_module(arc).map_err(|e| anyhow::anyhow!("rpc module: {e}"))
}

// ===========================================================================
// Tests
// ===========================================================================

/// AC1: a backend started with `--port 0` registers its name, the ACTUAL
/// OS-assigned port, schema identity, pid, and source=auto.
#[tokio::test]
async fn boot_registers_name_actual_port_and_schema_identity() {
    let recorded = Recorded::default();
    let (reg_addr, _reg_handle) = start_fake_registry(recorded.clone()).await;

    let server = TransportServer::builder(hub("z6test"), converter)
        .with_websocket(0)
        .with_registry_url(format!("ws://{reg_addr}"))
        .build()
        .await
        .unwrap();
    let _serve = tokio::spawn(server.serve());

    let registers = recorded.registers.clone();
    assert!(
        wait_for(|| !registers.lock().unwrap().is_empty(), Duration::from_secs(10)).await,
        "backend never registered with the fake registry"
    );

    let reg = recorded.registers.lock().unwrap()[0].clone();
    assert_eq!(reg.get("name").and_then(Value::as_str), Some("z6test"));
    assert_eq!(reg.get("host").and_then(Value::as_str), Some("127.0.0.1"));
    assert_eq!(reg.get("protocol").and_then(Value::as_str), Some("ws"));
    assert_eq!(reg.get("source").and_then(Value::as_str), Some("auto"));
    let port = reg.get("port").and_then(Value::as_u64).unwrap();
    assert!(port != 0, "must report the ACTUAL bound port, not 0");
    let metadata = reg.get("metadata").unwrap();
    assert_eq!(
        metadata.get("pid").and_then(Value::as_u64),
        Some(std::process::id() as u64)
    );
    let schema_hash = metadata.get("schema_hash").and_then(Value::as_str).unwrap();
    assert!(!schema_hash.is_empty(), "schema identity must be reported");
}

/// The `--no-register` convention: `.without_registry()` suppresses the
/// boot-time announcement entirely.
#[tokio::test]
async fn without_registry_opts_out() {
    let recorded = Recorded::default();
    let (reg_addr, _reg_handle) = start_fake_registry(recorded.clone()).await;

    let server = TransportServer::builder(hub("optout"), converter)
        .with_websocket(0)
        .with_registry_url(format!("ws://{reg_addr}"))
        .without_registry()
        .build()
        .await
        .unwrap();
    let _serve = tokio::spawn(server.serve());

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        recorded.registers.lock().unwrap().is_empty(),
        "without_registry() must suppress registration"
    );
}

/// Fail-open: a dead registry endpoint must not prevent the backend from
/// serving (S3: registration is never a boot dependency).
#[tokio::test]
async fn registry_down_is_fail_open() {
    // Reserve a port that is guaranteed closed once the listener drops.
    let closed_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // And a real port for the backend itself so we can probe it from outside.
    let backend_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let server = TransportServer::builder(hub("lonely"), converter)
        .with_websocket(backend_port)
        .with_registry_url(format!("ws://127.0.0.1:{closed_port}"))
        .build()
        .await
        .unwrap();
    let serve = tokio::spawn(server.serve());

    // Give it time to fail all three registration attempts.
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(!serve.is_finished(), "serve() must keep running with the registry down");
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", backend_port)).await.is_ok(),
        "backend must accept connections even when the registry is down"
    );
}

/// Stop-invalidation wire call: `registry::deregister` deactivates by name.
#[tokio::test]
async fn deregister_sends_the_invalidation() {
    let recorded = Recorded::default();
    let (reg_addr, _reg_handle) = start_fake_registry(recorded.clone()).await;

    let reg = RegistryRegistration {
        url: format!("ws://{reg_addr}"),
        name: "z6test".into(),
        host: "127.0.0.1".into(),
        port: 5151,
        version: "1.0.0".into(),
        schema_hash: "abc".into(),
    };
    registry::register(&reg).await.expect("register");
    registry::deregister(&reg).await.expect("deregister");

    let deregs = recorded.deregisters.lock().unwrap();
    assert_eq!(deregs.len(), 1);
    assert_eq!(deregs[0].get("name").and_then(Value::as_str), Some("z6test"));
}
