//! RETURNS-36 (RETURNS-40's finding): every 0.8 service carries plexus's own
//! types at its root — the typed error frame's `RemoteError` and its open
//! `OtherError` among them — so synapse's IR reader must take them, or
//! `synapse -i` refuses every 0.8 service whatever its own types are.
//!
//! The service here is the RETURNS-7 conformance fixture's activation under a
//! `DynamicHub`, served on a WebSocket with no gate: it has no type of its own
//! the reader refuses, so `synapse -i` must exit 0 and name no refusal.
//!
//! It runs the synapse that `PLEXUS_SYNAPSE` names (one with the IR 3.0
//! reader, synapse ≥ 4.0); with none named it says so and passes.

use std::sync::Arc;
use std::time::Duration;

use conformance::ConformanceActivation;
use plexus_core::plexus::DynamicHub;

fn free() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synapse_reads_a_0_8_service_with_no_refusable_types_of_its_own() {
    let Ok(synapse) = std::env::var("PLEXUS_SYNAPSE") else {
        eprintln!("PLEXUS_SYNAPSE names no synapse: not run");
        return;
    };
    let port = free();
    let service = Arc::new(DynamicHub::new("fixture").register(ConformanceActivation));
    let server = plexus_transport::TransportServer::builder(service, |h| {
        DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!("{e}"))
    })
    .without_registry()
    .with_websocket(port)
    .build()
    .await
    .unwrap();
    let serving = tokio::spawn(server.serve());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // its own cache, no connection holder left behind
    let cache = std::env::temp_dir().join(format!("synapse-reads-{port}"));
    let out = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(&synapse)
            .env("XDG_CACHE_HOME", &cache)
            .args(["--no-hold", "--no-cache", "-H", "127.0.0.1", "-P", &port.to_string(), "-i", "fixture"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("synapse -i answers within a minute")
    .expect("synapse runs");
    serving.abort();
    let _ = std::fs::remove_dir_all(&cache);

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "synapse -i exits 0; it said:\n{stderr}");
    assert!(
        !stderr.contains("refus") && !stderr.contains("no IR form"),
        "synapse -i names no refusal; it said:\n{stderr}"
    );
    let ir: serde_json::Value = serde_json::from_slice(&out.stdout).expect("synapse -i prints the IR");
    assert!(ir.get("irTypes").is_some(), "the IR has its types: {ir}");
}
