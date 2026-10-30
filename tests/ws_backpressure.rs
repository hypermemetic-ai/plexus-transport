//! PERF-20 / RPC-6: backpressure. A peer that writes and never reads holds no
//! memory on the server: its answers queue only up to the socket's bound
//! (1,024 frames), then its calls wait — the reader stops reading, TCP holds
//! the rest — and the server's resident set stays flat, back to where it was
//! once the socket closes. (RPC-6's probe: 1,500 × 64 KiB to a socket that
//! never read left RSS 282 → 283 MiB.) A test binary of its own: the RSS is
//! the whole process's.

mod ws_support;

use std::time::Duration;

use jsonrpsee::RpcModule;
use plexus_transport::websocket::UpgradeOptions;
use tokio::io::AsyncWriteExt;
use ws_support::*;

const MIB: u64 = 1024 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_never_reads_holds_no_memory() {
    let mut module = RpcModule::new(());
    module.register_method("echo", |params, _, _| params.as_str().map(|s| s.len()).unwrap_or(0)).unwrap();
    let port = serve(module, UpgradeOptions::default()).await;
    // Warm: one socket's worth of buffers made and freed before the baseline.
    {
        let mut ws = connect(port).await;
        request(&mut ws, 1, "echo", serde_json::json!([])).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let base = rss();

    let mut s = raw_upgrade(port).await;
    let mut call = br#"{"jsonrpc":"2.0","id":7,"method":"nope","params":{"pad":""#.to_vec();
    call.resize(64 * 1024 - 3, b'x');
    call.extend_from_slice(br#""}}"#);
    let frame = client_frame(0x1, true, &call);
    // 1,500 × 64 KiB, never reading an answer; a write that stays blocked is
    // the backpressure reaching the peer.
    let mut written = 0;
    for _ in 0..1500 {
        match tokio::time::timeout(Duration::from_secs(2), s.write_all(&frame)).await {
            Ok(Ok(())) => written += 1,
            Ok(Err(e)) => panic!("the server dropped the socket: {e}"),
            Err(_) => break,
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let during = rss();
    println!("{written} × 64 KiB written; RSS {} → {} MiB", base / MIB, during / MIB);
    assert!(during < base + 32 * MIB, "RSS grew {} MiB under a peer that never reads", (during - base) / MIB);

    // Another socket is served meanwhile.
    let mut other = connect(port).await;
    assert!(request(&mut other, 1, "echo", serde_json::json!([])).await.get("result").is_some());

    drop(s);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = rss();
    println!("after close: {} MiB", after / MIB);
    assert!(after < base + 16 * MIB, "RSS {} MiB above the baseline after the socket closed", (after.saturating_sub(base)) / MIB);
}
