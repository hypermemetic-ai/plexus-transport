//! README.md's example is `examples/serve.rs`, and it serves.
//!
//! Every fenced block in the README is named by the comment above it,
//! `<!-- examples/serve.rs -->`, and is lines that stand together in that
//! file, at any indentation. A block that names no file, or differs from
//! its file, fails; so does a file that is gone. The `Cargo.toml` block is
//! held to this crate's version and to the plexus-core it requires.

use std::fs;
use std::path::Path;
use std::time::Duration;

use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::WsClientBuilder;

#[allow(dead_code)]
#[path = "../examples/serve.rs"]
mod example;

/// The README's example, on a free port, answers a call over its WebSocket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_example_serves() {
    // The test announces itself to no registry.
    std::env::set_var(plexus_transport::NO_REGISTRY_ENV, "1");
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let serving = tokio::spawn(example::serve(port));
    for _ in 0..250 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        assert!(!serving.is_finished(), "the example stopped: {:?}", serving.await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let client = WsClientBuilder::default().build(format!("ws://127.0.0.1:{port}")).await.unwrap();
    let mut call = ObjectParams::new();
    call.insert("method", "health.check").unwrap();
    call.insert("params", serde_json::json!({})).unwrap();
    let mut items: Subscription<serde_json::Value> =
        client.subscribe("myservice.call", call, "myservice.call_unsub").await.unwrap();
    let mut seen = Vec::new();
    let healthy = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(item) = items.next().await {
            let item = item.unwrap();
            if item["content"]["status"] == "healthy" {
                return true;
            }
            seen.push(item);
        }
        false
    })
    .await
    .expect("health.check answers within ten seconds");
    assert!(healthy, "health.check answered {seen:?}");
    serving.abort();
}

struct Block {
    /// The file the comment above the block names.
    source: String,
    /// The index of the block's first line, and of its closing fence.
    lines: std::ops::Range<usize>,
}

fn blocks(readme: &[&str]) -> Vec<Block> {
    let mut found = Vec::new();
    let mut above = "";
    let mut index = 0;
    while index < readme.len() {
        let line = readme[index];
        if line.starts_with("```") {
            let source = above
                .strip_prefix("<!-- ")
                .and_then(|rest| rest.strip_suffix(" -->"))
                .unwrap_or_else(|| panic!("README.md:{}: the block names no file in a comment above it", index + 1));
            let close = (index + 1..readme.len())
                .find(|&i| readme[i].starts_with("```"))
                .unwrap_or_else(|| panic!("README.md:{}: the block is not closed", index + 1));
            found.push(Block { source: source.to_string(), lines: index + 1..close });
            index = close;
        } else if !line.trim().is_empty() {
            above = line;
        }
        index += 1;
    }
    found
}

/// The lines without their common indentation and trailing spaces.
fn dedented(lines: &[&str]) -> Vec<String> {
    let indent = lines.iter().filter(|l| !l.trim().is_empty()).map(|l| l.len() - l.trim_start().len()).min().unwrap_or(0);
    lines.iter().map(|l| l.get(indent..).unwrap_or("").trim_end().to_string()).collect()
}

/// `0.10` of `0.10.1`.
fn minor(version: &str) -> String {
    version.split('.').take(2).collect::<Vec<_>>().join(".")
}

/// What a `Cargo.toml` block's line must say of `name`, when it is one of
/// the two crates whose version this repository knows.
fn requirement(name: &str, manifest: &str) -> Option<String> {
    match name {
        "plexus-transport" => Some(minor(env!("CARGO_PKG_VERSION"))),
        "plexus-core" => {
            let line = manifest.lines().find(|l| l.starts_with("plexus-core = ")).expect("Cargo.toml requires plexus-core");
            let version = line.split("version = \"").nth(1).and_then(|rest| rest.split('"').next()).expect("its version");
            Some(minor(version))
        }
        _ => None,
    }
}

#[test]
fn every_block_is_its_file() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = fs::read_to_string(root.join("README.md")).unwrap();
    let readme: Vec<&str> = text.lines().collect();

    let found = blocks(&readme);
    assert!(!found.is_empty(), "README.md has no code block");

    let mut wrong = Vec::new();
    let mut installs = false;
    for block in &found {
        let file = fs::read_to_string(root.join(&block.source))
            .unwrap_or_else(|e| panic!("README.md:{}: {}: {e}", block.lines.start, block.source));
        let quoted = &readme[block.lines.clone()];

        if block.source == "Cargo.toml" {
            for line in quoted {
                let Some((name, said)) = line.split_once(" = ") else { continue };
                let Some(want) = requirement(name, &file) else { continue };
                installs |= name == "plexus-transport";
                if !said.contains(&format!("\"{want}\"")) {
                    wrong.push(format!("README.md: {name} is {want}, not {said}"));
                }
            }
            continue;
        }

        let file: Vec<&str> = file.trim_end().lines().collect();
        let quoted = dedented(quoted);
        if quoted.is_empty() || !file.windows(quoted.len()).any(|window| dedented(window) == quoted) {
            wrong.push(format!("README.md:{}: not what {} says", block.lines.start, block.source));
        }
    }
    assert!(installs, "README.md has no install line for plexus-transport");
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
