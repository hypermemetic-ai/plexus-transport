//! Stdio transport - Line-delimited JSON-RPC over stdin/stdout
//!
//! This transport is MCP-compatible and is the standard way to integrate
//! with Claude Desktop and other MCP clients.
//!
//! # This is NOT the ACP transport (PLX-139)
//!
//! ACP is also "line-delimited JSON-RPC over stdin/stdout", so this module is
//! the obvious thing to reach for and it is the wrong one. Three reasons, each
//! of which an editor would find within a minute:
//!
//! 1. **It is strictly sequential.** The loop below reads one line, runs it to
//!    completion, and only then reads the next. ACP's `session/prompt` runs for
//!    as long as a model takes, and `session/cancel` — the only way to stop it
//!    — would queue behind the very turn it is cancelling.
//! 2. **It has no server-to-client request direction.** ACP agents *ask*:
//!    `session/request_permission`, `fs/read_text_file`. `RpcModule` answers
//!    requests and forwards subscription notifications; it does not originate
//!    correlated requests.
//! 3. **stdout is unprotected.** Nothing here stops a `println!` anywhere in
//!    the process from landing between two frames.
//!
//! The ACP transport is `plexus_acp::v1::transport` behind that crate's
//! `stdio` feature: upstream's own NDJSON connection, every handler spawned so
//! both directions multiplex, and fd 1 taken away from the process before the
//! first frame. This module is left exactly as it was, for MCP.

use anyhow::Result;
use jsonrpsee::RpcModule;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config::StdioConfig;

/// Serve RPC module over stdio (MCP-compatible transport)
///
/// Reads line-delimited JSON-RPC requests from stdin and writes responses to stdout.
/// Subscription notifications are forwarded to stdout as they arrive.
///
/// This function will block until stdin is closed.
pub async fn serve_stdio(module: RpcModule<()>, config: StdioConfig) -> Result<()> {
    tracing::info!("Starting stdio transport (MCP-compatible)");

    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut lines = stdin.lines();

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        tracing::debug!("Received request: {}", trimmed);

        // Call the RpcModule with the configured subscription buffer size
        let (response, mut sub_receiver) = module
            .raw_json_request(trimmed, config.subscription_buffer_size)
            .await
            .map_err(|e| anyhow::anyhow!("RPC error: {}", e))?;

        // Write initial response to stdout
        let response_str = response.get();
        stdout.write_all(response_str.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;

        tracing::debug!("Sent response: {}", response_str);

        // Spawn task to forward subscription notifications (if any)
        // The receiver will be empty for non-subscription responses
        tokio::spawn(async move {
            while let Some(notification) = sub_receiver.recv().await {
                let notification_str = notification.get();
                tracing::debug!("Forwarding notification: {}", notification_str);

                // Get a new stdout handle for each notification
                let mut out = tokio::io::stdout();
                if out.write_all(notification_str.as_bytes()).await.is_err() {
                    break;
                }
                if out.write_all(b"\n").await.is_err() {
                    break;
                }
                if out.flush().await.is_err() {
                    break;
                }
            }
        });
    }

    Ok(())
}
