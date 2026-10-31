//! Stdio transport - Line-delimited JSON-RPC over stdin/stdout
//!
//! This transport is MCP-compatible and is the standard way to integrate
//! with Claude Desktop and other MCP clients.
//!
//! PERF-9 (spec/13): a client's first line may be the request
//! `plexus.encoding {encodings: ["msgpack","json"]}`, naming the encodings it
//! speaks; the service answers its pick as its own first line (from
//! [`StdioConfig::encodings`], both by default) and,
//! under msgpack, every frame after it both ways is a 4-byte big-endian length
//! and one msgpack map, written whole by one writer (TRANSPORT-17's rule).
//! Without that line the framing is one JSON line per frame, as before —
//! including its two writes a notification from a task per stream, which is
//! TRANSPORT-17's to change.

use std::sync::Arc;

use anyhow::Result;
use jsonrpsee::RpcModule;
use plexus_core::plexus::encoding::{stdio_answer_line, stdio_frame, stdio_read_ask, stdio_read_frame, WireEncoding};
use plexus_core::plexus::msgpack;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};

use crate::config::StdioConfig;

/// Serve RPC module over stdio (MCP-compatible transport)
///
/// Reads line-delimited JSON-RPC requests from stdin and writes responses to stdout.
/// Subscription notifications are forwarded to stdout as they arrive.
///
/// This function will block until stdin is closed.
pub async fn serve_stdio(module: RpcModule<()>, config: StdioConfig) -> Result<()> {
    serve_stdio_io(module, config, tokio::io::stdin(), tokio::io::stdout()).await
}

/// [`serve_stdio`] over any reader and writer (a test's pipes, a child's).
pub async fn serve_stdio_io<R, W>(module: RpcModule<()>, config: StdioConfig, input: R, mut output: W) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tracing::info!("Starting stdio transport (MCP-compatible)");
    let mut reader = BufReader::new(input);

    // The first line: the client's ask (spec/13), or its first frame.
    let mut first = String::new();
    loop {
        first.clear();
        if reader.read_line(&mut first).await? == 0 {
            return Ok(());
        }
        if !first.trim().is_empty() {
            break;
        }
    }
    let offered = config.encodings.clone();
    let (encoding, first_frame) = match stdio_read_ask(first.trim().as_bytes()) {
        Some((id, names)) => {
            let pick = offered.pick_names(names.iter().map(String::as_str));
            let answer = stdio_answer_line(&id, pick, &offered);
            output.write_all(format!("{answer}\n").as_bytes()).await?;
            output.flush().await?;
            match pick {
                Some(e) => (e, None),
                None => {
                    tracing::debug!(asked = ?names, offered = %offered, "stdio: no encoding the client named is offered");
                    return Ok(());
                }
            }
        }
        // A client that names nothing gets JSON lines, while JSON is offered.
        None if offered.offers(WireEncoding::Json) => (WireEncoding::Json, Some(first)),
        None => {
            let answer = stdio_answer_line(&serde_json::Value::Null, None, &offered);
            output.write_all(format!("{answer}\n").as_bytes()).await?;
            output.flush().await?;
            return Ok(());
        }
    };
    match encoding {
        WireEncoding::Msgpack => serve_frames(module, config, reader, output).await,
        _ => serve_lines(module, config, reader, output, first_frame).await,
    }
}

/// One JSON line per frame: today's framing, unchanged.
async fn serve_lines<R, W>(module: RpcModule<()>, config: StdioConfig, reader: BufReader<R>, output: W, first: Option<String>) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let stdout = Arc::new(Mutex::new(output));
    let mut lines = reader.lines();
    let mut pending = first;
    loop {
        let line = match pending.take() {
            Some(l) => l,
            None => match lines.next_line().await? {
                Some(l) => l,
                None => break,
            },
        };
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
        {
            let mut out = stdout.lock().await;
            out.write_all(response_str.as_bytes()).await?;
            out.write_all(b"\n").await?;
            out.flush().await?;
        }

        tracing::debug!("Sent response: {}", response_str);

        // Spawn task to forward subscription notifications (if any)
        // The receiver will be empty for non-subscription responses
        let stdout = stdout.clone();
        tokio::spawn(async move {
            while let Some(notification) = sub_receiver.recv().await {
                let notification_str = notification.get();
                tracing::debug!("Forwarding notification: {}", notification_str);

                // The message and its newline, each its own write (TRANSPORT-17).
                if stdout.lock().await.write_all(notification_str.as_bytes()).await.is_err() {
                    break;
                }
                let mut out = stdout.lock().await;
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

/// PERF-9: msgpack frames, each a 4-byte big-endian length and one map, both
/// ways; every frame out written whole by one writer, the ready ones in one
/// write (spec/13). A frame crosses to the method table as JSON text, and
/// what the table answers crosses back.
async fn serve_frames<R, W>(module: RpcModule<()>, config: StdioConfig, mut reader: BufReader<R>, output: W) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(write_frames(output, rx));
    let mut streams = tokio::task::JoinSet::new();
    let error = |code: i64, message: &str, data: Option<&str>| {
        let mut e = serde_json::json!({ "id": null, "error": { "code": code, "message": message } });
        if let Some(d) = data {
            e["error"]["data"] = serde_json::Value::from(d);
        }
        msgpack::to_vec(&e).unwrap_or_default()
    };
    let max = crate::ws::MAX_MESSAGE_SIZE;
    while let Some(frame) = stdio_read_frame(&mut reader, max).await? {
        let frame = match frame {
            Ok(f) => f,
            Err(_) => {
                let _ = tx.send(error(-32007, "Request is too big", Some(&format!("Exceeded max limit of {max}"))));
                continue;
            }
        };
        let request = match msgpack::frame_to_json(&frame).ok().and_then(|j| String::from_utf8(j).ok()) {
            Some(r) => r,
            None => {
                let _ = tx.send(error(-32700, "Parse error", None));
                continue;
            }
        };
        let (response, mut notifications) = match module.raw_json_request(&request, config.subscription_buffer_size).await {
            Ok(r) => r,
            Err(_) => {
                let _ = tx.send(error(-32700, "Parse error", None));
                continue;
            }
        };
        if let Ok(f) = msgpack::frame_from_json(response.get().as_bytes()) {
            let _ = tx.send(f);
        }
        let tx = tx.clone();
        streams.spawn(async move {
            while let Some(n) = notifications.recv().await {
                let Ok(f) = msgpack::frame_from_json(n.get().as_bytes()) else { break };
                if tx.send(f).is_err() {
                    break;
                }
            }
        });
        // Reap the streams that have ended.
        while streams.try_join_next().is_some() {}
    }
    // EOF ends every call (spec/11), then what was written goes out.
    streams.shutdown().await;
    drop(tx);
    let _ = writer.await;
    Ok(())
}

/// The one writer: every frame ready when it wakes, each length-prefixed, in
/// one write and one flush.
async fn write_frames<W: AsyncWrite + Unpin>(mut out: W, mut rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let mut buf = Vec::with_capacity(16 * 1024);
    while let Some(first) = rx.recv().await {
        buf.clear();
        stdio_frame(&first, &mut buf);
        while let Ok(more) = rx.try_recv() {
            stdio_frame(&more, &mut buf);
        }
        if out.write_all(&buf).await.is_err() || out.flush().await.is_err() {
            break;
        }
    }
}
