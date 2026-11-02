//! Stdio transport - Line-delimited JSON-RPC over stdin/stdout
//!
//! This transport is MCP-compatible and is the standard way to integrate
//! with Claude Desktop and other MCP clients.
//!
//! TRANSPORT-17: one writer. Every line out, an answer or a stream's item, goes
//! through one bounded queue to one task, so a message and its newline can never
//! be separated, however many streams are live (spec/11). The queue holds a
//! stream back as the pipe did: a reader that stops stops the stream.

use std::time::Duration;

use anyhow::Result;
use jsonrpsee::RpcModule;
use serde_json::value::RawValue;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::config::StdioConfig;

/// The bytes one write gathers before it goes out. A stream that outruns the
/// writer keeps the queue full, and it must still be written.
const WRITE_BUDGET: usize = 64 * 1024;

/// How long what is queued has to go out once the input has ended, so that a
/// client that closes stdin and stops reading stdout does not keep the process.
const DRAIN: Duration = Duration::from_secs(5);

/// Serve RPC module over stdio (MCP-compatible transport)
///
/// Reads line-delimited JSON-RPC requests from stdin and writes responses to stdout.
/// Subscription notifications are forwarded to stdout as they arrive.
///
/// This function will block until stdin is closed, which ends every call
/// (spec/11); what was already queued goes out first.
pub async fn serve_stdio(module: RpcModule<()>, config: StdioConfig) -> Result<()> {
    tracing::info!("Starting stdio transport (MCP-compatible)");
    serve_lines(module, config, tokio::io::stdin(), tokio::io::stdout(), DRAIN).await
}

/// [`serve_stdio`] on any pair of pipes.
async fn serve_lines<R, W>(module: RpcModule<()>, config: StdioConfig, input: R, output: W, drain: Duration) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (out, queue) = mpsc::channel::<Box<RawValue>>(config.subscription_buffer_size.max(1));
    let mut writer = tokio::spawn(write_lines(output, queue));
    let mut streams = JoinSet::new();
    let read: Result<()> = async {
        let mut lines = BufReader::new(input).lines();
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

            // The answer first, ahead of its stream's items.
            tracing::debug!("Sending response: {}", response.get());
            if out.send(response).await.is_err() {
                break; // the writer has stopped, and says why below
            }

            // Spawn task to forward subscription notifications (if any)
            // The receiver will be empty for non-subscription responses
            let out = out.clone();
            streams.spawn(async move {
                while let Some(notification) = sub_receiver.recv().await {
                    tracing::debug!("Forwarding notification: {}", notification.get());
                    if out.send(notification).await.is_err() {
                        break;
                    }
                }
            });
            // Reap the streams that have ended.
            while streams.try_join_next().is_some() {}
        }
        Ok(())
    }
    .await;

    // The end of the input ends every call (spec/11), then what is queued goes out.
    streams.shutdown().await;
    drop(out);
    match tokio::time::timeout(drain, &mut writer).await {
        Ok(wrote) => read.and(wrote?.map_err(Into::into)),
        Err(_) => {
            tracing::warn!("stdout took no more for {drain:?} after the end of stdin: dropping what is queued");
            writer.abort();
            read
        }
    }
}

/// The one writer: every line ready when it wakes, each with its newline, in one
/// write and one flush, up to [`WRITE_BUDGET`].
async fn write_lines<W: AsyncWrite + Unpin>(mut out: W, mut queue: mpsc::Receiver<Box<RawValue>>) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(WRITE_BUDGET);
    let put = |buf: &mut Vec<u8>, line: &RawValue| {
        buf.extend_from_slice(line.get().as_bytes());
        buf.push(b'\n');
    };
    while let Some(first) = queue.recv().await {
        buf.clear();
        put(&mut buf, &first);
        while buf.len() < WRITE_BUDGET {
            let Ok(more) = queue.try_recv() else { break };
            put(&mut buf, &more);
        }
        out.write_all(&buf).await?;
        out.flush().await?;
        buf.shrink_to(WRITE_BUDGET); // a line past it is not kept
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt};

    fn module() -> RpcModule<()> {
        let mut m = RpcModule::new(());
        m.register_method("ping", |_, _, _| "x".repeat(1024)).unwrap();
        m
    }

    fn config() -> StdioConfig {
        StdioConfig::default()
    }

    /// A client that closes stdin and never reads stdout does not keep the process.
    #[tokio::test]
    async fn a_stdout_nobody_reads_does_not_outlast_the_end_of_input() {
        let (mut client, input) = duplex(1 << 16);
        let (_output, stdout) = duplex(64);
        client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\",\"params\":[]}\n").await.unwrap();
        drop(client);
        let served = tokio::time::timeout(Duration::from_secs(5), serve_lines(module(), config(), input, stdout, Duration::from_millis(50)));
        served.await.expect("it returns").unwrap();
    }

    /// A stdout that has closed is an error, at the next line that needs it.
    #[tokio::test]
    async fn a_stdout_that_closed_is_an_error() {
        let (mut client, input) = duplex(1 << 16);
        let (output, stdout) = duplex(64);
        drop(output);
        client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\",\"params\":[]}\n").await.unwrap();
        drop(client);
        let served = serve_lines(module(), config(), input, stdout, Duration::from_secs(5)).await;
        assert!(served.is_err(), "{served:?}");
    }

    /// A line past the budget goes out whole, and the writer does not keep its size.
    #[tokio::test]
    async fn a_line_past_the_budget_goes_out_whole() {
        let (tx, rx) = mpsc::channel(4);
        let (mut output, stdout) = duplex(1 << 16);
        let writer = tokio::spawn(write_lines(stdout, rx));
        let long = format!("\"{}\"", "x".repeat(WRITE_BUDGET * 3));
        tx.send(RawValue::from_string(long.clone()).unwrap()).await.unwrap();
        tx.send(RawValue::from_string("1".to_string()).unwrap()).await.unwrap();
        drop(tx);
        let mut got = Vec::new();
        output.read_to_end(&mut got).await.unwrap();
        writer.await.unwrap().unwrap();
        assert_eq!(got, format!("{long}\n1\n").into_bytes());
    }

    /// A stream that outruns the writer is still written: the gathering ends at
    /// the budget, with the queue still full.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stream_that_outruns_the_writer_is_still_written() {
        let (tx, rx) = mpsc::channel(1024);
        let (mut output, stdout) = duplex(1 << 16);
        tokio::spawn(write_lines(stdout, rx));
        tokio::spawn(async move {
            let item = RawValue::from_string(format!("\"{}\"", "x".repeat(1024))).unwrap();
            while tx.send(item.clone()).await.is_ok() {}
        });
        let mut first = vec![0; 8 * WRITE_BUDGET];
        tokio::time::timeout(Duration::from_secs(5), output.read_exact(&mut first)).await.expect("written").unwrap();
    }
}
