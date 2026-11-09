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
use tracing::instrument::WithSubscriber;

use crate::config::StdioConfig;
// DYNAUTH-8 (TM-2): a frame logged at debug carries no credential.
use plexus_core::plexus::redact::redact_str;

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
    let quiet = tracing::Dispatch::new(NoRequestTraces(tracing::dispatcher::get_default(|d| d.clone())));
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

            tracing::debug!("Received request: {}", redact_str(trimmed));

            // Call the RpcModule with the configured subscription buffer size
            let (response, mut sub_receiver) = module
                .raw_json_request(trimmed, config.subscription_buffer_size)
                .with_subscriber(quiet.clone())
                .await
                .map_err(|e| anyhow::anyhow!("RPC error: {}", e))?;

            // The answer first, ahead of its stream's items.
            tracing::debug!("Sending response: {}", redact_str(response.get()));
            if out.send(response).await.is_err() {
                break; // the writer has stopped, and says why below
            }

            // Spawn task to forward subscription notifications (if any)
            // The receiver will be empty for non-subscription responses
            let out = out.clone();
            streams.spawn(async move {
                while let Some(notification) = sub_receiver.recv().await {
                    tracing::debug!("Forwarding notification: {}", redact_str(notification.get()));
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

/// DYNAUTH-8 (TM-2): jsonrpsee-core traces each request it is handed whole
/// (`[Methods::raw_json_request] Request: …`, a method's params and its
/// answer), credentials and all, at `trace`. Around the call this transport
/// makes, those records are dropped and every other one goes on to the
/// subscriber that was there; this transport logs each frame itself,
/// redacted.
struct NoRequestTraces(tracing::Dispatch);

impl NoRequestTraces {
    fn hidden(m: &tracing::Metadata<'_>) -> bool {
        *m.level() == tracing::Level::TRACE && (m.target().starts_with("jsonrpsee_core::server") || m.target().starts_with("jsonrpsee-"))
    }
}

impl tracing::Subscriber for NoRequestTraces {
    fn register_callsite(&self, m: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
        if Self::hidden(m) {
            tracing::subscriber::Interest::never()
        } else {
            self.0.register_callsite(m)
        }
    }
    fn enabled(&self, m: &tracing::Metadata<'_>) -> bool {
        !Self::hidden(m) && self.0.enabled(m)
    }
    fn new_span(&self, a: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        self.0.new_span(a)
    }
    fn record(&self, s: &tracing::span::Id, v: &tracing::span::Record<'_>) {
        self.0.record(s, v)
    }
    fn record_follows_from(&self, s: &tracing::span::Id, f: &tracing::span::Id) {
        self.0.record_follows_from(s, f)
    }
    fn event(&self, e: &tracing::Event<'_>) {
        if !Self::hidden(e.metadata()) {
            self.0.event(e)
        }
    }
    fn enter(&self, s: &tracing::span::Id) {
        self.0.enter(s)
    }
    fn exit(&self, s: &tracing::span::Id) {
        self.0.exit(s)
    }
    fn clone_span(&self, s: &tracing::span::Id) -> tracing::span::Id {
        self.0.clone_span(s)
    }
    fn try_close(&self, s: tracing::span::Id) -> bool {
        self.0.try_close(s)
    }
    fn current_span(&self) -> tracing_core::span::Current {
        self.0.current_span()
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

    /// DYNAUTH-8 (TM-2): a debug log of a call carrying a credential — in
    /// its params, its answer and its stream — holds none of its bytes.
    #[tokio::test]
    async fn a_debug_log_holds_no_credential() {
        use std::sync::{Arc, Mutex};
        const SECRET: &str = "ghu_dynauth8SecretTokenBytes";
        #[derive(Clone, Default)]
        struct Log(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Log {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let log = Log::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).with_ansi(false).with_writer(move || writer.clone()).finish();
        let _g = tracing::subscriber::set_default(subscriber);

        let mut m = RpcModule::new(());
        m.register_method("echo", |p, _, _| p.parse::<serde_json::Value>().unwrap_or_default()).unwrap();
        m.register_subscription("mint", "minted", "unmint", |_, pending: jsonrpsee::PendingSubscriptionSink, _, _| async move {
            let sink = pending.accept().await?;
            sink.send(serde_json::value::to_raw_value(&serde_json::json!({ "refresh_token": SECRET }))?).await?;
            Ok(())
        })
        .unwrap();
        let (mut client, input) = duplex(1 << 16);
        let (mut output, stdout) = duplex(1 << 16);
        let call = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"echo","params":{{"repo":"a/b","credential":{{"token":"{SECRET}"}},"authorization":"Bearer {SECRET}"}}}}"#
        );
        client.write_all(format!("{call}\n").as_bytes()).await.unwrap();
        client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"mint\",\"params\":[]}\n").await.unwrap();
        drop(client);
        serve_lines(m, config(), input, stdout, Duration::from_secs(1)).await.unwrap();
        let mut wrote = String::new();
        output.read_to_string(&mut wrote).await.unwrap();
        assert!(wrote.contains(SECRET), "the frames themselves are not touched: {wrote}");
        let text = String::from_utf8_lossy(&log.0.lock().unwrap()).to_string();
        assert!(text.contains("Received request") && text.contains("a/b"), "debug is on: {text}");
        assert!(!text.contains(SECRET) && !text.contains("dynauth8"), "a credential in the log: {text}");
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
