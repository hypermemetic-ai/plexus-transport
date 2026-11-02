//! BIDIR-6: the activation `tests/duplex_wire.rs` drives: every way a handler
//! can treat its input.

pub use plexus_client;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::stream::{Stream, StreamExt};
use plexus_core::plexus::Input;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

/// One token of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Token {
    pub t: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IngestEvent {
    Ingested { count: u32 },
    Ok,
    Error { message: String },
}

impl plexus_client::FromClientError for IngestEvent {
    fn from_client_error(error: plexus_client::ClientError) -> Self {
        IngestEvent::Error { message: error.to_string() }
    }
}

/// What a test sees of the handlers and tells them.
#[derive(Clone)]
pub struct Probe {
    /// Every token a handler read, in order.
    pub seen: Arc<Mutex<Vec<String>>>,
    /// Handlers dropped (their stream ended or was dropped).
    pub dropped: Arc<AtomicUsize>,
    /// `gated` takes a permit before it polls for its next item.
    pub permits: Arc<Semaphore>,
}

impl Default for Probe {
    fn default() -> Self {
        Self { seen: Arc::default(), dropped: Arc::default(), permits: Arc::new(Semaphore::new(0)) }
    }
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Probe {
    fn guard(&self) -> Dropped {
        Dropped(self.dropped.clone())
    }

    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }

    pub fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

#[derive(Clone, Default)]
pub struct IngestActivation {
    pub probe: Probe,
}

#[plexus_macros::activation]
impl IngestActivation {
    /// Count a run's tokens.
    #[plexus_macros::method(target = "run")]
    pub async fn tokens(
        &self,
        /// the run they belong to
        run: String,
        /// the tokens, as they arrive
        tokens: Input<Token>,
    ) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let _ = run;
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            let mut tokens = tokens.into_stream();
            let mut count = 0;
            while let Some(token) = tokens.next().await {
                probe.seen.lock().unwrap().push(token.t);
                count += 1;
            }
            yield IngestEvent::Ingested { count };
        }
    }

    /// Forward each token before asking for the next: a permit stands for
    /// what is forwarded.
    #[plexus_macros::method(input_window = 4)]
    pub async fn gated(&self, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            let mut tokens = tokens.into_stream();
            let mut count = 0;
            loop {
                probe.permits.acquire().await.unwrap().forget();
                match tokens.next().await {
                    Some(token) => {
                        probe.seen.lock().unwrap().push(token.t);
                        count += 1;
                    }
                    None => break,
                }
            }
            yield IngestEvent::Ingested { count };
        }
    }

    /// Take `take` tokens and end, the input half still open.
    #[plexus_macros::method]
    pub async fn early(&self, take: u32, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            let mut tokens = tokens.into_stream();
            let mut count = 0;
            while count < take {
                if tokens.next().await.is_none() {
                    break;
                }
                count += 1;
            }
            yield IngestEvent::Ingested { count };
        }
    }

    /// Say so at once, then wait: never polls its input again.
    #[plexus_macros::method(input_window = 4)]
    pub async fn stuck(&self, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            let _held = tokens;
            yield IngestEvent::Ok;
            futures::future::pending::<()>().await;
        }
    }

    /// Let its input go and carry on: the half closes, the call does not.
    #[plexus_macros::method]
    pub async fn letgo(&self, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            drop(tokens);
            probe.permits.acquire().await.unwrap().forget();
            yield IngestEvent::Ok;
        }
    }

    /// Opted into a lifetime limit of 3 items.
    #[plexus_macros::method(input_max_items = 3)]
    pub async fn capped(&self, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        async_stream::stream! {
            let count = tokens.into_stream().count().await as u32;
            yield IngestEvent::Ingested { count };
        }
    }

    /// Holds at most 64 bytes.
    #[plexus_macros::method(input_window = 1000, input_max_buffered = 64)]
    pub async fn small(&self, tokens: Input<Token>) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let probe = self.probe.clone();
        async_stream::stream! {
            let _guard = probe.guard();
            let _held = tokens;
            yield IngestEvent::Ok;
            futures::future::pending::<()>().await;
        }
    }

    /// No input half at all.
    #[plexus_macros::method]
    pub async fn plain(&self, run: String) -> impl Stream<Item = IngestEvent> + Send + 'static {
        let _ = run;
        futures::stream::iter(vec![IngestEvent::Ok])
    }
}
