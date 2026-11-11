//! KEEL-59: work that ticks a counter, so a test sees when a call's work
//! stops — where it runs, not where its caller reads.

pub use plexus_client;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::Stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkEvent {
    Started,
    Tick { n: u64 },
    Error { message: String },
}

impl plexus_client::FromClientError for WorkEvent {
    fn from_client_error(error: plexus_client::ClientError) -> Self {
        WorkEvent::Error { message: error.to_string() }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Answer {
    pub n: u64,
}

/// Units of work done, and when the last one was.
#[derive(Clone, Default)]
pub struct Counter(Arc<(AtomicU64, std::sync::Mutex<Option<Instant>>)>);

impl Counter {
    fn tick(&self) -> u64 {
        *self.0 .1.lock().unwrap() = Some(Instant::now());
        self.0 .0.fetch_add(1, Ordering::SeqCst)
    }

    /// Units of work done so far.
    pub fn ticks(&self) -> u64 {
        self.0 .0.load(Ordering::SeqCst)
    }

    /// When the last unit of work was done.
    pub fn last(&self) -> Option<Instant> {
        *self.0 .1.lock().unwrap()
    }

    /// Until some work has been done.
    pub async fn going(&self) {
        let t = Instant::now();
        while self.ticks() < 3 {
            assert!(t.elapsed() < Duration::from_secs(5), "no work began");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// The work stopped within `bound` of `cause`, and stays stopped.
    pub async fn stopped(&self, cause: Instant, bound: Duration) -> f64 {
        tokio::time::sleep(bound + Duration::from_millis(200)).await;
        let at = self.last().expect("work was done");
        let n = self.ticks();
        let ms = if at >= cause { at.duration_since(cause).as_secs_f64() * 1e3 } else { -(cause.duration_since(at).as_secs_f64() * 1e3) };
        assert!(at <= cause + bound, "the work ran {ms:.1} ms past its cause (bound {bound:?})");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(self.ticks(), n, "the work went on");
        ms
    }
}

const TICK: Duration = Duration::from_millis(2);

/// The work, its counters by lane: a call names the lane its work ticks.
#[derive(Clone, Default)]
pub struct WorkActivation {
    pub lanes: Arc<[Counter; 4]>,
}

impl WorkActivation {
    fn lane(&self, lane: u8) -> Counter {
        self.lanes[lane as usize % 4].clone()
    }
}

#[plexus_macros::activation(namespace = "work", roles(reader = ["quiet", "chatty", "slow"]))]
impl WorkActivation {
    /// Says it started, then works, saying nothing more.
    #[plexus_macros::method]
    pub async fn quiet(&self, lane: u8) -> impl Stream<Item = WorkEvent> + Send + 'static {
        let c = self.lane(lane);
        async_stream::stream! {
            yield WorkEvent::Started;
            loop {
                tokio::time::sleep(TICK).await;
                c.tick();
            }
        }
    }

    /// Says each unit of work it does.
    #[plexus_macros::method]
    pub async fn chatty(&self, lane: u8) -> impl Stream<Item = WorkEvent> + Send + 'static {
        let c = self.lane(lane);
        async_stream::stream! {
            yield WorkEvent::Started;
            loop {
                tokio::time::sleep(TICK).await;
                let n = c.tick();
                yield WorkEvent::Tick { n };
            }
        }
    }

    /// Open to anyone: works, quietly.
    #[plexus_macros::method(public)]
    pub async fn open(&self, lane: u8) -> impl Stream<Item = WorkEvent> + Send + 'static {
        let c = self.lane(lane);
        async_stream::stream! {
            yield WorkEvent::Started;
            loop {
                tokio::time::sleep(TICK).await;
                c.tick();
            }
        }
    }

    /// One answer, after 10 s of work.
    #[plexus_macros::method]
    pub async fn slow(&self, lane: u8) -> Answer {
        let c = self.lane(lane);
        for _ in 0..5000 {
            tokio::time::sleep(TICK).await;
            c.tick();
        }
        Answer { n: c.ticks() }
    }
}
