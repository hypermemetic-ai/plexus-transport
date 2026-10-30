//! One socket's writer: it wakes for a frame, takes every other frame that
//! is ready — the method table's queue and this side's own, up to its
//! budget — writes them into the socket's buffer and flushes once, so a call's
//! three frames, or a stream's ready items, share one `send` (PERF-17's E2a:
//! a 64 B stream 7.7×, a call −25 %). Past the budget it yields, so one busy
//! socket cannot starve the others. It pings on the interval, and at the end
//! sends the close (or its reply) after letting both queues go, so every call
//! still writing learns the socket is gone.

use std::time::Duration;

use futures::stream::SplitSink;
use futures::SinkExt;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

use super::conn::Socket;
use super::Outbound;

/// How the socket ends, from the writer's side.
#[derive(Debug)]
pub(crate) enum Close {
    /// The peer closed: the reply tungstenite queued goes out.
    Reply,
    /// The server closes, with this frame.
    Send(CloseFrame),
    /// The socket is gone: nothing to send.
    Drop,
}

/// How long the close gets before the writer lets go.
const CLOSE_WAIT: Duration = Duration::from_secs(2);

fn text(raw: Box<RawValue>) -> Message {
    Message::Text(String::from(Box::<str>::from(raw)).into())
}

fn own(frame: Outbound) -> Message {
    match frame {
        Outbound::Text(t) => Message::Text(t.into()),
        Outbound::Binary(b) => Message::Binary(b),
    }
}

/// What woke the writer.
enum Woke {
    Table(Box<RawValue>),
    Own(Outbound),
    Ping,
    Close(Close),
}

pub(crate) async fn run(
    mut sink: SplitSink<Socket, Message>,
    mut table: mpsc::Receiver<Box<RawValue>>,
    mut own_rx: mpsc::Receiver<Outbound>,
    mut close: oneshot::Receiver<Close>,
    ping_interval: Option<Duration>,
    budget: usize,
) {
    let mut ping = ping_interval.map(|p| {
        let mut i = tokio::time::interval_at(tokio::time::Instant::now() + p, p);
        i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        i
    });
    let mut own_open = true;
    let ending = loop {
        let woke = tokio::select! {
            biased;
            c = &mut close => Woke::Close(c.unwrap_or(Close::Drop)),
            // Pings first among the rest: a socket streaming without pause
            // still says it is alive.
            _ = async { ping.as_mut().unwrap().tick().await }, if ping.is_some() => Woke::Ping,
            m = table.recv() => match m {
                Some(m) => Woke::Table(m),
                None => Woke::Close(Close::Drop),
            },
            m = own_rx.recv(), if own_open => match m {
                Some(f) => Woke::Own(f),
                None => {
                    own_open = false;
                    continue;
                }
            },
        };
        let first = match woke {
            Woke::Close(c) => break c,
            Woke::Ping => Message::Ping(Default::default()),
            Woke::Table(m) => text(m),
            Woke::Own(f) => own(f),
        };
        // This frame, and every frame ready, up to the budget.
        let mut gathered = first.len();
        if sink.feed(first).await.is_err() {
            break Close::Drop;
        }
        let mut failed = false;
        while gathered < budget {
            let next = match table.try_recv() {
                Ok(m) => text(m),
                Err(_) => match own_rx.try_recv() {
                    Ok(f) => own(f),
                    Err(_) => break,
                },
            };
            gathered += next.len();
            if sink.feed(next).await.is_err() {
                failed = true;
                break;
            }
        }
        if failed || sink.flush().await.is_err() {
            break Close::Drop;
        }
        if gathered >= budget {
            tokio::task::yield_now().await;
        }
    };
    // Every call still writing learns the socket is gone.
    table.close();
    own_rx.close();
    drop((table, own_rx));
    let _ = tokio::time::timeout(CLOSE_WAIT, async {
        match ending {
            Close::Send(frame) => {
                let _ = sink.send(Message::Close(Some(frame))).await;
                let _ = sink.close().await;
            }
            Close::Reply => {
                let _ = sink.close().await;
            }
            Close::Drop => {}
        }
    })
    .await;
}
