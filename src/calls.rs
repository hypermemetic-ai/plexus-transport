//! BIDIR-8: the calls open on one connection, counted by the transport.
//!
//! Every Plexus call is a subscription for as long as its stream runs — a
//! unary answer for a moment, a `watch` for a session, a duplex call
//! (BIDIR-1) for as long as either half is open — and each is **one** call
//! on its connection. [`ConnectionCalls`] keeps that count: each WebSocket
//! [`Connection`](crate::websocket::Connection) carries one
//! ([`Connection::calls`](crate::websocket::Connection::calls)) at its
//! quotas' [`max_calls_per_connection`](crate::ConnectionQuotas::max_calls_per_connection).
//! A call takes its place with [`ConnectionCalls::try_open`] (or, for a
//! duplex call, through plexus-core's `DuplexGate::open`, which takes it
//! after the grant gate admits the call) and gives it back when the place
//! is dropped. Past the max it is refused `too_many_calls {max}`.
//!
//! It was menger's (ACCESS-29's `--max-calls-per-connection`, `calls.rs`);
//! a count every server needs is the transport's, so no service writes it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use plexus_core::plexus::duplex_gate::{CallCounter, CallPlace};

/// The calls a connection may have open at once, unless its quotas say
/// otherwise: room for an app's watches and windows, a quarter of
/// jsonrpsee's 1,024 subscriptions (menger's ACCESS-29 default).
pub const CALLS_PER_CONNECTION: usize = 256;

/// A call refused for want of a place: the connection holds `max` already.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooManyCalls {
    pub max: usize,
}

impl std::fmt::Display for TooManyCalls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "too many calls open on this connection (at most {})", self.max)
    }
}

impl std::error::Error for TooManyCalls {}

/// The calls open on one connection (the module docs). Cheap to clone: the
/// clones share one count.
#[derive(Debug, Clone)]
pub struct ConnectionCalls {
    open: Arc<AtomicUsize>,
    max: usize,
}

/// What a place gives back when dropped.
struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ConnectionCalls {
    /// A count at 0, allowing `max` calls at once.
    pub fn new(max: usize) -> Self {
        Self { open: Arc::new(AtomicUsize::new(0)), max }
    }

    /// The most calls this connection may have open at once.
    pub fn max(&self) -> usize {
        self.max
    }

    /// The calls open now.
    pub fn open_count(&self) -> usize {
        self.open.load(Ordering::Acquire)
    }

    /// A place for one more call, held until dropped; or `too_many_calls`.
    pub fn try_open(&self) -> Result<CallPlace, TooManyCalls> {
        self.try_open_within(self.max)
    }

    /// [`try_open`](Self::try_open), held to `limit` too — a module with a
    /// stricter limit of its own (menger's `--max-calls-per-connection`)
    /// counts in the connection's one count, within the smaller of the two.
    pub fn try_open_within(&self, limit: usize) -> Result<CallPlace, TooManyCalls> {
        let max = self.max.min(limit);
        let mut now = self.open.load(Ordering::Acquire);
        loop {
            if now >= max {
                return Err(TooManyCalls { max });
            }
            match self.open.compare_exchange(now, now + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(CallPlace::new(Held(self.open.clone()))),
                Err(seen) => now = seen,
            }
        }
    }
}

impl CallCounter for ConnectionCalls {
    fn take_place(&self) -> Result<CallPlace, usize> {
        self.try_open().map_err(|e| e.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_is_given_back_when_dropped() {
        let c = ConnectionCalls::new(1);
        let p = c.try_open().unwrap();
        assert_eq!(c.try_open().err(), Some(TooManyCalls { max: 1 }));
        drop(p);
        assert_eq!(c.open_count(), 0);
        let shared = c.clone();
        let _q = shared.try_open().unwrap();
        assert_eq!(c.open_count(), 1, "clones share one count");
    }
}
