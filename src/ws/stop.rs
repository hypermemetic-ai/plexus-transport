//! The server's stop: what `serve_websocket` hands back, and what the accept
//! loop and every connection watch. The handle keeps the shape of the one
//! jsonrpsee-server returned, so a caller's `stop()`, `stopped()` and
//! `is_stopped()` keep their meaning: `stop()` asks every connection and the
//! accept loop to end, `stopped()` resolves once all of them have, and
//! dropping the handle stops the server.

use std::sync::Arc;

use tokio::sync::watch;

/// The handle of a served WebSocket (see the module docs). Cheap to clone.
#[derive(Debug, Clone)]
pub struct ServerHandle(Arc<watch::Sender<bool>>);

/// `stop()` on a server that has already stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyStopped;

impl std::fmt::Display for AlreadyStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the server has already stopped")
    }
}

impl std::error::Error for AlreadyStopped {}

impl ServerHandle {
    /// Ask the accept loop and every connection to end. `Err` once every
    /// one of them already has.
    pub fn stop(&self) -> Result<(), AlreadyStopped> {
        self.0.send(true).map_err(|_| AlreadyStopped)
    }

    /// Resolves once the accept loop and every connection have ended.
    pub async fn stopped(self) {
        self.0.closed().await
    }

    /// Whether the accept loop and every connection have ended.
    pub fn is_stopped(&self) -> bool {
        self.0.is_closed()
    }
}

/// What the accept loop and each connection hold: the server's stop, seen.
#[derive(Debug, Clone)]
pub(crate) struct StopHandle(watch::Receiver<bool>);

impl StopHandle {
    /// Resolves once the server is stopped (or its handle dropped).
    pub(crate) async fn shutdown(mut self) {
        let _ = self.0.wait_for(|stopped| *stopped).await;
    }
}

/// A server's stop: the handle for its owner, the watch for its tasks.
pub(crate) fn stop_channel() -> (StopHandle, ServerHandle) {
    let (tx, rx) = watch::channel(false);
    (StopHandle(rx), ServerHandle(Arc::new(tx)))
}
