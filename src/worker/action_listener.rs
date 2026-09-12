use std::sync::Arc;
use std::time::Duration;

use crate::clients::grpc::v0::dispatcher;
use crate::clients::hatchet::Hatchet;
use crate::error::HatchetError;

/// How long to wait before reopening the action stream after it first drops.
/// Doubles with each consecutive failure, up to [`MAX_RECONNECT_BACKOFF`].
const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// The longest to wait between attempts to reopen the action stream.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

/// How many times the stream may fail to carry a single action, one attempt
/// after another, before the listener gives up and reports the worker unable to
/// receive work.
const MAX_RECONNECT_ATTEMPTS: usize = 10;

pub(crate) struct ActionListener {
    pub(crate) client: Hatchet,
}

impl ActionListener {
    pub(crate) fn new(client: Hatchet) -> Self {
        Self { client }
    }

    /// Listen for the actions assigned to this worker, reopening the stream
    /// whenever it ends.
    ///
    /// The engine ends this stream as a matter of course — when it is
    /// redeployed, and through whatever proxies and load balancers sit in front
    /// of it — so a stream ending is not by itself a reason to stop working.
    ///
    /// Reconnecting matters more than it looks. The heartbeat in
    /// [`crate::Worker::start`] runs independently of this stream, so a worker
    /// that stops listening does not stop looking healthy: it stays registered,
    /// keeps heartbeating, and is still assigned runs, every one of which then
    /// waits until it times out on scheduling. Without reconnecting, a single
    /// dropped stream takes a worker out until somebody restarts the process.
    ///
    /// Returns only once the stream has failed [`MAX_RECONNECT_ATTEMPTS`] times
    /// in a row without carrying anything, or once the dispatcher can no longer
    /// be sent to, which means the worker itself has gone.
    pub(crate) async fn listen(
        &mut self,
        worker_id: Arc<String>,
        tx: tokio::sync::mpsc::Sender<dispatcher::AssignedAction>,
    ) -> Result<(), HatchetError> {
        let mut consecutive_failures: usize = 0;

        loop {
            match self.listen_once(&worker_id, &tx).await {
                Ok(actions_received) => {
                    if actions_received > 0 {
                        // The stream did its job before it ended, so whatever
                        // ended it was not a standing fault. Start counting
                        // again, or a worker that runs for long enough would
                        // eventually exhaust the budget one routine reconnect
                        // at a time.
                        consecutive_failures = 0;
                    } else {
                        consecutive_failures += 1;
                        log::debug!(
                            "action stream closed without assigning anything (attempt {consecutive_failures})"
                        );
                    }
                }
                // The receiving half is gone, so there is nothing left to
                // listen on behalf of, and reopening the stream would only
                // fail the same way with an action in hand.
                Err(e @ HatchetError::DispatchError(_)) => return Err(e),
                Err(e) => {
                    consecutive_failures += 1;
                    log::warn!("action stream failed (attempt {consecutive_failures}): {e}");
                }
            }

            if consecutive_failures >= MAX_RECONNECT_ATTEMPTS {
                return Err(HatchetError::InternalError(format!(
                    "action stream could not be reopened after {MAX_RECONNECT_ATTEMPTS} attempts"
                )));
            }

            tokio::time::sleep(reconnect_backoff(consecutive_failures)).await;
        }
    }

    /// Open the action stream once and forward what it carries, returning the
    /// number of actions forwarded when it ends.
    async fn listen_once(
        &mut self,
        worker_id: &str,
        tx: &tokio::sync::mpsc::Sender<dispatcher::AssignedAction>,
    ) -> Result<u64, HatchetError> {
        let mut response = self.client.dispatcher_client.listen(worker_id).await?;
        let mut actions_received = 0;

        loop {
            match response.message().await {
                Ok(Some(message)) => {
                    actions_received += 1;
                    tx.send(message)
                        .await
                        .map_err(|e| HatchetError::DispatchError(e.to_string()))?;
                }
                Ok(None) => return Ok(actions_received),
                Err(e) => {
                    // A stream that failed part way through still carried what
                    // it carried, so the count goes back with the error rather
                    // than being lost with it.
                    if actions_received > 0 {
                        log::warn!("action stream failed after {actions_received} action(s): {e}");
                        return Ok(actions_received);
                    }
                    return Err(HatchetError::GrpcErrorStatus(e.message().to_string()));
                }
            }
        }
    }
}

/// How long to wait before the next attempt to reopen the stream, doubling with
/// each consecutive failure and levelling off at [`MAX_RECONNECT_BACKOFF`].
fn reconnect_backoff(consecutive_failures: usize) -> Duration {
    // Capped at 16 doublings before the shift, which is far past the point the
    // maximum takes over, and keeps the shift itself in range.
    let doublings = consecutive_failures.saturating_sub(1).min(16);

    INITIAL_RECONNECT_BACKOFF
        .saturating_mul(1_u32 << doublings)
        .min(MAX_RECONNECT_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_starts_at_the_initial_delay() {
        assert_eq!(reconnect_backoff(1), INITIAL_RECONNECT_BACKOFF);
    }

    #[test]
    fn backoff_doubles_with_each_consecutive_failure() {
        assert_eq!(reconnect_backoff(2), INITIAL_RECONNECT_BACKOFF * 2);
        assert_eq!(reconnect_backoff(3), INITIAL_RECONNECT_BACKOFF * 4);
    }

    #[test]
    fn backoff_levels_off_at_the_maximum() {
        assert_eq!(reconnect_backoff(20), MAX_RECONNECT_BACKOFF);
        assert_eq!(reconnect_backoff(usize::MAX), MAX_RECONNECT_BACKOFF);
    }
}
