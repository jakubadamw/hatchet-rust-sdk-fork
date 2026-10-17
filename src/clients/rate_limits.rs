use super::grpc::WorkflowClient;
use super::grpc::v0::workflows::PutRateLimitRequest;
use crate::{HatchetError, RateLimitDuration};

/// A client for managing static rate limits.
///
/// Tasks consume a static limit through [`RateLimit::Static`](crate::RateLimit::Static).
#[derive(Clone, Debug)]
pub struct RateLimitsClient {
    workflow_client: WorkflowClient,
}

impl RateLimitsClient {
    pub(crate) fn new(workflow_client: WorkflowClient) -> Self {
        Self { workflow_client }
    }

    /// Create or update the static rate limit `key`, allowing `limit` units per `duration`.
    ///
    /// ```no_run
    /// use hatchet_sdk::{Hatchet, RateLimitDuration};
    /// #[tokio::main]
    /// async fn main() {
    ///     let hatchet = Hatchet::from_env().await.unwrap();
    ///     hatchet
    ///         .rate_limits
    ///         .put("external-api", 100, RateLimitDuration::Minute)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn put(
        &self,
        key: &str,
        limit: i32,
        duration: RateLimitDuration,
    ) -> Result<(), HatchetError> {
        self.workflow_client
            .put_rate_limit(PutRateLimitRequest {
                key: key.to_string(),
                limit,
                duration: duration.to_proto_v0() as i32,
            })
            .await
    }
}
