use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimpleInput {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimpleOutput {
    pub transformed_message: String,
}

/// The status of a workflow run, as reported by the REST API.
#[cfg(feature = "tracing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[cfg(feature = "tracing")]
impl RunStatus {
    /// Whether the run has stopped and its status will not change again.
    pub fn is_final(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}
