#[derive(derive_builder::Builder, Debug, Default, Clone)]
pub struct TriggerWorkflowOptions {
    #[builder(default = None)]
    pub additional_metadata: Option<serde_json::Value>,
    #[builder(default = None)]
    pub desired_worker_id: Option<String>,
    #[builder(default = None)]
    pub namespace: Option<String>,
    #[builder(default = false)]
    pub sticky: bool,
    /// The key a child run is known to its parent task by. Hatchet answers a
    /// parent that spawns a child under a key it has used before with the run it
    /// spawned then, so a retried parent rejoins its earlier child rather than
    /// starting another. Without one, a child is known by the order it was
    /// spawned in, which a retry that spawns different children breaks.
    #[builder(default = None)]
    pub key: Option<String>,
}
