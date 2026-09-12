use std::time::Duration;

use hatchet_sdk::{Hatchet, Register, Task, Workflow};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::containers::shared_containers;
use super::types::{SimpleInput, SimpleOutput};

const WORKER_READY_TIMEOUT: Duration = Duration::from_secs(15);
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(feature = "tracing")]
const LOGS_TIMEOUT: Duration = Duration::from_secs(20);
#[cfg(feature = "tracing")]
const LOGS_POLL_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(feature = "tracing")]
const RUN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "tracing")]
const RUN_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub struct TestHarness {
    pub hatchet: Hatchet,
    rest_base_url: String,
    rest_token: String,
    tenant_id: String,
    prefix: String,
}

impl TestHarness {
    pub async fn new(test_name: &str) -> Self {
        let containers = shared_containers().await;
        let hatchet = Hatchet::from_token(
            &containers.server_url,
            &containers.grpc_address,
            &containers.token,
            "none",
        )
        .await
        .unwrap();

        Self {
            hatchet,
            rest_base_url: containers.server_url.clone(),
            rest_token: containers.token.clone(),
            tenant_id: containers.tenant_id.clone(),
            prefix: test_name.to_string(),
        }
    }

    pub fn prefixed(&self, name: &str) -> String {
        format!("{}-{}", self.prefix, name)
    }

    pub fn simple_task(&self, name: &str) -> Task<SimpleInput, SimpleOutput> {
        self.hatchet
            .task(
                &self.prefixed(name),
                async move |input: SimpleInput,
                            _ctx: hatchet_sdk::Context|
                            -> anyhow::Result<SimpleOutput> {
                    Ok(SimpleOutput {
                        transformed_message: input.message.clone(),
                    })
                },
            )
            .build()
            .unwrap()
    }

    pub async fn spawn_worker_for_task<I, O>(&self, task: &Task<I, O>) -> WorkerGuard
    where
        I: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
        O: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    {
        let hatchet = self.hatchet.clone();
        let task = task.clone();
        let worker_name = self.prefixed("worker");
        let handle = tokio::spawn(async move {
            hatchet
                .worker(&worker_name)
                .build()
                .unwrap()
                .add_task_or_workflow(&task)
                .start()
                .await
                .unwrap()
        });
        self.wait_for_worker_ready().await;
        WorkerGuard { handle }
    }

    pub async fn spawn_worker_for_workflow<I, O>(&self, workflow: &Workflow<I, O>) -> WorkerGuard
    where
        I: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
        O: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    {
        let hatchet = self.hatchet.clone();
        let workflow = workflow.clone();
        let worker_name = self.prefixed("worker");
        let handle = tokio::spawn(async move {
            hatchet
                .worker(&worker_name)
                .build()
                .unwrap()
                .add_task_or_workflow(&workflow)
                .start()
                .await
                .unwrap()
        });
        self.wait_for_worker_ready().await;
        WorkerGuard { handle }
    }

    pub async fn spawn_worker_for_tasks<I, O>(&self, tasks: &[&Task<I, O>]) -> WorkerGuard
    where
        I: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
        O: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    {
        let hatchet = self.hatchet.clone();
        let tasks: Vec<Task<I, O>> = tasks.iter().map(|t| (*t).clone()).collect();
        let worker_name = self.prefixed("worker");
        let handle = tokio::spawn(async move {
            let mut worker = hatchet.worker(&worker_name).build().unwrap();
            for task in &tasks {
                worker = worker.add_task_or_workflow(task);
            }
            worker.start().await.unwrap()
        });
        self.wait_for_worker_ready().await;
        WorkerGuard { handle }
    }

    #[cfg(feature = "tracing")]
    /// Block until a workflow run leaves the `RUNNING`/`QUEUED` states, returning its
    /// final status.
    pub async fn wait_for_run(&self, workflow_run_id: &str) -> String {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v1/stable/workflow-runs/{}/status",
            self.rest_base_url, workflow_run_id
        );
        let deadline = tokio::time::Instant::now() + RUN_TIMEOUT;

        loop {
            if let Ok(response) = client.get(&url).bearer_auth(&self.rest_token).send().await
                && let Ok(body) = response.text().await
            {
                let status = body.trim().trim_matches('"').to_string();
                if !matches!(status.as_str(), "RUNNING" | "QUEUED" | "") {
                    return status;
                }
            }

            if tokio::time::Instant::now() > deadline {
                panic!("workflow run {workflow_run_id} did not finish within {RUN_TIMEOUT:?}");
            }

            tokio::time::sleep(RUN_POLL_INTERVAL).await;
        }
    }

    #[cfg(feature = "tracing")]
    /// Resolve a workflow run id to the external id of its first task.
    ///
    /// The logs endpoint is keyed by task run, not workflow run, so a single-task
    /// workflow still needs this hop.
    pub async fn first_task_run_id(&self, workflow_run_id: &str) -> String {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v1/stable/workflow-runs/{}",
            self.rest_base_url, workflow_run_id
        );

        let body: serde_json::Value = client
            .get(&url)
            .bearer_auth(&self.rest_token)
            .send()
            .await
            .expect("failed to fetch workflow run")
            .json()
            .await
            .expect("workflow run response was not JSON");

        body["tasks"][0]["taskExternalId"]
            .as_str()
            .expect("workflow run had no tasks")
            .to_string()
    }

    #[cfg(feature = "tracing")]
    /// Fetch the log lines the dashboard would show for a task run, as
    /// `(level, message)` pairs in the order the API returns them.
    ///
    /// Log delivery is asynchronous, so this polls until at least `expected` lines are
    /// present rather than reading once.
    pub async fn task_logs(&self, task_run_id: &str, expected: usize) -> Vec<(String, String)> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v1/stable/tasks/{}/logs",
            self.rest_base_url, task_run_id
        );
        let deadline = tokio::time::Instant::now() + LOGS_TIMEOUT;

        loop {
            let mut lines = Vec::new();

            if let Ok(response) = client.get(&url).bearer_auth(&self.rest_token).send().await
                && let Ok(body) = response.json::<serde_json::Value>().await
                && let Some(rows) = body.get("rows").and_then(|rows| rows.as_array())
            {
                for row in rows {
                    lines.push((
                        row.get("level")
                            .and_then(|level| level.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        row.get("message")
                            .and_then(|message| message.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    ));
                }
            }

            if lines.len() >= expected || tokio::time::Instant::now() > deadline {
                return lines;
            }

            tokio::time::sleep(LOGS_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_worker_ready(&self) {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v1/tenants/{}/workflows",
            self.rest_base_url, self.tenant_id
        );
        let prefix_lower = self.prefix.to_lowercase();
        let deadline = tokio::time::Instant::now() + WORKER_READY_TIMEOUT;

        loop {
            if let Ok(resp) = client.get(&url).bearer_auth(&self.rest_token).send().await {
                if let Ok(body) = resp.text().await {
                    if body.to_lowercase().contains(&prefix_lower) {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        return;
                    }
                }
            }

            if tokio::time::Instant::now() > deadline {
                panic!(
                    "Worker did not register workflow with prefix '{}' within {:?}",
                    self.prefix, WORKER_READY_TIMEOUT
                );
            }

            tokio::time::sleep(WORKER_POLL_INTERVAL).await;
        }
    }
}

pub fn hatchet_version_at_least(major: u32, minor: u32) -> bool {
    let version = std::env::var("TEST_HATCHET_LITE_VERSION").unwrap_or("latest".to_string());
    if version == "latest" {
        return true;
    }
    let version = version.trim_start_matches('v');
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() < 2 {
        return false;
    }
    let Ok(v_major) = parts[0].parse::<u32>() else {
        return false;
    };
    let Ok(v_minor) = parts[1].parse::<u32>() else {
        return false;
    };
    (v_major, v_minor) >= (major, minor)
}

pub struct WorkerGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
