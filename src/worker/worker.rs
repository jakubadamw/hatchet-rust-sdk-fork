use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use crate::clients::grpc::v0::dispatcher;
use crate::clients::grpc::v0::dispatcher::WorkerRegisterRequest;
use crate::clients::hatchet::Hatchet;
use crate::clients::rest::apis::worker_api::worker_update;
use crate::clients::rest::models::WorkerUpdateRequest;
use crate::error::HatchetError;
use crate::runnables::*;
use crate::worker::action_listener::ActionListener;

/// How often a draining worker looks for its last task to have finished.
const DRAIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// A worker is a container for tasks that can be executed by a worker.
/// See [Hatchet.worker()](crate::Hatchet::worker()) for more information.
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Worker {
    pub name: String,
    client: Hatchet,
    #[builder(default = 100)]
    slots: i32,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    tasks: Arc<Mutex<HashMap<String, Arc<dyn ExecutableTask>>>>,
    #[builder(default = vec![])]
    workflows: Vec<crate::clients::grpc::v1::workflows::CreateWorkflowVersionRequest>,
    #[builder(default = HashMap::new())]
    labels: HashMap<String, String>,
}

impl Worker {
    /// Register a workflow with this worker. When the worker starts, it will register the workflow with Hatchet.
    /// Hatchet will then assign runs of the workflow to this worker.
    ///
    /// ```compile_fail
    /// use hatchet_sdk::{Context, Hatchet, EmptyModel};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let hatchet = Hatchet::from_env().await.unwrap();
    ///     let my_task = hatchet.task::<EmptyModel, EmptyModel, MyError>("my-task", |input: EmptyModel, _ctx: Context| async move {
    ///     Ok(EmptyModel)
    /// });
    ///
    /// let my_workflow = hatchet.workflow("my-workflow")
    ///     .build()
    ///     .unwrap()
    ///     .add_task(&my_task)
    ///
    ///     let worker = hatchet.worker("my-worker").build().unwrap();
    ///     worker.add_task_or_workflow(my_workflow);
    /// }
    /// ```
    async fn register_workflows(&mut self) {
        for workflow in &self.workflows {
            self.client
                .admin_client
                .put_workflow(workflow.clone())
                .await
                .unwrap();
        }
    }

    /// Start the worker.
    /// This will register the worker with Hatchet and start listening for assigned tasks.
    /// Use ctrl+c to stop the worker.
    ///
    /// ```compile_fail
    /// use hatchet_sdk::{Context, Hatchet, EmptyModel, Runnable,Register};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let hatchet = Hatchet::from_env().await.unwrap();
    ///     
    ///     let my_workflow = hatchet.
    ///         workflow::<EmptyModel, EmptyModel>("my-workflow")
    ///         .build()
    ///         .unwrap()
    ///         .add_task(&hatchet.task("my-task", async move |input: EmptyModel, _ctx: Context| -> anyhow::Result<EmptyModel> {
    ///             Ok(EmptyModel)
    ///         }))
    ///
    ///     let mut worker = hatchet.worker("my-worker")
    ///         .slots(5)
    ///         .build()
    ///         .unwrap()
    ///         .add_task_or_workflow(my_workflow);
    ///
    ///     worker.start().await.unwrap();
    /// }
    /// ```
    pub async fn start(&mut self) -> Result<(), HatchetError> {
        self.start_with_shutdown(std::future::pending()).await
    }

    /// Start the worker, as [`Worker::start`] does, and drain it once `shutdown`
    /// resolves, returning when it has drained.
    ///
    /// Draining pauses the worker in Hatchet, so that it is assigned nothing
    /// more, and waits for every task it is running to finish. The worker goes
    /// on heartbeating and listening meanwhile: one that stopped heartbeating
    /// would have its tasks taken off it and run again elsewhere after 30
    /// seconds, which is what draining is there to avoid, and an action assigned
    /// to it before the pause took effect is run rather than dropped.
    ///
    /// Nothing bounds the wait. A process that must be gone by a deadline drops
    /// the returned future, or exits, once the deadline comes; the tasks still
    /// running are then taken off the worker as they would have been without the
    /// drain.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), HatchetError> {
        log::info!("STARTING HATCHET...");
        let mut actions = vec![];
        for workflow in &self.workflows {
            for task in &workflow.tasks {
                actions.push(task.action.clone());
            }
        }
        log::debug!("{} waiting for actions: {:?}", self.name, actions);

        let worker_id = Arc::new(
            Self::register_worker(
                &mut self.client,
                &self.name,
                actions,
                self.slots,
                self.labels.clone(),
            )
            .await?,
        );
        self.register_workflows().await;

        let (action_tx, mut action_rx) =
            mpsc::channel::<dispatcher::AssignedAction>(self.slots as usize);

        let task_runs = Arc::new(Mutex::new(HashMap::new()));

        let dispatcher = Arc::new(tokio::sync::Mutex::new(
            crate::worker::task_dispatcher::TaskDispatcher {
                registry: self.tasks.clone(),
                client: self.client.clone(),
                task_runs: task_runs.clone(),
            },
        ));

        // Cloned for the pause, because the heartbeat below holds the worker's
        // own client mutably for as long as the worker runs.
        let pausing_client = self.client.clone();

        let action_listener = Arc::new(tokio::sync::Mutex::new(ActionListener::new(
            self.client.clone(),
        )));

        let worker_id_clone = worker_id.clone();
        let action_listener_handle = tokio::spawn(async move {
            log::debug!("starting action listener");
            action_listener
                .lock()
                .await
                .listen(worker_id_clone, action_tx)
                .await as Result<(), HatchetError>
        });

        // The heartbeat and the listener never finish of their own accord, and
        // either failing ends the worker. The dispatch loop is what finishes,
        // once the worker has drained.
        let heartbeat_and_listener = async {
            tokio::try_join!(
                async {
                    const HEARTBEAT_INTERVAL: u64 = 4;
                    loop {
                        log::debug!("sending heartbeat");
                        self.client.dispatcher_client.heartbeat(&worker_id).await?;
                        tokio::time::sleep(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL))
                            .await;
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), HatchetError>(())
                },
                // Joined rather than left to run on its own, so that a listener
                // which has given up ends the worker with it. Dropping the handle
                // instead discards the error, and because the heartbeat above never
                // returns, `start` would go on running — and the worker go on
                // looking healthy to the engine — long after it had stopped being
                // able to receive anything.
                async {
                    action_listener_handle.await.unwrap_or_else(|e| {
                        Err(HatchetError::InternalError(format!(
                            "action listener stopped unexpectedly: {e}"
                        )))
                    })
                }
            )
            .map(|_| ())
        };

        let dispatch = async {
            let mut shutdown = std::pin::pin!(shutdown);
            let mut draining = false;

            loop {
                tokio::select! {
                    biased;
                    action = action_rx.recv() => match action {
                        Some(action) => {
                            dispatcher
                                .lock()
                                .await
                                .dispatch(worker_id.clone(), action)
                                .await?
                        }
                        // Only the listener sends, so it has gone, and its own
                        // error says why.
                        None => {
                            return Err(HatchetError::InternalError(String::from(
                                "action listener stopped",
                            )));
                        }
                    },
                    () = &mut shutdown, if !draining => {
                        draining = true;
                        let running = task_runs.lock().expect("failed to acquire lock on task runs").len();
                        log::info!(
                            "shutting down: pausing the worker and waiting for its {running} running task(s) to finish"
                        );
                        pause(&pausing_client, &worker_id).await;
                    }
                    // Checked here, between dispatches, rather than on its own:
                    // an action taken off the channel is not in `task_runs`
                    // until it has been dispatched, and would otherwise be
                    // missed by a check made while it was on its way there.
                    () = tokio::time::sleep(DRAIN_POLL_INTERVAL), if draining => {
                        let idle = action_rx.is_empty()
                            && task_runs.lock().expect("failed to acquire lock on task runs").is_empty();

                        if idle {
                            log::info!("drained: no task is running any more");
                            return Ok(());
                        }
                    }
                }
            }
        };

        tokio::select! {
            result = heartbeat_and_listener => result,
            result = dispatch => result,
        }
    }

    async fn register_worker(
        client: &mut Hatchet,
        name: &str,
        actions: Vec<String>,
        slots: i32,
        labels: HashMap<String, String>,
    ) -> Result<String, HatchetError> {
        let registration = WorkerRegisterRequest {
            worker_name: name.to_string(),
            actions,
            services: vec![],
            slots: Some(slots),
            labels: labels
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        dispatcher::WorkerLabels {
                            str_value: Some(v),
                            int_value: None,
                        },
                    )
                })
                .collect(),
            webhook_id: None,
            runtime_info: None,
        };

        let response = client
            .dispatcher_client
            .register_worker(registration)
            .await?;

        Ok(response.into_inner().worker_id)
    }
}

impl<I, O> Register<Workflow<I, O>, I, O> for Worker
where
    I: Serialize + Send + Sync + 'static,
    O: DeserializeOwned + Send + Sync + 'static,
{
    fn add_task_or_workflow(mut self, workflow: &Workflow<I, O>) -> Self {
        self.workflows.push(workflow.to_proto());

        for task in &workflow.executable_tasks {
            let fully_qualified_name = format!("{}:{}", workflow.name, task.name());
            self.tasks
                .lock()
                .unwrap()
                .insert(fully_qualified_name, Arc::from(task.clone()));
        }
        self
    }
}

impl<I, O> Register<Task<I, O>, I, O> for Worker
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn add_task_or_workflow(mut self, workflow: &Task<I, O>) -> Self {
        let workflow_proto = workflow.to_standalone_workflow_proto();
        self.workflows.push(workflow_proto);

        let fully_qualified_name = format!("{}:{}", workflow.name, workflow.name);
        self.tasks
            .lock()
            .unwrap()
            .insert(fully_qualified_name, Arc::from(workflow.into_executable()));
        self
    }
}

/// Pause the worker in Hatchet, so that it is assigned no further tasks.
///
/// A pause that fails is logged rather than returned: the worker drains all the
/// same, only it may be handed more to run while it does.
async fn pause(client: &Hatchet, worker_id: &str) {
    let request = WorkerUpdateRequest {
        is_paused: Some(true),
    };

    if let Err(e) = worker_update(&client.rest_configuration(), worker_id, request).await {
        log::warn!(
            "could not pause worker {worker_id}, so it may be assigned more while it drains: {e}"
        );
    }
}

pub trait Register<T, I, O>
where
    I: Serialize + Send + Sync + 'static,
    O: DeserializeOwned + Send + Sync + 'static,
{
    fn add_task_or_workflow(self, workflow: &T) -> Self;
}
