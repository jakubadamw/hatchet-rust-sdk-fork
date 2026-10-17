# 🪓 Hatchet SDK for Rust

This is an unofficial Rust SDK for [Hatchet](https://hatchet.run), a distributed, fault-tolerant task queue.
This crate allows you to integrate Hatchet into your Rust applications.

## Setup

### Install `protoc`

This crate uses `tonic` to generate gRPC client stubs from Hatchet's protobuf files. To build the library, you'll need to install the Protocol Buffer Compiler (`protoc`). See the [installation instructions](https://protobuf.dev/installation/) for your operating system.

### Add crate to dependencies

Add the SDK as a dependency to your Rust project with Cargo:

```shell
cargo add hatchet-sdk
```

### Hatchet authentication

We recommend adding your Hatchet API token to a `.env` file and installing [dotenvy](https://crates.io/crates/dotenvy) to load it in your application for local development.

## Hatchet Version Compatibility

This library is tested against the following Hatchet versions:

| Version    | Compatible |
| :------: | :-------: |
| v0.67.0  | ❌ |
| v0.68.0  | ✅ |
| v0.69.0  | ✅ |
| v0.70.0  | ✅ |
| v0.71.0  | ✅ |
| v0.72.0  | ✅ |
| v0.73.0  | ✅ |
| v0.74.0  | ✅ |
| v0.75.0  | ✅ |
| v0.77.0  | ✅ |
| v0.78.0  | ✅ |
| v0.79.0  | ✅ |
| v0.80.0  | ✅ |
| v0.81.0  | ✅ |
| v0.82.0  | ✅ |
| v0.83.0  | ✅ |

## Declaring Your First Task

### Defining a task

Start by declaring a task with a name. The task object can be built with optional configuration options.
Tasks have input and output types, which should implement the `Serialize` and `Deserialize` traits from `serde` for JSON serialization and deserialization.

### Running a task

With your task defined, you can import it wherever you need to use it and invoke it with the `run` method.
<div class="warning">NOTE: You must first register the task on a worker before you can run it.</div>

```rust no_run
use hatchet_sdk::anyhow;
use hatchet_sdk::serde::*;
use hatchet_sdk::tokio;
use hatchet_sdk::{Context, Hatchet, Runnable, TriggerWorkflowOptionsBuilder};

#[derive(Serialize, Deserialize)]
#[serde(crate = "hatchet_sdk::serde")]
struct SimpleInput {
    pub message: String,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "hatchet_sdk::serde")]
struct SimpleOutput {
    pub transformed_message: String,
}

async fn simple_task_func(input: SimpleInput, ctx: Context) -> anyhow::Result<SimpleOutput> {
    ctx.log("Starting simple task").await?;
    Ok(SimpleOutput {
        transformed_message: input.message.to_lowercase(),
    })
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let hatchet: Hatchet = Hatchet::from_env().await.unwrap();

    let simple_task = hatchet
        .task("simple-task", simple_task_func)
        .build()
        .unwrap();

    let input = SimpleInput {
        message: String::from("Hello, world!"),
    };

    let options = TriggerWorkflowOptionsBuilder::default()
        .additional_metadata(Some(serde_json::json!({
            "environment": "dev",
        })))
        .build()
        .unwrap();

    // Run the task asynchronously, immediately returning the run ID
    let _run_id = simple_task.run_no_wait(&input, Some(&options)).await.unwrap();
    // Run the task synchronously, waiting for a worker to complete it and return the result
    let result = simple_task.run(&input, Some(&options)).await.unwrap();
    println!("Result: {}", result.transformed_message);
}

```

## Workers

Workers are responsible for executing individual tasks.

### Declaring a Worker

Declare a worker by calling the worker method on the Hatchet client. Tasks and workflows can be added to the worker. When the worker starts
it will register the tasks with the Hatchet engine, allowing them to be triggered and assigned.

```rust no_run
use hatchet_sdk::{Context, EmptyModel, Hatchet, Register, anyhow, serde_json, tokio};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let hatchet = Hatchet::from_env().await.unwrap();

    async fn simple_task_func(
        _input: EmptyModel,
        ctx: Context,
    ) -> anyhow::Result<serde_json::Value> {
        ctx.log("Starting simple task").await?;
        Ok(serde_json::json!({"message": "success"}))
    }

    let simple_task = hatchet
        .task("simple-task", simple_task_func)
        .build()
        .unwrap();

    hatchet
        .worker("example-worker")
        .build()
        .unwrap()
        .add_task_or_workflow(&simple_task)
        .start()
        .await
        .unwrap();
}
```

## Logging

Anything a task logs with `ctx.log` shows up against that task run in the Hatchet dashboard:

```rust no_run
use hatchet_sdk::{Context, EmptyModel, anyhow, serde_json};

async fn my_task(_input: EmptyModel, ctx: Context) -> anyhow::Result<serde_json::Value> {
    ctx.log("starting simple task").await?;
    Ok(serde_json::json!({"message": "success"}))
}
```

That means threading `ctx` into every function that wants to log, though. Enable the
`tracing` feature and you can instead let Hatchet pick up the `tracing` events your code
already emits:

```toml
[dependencies]
hatchet-sdk = { version = "...", features = ["tracing"] }
```

Add `HatchetLayer` to your subscriber stack, and every event recorded while a task handler
is running is forwarded to that task run — however deep in the call stack it came from, and
without the emitting code ever seeing a `Context`:

```rust ignore
use hatchet_sdk::{Context, EmptyModel, Hatchet, HatchetLayer, Register, anyhow, serde_json, tokio};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// A plain helper that has never heard of Hatchet.
fn price_order(order_id: &str) -> u32 {
    tracing::info!(order_id, "pricing order");
    order_id.len() as u32 * 100
}

#[tokio::main]
async fn main() {
    let hatchet = Hatchet::from_env().await.unwrap();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(HatchetLayer::new(&hatchet))
        .init();

    async fn my_task(_input: EmptyModel, _ctx: Context) -> anyhow::Result<serde_json::Value> {
        let total = price_order("abc123");
        tracing::info!(total, "priced order");
        Ok(serde_json::json!({ "total": total }))
    }

    let task = hatchet.task("my-task", my_task).build().unwrap();

    hatchet
        .worker("example-worker")
        .build()
        .unwrap()
        .add_task_or_workflow(&task)
        .start()
        .await
        .unwrap();
}
```

`Hatchet::init_tracing()` is a one-line shorthand for exactly the stack above, if you do not
need to compose the layer with anything else.

Events are mapped onto the four levels the dashboard understands — `DEBUG`, `INFO`, `WARN`
and `ERROR` (`TRACE` is reported as `DEBUG`) — and an event's structured fields, plus those
of any enclosing spans, are attached to the log line as metadata.

A few things worth knowing:

- **Events emitted outside a task run are ignored**, so a worker's own start-up logging does
  not reach the dashboard.
- **Only events at `INFO` and above are forwarded** by default. Use
  `HatchetLayer::new(&hatchet).with_max_level(tracing::Level::DEBUG)` to widen that.
- **This SDK's own events and those of the networking crates it is built on are dropped**
  (`hatchet_sdk`, `h2`, `hyper`, `tonic`, `tower`, `rustls`, `reqwest`), because a `tracing`
  registry otherwise sees every gRPC span in the process. Override the list with
  `with_ignored_targets`.
- **The current task run is tracked with a Tokio task-local, which a bare `tokio::spawn`
  does not inherit.** An event emitted from a freshly spawned sub-task is dropped rather
  than misattributed; use `ctx.log` from there instead.
- Hatchet limits a task run to 1000 log lines.

## Declarative Workflow Design (DAGs)

Hatchet workflows are designed in a Directed Acyclic Graph (DAG) format,
where each task is a node in the graph, and the dependencies between tasks are the edges.

### Building a DAG with Task Dependencies

The power of Hatchet’s workflow design comes from connecting tasks into a DAG structure.
Tasks can specify dependencies (parents) which must complete successfully before the task can start.

### Running a Workflow

You can run workflows directly or enqueue them for asynchronous execution.

```rust no_run
use hatchet_sdk::serde::{Deserialize, Serialize};
use hatchet_sdk::{Context, EmptyModel, Hatchet, Runnable, anyhow, serde_json, tokio};

#[derive(Serialize, Deserialize)]
#[serde(crate = "hatchet_sdk::serde")]
struct FirstTaskOutput {
    output: String,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "hatchet_sdk::serde")]
struct SecondTaskOutput {
    first_step_result: String,
    final_result: String,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "hatchet_sdk::serde")]
pub struct WorkflowOutput {
    first_task: FirstTaskOutput,
    second_task: SecondTaskOutput,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let hatchet = Hatchet::from_env().await.unwrap();

    let first_task = hatchet
        .task(
            "first_task",
            async move |_input: EmptyModel, _ctx: Context| -> anyhow::Result<FirstTaskOutput> {
                Ok(FirstTaskOutput {
                    output: "Hello World".to_string(),
                })
            },
        )
        .build()
        .unwrap();

    let second_task = hatchet
        .task(
            "second_task",
            async move |_input: EmptyModel, ctx: Context| -> anyhow::Result<SecondTaskOutput> {
                let first_result = ctx.parent_output("first_task").await?;
                Ok(SecondTaskOutput {
                    first_step_result: first_result.get("output").unwrap().to_string(),
                    final_result: "Completed".to_string(),
                })
            },
        )
        .build()
        .unwrap()
        .add_parent(&first_task);

    let workflow = hatchet
        .workflow::<EmptyModel, WorkflowOutput>("dag-workflow")
        .build()
        .unwrap()
        .add_task(&first_task)
        .add_task(&second_task);

    // Run the workflow asynchronously, immediately returning the run ID
    let _run_id = workflow.run_no_wait(&EmptyModel, None).await.unwrap();
    // Run the workflow synchronously, waiting for a worker to complete it and return the result
    let result = workflow.run(&EmptyModel, None).await.unwrap();
    println!(
        "First task result: {}",
        serde_json::to_string(&result.first_task).unwrap()
    );
    println!(
        "Second task result: {}",
        serde_json::to_string(&result.second_task).unwrap()
    );
}
```
