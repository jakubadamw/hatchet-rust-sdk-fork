use hatchet_sdk::{Context, Hatchet, HatchetLayer, Register};
use serde::{Deserialize, Serialize};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderInput {
    pub order_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderOutput {
    pub total: u32,
}

/// A plain helper, several frames below the handler, that has never heard of Hatchet.
/// Its events still land on the task run in the dashboard.
fn price_order(order_id: &str) -> u32 {
    tracing::info!(order_id, "pricing order");

    if order_id.is_empty() {
        tracing::warn!("order has no id, falling back to the default price");
        return 0;
    }

    let total = order_id.len() as u32 * 100;
    tracing::info!(total, "priced order");
    total
}

pub async fn create_logging_task() -> hatchet_sdk::Task<OrderInput, OrderOutput> {
    async fn logging_task_func(input: OrderInput, ctx: Context) -> anyhow::Result<OrderOutput> {
        // `ctx.log` still works and is unaffected by the tracing layer.
        ctx.log("starting the logging task").await?;

        // Span fields are folded into each event's metadata.
        let span = tracing::info_span!("handle_order", order_id = %input.order_id);
        let _guard = span.enter();

        let total = price_order(&input.order_id);

        tracing::info!("finished");

        Ok(OrderOutput { total })
    }

    let hatchet: Hatchet = Hatchet::from_env().await.unwrap();

    hatchet
        .task("logging-task", logging_task_func)
        .build()
        .unwrap()
}

#[tokio::main]
#[allow(dead_code)]
async fn main() {
    dotenvy::dotenv().ok();

    let hatchet = Hatchet::from_env().await.unwrap();

    // Print to stderr as usual, and forward to Hatchet on top. `init_tracing()` is the
    // one-liner equivalent of this stack.
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(HatchetLayer::new(&hatchet))
        .init();

    let task = create_logging_task().await;

    hatchet
        .worker("logging-worker")
        .build()
        .unwrap()
        .add_task_or_workflow(&task)
        .start()
        .await
        .unwrap();
}
