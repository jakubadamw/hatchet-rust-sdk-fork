use hatchet_sdk::{ConcurrencyExpression, ConcurrencyLimitStrategy, Context, Hatchet, Runnable};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ConcurrencyInput {
    pub user_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ConcurrencyOutput {
    pub user_id: String,
}

pub async fn create_concurrency_task() -> hatchet_sdk::Task<ConcurrencyInput, ConcurrencyOutput> {
    async fn concurrency_task_func(
        input: ConcurrencyInput,
        ctx: Context,
    ) -> anyhow::Result<ConcurrencyOutput> {
        ctx.log(&format!("Processing user {}", input.user_id))
            .await?;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        Ok(ConcurrencyOutput {
            user_id: input.user_id,
        })
    }

    let hatchet: Hatchet = Hatchet::from_env().await.unwrap();

    // Run at most one task per user at a time, sharing slots fairly between users.
    hatchet
        .task("concurrency-task", concurrency_task_func)
        .concurrency(vec![ConcurrencyExpression::new(
            "input.user_id",
            1,
            ConcurrencyLimitStrategy::GroupRoundRobin,
        )])
        .build()
        .unwrap()
}

#[tokio::main]
#[allow(dead_code)]
async fn main() {
    dotenvy::dotenv().ok();

    let task = create_concurrency_task().await;

    // The runs for `alice` execute one after the other, while `bob`'s run executes alongside them.
    let inputs = ["alice", "alice", "bob"].map(|user_id| ConcurrencyInput {
        user_id: user_id.to_string(),
    });
    let results = futures::future::join_all(inputs.iter().map(|input| task.run(input, None))).await;
    for result in results {
        println!("Processed user {}", result.unwrap().user_id);
    }
}
