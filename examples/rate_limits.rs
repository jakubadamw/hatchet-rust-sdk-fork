use hatchet_sdk::{Context, Hatchet, RateLimit, RateLimitDuration, Runnable};
use serde::{Deserialize, Serialize};

pub const RATE_LIMIT_KEY: &str = "example-api";

#[derive(Serialize, Deserialize)]
pub struct RateLimitInput {
    pub user_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct RateLimitOutput {
    pub user_id: String,
}

pub async fn create_rate_limit_task() -> hatchet_sdk::Task<RateLimitInput, RateLimitOutput> {
    async fn rate_limit_task_func(
        input: RateLimitInput,
        _ctx: Context,
    ) -> anyhow::Result<RateLimitOutput> {
        Ok(RateLimitOutput {
            user_id: input.user_id,
        })
    }

    let hatchet: Hatchet = Hatchet::from_env().await.unwrap();

    // The static limit has to exist before a run consumes it.
    hatchet
        .rate_limits
        .put(RATE_LIMIT_KEY, 10, RateLimitDuration::Minute)
        .await
        .unwrap();

    hatchet
        .task("rate-limit-task", rate_limit_task_func)
        .rate_limits(vec![
            // Share ten runs per minute between all users.
            RateLimit::Static {
                key: RATE_LIMIT_KEY.to_string(),
                units: 1.into(),
            },
            // Allow each user three runs per minute.
            RateLimit::Dynamic {
                key_expression: String::from("input.user_id"),
                units: 1.into(),
                limit: 3.into(),
                duration: RateLimitDuration::Minute,
            },
        ])
        .build()
        .unwrap()
}

#[tokio::main]
#[allow(dead_code)]
async fn main() {
    dotenvy::dotenv().ok();

    let task = create_rate_limit_task().await;

    // The fourth run waits until `alice`'s limit is replenished a minute later.
    for _ in 0..4 {
        let input = RateLimitInput {
            user_id: String::from("alice"),
        };
        let result = task.run(&input, None).await.unwrap();
        println!("Processed user {}", result.user_id);
    }
}
