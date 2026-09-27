use crate::clients::grpc::v0::workflows::RateLimitDuration as RateLimitDurationProtoV0;
use crate::clients::grpc::v1::workflows::{
    Concurrency as ConcurrencyProto, ConcurrencyLimitStrategy as ConcurrencyLimitStrategyProto,
    CreateTaskRateLimit, RateLimitDuration as RateLimitDurationProto,
};

/// The strategy applied when a concurrency limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyLimitStrategy {
    /// Cancel the in-progress run to make room for the new one.
    CancelInProgress,
    /// Queue new runs and distribute slots between concurrency keys in a round-robin fashion.
    GroupRoundRobin,
    /// Cancel the new run.
    CancelNewest,
}

impl ConcurrencyLimitStrategy {
    fn to_proto(self) -> ConcurrencyLimitStrategyProto {
        match self {
            ConcurrencyLimitStrategy::CancelInProgress => {
                ConcurrencyLimitStrategyProto::CancelInProgress
            }
            ConcurrencyLimitStrategy::GroupRoundRobin => {
                ConcurrencyLimitStrategyProto::GroupRoundRobin
            }
            ConcurrencyLimitStrategy::CancelNewest => ConcurrencyLimitStrategyProto::CancelNewest,
        }
    }
}

/// Limits how many runs sharing a concurrency key can execute at the same time.
///
/// Set on a workflow, it limits concurrent runs of the workflow. Set on a standalone task, it
/// limits concurrent runs of that task. Set on a task that is part of a workflow, it limits
/// concurrent runs of that task only. To have several tasks or workflows count against one
/// limit, create the entry with [`ConcurrencyExpression::shared`].
///
/// ```
/// use hatchet_sdk::{ConcurrencyExpression, ConcurrencyLimitStrategy};
///
/// // At most two runs per user at a time, with slots shared fairly between users.
/// let concurrency = ConcurrencyExpression::new(
///     "input.user_id",
///     2,
///     ConcurrencyLimitStrategy::GroupRoundRobin,
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyExpression {
    /// A CEL expression evaluated against the run to compute its concurrency key, e.g. `input.user_id`.
    pub expression: String,
    /// The maximum number of runs with the same key that can execute at the same time.
    pub max_runs: i32,
    /// What to do with a run once the limit is reached.
    pub limit_strategy: ConcurrencyLimitStrategy,
    /// The name of the limit when it is shared across the tenant, or `None` when it is not.
    pub shared_name: Option<String>,
}

impl ConcurrencyExpression {
    pub fn new(
        expression: impl Into<String>,
        max_runs: i32,
        limit_strategy: ConcurrencyLimitStrategy,
    ) -> Self {
        Self {
            expression: expression.into(),
            max_runs,
            limit_strategy,
            shared_name: None,
        }
    }

    /// A limit shared across the tenant: every task or workflow declaring an entry with the
    /// same `name` counts against the same limit.
    ///
    /// The limit is defined by whichever registration came last, so declare it with the same
    /// expression, maximum and strategy everywhere, e.g. by building it in one place. A
    /// workflow with several tasks can only declare a shared limit at the workflow level if the
    /// engine runs it with the DAG operator; declaring it on the tasks always works. When a task
    /// or workflow declares several shared limits, every declaration has to list them in the
    /// same order, otherwise registration fails.
    ///
    /// Requires Hatchet v0.106.0 or later. Older engines ignore the name and treat the entry as
    /// an ordinary limit.
    ///
    /// ```
    /// use hatchet_sdk::{ConcurrencyExpression, ConcurrencyLimitStrategy};
    ///
    /// // At most two renders at a time, across every task that declares this limit.
    /// let render_limit = ConcurrencyExpression::shared(
    ///     "render-page",
    ///     "'all'",
    ///     2,
    ///     ConcurrencyLimitStrategy::GroupRoundRobin,
    /// );
    /// ```
    pub fn shared(
        name: impl Into<String>,
        expression: impl Into<String>,
        max_runs: i32,
        limit_strategy: ConcurrencyLimitStrategy,
    ) -> Self {
        Self {
            shared_name: Some(name.into()),
            ..Self::new(expression, max_runs, limit_strategy)
        }
    }

    pub(crate) fn to_proto(&self) -> ConcurrencyProto {
        ConcurrencyProto {
            expression: self.expression.clone(),
            max_runs: Some(self.max_runs),
            limit_strategy: Some(self.limit_strategy.to_proto() as i32),
            name: self.shared_name.clone(),
            is_tenant_scoped: self.shared_name.is_some().then_some(true),
        }
    }
}

/// The window over which a rate limit is enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDuration {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl RateLimitDuration {
    fn to_proto(self) -> RateLimitDurationProto {
        match self {
            RateLimitDuration::Second => RateLimitDurationProto::Second,
            RateLimitDuration::Minute => RateLimitDurationProto::Minute,
            RateLimitDuration::Hour => RateLimitDurationProto::Hour,
            RateLimitDuration::Day => RateLimitDurationProto::Day,
            RateLimitDuration::Week => RateLimitDurationProto::Week,
            RateLimitDuration::Month => RateLimitDurationProto::Month,
            RateLimitDuration::Year => RateLimitDurationProto::Year,
        }
    }

    pub(crate) fn to_proto_v0(self) -> RateLimitDurationProtoV0 {
        match self {
            RateLimitDuration::Second => RateLimitDurationProtoV0::Second,
            RateLimitDuration::Minute => RateLimitDurationProtoV0::Minute,
            RateLimitDuration::Hour => RateLimitDurationProtoV0::Hour,
            RateLimitDuration::Day => RateLimitDurationProtoV0::Day,
            RateLimitDuration::Week => RateLimitDurationProtoV0::Week,
            RateLimitDuration::Month => RateLimitDurationProtoV0::Month,
            RateLimitDuration::Year => RateLimitDurationProtoV0::Year,
        }
    }
}

/// A rate-limit quantity: either a fixed number or a CEL expression evaluated against the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimitValue {
    Fixed(i32),
    Expression(String),
}

impl From<i32> for RateLimitValue {
    fn from(value: i32) -> Self {
        RateLimitValue::Fixed(value)
    }
}

impl RateLimitValue {
    /// Split the value into the fixed and expression fields of the proto.
    fn to_proto_parts(&self) -> (Option<i32>, Option<String>) {
        match self {
            RateLimitValue::Fixed(value) => (Some(*value), None),
            RateLimitValue::Expression(expression) => (None, Some(expression.clone())),
        }
    }

    fn to_expression(&self) -> String {
        match self {
            RateLimitValue::Fixed(value) => value.to_string(),
            RateLimitValue::Expression(expression) => expression.clone(),
        }
    }
}

/// A rate limit consumed by each run of a task.
///
/// ```
/// use hatchet_sdk::{RateLimit, RateLimitDuration, RateLimitValue};
///
/// // Consume one unit of a limit created with `hatchet.rate_limits.put(...)`.
/// let global = RateLimit::Static {
///     key: String::from("external-api"),
///     units: 1.into(),
/// };
///
/// // Allow each user ten runs per minute.
/// let per_user = RateLimit::Dynamic {
///     key_expression: String::from("input.user_id"),
///     units: 1.into(),
///     limit: 10.into(),
///     duration: RateLimitDuration::Minute,
/// };
///
/// // Consume as many units as the input says.
/// let weighted = RateLimit::Static {
///     key: String::from("external-api"),
///     units: RateLimitValue::Expression(String::from("input.weight")),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimit {
    /// A limit with a fixed key. The limit itself must be created beforehand with
    /// [`RateLimitsClient::put`](crate::RateLimitsClient::put).
    Static {
        /// The key of the rate limit.
        key: String,
        /// The number of units each run consumes.
        units: RateLimitValue,
    },
    /// A limit whose key is computed per run, so each distinct key gets its own limit.
    Dynamic {
        /// A CEL expression evaluated against the run to compute the key, e.g. `input.user_id`.
        key_expression: String,
        /// The number of units each run consumes.
        units: RateLimitValue,
        /// The number of units available for each key per `duration`.
        limit: RateLimitValue,
        /// The window over which the limit is enforced.
        duration: RateLimitDuration,
    },
}

impl RateLimit {
    pub(crate) fn to_proto(&self) -> CreateTaskRateLimit {
        match self {
            RateLimit::Static { key, units } => {
                let (units, units_expr) = units.to_proto_parts();
                CreateTaskRateLimit {
                    key: key.clone(),
                    units,
                    key_expr: None,
                    units_expr,
                    limit_values_expr: None,
                    duration: None,
                }
            }
            RateLimit::Dynamic {
                key_expression,
                units,
                limit,
                duration,
            } => {
                let (units, units_expr) = units.to_proto_parts();
                CreateTaskRateLimit {
                    key: key_expression.clone(),
                    units,
                    key_expr: Some(key_expression.clone()),
                    units_expr,
                    limit_values_expr: Some(limit.to_expression()),
                    duration: Some(duration.to_proto() as i32),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_expression_to_proto() {
        let proto =
            ConcurrencyExpression::new("input.group", 3, ConcurrencyLimitStrategy::GroupRoundRobin)
                .to_proto();

        assert_eq!(proto.expression, "input.group");
        assert_eq!(proto.max_runs, Some(3));
        assert_eq!(
            proto.limit_strategy,
            Some(ConcurrencyLimitStrategyProto::GroupRoundRobin as i32)
        );
    }

    #[test]
    fn unshared_concurrency_expression_leaves_the_shared_fields_unset() {
        let proto =
            ConcurrencyExpression::new("input.group", 1, ConcurrencyLimitStrategy::CancelNewest)
                .to_proto();

        assert_eq!(proto.name, None);
        assert_eq!(proto.is_tenant_scoped, None);
    }

    #[test]
    fn shared_concurrency_expression_to_proto() {
        let proto = ConcurrencyExpression::shared(
            "render-page",
            "'all'",
            2,
            ConcurrencyLimitStrategy::GroupRoundRobin,
        )
        .to_proto();

        assert_eq!(
            proto,
            ConcurrencyProto {
                expression: String::from("'all'"),
                max_runs: Some(2),
                limit_strategy: Some(ConcurrencyLimitStrategyProto::GroupRoundRobin as i32),
                name: Some(String::from("render-page")),
                is_tenant_scoped: Some(true),
            }
        );
    }

    #[test]
    fn static_rate_limit_with_fixed_units_to_proto() {
        let proto = RateLimit::Static {
            key: String::from("api"),
            units: 2.into(),
        }
        .to_proto();

        assert_eq!(
            proto,
            CreateTaskRateLimit {
                key: String::from("api"),
                units: Some(2),
                key_expr: None,
                units_expr: None,
                limit_values_expr: None,
                duration: None,
            }
        );
    }

    #[test]
    fn static_rate_limit_with_units_expression_to_proto() {
        let proto = RateLimit::Static {
            key: String::from("api"),
            units: RateLimitValue::Expression(String::from("input.weight")),
        }
        .to_proto();

        assert_eq!(proto.units, None);
        assert_eq!(proto.units_expr.as_deref(), Some("input.weight"));
    }

    #[test]
    fn dynamic_rate_limit_to_proto() {
        let proto = RateLimit::Dynamic {
            key_expression: String::from("input.user_id"),
            units: 1.into(),
            limit: 10.into(),
            duration: RateLimitDuration::Hour,
        }
        .to_proto();

        assert_eq!(
            proto,
            CreateTaskRateLimit {
                key: String::from("input.user_id"),
                units: Some(1),
                key_expr: Some(String::from("input.user_id")),
                units_expr: None,
                limit_values_expr: Some(String::from("10")),
                duration: Some(RateLimitDurationProto::Hour as i32),
            }
        );
    }

    #[test]
    fn dynamic_rate_limit_with_limit_expression_to_proto() {
        let proto = RateLimit::Dynamic {
            key_expression: String::from("input.user_id"),
            units: 1.into(),
            limit: RateLimitValue::Expression(String::from("input.tier_limit")),
            duration: RateLimitDuration::Day,
        }
        .to_proto();

        assert_eq!(proto.limit_values_expr.as_deref(), Some("input.tier_limit"));
    }
}
