//! Forward [`tracing`] events emitted inside a task handler to the Hatchet dashboard.
//!
//! Requires the `tracing` feature.
//!
//! [`HatchetLayer`] is a [`tracing_subscriber::Layer`] that picks up every event recorded
//! while a task handler is running — however deep in the call stack it was emitted, and
//! without the emitting code ever seeing a [`Context`](crate::Context) — and sends it to
//! the task run it belongs to. Events emitted outside a task run are ignored, so a
//! worker's own start-up logging never reaches the dashboard.
//!
//! ```no_run
//! use hatchet_sdk::{Hatchet, HatchetLayer};
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let hatchet = Hatchet::from_env().await?;
//!
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(HatchetLayer::new(&hatchet))
//!     .init();
//! # Ok(())
//! # }
//! ```
//!
//! # Scope
//!
//! The current task run is tracked with a Tokio task-local, which is **not** inherited by
//! a bare [`tokio::spawn`]. An event emitted from a freshly spawned task is therefore
//! dropped rather than misattributed. To log from a spawned sub-task, hand the work to
//! [`Context::log`](crate::Context::log) instead, or capture the values you need and emit
//! the event from the handler's own future.

use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::{EXECUTION_CONTEXT, Hatchet};

/// How many log lines may be waiting for delivery before further lines are dropped.
///
/// Bounded rather than unbounded so a handler logging in a tight loop cannot grow memory
/// without limit. The engine caps a task run at 1000 log lines anyway.
const SINK_CAPACITY: usize = 1000;

/// Targets whose events are dropped unless the user overrides the list.
///
/// A `tracing` registry sees every span and event in the process, including the very
/// gRPC machinery used to deliver these log lines. Without this filter a single task run
/// would blow through the engine's per-run line limit before the handler did any work.
const DEFAULT_IGNORED_TARGETS: &[&str] = &[
    "hatchet_sdk",
    "h2",
    "hyper",
    "hyper_util",
    "tonic",
    "tower",
    "rustls",
    "reqwest",
];

/// A single log line on its way to the Hatchet API.
#[derive(Debug)]
struct LogLine {
    task_run_external_id: String,
    message: String,
    level: &'static str,
    metadata: String,
    retry_count: i32,
}

/// What the background pump consumes.
#[derive(Debug)]
enum SinkMessage {
    Line(Box<LogLine>),
    /// Delivered in order behind any queued lines, so completing the receive proves
    /// everything ahead of it has been sent.
    Flush(oneshot::Sender<()>),
}

/// The process-wide sink.
///
/// [`Layer::on_event`] is a synchronous `&self` callback with no way to reach a value the
/// caller holds, so the sender has to be reachable from a static. Keeping it here also
/// lets the task dispatcher flush the queue before reporting a task complete, without
/// threading a handle through the worker.
static SINK: OnceLock<mpsc::Sender<SinkMessage>> = OnceLock::new();

/// Lines discarded because the sink was full, reported alongside the next line that fits.
static DROPPED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Guards against a log-to-tracing bridge turning our own failure warnings back into
    /// events, which would recurse until the stack ran out.
    static IN_ON_EVENT: Cell<bool> = const { Cell::new(false) };
}

/// Start the background pump, unless one is already running.
fn init_sink(client: &Hatchet) {
    SINK.get_or_init(|| {
        let (tx, mut rx) = mpsc::channel::<SinkMessage>(SINK_CAPACITY);
        let mut client = client.clone();

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
                    SinkMessage::Line(line) => {
                        // Sent serially, matching `Context::log`, so lines arrive in the
                        // order they were emitted.
                        if let Err(error) = client
                            .event_client
                            .put_log(
                                &line.task_run_external_id,
                                line.message,
                                Some(line.level.to_string()),
                                line.metadata,
                                Some(line.retry_count),
                            )
                            .await
                        {
                            log::warn!("failed to send log to hatchet: {error}");
                        }
                    }
                    SinkMessage::Flush(responder) => {
                        let _ = responder.send(());
                    }
                }
            }
        });

        tx
    });
}

/// Wait for every queued log line to be delivered, giving up after `timeout`.
///
/// A no-op when no [`HatchetLayer`] has been constructed. Called by the task dispatcher
/// before it reports a task complete, so a line emitted on the handler's last statement is
/// not lost when the run ends.
pub(crate) async fn flush(timeout: Duration) {
    let Some(sink) = SINK.get() else {
        return;
    };

    let (tx, rx) = oneshot::channel();
    if sink.send(SinkMessage::Flush(tx)).await.is_err() {
        return;
    }

    // A wedged connection must not hold up the task's completion event indefinitely.
    let _ = tokio::time::timeout(timeout, rx).await;
}

/// Map a `tracing` level onto one of the four levels the Hatchet server recognises.
///
/// `TRACE` has no counterpart on the server and is reported as `DEBUG`.
fn level_name(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

/// Collects an event's fields, separating the message from everything else.
///
/// `tracing` records the format string of `info!("hello {x}")` as a reserved field named
/// `message`; the remaining fields are the structured payload and become log metadata.
#[derive(Default)]
struct FieldCollector {
    message: Option<String>,
    fields: serde_json::Map<String, serde_json::Value>,
}

impl FieldCollector {
    fn insert(&mut self, field: &Field, value: serde_json::Value) {
        if field.name() == "message" {
            // A message recorded as anything but a string is unusual, but stringifying it
            // is still more useful than hiding it in the metadata.
            self.message = Some(match value {
                serde_json::Value::String(message) => message,
                other => other.to_string(),
            });
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.insert(field, serde_json::Value::String(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, serde_json::Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }
}

/// The fields recorded on a span, stashed so an event nested inside it can inherit them.
#[derive(Default, Debug)]
struct SpanFields(serde_json::Map<String, serde_json::Value>);

/// A [`tracing_subscriber::Layer`] that sends events to the Hatchet dashboard.
///
/// See the [module documentation](self) for the full picture.
pub struct HatchetLayer {
    max_level: Level,
    ignored_targets: Vec<String>,
}

impl std::fmt::Debug for HatchetLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HatchetLayer")
            .field("max_level", &self.max_level)
            .field("ignored_targets", &self.ignored_targets)
            .finish()
    }
}

impl HatchetLayer {
    /// Build a layer that forwards events through `client`.
    ///
    /// Must be called from within a Tokio runtime: it starts the background task that
    /// delivers log lines. Building a second layer reuses the first one's background task
    /// rather than starting another.
    pub fn new(client: &Hatchet) -> Self {
        init_sink(client);

        Self {
            max_level: Level::INFO,
            ignored_targets: DEFAULT_IGNORED_TARGETS
                .iter()
                .map(|target| (*target).to_string())
                .collect(),
        }
    }

    /// Forward only events at or above `level`. Defaults to [`Level::INFO`].
    ///
    /// This filters what reaches Hatchet, and is independent of the level filtering
    /// applied to the rest of your subscriber stack.
    pub fn with_max_level(mut self, level: Level) -> Self {
        self.max_level = level;
        self
    }

    /// Drop events whose target starts with any of `targets`, replacing the default list.
    ///
    /// The default drops this SDK's own events along with the networking crates it is
    /// built on (`h2`, `hyper`, `tonic`, `tower`, `rustls`, `reqwest`). Pass an empty
    /// vector to forward everything.
    pub fn with_ignored_targets(mut self, targets: Vec<String>) -> Self {
        self.ignored_targets = targets;
        self
    }

    fn is_ignored(&self, target: &str) -> bool {
        self.ignored_targets
            .iter()
            .any(|ignored| target.starts_with(ignored.as_str()))
    }
}

impl<S> Layer<S> for HatchetLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: LayerContext<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut collector = FieldCollector::default();
        attrs.record(&mut collector);
        span.extensions_mut().insert(SpanFields(collector.fields));
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: LayerContext<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut collector = FieldCollector::default();
        values.record(&mut collector);

        let mut extensions = span.extensions_mut();
        if let Some(existing) = extensions.get_mut::<SpanFields>() {
            existing.0.extend(collector.fields);
        } else {
            extensions.insert(SpanFields(collector.fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        // Anything below runs on the thread that emitted the event, so it must neither
        // block nor await.
        let Some(_guard) = ReentrancyGuard::acquire() else {
            return;
        };
        self.forward(event, ctx);
    }
}

/// Marks the current thread as being inside `on_event`, and unmarks it on drop.
///
/// Released via `Drop` rather than a plain assignment so a panic while forwarding cannot
/// leave the thread permanently marked, silently disabling logging on it.
struct ReentrancyGuard;

impl ReentrancyGuard {
    /// Returns `None` when this thread is already inside `on_event`.
    fn acquire() -> Option<Self> {
        if IN_ON_EVENT.with(|guard| guard.replace(true)) {
            None
        } else {
            Some(Self)
        }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        IN_ON_EVENT.with(|guard| guard.set(false));
    }
}

impl HatchetLayer {
    fn forward<S>(&self, event: &Event<'_>, ctx: LayerContext<'_, S>)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let metadata = event.metadata();

        if *metadata.level() > self.max_level || self.is_ignored(metadata.target()) {
            return;
        }

        // Outside a task run there is nothing to attribute the event to, so drop it.
        let Ok((task_run_external_id, retry_count)) = EXECUTION_CONTEXT.try_with(|execution| {
            let execution = execution.borrow();
            (
                execution.task_run_external_id.clone(),
                execution.retry_count,
            )
        }) else {
            return;
        };

        let Some(sink) = SINK.get() else {
            return;
        };

        let mut collector = FieldCollector::default();
        event.record(&mut collector);

        // Fold in the enclosing spans, outermost first, so a nested span's field wins over
        // an outer one of the same name.
        let mut fields = serde_json::Map::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(span_fields) = span.extensions().get::<SpanFields>() {
                    fields.extend(span_fields.0.clone());
                }
            }
        }
        fields.extend(collector.fields);

        fields.insert(
            String::from("target"),
            serde_json::Value::String(metadata.target().to_string()),
        );

        let dropped = DROPPED.load(Ordering::Relaxed);
        if dropped > 0 {
            fields.insert(
                String::from("hatchet_dropped_log_lines"),
                serde_json::Value::from(dropped),
            );
        }

        let line = LogLine {
            task_run_external_id,
            message: collector.message.unwrap_or_default(),
            level: level_name(metadata.level()),
            metadata: serde_json::Value::Object(fields).to_string(),
            retry_count,
        };

        // `try_send` rather than `send`: `on_event` cannot await, and stalling the handler
        // to deliver a log line would be worse than losing the line.
        if sink.try_send(SinkMessage::Line(Box::new(line))).is_err() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        } else {
            // Only clear the count once a line carrying it has been accepted.
            DROPPED.fetch_sub(dropped, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};

    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::ExecutionContext;

    fn execution_context() -> RefCell<ExecutionContext> {
        RefCell::new(ExecutionContext {
            workflow_run_id: String::from("workflow-run-1"),
            task_run_external_id: String::from("task-run-1"),
            child_index: 0,
            retry_count: 2,
        })
    }

    /// A layer sharing `HatchetLayer`'s filtering and field-collection logic, but writing
    /// the lines it accepts into a vector instead of a gRPC channel. This lets the tests
    /// exercise the real `tracing` plumbing without a Hatchet server.
    struct CapturingLayer {
        inner: HatchetLayer,
        lines: Arc<Mutex<Vec<CapturedLine>>>,
    }

    #[derive(Debug, Clone)]
    struct CapturedLine {
        task_run_external_id: String,
        message: String,
        level: &'static str,
        metadata: serde_json::Value,
        retry_count: i32,
    }

    impl<S> Layer<S> for CapturingLayer
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            ctx: LayerContext<'_, S>,
        ) {
            Layer::<S>::on_new_span(&self.inner, attrs, id, ctx);
        }

        fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
            let metadata = event.metadata();

            if *metadata.level() > self.inner.max_level || self.inner.is_ignored(metadata.target())
            {
                return;
            }

            let Ok((task_run_external_id, retry_count)) = EXECUTION_CONTEXT.try_with(|execution| {
                let execution = execution.borrow();
                (
                    execution.task_run_external_id.clone(),
                    execution.retry_count,
                )
            }) else {
                return;
            };

            let mut collector = FieldCollector::default();
            event.record(&mut collector);

            let mut fields = serde_json::Map::new();
            if let Some(scope) = ctx.event_scope(event) {
                for span in scope.from_root() {
                    if let Some(span_fields) = span.extensions().get::<SpanFields>() {
                        fields.extend(span_fields.0.clone());
                    }
                }
            }
            fields.extend(collector.fields);

            self.lines.lock().unwrap().push(CapturedLine {
                task_run_external_id,
                message: collector.message.unwrap_or_default(),
                level: level_name(metadata.level()),
                metadata: serde_json::Value::Object(fields),
                retry_count,
            });
        }
    }

    /// Run `body` under a capturing subscriber and return the lines it accepted.
    fn capture(layer: HatchetLayer, body: impl FnOnce()) -> Vec<CapturedLine> {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CapturingLayer {
            inner: layer,
            lines: lines.clone(),
        });

        tracing::subscriber::with_default(subscriber, body);

        let captured = lines.lock().unwrap();
        captured.clone()
    }

    /// A layer that does not touch the global sink, so tests need no runtime or client.
    ///
    /// The ignore list is empty because these tests emit from inside `hatchet_sdk`, which
    /// the default list deliberately drops. [`test_default_ignored_targets`] covers the
    /// real default.
    fn test_layer() -> HatchetLayer {
        HatchetLayer {
            max_level: Level::INFO,
            ignored_targets: Vec::new(),
        }
    }

    /// The shipped default, for the tests that exercise target filtering itself.
    fn default_layer() -> HatchetLayer {
        HatchetLayer {
            max_level: Level::INFO,
            ignored_targets: DEFAULT_IGNORED_TARGETS
                .iter()
                .map(|target| (*target).to_string())
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_event_fields_become_message_and_metadata() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer(), || {
                    tracing::info!(user_id = 7, retries = "none", "hello");
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("hello", lines[0].message);
        assert_eq!(serde_json::json!(7), lines[0].metadata["user_id"]);
        assert_eq!(serde_json::json!("none"), lines[0].metadata["retries"]);
        // The message is the line itself, not a metadata key.
        assert!(lines[0].metadata.get("message").is_none());
    }

    #[tokio::test]
    async fn test_span_fields_are_folded_into_metadata() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer(), || {
                    let span = tracing::info_span!("request", request_id = "abc");
                    let _guard = span.enter();
                    tracing::info!("handling");
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!(serde_json::json!("abc"), lines[0].metadata["request_id"]);
    }

    #[tokio::test]
    async fn test_task_run_id_and_retry_count_are_attached() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer(), || tracing::info!("hello"))
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("task-run-1", lines[0].task_run_external_id);
        assert_eq!(2, lines[0].retry_count);
    }

    #[test]
    fn test_events_outside_a_task_run_are_dropped() {
        // No `EXECUTION_CONTEXT` scope: this is what a worker's own start-up logging looks
        // like, and none of it belongs on the dashboard.
        let lines = capture(test_layer(), || tracing::error!("not part of a task"));

        assert!(lines.is_empty());
    }

    #[tokio::test]
    async fn test_max_level_filters_events() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer(), || {
                    tracing::debug!("too quiet");
                    tracing::info!("loud enough");
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("loud enough", lines[0].message);
    }

    #[tokio::test]
    async fn test_lowering_max_level_admits_debug_events() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer().with_max_level(Level::DEBUG), || {
                    tracing::debug!("now visible")
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("DEBUG", lines[0].level);
    }

    #[tokio::test]
    async fn test_ignored_targets_are_dropped() {
        let layer = test_layer().with_ignored_targets(vec![String::from("noisy_crate")]);

        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(layer, || {
                    tracing::info!(target: "noisy_crate::inner", "dropped");
                    tracing::info!(target: "my_app", "kept");
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("kept", lines[0].message);
    }

    #[tokio::test]
    async fn test_all_server_levels_are_reachable() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(test_layer().with_max_level(Level::TRACE), || {
                    tracing::error!("e");
                    tracing::warn!("w");
                    tracing::info!("i");
                    tracing::debug!("d");
                    tracing::trace!("t");
                })
            })
            .await;

        let levels: Vec<&str> = lines.iter().map(|line| line.level).collect();
        // `TRACE` has no server counterpart and folds into `DEBUG`.
        assert_eq!(vec!["ERROR", "WARN", "INFO", "DEBUG", "DEBUG"], levels);
    }

    #[tokio::test]
    async fn test_default_ignored_targets() {
        let lines = EXECUTION_CONTEXT
            .scope(execution_context(), async {
                capture(default_layer(), || {
                    // The SDK's own events, and those of the networking stack delivering
                    // them, must never reach the dashboard.
                    tracing::info!(target: "hatchet_sdk::worker", "internal");
                    tracing::info!(target: "hyper::client", "internal");
                    tracing::info!(target: "tonic::transport", "internal");
                    tracing::info!(target: "my_app::orders", "kept");
                })
            })
            .await;

        assert_eq!(1, lines.len());
        assert_eq!("kept", lines[0].message);
    }

    #[test]
    fn test_level_names_match_the_server_enum() {
        assert_eq!("ERROR", level_name(&Level::ERROR));
        assert_eq!("WARN", level_name(&Level::WARN));
        assert_eq!("INFO", level_name(&Level::INFO));
        assert_eq!("DEBUG", level_name(&Level::DEBUG));
        assert_eq!("DEBUG", level_name(&Level::TRACE));
    }

    #[tokio::test]
    async fn test_events_from_a_spawned_task_are_dropped() {
        // The task-local does not cross `tokio::spawn`. Pinning the behaviour here so the
        // boundary is a documented decision rather than a surprise.
        let lines = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CapturingLayer {
            inner: test_layer(),
            lines: lines.clone(),
        });
        let _default = tracing::subscriber::set_default(subscriber);

        EXECUTION_CONTEXT
            .scope(execution_context(), async {
                tokio::spawn(async { tracing::info!("from a spawned task") })
                    .await
                    .unwrap();
            })
            .await;

        assert!(lines.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_flush_without_a_sink_returns_immediately() {
        // Nothing has built a layer, so there is no pump to drain.
        flush(Duration::from_secs(1)).await;
    }
}
