//! Binary entry point for tele-s3.
use std::{env, path::Path, sync::Arc, time::Duration};

use clap::Parser;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use sea_orm::Database;
use tokio::{net::TcpListener, signal, sync::Semaphore};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};

use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt::Subscriber as FmtSubscriber, layer::SubscriberExt,
    util::SubscriberInitExt,
};

use tele_s3::{
    admin::{
        AdminRoute, AdminState, ORPHAN_BLOB_RECONCILER_ERROR_TYPE,
        PUBLISH_INTENT_RECONCILER_ERROR_TYPE, ReconcilerOutcome, StartupReport,
    },
    backend::{Grammers, GrammersConfig, GrammersLimits},
    s3::TeleS3,
};

mod config;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    config: String,
}

/// Outbound Telegram bounds, overridable from the environment so an operator
/// can tune them without a rebuild. Unset or unparsable values keep the
/// built-in default.
///
/// `TELEGRAM_MAX_CONCURRENT_REQUESTS`, `TELEGRAM_IO_TIMEOUT_SECS` and
/// `TELEGRAM_MAX_ATTEMPTS` are read from the environment rather than the
/// config file, so an operator can set them on a container without touching
/// `config.toml`. They are read once, here, before the backend is built.
/// What they bound, and their defaults, are documented in the README's
/// configuration reference; the defaults themselves live in
/// [`GrammersLimits::default`].
///
/// Out-of-range values are not rejected here — they are clamped later, by
/// [`GrammersLimits::sanitized`] — so an absurd setting degrades instead of
/// wedging the backend at start-up.
fn telegram_limits() -> GrammersLimits {
    let defaults = GrammersLimits::default();

    fn env_or<T: std::str::FromStr>(name: &str, fallback: T) -> T {
        match env::var(name) {
            Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(
                    env_var = name,
                    value = %raw,
                    error.type = "unparsable_env_override",
                    "ignoring unparsable environment override"
                );
                fallback
            }),
            Err(_) => fallback,
        }
    }

    let max_concurrent_requests = env_or(
        "TELEGRAM_MAX_CONCURRENT_REQUESTS",
        defaults.max_concurrent_requests,
    );
    let io_timeout = Duration::from_secs(env_or(
        "TELEGRAM_IO_TIMEOUT_SECS",
        defaults.io_timeout.as_secs(),
    ));
    let max_attempts = env_or("TELEGRAM_MAX_ATTEMPTS", defaults.max_attempts);

    GrammersLimits {
        max_concurrent_requests,
        io_timeout,
        max_attempts,
    }
}

/// Line-oriented output format for the process log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    /// `tracing-subscriber`'s human-readable renderer. The default, and the
    /// only format that has always been available.
    Text,
    /// One self-describing JSON object per event, for ingestion.
    Json,
}

impl LogFormat {
    /// Parse a format name, case- and whitespace-insensitively. `None` for a
    /// name this build does not have.
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// Read [`LOG_FORMAT_ENV`]. An unset variable, or one naming a format
    /// this build does not have, keeps [`LogFormat::Text`], so a typo
    /// degrades to the default rather than to a process that logs nothing.
    fn from_env() -> Self {
        let Ok(raw) = env::var(LOG_FORMAT_ENV) else {
            return Self::Text;
        };

        if let Some(parsed) = Self::parse(&raw) { parsed } else {
            tracing::warn!(
                env_var = LOG_FORMAT_ENV,
                value = %raw,
                error.type = "unparsable_env_override",
                "unknown log format requested; using the default text format"
            );
            Self::Text
        }
    }
}

/// Selects the process-wide log format. See [`LOG_FORMAT_ENV`].
const LOG_FORMAT_ENV: &str = "TELE_S3_LOG_FORMAT";

/// Name this process reports to the collector, as `service.name`.
const SERVICE_NAME: &str = "tele-s3";

/// OTLP endpoint traces are exported to. Unset means trace export is off.
///
/// Follows the same environment-variable convention as the log format and the
/// Telegram limits: read once, here, before anything is built, so an operator
/// can set it on a container without touching `config.toml`.
const OTLP_ENDPOINT_ENV: &str = "TELE_S3_OTLP_ENDPOINT";

/// The standard OpenTelemetry endpoint variable, honoured as a fallback.
///
/// An operator who already points other services at a collector does not have
/// to learn this crate's spelling. [`OTLP_ENDPOINT_ENV`] takes precedence, so
/// the crate's own variable works standalone.
const OTEL_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Fraction of traces to export, `0.0` to `1.0`. Unset means every trace.
const OTLP_SAMPLE_RATIO_ENV: &str = "TELE_S3_OTLP_SAMPLE_RATIO";

/// The only transport built into this binary.
///
/// `opentelemetry-otlp` is compiled without its gRPC client, so there is no
/// code path that could speak gRPC even if a collector asked for it. The name
/// is published at start-up so an operator can see which one they got.
const TRACE_TRANSPORT: &str = "otlp_http_protobuf";

/// Every trace is exported unless the operator says otherwise.
const DEFAULT_SAMPLE_RATIO: f64 = 1.0;

/// How often the meter provider pushes a collection to the collector.
///
/// Sixty seconds, the SDK's own default, stated here so the number is
/// visible next to the interval operator reads in the exporter's own
/// documentation.
const METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// `error.type` for a telemetry setting that could not be read.
const TELEMETRY_CONFIG_ERROR_TYPE: &str = "unparsable_telemetry_config";

/// `error.type` for spans that could not be flushed to the collector.
const TELEMETRY_EXPORT_ERROR_TYPE: &str = "telemetry_export";

/// Where and how much to export, read once from the environment.
///
/// `None` — no endpoint configured — is the default and by far the common
/// case. It is genuinely inert rather than merely quiet: with no endpoint
/// nothing here builds an exporter, no batching thread is started, and the
/// subscriber is the log-only one that shipped before this scope existed. A
/// self-hosted operator who never configures telemetry pays nothing for the
/// machinery.
#[derive(Debug, Clone, PartialEq)]
struct TraceExport {
    /// OTLP collector endpoint, HTTP scheme, protobuf-encoded payloads.
    endpoint: String,
    /// Fraction of traces to keep.
    sample_ratio: f64,
}

impl TraceExport {
    /// Read the settings, or `None` when no endpoint is configured.
    fn from_env() -> Option<Self> {
        let endpoint = Self::endpoint_from(
            env::var(OTLP_ENDPOINT_ENV).ok().as_deref(),
            env::var(OTEL_ENDPOINT_ENV).ok().as_deref(),
        )?;

        Some(Self {
            endpoint,
            sample_ratio: Self::sample_ratio_from_env(),
        })
    }

    /// The first usable endpoint among the crate's own variable, then the
    /// standard one. An unset *or blank* value is not a configuration: it
    /// falls through rather than producing an endpoint of `""`.
    ///
    /// Pure, so the precedence rule can be tested without mutating the process
    /// environment, which every test in a binary shares.
    fn endpoint_from(own: Option<&str>, standard: Option<&str>) -> Option<String> {
        [own, standard]
            .into_iter()
            .flatten()
            .map(str::trim)
            .find(|raw| !raw.is_empty())
            .map(str::to_string)
    }

    /// A usable sample ratio, or `None` if `raw` is not one.
    ///
    /// Out-of-range and non-finite values are rejected here rather than left
    /// to the sampler's own clamping, so an absurd setting degrades to a
    /// logged default instead of quietly meaning "export nothing".
    fn sample_ratio(raw: &str) -> Option<f64> {
        let parsed = raw.trim().parse::<f64>().ok()?;

        parsed
            .is_finite()
            .then_some(parsed)
            .filter(|ratio| (0.0..=1.0).contains(ratio))
    }

    /// The configured ratio, or [`DEFAULT_SAMPLE_RATIO`] with a reason logged.
    fn sample_ratio_from_env() -> f64 {
        let Ok(raw) = env::var(OTLP_SAMPLE_RATIO_ENV) else {
            return DEFAULT_SAMPLE_RATIO;
        };

        Self::sample_ratio(&raw).unwrap_or_else(|| {
            tracing::warn!(
                env_var = OTLP_SAMPLE_RATIO_ENV,
                value = %raw,
                error.type = TELEMETRY_CONFIG_ERROR_TYPE,
                "unparsable trace sample ratio; exporting every trace"
            );
            DEFAULT_SAMPLE_RATIO
        })
    }

    /// The resource every exported span carries.
    ///
    /// The version is the crate version, so it cannot drift from the binary
    /// that produced the span. Built empty rather than from the SDK's
    /// detectors: this document is the identity of *this* process, and an
    /// operator diffing two of them should not be reading SDK filler.
    fn resource(&self) -> Resource {
        Resource::builder_empty()
            .with_service_name(SERVICE_NAME)
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build()
    }

    /// Build the batching pipeline, or `None` if the exporter refuses.
    ///
    /// An endpoint the exporter will not accept is a configuration error, not
    /// a reason to refuse to serve S3. The caller logs it and continues with
    /// the log-only subscriber.
    fn build_pipeline(&self) -> Option<SdkTracerProvider> {
        let exporter = SpanExporter::builder()
            .with_http()
            .with_endpoint(self.endpoint.clone())
            .build()
            .inspect_err(|err| {
                tracing::warn!(
                    telemetry_endpoint = %self.endpoint,
                    error.type = TELEMETRY_CONFIG_ERROR_TYPE,
                    error.message = %err,
                    "cannot build the OTLP exporter; continuing without trace export"
                );
            })
            .ok()?;

        // `ParentBased` so the ratio applies to whole traces and a child span
        // never joins a trace its parent was excluded from.
        let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.sample_ratio)));

        Some(
            SdkTracerProvider::builder()
                .with_resource(self.resource())
                .with_sampler(sampler)
                // Batching: a span costs a channel send rather than a round
                // trip, and the exporter's own worker thread does the network
                // work off the request path.
                .with_batch_exporter(exporter)
                .build(),
        )
    }

    /// Build the metric pipeline, or `None` if the exporter refuses.
    ///
    /// Same rule as [`TraceExport::build_pipeline`] and for the same reason:
    /// an endpoint the exporter will not accept is a configuration error, not
    /// a reason to refuse to serve S3. Metrics share the trace export
    /// decision — one endpoint setting turns both on — so an operator never
    /// has to work out which half of the pipeline is live.
    fn build_meter_pipeline(&self) -> Option<SdkMeterProvider> {
        let exporter = MetricExporter::builder()
            .with_http()
            .with_endpoint(self.endpoint.clone())
            .build()
            .inspect_err(|err| {
                tracing::warn!(
                    telemetry_endpoint = %self.endpoint,
                    error.type = TELEMETRY_CONFIG_ERROR_TYPE,
                    error.message = %err,
                    "cannot build the OTLP metric exporter; continuing without metric export"
                );
            })
            .ok()?;

        // The reader wraps the exporter: it owns the interval and the worker
        // thread the exporter itself does not have.
        let reader = PeriodicReader::builder(exporter)
            .with_interval(METRIC_EXPORT_INTERVAL)
            .build();

        Some(
            SdkMeterProvider::builder()
                .with_resource(self.resource())
                // Periodic, like the trace pipeline: an operator tuning rate
                // limits wants a distribution over minutes, not a synchronous
                // HTTP round trip on the request path.
                .with_reader(reader)
                .build(),
        )
    }
}

/// Install the process-wide `tracing` subscriber.
///
/// Returns the trace provider when export is on, which the caller keeps and
/// flushes on the graceful-exit path. `None` means export is off — either no
/// endpoint was configured or the exporter refused the one that was — and the
/// subscriber installed is then byte for byte the one that shipped before
/// trace export existed.
///
/// An OTLP failure is never fatal. The exporter is built before the log layer
/// so the reason for skipping it reaches the log; once the pipeline is up,
/// export happens on its own background thread and a collector that is down,
/// slow or gone cannot fail a request or panic the process.
///
/// # Field vocabulary
///
/// Every event this crate emits carries its data as named fields rather than
/// prose, so the same event can be counted, filtered and aggregated by a log
/// pipeline. The names are the crate's contract: a downstream exporter binds
/// to these strings, so they are appended to, never repurposed. One name
/// means one thing in every file.
///
/// ## Identity and location
///
/// | Field | Meaning |
/// | --- | --- |
/// | `bucket` | S3 bucket name. |
/// | `key` | S3 object key. |
/// | `blob_id` | Metadata-store id of a backend blob. |
/// | `publish_id` | Durable publish intent id; names an upload that may exist in the chat. |
/// | `message_id` | Telegram message id holding the object. |
/// | `file_id` | Telegram file id of an uploaded part set. |
/// | `name` | Telegram part/file name of an upload. |
/// | `dc_id` | Telegram datacentre id. |
/// | `endpoint` | Which admin endpoint answered a request. |
/// | `telemetry_endpoint` | OTLP collector this process exports traces to. |
/// | `transport` | Wire protocol a telemetry export speaks. |
///
/// ## Sizes, counts and durations
///
/// | Field | Meaning |
/// | --- | --- |
/// | `bytes` | A payload size. |
/// | `size` | A declared object size. |
/// | `declared_bytes` | The size the caller declared, when it disagrees with the body. |
/// | `chunk_index` | Index into the memory backend's chunk table. |
/// | `part_index` | Zero-based index of a part within one multipart upload. |
/// | `attempts` | Attempts a retry loop has made, the first included. |
/// | `reconciled` / `cleared` / `retained` / `candidates` | Row counts from a reconciliation pass. |
/// | `cap` | The bound a reconciliation pass refused to exceed. |
/// | `duration_ms` | Milliseconds waited on a Telegram rate limit. |
/// | `timeout_ms` | Milliseconds an operation was allowed before its deadline. |
/// | `port` | The listening TCP port. |
/// | `status_code` | HTTP status an admin endpoint answered with. |
///
/// ## Telegram tuning, echoed at start-up
///
/// | Field | Meaning |
/// | --- | --- |
/// | `max_concurrent_requests` | Permits held process-wide by outbound Telegram requests. |
/// | `io_timeout_ms` | Deadline for a single Telegram request. |
/// | `max_attempts` | Attempt budget per retry loop. |
///
/// ## Errors and outcomes
///
/// | Field | Meaning |
/// | --- | --- |
/// | `error.type` | Stable, machine-readable classification of the failure. Never changes meaning. |
/// | `error.message` | Human-readable rendering of the error. |
/// | `operation` | The Telegram RPC or S3 operation a failure belongs to. Never the Telegram error name. |
/// | `telegram_error` | Telegram's own name for a rate limit: `FLOOD_WAIT_X`, `FLOOD_PREMIUM_WAIT_X`, .... |
/// | `rpc_error_code` | The integer RPC error code Telegram replied with (`420` for every rate limit). |
/// | `peer` | Telegram dialog id the request was addressed to. |
/// | `outcome` | Classification of a state a message may or may not be in. |
/// | `lock` | Name of the poisoned mutex. |
/// | `env_var` / `value` | An environment override and the raw text it was set to. |
///
/// Spans are the other half of the vocabulary: `#[instrument]` puts the
/// function's arguments on the span, so `operation`-shaped fields such as
/// `key`, `blob_id` and `publish_id` also arrive as span fields. Promote a
/// field to the span rather than re-logging it if it is already there.
///
/// They are also the OTLP span attributes. The [`tracing-opentelemetry`]
/// layer binds to these same names, so a field is spelled once for both a log
/// line and a trace.
///
/// # Metric vocabulary
///
/// The same names are the attributes on the metrics below, so one query
/// joins a log line and a data point. The span `telegram.flood_wait` is the
/// one span added for this vocabulary: it exists so an individual
/// rate-limit wait is navigable with its own duration.
///
/// | Instrument | Kind | Unit | Attributes |
/// | --- | --- | --- | --- |
/// | `tele_s3.telegram.flood_limits` | counter | `{limit}` | `telegram_error`, `operation` |
/// | `tele_s3.telegram.flood_wait_duration` | histogram | `s` | `operation`, `peer` |
/// | `tele_s3.telegram.rpc_calls` | counter | `{call}` | `operation`, `outcome` |
/// | `tele_s3.telegram.rpc_duration` | histogram | `s` | `operation`, `outcome` |
///
/// `outcome` is a closed set on the RPC instruments: `ok`, `rpc_error`,
/// `timeout`. All four are named with the `tele_s3.` prefix, so they cannot
/// collide with another library's metrics in a shared collector.
///
/// Metrics and traces are one decision: no endpoint means no meter provider,
/// no periodic reader and no background thread, exactly as it means no
/// tracer provider. Everything the crate records goes through the
/// process-wide meter, so a later scope adds instruments without a second
/// configuration surface.
fn init_tracing() -> Telemetry {
    let format = LogFormat::from_env();

    let Some(settings) = TraceExport::from_env() else {
        init_log_subscriber(format);
        return Telemetry::default();
    };

    // The metric half is built from the same settings, so there is no
    // configuration in which traces are leaving the process and metrics are
    // not, or the reverse.
    let metrics = settings.build_meter_pipeline();

    let Some(provider) = settings.build_pipeline() else {
        init_log_subscriber(format);
        return Telemetry {
            metrics,
            ..Default::default()
        };
    };

    tracing::info!(
        telemetry_endpoint = %settings.endpoint,
        transport = TRACE_TRANSPORT,
        "exporting traces and metrics over OTLP"
    );

    init_otel_subscriber(format, provider.tracer(SERVICE_NAME));

    // Set globally rather than threaded through every constructor: the
    // backend resolves its instruments once at start-up, and the S3 and
    // reconciler layers a later scope adds will do the same.
    if let Some(metrics) = &metrics {
        opentelemetry::global::set_meter_provider(metrics.clone());
    }

    Telemetry {
        traces: Some(provider),
        metrics,
    }
}

/// The telemetry pipelines this process owns.
///
/// Either half is absent when telemetry is off, which is the default. An
/// absent provider is not a stub: nothing was constructed for it, so nothing
/// runs in the background and nothing is flushed on exit.
#[derive(Default)]
struct Telemetry {
    traces: Option<SdkTracerProvider>,
    metrics: Option<SdkMeterProvider>,
}

impl Telemetry {
    /// Whether spans are leaving this process.
    fn trace_export(&self) -> bool {
        self.traces.is_some()
    }

    /// Whether metrics are leaving this process.
    fn metrics_export(&self) -> bool {
        self.metrics.is_some()
    }

    /// Drain both pipelines and stop their workers.
    ///
    /// Both halves are attempted even if the first fails: draining the queue
    /// is what saves the data, and stopping the thread is what lets the
    /// process exit rather than wait on a socket that is never going to
    /// answer.
    fn flush(&self) {
        if let Some(traces) = &self.traces
            && let Err(err) = traces.force_flush().and(traces.shutdown())
        {
            tracing::warn!(
                error.type = TELEMETRY_EXPORT_ERROR_TYPE,
                error.message = %err,
                "could not flush traces on shutdown"
            );
        }

        if let Some(metrics) = &self.metrics
            && let Err(err) = metrics.force_flush().and(metrics.shutdown())
        {
            tracing::warn!(
                error.type = TELEMETRY_EXPORT_ERROR_TYPE,
                error.message = %err,
                "could not flush metrics on shutdown"
            );
        }
    }
}

/// `RUST_LOG`, or the crate's own default when it is unset or unreadable.
///
/// Built per layer rather than once and shared: [`EnvFilter`] is not `Clone`,
/// and each layer needs its own copy.
fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("tele_s3=info"))
}

/// Install the log layer on its own.
///
/// This is the whole subscriber for a process with no trace export: the same
/// `fmt` renderer, the same environment filter, the same default of pretty
/// text.
fn init_log_subscriber(format: LogFormat) {
    match format {
        LogFormat::Text => FmtSubscriber::builder()
            .pretty()
            .with_env_filter(env_filter())
            .init(),
        LogFormat::Json => FmtSubscriber::builder()
            .json()
            .with_env_filter(env_filter())
            .init(),
    }
}

/// Install the log layer *and* the OTLP trace layer.
///
/// The log layer is `tracing-subscriber`'s own `fmt` renderer — the same one
/// [`init_log_subscriber`] installs, only expressed as a layer rather than as
/// a whole subscriber, because a trace layer can only be composed onto a
/// [`Registry`]. Both log formats therefore keep working exactly as before,
/// and the trace layer writes nothing to stdout: it observes spans on their
/// way to the collector.
///
/// Both layers are filtered by the same `RUST_LOG`. Filtering only the log
/// layer would let a `tele_s3=info` operator export the debug spans they
/// asked not to see.
fn init_otel_subscriber(format: LogFormat, tracer: SdkTracer) {
    let logs: Box<dyn Layer<Registry> + Send + Sync> = match format {
        LogFormat::Text => Box::new(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_filter(env_filter()),
        ),
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_filter(env_filter()),
        ),
    };

    Registry::default()
        .with(logs)
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(env_filter()),
        )
        .init();
}

/// The response sent to a connection that arrives when no permit is free.
///
/// `Content-Length: 0` keeps the response framing unambiguous.
/// `Connection: close` tells the client that this socket carries one response.
const SERVICE_UNAVAILABLE_RESPONSE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\n\
    Content-Length: 0\r\n\
    Connection: close\r\n\r\n";

/// Hold the permits that bound inbound S3 connections.
///
/// One connection holds one permit until the connection ends. When no permit
/// is free, the accept loop sends `503 Service Unavailable` and closes the
/// socket. It does not queue the connection, so a slow client cannot exhaust
/// the listener.
#[derive(Debug)]
struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
}

impl ConnectionLimiter {
    /// Create a limiter with `max` permits.
    ///
    /// The value is clamped to at least 1, because a limiter with no permits
    /// would refuse every connection.
    fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max.clamp(1, Semaphore::MAX_PERMITS))),
        }
    }

    /// Try to take one permit without waiting.
    ///
    /// `Err` means that no permit is free. The accept loop answers such a
    /// connection with `503 Service Unavailable`.
    fn try_acquire(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        self.semaphore.clone().try_acquire_owned()
    }
}

/// Environment variable that overrides `max_concurrent_connections`.
const CONNECTION_LIMIT_ENV: &str = "TELE_S3_MAX_CONCURRENT_CONNECTIONS";

/// Resolve the inbound connection limit.
///
/// Read [`CONNECTION_LIMIT_ENV`] when it is set. An unset or unparsable value
/// uses `config_value`, and a warning names the bad value. The result is
/// clamped to `1..=Semaphore::MAX_PERMITS`.
fn inbound_connection_limit(config_value: usize) -> usize {
    connection_limit_from(env::var(CONNECTION_LIMIT_ENV).ok().as_deref(), config_value)
}

/// Resolve a connection limit from a raw environment value.
///
/// Pure, so the parsing rule can be tested without changing the process
/// environment, which every test in the binary shares.
fn connection_limit_from(raw: Option<&str>, config_value: usize) -> usize {
    let fallback = config_value.clamp(1, Semaphore::MAX_PERMITS);
    let Some(raw) = raw else {
        return fallback;
    };

    match raw.trim().parse::<usize>() {
        Ok(parsed) => parsed.clamp(1, Semaphore::MAX_PERMITS),
        Err(_) => {
            tracing::warn!(
                env_var = CONNECTION_LIMIT_ENV,
                value = %raw,
                error.type = "unparsable_env_override",
                "ignoring unparsable environment override"
            );
            fallback
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = init_tracing();

    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);

        config::Config::try_from(config_path)?
    };

    let db = Database::connect(config.database_uri).await?;

    // Resolved once, here, so the admin endpoints publish the limits actually
    // in force rather than a second reading of the environment.
    let limits = telegram_limits();

    // Inbound S3 connections are bounded separately from Telegram I/O, so a
    // slow client cannot exhaust the listener.
    let max_concurrent_connections = inbound_connection_limit(config.max_concurrent_connections);

    let grammers = {
        let config = GrammersConfig {
            app_id: config.api_id,
            app_hash: config.api_hash,
            bot_token: config.bot_token,
            db: db.clone(),
            username: config.username,
            limits,
        };

        Grammers::init(config).await?
    };

    let s3_service = {
        let teles3 = TeleS3::init(grammers.clone(), db.clone()).await?;

        // Two bounded, non-fatal reconciliation passes before any traffic.
        // They are independent of each other and either may be skipped.
        //
        // Blob rows left behind by an interrupted publish are invisible to
        // every listing and nothing else reclaims them.
        let orphan_blobs = match teles3.reconcile_orphan_blobs().await {
            Ok(0) => ReconcilerOutcome::Completed { affected: 0 },
            Ok(reconciled) => {
                tracing::info!(reconciled, "reconciled orphaned blob rows");
                ReconcilerOutcome::Completed {
                    affected: reconciled,
                }
            }
            Err(err) => {
                tracing::warn!(
                    error.type = ORPHAN_BLOB_RECONCILER_ERROR_TYPE,
                    error.message = %err,
                    "blob reconciliation failed"
                );
                ReconcilerOutcome::Failed
            }
        };

        // Publishes that were never confirmed hold real bytes in the chat that
        // no key names, so no blob row points at them and the pass above
        // cannot see them.
        let publish_intents = match teles3.reconcile_publish_intents().await {
            Ok(0) => ReconcilerOutcome::Completed { affected: 0 },
            Ok(cleared) => {
                tracing::info!(cleared, "reconciled unconfirmed publish intents");
                ReconcilerOutcome::Completed { affected: cleared }
            }
            Err(err) => {
                tracing::warn!(
                    error.type = PUBLISH_INTENT_RECONCILER_ERROR_TYPE,
                    error.message = %err,
                    "publish intent reconciliation failed"
                );
                ReconcilerOutcome::Failed
            }
        };

        // What the admin endpoints publish; the messages above stay in the
        // log, where a reader already trusted to see them can find them.
        let startup = StartupReport {
            orphan_blobs,
            publish_intents,
        };

        let mut builder = S3ServiceBuilder::new(teles3);

        let auth = SimpleAuth::from_single(&config.auth_access_key, config.auth_secret_key);
        builder.set_auth(auth);

        // Health, readiness and introspection, answered ahead of S3 and ahead
        // of any signature check. See `admin` for what they publish and why.
        // The bool, not the endpoint: a probe should learn whether spans are
        // leaving this process, not where the collector lives.
        builder.set_route(AdminRoute::new(AdminState::new(
            config.listen_port,
            limits,
            startup,
            telemetry.trace_export(),
            telemetry.metrics_export(),
            max_concurrent_connections,
        )));

        builder.build()
    };

    let listener = TcpListener::bind(("0.0.0.0", config.listen_port)).await?;
    tracing::info!(port = config.listen_port, "listening for S3 connections");

    let connection_limiter = ConnectionLimiter::new(max_concurrent_connections);

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, _)) => {
                        match connection_limiter.try_acquire() {
                            Ok(permit) => {
                                let io = TokioIo::new(stream);
                                let svc = s3_service.clone();

                                tokio::spawn(async move {
                                    let _permit = permit;
                                    let builder = Builder::new(TokioExecutor::new());
                                    if let Err(err) = builder.serve_connection(io, svc).await {
                                        tracing::error!(
                                            error.type = "serve_connection",
                                            error.message = %err,
                                            "error serving connection"
                                        );
                                    }
                                });
                            }
                            Err(_) => {
                                // Bound reached: answer 503 and close. The
                                // write runs in its own task, so a client that
                                // does not read cannot stall the accept loop.
                                let mut stream = stream;
                                tokio::spawn(async move {
                                    use tokio::io::AsyncWriteExt;
                                    let _ = stream.write_all(SERVICE_UNAVAILABLE_RESPONSE).await;
                                    let _ = stream.shutdown().await;
                                });
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            error.type = "accept",
                            error.message = %err,
                            "accept error"
                        );
                    }
                }
            }
            _ = signal::ctrl_c() => {
                tracing::info!("shutdown signal received, starting graceful exit");
                break;
            }
        }
    }
    tracing::info!("shutting down services");

    // Before the closing log lines, not after: the spans those lines open are
    // the ones most likely to be sitting in the batch queue.
    telemetry.flush();

    db.close().await?;
    grammers.close();

    tracing::info!("exited gracefully");

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;

    use tele_s3::admin::ReconcilerOutcome;

    /// The introspection document is built from the port, the Telegram limits
    /// and two closed enums — no argument of [`AdminState::new`] can carry a
    /// credential. Proved here against a real [`config::Config`], so the
    /// guarantee is about the config an operator actually writes rather than
    /// about a hypothetical one.
    #[test]
    #[allow(clippy::expect_used)]
    fn admin_endpoints_publish_no_config_credentials() {
        const SENTINELS: [&str; 5] = [
            "AKIAEXAMPLEACCESSKEY",
            "s3cr3t-signing-key-value",
            "0123456789abcdef0123456789abcdef",
            "12345678:telegram-bot-token-value",
            "postgres://tele_s3:hunter2@db.internal/tele_s3",
        ];

        let config = config::Config {
            api_id: 12345,
            api_hash: SENTINELS[2].to_string(),
            bot_token: SENTINELS[3].to_string(),
            username: "tele_s3_channel".to_string(),
            database_uri: SENTINELS[4].to_string(),
            listen_port: 8000,
            auth_access_key: SENTINELS[0].to_string(),
            auth_secret_key: SENTINELS[1].to_string(),
            max_concurrent_connections: 128,
        };

        // The worst case for the operator: start-up reconciliation failed, so
        // the outcome is being published for a server that has something to
        // report.
        let state = tele_s3::admin::AdminState::new(
            config.listen_port,
            GrammersLimits::default(),
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Failed,
                publish_intents: ReconcilerOutcome::Failed,
            },
            true,
            true,
            config.max_concurrent_connections,
        );

        for response in [state.liveness(), state.readiness(), state.introspection()] {
            for secret in SENTINELS {
                assert!(
                    !response.body.contains(secret),
                    "{secret:?} leaked into {}",
                    response.status
                );
            }
            // Nor the names of the fields that hold them.
            for key in [
                "auth_access_key",
                "auth_secret_key",
                "api_hash",
                "bot_token",
                "database_uri",
                "password",
                "secret",
                "token",
            ] {
                assert!(
                    !response.body.contains(key),
                    "{key} leaked into introspection"
                );
            }
        }
    }

    /// The limiter refuses the second connection until the first permit is
    /// free.
    ///
    /// A limit of 0 is not a valid configuration, so it resolves to one
    /// permit instead of refusing every connection.
    #[test]
    fn the_connection_limiter_refuses_a_connection_when_no_permit_is_free() {
        for limiter in [ConnectionLimiter::new(1), ConnectionLimiter::new(0)] {
            let first = limiter.try_acquire().expect("the first permit is free");
            assert!(limiter.try_acquire().is_err());
            drop(first);
            assert!(limiter.try_acquire().is_ok());
        }
    }

    /// A valid override wins over the config value. An unset or unparsable
    /// value falls back to the config value. Both ends are clamped to at
    /// least one.
    #[test]
    fn the_connection_limit_override_falls_back_and_clamps() {
        assert_eq!(connection_limit_from(None, 128), 128);
        assert_eq!(connection_limit_from(Some("16"), 128), 16);
        assert_eq!(connection_limit_from(Some(" 16 \n"), 128), 16);
        assert_eq!(connection_limit_from(Some("many"), 128), 128);
        assert_eq!(connection_limit_from(Some(""), 128), 128);
        assert_eq!(connection_limit_from(Some("0"), 128), 1);
        assert_eq!(connection_limit_from(None, 0), 1);
    }

    /// The rejection is a complete HTTP/1.1 response with no body.
    ///
    /// A client must be able to parse it without a second read.
    #[test]
    fn the_rejection_is_a_complete_http_response() {
        let response =
            std::str::from_utf8(SERVICE_UNAVAILABLE_RESPONSE).expect("the response is ASCII");

        assert_eq!(
            response,
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
    }

    /// The default has to stay the human-readable renderer: nothing about an
    /// operator's existing output changes unless they ask for it.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_log_format_defaults_to_text() {
        assert_eq!(LogFormat::from_env(), LogFormat::Text);
    }

    /// `json` is the opt-in, and the spelling an operator will guess. Case
    /// and surrounding whitespace do not matter.
    #[test]
    #[allow(clippy::expect_used)]
    fn json_output_is_opt_in() {
        for raw in ["json", "JSON", " JSON \n"] {
            assert_eq!(LogFormat::parse(raw), Some(LogFormat::Json), "{raw}");
        }
    }

    /// An unrecognised value degrades to the default rather than to a process
    /// that logs nothing.
    #[test]
    #[allow(clippy::expect_used)]
    fn an_unknown_log_format_falls_back_to_text() {
        for raw in ["yaml", "1", ""] {
            assert_eq!(LogFormat::parse(raw), None, "{raw}");
        }
    }

    /// The default, and the one that has to stay free: no endpoint means
    /// export is off, so no exporter, no batching thread and no trace layer
    /// are ever built.
    ///
    /// This is the whole zero-cost argument, so it is asserted on the decision
    /// itself rather than on any side effect: [`TraceExport::from_env`] is
    /// the only thing that reads the environment, and it returns nothing.
    #[test]
    #[allow(clippy::expect_used)]
    fn trace_export_stays_off_without_a_configured_endpoint() {
        assert_eq!(TraceExport::endpoint_from(None, None), None);
        // A variable that is set to nothing is not a configuration either, and
        // must not become an endpoint the exporter then chokes on.
        assert_eq!(TraceExport::endpoint_from(Some(""), None), None);
        assert_eq!(TraceExport::endpoint_from(Some("   \n"), None), None);
        assert_eq!(TraceExport::endpoint_from(None, Some("")), None);

        // The graceful-exit path is reached with no provider at all, and is a
        // no-op rather than a panic. Neither half was ever built: the
        // decision to build them lives in the `from_env` above.
        let telemetry = Telemetry::default();
        assert!(!telemetry.trace_export());
        assert!(!telemetry.metrics_export());
        telemetry.flush();
    }

    /// The crate's own variable works standalone; the standard one is honoured
    /// when it is the only one set; and when both are set the crate's own
    /// spelling wins, so an operator can override a cluster-wide default.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_crates_own_endpoint_variable_takes_precedence() {
        let own = "http://collector.internal:4318/v1/traces";
        let standard = "http://otel-collector:4318/v1/traces";

        assert_eq!(
            TraceExport::endpoint_from(Some(own), Some(standard)),
            Some(own.to_string())
        );
        assert_eq!(
            TraceExport::endpoint_from(None, Some(standard)),
            Some(standard.to_string())
        );
        // A blank value falls through instead of shadowing the one below it.
        assert_eq!(
            TraceExport::endpoint_from(Some("  "), Some(standard)),
            Some(standard.to_string())
        );
        // Surrounding whitespace is an operator's editor, not a hostname.
        assert_eq!(
            TraceExport::endpoint_from(Some(" http://a/v1/traces \n"), None),
            Some("http://a/v1/traces".to_string())
        );
    }

    /// A configured endpoint is enough to build the pipeline. Nothing is
    /// exported and nothing is sent: the batching processor owns a queue and
    /// only talks to the collector when a span arrives, and the periodic
    /// reader only collects when an instrument records, so this needs no
    /// collector and no network.
    #[test]
    #[allow(clippy::expect_used)]
    fn a_configured_endpoint_initialises_the_pipeline() {
        let settings = TraceExport {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            sample_ratio: DEFAULT_SAMPLE_RATIO,
        };

        let provider = settings
            .build_pipeline()
            .expect("a well-formed endpoint builds an exporter");
        let meter = settings
            .build_meter_pipeline()
            .expect("a well-formed endpoint builds a metric exporter");

        // And shutting both down is a no-op, not a panic: this is the
        // graceful exit path, and it must survive an endpoint nobody is
        // listening on.
        Telemetry {
            traces: Some(provider),
            metrics: Some(meter),
        }
        .flush();
    }

    /// Telemetry must never take the server down. A collector that is not
    /// there is the ordinary case — the service starts before its collector
    /// does — so the end of the graceful-exit path is exercised against a
    /// port nothing is listening on.
    ///
    /// The export attempt fails on the exporter's own worker thread and is
    /// reported to [`Telemetry::flush`], which logs it and returns. No panic,
    /// and no error handed back to the caller that is trying to shut down.
    #[test]
    #[allow(clippy::expect_used)]
    fn an_unreachable_collector_cannot_break_shutdown() {
        let settings = TraceExport {
            // Port 1 on loopback: nothing listens there, and the connection is
            // refused immediately rather than left to time out.
            endpoint: "http://127.0.0.1:1/v1/traces".to_string(),
            sample_ratio: DEFAULT_SAMPLE_RATIO,
        };

        let provider = settings.build_pipeline().expect(
            "the exporter is built regardless; delivery is the background thread's problem",
        );

        Telemetry {
            traces: Some(provider),
            metrics: settings.build_meter_pipeline(),
        }
        .flush();
    }

    /// The resource is what tells a collector which service a trace belongs
    /// to. The version is the crate version, so it cannot drift from the
    /// binary that produced the span.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_resource_carries_the_service_name_and_crate_version() {
        let resource = TraceExport {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            sample_ratio: DEFAULT_SAMPLE_RATIO,
        }
        .resource();

        let attributes: std::collections::HashMap<String, String> = resource
            .iter()
            .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
            .collect();

        assert_eq!(
            attributes.get("service.name").map(String::as_str),
            Some(SERVICE_NAME)
        );
        assert_eq!(
            attributes.get("service.version").map(String::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    /// Every trace is exported unless the operator says otherwise, and every
    /// spelling of "otherwise" that a shell would produce is accepted.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_sample_ratio_defaults_to_every_trace() {
        assert_eq!(DEFAULT_SAMPLE_RATIO, 1.0);

        for (raw, expected) in [
            ("1", Some(1.0)),
            ("0.25", Some(0.25)),
            (" 0.5 \n", Some(0.5)),
            ("0", Some(0.0)),
        ] {
            assert_eq!(TraceExport::sample_ratio(raw), expected, "{raw}");
        }
    }

    /// A ratio that cannot be used falls back to the documented default and
    /// says so, rather than panicking or quietly meaning "export nothing".
    #[test]
    #[allow(clippy::expect_used)]
    fn an_unusable_sample_ratio_falls_back_to_the_default() {
        for raw in ["", "  ", "half", "1.5", "-0.1", "NaN", "inf"] {
            assert_eq!(TraceExport::sample_ratio(raw), None, "{raw}");
        }
    }
}
