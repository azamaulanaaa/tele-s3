//! Binary entry point for tele-s3.
use std::{sync::Arc, time::Duration};

use clap::Parser;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
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
    backend::{Grammers, GrammersConfig},
    s3::TeleS3,
    storage::Store,
};

mod config;

/// Name this process reports to the collector, as `service.name`.
const SERVICE_NAME: &str = "tele-s3";

/// The only transport built into this binary.
///
/// The build compiles `opentelemetry-otlp` without its gRPC client. No code
/// path can speak gRPC, even if a collector asks for it. The code publishes
/// the name at start-up. An operator can see which transport they got.
const TRACE_TRANSPORT: &str = "otlp_http_protobuf";

/// How often the meter provider pushes a collection to the collector.
///
/// Sixty seconds. This is the SDK's own default. The value appears here so
/// the number sits next to the interval an operator reads in the exporter's
/// own documentation.
const METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// `error.type` for a telemetry setting that could not be read.
const TELEMETRY_CONFIG_ERROR_TYPE: &str = "unparsable_telemetry_config";

/// `error.type` for spans that could not be flushed to the collector.
const TELEMETRY_EXPORT_ERROR_TYPE: &str = "telemetry_export";

/// Where and how much to export.
///
/// `None` means no endpoint is configured. This is the default and the common
/// case. It is genuinely inert, not merely quiet. With no endpoint, nothing
/// here builds an exporter. No batching thread starts. The subscriber is the
/// log-only one that shipped before this scope existed. A self-hosted operator
/// who never configures telemetry pays nothing for the machinery.
#[derive(Debug, Clone, PartialEq)]
struct TraceExport {
    /// OTLP collector endpoint. It uses the HTTP scheme and protobuf-encoded
    /// payloads.
    endpoint: String,
    /// Fraction of traces to keep.
    sample_ratio: f64,
}

impl TraceExport {
    /// Read the settings from the resolved config.
    ///
    /// `None` means that no endpoint is configured.
    fn from_config(config: &config::Config) -> Option<Self> {
        let endpoint = config
            .otlp_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())?
            .to_string();

        Some(Self {
            endpoint,
            sample_ratio: config.otlp_sample_ratio,
        })
    }

    /// The resource every exported span carries.
    ///
    /// The version is the crate version. It cannot drift from the binary that
    /// produced the span. Built empty rather than from the SDK's detectors.
    /// This document is the identity of this process. An operator who diffs
    /// two of them should not read SDK filler.
    fn resource(&self) -> Resource {
        Resource::builder_empty()
            .with_service_name(SERVICE_NAME)
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build()
    }

    /// Build the batching pipeline, or `None` if the exporter refuses.
    ///
    /// An endpoint the exporter will not accept is a configuration error. It
    /// is not a reason to refuse to serve S3. The caller logs it. The caller
    /// continues with the log-only subscriber.
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
    /// Same rule as [`TraceExport::build_pipeline`], for the same reason. An
    /// endpoint the exporter will not accept is a configuration error. It is
    /// not a reason to refuse to serve S3. Metrics share the trace export
    /// decision. One endpoint setting turns both on. An operator never has to
    /// determine which half of the pipeline is live.
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
/// Returns the trace provider when export is on. The caller keeps it. The
/// caller flushes it on the graceful-exit path. `None` means export is off.
/// No endpoint was configured, or the exporter refused the one that was. The
/// subscriber installed is then byte for byte the one that shipped before
/// trace export existed.
///
/// An OTLP failure is never fatal. The code builds the exporter before the log
/// layer. The reason for skipping it reaches the log. Once the pipeline is
/// up, export happens on its own background thread. A collector that is down,
/// slow, or gone cannot fail a request. It cannot panic the process.
///
/// # Field vocabulary
///
/// Every event this crate emits carries its data as named fields, not prose.
/// A log pipeline can count, filter, and aggregate the same event. The names
/// are the crate's contract. A downstream exporter binds to these strings.
/// The crate appends to them. It never repurposes them. One name means one
/// thing in every file.
///
/// ## Identity and location
///
/// | Field | Meaning |
/// | --- | --- |
/// | `bucket` | S3 bucket name. |
/// | `key` | S3 object key. |
/// | `blob_id` | Metadata-store id of a backend blob. |
/// | `publish_id` | Durable publish intent id that names an upload that may exist in the chat. |
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
/// Spans are the other half of the vocabulary. `#[instrument]` puts the
/// function's arguments on the span. `operation`-shaped fields such as `key`,
/// `blob_id`, and `publish_id` also arrive as span fields. Promote a field to
/// the span rather than re-log it. Do this if it is already there.
///
/// They are also the OTLP span attributes. The [`tracing-opentelemetry`]
/// layer binds to these same names. A field is spelled once for both a log
/// line and a trace.
///
/// # Metric vocabulary
///
/// The same names are the attributes on the metrics below. One query joins a
/// log line and a data point. The span `telegram.flood_wait` is the one span
/// added for this vocabulary. It exists so an individual rate-limit wait is
/// navigable with its own duration.
///
/// | Instrument | Kind | Unit | Attributes |
/// | --- | --- | --- | --- |
/// | `tele_s3.telegram.flood_limits` | counter | `{limit}` | `telegram_error`, `operation` |
/// | `tele_s3.telegram.flood_wait_duration` | histogram | `s` | `operation`, `peer` |
/// | `tele_s3.telegram.rpc_calls` | counter | `{call}` | `operation`, `outcome` |
/// | `tele_s3.telegram.rpc_duration` | histogram | `s` | `operation`, `outcome` |
///
/// `outcome` is a closed set on the RPC instruments: `ok`, `rpc_error`,
/// `timeout`. All four carry the `tele_s3.` prefix. They cannot collide with
/// another library's metrics in a shared collector.
///
/// Metrics and traces are one decision. No endpoint means no meter provider.
/// It means no periodic reader. It means no background thread. It means no
/// tracer provider. Everything the crate records goes through the
/// process-wide meter. A later scope adds instruments without a second
/// configuration surface.
fn init_tracing(config: &config::Config) -> Telemetry {
    let format = config.log_format;
    let log_filter = &config.log_filter;

    let Some(settings) = TraceExport::from_config(config) else {
        init_log_subscriber(format, log_filter);
        return Telemetry::default();
    };

    // The metric half is built from the same settings, so there is no
    // configuration in which traces are leaving the process and metrics are
    // not, or the reverse.
    let metrics = settings.build_meter_pipeline();

    let Some(provider) = settings.build_pipeline() else {
        init_log_subscriber(format, log_filter);
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

    init_otel_subscriber(format, log_filter, provider.tracer(SERVICE_NAME));

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
/// Either half is absent when telemetry is off. That is the default. An
/// absent provider is not a stub. Nothing was constructed for it. Nothing
/// runs in the background. Nothing is flushed on exit.
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
    /// Both halves are attempted even if the first fails. Draining the queue
    /// saves the data. Stopping the thread lets the process exit. It does not
    /// wait on a socket that never answers.
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

/// Build one filter from the resolved `log_filter` setting.
///
/// Built per layer, not once and shared. [`EnvFilter`] is not `Clone`. Each
/// layer needs its own copy.
fn env_filter(log_filter: &str) -> EnvFilter {
    EnvFilter::new(log_filter)
}

/// Install the log layer on its own.
///
/// This is the whole subscriber for a process with no trace export. It uses
/// the same `fmt` renderer. It uses the resolved filter.
fn init_log_subscriber(format: config::LogFormat, log_filter: &str) {
    match format {
        config::LogFormat::Text => FmtSubscriber::builder()
            .pretty()
            .with_env_filter(env_filter(log_filter))
            .init(),
        config::LogFormat::Json => FmtSubscriber::builder()
            .json()
            .with_env_filter(env_filter(log_filter))
            .init(),
    }
}

/// Install the log layer and the OTLP trace layer.
///
/// The log layer is `tracing-subscriber`'s own `fmt` renderer. It is the same
/// one [`init_log_subscriber`] installs. It is expressed as a layer, not as a
/// whole subscriber. A trace layer can only compose onto a [`Registry`]. Both
/// log formats keep working as before. The trace layer writes nothing to
/// stdout. It observes spans on their way to the collector.
///
/// Both layers use the same resolved `log_filter`. Filtering only the log
/// layer would let a `tele_s3=info` operator export debug spans they asked
/// not to see.
fn init_otel_subscriber(format: config::LogFormat, log_filter: &str, tracer: SdkTracer) {
    let logs: Box<dyn Layer<Registry> + Send + Sync> = match format {
        config::LogFormat::Text => Box::new(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_filter(env_filter(log_filter)),
        ),
        config::LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_filter(env_filter(log_filter)),
        ),
    };

    Registry::default()
        .with(logs)
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(env_filter(log_filter)),
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
/// is free, the accept loop sends `503 Service Unavailable`. It closes the
/// socket. It does not queue the connection. A slow client cannot exhaust
/// the listener.
#[derive(Debug)]
struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
}

impl ConnectionLimiter {
    /// Create a limiter with `max` permits.
    ///
    /// The code clamps the value to at least 1. A limiter with no permits would
    /// refuse every connection.
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = config::Args::parse();
    let resolved = config::Config::resolve(args, &config::process_env)?;
    let config = resolved.config;

    // Install tracing before any warning. The resolved settings select the
    // subscriber.
    let telemetry = init_tracing(&config);
    for warning in &resolved.warnings {
        warning.emit();
    }

    let store = Store::open(&config.database_path)?;

    // Sanitized once, here, so the backend and the admin endpoints share the
    // limits actually in force.
    let limits = config.telegram_limits();

    // Inbound S3 connections are bounded separately from Telegram I/O, so a
    // slow client cannot exhaust the listener. The resolver clamps the value.
    let max_concurrent_connections = config.max_concurrent_connections;

    let grammers = {
        let config = GrammersConfig {
            app_id: config.api_id,
            app_hash: config.api_hash,
            bot_token: config.bot_token,
            store: store.clone(),
            username: config.username,
            limits,
        };

        Grammers::init(config).await?
    };

    let s3_service = {
        let teles3 = TeleS3::init(grammers.clone(), store.clone()).await?;

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

    grammers.close();

    tracing::info!("exited gracefully");

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;

    use tele_s3::admin::ReconcilerOutcome;
    use tele_s3::backend::GrammersLimits;

    fn sample_config() -> config::Config {
        config::Config {
            api_id: 12345,
            api_hash: "api-hash".to_string(),
            bot_token: "12345:bot-token".to_string(),
            username: "tele_s3_channel".to_string(),
            database_path: "storage.redb".to_string(),
            listen_port: 8000,
            auth_access_key: "access-key".to_string(),
            auth_secret_key: "secret-key".to_string(),
            max_concurrent_connections: 128,
            telegram_max_concurrent_requests: 8,
            telegram_io_timeout_secs: 120,
            telegram_max_attempts: 8,
            log_format: config::LogFormat::Text,
            log_filter: "tele_s3=info".to_string(),
            otlp_endpoint: None,
            otlp_sample_ratio: 1.0,
        }
    }

    /// The code builds the introspection document from the port, the Telegram limits,
    /// and two closed enums. No argument of [`AdminState::new`] can carry a
    /// credential. This test uses a real [`config::Config`]. The guarantee
    /// covers the config an operator actually writes, not a hypothetical one.
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

        let mut config = sample_config();
        config.api_hash = SENTINELS[2].to_string();
        config.bot_token = SENTINELS[3].to_string();
        config.database_path = SENTINELS[4].to_string();
        config.auth_access_key = SENTINELS[0].to_string();
        config.auth_secret_key = SENTINELS[1].to_string();

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
                "database_path",
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
    /// A limit of 0 is not a valid configuration. It resolves to one
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

    /// No endpoint means export is off. The code never builds an exporter, a
    /// batching thread, or a trace layer.
    ///
    /// The graceful-exit path is reached with no provider at all. It is a
    /// no-op rather than a panic.
    #[test]
    #[allow(clippy::expect_used)]
    fn trace_export_stays_off_without_a_configured_endpoint() {
        let mut config = sample_config();
        config.otlp_endpoint = None;
        assert_eq!(TraceExport::from_config(&config), None);

        // A blank value is not a configuration either, and must not become an
        // endpoint the exporter then chokes on.
        config.otlp_endpoint = Some("   \n".to_string());
        assert_eq!(TraceExport::from_config(&config), None);

        let telemetry = Telemetry::default();
        assert!(!telemetry.trace_export());
        assert!(!telemetry.metrics_export());
        telemetry.flush();
    }

    /// A configured endpoint is enough to build the pipeline. The code exports
    /// nothing and sends nothing. The batching processor owns a queue. It
    /// talks to the collector only when a span arrives. The periodic reader
    /// collects only when an instrument records. This needs no collector and
    /// no network.
    #[test]
    #[allow(clippy::expect_used)]
    fn a_configured_endpoint_initialises_the_pipeline() {
        let settings = TraceExport {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            sample_ratio: 1.0,
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
    /// there is the ordinary case. The service starts before its collector
    /// does. The end of the graceful-exit path is exercised against a port
    /// nothing is listening on.
    ///
    /// The export attempt fails on the exporter's own worker thread. It is
    /// reported to [`Telemetry::flush`]. That logs it and returns. No panic
    /// occurs. The code hands no error back to the caller that is trying to
    /// exit.
    #[test]
    #[allow(clippy::expect_used)]
    fn an_unreachable_collector_cannot_break_shutdown() {
        let settings = TraceExport {
            // Port 1 on loopback: nothing listens there, and the connection is
            // refused immediately rather than left to time out.
            endpoint: "http://127.0.0.1:1/v1/traces".to_string(),
            sample_ratio: 1.0,
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

    /// The resource tells a collector which service a trace belongs to. The
    /// version is the crate version. It cannot drift from the binary that
    /// produced the span.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_resource_carries_the_service_name_and_crate_version() {
        let resource = TraceExport {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_string(),
            sample_ratio: 1.0,
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
}
