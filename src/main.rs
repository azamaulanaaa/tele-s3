use std::{env, path::Path, time::Duration};

use clap::Parser;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use sea_orm::Database;
use tokio::{net::TcpListener, signal};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tracing_subscriber::{EnvFilter, fmt::Subscriber as FmtSubscriber};

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

        match Self::parse(&raw) {
            Some(parsed) => parsed,
            None => {
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
}

/// Selects the process-wide log format. See [`LOG_FORMAT_ENV`].
const LOG_FORMAT_ENV: &str = "TELE_S3_LOG_FORMAT";

/// Install the process-wide `tracing` subscriber.
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
/// | `operation` | The Telegram RPC or S3 operation a failure belongs to. |
/// | `outcome` | Classification of a state a message may or may not be in. |
/// | `lock` | Name of the poisoned mutex. |
/// | `env_var` / `value` | An environment override and the raw text it was set to. |
///
/// Spans are the other half of the vocabulary: `#[instrument]` puts the
/// function's arguments on the span, so `operation`-shaped fields such as
/// `key`, `blob_id` and `publish_id` also arrive as span fields. Promote a
/// field to the span rather than re-logging it if it is already there.
fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("tele_s3=info"));

    let format = LogFormat::from_env();

    match format {
        LogFormat::Text => FmtSubscriber::builder()
            .pretty()
            .with_env_filter(env_filter)
            .init(),
        LogFormat::Json => FmtSubscriber::builder()
            .json()
            .with_env_filter(env_filter)
            .init(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);

        config::Config::try_from(config_path)?
    };

    let db = Database::connect(config.database_uri).await?;

    // Resolved once, here, so the admin endpoints publish the limits actually
    // in force rather than a second reading of the environment.
    let limits = telegram_limits();

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
        builder.set_route(AdminRoute::new(AdminState::new(
            config.listen_port,
            limits,
            startup,
        )));

        builder.build()
    };

    let listener = TcpListener::bind(("0.0.0.0", config.listen_port)).await?;
    tracing::info!(port = config.listen_port, "listening for S3 connections");

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, _)) => {
                        let io = TokioIo::new(stream);
                        let svc = s3_service.clone();

                        tokio::spawn(async move {
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

    db.close().await?;
    grammers.close();

    tracing::info!("exited gracefully");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tele_s3::admin::ReconcilerOutcome;

    /// The introspection document is built from the port, the Telegram limits
    /// and two closed enums — no argument of [`AdminState::new`] can carry a
    /// credential. Proved here against a real [`config::Config`], so the
    /// guarantee is about the config an operator actually writes rather than
    /// about a hypothetical one.
    #[test]
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

    /// The default has to stay the human-readable renderer: nothing about an
    /// operator's existing output changes unless they ask for it.
    #[test]
    fn the_log_format_defaults_to_text() {
        assert_eq!(LogFormat::from_env(), LogFormat::Text);
    }

    /// `json` is the opt-in, and the spelling an operator will guess. Case
    /// and surrounding whitespace do not matter.
    #[test]
    fn json_output_is_opt_in() {
        for raw in ["json", "JSON", " JSON \n"] {
            assert_eq!(LogFormat::parse(raw), Some(LogFormat::Json), "{raw}");
        }
    }

    /// An unrecognised value degrades to the default rather than to a process
    /// that logs nothing.
    #[test]
    fn an_unknown_log_format_falls_back_to_text() {
        for raw in ["yaml", "1", ""] {
            assert_eq!(LogFormat::parse(raw), None, "{raw}");
        }
    }
}
