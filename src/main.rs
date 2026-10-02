use std::{env, path::Path, time::Duration};

use clap::Parser;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use sea_orm::Database;
use tokio::{net::TcpListener, signal};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tracing_subscriber::EnvFilter;

use tele_s3::{
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
fn telegram_limits() -> GrammersLimits {
    let defaults = GrammersLimits::default();

    fn env_or<T: std::str::FromStr>(name: &str, fallback: T) -> T {
        match env::var(name) {
            Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(value = %raw, "Ignoring {}", name);
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("tele_s3=info"));

    tracing_subscriber::fmt()
        .pretty()
        // .with_target(true)
        // .with_file(true)
        // .with_line_number(true)
        .with_env_filter(env_filter)
        .init();

    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);

        config::Config::try_from(config_path)?
    };

    let db = Database::connect(config.database_uri).await?;

    let grammers = {
        let config = GrammersConfig {
            app_id: config.api_id,
            app_hash: config.api_hash,
            bot_token: config.bot_token,
            db: db.clone(),
            username: config.username,
            limits: telegram_limits(),
        };

        Grammers::init(config).await?
    };

    let s3_service = {
        let teles3 = TeleS3::init(grammers.clone(), db.clone()).await?;

        // One bounded, non-fatal reconciliation pass before any traffic:
        // blob rows left behind by an interrupted publish are invisible to
        // every listing and nothing else reclaims them.
        match teles3.reconcile_orphan_blobs().await {
            Ok(0) => {}
            Ok(removed) => tracing::info!("Reconciled {removed} orphaned blob(s)"),
            Err(err) => tracing::warn!("Blob reconciliation failed: {:?}", err),
        }

        let mut builder = S3ServiceBuilder::new(teles3);

        let auth = SimpleAuth::from_single(&config.auth_access_key, config.auth_secret_key);
        builder.set_auth(auth);

        builder.build()
    };

    let listener = TcpListener::bind(("0.0.0.0", config.listen_port)).await?;
    tracing::info!("Listening on port {}", config.listen_port);

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
                                tracing::error!("Error serving connection: {:?}", err);
                            }
                        });
                    }
                    Err(err) => {
                        tracing::error!("Accept error: {:?}", err);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Shutdown signal received, starting graceful exit...");
                break;
            }
        }
    }
    tracing::info!("Shutting down services...");

    db.close().await?;
    grammers.close();

    tracing::info!("Exited gracefully.");

    Ok(())
}
