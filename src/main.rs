use std::path::Path;

use clap::Parser;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use sea_orm::Database;
use tokio::{net::TcpListener, signal};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tracing_subscriber::EnvFilter;

use crate::{
    backend::{Grammers, GrammersConfig},
    s3::TeleS3,
};

mod backend;
mod config;
mod s3;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("tele_s3=info"));

    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);
        let config = config::Config::try_from(config_path)?;

        config
    };

    let db = Database::connect(config.database_uri).await?;

    let grammers = {
        let config = GrammersConfig {
            app_id: config.api_id,
            app_hash: config.api_hash,
            bot_token: config.bot_token,
            db: db.clone(),
            username: config.username,
        };

        Grammers::init(config).await?
    };

    let s3_service = {
        let teles3 = TeleS3::init(grammers.clone(), db.clone()).await?;
        let mut builder = S3ServiceBuilder::new(teles3);

        let auth = SimpleAuth::from_single(&config.auth_access_key, config.auth_secret_key);
        builder.set_auth(auth);

        let service = builder.build();

        service
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
