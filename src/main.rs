use std::path::Path;

use clap::Parser;

use crate::grammers::Grammers;

mod config;
mod grammers;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);
        let config = config::Config::try_from(config_path)?;

        config
    };

    let _grammers = {
        let session_path = Path::new(&config.session_file);
        let mut grammers = Grammers::init(config.api_id, &session_path).await?;
        grammers
            .authenticate(&config.bot_token, &config.api_hash)
            .await?;

        grammers
    };

    Ok(())
}
