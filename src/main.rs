use std::path::Path;

use clap::Parser;

mod config;
mod grammers;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    config: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = {
        let config_path = Path::new(&args.config);
        let config = config::Config::try_from(config_path)?;

        config
    };

    println!("{:?}", config);

    Ok(())
}
