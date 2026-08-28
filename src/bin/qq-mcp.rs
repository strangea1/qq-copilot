use std::{io, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use qq_copilot_remote::{config::default_config_path, mcp::run_stdio};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(io::stderr)
        .without_time()
        .init();
    let cli = Cli::parse();
    let config_path = cli.config.map_or_else(default_config_path, Ok)?;
    run_stdio(&config_path).await
}
