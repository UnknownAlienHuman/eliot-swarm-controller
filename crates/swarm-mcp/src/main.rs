use clap::Parser;
use std::{path::PathBuf, process::ExitCode};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

#[derive(Debug, Parser)]
#[command(
    name = "swarm-mcp",
    about = "Independent MCP client for a local Swarm host"
)]
struct Cli {
    /// Existing controller config.toml; only storage.data_dir, ipc, and mcp are read.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override storage.data_dir using the same absolute/current-directory rules as swarm.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Credential JSON; defaults to <storage.data_dir>/operator.json.
    #[arg(long)]
    credential: Option<PathBuf>,
    /// Fixed named profile from [mcp.profiles].
    #[arg(long)]
    profile: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {}", error.code, error.message);
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = swarm_mcp::Config::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    let credential_path = cli
        .credential
        .unwrap_or_else(|| config.storage.data_dir.join("operator.json"));
    let credential = load_credential(&credential_path)?;
    swarm_mcp::run_profiled(config, credential, cli.profile.as_deref()).await
}

fn load_credential(path: &std::path::Path) -> Result<Credential> {
    let credential: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if credential.client_id.is_empty() || credential.token.len() < 32 {
        return Err(Error::new("AUTH_ERROR", "invalid credential file"));
    }
    Ok(credential)
}
