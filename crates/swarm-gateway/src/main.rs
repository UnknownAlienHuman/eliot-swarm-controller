use clap::Parser;
use std::{path::PathBuf, process::ExitCode};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

#[derive(Debug, Parser)]
#[command(name = "swarm-gateway", about = "Optional loopback MCP HTTP gateway")]
struct Cli {
    /// Existing controller config.toml; only gateway, storage.data_dir, ipc,
    /// and mcp are read.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override storage.data_dir using the existing controller rules.
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
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
    let config = swarm_gateway::Config::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    if !config.gateway.enabled {
        return Err(Error::new(
            "CONFIG_ERROR",
            "the local Remote Agent Gateway is disabled",
        ));
    }
    let credential_path =
        config.gateway.credential_file.as_deref().ok_or_else(|| {
            Error::new("CONFIG_ERROR", "gateway credential_file is not configured")
        })?;
    let bearer_path = config.gateway.local_bearer_file.as_deref().ok_or_else(|| {
        Error::new(
            "CONFIG_ERROR",
            "gateway local_bearer_file is not configured",
        )
    })?;
    let credential = load_credential(credential_path)?;
    let bearer_token = load_local_bearer(bearer_path)?;
    swarm_gateway::run(config, credential, bearer_token).await
}

fn load_credential(path: &std::path::Path) -> Result<Credential> {
    let credential: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if credential.client_id.is_empty() || credential.token.len() < 32 {
        return Err(Error::new("AUTH_ERROR", "invalid credential file"));
    }
    Ok(credential)
}

fn load_local_bearer(path: &std::path::Path) -> Result<String> {
    use std::io::Read;

    let mut contents = String::new();
    std::fs::File::open(path)?
        .take(515)
        .read_to_string(&mut contents)?;
    if contents.len() > 514 {
        return Err(Error::new(
            "AUTH_ERROR",
            "gateway bearer file exceeds 512 bytes plus a final line ending",
        ));
    }
    let token = contents.trim_end_matches(['\r', '\n']);
    if !(32..=512).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::new(
            "AUTH_ERROR",
            "gateway bearer file must contain one printable token of 32 to 512 bytes",
        ));
    }
    Ok(token.to_owned())
}
