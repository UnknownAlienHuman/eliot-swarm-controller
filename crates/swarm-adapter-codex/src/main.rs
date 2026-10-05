use std::{env, fs::File, path::PathBuf};

use swarm_adapter_codex::{AdapterConfig, run};

#[tokio::main]
async fn main() {
    let code = match run_from_args().await {
        Ok(()) => "CODEX_ADAPTER_STOPPED",
        Err(error) => error.code(),
    };
    eprintln!("{code}");
}

async fn run_from_args() -> Result<(), swarm_adapter_codex::AdapterError> {
    let mut args = env::args_os();
    let _program = args.next();
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or(swarm_adapter_codex::AdapterError::Configuration)?;
    if args.next().is_some() {
        return Err(swarm_adapter_codex::AdapterError::Configuration);
    }
    let file = File::open(path).map_err(|_| swarm_adapter_codex::AdapterError::Configuration)?;
    let config: AdapterConfig = serde_json::from_reader(file)
        .map_err(|_| swarm_adapter_codex::AdapterError::Configuration)?;
    run(config).await
}
