use std::{path::PathBuf, process};
use swarm_adapter_claude::{OwnedBootstrap, run_owned};
use swarm_contracts::error::Error;

#[tokio::main]
async fn main() {
    let config_path = match config_path() {
        Ok(path) => path,
        Err(error) => fail(error),
    };
    let bootstrap = match OwnedBootstrap::from_config_path(&config_path) {
        Ok(bootstrap) => bootstrap,
        Err(error) => fail(error),
    };
    if let Err(error) = run_owned(bootstrap).await {
        fail(error);
    }
}

fn config_path() -> Result<PathBuf, Error> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--config")) {
        return Err(Error::new(
            "ADAPTER_USAGE",
            "expected --config <absolute-path>",
        ));
    }
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("ADAPTER_USAGE", "configuration path is missing"))?;
    if args.next().is_some() || !path.is_absolute() {
        return Err(Error::new(
            "ADAPTER_USAGE",
            "configuration path must be the only absolute argument",
        ));
    }
    Ok(path)
}

fn fail(error: Error) -> ! {
    eprintln!("swarm-adapter-claude: {}", error.code);
    process::exit(2)
}
