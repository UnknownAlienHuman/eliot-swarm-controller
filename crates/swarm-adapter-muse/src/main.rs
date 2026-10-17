use std::{ffi::OsStr, path::PathBuf};
use swarm_adapter_muse::module_runtime::{ModuleOwnedBootstrap, run_owned};

#[tokio::main]
async fn main() {
    let mut args = std::env::args_os().skip(1);
    let path = if args.next().as_deref() == Some(OsStr::new("--config")) {
        args.next().map(PathBuf::from).filter(|path| path.is_absolute())
    } else { None };
    let Some(path) = path.filter(|_| args.next().is_none()) else { fatal("MUSE_MODULE_USAGE"); };
    let bootstrap = ModuleOwnedBootstrap::from_config_path(&path).unwrap_or_else(|error| fatal(&error.code));
    if let Err(error) = run_owned(bootstrap).await { fatal(&error.code); }
}

fn fatal(code: &str) -> ! {
    // Only a closed code leaves the worker. Never native errors/config/stderr.
    let code = if code.len() <= 128 && code.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') { code } else { "MUSE_MODULE_FATAL" };
    eprintln!("{{\"error_code\":\"{code}\"}}");
    std::process::exit(2)
}
