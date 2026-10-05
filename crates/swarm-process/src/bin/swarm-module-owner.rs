use std::{env, path::Path};
use swarm_process::module_owner::ModuleOwnerBootstrap;

fn main() {
    let mut args = env::args_os();
    let _program = args.next();
    let Some(plan) = args.next() else {
        eprintln!("swarm-module-owner requires an absolute plan path and resolver-map path");
        std::process::exit(64);
    };
    let Some(resolver_map) = args.next() else {
        eprintln!("swarm-module-owner requires an explicit resolver-map path");
        std::process::exit(64);
    };
    if args.next().is_some() {
        eprintln!("swarm-module-owner accepts exactly two paths");
        std::process::exit(64);
    }
    match ModuleOwnerBootstrap::run(Path::new(&plan), Path::new(&resolver_map)) {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            eprintln!("{}", error.code);
            std::process::exit(1);
        }
    }
}
