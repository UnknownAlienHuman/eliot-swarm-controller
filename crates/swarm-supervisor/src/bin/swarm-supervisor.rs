//! On-demand standalone supervisor process entrypoint.
//!
//! The trusted host writes exactly one bounded, typed bootstrap frame over a
//! private stdin pipe after its IPC listener has published readiness. The
//! frame contains no Store payload; the child then uses the existing
//! module-supervisor credential and typed control surface for every durable
//! read/write.

use std::{io, process::ExitCode};
use swarm_supervisor::{StandaloneSupervisor, SupervisorBootstrap};

fn main() -> ExitCode {
    let result = (|| -> swarm_supervisor::Result<()> {
        let bootstrap = SupervisorBootstrap::from_reader(io::stdin().lock())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                swarm_supervisor::Error::new(
                    "MODULE_SUPERVISOR_RUNTIME_UNAVAILABLE",
                    format!("standalone supervisor runtime could not start: {error}"),
                )
            })?;
        runtime.block_on(async move {
            let supervisor = StandaloneSupervisor::start_from_bootstrap(bootstrap).await?;
            supervisor.run_forever().await
        })
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("standalone module supervisor: {}", error.code);
            ExitCode::FAILURE
        }
    }
}
