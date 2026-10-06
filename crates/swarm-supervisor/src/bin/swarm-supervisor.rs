//! On-demand standalone supervisor process entrypoint.
//!
//! The trusted host writes exactly one bounded, typed bootstrap frame over a
//! private stdin pipe after its IPC listener has published readiness. The
//! frame contains no Store payload; the child then uses the existing
//! module-supervisor credential and typed control surface for every durable
//! read/write.

use std::{
    io::{self, BufReader, Read},
    process::ExitCode,
};
use swarm_supervisor::{StandaloneSupervisor, SupervisorBootstrap};

fn main() -> ExitCode {
    let result = (|| -> swarm_supervisor::Result<()> {
        let bootstrap = SupervisorBootstrap::from_frame(BufReader::new(io::stdin()))?;
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
            let (stop, stopping) = tokio::sync::watch::channel(false);
            let (eof_tx, mut eof_rx) = tokio::sync::oneshot::channel();
            let eof_thread = std::thread::Builder::new()
                .name("swarm-supervisor-stdin-eof".to_owned())
                .spawn(move || {
                    let mut input = io::stdin().lock();
                    let mut bytes = [0_u8; 1024];
                    loop {
                        match input.read(&mut bytes) {
                            Ok(0) | Err(_) => {
                                let _ = eof_tx.send(());
                                break;
                            }
                            Ok(_) => {}
                        }
                    }
                })
                .map_err(|_| {
                    swarm_supervisor::Error::new(
                        "MODULE_SUPERVISOR_STDIN_WATCH_UNAVAILABLE",
                        "standalone supervisor could not watch its private stdin",
                    )
                })?;
            let mut eof_thread = Some(eof_thread);
            let mut run = Box::pin(supervisor.run(stopping));
            tokio::select! {
                result = &mut run => {
                    // Dropping a running OS thread detaches it; the process
                    // may exit without waiting on an uncancellable stdin
                    // read after an idle supervisor pass.
                    drop(eof_thread.take());
                    result
                }
                _ = &mut eof_rx => {
                    let _ = stop.send(true);
                    drop(eof_thread.take());
                    run.await
                }
            }
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
