use crate::{
    config::Config,
    error::{Error, Result},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use swarm_observer::{LiveConfigSource, RecorderConfig};
use tokio::{
    sync::{Semaphore, watch},
    task::{Id, JoinSet},
};

mod legacy_optional_workers;

pub async fn run(config: Config) -> Result<()> {
    run_until(config, async {
        tokio::signal::ctrl_c().await.map_err(Into::into)
    })
    .await
}

/// Foreground embedding uses the owner's stdin EOF as a graceful shutdown
/// request. A detached std thread avoids blocking Tokio runtime shutdown if a
/// different supervisor fails while stdin is still open.
pub async fn run_on_stdin_eof(config: Config) -> Result<()> {
    let (sent, received) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("swarm-host-stdin".into())
        .spawn(move || {
            use std::io::Read;
            let result = (|| -> Result<()> {
                let mut input = std::io::stdin().lock();
                let mut bytes = [0_u8; 1024];
                loop {
                    match input.read(&mut bytes) {
                        Ok(0) => return Ok(()),
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            })();
            let _ = sent.send(result);
        })?;
    run_until(config, async {
        received.await.map_err(|_| {
            Error::new(
                "HOST_SHUTDOWN",
                "foreground owner input ended without a result",
            )
        })?
    })
    .await
}

fn spawn_supervisor<F>(
    supervisors: &mut JoinSet<(&'static str, Result<()>)>,
    names: &mut HashMap<Id, &'static str>,
    name: &'static str,
    run: F,
) where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let id = supervisors.spawn(async move { (name, run.await) }).id();
    names.insert(id, name);
}

async fn run_until(
    config: Config,
    foreground_stop: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(foreground_stop);
    let root = DataRoot::acquire(&config.storage.data_dir)
        .map_err(|error| startup_error("data_root", error))?;
    let credential = bootstrap_credential(&root.path)
        .map_err(|error| startup_error("credential_bootstrap", error))?;
    let root_path = root.path.clone();
    let mut observer_config_valid = false;
    let mut text_capture_policy = None;
    let observer = config.observability.enabled.then(|| {
        let live_config = config
            .observability
            .live_config_file
            .as_ref()
            .map(|path| LiveConfigSource::new(path.clone(), &root.path));
        let policy_source = live_config
            .clone()
            .unwrap_or_else(|| LiveConfigSource::defaults_for_scope(&root.path));
        let recorder_config = RecorderConfig {
            directory: config.observability.recording_directory(&root.path),
            queue_records: config.observability.queue_records,
            queue_bytes: config.observability.queue_bytes,
            max_record_bytes: config.observability.max_record_bytes,
            segment_bytes: config.observability.file_segment_bytes,
            retention_bytes: config.observability.retention_bytes,
            retention_days: config.observability.retention_days,
        };
        observer_config_valid = recorder_config.validate().is_ok();
        text_capture_policy = if observer_config_valid {
            Some(policy_source.text_capture_policy())
        } else {
            None
        };
        let recorder = if observer_config_valid {
            swarm_observer::host::HostRecorder::new_with_live_config(recorder_config, live_config)
        } else {
            swarm_observer::host::HostRecorder::disabled_for_invalid_config(recorder_config)
        };
        Arc::new(recorder)
    });
    let line_observer = observer.as_ref().map(|recorder| {
        let recorder = Arc::clone(recorder);
        Arc::new(move |line: &[u8], manager_policy| {
            let _ = recorder.observe_line_with_manager_policy(line, manager_policy);
        }) as swarm_telemetry::LineObserver
    });
    let config = Arc::new(config);
    let owner = StoreOwner::start_with_observer_policies(
        root,
        config.clone(),
        credential,
        line_observer,
        text_capture_policy,
    )
    .await
    .map_err(|error| startup_error("store_start", error))?;
    let mut host_image_receipt = if observer_config_valid {
        match swarm_observer::host_image_receipt::HostImageReceipt::publish(&root_path) {
            Ok(receipt) => Some(receipt),
            Err(error) => {
                eprintln!("observer host image receipt: {}", error.code);
                None
            }
        }
    } else {
        None
    };
    let ipc_config = Arc::new(config.ipc.clone());
    let startup: Result<ipc::Listener> = async {
        owner.store.record_host_start().await?;
        owner.store.initialize_workspace_authority().await?;
        let listener = ipc::Listener::bind(&root_path)?;
        owner.store.record_host_ready().await?;
        Ok(listener)
    }
    .await;
    let mut listener = match startup {
        Ok(listener) => listener,
        Err(error) => {
            if let Err(receipt_error) = owner
                .store
                .record_host_exit(Some(error.code.clone()), None)
                .await
            {
                eprintln!("host startup failure receipt: {}", receipt_error.code);
            }
            let producer_drained = owner.store.drain_diagnostics(Duration::from_secs(5)).await;
            let producer_stats = owner.store.diagnostic_stats();
            cleanup_host_image_receipt(&mut host_image_receipt);
            if let Err(close_error) = owner.close().await {
                eprintln!("host startup Store close: {}", close_error.code);
            }
            report_observer_shutdown(&observer, Some(producer_stats), producer_drained);
            return Err(error);
        }
    };
    let (shutdown, stopping) = watch::channel(false);
    // Keep the optional actor outside the host's required JoinSet: a module
    // failure cannot stop the Store, listener, or unrelated workers.
    let optional_module_supervisor = if config.module_supervisor.enabled {
        Some(
            crate::host_module_supervisor::spawn_isolated_module_supervisor(
                owner.store.clone(),
                owner.module_supervisor_credential(),
                root_path.clone(),
                (*ipc_config).clone(),
                config.module_supervisor.clone(),
                stopping.clone(),
            ),
        )
    } else {
        None
    };
    let optional_bus_supervisor = crate::host_bus_supervisor::spawn_isolated_managed_bus_supervisor(
        owner.store.clone(),
        root_path.clone(),
        config.bus_supervisor.clone(),
        stopping.clone(),
    );
    let mut supervisors: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
    // A JoinError contains the task ID but no output label; retain only each
    // fixed supervisor name so a panic can be attributed without its payload.
    let mut supervisor_names: HashMap<Id, &'static str> = HashMap::new();
    let legacy_store = owner.store.clone();
    let legacy_stop = stopping.clone();
    spawn_supervisor(
        &mut supervisors,
        &mut supervisor_names,
        "legacy-workers",
        async move { legacy_optional_workers::run(legacy_store, legacy_stop).await },
    );
    let semaphore = Arc::new(Semaphore::new(config.ipc.max_connections));
    let mut connections = JoinSet::new();
    eprintln!("swarm host ready: {}", listener.endpoint());
    let mut failed_supervisor: Option<&'static str> = None;
    let mut exit = loop {
        tokio::select! {
            signal=&mut foreground_stop=>break signal,
            Some(result)=supervisors.join_next_with_id()=>{
                break match result {
                    Ok((id, (name, Ok(())))) => {
                        let _ = supervisor_names.remove(&id);
                        failed_supervisor = Some(name);
                        Err(Error::new("SUPERVISOR_STOPPED", format!("{name} stopped before host shutdown")))
                    }
                    Ok((id, (name, Err(error)))) => {
                        let _ = supervisor_names.remove(&id);
                        failed_supervisor = Some(name);
                        Err(Error::new(error.code, format!("{name}: {}", error.message)))
                    }
                    Err(error) => {
                        failed_supervisor = supervisor_names.remove(&error.id());
                        Err(Error::new("SUPERVISOR_FAILED", error.to_string()))
                    }
                };
            }
            Some(result)=connections.join_next(),if !connections.is_empty()=>{
                if let Err(e)=result{eprintln!("IPC worker ended: {e}");}
            }
            accepted=listener.accept()=>{
                let stream=match accepted{Ok(s)=>s,Err(e)=>break Err(e)};
                let permit=match semaphore.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>{drop(stream);continue}};
                let store=owner.store.clone();let config=ipc_config.clone();let stopping=stopping.clone();
                connections.spawn(async move{
                    let _permit=permit;
                    if let Err(e)=ipc::serve_connection(
                        stream,store,config,stopping
                    ).await{eprintln!("IPC connection: {}",e.code);}
                });
            }
        }
    };
    drop(listener);
    let _ = shutdown.send(true);
    while connections.join_next().await.is_some() {}
    if let Some(actor) = optional_module_supervisor {
        actor.join().await;
    }
    if let Some(actor) = optional_bus_supervisor {
        actor.join().await;
    }
    // Await host-owned workers. Dropping the IPC caller or beginning shutdown
    // must not detach or replay an already admitted external publication.
    while let Some(result) = supervisors.join_next_with_id().await {
        match result {
            Ok((id, (_, Ok(())))) => {
                let _ = supervisor_names.remove(&id);
            }
            Ok((id, (name, Err(error)))) => {
                let _ = supervisor_names.remove(&id);
                eprintln!("{name} supervisor: {}", error.code);
                if exit.is_ok() {
                    failed_supervisor = Some(name);
                    exit = Err(error);
                }
            }
            Err(error) => {
                let name = supervisor_names.remove(&error.id());
                eprintln!("{} supervisor join failed", name.unwrap_or("unknown"));
                if exit.is_ok() {
                    failed_supervisor = name;
                    exit = Err(Error::new("SUPERVISOR_FAILED", error.to_string()));
                }
            }
        }
    }
    if let Some(error_code) = kernel_admission_error_code(owner.store.kernel_snapshot()) {
        eprintln!("host kernel admission: {error_code}");
        if exit.is_ok() {
            exit = Err(Error::new(error_code, "kernel durable admission is closed"));
        }
    }
    if let Err(error) = owner
        .store
        .record_host_exit(
            exit.as_ref().err().map(|error| error.code.clone()),
            failed_supervisor,
        )
        .await
    {
        eprintln!("host exit receipt: {}", error.code);
        if exit.is_ok() {
            exit = Err(error);
        }
    }
    let producer_drained = owner.store.drain_diagnostics(Duration::from_secs(5)).await;
    let producer_stats = owner.store.diagnostic_stats();
    cleanup_host_image_receipt(&mut host_image_receipt);
    if let Err(error) = owner.close().await
        && exit.is_ok()
    {
        exit = Err(error);
    }
    report_observer_shutdown(&observer, Some(producer_stats), producer_drained);
    exit
}

fn kernel_admission_error_code(snapshot: swarm_kernel::KernelHostSnapshot) -> Option<&'static str> {
    match snapshot.admission {
        swarm_kernel::KernelAdmissionState::Closed { fault } => Some(match fault {
            swarm_kernel::KernelAdmissionFault::InitializationFailed => {
                "KERNEL_INITIALIZATION_FAILED"
            }
            swarm_kernel::KernelAdmissionFault::StoreUnavailable => "KERNEL_STORE_UNAVAILABLE",
            swarm_kernel::KernelAdmissionFault::DurableJournalUnavailable => {
                "KERNEL_JOURNAL_UNAVAILABLE"
            }
            swarm_kernel::KernelAdmissionFault::ShuttingDown => "KERNEL_SHUTTING_DOWN",
        }),
        swarm_kernel::KernelAdmissionState::Starting | swarm_kernel::KernelAdmissionState::Open => {
            None
        }
    }
}

fn cleanup_host_image_receipt(
    receipt: &mut Option<swarm_observer::host_image_receipt::HostImageReceipt>,
) {
    if let Some(receipt) = receipt.take()
        && let Err(error) = receipt.cleanup_under_store_lock()
    {
        eprintln!("observer host image receipt cleanup: {}", error.code);
    }
}

fn report_observer_shutdown(
    observer: &Option<Arc<swarm_observer::host::HostRecorder>>,
    producer: Option<swarm_telemetry::Stats>,
    producer_drained: bool,
) {
    let Some(observer) = observer else { return };
    let (stats, shutdown_attempted, error_code) = if producer_drained {
        let (stats, error_code) = observer.shutdown_with_status();
        (Some(stats), true, error_code)
    } else {
        (
            observer.try_stats(),
            false,
            Some("TELEMETRY_PRODUCER_DRAIN_TIMEOUT"),
        )
    };
    let dropped_records = stats.as_ref().map(|stats| {
        stats
            .recorder
            .dropped_records
            .saturating_add(stats.startup_dropped_records)
            .saturating_add(stats.post_shutdown_dropped_records)
    });
    let dropped_bytes = stats.as_ref().map(|stats| {
        stats
            .recorder
            .dropped_bytes
            .saturating_add(stats.startup_dropped_bytes)
            .saturating_add(stats.post_shutdown_dropped_bytes)
    });
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "observer.shutdown",
            "enabled": true,
            "recorder_started": stats.as_ref().map(|stats| stats.started),
            "recorder_shutdown_attempted": shutdown_attempted,
            "recorder_stats_available": stats.is_some(),
            "producer_drained": producer_drained,
            "producer_pending_records": producer.as_ref().map(|value| value.pending_records),
            "producer_pending_bytes": producer.as_ref().map(|value| value.pending_bytes),
            "producer_enqueued_records": producer.as_ref().map(|value| value.enqueued_records),
            "producer_written_records": producer.as_ref().map(|value| value.written_records),
            "producer_written_bytes": producer.as_ref().map(|value| value.written_bytes),
            "producer_dropped_records": producer.as_ref().map(|value| value.dropped_records),
            "producer_dropped_bytes": producer.as_ref().map(|value| value.dropped_bytes),
            "producer_sink_failures": producer.as_ref().map(|value| value.sink_failures),
            "producer_startup_failures": producer.as_ref().map(|value| value.startup_failures),
            "producer_observer_panics": producer.as_ref().map(|value| value.observer_panics),
            "accepted_records": stats.as_ref().map(|stats| stats.recorder.accepted_records),
            "written_records": stats.as_ref().map(|stats| stats.recorder.written_records),
            "written_bytes": stats.as_ref().map(|stats| stats.recorder.written_bytes),
            "dropped_records": dropped_records,
            "dropped_bytes": dropped_bytes,
            "durability_unknown_records": stats.as_ref().map(|stats| stats.recorder.durability_unknown_records),
            "durability_unknown_bytes": stats.as_ref().map(|stats| stats.recorder.durability_unknown_bytes),
            "sink_failures": stats.as_ref().map(|stats| stats.recorder.sink_failures),
            "startup_failures": stats.as_ref().map(|stats| stats.startup_failures),
            "callback_errors": stats.as_ref().map(|stats| stats.callback_errors),
            "post_shutdown_dropped_records": stats.as_ref().map(|stats| stats.post_shutdown_dropped_records),
            "post_shutdown_dropped_bytes": stats.as_ref().map(|stats| stats.post_shutdown_dropped_bytes),
            "pending_records": stats.as_ref().map(|stats| stats.recorder.pending_records),
            "pending_bytes": stats.as_ref().map(|stats| stats.recorder.pending_bytes),
            "filtered_records": stats.as_ref().map(|stats| stats.recorder.filtered_records),
            "filtered_bytes": stats.as_ref().map(|stats| stats.recorder.filtered_bytes),
            "live_config_version": stats.as_ref().map(|stats| stats.recorder.live_config_version),
            "live_config_reload_failures": stats.as_ref().map(|stats| stats.recorder.live_config_reload_failures),
            "live_config_last_error_code": stats.as_ref().and_then(|stats| stats.recorder.live_config_last_error_code),
            "error_code": error_code
        })
    );
}

/// Before the Store is available, the foreground launcher receives a precise
/// phase and controller code as the terminal startup result. No DB receipt can
/// be promised when opening that database itself failed.
fn startup_error(phase: &'static str, error: Error) -> Error {
    Error::new(
        error.code,
        format!(
            "host startup failed at {phase}; inspect that stage before starting the host again"
        ),
    )
}

/// One host-owned reconciler for enabled automation entries and passive scoped
/// watches. Bounded startup/periodic scans recover after missed hints or
/// restart without creating per-watch tasks.
async fn supervise_automation(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut changed = store.subscribe_schedule_changes();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            result = changed.changed() => {
                if result.is_err() { return Err(Error::new("STORE_CLOSED", "automation change stream ended")); }
                if *stopping.borrow() { return Ok(()); }
                store.reconcile_automations_once().await?;
                store.reconcile_review_dispositions_once().await?;
                store.reconcile_watches_once().await?;
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                store.reconcile_automations_once().await?;
                store.reconcile_review_dispositions_once().await?;
                store.reconcile_watches_once().await?;
            }
        }
    }
}

async fn supervise_launcher(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                store.reconcile_launches_once().await?;
                store.reconcile_launch_issuance_once().await?;
                store.reconcile_workspace_lifecycle_once().await?;
            }
        }
    }
}

async fn supervise_native_mcp(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                // Await the bounded readback through shutdown, independently
                // of launch preparation and passive watch reconciliation.
                store.reconcile_native_mcp_once().await?;
            }
        }
    }
}

async fn supervise_native_mcp_tools(
    store: Store,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                // Store coalescing and durable backoff bound discovery work;
                // shutdown awaits the admitted pass rather than detaching it.
                store.reconcile_native_mcp_tools_once().await?;
            }
        }
    }
}

async fn supervise_forge(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                // This pass is awaited through shutdown, independent of IPC.
                // The Store reads unknown outcomes before never-sent effects.
                store.supervise_forge_once().await?;
            }
        }
    }
}
