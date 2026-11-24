pub(crate) mod workers;

use crate::{
    automation,
    bus::Bus,
    common::now_ms,
    logging, maintenance,
    platform::{DataDir, IpcListener, IpcStream, data_dir::DaemonLock},
    restart_capacity,
    router::Router,
};
use eliot_core::api::{
    CommandGetRequest, MAX_SWARM_WAIT_MS, SwarmWaitRequest, SwarmWaitResponse, WaitTurnRef,
};
use eliot_core::{
    Command, CommandState, Error, FailureOrigin, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SecretPolicy,
    StderrDiagnostic, bound_stderr_diagnostics,
};
use eliot_plugin::{RpcError, RpcRequest, RpcResponse};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinSet,
    time::{Instant, sleep_until, timeout, timeout_at},
};
use tokio_util::codec::{Framed, LinesCodec};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

pub const MAX_RPC_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MANAGED_ACL_DIRECTORIES: [&str; 3] = ["logs", "plugins", "worktrees"];
const MONITOR_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

/// Nonsemantic IPC timings. Handler time includes authentication, lock waits and
/// Store work; the remaining round trip includes framing/serialization and I/O.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CallTimings {
    pub connect_ns: u64,
    pub round_trip_ns: u64,
    pub decode_ns: u64,
    pub handle_ns: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct HandleTiming {
    handle_ns: u64,
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// Measure the same authenticated client path, with no retry after uncertain
/// delivery. Timings are diagnostic metadata, never part of a durable result.
pub async fn client_call_measured(
    path: &Path,
    method: &str,
    params: Value,
) -> Result<(Value, CallTimings)> {
    validate_client_call(method, &params)?;
    let identity = caller_identity_for_target(path)?;
    let mut timings = CallTimings::default();
    let value = client_call_with_identity_measured(
        path,
        method,
        params,
        identity,
        Some(&mut timings),
        None,
    )
    .await?;
    Ok((value, timings))
}
const MAX_CLIENTS: usize = 128;
const ACCEPT_FAILURE_LIMIT: u32 = 20;
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(35);
const OVERLOAD_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound for one server-side response write. A peer that stops reading must
/// not hold its connection task past this deadline: the task ends and finishes
/// response bookkeeping while the committed result stays readable by retry.
const RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Disk-space estimate reported immediately before a Store VACUUM rewrite.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StoreCompactionSpaceEstimate {
    pub main_database_bytes: u64,
    pub wal_bytes: u64,
    pub database_and_wal_bytes: u64,
    pub estimated_required_bytes: u64,
    pub available_bytes: u64,
}

/// Result of explicit offline or opt-in startup Store compaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StoreCompactionOutcome {
    pub estimate: Option<StoreCompactionSpaceEstimate>,
    pub converted: bool,
    pub pages_reclaimed: u64,
    pub freelist_pages: u64,
}

/// Compact an existing private Store while holding its normal DataDir lock.
/// The callback reports the shared disk-space preflight before SQLite can run
/// VACUUM. This local path never starts or contacts a daemon.
pub fn compact_store_offline(
    path: &Path,
    before_vacuum: impl FnMut(&StoreCompactionSpaceEstimate),
) -> Result<StoreCompactionOutcome> {
    if AGENT_IDENTITY_ENV
        .iter()
        .chain(&RUN_IDENTITY_ENV)
        .any(|name| std::env::var_os(name).is_some())
        || std::env::var_os(AGENT_DATA_DIR_ENV).is_some()
        || std::env::var_os(AGENT_DATA_SCOPE_ENV).is_some()
        || std::env::var_os(SHELL_IDENTITY_DISABLED_ENV).is_some()
    {
        return Err(Error::new(
            "not_allowed",
            "offline Store compaction requires the local Owner",
        ));
    }
    let data_dir = DataDir::open_existing(path)?;
    let _daemon_lock = data_dir.acquire_daemon_lock()?;
    let store_path = data_dir.path().join("eliot.db");
    crate::platform::data_dir::validate_store_files(data_dir.path())?;
    let metadata = fs::symlink_metadata(&store_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::new("unavailable", "store database does not exist")
        } else {
            Error::new(
                "platform_io",
                format!("cannot inspect store database: {error}"),
            )
        }
    })?;
    if !metadata.file_type().is_file() || crate::platform::is_reparse_point(&metadata) {
        return Err(Error::new(
            "not_allowed",
            "store database path is not a regular file",
        ));
    }
    let mut store = eliot_store::Store::open(&store_path)?;
    compact_store_with_preflight(&mut store, data_dir.path(), &store_path, before_vacuum)
}

fn compact_store_with_preflight(
    store: &mut eliot_store::Store,
    data_dir: &Path,
    store_path: &Path,
    mut before_vacuum: impl FnMut(&StoreCompactionSpaceEstimate),
) -> Result<StoreCompactionOutcome> {
    let estimate = match store.retention_space_status()?.auto_vacuum_mode {
        2 => None,
        _ => {
            let main_database_bytes = fs::metadata(store_path)
                .map_err(|error| {
                    Error::new(
                        "platform_io",
                        format!("cannot inspect Store size before compaction: {error}"),
                    )
                })?
                .len();
            let mut wal_name = store_path.as_os_str().to_os_string();
            wal_name.push("-wal");
            let wal_path = PathBuf::from(wal_name);
            let wal_bytes = match fs::metadata(&wal_path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => {
                    return Err(Error::new(
                        "platform_io",
                        format!("cannot inspect Store WAL before compaction: {error}"),
                    ));
                }
            };
            let database_and_wal_bytes = main_database_bytes
                .checked_add(wal_bytes)
                .ok_or_else(|| Error::new("invalid", "Store file size estimate overflowed"))?;
            let estimated_required_bytes = database_and_wal_bytes
                .checked_mul(2)
                .ok_or_else(|| Error::new("invalid", "Store compaction estimate overflowed"))?;
            let available_bytes = crate::platform::available_disk_space(data_dir)?;
            Some(StoreCompactionSpaceEstimate {
                main_database_bytes,
                wal_bytes,
                database_and_wal_bytes,
                estimated_required_bytes,
                available_bytes,
            })
        }
    };
    if let Some(estimate) = &estimate {
        before_vacuum(estimate);
        if estimate.available_bytes < estimate.estimated_required_bytes {
            return Err(Error::new(
                "insufficient_disk_space",
                format!(
                    "Store compaction estimates {} bytes required, but only {} bytes are available",
                    estimate.estimated_required_bytes, estimate.available_bytes
                ),
            ));
        }
    }
    let report = store.compact_to_incremental()?;
    Ok(StoreCompactionOutcome {
        estimate,
        converted: report.converted,
        pages_reclaimed: report
            .pages_before
            .saturating_sub(report.pages_after)
            .max(0) as u64,
        freelist_pages: report.freelist_pages.max(0) as u64,
    })
}

/// Open the private local daemon, recover durable state without replaying any
/// external effects, and serve concurrent local RPC connections.
pub async fn run(path: PathBuf) -> Result<()> {
    run_with_update(path, None).await
}

pub async fn run_for_update(path: PathBuf, attempt: &str) -> Result<()> {
    run_with_update(path, Some(attempt)).await
}

async fn run_with_update(path: PathBuf, attempt: Option<&str>) -> Result<()> {
    eliot_core::api::HarnessCapabilities::require_registered()?;

    // Daemon-private files carry explicit current-user-only creation modes
    // (0600 files, 0700 directories) at each creation site; the process umask
    // is left alone so Agents' files in shared workspaces keep the ambient
    // mask. Windows keeps its ACL checks.
    use crate::platform::data_dir::{repair_existing_managed_dir_acl, repair_startup_data_root};

    let mut removed = repair_startup_data_root(&path)?;
    let data_dir = DataDir::open(&path)?;
    let daemon_lock = data_dir.acquire_daemon_lock()?;
    data_dir.initialize_retention_config(&daemon_lock)?;
    if let Some(attempt) = attempt {
        let backup = crate::update_backup::update_backup_path(data_dir.path(), attempt)?;
        crate::platform::data_dir::validate_private_file(&backup)?;
    }
    removed.extend(crate::skill::materialize(data_dir.path())?);
    // The log must be initialized before the periodic checks can report.
    // Plugin and worktree parents can wait for the first live check.
    removed.extend(repair_existing_managed_dir_acl(
        &data_dir.path().join("logs"),
    )?);
    let _log = logging::init(&data_dir, &daemon_lock)?;
    let policy = Arc::new(SecretPolicy::new([data_dir.owner_token()]));
    let build_identity = crate::build_identity::current();
    let unavailable = "unavailable";
    logging::record(
        &policy,
        "daemon.build_identity",
        &[
            ("version", &build_identity.version),
            (
                "executable_directory",
                build_identity
                    .executable_directory
                    .as_deref()
                    .unwrap_or(unavailable),
            ),
            (
                "git_sha",
                build_identity.git_sha.as_deref().unwrap_or(unavailable),
            ),
            (
                "executable_sha256",
                build_identity
                    .executable_sha256
                    .as_deref()
                    .unwrap_or(unavailable),
            ),
        ],
        None,
    );
    // This start ends the Owner's previous stop: a later exit without a
    // new `daemon.shutdown` is a failure the desktop may restart.
    if let Err(error) = data_dir.clear_owner_stop(&daemon_lock) {
        logging::record(
            &policy,
            "daemon.owner_stop_clear_failed",
            &[
                ("producer", "daemon.startup"),
                ("method", "daemon.owner_stop.clear"),
            ],
            Some(&error),
        );
    }
    // `[daemon] log_level` sets process verbosity at startup; an invalid
    // value fails startup like invalid retention or mail config.
    let daemon_config = crate::config::load_daemon_config(data_dir.path()).map_err(|error| {
        let failure = startup_config_failure(error);
        logging::record(
            &policy,
            "daemon.failed",
            &[
                ("producer", "daemon.startup"),
                ("method", "daemon.config.load"),
            ],
            Some(&failure),
        );
        failure
    })?;
    crate::logging::set_log_level(crate::logging::LogLevel::parse(&daemon_config.log_level)?);
    logging::record(&policy, "daemon.starting", &[], None);
    if let Err(error) =
        crate::platform::data_dir::remove_abandoned_mcp_temps(&data_dir, &daemon_lock)
    {
        logging::record(
            &policy,
            "daemon.mcp_temp_cleanup_failed",
            &[
                ("producer", "daemon.startup"),
                ("method", "daemon.mcp_temp.cleanup"),
            ],
            Some(&error),
        );
    }
    let outcome = run_owned(
        &data_dir,
        &daemon_lock,
        Arc::clone(&policy),
        attempt,
        daemon_config,
        removed,
    )
    .await;
    logging::record(
        &policy,
        if outcome.is_ok() {
            "daemon.stopped"
        } else {
            "daemon.failed"
        },
        &[("producer", "daemon.process"), ("method", "daemon.run")],
        outcome.as_ref().err(),
    );
    outcome
}

async fn run_owned(
    data_dir: &DataDir,
    daemon_lock: &DaemonLock,
    policy: Arc<SecretPolicy>,
    attempt: Option<&str>,
    daemon_config: crate::config::DaemonConfig,
    removed: Vec<crate::platform::ForeignDataDirAce>,
) -> Result<()> {
    let runtime = initialize_runtime(data_dir, policy, daemon_config).await?;
    runtime
        .router
        .report_acl_repairs(&runtime.policy, removed)
        .await;
    let supervision = spawn_workers(data_dir, daemon_lock, runtime, attempt).await?;
    let (outcome, supervision) = supervise(supervision, data_dir.path()).await;
    shutdown_supervision(supervision, outcome).await
}

/// Validated startup state handed from initialization to the spawn phase: the
/// recovered store, configured router and policy, loaded configs, resolved
/// rule seeds, and the attention suspicion window. Nothing here is a live
/// worker or channel.
struct DaemonRuntime {
    store: Arc<Mutex<eliot_store::Store>>,
    router: Router,
    policy: Arc<SecretPolicy>,
    daemon_config: crate::config::DaemonConfig,
    automation_config: crate::config::AutomationConfig,
    cloud_mcp_config: Option<crate::cloud_mcp::CloudMcpConfig>,
    attention_config: crate::config::AttentionConfig,
    capacity: restart_capacity::Reconcile,
    capacity_retained: usize,
    global_rules: Vec<automation::ActiveRule>,
    initial_deferred_notice: automation::ReloadNoticeContext,
    project_rule_namespaces: Vec<automation::ProjectRuleNamespace>,
}

/// Live workers and channels owned by the supervision loop and released by the
/// shutdown drain. Task sets precede their shutdown senders so abort-on-drop
/// preserves ownership if normal draining cannot complete.
struct DaemonSupervision {
    connections: JoinSet<()>,
    progress: watch::Receiver<u64>,
    workers: JoinSet<()>,
    worker_stop: watch::Sender<bool>,
    // Never polled: holding the JoinSet keeps the recheck task alive, and its
    // abort-on-drop runs before the IPC listener below closes.
    _capacity_recheck: JoinSet<()>,
    recovery: JoinSet<(&'static str, Result<()>)>,
    monitoring: crate::monitor::Monitoring,
    monitor_tasks: JoinSet<()>,
    monitor_stop: watch::Sender<bool>,
    cloud_mcp_tasks: JoinSet<Result<()>>,
    cloud_mcp_shutdown: CancellationToken,
    listener: IpcListener,
    router: Router,
    policy: Arc<SecretPolicy>,
}

/// Validate configuration, recover durable state without replaying external
/// effects, reconcile restart capacity, and load rule seeds. Invalid config
/// fails startup exactly as before; no worker spawns here.
async fn initialize_runtime(
    data_dir: &DataDir,
    policy: Arc<SecretPolicy>,
    daemon_config: crate::config::DaemonConfig,
) -> Result<DaemonRuntime> {
    let store_path = data_dir.path().join("eliot.db");
    crate::platform::data_dir::validate_store_files(data_dir.path())?;
    logging::record(&policy, "daemon.store_opening", &[], None);
    let mut store = eliot_store::Store::open_with_observer(&store_path, |phase| match phase {
        eliot_store::StoreOpenPhase::ConnectionOpened => {
            logging::record(&policy, "daemon.store_connection_opened", &[], None);
        }
        eliot_store::StoreOpenPhase::PragmasApplied => {
            logging::record(&policy, "daemon.store_pragmas_applied", &[], None);
        }
        eliot_store::StoreOpenPhase::Migrated => {
            logging::record(&policy, "daemon.store_migrated", &[], None);
        }
    })?;
    let provider_scopes = crate::config::configured_provider_account_scopes(data_dir.path())
        .map_err(startup_config_failure)?;
    store.attribute_legacy_provider_holds(&provider_scopes)?;
    // Startup compaction is opt-in because converting an existing DB needs a
    // full VACUUM and temporary disk space. It shares the same preflight as
    // the offline owner command and runs before workers or IPC bind.
    if daemon_config.retention.compact_on_start {
        let report =
            compact_store_with_preflight(&mut store, data_dir.path(), &store_path, |estimate| {
                let main = estimate.main_database_bytes.to_string();
                let wal = estimate.wal_bytes.to_string();
                let required = estimate.estimated_required_bytes.to_string();
                let available = estimate.available_bytes.to_string();
                logging::record(
                    &policy,
                    "daemon.store_compaction_preflight",
                    &[
                        ("main_database_bytes", &main),
                        ("wal_bytes", &wal),
                        ("estimated_required_bytes", &required),
                        ("available_bytes", &available),
                    ],
                    None,
                );
            })?;
        logging::record_counts(
            &policy,
            if report.converted {
                "daemon.store_compaction_converted"
            } else {
                "daemon.store_compaction_already_incremental"
            },
            logging::Counts {
                events: 0,
                pruned_through: 0,
                commands: 0,
                writes: 0,
                mail: 0,
                digests: 0,
                processes: 0,
                pages_reclaimed: report.pages_reclaimed,
                freelist_pages: report.freelist_pages,
                auto_vacuum_mode: 2,
                wal_frames: 0,
                checkpointed_frames: 0,
                checkpoint_busy: 0,
            },
            None,
        );
    }
    // Startup phase markers: a readiness timeout names the phase that stalled.
    logging::record(&policy, "daemon.store_opened", &[], None);
    let capacity_root = data_dir.path().to_path_buf();
    store.set_capacity_policy(move || crate::config::load_scope_limits(&capacity_root));
    let store = std::sync::Arc::new(Mutex::new(store));
    // Settle crash-left pending settings history rows once at startup: reads
    // never reconcile, so this plus the save path cover every pending row.
    {
        let mut store = store.lock().await;
        crate::config::settings::reconcile_at_startup(data_dir.path(), &mut store)?;
    }
    let bus = Bus::new(1_024);
    let daemon_config =
        crate::config::load_daemon_config(data_dir.path()).map_err(startup_config_failure)?;
    // Invalid `[mail]` fails startup like invalid retention; the Router keeps
    // the loaded limits for the run instead of re-reading the file per send.
    let mail_config =
        crate::config::load_mail_config(data_dir.path()).map_err(startup_config_failure)?;
    let router = Router::new(
        data_dir.path().to_path_buf(),
        data_dir.owner_token().to_owned(),
        std::sync::Arc::clone(&store),
        bus.clone(),
        mail_config,
        daemon_config.attention,
        daemon_config.background_wake_max_wait_secs * 1_000,
    );

    crate::bootstrap::ensure_hot_start(data_dir.path(), &store, &policy).await?;

    // A previous run's panic is reported once: in the log as
    // `daemon.panicked` (with when, where and which code in ids), in
    // `daemon.status`, and in `swarm daemon` startup output. Markers are
    // consumed on read, so a later restart does not repeat the report.
    // A WMI-started daemon has no console, so this file is the durable path.
    if let Some(previous) = crate::crash::load_previous_crash(data_dir.path(), &policy) {
        let when = previous.timestamp;
        let summary = previous.summary();
        crate::logging::record(
            &policy,
            "daemon.panicked",
            &[
                ("when", &when.to_string()),
                ("location", previous.location.as_str()),
                ("thread", previous.thread.as_str()),
            ],
            Some(
                &eliot_core::Error::new("daemon.panicked", summary)
                    .with_where("daemon.runtime.panic")
                    .with_origin(
                        eliot_core::FailureOrigin::Swarm,
                        "previous daemon run panicked before shutdown",
                    ),
            ),
        );
        router.set_previous_crash(Some(previous));
    }
    let recovered = {
        let mut store = store.lock().await;
        let events = store.recover_on_restart(now_ms())?;
        store.inference_recover()?;
        bus.publish(&events);
        events
    };
    router.note_events(&recovered);
    router.reconcile_git_integrations(None).await?;
    logging::record(&policy, "daemon.recovered", &[], None);
    let capacity = restart_capacity::Reconcile {
        store: Arc::clone(&store),
        bus: bus.clone(),
        router: router.clone(),
        policy: Arc::clone(&policy),
        on_restart: daemon_config.on_restart,
    };
    let report = restart_capacity::reconcile_first_pass(&capacity).await?;
    logging::record(&policy, "daemon.capacity_reconciled", &[], None);
    logging::record_debug(
        &policy,
        "daemon.capacity_reconciled",
        &[],
        &[
            ("processed", report.processed as u64),
            ("batches", report.batches as u64),
            ("max_batch", report.max_batch as u64),
            ("retained", report.retained as u64),
        ],
    );
    let automation_config =
        crate::config::load_automation_config(data_dir.path()).map_err(startup_config_failure)?;
    let cloud_mcp_config =
        crate::config::load_cloud_mcp_config(data_dir.path()).map_err(startup_config_failure)?;
    let attention_config = router.attention_config();
    // Install [daemon] observe thresholds once: monitor sweeps and Router
    // readbacks share them; the daemon never re-reads the file per sweep.
    router.set_observe_thresholds(crate::monitor::ObserveThresholds {
        heavy: daemon_config.heavy_thresholds()?,
        growth: daemon_config.session_growth_thresholds()?,
    });
    // Install `[daemon.limits]` once, before IPC binds: every launch and
    // selector admission in this run checks these bounds.
    {
        let mut store = store.lock().await;
        store.set_hierarchy_limits(daemon_config.limits)?;
    }
    // Rule initialization carries nested reconciliation state. Keep it off the
    // startup stack, which also has to deserialize populated project config.
    let (global_rules, initial_deferred_notice) = Box::pin(automation::initialize_global_rules(
        &router,
        data_dir.path(),
        &policy,
    ))
    .await?;
    // A configured namespace adds nested Git and Store futures during
    // restart. Construct them on a runtime worker's stack.
    let project_router = router.clone();
    let project_root = data_dir.path().to_owned();
    let project_policy = Arc::clone(&policy);
    let project_rule_namespaces = tokio::spawn(async move {
        Box::pin(automation::initialize_project_rules(
            &project_router,
            &project_root,
            &project_policy,
        ))
        .await
    })
    .await
    .map_err(|_| Error::new("internal", "project rule startup task did not finish"))??;
    logging::record(&policy, "daemon.rules_loaded", &[], None);
    Ok(DaemonRuntime {
        store,
        router,
        policy,
        capacity,
        capacity_retained: report.retained,
        daemon_config,
        automation_config,
        cloud_mcp_config,
        attention_config,
        global_rules,
        initial_deferred_notice,
        project_rule_namespaces,
    })
}

/// Bind IPC, publish readiness, and spawn every background worker. The
/// returned supervision owns all live tasks until the shutdown drain.
async fn spawn_workers(
    data_dir: &DataDir,
    daemon_lock: &DaemonLock,
    runtime: DaemonRuntime,
    attempt: Option<&str>,
) -> Result<DaemonSupervision> {
    let DaemonRuntime {
        store,
        router,
        policy,
        capacity,
        capacity_retained,
        daemon_config,
        automation_config,
        cloud_mcp_config,
        attention_config,
        global_rules,
        initial_deferred_notice,
        project_rule_namespaces,
    } = runtime;
    let listener = IpcListener::bind(data_dir, daemon_lock).await?;
    logging::record(&policy, "daemon.ipc_bound", &[], None);
    let info = {
        let _publication = attempt
            .map(|id| crate::update_backup::lock_update_publication(data_dir.path(), id))
            .transpose()?;
        // This durable endpoint change is the admission boundary. Boot
        // recovery workers, automation and RPC handling all follow it.
        data_dir.publish_daemon(
            daemon_lock,
            listener.endpoint(),
            env!("ELIOT_BUILD_VERSION"),
        )?
    };
    logging::record(&policy, "daemon.ready", &[("boot_id", &info.boot_id)], None);

    let cloud_mcp_shutdown = CancellationToken::new();
    let mut cloud_mcp_tasks = JoinSet::new();
    if let Some(config) = cloud_mcp_config {
        logging::record(&policy, "daemon.cloud_mcp_starting", &[], None);
        let data_root = data_dir.path().to_path_buf();
        let shutdown = cloud_mcp_shutdown.clone();
        cloud_mcp_tasks
            .spawn(async move { crate::cloud_mcp::run(config, &data_root, shutdown).await });
    }

    let (monitor_stop, monitor_shutdown) = watch::channel(false);
    let mut monitor_tasks = JoinSet::new();
    let monitoring = router.monitoring();
    let worker_reporter =
        crate::daemon::workers::WorkerReporter::new(router.clone(), Arc::clone(&policy));
    let monitor_instance = monitoring.clone();
    let monitor_failure_state = monitoring.clone();
    let monitor_reporter = worker_reporter.clone();
    let monitor_factory = move |shutdown: watch::Receiver<bool>| {
        let monitoring = monitor_instance.clone();
        let reporter = monitor_reporter.clone();
        async move {
            let result = monitoring.run(shutdown.clone(), reporter).await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    monitor_tasks.spawn(crate::daemon::workers::supervise_with(
        "monitor",
        worker_reporter.clone(),
        monitor_shutdown.clone(),
        monitor_factory,
        move |error| monitor_failure_state.mark_unavailable(&error.code),
    ));
    let git_monitoring = monitoring.clone();
    let git_failure_state = monitoring.clone();
    let git_reporter = worker_reporter.clone();
    monitor_tasks.spawn(crate::daemon::workers::supervise_with(
        "git_sampler",
        worker_reporter.clone(),
        monitor_shutdown,
        move |shutdown: watch::Receiver<bool>| {
            let monitoring = git_monitoring.clone();
            let reporter = git_reporter.clone();
            async move {
                let result = monitoring.run_git(shutdown.clone(), reporter).await;
                crate::daemon::workers::from_result(result, &shutdown)
            }
        },
        move |error| git_failure_state.mark_git_unavailable(&error.code),
    ));

    let (worker_stop, _) = watch::channel(false);
    let mut workers = JoinSet::new();
    let attention_router = router.clone();
    let attention_reporter = worker_reporter.clone();
    let attention_factory = move |shutdown: watch::Receiver<bool>| {
        let router = attention_router.clone();
        let reporter = attention_reporter.clone();
        async move {
            let result =
                crate::router::run_attention(router, attention_config, shutdown.clone(), reporter)
                    .await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "attention",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        attention_factory,
    ));
    let tool_router = router.clone();
    let tool_reporter = worker_reporter.clone();
    let tool_failure_factory = move |shutdown: watch::Receiver<bool>| {
        let router = tool_router.clone();
        let reporter = tool_reporter.clone();
        async move {
            let result =
                crate::router::run_tool_failure_attention(router, shutdown.clone(), reporter).await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "tool_failure_attention",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        tool_failure_factory,
    ));
    let context_router = router.clone();
    workers.spawn(crate::daemon::workers::supervise(
        "context_notes",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        move |shutdown: watch::Receiver<bool>| {
            let router = context_router.clone();
            async move {
                let result = crate::router::run_context_notes(router, shutdown.clone()).await;
                crate::daemon::workers::from_result(result, &shutdown)
            }
        },
    ));
    let mail_router = router.clone();
    let mail_reporter = worker_reporter.clone();
    let mail_factory = move |shutdown: watch::Receiver<bool>| {
        let router = mail_router.clone();
        let reporter = mail_reporter.clone();
        async move {
            let result = crate::router::run_mail_reoffer(router, shutdown.clone(), reporter).await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "mail_reoffer",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        mail_factory,
    ));
    let wake_router = router.clone();
    let wake_reporter = worker_reporter.clone();
    let wake_factory = move |shutdown: watch::Receiver<bool>| {
        let router = wake_router.clone();
        let reporter = wake_reporter.clone();
        async move {
            let result = crate::router::run_agent_wakes(router, shutdown.clone(), reporter).await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "agent_wake",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        wake_factory,
    ));
    let (restart_ready, digest_restart_ready) = watch::channel(false);
    let digest_router = router.clone();
    let digest_reporter = worker_reporter.clone();
    let digest_factory = move |shutdown: watch::Receiver<bool>| {
        let router = digest_router.clone();
        let reporter = digest_reporter.clone();
        let restart_ready = digest_restart_ready.clone();
        async move {
            let result = crate::router::run_completion_digest(
                router,
                shutdown.clone(),
                reporter,
                restart_ready,
            )
            .await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "completion_digest",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        digest_factory,
    ));
    let mut recovery = JoinSet::new();
    let recovery_router = router.clone();
    recovery.spawn(async move { ("workspace", recovery_router.recover_workspaces().await) });
    // Owned by supervision; its abort-on-drop runs before the IPC listener
    // closes because the struct declares this field ahead of the listener.
    let mut capacity_recheck = JoinSet::new();
    // Serialize the initial resume pass and later custody passes so their
    // batches cannot add to each other's concurrency limit.
    capacity_recheck.spawn(async move {
        if let Err(error) = capacity.router.recover_accepted_commands().await {
            logging::record(
                &capacity.policy,
                "daemon.command_recovery_failed",
                &[
                    ("producer", "daemon.recovery"),
                    ("method", "commands.recover"),
                ],
                Some(&error),
            );
        }
        if let Err(error) = capacity
            .router
            .recover_restart_agents(capacity.on_restart)
            .await
        {
            logging::record(
                &capacity.policy,
                "daemon.restart_agent_recovery_failed",
                &[
                    ("producer", "daemon.recovery"),
                    ("method", "agents.restart_recover"),
                ],
                Some(&error),
            );
        }
        restart_ready.send_replace(true);
        if capacity_retained > 0 {
            restart_capacity::recheck(capacity).await;
        }
    });
    let initial_rule_start = Arc::new(Mutex::new(Some(automation::RuleWorkerStart {
        global_rules,
        project_rules: project_rule_namespaces,
        max_event_backlog: automation_config.max_event_backlog,
        data_root: data_dir.path().to_path_buf(),
        policy: Arc::clone(&policy),
        initial_notices: initial_deferred_notice,
    })));
    let rule_router = router.clone();
    let rule_data_root = data_dir.path().to_path_buf();
    let rule_policy = Arc::clone(&policy);
    let rule_start = Arc::clone(&initial_rule_start);
    let rule_factory = move |shutdown: watch::Receiver<bool>| {
        let router = rule_router.clone();
        let data_root = rule_data_root.clone();
        let policy = Arc::clone(&rule_policy);
        let first_start = Arc::clone(&rule_start);
        async move {
            let initial = first_start.lock().await.take();
            let start = match initial {
                Some(start) => start,
                None => {
                    let (global_rules, initial_notices) =
                        match automation::initialize_global_rules(&router, &data_root, &policy)
                            .await
                        {
                            Ok(initialized) => initialized,
                            Err(error) => {
                                return crate::daemon::workers::WorkerOutcome::Failed(error);
                            }
                        };
                    let project_rules =
                        match automation::initialize_project_rules(&router, &data_root, &policy)
                            .await
                        {
                            Ok(rules) => rules,
                            Err(error) => {
                                return crate::daemon::workers::WorkerOutcome::Failed(error);
                            }
                        };
                    let automation_config = match crate::config::load_automation_config(&data_root)
                    {
                        Ok(config) => config,
                        Err(error) => {
                            return crate::daemon::workers::WorkerOutcome::Failed(error);
                        }
                    };
                    automation::RuleWorkerStart {
                        global_rules,
                        project_rules,
                        max_event_backlog: automation_config.max_event_backlog,
                        data_root: data_root.clone(),
                        policy: Arc::clone(&policy),
                        initial_notices,
                    }
                }
            };
            match automation::run_global_rules(router, start, shutdown).await {
                Ok(automation::RuleWorkerExit::Shutdown) => {
                    crate::daemon::workers::WorkerOutcome::Stopped
                }
                Ok(automation::RuleWorkerExit::Exited) => {
                    crate::daemon::workers::WorkerOutcome::Failed(Error::new(
                        "worker_exited",
                        "rule worker exited unexpectedly",
                    ))
                }
                Err(error) => crate::daemon::workers::WorkerOutcome::Failed(error),
            }
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "rule_worker",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        rule_factory,
    ));

    let goal_router = router.clone();
    let goal_factory = move |shutdown: watch::Receiver<bool>| {
        let router = goal_router.clone();
        async move {
            let result = crate::router::run_goal_driver(
                router,
                crate::router::real_goal_check,
                shutdown.clone(),
            )
            .await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "goal_driver",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        goal_factory,
    ));

    let project_check_router = router.clone();
    let project_check_factory = move |shutdown: watch::Receiver<bool>| {
        let router = project_check_router.clone();
        async move {
            let result = crate::router::run_project_checks(router, shutdown.clone()).await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "project_checks",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        project_check_factory,
    ));

    let switch_router = router.clone();
    let switch_reporter = worker_reporter.clone();
    workers.spawn(crate::daemon::workers::supervise(
        "provider_switch",
        worker_reporter.clone(),
        worker_stop.subscribe(),
        move |shutdown| {
            let router = switch_router.clone();
            let events = router.subscribe_rule_events();
            let reporter = switch_reporter.clone();
            async move {
                reporter.running("provider_switch");
                let result = router.run_provider_switches(events, shutdown.clone()).await;
                crate::daemon::workers::from_result(result, &shutdown)
            }
        },
    ));

    let maintenance_router = router.clone();
    let initial_maintenance_config = Arc::new(Mutex::new(Some(daemon_config)));
    let maintenance_store = Arc::clone(&store);
    let maintenance_root = data_dir.path().to_path_buf();
    let maintenance_policy = Arc::clone(&policy);
    let maintenance_reporter = worker_reporter.clone();
    let maintenance_initial = Arc::clone(&initial_maintenance_config);
    let maintenance_factory = move |shutdown: watch::Receiver<bool>| {
        let store = Arc::clone(&maintenance_store);
        let data_root = maintenance_root.clone();
        let policy = Arc::clone(&maintenance_policy);
        let reporter = maintenance_reporter.clone();
        let initial = Arc::clone(&maintenance_initial);
        let router = maintenance_router.clone();
        async move {
            let config = match initial.lock().await.take() {
                Some(config) => config,
                None => match crate::config::load_daemon_config(&data_root) {
                    Ok(config) => config,
                    Err(error) => return crate::daemon::workers::WorkerOutcome::Failed(error),
                },
            };
            let result = maintenance::run(
                store,
                data_root,
                policy,
                config,
                router,
                shutdown.clone(),
                reporter,
            )
            .await;
            crate::daemon::workers::from_result(result, &shutdown)
        }
    };
    workers.spawn(crate::daemon::workers::supervise(
        "maintenance",
        worker_reporter,
        worker_stop.subscribe(),
        maintenance_factory,
    ));
    let progress = router.progress_receiver();
    let connections = JoinSet::new();
    Ok(DaemonSupervision {
        connections,
        progress,
        workers,
        worker_stop,
        _capacity_recheck: capacity_recheck,
        recovery,
        monitoring,
        monitor_tasks,
        monitor_stop,
        cloud_mcp_tasks,
        cloud_mcp_shutdown,
        listener,
        router,
        policy,
    })
}

struct AclCheck {
    path: PathBuf,
    due: Instant,
    last_problem: Instant,
    delay: Duration,
}

impl AclCheck {
    fn new(path: PathBuf) -> Self {
        let now = Instant::now();
        Self {
            path,
            due: now,
            last_problem: now,
            delay: Duration::from_secs(1),
        }
    }

    /// Defer retries for this path and report when the backoff advances.
    fn defer(&mut self) -> bool {
        let now = Instant::now();
        let previous = if now.duration_since(self.last_problem) >= Duration::from_secs(60) {
            Duration::from_secs(1)
        } else {
            self.delay
        };
        self.delay = (previous * 2).min(Duration::from_secs(30));
        self.due = now + self.delay;
        self.last_problem = now;
        self.delay != previous
    }
}

/// Drive the accept loop and worker supervision. Returns the loop outcome with
/// supervision intact so the shutdown drain releases workers in order.
async fn supervise(
    mut supervision: DaemonSupervision,
    data_root: &Path,
) -> (Result<()>, DaemonSupervision) {
    let DaemonSupervision {
        router,
        policy,
        listener,
        workers,
        monitor_tasks,
        recovery,
        cloud_mcp_tasks,
        progress,
        connections,
        ..
    } = &mut supervision;
    let router = router.clone();
    let policy = Arc::clone(policy);
    let mut accept_failures = 0_u32;
    let mut accept_retry_at: Option<Instant> = None;
    let mut acl_tick = tokio::time::interval(Duration::from_secs(1));
    acl_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut acl_checks: Vec<_> = std::iter::once(data_root.to_path_buf())
        .chain(MANAGED_ACL_DIRECTORIES.map(|name| data_root.join(name)))
        .map(AclCheck::new)
        .collect();
    let outcome = 'serve: loop {
        match router.should_exit().await {
            Ok(true) => break Ok(()),
            Ok(false) => {}
            Err(error) => break Err(error),
        }
        tokio::select! {
            _ = acl_tick.tick(), if cfg!(windows) => {
                for check in &mut acl_checks {
                    if Instant::now() < check.due {
                        continue;
                    }
                    let root = check.path.as_path() == data_root;
                    let repaired = if root {
                        crate::platform::data_dir::repair_live_data_root(&check.path)
                    } else {
                        crate::platform::data_dir::repair_existing_managed_dir_acl(&check.path)
                    };
                    match repaired {
                        Ok(removed) => {
                            if router.report_acl_repairs(&policy, removed).await
                                && check.defer()
                            {
                                let operation = format!("Recurring grant. Next ACL check in {} s", check.delay.as_secs());
                                logging::record(
                                    &policy,
                                    "daemon.data_dir_acl_backoff",
                                    &[("path", &check.path.to_string_lossy()), ("operation", &operation)],
                                    None,
                                );
                            }
                        }
                        Err(error) => {
                            logging::record(
                                &policy,
                                "daemon.data_dir_acl_check_failed",
                                &[("path", &check.path.to_string_lossy())],
                                Some(&error),
                            );
                            if root && crate::platform::data_dir::unrepairable_root_acl(&error) {
                                break 'serve Err(error);
                            }
                            check.defer();
                        }
                    }
                }
            }
            accepted = listener.accept(), if accept_retry_at.is_none() => {
                match accepted {
                    Ok(stream) => {
                        accept_failures = 0;
                        if connections.len() < MAX_CLIENTS {
                            let router = router.clone();
                            let policy = Arc::clone(&policy);
                            connections.spawn(async move {
                                serve_connection(stream, router, policy).await;
                            });
                        } else {
                            // At the connection limit the request was never read or routed.
                            tokio::spawn(reply_overloaded(stream));
                        }
                    }
                    Err(error) => {
                        if router.is_force_requested() {
                            break Ok(());
                        }
                        accept_failures = accept_failures.saturating_add(1);
                        if accept_failures >= ACCEPT_FAILURE_LIMIT {
                            break Err(report_accept_exhausted(&router, &policy, accept_failures, &error).await);
                        }
                        accept_retry_at = Some(note_accept_retry(&policy, accept_failures, &error));
                    }
                }
            }
            _ = async {
                if let Some(retry_at) = accept_retry_at {
                    sleep_until(retry_at).await;
                }
            }, if accept_retry_at.is_some() => {
                accept_retry_at = None;
            }
            changed = progress.changed() => {
                if changed.is_err() {
                    break Err(Error::new("internal", "daemon progress channel closed"));
                }
            }
            joined = workers.join_next(), if !workers.is_empty() => {
                if matches!(joined, Some(Err(_))) {
                    let error = Error::new(
                        "worker_supervisor_failed",
                        "daemon worker supervisor observed a panicked or cancelled task",
                    );
                    logging::record(
                        &policy,
                        "daemon.worker_supervisor_failed",
                        &[
                            ("producer", "daemon.worker_supervisor"),
                            ("method", "worker.join"),
                        ],
                        Some(&error),
                    );
                    break Err(Error::new(
                        "worker_supervisor_failed",
                        "daemon worker supervisor stopped unexpectedly",
                    ));
                }
            }
            joined = monitor_tasks.join_next(), if !monitor_tasks.is_empty() => {
                if matches!(joined, Some(Err(_))) {
                    let error = Error::new(
                        "monitor_supervisor_failed",
                        "monitor supervisor observed a panicked or cancelled task",
                    );
                    logging::record(
                        &policy,
                        "daemon.monitor_supervisor_failed",
                        &[
                            ("producer", "daemon.monitor_supervisor"),
                            ("method", "monitor.join"),
                        ],
                        Some(&error),
                    );
                    break Err(Error::new(
                        "monitor_supervisor_failed",
                        "monitor supervisor stopped unexpectedly",
                    ));
                }
            }
            joined = recovery.join_next(), if !recovery.is_empty() => {
                match joined {
                    Some(Ok((_, Ok(())))) => {}
                    Some(Ok((_, Err(error)))) => {
                        logging::record(
                            &policy,
                            "daemon.workspace_recovery_failed",
                            &[
                                ("producer", "daemon.workspace_recovery"),
                                ("method", "workspace.recover"),
                            ],
                            Some(&error),
                        );
                        router.note_workspace_recovery_failure(&error.code);
                    }
                    _ => {
                        let error = Error::new(
                            "workspace_recovery_task_failed",
                            "workspace recovery task panicked or was cancelled",
                        );
                        logging::record(
                            &policy,
                            "daemon.workspace_recovery_task_failed",
                            &[
                                ("producer", "daemon.workspace_recovery"),
                                ("method", "workspace.recover.join"),
                            ],
                            Some(&error),
                        );
                        router.note_workspace_recovery_failure("workspace_recovery_task_failed");
                    }
                }
            }
            joined = cloud_mcp_tasks.join_next(), if !cloud_mcp_tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) => {
                        let error = Error::new(
                            "cloud_mcp_stopped",
                            "cloud MCP service exited unexpectedly",
                        );
                        logging::record(
                            &policy,
                            "daemon.cloud_mcp_stopped_unexpectedly",
                            &[
                                ("producer", "daemon.cloud_mcp"),
                                ("method", "cloud_mcp.serve"),
                            ],
                            Some(&error),
                        );
                    }
                    Some(Ok(Err(error))) => {
                        logging::record(
                            &policy,
                            "daemon.cloud_mcp_failed",
                            &[
                                ("producer", "daemon.cloud_mcp"),
                                ("method", "cloud_mcp.serve"),
                            ],
                            Some(&error),
                        );
                    }
                    Some(Err(_)) => {
                        let error = Error::new(
                            "cloud_mcp_task_failed",
                            "cloud MCP service task panicked or was cancelled",
                        );
                        logging::record(
                            &policy,
                            "daemon.cloud_mcp_task_failed",
                            &[
                                ("producer", "daemon.cloud_mcp"),
                                ("method", "cloud_mcp.serve.join"),
                            ],
                            Some(&error),
                        );
                    }
                    None => {}
                }
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if matches!(joined, Some(Err(_))) {
                    let error = Error::new(
                        "connection_task_failed",
                        "RPC connection task panicked or was cancelled",
                    );
                    logging::record(
                        &policy,
                        "daemon.connection_task_failed",
                        &[
                            ("producer", "daemon.ipc_connection"),
                            ("method", "rpc.connection.join"),
                        ],
                        Some(&error),
                    );
                }
                let _ = joined;
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    break Err(Error::new(
                        "platform_io",
                        format!("listen for shutdown signal: {error}"),
                    ));
                }
                logging::record(&policy, "daemon.force_requested", &[], None);
                router.request_force().await;
            }
        }
    };
    (outcome, supervision)
}

fn note_accept_retry(policy: &SecretPolicy, attempt: u32, error: &Error) -> Instant {
    let delay = crate::daemon::workers::retry_backoff(attempt);
    let attempt = attempt.to_string();
    let retry_after_ms = delay.as_millis().to_string();
    logging::record(
        policy,
        "daemon.ipc_accept_retry",
        &[
            ("producer", "daemon.ipc_listener"),
            ("method", "ipc.accept"),
            ("attempt", &attempt),
            ("retry_after_ms", &retry_after_ms),
        ],
        Some(error),
    );
    Instant::now() + delay
}

async fn report_accept_exhausted(
    router: &Router,
    policy: &SecretPolicy,
    failures: u32,
    error: &Error,
) -> Error {
    let attempt = failures.to_string();
    let fields = [
        ("producer", "daemon.ipc_listener"),
        ("method", "ipc.accept"),
        ("attempt", attempt.as_str()),
    ];
    logging::record(policy, "daemon.ipc_accept_attention", &fields, Some(error));
    let message = format!(
        "The local IPC listener failed {failures} consecutive times. Swarm is exiting. Check the daemon log for the last accept error."
    );
    if let Err(error) = router.report_daemon_attention(message.clone()).await {
        logging::record(
            policy,
            "daemon.attention_delivery_failed",
            &[
                ("producer", "daemon.attention"),
                ("method", "attention.deliver"),
                ("attempt", fields[2].1),
            ],
            Some(&error),
        );
    }
    Error::new("ipc_accept_exhausted", message)
        .with_where("daemon.ipc.accept")
        .with_origin(
            FailureOrigin::Swarm,
            "consecutive local IPC accept failures reached the retry limit",
        )
}

/// Drain workers in dependency order after the supervision loop ends. Task
/// sets still owned here abort on drop, matching the previous inline drain at
/// the end of `run_owned`.
async fn shutdown_supervision(
    mut supervision: DaemonSupervision,
    outcome: Result<()>,
) -> Result<()> {
    let DaemonSupervision {
        router,
        policy,
        workers,
        worker_stop,
        monitor_stop,
        monitor_tasks,
        monitoring,
        cloud_mcp_shutdown,
        cloud_mcp_tasks,
        recovery,
        connections,
        ..
    } = &mut supervision;
    let router = router.clone();
    let policy = Arc::clone(policy);
    if outcome.is_err() {
        router.request_force().await;
    }
    router.finish_var_writes().await;
    cloud_mcp_shutdown.cancel();
    while let Some(joined) = cloud_mcp_tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                logging::record(
                    &policy,
                    "daemon.cloud_mcp_failed",
                    &[
                        ("producer", "daemon.cloud_mcp"),
                        ("method", "cloud_mcp.serve"),
                    ],
                    Some(&error),
                );
            }
            Err(_) => {
                let error = Error::new(
                    "cloud_mcp_task_failed",
                    "cloud MCP service task panicked or was cancelled",
                );
                logging::record(
                    &policy,
                    "daemon.cloud_mcp_task_failed",
                    &[
                        ("producer", "daemon.cloud_mcp"),
                        ("method", "cloud_mcp.serve.join"),
                    ],
                    Some(&error),
                );
            }
        }
    }
    worker_stop.send_replace(true);
    while let Some(joined) = workers.join_next().await {
        if joined.is_err() {
            let error = Error::new(
                "worker_supervisor_failed",
                "daemon worker supervisor observed a panicked or cancelled task",
            );
            logging::record(
                &policy,
                "daemon.worker_supervisor_failed",
                &[
                    ("producer", "daemon.worker_supervisor"),
                    ("method", "worker.join"),
                ],
                Some(&error),
            );
        }
    }
    if router.is_force_requested() {
        router.force_owned_processes().await;
    } else if router.is_restart_requested() {
        router.stop_for_restart().await;
    }
    recovery.abort_all();
    while recovery.join_next().await.is_some() {}
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    monitor_stop.send_replace(true);
    let shutdown = timeout(MONITOR_SHUTDOWN_GRACE, async {
        while let Some(joined) = monitor_tasks.join_next().await {
            if joined.is_err() {
                let error = Error::new(
                    "monitor_supervisor_failed",
                    "monitor supervisor observed a panicked or cancelled task",
                );
                logging::record(
                    &policy,
                    "daemon.monitor_supervisor_failed",
                    &[
                        ("producer", "daemon.monitor_supervisor"),
                        ("method", "monitor.join"),
                    ],
                    Some(&error),
                );
            }
        }
    })
    .await;
    if shutdown.is_err() {
        monitor_tasks.shutdown().await;
        let error = Error::new("monitor_shutdown_incomplete", "monitor shutdown timed out");
        monitoring.mark_git_unavailable(&error.code);
        logging::record(
            &policy,
            "daemon.monitor_shutdown_incomplete",
            &[],
            Some(&error),
        );
    }
    monitoring.finish_git().await;
    outcome
}

/// Make one authenticated local JSON-RPC call. Clients open only an existing
/// private root; reads never initialize credentials, a database, or a daemon.
pub async fn client_call(path: &Path, method: &str, params: Value) -> Result<Value> {
    validate_client_call(method, &params)?;
    let identity = caller_identity_for_target(path)?;
    client_call_with_identity(path, method, params, identity).await
}

/// Require the inherited identity to be a complete Agent identity scoped to
/// `target`. This validates only local role context and never opens the data
/// directory or reads the Owner token; the Router remains authoritative for
/// generation. An Agent identity issued by another data directory is rejected
/// here instead of falling back to Owner authority.
pub fn require_agent_identity(target: &Path) -> Result<()> {
    required_agent_identity_for(target).map(|_| ())
}

/// Make one local JSON-RPC call using only a complete inherited Agent identity
/// scoped to `path`. Owner and Run contexts are rejected before any Owner
/// credential lookup, as is an Agent identity issued by another data
/// directory: this path never sends a foreign token and never falls back to
/// Owner authority.
pub async fn client_call_with_agent_identity(
    path: &Path,
    method: &str,
    params: Value,
) -> Result<Value> {
    validate_client_call(method, &params)?;
    let identity = required_agent_identity_for(path)?;
    client_call_with_identity(path, method, params, identity).await
}

/// Make one Agent-authenticated daemon RPC from the shared Swarm MCP tool boundary.
/// The call id is private provenance metadata; RPC params remain unchanged.
pub async fn client_call_with_agent_mcp_tool(
    path: &Path,
    method: &str,
    params: Value,
    call_id: &str,
) -> Result<Value> {
    validate_client_call(method, &params)?;
    if Ulid::from_string(call_id).is_err() {
        return Err(Error::new("invalid", "Swarm MCP call id is invalid"));
    }
    let identity = required_agent_identity_for(path)?;
    client_call_with_identity_measured(path, method, params, identity, None, Some(call_id)).await
}

fn required_agent_identity_for(target: &Path) -> Result<ClientIdentity> {
    match caller_identity_for_target(target)? {
        identity @ ClientIdentity::Agent { .. } => Ok(identity),
        ClientIdentity::Owner | ClientIdentity::Run { .. } => Err(Error::new(
            "not_allowed",
            "a complete Agent identity is required; Owner and Run contexts are rejected",
        )),
    }
}

async fn client_call_with_identity(
    path: &Path,
    method: &str,
    params: Value,
    identity: ClientIdentity,
) -> Result<Value> {
    client_call_with_identity_measured(path, method, params, identity, None, None).await
}

async fn client_call_with_identity_measured(
    path: &Path,
    method: &str,
    params: Value,
    identity: ClientIdentity,
    timings: Option<&mut CallTimings>,
    swarm_mcp_call_id: Option<&str>,
) -> Result<Value> {
    let data_dir = crate::startup::open_client_data_dir(path)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let info = data_dir
        .read_daemon_info()
        .map_err(|error| with_connect_hint(path, error))?
        .ok_or_else(|| {
            with_connect_hint(path, Error::new("unavailable", "daemon is not running"))
        })?;
    let mut auth = match identity {
        ClientIdentity::Owner => {
            let token = data_dir.read_owner_token()?;
            json!({
                "kind": "owner",
                "credential": token,
            })
        }
        ClientIdentity::Agent {
            id,
            generation,
            token,
        } => json!({
            "kind": "agent",
            "agent_id": id,
            "generation": generation,
            "credential": token,
        }),
        ClientIdentity::Run {
            run_command_id,
            token,
        } => json!({
            "kind": "run",
            "run_command_id": run_command_id,
            "credential": token,
        }),
    };
    if let Some(call_id) = swarm_mcp_call_id {
        auth["swarm_mcp_call_id"] = json!(call_id);
    }
    let credential = auth["credential"].as_str().unwrap_or("");
    let policy = SecretPolicy::new([credential]);
    raw_client_call_measured(&info.endpoint, method, params, auth, &policy, timings)
        .await
        .map_err(|error| with_connect_hint(path, error))
}

fn validate_client_call(method: &str, params: &Value) -> Result<()> {
    if method.trim().is_empty() || !params.is_object() {
        return Err(Error::new(
            "invalid",
            "daemon RPC requires a method and object params",
        ));
    }
    Ok(())
}

/// Make one JSON-RPC call using an explicitly supplied owner credential.
///
/// Discovers the validated daemon endpoint without inspecting ambient role
/// credentials or reading owner.token from disk.
pub async fn client_call_with_owner_credential(
    path: &Path,
    method: &str,
    params: Value,
    owner_credential: &str,
) -> Result<Value> {
    if method.trim().is_empty() || !params.is_object() {
        return Err(Error::new(
            "invalid",
            "daemon RPC requires a method and object params",
        ));
    }
    let data_dir = crate::startup::open_client_data_dir(path)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let info = data_dir
        .read_daemon_info()
        .map_err(|error| with_connect_hint(path, error))?
        .ok_or_else(|| {
            with_connect_hint(path, Error::new("unavailable", "daemon is not running"))
        })?;
    let auth = json!({
        "kind": "owner",
        "credential": owner_credential,
    });
    let policy = SecretPolicy::new([owner_credential]);
    raw_client_call(&info.endpoint, method, params, auth, &policy)
        .await
        .map_err(|error| with_connect_hint(path, error))
}

/// HTTP bridge only: signed identity has already been checked at the origin.
/// The private Owner credential authenticates this context to the local Router.
pub async fn client_call_with_remote_context(
    path: &Path,
    method: &str,
    params: Value,
    remote: &eliot_core::RemoteCallContext,
) -> Result<Value> {
    remote.validate()?;
    if method.trim().is_empty() || !params.is_object() {
        return Err(Error::new("invalid", "invalid remote RPC"));
    }
    let data_dir = crate::startup::open_client_data_dir(path)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let info = data_dir
        .read_daemon_info()
        .map_err(|error| with_connect_hint(path, error))?
        .ok_or_else(|| {
            with_connect_hint(path, Error::new("unavailable", "daemon is not running"))
        })?;
    let token = data_dir
        .read_owner_token()
        .map_err(|error| with_connect_hint(path, error))?;
    let policy = SecretPolicy::new([token.as_str()]);
    let auth = json!({"kind":"owner", "credential": token, "remote_call": remote});
    let result = raw_client_call(&info.endpoint, method, params, auth, &policy)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let echoed: eliot_core::RemoteCallContext =
        serde_json::from_value(result["remote_call"].clone())
            .map_err(|_| Error::new("unknown", "daemon omitted verified remote call binding"))?;
    if &echoed != remote || result.get("result").is_none() {
        return Err(Error::new("unknown", "daemon remote call binding differs"));
    }
    Ok(result)
}

/// HTTP bridge variant for one authenticated Swarm MCP tools/call request.
/// The signed remote request id is forwarded as private method-attribution context.
pub async fn client_call_with_remote_mcp_tool(
    path: &Path,
    method: &str,
    params: Value,
    remote: &eliot_core::RemoteCallContext,
) -> Result<Value> {
    remote.validate()?;
    if method.trim().is_empty() || !params.is_object() || remote.mcp_request_id.is_none() {
        return Err(Error::new("invalid", "invalid remote Swarm MCP tool call"));
    }
    if Ulid::from_string(&remote.call_id).is_err() {
        return Err(Error::new("invalid", "Swarm MCP call id is invalid"));
    }
    let data_dir = crate::startup::open_client_data_dir(path)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let info = data_dir
        .read_daemon_info()
        .map_err(|error| with_connect_hint(path, error))?
        .ok_or_else(|| {
            with_connect_hint(path, Error::new("unavailable", "daemon is not running"))
        })?;
    let token = data_dir
        .read_owner_token()
        .map_err(|error| with_connect_hint(path, error))?;
    let policy = SecretPolicy::new([token.as_str()]);
    let mut auth = json!({"kind":"owner", "credential": token, "remote_call": remote});
    auth["swarm_mcp_call_id"] = json!(remote.call_id);
    let result = raw_client_call(&info.endpoint, method, params, auth, &policy)
        .await
        .map_err(|error| with_connect_hint(path, error))?;
    let echoed: eliot_core::RemoteCallContext =
        serde_json::from_value(result["remote_call"].clone())
            .map_err(|_| Error::new("unknown", "daemon omitted verified remote call binding"))?;
    if &echoed != remote || result.get("result").is_none() {
        return Err(Error::new("unknown", "daemon remote call binding differs"));
    }
    Ok(result)
}

async fn raw_client_call(
    endpoint: &str,
    method: &str,
    params: Value,
    auth: Value,
    policy: &SecretPolicy,
) -> Result<Value> {
    raw_client_call_measured(endpoint, method, params, auth, policy, None).await
}

async fn raw_client_call_measured(
    endpoint: &str,
    method: &str,
    params: Value,
    auth: Value,
    policy: &SecretPolicy,
    mut timings: Option<&mut CallTimings>,
) -> Result<Value> {
    let mut envelope = serde_json::Map::new();
    let id = auth
        .get("remote_call")
        .and_then(|remote| remote.get("call_id"))
        .cloned()
        .unwrap_or_else(|| Value::String(Ulid::new().to_string()));
    envelope.insert("_auth".to_owned(), auth);
    envelope.insert("_payload".to_owned(), params.clone());
    if timings.is_some() {
        envelope.insert("_timing".to_owned(), Value::Bool(true));
    }

    let request = RpcRequest {
        jsonrpc: "2.0".to_owned(),
        id: id.clone(),
        method: method.to_owned(),
        params: Value::Object(envelope),
    };
    let frame = serde_json::to_string(&request)
        .map_err(|error| Error::new("internal", format!("encode daemon RPC: {error}")))?;
    check_frame_size(&frame)?;

    let timeout = call_timeout(method, &params);
    let deadline = Instant::now() + timeout;
    // Connect first, on its own: the handshake writes no request bytes, so a
    // connect timeout or refusal positively establishes zero bytes written
    // and stays a retryable `unavailable` — nothing was admitted.
    let connect_started = Instant::now();
    let stream = timeout_at(deadline, IpcStream::connect(endpoint, deadline))
        .await
        .map_err(|_| {
            Error::new(
                "unavailable",
                "daemon IPC connect deadline elapsed before any request was sent",
            )
        })??;
    if let Some(timing) = timings.as_deref_mut() {
        timing.connect_ns = nanos(connect_started.elapsed());
    }
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_RPC_FRAME_BYTES.saturating_sub(1)),
    );
    // Once any byte of the frame may have been written, a stalled or failed
    // send can no longer prove non-execution: `LinesCodec::decode_eof` yields
    // a trailing unterminated line, so the daemon may admit a request the
    // client never sees answered. Send-phase failures are `unknown`.
    let round_trip_started = Instant::now();
    timeout_at(deadline, framed.send(frame))
        .await
        .map_err(|_| Error::new("unknown", "daemon RPC exceeded its absolute deadline"))?
        .map_err(|_| Error::new("unknown", "daemon RPC write failed"))?;
    // The request is written: a local reply-integrity failure cannot prove
    // non-execution. Matching, decoded daemon errors keep their domain code.
    let line = timeout_at(deadline, framed.next())
        .await
        .map_err(|_| Error::new("unknown", "daemon RPC exceeded its absolute deadline"))?
        .ok_or_else(|| Error::new("unknown", "daemon closed the RPC connection"))?
        .map_err(|_| Error::new("unknown", "daemon response frame is invalid"))?;
    if let Some(timing) = timings.as_deref_mut() {
        timing.round_trip_ns = nanos(round_trip_started.elapsed());
    }
    if line.len().saturating_add(1) > MAX_RPC_FRAME_BYTES {
        return Err(Error::new(
            "unknown",
            "daemon response exceeds the 4 MiB frame limit",
        ));
    }
    let decode_started = Instant::now();
    #[derive(Deserialize)]
    struct TimedResponse {
        #[serde(flatten)]
        response: RpcResponse,
        #[serde(default)]
        _timing: Option<Value>,
    }
    let (response, handle_ns) = if timings.is_some() {
        let decoded: TimedResponse = serde_json::from_str(&line)
            .map_err(|_| Error::new("unknown", "daemon response is malformed"))?;
        (
            decoded.response,
            decoded
                ._timing
                .and_then(|timing| timing.get("handle_ns").and_then(Value::as_u64)),
        )
    } else {
        (
            serde_json::from_str::<RpcResponse>(&line)
                .map_err(|_| Error::new("unknown", "daemon response is malformed"))?,
            None,
        )
    };
    if let Some(timing) = timings {
        timing.decode_ns = nanos(decode_started.elapsed());
        timing.handle_ns = handle_ns;
    }
    if response.jsonrpc != "2.0" || response.id != id {
        return Err(Error::new(
            "unknown",
            "daemon response does not match the request",
        ));
    }
    match (response.result, response.error) {
        (Some(value), None) => Ok(value),
        (None, Some(error)) => Err(core_error(error, policy)),
        _ => Err(Error::new(
            "unknown",
            "daemon response must contain exactly one result or error",
        )),
    }
}

/// Typed caller boundary over the existing authenticated client. Params are
/// serialized once; transport/domain errors retain their original diagnostics.
/// Current public RPC results are objects, never positional arrays or scalars.
pub async fn client_call_typed<P: Serialize + ?Sized, R: DeserializeOwned>(
    data_dir: &Path,
    method: &str,
    params: &P,
) -> Result<R> {
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("invalid", "daemon RPC parameters could not be serialized"))?;
    decode_client_result(method, client_call(data_dir, method, params).await)
}

/// Wait server-side (`swarm.wait`, mode `all`) until every listed command and
/// turn is terminal, in slices of at most the server limit. Returns false when
/// `deadline` passes first. A command bound to a turn waits for that turn, as
/// `swarm.wait` defines. Callers read the record back themselves.
pub async fn wait_terminal(
    data_dir: &Path,
    commands: Vec<String>,
    turns: Vec<WaitTurnRef>,
    deadline: Instant,
) -> Result<bool> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        let wait_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .min(MAX_SWARM_WAIT_MS);
        let request = SwarmWaitRequest::new(commands.clone(), turns.clone(), "all", wait_ms);
        request.validate()?;
        let reply: SwarmWaitResponse = client_call_typed(data_dir, "swarm.wait", &request).await?;
        request.validate_response(&reply)?;
        if reply.completed {
            return Ok(true);
        }
    }
}

/// Pause before re-waiting when `swarm.wait` reported a turn-bound command
/// complete by its turn while the command row itself still reads open.
const SETTLE_LAG: Duration = Duration::from_millis(200);

/// Wait server-side until one command is terminal (unknown included), then
/// return its record; `None` when `deadline` passes first.
pub async fn wait_command(
    data_dir: &Path,
    request: &CommandGetRequest,
    deadline: Instant,
) -> Result<Option<Command>> {
    request.validate()?;
    while wait_terminal(data_dir, vec![request.id.clone()], Vec::new(), deadline).await? {
        let command: Command = client_call_typed(data_dir, "command.get", request).await?;
        request.validate_response(&command)?;
        if !matches!(
            command.state,
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
        ) {
            return Ok(Some(command));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "This bounded wait is owned by the surrounding operation."
        )]
        tokio::time::sleep(SETTLE_LAG.min(deadline.saturating_duration_since(Instant::now())))
            .await;
    }
    Ok(None)
}

fn decode_client_result<R: DeserializeOwned>(method: &str, result: Result<Value>) -> Result<R> {
    let result = result?;
    let context = match method {
        "events.bounds" => "events.bounds returned a malformed typed result",
        "events.read" => "events.read returned a malformed typed result",
        "events.subscribe" => "events.subscribe returned a malformed typed result",
        _ => "daemon returned a malformed typed result",
    };
    if !result.is_object() {
        return Err(Error::new("unknown", context));
    }
    serde_json::from_value(result).map_err(|_| Error::new("unknown", context))
}

async fn serve_connection(stream: IpcStream, router: Router, policy: Arc<SecretPolicy>) {
    serve_connection_with_write_timeout(stream, router, policy, RESPONSE_WRITE_TIMEOUT).await;
}

/// Refuse one connection past the client limit without reading it into the
/// Router: the frame is decoded only to echo its id back with a typed
/// retryable error, so the caller can retry knowing nothing was admitted.
async fn reply_overloaded(stream: IpcStream) {
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_RPC_FRAME_BYTES.saturating_sub(1)),
    );
    let incoming = timeout_at(Instant::now() + OVERLOAD_READ_TIMEOUT, framed.next()).await;
    let response = match incoming {
        Ok(Some(Ok(line))) if line.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => {
            match decode_request(&line) {
                Ok(request) => RpcResponse {
                    jsonrpc: "2.0".to_owned(),
                    id: request.id,
                    result: None,
                    error: Some(RpcError {
                        code: -32000,
                        message: "daemon is at its connection limit; retry the call".to_owned(),
                        data: Some(json!({"code": "busy"})),
                    }),
                },
                Err((id, error)) => RpcResponse {
                    jsonrpc: "2.0".to_owned(),
                    id,
                    result: None,
                    error: Some(error),
                },
            }
        }
        Ok(Some(Ok(_))) => RpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Value::Null,
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "request frame exceeds the 4 MiB limit".to_owned(),
                data: Some(json!({"code": "frame_too_large"})),
            }),
        },
        _ => return,
    };
    let frame = match serde_json::to_string(&response) {
        Ok(frame) if frame.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => frame,
        _ => return,
    };
    send_response_frame(&mut framed, frame, RESPONSE_WRITE_TIMEOUT).await;
}

/// Bound one server-side response write. A peer that stops reading must not
/// hold its connection task past `write_timeout`; the caller ends the task and
/// finishes response bookkeeping instead of retrying the effect.
async fn send_response_frame(
    framed: &mut Framed<IpcStream, LinesCodec>,
    frame: String,
    write_timeout: Duration,
) -> bool {
    match timeout(write_timeout, framed.send(frame)).await {
        Ok(Ok(())) => true,
        Ok(Err(_)) | Err(_) => false,
    }
}

async fn serve_connection_with_write_timeout(
    mut stream: IpcStream,
    router: Router,
    policy: Arc<SecretPolicy>,
    write_timeout: Duration,
) {
    let peer = stream.take_local_peer();
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_RPC_FRAME_BYTES.saturating_sub(1)),
    );
    let mut arm_shutdown = false;
    #[cfg(debug_assertions)]
    let mut arm_panic = false;
    let incoming = timeout_at(Instant::now() + DEFAULT_CALL_TIMEOUT, framed.next()).await;
    let handle_started = Instant::now();
    let mut measured = false;
    let mut rpc_context = None;
    let response = match incoming {
        Ok(Some(Ok(line))) if line.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => {
            match decode_request(&line) {
                Ok(request) => {
                    measured = request.params.get("_timing") == Some(&Value::Bool(true));
                    let is_shutdown = request.method == "daemon.shutdown"
                        && !matches!(
                            request.params.get("mode").and_then(Value::as_str),
                            Some("prepare_restart" | "cancel_restart")
                        );
                    let (result, context) =
                        router.handle_rpc_with_log_context(&request, peer).await;
                    let response = match result {
                        Ok(result) => {
                            arm_shutdown = is_shutdown;
                            #[cfg(debug_assertions)]
                            {
                                arm_panic =
                                    request.method == crate::crash::DAEMON_PANIC_TEST_METHOD;
                            }
                            RpcResponse {
                                jsonrpc: "2.0".to_owned(),
                                id: request.id,
                                result: Some(result),
                                error: None,
                            }
                        }
                        Err(error) => {
                            logging::record(
                                &policy,
                                "rpc.error",
                                &[
                                    ("principal", context.principal.as_str()),
                                    ("method", context.method.as_str()),
                                ],
                                Some(&error),
                            );
                            RpcResponse {
                                jsonrpc: "2.0".to_owned(),
                                id: request.id,
                                result: None,
                                error: Some(rpc_error(error, &policy)),
                            }
                        }
                    };
                    rpc_context = Some(context);
                    response
                }
                Err((id, error)) => RpcResponse {
                    jsonrpc: "2.0".to_owned(),
                    id,
                    result: None,
                    error: Some(error),
                },
            }
        }
        Ok(Some(Ok(_))) | Ok(Some(Err(_))) => RpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Value::Null,
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "request frame exceeds the 4 MiB limit".to_owned(),
                data: Some(json!({"code": "frame_too_large"})),
            }),
        },
        Ok(None) | Err(_) => return,
    };

    let handle_ns = nanos(handle_started.elapsed());
    let mut encoded = if measured {
        #[derive(Serialize)]
        struct TimedResponse<'a> {
            #[serde(flatten)]
            response: &'a RpcResponse,
            _timing: HandleTiming,
        }
        serde_json::to_string(&TimedResponse {
            response: &response,
            _timing: HandleTiming { handle_ns },
        })
    } else {
        serde_json::to_string(&response)
    };
    if measured
        && encoded
            .as_ref()
            .is_ok_and(|frame| frame.len().saturating_add(1) > MAX_RPC_FRAME_BYTES)
    {
        // Optional diagnostics never displace a semantic result that fits.
        encoded = serde_json::to_string(&response);
    }
    #[cfg(debug_assertions)]
    let sending_panic_ack = arm_panic
        && encoded
            .as_ref()
            .is_ok_and(|frame| frame.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES);
    let wrote_response = match encoded {
        Ok(frame) if frame.len().saturating_add(1) <= MAX_RPC_FRAME_BYTES => {
            send_response_frame(&mut framed, frame, write_timeout).await
        }
        _ => {
            let fallback = RpcResponse {
                jsonrpc: "2.0".to_owned(),
                id: response.id.clone(),
                result: None,
                error: Some(undeliverable_response_error()),
            };
            match serde_json::to_string(&fallback) {
                Ok(frame) => send_response_frame(&mut framed, frame, write_timeout).await,
                Err(_) => return,
            }
        }
    };
    if !wrote_response {
        let (principal, method) = rpc_context
            .as_ref()
            .map(|context| (context.principal.as_str(), context.method.as_str()))
            .unwrap_or(("unauthenticated", "unavailable"));
        let error = Error::new(
            "rpc_response_write_failed",
            "RPC response could not be delivered before the connection ended or its deadline elapsed",
        );
        logging::record(
            &policy,
            "rpc.response_write_failed",
            &[("principal", principal), ("method", method)],
            Some(&error),
        );
    }
    router.response_finished(arm_shutdown);
    #[cfg(debug_assertions)]
    if sending_panic_ack && wrote_response {
        // The one-call client closes after reading its reply. On Windows a
        // successful write alone can leave unread bytes in the named pipe.
        // Give the client a bounded chance to read before dropping the server.
        let _ = timeout(write_timeout, framed.next()).await;
        crate::crash::panic_and_abort();
    }
}

/// Fallback error when a computed response cannot be framed or delivered:
/// the handler already ran, so the request may have committed a write and
/// the effect is unknown, never a pre-admission rejection.
fn undeliverable_response_error() -> RpcError {
    RpcError {
        code: -32603,
        message: "daemon response exceeded the frame limit and was not delivered; the request may have been applied".to_owned(),
        data: Some(json!({"code": "unknown"})),
    }
}

fn decode_request(line: &str) -> std::result::Result<RpcRequest, (Value, RpcError)> {
    let value: Value = serde_json::from_str(line).map_err(|_| {
        (
            Value::Null,
            RpcError {
                code: -32700,
                message: "request is not valid JSON".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        )
    })?;
    let Some(object) = value.as_object() else {
        return Err((
            Value::Null,
            RpcError {
                code: -32600,
                message: "JSON-RPC request must be an object".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    };
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err((
            id,
            RpcError {
                code: -32600,
                message: "JSON-RPC version must be 2.0".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    }
    let request: RpcRequest = serde_json::from_value(value).map_err(|_| {
        (
            id.clone(),
            RpcError {
                code: -32600,
                message: "JSON-RPC request is malformed".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        )
    })?;
    if !valid_id(&request.id) || !request.params.is_object() {
        return Err((
            request.id,
            RpcError {
                code: -32600,
                message: "JSON-RPC request requires a scalar id and object params".to_owned(),
                data: Some(json!({"code": "invalid"})),
            },
        ));
    }
    Ok(request)
}

fn redact_schema_strings(policy: &SecretPolicy, value: &mut Value) {
    match value {
        Value::String(text) => *text = policy.redact_text(text),
        Value::Array(values) => {
            for value in values {
                redact_schema_strings(policy, value);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact_schema_strings(policy, value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn rpc_error(mut error: Error, policy: &SecretPolicy) -> RpcError {
    error.message = policy
        .redact_text(&error.message)
        .chars()
        .take(1_024)
        .collect::<String>()
        .into();
    for diagnostic in error.diagnostics.iter_mut() {
        diagnostic.snapshot.text = policy.redact_text(&diagnostic.snapshot.text);
    }
    if let Some(detail) = error.details.as_mut() {
        detail.field = policy.redact_text(&detail.field);
        redact_schema_strings(policy, &mut detail.expected);
    }
    bound_stderr_diagnostics(&mut error.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    let code = match error.code.as_str() {
        "unsupported" => -32601,
        "invalid" | "frame_too_large" => -32602,
        _ => -32000,
    };
    let mut data = serde_json::to_value(&error).unwrap_or_else(|_| json!({"code": error.code}));
    if let Some(object) = data.as_object_mut() {
        object.remove("message");
    }
    RpcError {
        code,
        message: *error.message,
        data: Some(data),
    }
}

fn core_error(error: RpcError, policy: &SecretPolicy) -> Error {
    let mut data = error.data.unwrap_or_else(|| json!({}));
    let code = data
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal")
        .to_owned();
    let message = policy.redact_text(&error.message);
    for field in ["field", "expected"] {
        if let Some(Value::String(value)) = data.get_mut("details").and_then(|d| d.get_mut(field)) {
            *value = policy.redact_text(value);
        }
    }
    if let Some(values) = data.get_mut("diagnostics").and_then(Value::as_array_mut) {
        for diagnostic in values.iter_mut().take(2) {
            if let Some(Value::String(text)) = diagnostic.pointer_mut("/snapshot/text") {
                *text = policy.redact_text(text);
            }
        }
    }
    let Some(object) = data.as_object_mut() else {
        return Error::new(code, message);
    };
    object.insert("code".to_owned(), json!(code));
    object.insert("message".to_owned(), json!(message));
    let mut result =
        serde_json::from_value::<Error>(data).unwrap_or_else(|_| Error::new(code, message));
    bound_stderr_diagnostics(&mut result.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    result
}

enum ClientIdentity {
    Owner,
    Agent {
        id: String,
        generation: i64,
        token: String,
    },
    Run {
        run_command_id: String,
        token: String,
    },
}

const AGENT_IDENTITY_ENV: [&str; 3] = [
    "ELIOT_AGENT_ID",
    "ELIOT_AGENT_GENERATION",
    "ELIOT_AGENT_TOKEN",
];
/// Issuing data directory of the inherited Agent identity, normalized by the
/// spawning daemon (see [`normalize_identity_data_dir`]).
pub(crate) const AGENT_DATA_DIR_ENV: &str = "ELIOT_AGENT_DATA_DIR";
pub(crate) const AGENT_DATA_SCOPE_ENV: &str = "ELIOT_AGENT_DATA_SCOPE";
pub(crate) const SHELL_IDENTITY_DISABLED_ENV: &str = "ELIOT_SHELL_IDENTITY_DISABLED";
const RUN_IDENTITY_ENV: [&str; 2] = ["ELIOT_RUN_COMMAND_ID", "ELIOT_RUN_TOKEN"];
const OWNER_IDENTITY_ENV: [&str; 3] = [
    "ELIOT_OWNER_ID",
    "ELIOT_OWNER_TOKEN",
    "ELIOT_OWNER_CREDENTIAL",
];

/// Lexically normalize a data directory for identity-scope comparison. This
/// performs no filesystem I/O: a relative path resolves against the current
/// directory, `.` segments and redundant separators drop out, and `..`
/// segments pop the previous normal segment. A `..` that cannot pop stays in
/// place so the comparison safely misses instead of escaping the root.
pub(crate) fn normalize_identity_data_dir(path: &Path) -> PathBuf {
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

/// Compare two data directories after [`normalize_identity_data_dir`].
/// Directory paths compare case-insensitively on Windows and exactly
/// elsewhere, matching environment-name handling on each platform.
pub(crate) fn identity_data_dirs_equal(left: &Path, right: &Path) -> bool {
    let left = normalize_identity_data_dir(left);
    let right = normalize_identity_data_dir(right);
    #[cfg(windows)]
    {
        left.as_os_str()
            .as_encoded_bytes()
            .eq_ignore_ascii_case(right.as_os_str().as_encoded_bytes())
    }
    #[cfg(not(windows))]
    {
        left.as_os_str() == right.as_os_str()
    }
}

/// Carry the issuing root in the shell as a fingerprint rather than a path.
pub(crate) fn identity_data_scope(path: &Path) -> String {
    let path = normalize_identity_data_dir(path);
    #[cfg(windows)]
    let bytes = path.to_str().map_or_else(
        || path.as_os_str().as_encoded_bytes().to_ascii_lowercase(),
        |path| {
            eliot_core::display_path(path)
                .to_ascii_lowercase()
                .into_bytes()
        },
    );
    #[cfg(windows)]
    let bytes = bytes.as_slice();
    #[cfg(not(windows))]
    let bytes = path.as_os_str().as_encoded_bytes();
    eliot_core::api::inference::sha256_hex(bytes)
}

fn caller_identity_for_target(target: &Path) -> Result<ClientIdentity> {
    let identity = caller_identity_from(
        target,
        AGENT_IDENTITY_ENV.map(std::env::var_os),
        std::env::var_os(AGENT_DATA_DIR_ENV),
        std::env::var_os(AGENT_DATA_SCOPE_ENV),
        RUN_IDENTITY_ENV.map(std::env::var_os),
        OWNER_IDENTITY_ENV.map(std::env::var_os),
    )?;
    if matches!(identity, ClientIdentity::Owner)
        && std::env::var_os(SHELL_IDENTITY_DISABLED_ENV).is_some()
    {
        return Err(Error::new(
            "not_allowed",
            "this Agent shell has no scoped identity; use its managed Swarm MCP connection",
        ));
    }
    Ok(identity)
}

/// Whether the effective inherited identity for `target` is the local Owner.
/// Startup uses the same target scoping and malformed-identity checks as RPC
/// calls before deciding whether it may issue the Owner-only readiness probe.
pub(crate) fn client_is_owner(target: &Path) -> Result<bool> {
    caller_identity_for_target(target).map(|identity| matches!(identity, ClientIdentity::Owner))
}

fn caller_identity_from(
    target: &Path,
    agent_values: [Option<std::ffi::OsString>; 3],
    agent_data_dir: Option<std::ffi::OsString>,
    agent_data_scope: Option<std::ffi::OsString>,
    run_values: [Option<std::ffi::OsString>; 2],
    owner_values: [Option<std::ffi::OsString>; 3],
) -> Result<ClientIdentity> {
    let has_agent = agent_values.iter().any(Option::is_some)
        || agent_data_dir.is_some()
        || agent_data_scope.is_some();
    let has_run = run_values.iter().any(Option::is_some);
    let has_owner = owner_values.iter().any(Option::is_some);
    if has_owner || (has_agent && has_run) {
        return Err(Error::new(
            "not_allowed",
            "managed role credentials conflict",
        ));
    }

    if has_agent {
        if agent_data_dir.is_none() && agent_data_scope.is_none() {
            return Err(Error::new(
                "not_allowed",
                "managed Agent credentials require an issuing data directory binding",
            ));
        }
        if let Some(scope) = agent_data_scope.as_deref()
            && scope != std::ffi::OsStr::new(&identity_data_scope(target))
        {
            return Err(Error::new(
                "not_allowed",
                "managed Agent credentials belong to another daemon data directory",
            ));
        }
        if let Some(issuing) = agent_data_dir.as_deref() {
            if issuing.is_empty() || !Path::new(issuing).is_absolute() {
                return Err(Error::new(
                    "not_allowed",
                    "managed Agent issuing data directory is invalid",
                ));
            }
            if !identity_data_dirs_equal(target, Path::new(issuing)) {
                return Err(Error::new(
                    "not_allowed",
                    "managed Agent credentials belong to another daemon data directory",
                ));
            }
        }
        if agent_values.iter().any(Option::is_none) {
            return Err(Error::new(
                "not_allowed",
                "managed Agent credentials are incomplete",
            ));
        }
        let id = agent_values[0]
            .as_ref()
            .and_then(|value| value.to_str())
            .ok_or_else(|| Error::new("not_allowed", "managed Agent id is invalid"))?;
        let generation = agent_values[1]
            .as_ref()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| Error::new("not_allowed", "managed Agent generation is invalid"))?;
        let token = agent_values[2]
            .as_ref()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("not_allowed", "managed Agent token is invalid"))?;
        if eliot_core::parse_agent_id(id).is_err() {
            return Err(Error::new("not_allowed", "managed Agent id is invalid"));
        }
        return Ok(ClientIdentity::Agent {
            id: id.to_owned(),
            generation,
            token: token.to_owned(),
        });
    }

    if has_run {
        if run_values.iter().any(Option::is_none) {
            return Err(Error::new("not_allowed", "Run credentials are incomplete"));
        }
        let run_command_id = run_values[0]
            .as_ref()
            .and_then(|value| value.to_str())
            .filter(|value| !value.trim().is_empty() && !value.contains('\0'))
            .ok_or_else(|| Error::new("not_allowed", "Run command id is invalid"))?;
        let token = run_values[1]
            .as_ref()
            .and_then(|value| value.to_str())
            .filter(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .ok_or_else(|| Error::new("not_allowed", "Run credential is invalid"))?;
        return Ok(ClientIdentity::Run {
            run_command_id: run_command_id.to_owned(),
            token: token.to_owned(),
        });
    }

    Ok(ClientIdentity::Owner)
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

fn call_timeout(method: &str, params: &Value) -> Duration {
    if method == "daemon.shutdown" && params.get("backup") == Some(&Value::Bool(true)) {
        Duration::from_secs(150)
    } else if matches!(method, "models.catalog" | "harness.list") {
        // Leave time for IPC and encoding after the shared inventory deadline.
        crate::adhoc::DISCOVERY_BUDGET + Duration::from_secs(5)
    } else if method == "agent.discover" {
        // Includes bounded reader shutdown/reaping after the read deadline.
        Duration::from_secs(45)
    } else if method == "events.subscribe" {
        let wait_ms = params
            .get("wait_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(30_000);
        // One long-poll legitimately spans the server wait window plus store
        // contention under sustained multi-hundred-events/s load. Bound the
        // processing/connect slack by the standard RPC timeout instead of a
        // fixed few seconds, so a transient stall delays one page instead of
        // killing the stream with an absolute deadline.
        Duration::from_millis(wait_ms).saturating_add(DEFAULT_CALL_TIMEOUT)
    } else {
        DEFAULT_CALL_TIMEOUT
    }
}

fn check_frame_size(frame: &str) -> Result<()> {
    if frame.len().saturating_add(1) > MAX_RPC_FRAME_BYTES {
        Err(Error::new(
            "frame_too_large",
            "daemon RPC frame exceeds 4 MiB",
        ))
    } else {
        Ok(())
    }
}

/// Bounded startup-failure record from the private daemon log. Only the
/// sanitized `error_code`, timestamp and already-redacted diagnostics are
/// exposed; no raw messages or request payloads are ever read here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonStartupFailure {
    pub error_code: String,
    pub ts_ms: u128,
    pub diagnostics: Vec<StderrDiagnostic>,
    /// The source-free config parser issue from a typed Swarm startup failure.
    pub config_diagnostic: Option<String>,
}

const LOG_TAIL_BYTES: u64 = 64 * 1024;
const LOG_TAIL_RECORDS: usize = 64;
const CONFIG_STARTUP_EVIDENCE_BYTES: usize = 512;
const CONFIG_STARTUP_MAX_LINE: u32 = 128 * 1024;
const CONFIG_STARTUP_ISSUES: &[&str] = &[
    "invalid TOML syntax or value",
    "unknown configuration field",
    "unsupported configuration field",
    "daemon configuration could not be loaded",
];

fn startup_config_failure(error: Error) -> Error {
    let evidence = config_startup_evidence(&error.message);
    error
        .with_where(crate::startup::CONFIG_STARTUP_LOCATION)
        .with_origin(FailureOrigin::Swarm, evidence)
}

/// Reduce the config parser's message to fixed issue vocabulary and a line
/// number. Never persist its field text, source line or caret excerpt.
fn config_startup_evidence(message: &str) -> String {
    let issue = message
        .strip_prefix("config.toml:")
        .and_then(|location_and_detail| location_and_detail.split_once(": "))
        .and_then(|(location, detail)| {
            let (line, column) = location.split_once(':')?;
            let line = line.parse::<u32>().ok()?;
            let _column = column.parse::<u32>().ok()?;
            if line == 0 || line > CONFIG_STARTUP_MAX_LINE {
                return None;
            }
            let issue = detail.rsplit_once(": ")?.1;
            CONFIG_STARTUP_ISSUES
                .contains(&issue)
                .then(|| format!("config.toml, line {line}: {issue}"))
        })
        .unwrap_or_else(|| {
            "config.toml, line unavailable: daemon configuration could not be loaded".to_owned()
        });
    issue.chars().take(CONFIG_STARTUP_EVIDENCE_BYTES).collect()
}

/// Accept only the fixed evidence format emitted above from private logs.
fn validated_config_startup_diagnostic(evidence: &str) -> Option<String> {
    let (file, remainder) = evidence.split_once(", line ")?;
    if file != "config.toml" {
        return None;
    }
    let (line, issue) = remainder.split_once(": ")?;
    let line = if line == "unavailable" {
        line.to_owned()
    } else {
        let line = line.parse::<u32>().ok()?;
        if line == 0 || line > CONFIG_STARTUP_MAX_LINE {
            return None;
        }
        line.to_string()
    };
    if !CONFIG_STARTUP_ISSUES.contains(&issue) {
        return None;
    }
    let diagnostic = format!("config.toml, line {line}: {issue}");
    (diagnostic.len() <= CONFIG_STARTUP_EVIDENCE_BYTES).then_some(diagnostic)
}

/// Newest `daemon.failed` record in the bounded log tail, if any.
pub fn last_daemon_failure(data_dir: &Path) -> Option<DaemonStartupFailure> {
    read_log_failures(data_dir).into_iter().next_back()
}

/// Newest `daemon.failed` record at or after `since_ms`, if any. Callers pass
/// the start instant captured before spawning so an old failure is never
/// reported for a new start.
pub fn daemon_failure_since(data_dir: &Path, since_ms: u128) -> Option<DaemonStartupFailure> {
    read_log_failures(data_dir)
        .into_iter()
        .rfind(|failure| failure.ts_ms >= since_ms)
}

/// Newest `daemon.*` startup phase marker in the bounded log tail, if any. A
/// readiness timeout names the phase that stalled; a phase of `daemon.ready`
/// with failing probes means the daemon is up but rejecting the caller.
pub fn last_daemon_phase(data_dir: &Path) -> Option<String> {
    read_log_tail(data_dir).into_iter().rev().find_map(|value| {
        let code = value.get("code")?.as_str()?;
        code.starts_with("daemon.")
            .then(|| code.chars().take(64).collect())
    })
}

/// Bounded parsed records from the private daemon log tail, oldest first.
/// Only the resolved private data dir is ever read. An unvalidated or
/// missing root yields no records rather than an error with paths.
fn read_log_tail(data_dir: &Path) -> Vec<Value> {
    if DataDir::open_existing(data_dir).is_err() {
        return Vec::new();
    }
    let log_path = data_dir.join("logs").join("daemon.log");
    let mut file = match File::open(&log_path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let size = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let offset = size.saturating_sub(LOG_TAIL_BYTES);
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return Vec::new();
    }
    let mut bytes = Vec::new();
    // Bounded tail: at most 64 KiB plus the current line.
    if file
        .take(LOG_TAIL_BYTES.saturating_add(4096))
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&bytes);
    let start = if offset > 0 {
        // Skip a possible partial first line when seeking into the file.
        text.find('\n').map(|index| index + 1).unwrap_or(text.len())
    } else {
        0
    };
    let tail: Vec<&str> = text[start..].lines().rev().take(LOG_TAIL_RECORDS).collect();
    let mut records = Vec::new();
    for line in tail.into_iter().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            records.push(value);
        }
    }
    records
}

fn read_log_failures(data_dir: &Path) -> Vec<DaemonStartupFailure> {
    let mut failures = Vec::new();
    for value in read_log_tail(data_dir) {
        if value.get("code").and_then(Value::as_str) != Some("daemon.failed") {
            continue;
        }
        let ts_ms = value
            .get("ts")
            .and_then(Value::as_u64)
            .map(|ts| ts as u128)
            .unwrap_or(0);
        let error_code = value
            .get("error_code")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .chars()
            .take(64)
            .collect::<String>();
        let diagnostics = value
            .get("diagnostics")
            .and_then(|diagnostics| {
                serde_json::from_value::<Vec<StderrDiagnostic>>(diagnostics.clone()).ok()
            })
            .unwrap_or_default();
        let config_diagnostic = (value
            .get("origin")
            .and_then(Value::as_str)
            .and_then(FailureOrigin::parse)
            == Some(FailureOrigin::Swarm)
            && value.get("where").and_then(Value::as_str)
                == Some(crate::startup::CONFIG_STARTUP_LOCATION))
        .then(|| {
            value
                .get("origin_evidence")
                .and_then(Value::as_str)
                .and_then(validated_config_startup_diagnostic)
        })
        .flatten();
        failures.push(DaemonStartupFailure {
            error_code,
            ts_ms,
            diagnostics,
            config_diagnostic,
        });
    }
    failures
}

/// Bounded connect hint for an unreachable daemon. Names only lock/log
/// presence and the last sanitized `daemon.failed` code; never secrets.
pub fn daemon_connect_hint(data_dir: &Path) -> String {
    let metadata = data_dir.join("daemon.json");
    let lock = data_dir.join("daemon.lock");
    let log = data_dir.join("logs").join("daemon.log");
    let metadata_state = if metadata.is_file() {
        "daemon metadata present"
    } else {
        "no daemon metadata"
    };
    let lock_state = if lock.is_file() {
        "daemon lock present"
    } else {
        "no daemon lock"
    };
    let log_state = if log.is_file() {
        "daemon log present"
    } else {
        "no daemon log"
    };
    let failure_state = match last_daemon_failure(data_dir) {
        Some(failure) => format!("last failure {}", failure.error_code),
        None => "no daemon.failed record".to_owned(),
    };
    format!(
        "hint: data_dir={}; {metadata_state}; {lock_state}; {log_state}; {failure_state}",
        data_dir.display()
    )
}

fn with_connect_hint(path: &Path, mut error: Error) -> Error {
    if !matches!(
        error.code.as_str(),
        "unavailable" | "platform_io" | "data_dir_unavailable"
    ) {
        return error;
    }
    let hint = daemon_connect_hint(path);
    if !error.message.contains("hint: data_dir=") {
        error.message = format!("{}; {}", error.message, hint).into();
        bound_stderr_diagnostics(&mut error.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    }
    error
}
