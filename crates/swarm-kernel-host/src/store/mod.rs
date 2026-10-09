//! A single database owner. The async facade never holds a SQLite connection.
mod acceptance;
mod assembly;
mod automation;
mod automation_acceptance;
mod automation_cron;
pub(crate) mod automation_dispatch;
mod automation_disposition;
mod automation_github_projection;
pub(crate) mod automation_goal_progression;
mod automation_intake;
mod automation_publication;
pub(crate) mod automation_repair;
mod automation_scheduler;
mod automation_transfer;
pub(crate) mod automation_work_dispatch;
pub(crate) mod bus_kernel;
#[cfg(test)]
mod c33_restart_diagnosis_fixture;
#[cfg(test)]
mod c34_workspace_start_diagnosis_fixture;
pub(crate) mod capacity;
mod checks;
mod code_scopes;
mod command_results;
mod concilium;
mod coordination;
mod coordination_threads;
mod coordination_watch;
mod forge;
mod github;
#[cfg(test)]
mod github_effect_tests;
mod github_effects;
mod github_pr_effects;
mod gm;
mod goals;
mod hooks;
mod host_lifecycle;
mod integration;
mod launch_registration;
pub(crate) mod launcher;
mod launcher_dispatch;
mod launcher_issuance;
mod launcher_mcp_tools;
pub(crate) use launcher_mcp_tools::{
    ParticipantCapabilityScope, participant_capability_projection,
};
mod launcher_native_mcp;
mod launcher_owned_service;
mod launcher_participant;
mod legacy_worker_demand;
mod logging;
mod mailbox;
mod message_batch;
#[cfg(test)]
mod module_bridge_recovery_tests;
mod module_credential;
pub(crate) mod module_demand;
pub(crate) use legacy_worker_demand::LegacyWorkerDemand;
mod module_handshake;
mod module_supervisor_observation;
mod monitor;
mod native_mcp;
mod normalized_result;
#[cfg(test)]
mod o6_taskless_path_fixture;
mod opencode;
mod operation_cancel_event_schema;
#[cfg(test)]
mod operation_failure_event_fixture;
mod operation_failure_event_schema;
#[cfg(test)]
mod operation_failure_event_schema_fixture;
mod operations;
pub(crate) mod participant_credentials;
mod prerequisites;
mod producers;
mod projection;
mod results;
mod review_disposition;
mod reviews;
mod runtime;
mod schedule_run_now;
#[cfg(test)]
mod schedule_run_now_tests;
mod schedules;
mod script_event_schema;
#[cfg(test)]
mod script_event_schema_fixture;
mod scripts;
mod status_reader;
mod submissions;
mod task_prompt;
mod tasks;
#[cfg(test)]
mod work_dispatch_regression;
mod workspace;
mod workspace_lifecycle;
use crate::{
    artifacts::{ArtifactFiles, MAX_PAGE_BYTES, ResultPage},
    config::Config,
    error::{Error, Result},
    model::{self, Credential, Principal, Role},
    platform::DataRoot,
};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, named_params, params,
};
use serde_json::{Value, json};
use std::{fs::File, path::Path, sync::Arc, thread::JoinHandle, time::Duration};
use tokio::sync::{Semaphore, oneshot, watch};

const SCHEMA: &str = include_str!("../../migrations/001_core.sql");
const WORKSPACE_SCHEMA: &str = include_str!("../../migrations/002_workspace.sql");
const OWNED_SERVICE_SCHEMA: &str = include_str!("../../migrations/003_owned_services.sql");
const SCRIPT_SCHEMA: &str = include_str!("../../migrations/004_scripts.sql");
const GITHUB_SCHEMA: &str = include_str!("../../migrations/006_github.sql");
const GITHUB_EFFECTS_SCHEMA: &str = include_str!("../../migrations/007_github_label_effects.sql");
const GITHUB_PR_EFFECTS_SCHEMA: &str = include_str!("../../migrations/008_github_pr_effects.sql");
const APPLICATION_ID: i64 = 0x45534331;
const LOCAL_OPERATOR_CLIENT_ID_KEY: &str = "local_operator_client_id";
pub(crate) const MANAGER_EVENT_SOURCE_STREAM: &str = "controller:manager-events";
type RunJob = Box<dyn FnOnce(&mut Connection) + Send>;
type Job = swarm_kernel::WriterJob<RunJob, message_batch::Request>;
type KernelHost = swarm_kernel::KernelHost<RunJob, message_batch::Request, Error>;
type KernelHostHandle = swarm_kernel::KernelHostHandle<RunJob, message_batch::Request>;
#[derive(Clone)]
pub struct Store {
    kernel: KernelHostHandle,
    status_reader: status_reader::Sender,
    config: Arc<Config>,
    changed: watch::Sender<u64>,
    artifacts: ArtifactFiles,
    artifact_io: Arc<Semaphore>,
    data_dir: std::path::PathBuf,
    automation_scheduler_owner_token: Option<Arc<String>>,
    launch_issuance: Arc<launcher_issuance::IssuanceRuntime>,
    telemetry: swarm_telemetry::Producer,
}
pub struct StoreOwner {
    kernel: KernelHost,
    status_thread: JoinHandle<()>,
    module_supervisor_credential: Credential,
    recovery: StoreRecovery,
    pub store: Store,
}

/// Inputs retained for the one recovery owner that may run only after the
/// original kernel writer has joined. Sharing the same File keeps the data-root
/// ownership fence without opening a concurrent Store writer.
struct StoreRecovery {
    root: std::path::PathBuf,
    lock: Arc<File>,
}

struct LaunchWorkspaceWork {
    actor: launcher::LaunchActor,
    plan: crate::workspace::WorkspaceLeasePlan,
    registration: crate::workspace::WorkspaceRegistration,
    reservation: crate::workspace::LeaseReservation,
    readback_only: bool,
}

impl StoreOwner {
    pub async fn start(
        root: DataRoot,
        config: Arc<Config>,
        credential: Credential,
    ) -> Result<Self> {
        Self::start_with_line_observer(root, config, credential, None).await
    }

    pub(crate) async fn start_with_line_observer(
        root: DataRoot,
        config: Arc<Config>,
        credential: Credential,
        line_observer: Option<swarm_telemetry::LineObserver>,
    ) -> Result<Self> {
        Self::start_with_observer_policies(root, config, credential, line_observer, None).await
    }

    pub(crate) async fn start_with_observer_policies(
        root: DataRoot,
        config: Arc<Config>,
        credential: Credential,
        line_observer: Option<swarm_telemetry::LineObserver>,
        text_capture_policy: Option<swarm_telemetry::TextCapturePolicy>,
    ) -> Result<Self> {
        // Recorder limits cannot reconfigure or prevent the operational stderr producer.
        let mut telemetry_config = swarm_telemetry::Config::default()
            .with_text_redactor(crate::redaction::diagnostic_text);
        if let Some(policy) = text_capture_policy {
            telemetry_config = telemetry_config.with_text_capture_policy(policy);
        }
        let artifacts = ArtifactFiles::new(&root.path)?;
        let data_dir = root.path.clone();
        let automation_scheduler_enabled = config.automation_scheduler.enabled;
        let retained_scheduler_config = if automation_scheduler_enabled {
            swarm_automation::load_worker_config(&data_dir).map_err(|_| {
                Error::new(
                    "AUTOMATION_SERVICE_CONFIG_INVALID",
                    "private scheduler config cannot be validated",
                )
            })?
        } else {
            None
        };
        let automation_scheduler_credential = if automation_scheduler_enabled {
            Some(
                retained_scheduler_config
                    .as_ref()
                    .map(|entry| entry.credential.clone())
                    .unwrap_or_else(|| Credential {
                        client_id: automation_scheduler::SERVICE_ID.to_owned(),
                        token: format!("{}{}", model::new_id(), model::new_id()),
                    }),
            )
        } else {
            None
        };
        let retained_scheduler_scope = retained_scheduler_config
            .as_ref()
            .map(|entry| entry.scope.clone());
        let scheduler_owner_token = if automation_scheduler_enabled {
            Some(
                retained_scheduler_config
                    .as_ref()
                    .map(|entry| entry.service_owner_token.clone())
                    .unwrap_or_else(|| format!("{}{}", model::new_id(), model::new_id())),
            )
        } else {
            None
        };
        let automation_scheduler_config_sha256 = if automation_scheduler_enabled {
            let scheduler_credential =
                automation_scheduler_credential.as_ref().ok_or_else(|| {
                    Error::new(
                        "AUTOMATION_SERVICE_IDENTITY_INVALID",
                        "enabled scheduler has no host-issued Module credential",
                    )
                })?;
            let owner_token = scheduler_owner_token.as_deref().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_IDENTITY_INVALID",
                    "enabled scheduler has no service-owner token",
                )
            })?;
            let scope = match retained_scheduler_scope.as_ref() {
                Some(scope) => scope.clone(),
                None => swarm_contracts::DeclaredServiceScope::new(
                    swarm_contracts::DeclaredServicePurpose::AutomationScheduler,
                    automation_scheduler::SERVICE_ID,
                    1,
                )
                .map_err(|_| {
                    Error::new(
                        "AUTOMATION_SERVICE_IDENTITY_INVALID",
                        "scheduler service scope could not be constructed",
                    )
                })?,
            };
            if let Some(retained) = retained_scheduler_config.as_ref() {
                Some(retained.config_sha256.clone())
            } else {
                Some(
                    swarm_automation::worker_config_sha256(
                        data_dir.to_str().ok_or_else(|| {
                            Error::new(
                                "AUTOMATION_SERVICE_CONFIG_INVALID",
                                "scheduler data root must be valid UTF-8",
                            )
                        })?,
                        &scope,
                        scheduler_credential,
                        owner_token,
                    )
                    .map_err(|_| {
                        Error::new(
                            "AUTOMATION_SERVICE_CONFIG_INVALID",
                            "scheduler config digest could not be computed",
                        )
                    })?,
                )
            }
        } else {
            None
        };
        let module_supervisor_credential = Credential {
            client_id: model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID.to_owned(),
            token: format!("{}{}", model::new_id(), model::new_id()),
        };
        let writer_lock = Arc::new(root.lock);
        let recovery = StoreRecovery {
            root: root.path.clone(),
            lock: Arc::clone(&writer_lock),
        };
        let writer_supervisor_credential = module_supervisor_credential.clone();
        let writer_root = root.path.clone();
        let writer_config = config.clone();
        let writer_open_config = config.clone();
        let writer_scheduler_credential = automation_scheduler_credential.clone();
        let writer_retained_scheduler_scope = retained_scheduler_scope.clone();
        let writer_scheduler_config_sha256 = automation_scheduler_config_sha256.clone();
        let mut kernel = swarm_kernel::spawn_kernel_host(
            swarm_kernel::KernelHostConfig {
                queue_capacity: config.storage.queue_capacity,
                batch_capacity: message_batch::MAX_BATCH_SIZE,
            },
            writer_lock,
            move || {
                open_database(
                    &writer_root,
                    &credential,
                    &writer_supervisor_credential,
                    &writer_open_config,
                    writer_scheduler_credential.as_ref(),
                    writer_retained_scheduler_scope.as_ref(),
                    writer_scheduler_config_sha256.as_deref(),
                )
            },
            |db, job: RunJob| job(db),
            move |db, batch| message_batch::process(db, batch, &writer_config),
        )
        .map_err(map_kernel_spawn_error)?;
        match kernel.wait_ready().await {
            Ok(()) => {}
            Err(swarm_kernel::KernelHostReadyError::Initialization(error)) => {
                return Err(merge_cleanup(
                    error,
                    join_kernel_host(kernel, "database owner").await,
                ));
            }
            Err(_) => {
                let error = Error::new("STORE_CLOSED", "initialization thread ended");
                return Err(merge_cleanup(
                    error,
                    join_kernel_host(kernel, "database owner").await,
                ));
            }
        }
        let kernel_handle = kernel.handle();
        let (status_reader, status_thread) = match status_reader::start(
            data_dir.join("swarm.db"),
            config.storage.queue_capacity,
            config.clone(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) => {
                drop(kernel_handle);
                return Err(merge_cleanup(
                    error,
                    join_kernel_host(kernel, "database owner").await,
                ));
            }
        };
        let store = Store {
            kernel: kernel_handle,
            status_reader,
            config: config.clone(),
            changed: watch::channel(0).0,
            artifacts,
            data_dir: data_dir.clone(),
            automation_scheduler_owner_token: scheduler_owner_token.clone().map(Arc::new),
            launch_issuance: Arc::new(launcher_issuance::IssuanceRuntime::new()),
            artifact_io: Arc::new(Semaphore::new(4)),
            telemetry: swarm_telemetry::Producer::with_line_observer(
                telemetry_config,
                line_observer,
            ),
        };
        if automation_scheduler_enabled && retained_scheduler_config.is_none() {
            let scheduler_credential = automation_scheduler_credential
                .as_ref()
                .expect("enabled scheduler has an issued Module credential");
            let scope = match store
                .run(|db| automation_scheduler::current_scope(db))
                .await
            {
                Ok(scope) => scope,
                Err(error) => {
                    kernel.close_admission(swarm_kernel::KernelAdmissionFault::ShuttingDown);
                    let status_shutdown = store.status_reader.shutdown().await;
                    drop(store);
                    let cleanup = merge_results(
                        join_store_threads(status_thread, kernel).await,
                        status_shutdown,
                    );
                    return Err(merge_cleanup(error, cleanup));
                }
            };
            if swarm_automation::write_worker_config(
                &data_dir,
                &scope,
                scheduler_credential,
                scheduler_owner_token
                    .as_deref()
                    .expect("enabled scheduler has an owner token"),
            )
            .is_err()
            {
                kernel.close_admission(swarm_kernel::KernelAdmissionFault::ShuttingDown);
                let status_shutdown = store.status_reader.shutdown().await;
                drop(store);
                let cleanup = merge_results(
                    join_store_threads(status_thread, kernel).await,
                    status_shutdown,
                );
                return Err(merge_cleanup(
                    Error::new(
                        "AUTOMATION_SERVICE_CONFIG_WRITE_FAILED",
                        "private scheduler config could not be created",
                    ),
                    cleanup,
                ));
            }
        }
        if let Err(error) = store.reload_logging_filters().await {
            kernel.close_admission(swarm_kernel::KernelAdmissionFault::ShuttingDown);
            let status_shutdown = store.status_reader.shutdown().await;
            drop(store);
            let cleanup = merge_results(
                join_store_threads(status_thread, kernel).await,
                status_shutdown,
            );
            return Err(merge_cleanup(error, cleanup));
        }
        Ok(Self {
            kernel,
            status_thread,
            module_supervisor_credential,
            recovery,
            store,
        })
    }
    pub async fn close(self) -> Result<()> {
        let StoreOwner {
            kernel,
            status_thread,
            module_supervisor_credential: _,
            recovery: _,
            store,
        } = self;
        kernel.close_admission(swarm_kernel::KernelAdmissionFault::ShuttingDown);
        let status_shutdown = store.status_reader.shutdown().await;
        drop(store);
        // Join both owners even if the reader already failed. Retained Store
        // handles must not keep an idle reader or writer alive after close.
        merge_results(
            join_store_threads(status_thread, kernel).await,
            status_shutdown,
        )
    }

    /// Close the status reader and join the original writer before opening one
    /// sequential recovery owner for the final lifecycle receipt. A receipt is
    /// never committed while the original writer can still tear down.
    pub(crate) async fn close_with_exit(
        self,
        mut exit: Result<()>,
        failed_supervisor: Option<&'static str>,
    ) -> Result<()> {
        let StoreOwner {
            kernel,
            status_thread,
            module_supervisor_credential: _,
            recovery,
            store,
        } = self;
        kernel.close_admission(swarm_kernel::KernelAdmissionFault::ShuttingDown);
        let status_shutdown = store.status_reader.shutdown().await;
        let status_join = join_store_thread(status_thread, "status reader").await;
        exit = merge_results(exit, merge_results(status_join, status_shutdown));
        drop(store);
        let kernel_join = join_kernel_host(kernel, "database owner").await;
        let kernel_join_confirmed = !matches!(
            kernel_join.as_ref().err().map(|error| error.code.as_str()),
            Some("KERNEL_JOIN_UNCONFIRMED")
        );
        exit = merge_results(exit, kernel_join);
        if !kernel_join_confirmed {
            return exit;
        }
        let (error_code, secondary_codes) = match exit.as_ref() {
            Ok(()) => (None, Vec::new()),
            Err(error) => (Some(error.code.clone()), error.secondary_codes.clone()),
        };
        let receipt = recovery
            .record_host_exit(error_code, secondary_codes, failed_supervisor)
            .await;
        merge_results(exit, receipt)
    }

    /// Return this host's credential for the trusted local module supervisor.
    /// Its only Store access is the bounded module-supervisor control surface;
    /// do not pass this credential to Managers or module adapters.
    pub fn module_supervisor_credential(&self) -> Credential {
        self.module_supervisor_credential.clone()
    }
}

impl StoreRecovery {
    async fn record_host_exit(
        self,
        error_code: Option<String>,
        secondary_codes: Vec<String>,
        failed_supervisor: Option<&'static str>,
    ) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            let StoreRecovery { root, lock } = self;
            let _lock = lock;
            let mut db = open_recovery_database(&root)?;
            record_host_exit_on_connection(&mut db, error_code, secondary_codes, failed_supervisor)
        })
        .await
        .map_err(|error| {
            Error::new(
                "STORE_CLOSED",
                format!("lifecycle recovery owner failed: {error}"),
            )
        })?
    }
}

async fn join_store_threads(status_thread: JoinHandle<()>, kernel: KernelHost) -> Result<()> {
    let status_result = join_store_thread(status_thread, "status reader").await;
    let database_result = join_kernel_host(kernel, "database owner").await;
    merge_results(status_result, database_result)
}

fn merge_results(primary: Result<()>, secondary: Result<()>) -> Result<()> {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(primary), Err(secondary)) => Err(primary.with_secondary_error(secondary)),
    }
}

fn merge_cleanup(primary: Error, cleanup: Result<()>) -> Error {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => primary.with_secondary_error(cleanup),
    }
}
async fn join_store_thread(thread: JoinHandle<()>, name: &'static str) -> Result<()> {
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .map_err(|error| Error::new("STORE_CLOSED", format!("{name} join failed: {error}")))?
        .map_err(|_| Error::new("STORE_PANIC", format!("{name} panicked")))
}

async fn join_kernel_host(kernel: KernelHost, name: &'static str) -> Result<()> {
    tokio::task::spawn_blocking(move || kernel.join())
        .await
        .map_err(|error| {
            Error::new(
                "KERNEL_JOIN_UNCONFIRMED",
                format!("{name} join could not confirm writer teardown: {error}"),
            )
        })?
        .map_err(|error| match error {
            swarm_kernel::KernelHostJoinError::WriterFailed(_) => {
                Error::new("KERNEL_WRITER_EXITED", "kernel writer exited unexpectedly")
            }
            swarm_kernel::KernelHostJoinError::Panicked => {
                Error::new("STORE_PANIC", format!("{name} panicked"))
            }
        })
}

fn map_kernel_spawn_error(error: swarm_kernel::KernelHostSpawnError) -> Error {
    match error {
        swarm_kernel::KernelHostSpawnError::InvalidConfig(error) => {
            Error::new("STORE_CONFIG", error.to_string())
        }
        swarm_kernel::KernelHostSpawnError::Thread(_) => Error::new(
            "KERNEL_THREAD_START_FAILED",
            "kernel writer thread could not start",
        ),
    }
}

fn map_kernel_submit_error(error: swarm_kernel::KernelSubmitError) -> Error {
    match error {
        swarm_kernel::KernelSubmitError::NotReady => {
            Error::new("STORE_NOT_READY", "kernel writer is not ready")
        }
        swarm_kernel::KernelSubmitError::AdmissionClosed { fault } => Error::new(
            kernel_admission_error_code(fault),
            "kernel durable admission is closed",
        ),
        swarm_kernel::KernelSubmitError::WriterClosed => {
            Error::new("KERNEL_WRITER_CLOSED", "kernel writer stopped")
        }
    }
}

fn kernel_admission_error_code(fault: swarm_kernel::KernelAdmissionFault) -> &'static str {
    match fault {
        swarm_kernel::KernelAdmissionFault::InitializationFailed => "KERNEL_INITIALIZATION_FAILED",
        swarm_kernel::KernelAdmissionFault::StoreUnavailable => "KERNEL_STORE_UNAVAILABLE",
        swarm_kernel::KernelAdmissionFault::DurableJournalUnavailable => {
            "KERNEL_JOURNAL_UNAVAILABLE"
        }
        swarm_kernel::KernelAdmissionFault::ShuttingDown => "STORE_CLOSED",
    }
}

fn kernel_host_status_value(snapshot: swarm_kernel::KernelHostSnapshot) -> Value {
    let (admission, fault) = match snapshot.admission {
        swarm_kernel::KernelAdmissionState::Starting => ("starting", None),
        swarm_kernel::KernelAdmissionState::Open => ("open", None),
        swarm_kernel::KernelAdmissionState::Closed { fault } => {
            ("closed", Some(kernel_admission_fault_name(fault)))
        }
    };
    let failure = snapshot.failure.map(kernel_failure_name);
    json!({
        "admission": admission,
        "fault": fault,
        "failure": failure,
        "lifecycle": kernel_lifecycle_name(snapshot.lifecycle),
    })
}

fn kernel_failure_name(failure: swarm_kernel::KernelHostFailure) -> &'static str {
    match failure {
        swarm_kernel::KernelHostFailure::WriterExited => "writer_exited",
    }
}

fn kernel_admission_fault_name(fault: swarm_kernel::KernelAdmissionFault) -> &'static str {
    match fault {
        swarm_kernel::KernelAdmissionFault::InitializationFailed => "initialization_failed",
        swarm_kernel::KernelAdmissionFault::StoreUnavailable => "store_unavailable",
        swarm_kernel::KernelAdmissionFault::DurableJournalUnavailable => {
            "durable_journal_unavailable"
        }
        swarm_kernel::KernelAdmissionFault::ShuttingDown => "shutting_down",
    }
}

fn supervisor_kernel_fault_name(fault: swarm_kernel::KernelAdmissionFault) -> &'static str {
    match fault {
        swarm_kernel::KernelAdmissionFault::DurableJournalUnavailable => {
            "durable_journal_unavailable"
        }
        swarm_kernel::KernelAdmissionFault::InitializationFailed
        | swarm_kernel::KernelAdmissionFault::StoreUnavailable
        | swarm_kernel::KernelAdmissionFault::ShuttingDown => "store_unavailable",
    }
}

fn kernel_lifecycle_name(lifecycle: swarm_kernel::KernelHostLifecycle) -> &'static str {
    match lifecycle {
        swarm_kernel::KernelHostLifecycle::Starting => "starting",
        swarm_kernel::KernelHostLifecycle::Ready => "ready",
        swarm_kernel::KernelHostLifecycle::AdmissionClosed { .. } => "admission_closed",
        swarm_kernel::KernelHostLifecycle::Stopping => "stopping",
        swarm_kernel::KernelHostLifecycle::Stopped => "stopped",
        swarm_kernel::KernelHostLifecycle::Failed => "failed",
    }
}

impl Store {
    /// A periodic durable snapshot covers missed change notifications and restarts.
    pub(crate) fn subscribe_module_demand_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) async fn module_demand_snapshot(
        &self,
        cursor: Option<module_demand::ModuleDemandCursor>,
    ) -> Result<module_demand::ModuleDemandSnapshot> {
        let config = self.config.clone();
        self.run(move |db| module_demand::pending(db, config.as_ref(), cursor.as_ref()))
            .await
    }

    /// One bounded demand snapshot for all legacy host reconcilers. The
    /// listener and Store remain live while disabled workers stay unspawned.
    pub(crate) fn subscribe_legacy_worker_demand_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) async fn legacy_worker_demand_snapshot(&self) -> Result<LegacyWorkerDemand> {
        let config = self.config.clone();
        self.run(move |db| legacy_worker_demand::snapshot(db, &config))
            .await
    }

    pub(crate) fn automation_scheduler_worker_config_path(&self) -> std::path::PathBuf {
        swarm_automation::worker_config_path(&self.data_dir)
    }

    pub(crate) fn scheduler_owner_token(&self) -> Result<&str> {
        self.automation_scheduler_owner_token
            .as_deref()
            .map(String::as_str)
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_NOT_CONFIGURED",
                    "scheduler process owner token is unavailable",
                )
            })
    }

    pub(crate) async fn begin_automation_scheduler_worker(
        &self,
    ) -> Result<(swarm_contracts::DeclaredServiceScope, String)> {
        if !self.config.automation_scheduler.enabled {
            return Err(Error::new(
                "AUTOMATION_SCHEDULER_DISABLED",
                "independent scheduler worker is not enabled",
            ));
        }
        let owner_token = self
            .automation_scheduler_owner_token
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_NOT_CONFIGURED",
                    "scheduler owner token is unavailable",
                )
            })?
            .clone();
        let scope = self
            .run(|db| automation_scheduler::current_scope(db))
            .await?;
        let launch_id = model::new_id();
        let tx_launch_id = launch_id.clone();
        let tx_scope = scope.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            automation_scheduler::begin_owner(&tx, &tx_scope, &tx_launch_id, &owner_token)?;
            tx.commit()?;
            Ok(())
        })
        .await?;
        Ok((scope, launch_id))
    }

    pub(crate) async fn activate_automation_scheduler_worker(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        launch_id: String,
        identity: Value,
    ) -> Result<()> {
        let owner_token = self
            .automation_scheduler_owner_token
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_NOT_CONFIGURED",
                    "scheduler process owner token is unavailable",
                )
            })?
            .clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            automation_scheduler::activate_owner(&tx, &scope, &launch_id, identity, &owner_token)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn abandon_automation_scheduler_worker(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        launch_id: String,
    ) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            automation_scheduler::abandon_unspawned_owner(&tx, &scope, &launch_id)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn abandon_exited_automation_scheduler_worker(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        launch_id: String,
        identity: Value,
    ) -> Result<()> {
        let owner_token = self
            .automation_scheduler_owner_token
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_NOT_CONFIGURED",
                    "scheduler process owner token is unavailable",
                )
            })?
            .clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            automation_scheduler::abandon_exited_owner(
                &tx,
                &scope,
                &launch_id,
                &identity,
                &owner_token,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn finish_automation_scheduler_worker(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        launch_id: String,
        identity: Value,
    ) -> Result<()> {
        let owner_token = self
            .automation_scheduler_owner_token
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_NOT_CONFIGURED",
                    "scheduler process owner token is unavailable",
                )
            })?
            .clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            automation_scheduler::finish_owner(&tx, &scope, &launch_id, &identity, &owner_token)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Persist only a fixed worker name/state/error code. `host.status` exposes
    /// this bounded readback; a failed Store write is never reported delivered.
    pub(crate) async fn record_legacy_worker_status(
        &self,
        name: &'static str,
        state: &'static str,
        consecutive_failures: u32,
        error_code: Option<String>,
        retry_in_ms: Option<u64>,
    ) -> Result<()> {
        self.record_legacy_worker_status_with_child(
            name,
            state,
            consecutive_failures,
            error_code,
            retry_in_ms,
            None,
        )
        .await
    }

    pub(crate) async fn record_legacy_worker_status_with_child(
        &self,
        name: &'static str,
        state: &'static str,
        consecutive_failures: u32,
        error_code: Option<String>,
        retry_in_ms: Option<u64>,
        child_receipt: Option<Value>,
    ) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            host_lifecycle::update_optional_worker_with_child(
                &tx,
                name,
                state,
                consecutive_failures,
                error_code.as_deref(),
                retry_in_ms,
                child_receipt.as_ref(),
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn module_supervisor_health_readback(
        &self,
    ) -> Result<swarm_supervisor::control::SupervisorChildHealthReadback> {
        self.run(|db: &mut Connection| host_lifecycle::module_supervisor_health_readback(&*db))
            .await
    }

    /// Demand comes from explicitly managed, durable consumer registrations and
    /// their exact selected ScriptRun scope. Catalog inspection never starts a worker.
    pub(crate) async fn managed_bus_service_snapshot(
        &self,
    ) -> Result<Vec<bus_kernel::ManagedBusServiceDemand>> {
        self.run(|db: &mut Connection| bus_kernel::managed_service_demands(&*db))
            .await
    }

    pub(crate) fn subscribe_managed_bus_service_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) async fn record_managed_bus_service_health(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        state: String,
        consecutive_failures: u32,
        error_code: Option<String>,
        retry_in_ms: Option<u64>,
    ) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            bus_kernel::record_managed_service_health(
                &tx,
                &scope,
                &state,
                consecutive_failures,
                error_code.as_deref(),
                retry_in_ms,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Reserve the durable rolling start slot before spawning the dispatcher.
    pub(crate) async fn record_managed_bus_service_start(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
    ) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            bus_kernel::record_managed_service_start(&tx, &scope, model::now_ms()?)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Record a host-verified declared-service owner readback. This does not
    /// advance or replay any bus cursor or admitted action.
    pub(crate) async fn record_managed_bus_owner_readback(
        &self,
        scope: swarm_contracts::DeclaredServiceScope,
        state: bus_kernel::ManagedBusOwnerState,
        receipt_sha256: Option<String>,
    ) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            bus_kernel::record_managed_service_owner_readback(
                &tx,
                &scope,
                state,
                receipt_sha256.as_deref(),
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Trusted status readback before releasing a stale scope's final demand lease.
    pub(crate) async fn module_scope_readback(
        &self,
        module_id: String,
        binding_id: String,
        generation: i64,
    ) -> Result<module_demand::ModuleScopeReadback> {
        self.run(move |db| module_demand::scope_readback(db, &module_id, &binding_id, generation))
            .await
    }

    /// Recover existing bounded source journals without dispatching automations.
    pub(crate) async fn reconcile_module_recovery_journal_page(&self) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = model::now_ms()?;
            automation_dispatch::reconcile_source_intake(&tx, 64, false, now)?;
            automation_intake::reconcile_registered_source_page(
                &tx,
                crate::automation::intake::LocalProducer::HookCommit,
                64,
                now,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn drain_diagnostics(&self, timeout: Duration) -> bool {
        let telemetry = self.telemetry.clone();
        tokio::task::spawn_blocking(move || telemetry.wait_for_idle(timeout))
            .await
            .unwrap_or(false)
    }

    pub(crate) fn diagnostic_stats(&self) -> swarm_telemetry::Stats {
        self.telemetry.stats()
    }

    /// Reconcile the persisted metadata policy into the live Producer only
    /// after the database commit or during startup. The observer's existing
    /// file selector and retention worker remain independent.
    async fn reload_logging_filters(&self) -> Result<()> {
        let filters = self.run(|db| logging::load_filters(db)).await?;
        self.telemetry.replace_scoped_filters(filters).map_err(|_| {
            Error::new(
                "LOGGING_POLICY_LIMIT",
                "diagnostic policy snapshot exceeds the live Producer bound",
            )
        })
    }

    pub(crate) fn kernel_snapshot(&self) -> swarm_kernel::KernelHostSnapshot {
        self.kernel.snapshot()
    }

    pub(crate) fn kernel_status_receiver(
        &self,
    ) -> watch::Receiver<swarm_kernel::KernelHostSnapshot> {
        self.kernel.status_receiver()
    }

    pub(crate) async fn record_host_start(&self) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now_ms = model::now_ms()?;
            host_lifecycle::start(&tx, now_ms)?;
            bus_kernel::reset_managed_service_health_for_host_start(&tx, now_ms)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn record_host_ready(&self) -> Result<()> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            host_lifecycle::ready(&tx, model::now_ms()?)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn record_host_exit(
        &self,
        error_code: Option<String>,
        failed_supervisor: Option<&'static str>,
    ) -> Result<()> {
        self.run(move |db| {
            record_host_exit_on_connection(db, error_code, Vec::new(), failed_supervisor)
        })
        .await
    }
    pub(crate) async fn reconcile_workspace_lifecycle_once(&self) -> Result<Value> {
        // Exact owned-process departure is observed outside the DB owner
        // transaction. Unknown or live starts remain a separate lease fence.
        let owned_departures = self.reconcile_owned_opencode_departures_once().await?;
        let result = self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let sweep = workspace_lifecycle::reconcile(&tx, model::now_ms()?, 32)?;
            tx.commit()?;
            Ok(json!({"examined":sweep.examined,"stale":sweep.stale,"released":sweep.released,"raced":sweep.raced,"owned_departures":owned_departures}))
        }).await?;
        if result["stale"].as_u64().unwrap_or(0) != 0
            || result["released"].as_u64().unwrap_or(0) != 0
        {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(result)
    }

    /// Configuration is local Operator authority; filesystem checks happen in
    /// the later host-owned preparation phase, outside this transaction.
    pub(crate) async fn initialize_workspace_authority(&self) -> Result<Value> {
        let config = self.config.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operator = meta(&tx, LOCAL_OPERATOR_CLIENT_ID_KEY)?
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| Error::new("LOCAL_OPERATOR_MISMATCH", "workspace initialization needs the pinned local Operator"))?;
            require_local_operator(&tx, &operator)?;
            let now = model::now_ms()?;
            workspace::sync_configured_registrations(&tx, &operator, &config, now)?;
            workspace::mark_preparing_unknown(&tx, now)?;
            let recovery_pending = workspace::pending_leases(&tx, &config, 32)?.len();
            tx.commit()?;
            Ok(json!({"configured_projects":config.workspace.projects.len(),"recovery":"preparing_leases_require_readback","recovery_pending_in_bounded_window":recovery_pending}))
        }).await
    }

    /// One bounded host worker. The Store retains intent before Git work,
    /// joins the preparation, then commits verified facts before native open.
    pub(crate) async fn reconcile_launches_once(&self) -> Result<Value> {
        let ids = self
            .run(move |db| launcher::pending_launches(db, 8))
            .await?;
        let mut progressed = 0usize;
        let mut unknown = 0usize;
        for operation_id in ids {
            let config = self.config.clone();
            let intent = operation_id.clone();
            let prepared = self.run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let actor = launcher::launch_actor(&tx, &intent)?;
                let raw: String = tx.query_row("SELECT effective_request_json FROM operations WHERE operation_id=?1", [&intent], |row| row.get(0))?;
                let effective: Value = serde_json::from_str(&raw)?;
                let phase = effective["launch_manifest"]["state"].as_str().unwrap_or("");
                let unknown_workspace = phase == "outcome_unknown"
                    && effective["launch_manifest"]["failure"]["code"] == "workspace_effect_unknown";
                if phase != "pending_workspace" && !unknown_workspace {
                    launcher::reconcile_launch(&tx, &config, &intent, model::now_ms()?)?;
                    tx.commit()?;
                    return Ok(None);
                }
                if let Some(held) = workspace::held_lease_for_operation(&tx, &intent)? {
                    launcher::launch_after_workspace_held(&tx, &actor, &config, &intent, &held, model::now_ms()?)?;
                    tx.commit()?;
                    return Ok(None);
                }
                let ticket = workspace::pending_lease_for_operation(&tx, &config, &intent)?;
                let (plan, reservation, registration, readback_only) = if let Some(ticket) = ticket {
                    if ticket.state == "preparing" {
                        tx.execute("UPDATE workspace_leases SET state='outcome_unknown',updated_at_ms=?2 WHERE lease_id=?1 AND state='preparing'", params![ticket.reservation.lease_id,model::now_ms()?])?;
                    }
                    (ticket.plan,ticket.reservation,ticket.registration,true)
                } else {
                    if unknown_workspace {
                        tx.commit()?;
                        return Ok(None);
                    }
                    let plan = launcher::launch_workspace_plan(&tx, &actor, &intent, &config)?;
                    let reservation = workspace::reserve_lease_for_launch(&tx, &actor, &plan, model::now_ms()?)?;
                    let registration = workspace::get_registration(&tx, &plan.project_id, &config)?;
                    (plan,reservation,registration,false)
                };
                tx.commit()?;
                Ok(Some(LaunchWorkspaceWork { actor, plan, registration, reservation, readback_only }))
            }).await;
            let work = match prepared {
                Ok(Some(work)) => work,
                Ok(None) => {
                    progressed += 1;
                    continue;
                }
                Err(error) => {
                    let closed_admission =
                        self.record_launch_failure(operation_id, error.code).await?;
                    if closed_admission {
                        progressed += 1;
                    } else {
                        unknown += 1;
                    }
                    continue;
                }
            };
            let config = self.config.clone();
            let fs_work = work.reservation.clone();
            let fs_registration = work.registration.clone();
            let fs_plan = work.plan.clone();
            let readback_only = work.readback_only;
            let evidence = tokio::task::spawn_blocking(move || {
                let project = config
                    .forge
                    .projects
                    .get(&fs_plan.project_id)
                    .ok_or_else(|| {
                        Error::new(
                            "WORKSPACE_UNREGISTERED",
                            "launch project mapping is unavailable",
                        )
                    })?;
                if readback_only {
                    crate::workspace::reconcile_workspace(
                        &config.forge,
                        project,
                        &fs_registration,
                        &fs_work,
                        &fs_plan,
                    )
                } else {
                    crate::workspace::prepare_lease(
                        &config.forge,
                        project,
                        &fs_registration,
                        &fs_work,
                        &fs_plan,
                    )
                    .map(|workspace| workspace.evidence().clone())
                }
            })
            .await
            .map_err(|_| Error::new("WORKSPACE_WORKER_FAILED", "workspace worker did not return"))
            .and_then(|result| result);
            let evidence = match evidence {
                Ok(evidence) => evidence,
                Err(error) => {
                    let closed_admission =
                        self.record_launch_failure(operation_id, error.code).await?;
                    if closed_admission {
                        progressed += 1;
                    } else {
                        unknown += 1;
                    }
                    continue;
                }
            };
            let config = self.config.clone();
            let intent = operation_id.clone();
            let result = self
                .run(move |db| {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let actor = launcher::launch_actor(&tx, &intent)?;
                    if !actor.same_authority_identity(&work.actor) {
                        return Err(Error::new(
                            "STALE_LAUNCH",
                            "launch actor changed during workspace preparation",
                        ));
                    }
                    let lease = if work.readback_only {
                        workspace::reconcile_lease_for_launch(
                            &tx,
                            &actor,
                            &work.registration,
                            &work.reservation,
                            &work.plan,
                            &evidence,
                            model::now_ms()?,
                        )?
                    } else {
                        workspace::commit_lease_for_launch(
                            &tx,
                            &actor,
                            &work.registration,
                            &work.reservation,
                            &work.plan,
                            &evidence,
                            model::now_ms()?,
                        )?
                    };
                    let result = launcher::launch_after_workspace_held(
                        &tx,
                        &actor,
                        &config,
                        &intent,
                        &lease,
                        model::now_ms()?,
                    )?;
                    tx.commit()?;
                    Ok(result)
                })
                .await;
            match result {
                Ok(_) => progressed += 1,
                Err(error) => {
                    let closed_admission =
                        self.record_launch_failure(operation_id, error.code).await?;
                    if closed_admission {
                        progressed += 1;
                    } else {
                        unknown += 1;
                    }
                }
            }
        }
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(json!({"progressed":progressed,"outcome_unknown":unknown}))
    }

    async fn record_launch_failure(&self, operation_id: String, safe_code: String) -> Result<bool> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = model::now_ms()?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            let has_preparing_lease: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_leases \
                 WHERE operation_id=?1 AND state='preparing')",
                [&operation_id],
                |row| row.get(0),
            )?;
            let closed_admission = safe_code == "WORKSPACE_GIT_PATH_TOO_LONG"
                && operation["state"] == "queued"
                && operation["binding_id"].is_null()
                && has_preparing_lease;
            if closed_admission {
                let facts = model::canonical(&json!({
                    "status":"admission_rejected",
                    "reason_code":safe_code.clone(),
                    "native_effect_status":"not_attempted",
                    "runtime_observation":"not_performed",
                    "filesystem_cleanup":"not_attempted",
                }))?;
                tx.execute(
                    "UPDATE workspace_leases SET state='stale',
                         clean_state_json=json_set(?2,'$.prior_lease_state',state),
                         updated_at_ms=?3
                     WHERE operation_id=?1 AND state='preparing'",
                    params![operation_id, facts, now],
                )?;
            } else {
                // Preserve an uncertain filesystem effect and never re-create it.
                tx.execute(
                    "UPDATE workspace_leases
                     SET state='outcome_unknown',updated_at_ms=?2
                     WHERE operation_id=?1 AND state='preparing'",
                    params![operation_id, now],
                )?;
            }
            let has_lease: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_leases WHERE operation_id=?1)",
                [&operation_id],
                |row| row.get(0),
            )?;
            let failure = if closed_admission {
                "workspace_admission_rejected"
            } else if !operation["binding_id"].is_null() {
                "binding_effect_unknown"
            } else if has_lease {
                "workspace_effect_unknown"
            } else if safe_code.starts_with("STALE") {
                "workspace_stale_before_effect"
            } else {
                "workspace_admission_rejected"
            };
            let failure_key = format!("launcher:failure:{operation_id}");
            let first_failure = match meta(&tx, &failure_key)? {
                Some(previous) => {
                    // Pre-change rows retain only their latest observation. Use
                    // that as the first retained evidence; earlier history is
                    // unavailable and is not reconstructed here.
                    let retained = if previous["first_failure"].is_object() {
                        &previous["first_failure"]
                    } else {
                        &previous
                    };
                    json!({
                        "code":retained["code"].clone(),
                        "classification":retained["classification"].clone(),
                        "observed_at_ms":retained["observed_at_ms"].clone(),
                    })
                }
                None => json!({
                    "code":safe_code.clone(),
                    "classification":failure,
                    "observed_at_ms":now,
                }),
            };
            set_meta(
                &tx,
                &failure_key,
                &json!({
                    "code":safe_code,
                    "classification":failure,
                    "observed_at_ms":now,
                    "first_failure":first_failure,
                    "closed_admission":closed_admission,
                }),
            )?;
            launcher::fail_launch(&tx, &operation_id, failure, now)?;
            tx.commit()?;
            Ok(closed_admission)
        })
        .await
    }

    pub(crate) async fn reconcile_automations_once(&self) -> Result<Value> {
        let config = self.config.clone();
        let forge_preparation = if config.forge.enabled {
            let preflight_now = model::now_ms()?;
            let demand = self
                .run(move |db| {
                    automation_publication::forge_preparation_demand(db, 16, 64, preflight_now)
                })
                .await?;
            if demand {
                self.prepare_forge_execution(config.clone()).await
            } else {
                forge::ForgeExecutionPreparation::Skipped
            }
        } else {
            forge::ForgeExecutionPreparation::Skipped
        };
        let mut result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let now = model::now_ms()?;
                let review_dispatch = automation_dispatch::reconcile(&tx, &config, 16, 64, now)?;
                let work_dispatch = automation_work_dispatch::reconcile(&tx, &config, 16, 64, now)?;
                let publication = automation_publication::reconcile(
                    &tx,
                    &config,
                    16,
                    64,
                    now,
                    &forge_preparation,
                )?;
                let goal_progression =
                    automation_goal_progression::reconcile(&tx, 16, 64, now, |tx, admission| {
                        admit_goal_progression_operation(tx, admission, &config, now)
                    })?;
                let github_projection = automation_github_projection::reconcile(&tx, 16, 16, now)?;
                // The cursor, pending reasons, semantic slot and Operation are
                // durable before the host can observe an admitted action.
                tx.commit()?;
                Ok(json!({
                    "review_dispatch":review_dispatch,
                    "work_dispatch":work_dispatch,
                    "publication":publication,
                    "goal_progression":goal_progression,
                    "github_projection":github_projection
                }))
            })
            .await?;
        // Projection effects are invoked only after their cursor and Operations commit.
        result["github_projection_effects"] =
            automation_github_projection::reconcile_effects_once(self, 16).await?;
        // Script bundle preparation performs file I/O after the durable
        // event cursor and pending causes have committed.
        result["script_run"] = self.reconcile_script_triggers_once(16).await?;
        Ok(result)
    }

    /// Goal reminders use the same durable scheduler wake and transaction owner.
    pub(crate) async fn reconcile_goals_once(&self, now: i64) -> Result<Option<i64>> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            goals::reconcile(&tx, now, 64)?;
            let next_due = goals::next_due_at_ms(&tx)?;
            tx.commit()?;
            Ok(next_due)
        })
        .await
    }

    pub(crate) async fn reconcile_review_dispositions_once(&self) -> Result<Value> {
        let config = self.config.clone();
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let result = review_disposition::reconcile(&tx, &config, 16, 64, model::now_ms()?)?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        let decisions = self.verify_automation_acceptances_once().await?;
        Ok(json!({"review_results":result,"acceptance_verifications":decisions}))
    }

    /// The normal artifact verifier runs off the DB thread. Only exact retained
    /// task.accept Operations are resumed, including after host recovery has
    /// marked an interrupted verification outcome_unknown.
    async fn verify_automation_acceptances_once(&self) -> Result<Value> {
        let operation_ids = self
            .run(move |db| {
                let mut statement = db.prepare(
                    "SELECT operation_id FROM operations WHERE method='task.accept' \
                 AND caller_id=?1 AND state IN ('queued','outcome_unknown') \
                 AND json_type(effective_request_json,'$.automation_on_behalf')='object' \
                 ORDER BY created_at_ms,operation_id LIMIT 8",
                )?;
                let ids = statement
                    .query_map(
                        [crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                        |row| row.get::<_, String>(0),
                    )?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(ids)
            })
            .await?;
        let mut decisions = Vec::with_capacity(operation_ids.len());
        for id in operation_ids {
            let start_id = id.clone();
            let Some(work) = self
                .run(move |db| acceptance::begin_on_behalf(db, &start_id))
                .await?
            else {
                continue;
            };
            let outcome = match work {
                Ok(records) => {
                    self.file_io(move |files| {
                        for record in &records {
                            files.verify(record)?;
                        }
                        Ok(())
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            let finish_id = id.clone();
            self.run(move |db| acceptance::finish_on_behalf(db, &finish_id, outcome))
                .await?;
            let read_id = id.clone();
            let operation = self
                .run(move |db| operations::get_operation(db, &read_id))
                .await?;
            decisions.push(json!({
                "operation_id":id,
                "state":operation["state"],
                "outcome":operation["result"]["outcome"],
                "task_accepted":operation["result"]["task_accepted"]
            }));
        }
        Ok(json!({"processed":decisions.len(),"decisions":decisions}))
    }

    /// Scoped watches share the host reconciler. Notification headers are
    /// passive facts and never enqueue native work.
    pub(crate) async fn reconcile_watches_once(&self) -> Result<Value> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let result = coordination_watch::reconcile(&tx, 32, model::now_ms()?)?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }

    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.kernel
            .submit(Job::Run(Box::new(move |db| {
                let _ = tx.send(f(db));
            })))
            .await
            .map_err(map_kernel_submit_error)?;
        rx.await
            .map_err(|_| Error::new("STORE_CLOSED", "database operation lost its response"))?
    }
    async fn message_send(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        let (response, receive) = oneshot::channel();
        self.kernel
            .submit(Job::Batch(message_batch::Request {
                principal,
                method,
                params,
                response,
            }))
            .await
            .map_err(map_kernel_submit_error)?;
        receive
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "database operation lost its response"))?
    }
    pub async fn authenticate(&self, credential: Credential) -> Result<Principal> {
        self.run(move |db| {
            let value = meta(db, &format!("client:{}", credential.client_id))?
                .ok_or_else(|| Error::new("UNAUTHORIZED", "unknown client or credential"))?;
            if value["internal_only"] == true {
                return Err(Error::new(
                    "UNAUTHORIZED",
                    "internal principals have no transport credentials",
                ));
            }
            let expected = value["token_hash"].as_str().unwrap_or("");
            if expected != model::digest(credential.token.as_bytes()) || value["disabled"] == true {
                return Err(Error::new("UNAUTHORIZED", "unknown client or credential"));
            }
            let role: Role = serde_json::from_value(value["role"].clone())?;
            if role == Role::Operator {
                require_local_operator(db, &credential.client_id)?;
            }
            let principal = Principal {
                link_id: model::new_id(),
                client_id: credential.client_id,
                role,
            };
            if principal.role == Role::ModuleSupervisor {
                module_handshake::require_supervisor_scope(db, &principal)?;
            }
            Ok(principal)
        })
        .await
    }
    pub async fn call(&self, principal: Principal, method: String, params: Value) -> Result<Value> {
        if principal.client_id == automation_scheduler::SERVICE_ID {
            if !matches!(
                method.as_str(),
                "automation.scheduler.page" | "automation.scheduler.admit"
            ) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "scheduler Module may call only its exact page and admission methods",
                ));
            }
            return automation_scheduler::call(self, principal, &method, params).await;
        }
        if principal.role == Role::ModuleSupervisor {
            return self.module_supervisor_call(principal, method, params).await;
        }
        if principal.role == Role::HookSource
            && !matches!(method.as_str(), "hook.emit" | "hook.source.get")
        {
            return Err(Error::new(
                "FORBIDDEN",
                "hook credentials may emit and read only their source",
            ));
        }
        if principal.role == Role::Participant && !participant_method_allowed(&method) {
            return Err(Error::new(
                "FORBIDDEN",
                "method is outside this participant's scoped surface",
            ));
        }
        if method == "host.status" {
            let mut status = self.status_reader.host_status(principal, params).await?;
            let status_object = status.as_object_mut().ok_or_else(|| {
                Error::new(
                    "STORE_STATUS_INVALID",
                    "host status response is not an object",
                )
            })?;
            status_object.insert(
                "kernel_host".into(),
                kernel_host_status_value(self.kernel.snapshot()),
            );
            return Ok(status);
        }
        if matches!(method.as_str(), "monitor.snapshot" | "monitor.follow") {
            let mut monitor = self
                .status_reader
                .monitor(principal, method.clone(), params)
                .await?;
            if method == "monitor.snapshot" {
                let state = monitor
                    .get_mut("state")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| {
                        Error::new(
                            "STORE_MONITOR_INVALID",
                            "monitor snapshot state is not an object",
                        )
                    })?;
                let host = state
                    .get_mut("host")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| {
                        Error::new(
                            "STORE_MONITOR_INVALID",
                            "monitor snapshot host state is not an object",
                        )
                    })?;
                host.insert(
                    "kernel_host".into(),
                    kernel_host_status_value(self.kernel.snapshot()),
                );
            }
            return Ok(monitor);
        }
        if method.starts_with("script.") {
            return self.script_call(principal, method, params).await;
        }
        if method.starts_with("github.") {
            if method == "github.effect.managed_label" {
                return github_effects::call(self, principal, params).await;
            }
            if method == "github.effect.reconcile_managed_label" {
                return github_effects::reconcile_call(self, principal, params).await;
            }
            if method == "github.pull_request.update_description" {
                return github_pr_effects::call(self, principal, params).await;
            }
            if method == "github.pull_request.reconcile_description" {
                return github_pr_effects::reconcile_call(self, principal, params).await;
            }
            return self.github_call(principal, method, params).await;
        }
        if matches!(
            method.as_str(),
            "hook.source.setup" | "hook.emit" | "hook.source.get"
        ) {
            return self.hook_call(principal, method, params).await;
        }
        if matches!(method.as_str(), "message.send" | "coordination.send") {
            return self.message_send(principal, method, params).await;
        }
        if method == "forge.publish_ref" {
            return self.publish_ref(principal, params).await;
        }
        if method == "module.hello" {
            let p = principal.clone();
            let v = params.clone();
            let mut plan = self.run(move |db| runtime::hello_plan(db, &p, &v)).await?;
            let inspect = plan.clone();
            let new_owner = params.get("managed_owner").cloned();
            self.file_io(move |_| {
                if let Some(owner) = new_owner {
                    let token = model::text(&owner, "token")?;
                    if uuid::Uuid::parse_str(token).is_err()
                        || owner["process"]["purpose"] != "module"
                        || crate::platform::process_group::departed_empty(&owner["process"], token)?
                    {
                        return Err(Error::invalid(
                            "managed module owner must identify a live local process group",
                        ));
                    }
                }
                if inspect["changed"] == true && !inspect["owner"].is_null() {
                    crate::runtime::owner::verify_departed(&inspect["owner"])?;
                }
                Ok(())
            })
            .await?;
            plan["departed"] = json!(plan["changed"] == true && !plan["owner"].is_null());
            return self
                .run(move |db| runtime::hello(db, &principal, &params, &plan))
                .await;
        }
        if method == "source.capture" {
            return self.capture_source(principal, params).await;
        }
        if method == "check.run" {
            return self.check_run(principal, params).await;
        }
        if method == "schedule.run_now" {
            return self.schedule_run_now(principal, params).await;
        }
        if method == "task.accept" {
            return self.accept_task(principal, params).await;
        }
        if method == "task.submit" {
            return self.submit_task(principal, params).await;
        }
        if method == "task.submit.recover" {
            return self.recover_task_submission(principal, params).await;
        }
        if method == "module.result" {
            return self.persist_result(principal, params).await;
        }
        if method == "artifact.assemble" {
            return self.assemble_artifact(principal, params).await;
        }
        if method == "artifact.read" {
            return self.read_artifact(principal, params).await;
        }
        if method == "doctor.inspect" {
            // Read-only diagnostics over already recorded facts. The database
            // side runs on the DB thread like every read; the filesystem side
            // is metadata-only and is attached here, where the data directory
            // is known. Doctor performs no mutation or repair.
            model::fields(&params, &[])?;
            let config = self.config.clone();
            let mut inspection = self
                .run(move |db| {
                    let p = current_principal(db, principal)?;
                    if matches!(p.role, Role::Module | Role::Participant) {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "module credentials serve only their native binding",
                        ));
                    }
                    crate::doctor::inspect(db, &config)
                })
                .await?;
            crate::doctor::attach_filesystem(&mut inspection, &self.data_dir);
            return Ok(inspection.report);
        }

        if method == "module.next" {
            model::fields(&params, &[])?;
            let mut changed = self.changed.subscribe();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let p = principal.clone();
                let config = self.config.clone();
                let result = self
                    .run(move |db| runtime::next_with_config(db, &p, &config))
                    .await?;
                if !result["command"].is_null() || result.get("rejected_operation_id").is_some() {
                    return Ok(result);
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return Ok(result),
                    r = changed.changed() => { if r.is_err() {return Err(Error::new("STORE_CLOSED","host stopped"));} }
                }
            }
        }
        let wake_dispatch = matches!(
            method.as_str(),
            "check.run"
                | "check.cancel"
                | "task.release"
                | "operation.cancel"
                | "agent.open"
                | "task.dispatch"
                | "agent.send"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.background"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.result"
                | "agent.recover"
                | "host.mode"
                | "module.outcome"
                | "module.event"
                | "bus.consumer.admit"
                | "swarm.launch"
                | "coordination.watch.create"
                | "coordination.watch.cancel"
                | "automation.config.apply"
                | "event.emit"
                | "automation.config.transfer"
                | "review.assign"
                | "review.submit"
                | "task.request_changes"
                | "goal.create"
                | "goal.revise"
                | "goal.enable"
                | "goal.disable"
                | "goal.readback"
                | "hook.source.revoke"
        );
        let config = self.config.clone();
        let reload_logging = method == "logging.set";
        let read_logging = method == "logging.get";
        let result = self
            .run(move |db| {
                let principal = current_principal(db, principal)?;
                if principal.role == Role::Module {
                    return match method.as_str() {
                        "module.outcome" => runtime::outcome(db, &principal, &params),
                        "module.event" => runtime::event(db, &principal, &params),
                        "module.observe" => runtime::observe(db, &principal, &params),
                        "bus.events.page" => {
                            bus_kernel::read(db, &principal, &method, &params, &config)
                        }
                        "bus.consumer.admit" => mutate(db, &principal, &method, &params, &config),
                        _ => Err(Error::new(
                            "FORBIDDEN",
                            "module credentials serve only their explicitly scoped methods",
                        )),
                    };
                }
                if method == "mcp.authorization" {
                    return mcp_authorization(db, &principal, &params);
                }
                if principal.role == Role::Participant {
                    if method == "review.submit" {
                        // This one terminal result has its own retained-slot
                        // guard; Task liveness must not erase late evidence.
                        return mutate(db, &principal, &method, &params, &config);
                    }
                    if swarm_contracts::method_policy::participant_coordination_read(&method) {
                        if matches!(method.as_str(), "concilium.get" | "concilium.list") {
                            return concilium::read(db, &principal, &method, &params);
                        }
                        if matches!(
                            method.as_str(),
                            "coordination.thread.get" | "coordination.thread.list"
                        ) {
                            return coordination_threads::read(db, &principal, &method, &params);
                        }
                        if matches!(
                            method.as_str(),
                            "code.scope.inspect" | "code.scope.conflicts"
                        ) {
                            return code_scopes::read(db, &principal, &method, &params);
                        }
                        if method == "coordination.agreement.get" {
                            return integration::read(db, &principal, &method, &params);
                        }
                        if method == "swarm.overlap.check" {
                            return integration::read(db, &principal, &method, &params);
                        }
                        if method == "coordination.watch.list" {
                            return coordination_watch::read(db, &principal, &params);
                        }
                        if method == "operation.get" {
                            let id = model::text(&params, "operation_id")?;
                            if concilium::authorize_operation_read(db, &principal, id).is_ok() {
                                model::fields(&params, &["operation_id"])?;
                                return operations::get_operation(db, id);
                            }
                            if reviews::authorize_operation_read(db, &principal, id).is_ok() {
                                model::fields(&params, &["operation_id"])?;
                                return operations::get_operation(db, id);
                            }
                            let is_thread_operation: bool = db.query_row(
                                "SELECT EXISTS(SELECT 1 FROM operations WHERE operation_id=?1 AND method IN ('coordination.thread.open','coordination.message.send','coordination.thread.resolve','coordination.thread.withdraw','coordination.thread.supersede'))",
                                [id],
                                |row| row.get(0),
                            )?;
                            if is_thread_operation
                                && coordination_threads::authorize_operation_read(
                                    db, &principal, id,
                                )
                                .is_ok()
                            {
                                model::fields(&params, &["operation_id"])?;
                                return operations::get_operation(db, id);
                            }
                            if integration::authorize_operation_read(db, &principal, id).is_ok() {
                                model::fields(&params, &["operation_id"])?;
                                return operations::get_operation(db, id);
                            }
                            let own_code_scope_receipt: bool = db.query_row(
                                "SELECT EXISTS(SELECT 1 FROM operations WHERE operation_id=?1 AND caller_id=?2 AND method IN ('code.scope.propose','code.scope.release'))",
                                params![id, principal.client_id],
                                |row| row.get(0),
                            )?;
                            if own_code_scope_receipt {
                                model::fields(&params, &["operation_id"])?;
                                return operations::get_operation(db, id);
                            }
                        }
                        return coordination::read(db, &principal, &method, &params);
                    }
                    if matches!(
                        method.as_str(),
                        "review.get" | "review.list" | "swarm.review.context"
                    ) {
                        return reviews::read(db, &principal, &method, &params);
                    }
                    if matches!(method.as_str(), "task.submission" | "check.get") {
                        reviews::authorize_evidence_read(db, &principal, &method, &params)?;
                        return read(db, &principal, &method, &params, &config);
                    }
                    if method == "coordination.integration.ack"
                        || matches!(method.as_str(), "code.scope.propose" | "code.scope.release")
                    {
                        return mutate(db, &principal, &method, &params, &config);
                    }
                    if swarm_contracts::method_policy::participant_coordination_mutation(&method) {
                        if matches!(
                            method.as_str(),
                            "coordination.thread.open"
                                | "coordination.message.send"
                                | "coordination.thread.resolve"
                                | "coordination.thread.withdraw"
                                | "coordination.thread.supersede"
                                | "coordination.contract.propose"
                                | "coordination.contract.respond"
                        ) {
                            return mutate(db, &principal, &method, &params, &config);
                        }
                        if matches!(
                            method.as_str(),
                            "concilium.propose" | "concilium.position.submit"
                        ) {
                            concilium::authorize_participant_mutation(
                                db, &principal, &method, &params,
                            )?;
                            return mutate(db, &principal, &method, &params, &config);
                        }
                        coordination::authorize_participant_mutation(db, &principal, &method)?;
                        return mutate(db, &principal, &method, &params, &config);
                    }
                    return Err(Error::new(
                        "FORBIDDEN",
                        "method is outside this participant's scoped surface",
                    ));
                }
                if is_read(&method) {
                    return read(db, &principal, &method, &params, &config);
                }
                principal.require_writer()?;
                mutate(db, &principal, &method, &params, &config)
            })
            .await;
        let result = if reload_logging {
            match result {
                Ok(mut value) => match self.reload_logging_filters().await {
                    Ok(()) => Ok(value),
                    Err(reload_error) => {
                        // The Store mutation has already committed. Return
                        // its receipt with a bounded apply status instead of
                        // turning a durable policy into an opaque transport
                        // error or inviting an unsafe duplicate retry.
                        value["apply"] = json!("reload_failed");
                        value["apply_error"] = json!({
                            "code": reload_error.code,
                            "durable": true,
                            "live_producer": "previous_snapshot_retained",
                        });
                        Ok(value)
                    }
                },
                Err(error) => Err(error),
            }
        } else {
            result
        };
        let result = if read_logging {
            result.map(|mut value| {
                let runtime = logging::producer_projection(&self.telemetry, &value);
                value["runtime"] = runtime;
                value
            })
        } else {
            result
        };
        if result.is_ok() && wake_dispatch {
            self.changed.send_modify(|n| *n = n.wrapping_add(1));
        }
        result
    }

    async fn register_module_descriptor(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<Value> {
        model::fields(&params, &["descriptor"])?;
        let descriptor: swarm_contracts::module_catalog::ModuleDescriptor =
            serde_json::from_value(params["descriptor"].clone()).map_err(|_| {
                Error::new(
                    "MODULE_DESCRIPTOR_INVALID",
                    "descriptor payload is malformed",
                )
            })?;
        descriptor
            .validate()
            .map_err(|error| Error::new("MODULE_DESCRIPTOR_INVALID", error.to_string()))?;
        let result = self
            .run(move |db| {
                let principal = current_principal(db, principal)?;
                module_handshake::require_supervisor_scope(db, &principal)?;
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let result = module_handshake::register_trusted_descriptor(&tx, descriptor)?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        if result["registered"] == true {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(result)
    }

    /// Authenticated, bounded control surface consumed by the independent
    /// module supervisor. The supervisor never receives a Store handle or a
    /// database connection; every operation below remains a kernel-owned
    /// transaction or an admission snapshot.
    async fn module_supervisor_call(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        let authorization = principal.clone();
        self.run(move |db| {
            let current = current_principal(db, authorization)?;
            module_handshake::require_supervisor_scope(db, &current)
        })
        .await?;

        match method.as_str() {
            "module.descriptor.register" => {
                self.register_module_descriptor(principal, params).await
            }
            "module.supervisor.admission" => {
                model::fields(&params, &[])?;
                let value = match self.kernel.snapshot().admission {
                    swarm_kernel::KernelAdmissionState::Open => {
                        json!({"state":"open"})
                    }
                    swarm_kernel::KernelAdmissionState::Starting => {
                        json!({"state":"closed","fault":"store_unavailable"})
                    }
                    swarm_kernel::KernelAdmissionState::Closed { fault } => json!({
                        "state":"closed",
                        "fault": supervisor_kernel_fault_name(fault),
                    }),
                };
                Ok(value)
            }
            "module.supervisor.demand.page" => {
                model::fields(&params, &["cursor"])?;
                let cursor = match params.get("cursor") {
                    None | Some(Value::Null) => None,
                    Some(value) => Some(serde_json::from_value(value.clone()).map_err(|_| {
                        Error::invalid("cursor does not match the bounded module page shape")
                    })?),
                };
                serde_json::to_value(self.module_demand_snapshot(cursor).await?).map_err(Into::into)
            }
            "module.supervisor.scope.readback" => {
                model::fields(&params, &["module_id", "binding_id", "generation"])?;
                let module_id = model::text(&params, "module_id")?.to_owned();
                let binding_id = model::text(&params, "binding_id")?.to_owned();
                let generation = model::positive(&params, "generation")?;
                serde_json::to_value(
                    self.module_scope_readback(module_id, binding_id, generation)
                        .await?,
                )
                .map_err(Into::into)
            }
            "module.supervisor.credential.ensure" | "module.supervisor.credential.ready" => {
                model::fields(&params, &["operation_id", "binding_id", "generation"])?;
                let operation_id = model::text(&params, "operation_id")?.to_owned();
                let binding_id = model::text(&params, "binding_id")?.to_owned();
                let generation = model::positive(&params, "generation")?;
                let credential = if method == "module.supervisor.credential.ensure" {
                    self.ensure_module_binding_credential(&operation_id, &binding_id, generation)
                        .await?
                } else {
                    self.check_module_binding_credential_ready(
                        &operation_id,
                        &binding_id,
                        generation,
                    )
                    .await?
                };
                serde_json::to_value(credential).map_err(Into::into)
            }
            "module.supervisor.recovery.reconcile" => {
                model::fields(&params, &[])?;
                self.reconcile_module_recovery_journal_page().await?;
                Ok(json!({"reconciled":true}))
            }
            "module.supervisor.observation.record" => {
                model::fields(&params, &["observation"])?;
                let observation = params
                    .get("observation")
                    .cloned()
                    .ok_or_else(|| Error::invalid("observation is required"))?;
                self.record_module_supervisor_observation(observation)
                    .await?;
                Ok(json!({"recorded":true}))
            }
            "module.supervisor.health.record" => {
                model::fields(
                    &params,
                    &[
                        "name",
                        "state",
                        "consecutive_failures",
                        "error_code",
                        "retry_in_ms",
                        "child",
                    ],
                )?;
                if model::text(&params, "name")? != "module-supervisor" {
                    return Err(Error::invalid("unsupported module supervisor health name"));
                }
                let state = match model::text(&params, "state")? {
                    "dormant" => "dormant",
                    "running" => "running",
                    "retry_wait" => "retry_wait",
                    "isolated" => "isolated",
                    _ => return Err(Error::invalid("invalid module supervisor health state")),
                };
                let consecutive_failures = params
                    .get("consecutive_failures")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| Error::invalid("consecutive_failures must be a u32"))?;
                let error_code = match params.get("error_code") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(value)) => Some(value.clone()),
                    Some(_) => return Err(Error::invalid("error_code must be a string or null")),
                };
                let retry_in_ms = match params.get("retry_in_ms") {
                    None | Some(Value::Null) => None,
                    Some(value) => Some(
                        value
                            .as_u64()
                            .ok_or_else(|| Error::invalid("retry_in_ms must be a u64 or null"))?,
                    ),
                };
                let child_receipt = match params.get("child") {
                    None | Some(Value::Null) => None,
                    Some(value) => {
                        let child: swarm_supervisor::control::SupervisorChildHealth =
                            serde_json::from_value(value.clone()).map_err(|_| {
                                Error::invalid("child health receipt has an invalid typed shape")
                            })?;
                        child.validate().map_err(|_| {
                            Error::invalid("child health receipt failed its bounded validation")
                        })?;
                        Some(serde_json::to_value(child).map_err(|_| {
                            Error::invalid("child health receipt could not be retained")
                        })?)
                    }
                };
                self.record_legacy_worker_status_with_child(
                    "module-supervisor",
                    state,
                    consecutive_failures,
                    error_code,
                    retry_in_ms,
                    child_receipt,
                )
                .await?;
                Ok(json!({"recorded":true}))
            }
            "module.supervisor.health.read" => {
                model::fields(&params, &[])?;
                serde_json::to_value(self.module_supervisor_health_readback().await?)
                    .map_err(Into::into)
            }
            _ => Err(Error::new(
                "FORBIDDEN",
                "method is outside the module supervisor control surface",
            )),
        }
    }

    /// Hook issuance/ingress has a fixed repository scope. Setup acknowledges
    /// a credential already retained privately by the caller before dispatch.
    async fn hook_call(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        let config = self.config.clone();
        match method.as_str() {
            "hook.source.setup" => {
                let request = crate::hooks::contract::HookSetupRequest::parse(&params)?;
                self.run(move |db| {
                    let p = current_principal(db, principal)?;
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let response =
                        hooks::setup_source(&tx, &p, &config, &request, model::now_ms()?)?;
                    tx.commit()?;
                    Ok(json!({"source":response.public_value()}))
                })
                .await
            }
            "hook.emit" => {
                model::fields(&params, &["source_id", "commit_oid", "client_request_id"])?;
                let source_id = model::text(&params, "source_id")?.to_owned();
                let commit_oid = model::text(&params, "commit_oid")?.to_owned();
                let p = principal.clone();
                let scope_config = config.clone();
                let scope = self
                    .run(move |db| {
                        let p = current_principal(db, p)?;
                        hooks::emit_scope(db, &p, &scope_config, &source_id)
                    })
                    .await?;
                let verify_scope = scope.clone();
                let snapshot = tokio::task::spawn_blocking(move || {
                    crate::hooks::git::resolve_commit(&verify_scope, &commit_oid)
                })
                .await
                .map_err(|_| {
                    Error::new("HOOK_GIT_FAILED", "hook Git verification worker ended")
                })??;
                let result = self
                    .run(move |db| {
                        let p = current_principal(db, principal)?;
                        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                        let result =
                            hooks::emit(&tx, &p, &config, &scope, &snapshot, model::now_ms()?)?;
                        tx.commit()?;
                        Ok(result)
                    })
                    .await?;
                if result["recorded"] == true {
                    self.changed.send_modify(|n| *n = n.wrapping_add(1));
                }
                Ok(result)
            }
            "hook.source.get" => {
                model::fields(&params, &["source_id", "after", "limit"])?;
                let source_id = model::text(&params, "source_id")?.to_owned();
                let after = match params.get("after") {
                    None => 0,
                    Some(value) => value
                        .as_i64()
                        .filter(|after| *after >= 0)
                        .ok_or_else(|| Error::invalid("after must be a nonnegative integer"))?,
                };
                let limit = match params.get("limit") {
                    None => 20,
                    Some(value) => value
                        .as_u64()
                        .filter(|limit| (1..=64).contains(limit))
                        .ok_or_else(|| Error::invalid("limit must be 1..64"))?
                        as usize,
                };
                self.run(move |db| {
                    let p = current_principal(db, principal)?;
                    hooks::get(db, &p, &source_id, after, limit, &config)
                })
                .await
            }
            _ => Err(Error::new("METHOD_NOT_FOUND", method)),
        }
    }

    async fn file_io<T: Send + 'static>(
        &self,
        f: impl FnOnce(ArtifactFiles) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .artifact_io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("ARTIFACT_IO_CLOSED", "artifact writer stopped"))?;
        let files = self.artifacts.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(files)
        })
        .await
        .map_err(|e| Error::new("ARTIFACT_IO_ERROR", e.to_string()))?
    }
    async fn persist_result(&self, principal: Principal, params: Value) -> Result<Value> {
        model::fields(&params, &["operation_id", "page"])?;
        let op = model::text(&params, "operation_id")?.to_string();
        let page: ResultPage = serde_json::from_value(params["page"].clone())?;
        let p = principal.clone();
        let source = page.source.clone();
        let normalized_result_page = source["schema_id"]
            == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID;
        let command_result_page = !normalized_result_page
            && matches!(
                page.source["kind"].as_str(),
                Some("command_status" | "command_output")
            );
        let mut metadata = self
            .run(move |db| {
                if command_result_page {
                    command_results::prepare(db, &p, &op, &source)
                } else {
                    results::prepare(db, &p, &op, &source)
                }
            })
            .await?;
        let bytes = page.decode()?;
        if normalized_result_page {
            normalized_result::validate_page(&page, &metadata, &bytes)?;
        }
        if metadata["selector"]["kind"] == "input_status" {
            let status_bytes = serde_json::to_vec(&json!({
                "status":"native_input_admitted",
                "input_operation_id":metadata["target_operation_id"],
                "native_session_id":metadata["selector"]["session_id"],
                "native_input_id":page.source["native_input_id"],
                "input_message_sha256":page.source["input_message_sha256"],
                "task_completion":"unknown",
                "execution_complete":false
            }))?;
            let offset = usize::try_from(page.offset_bytes)
                .map_err(|_| Error::invalid("result offset is too large"))?;
            let length = usize::try_from(page.byte_length)
                .map_err(|_| Error::invalid("result length is too large"))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| Error::invalid("result byte range overflow"))?;
            if page.total_bytes != status_bytes.len() as u64
                || end > status_bytes.len()
                || bytes.as_slice() != &status_bytes[offset..end]
            {
                return Err(Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "input status bytes do not match the validated source metadata",
                ));
            }
        } else if metadata["selector"]["kind"] == "antigravity_status" {
            let status_bytes = results::antigravity_status_page_bytes(&metadata)?;
            let offset = usize::try_from(page.offset_bytes)
                .map_err(|_| Error::invalid("result offset is too large"))?;
            let length = usize::try_from(page.byte_length)
                .map_err(|_| Error::invalid("result length is too large"))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| Error::invalid("result byte range overflow"))?;
            if page.total_bytes != status_bytes.len() as u64
                || end > status_bytes.len()
                || bytes.as_slice() != &status_bytes[offset..end]
            {
                return Err(Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "Antigravity status bytes do not match the validated Operation receipt",
                ));
            }
        } else if metadata["selector"]["kind"] == "command_status" {
            let status_bytes = command_results::page_bytes(&page.source)?;
            let offset = usize::try_from(page.offset_bytes)
                .map_err(|_| Error::invalid("result offset is too large"))?;
            let length = usize::try_from(page.byte_length)
                .map_err(|_| Error::invalid("result length is too large"))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| Error::invalid("result byte range overflow"))?;
            if page.total_bytes != status_bytes.len() as u64
                || end > status_bytes.len()
                || bytes.as_slice() != &status_bytes[offset..end]
            {
                return Err(Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "Command status bytes do not match the retained terminal Operation receipt",
                ));
            }
        }
        if !normalized_result_page && metadata["selector"]["kind"] == "command_output" {
            command_results::validate_output_page(&page, &metadata, &bytes)?;
        }
        if metadata["requested_offset"].as_u64() != Some(page.offset_bytes)
            || page.byte_length > metadata["requested_length"].as_u64().unwrap_or(0)
        {
            return Err(Error::invalid(
                "result page differs from the admitted byte range",
            ));
        }
        if let Value::Object(fields) = page.metadata() {
            for (key, value) in fields {
                metadata[key] = value;
            }
        }
        let record = ArtifactFiles::record(
            model::text(&metadata, "operation_id")?,
            &bytes,
            metadata.clone(),
        );
        let saved = record.clone();
        self.file_io(move |files| files.publish(&saved, &bytes))
            .await?;
        let command_result_page = !normalized_result_page
            && matches!(
                record.metadata["selector"]["kind"].as_str(),
                Some("command_status" | "command_output")
            );
        let result = self
            .run(move |db| {
                if command_result_page {
                    command_results::record(db, &principal, &record)
                } else {
                    results::record(db, &principal, &record)
                }
            })
            .await?;
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(result)
    }
    async fn submit_task(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if !matches!(p.role, Role::Operator | Role::Manager | Role::Participant) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "submission requires an assigned Participant, Attempt owner, current GM, or operator",
                    ));
                }
                mutate(db, &p, "task.submit", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_string();
        let start_id = id.clone();
        let p = principal.clone();
        if let Some((candidate, document)) = self
            .run(move |db| submissions::begin(db, p, &start_id))
            .await?
        {
            let file_id = id.clone();
            let outcome = self
                .file_io(move |files| {
                    files.verify(&candidate)?;
                    let (record, bytes) = ArtifactFiles::submission(&file_id, &document)?;
                    files.publish(&record, &bytes)?;
                    Ok(record)
                })
                .await;
            self.run(move |db| submissions::finish(db, principal, &id, outcome))
                .await?;
        }
        Ok(receipt)
    }
    async fn recover_task_submission(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if !matches!(p.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "submission recovery requires the current GM or operator",
                    ));
                }
                mutate(db, &p, "task.submit.recover", &params, &config)
            })
            .await?;
        let recovery_id = model::text(&receipt, "operation_id")?.to_owned();
        let p = principal.clone();
        let start_id = recovery_id.clone();
        let start = self
            .run(move |db| submissions::begin_recovery(db, p, &start_id))
            .await?;
        match start {
            submissions::SubmissionRecoveryStart::Complete(value) => Ok(value),
            submissions::SubmissionRecoveryStart::Verify {
                target_operation_id,
                expected_artifact,
            } => {
                let verified = self
                    .file_io(move |files| files.verify_existing(&expected_artifact))
                    .await?;
                let finish_id = recovery_id;
                let value = self
                    .run(move |db| {
                        submissions::finish_recovery(
                            db,
                            principal,
                            &finish_id,
                            &target_operation_id,
                            verified,
                        )
                    })
                    .await?;
                self.changed.send_modify(|n| *n = n.wrapping_add(1));
                Ok(value)
            }
        }
    }
    async fn accept_task(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                gm::require_authority(db, &p)?;
                mutate(db, &p, "task.accept", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_owned();
        let start_id = id.clone();
        let p = principal.clone();
        if let Some(work) = self
            .run(move |db| acceptance::begin(db, p, &start_id))
            .await?
        {
            let outcome = match work {
                Ok(records) => {
                    self.file_io(move |files| {
                        for record in &records {
                            files.verify(record)?;
                        }
                        Ok(())
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            self.run(move |db| acceptance::finish(db, principal, &id, outcome))
                .await?;
        }
        // Like submission, acceptance keeps its original admission receipt. The
        // current decision outcome is available via operation.get/task.acceptance.
        Ok(receipt)
    }
    async fn assemble_artifact(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if !matches!(p.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "assembly requires a manager or operator",
                    ));
                }
                mutate(db, &p, "artifact.assemble", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_string();
        let start_id = id.clone();
        if let Some((request, pages)) = self
            .run(move |db| assembly::begin(db, principal, &start_id))
            .await?
        {
            let file_id = id.clone();
            let outcome = self
                .file_io(move |files| {
                    files.assemble(&file_id, &pages, request.expected_sha256.as_deref())
                })
                .await;
            self.run(move |db| assembly::finish(db, &id, outcome))
                .await?;
        }
        // Always return the same admission receipt, including after reconnect.
        // operation.get provides the current file-processing result.
        Ok(receipt)
    }
    async fn read_artifact(&self, principal: Principal, params: Value) -> Result<Value> {
        model::fields(&params, &["artifact_id", "offset_bytes", "length_bytes"])?;
        let id = model::text(&params, "artifact_id")?.to_string();
        let integer = |name: &str, fallback| -> Result<u64> {
            params.get(name).map_or(Ok(fallback), |v| {
                v.as_u64()
                    .ok_or_else(|| Error::invalid(format!("{name} must be a nonnegative integer")))
            })
        };
        let offset = integer("offset_bytes", 0)?;
        let length = integer("length_bytes", MAX_PAGE_BYTES as u64)?;
        if length == 0 || length > MAX_PAGE_BYTES as u64 {
            return Err(Error::invalid("length_bytes must be 1..65536"));
        }
        let record = self
            .run(move |db| {
                let p = current_principal(db, principal)?;
                if p.role == Role::Module {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "module cannot inspect other results",
                    ));
                }
                reviews::authorize_artifact_read(db, &p, &id)?;
                let artifact = results::get(db, &id)?;
                if matches!(
                    artifact.kind.as_str(),
                    "script_bundle" | "script_result" | "script_output"
                ) {
                    scripts::authorize_artifact_read(db, &p, &artifact)?;
                }
                Ok(artifact)
            })
            .await?;
        self.file_io(move |files| files.read(&record, offset, length as usize))
            .await
    }
    pub async fn disconnected(&self, principal: Principal) -> Result<()> {
        let client_id = principal.client_id.clone();
        let link_id = principal.link_id.clone();
        let changed = match self
            .run(move |db| runtime::disconnected(db, &principal))
            .await
        {
            Ok(changed) => changed,
            Err(error) => {
                use swarm_telemetry::{Code, Kind, Phase, Record, Severity};
                let _ = self.telemetry.emit(
                    Record::new(
                        Severity::Error,
                        Kind::StoreOperationFailed,
                        Phase::StoreDisconnect,
                    )
                    .with_client_id(Some(&client_id))
                    .with_link_id(Some(&link_id))
                    .with_code(Some(Code::DisconnectPersistenceFailed)),
                );
                return Err(error);
            }
        };
        if changed {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }
}
fn current_principal(db: &Connection, principal: Principal) -> Result<Principal> {
    if principal.role == Role::Scheduler
        && principal.client_id == model::INTERNAL_SCHEDULER_CLIENT_ID
    {
        return Ok(principal);
    }
    let current = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "client no longer registered"))?;
    if current["disabled"] == true {
        return Err(Error::new("UNAUTHORIZED", "client disabled"));
    }
    let role: Role = serde_json::from_value(current["role"].clone())?;
    if role == Role::Operator {
        require_local_operator(db, &principal.client_id)?;
    }
    let principal = Principal { role, ..principal };
    if principal.role == Role::ModuleSupervisor {
        module_handshake::require_supervisor_scope(db, &principal)?;
    }
    Ok(principal)
}

fn require_local_operator(db: &Connection, client_id: &str) -> Result<()> {
    let local_operator = meta(db, LOCAL_OPERATOR_CLIENT_ID_KEY)?;
    if local_operator.as_ref().and_then(Value::as_str) != Some(client_id) {
        return Err(Error::new(
            "LOCAL_OPERATOR_MISMATCH",
            "operator identity does not match the bootstrap credential",
        ));
    }
    Ok(())
}

fn record_host_exit_on_connection(
    db: &mut Connection,
    error_code: Option<String>,
    secondary_codes: Vec<String>,
    failed_supervisor: Option<&'static str>,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    host_lifecycle::finish(
        &tx,
        error_code.as_deref(),
        &secondary_codes,
        failed_supervisor,
        model::now_ms()?,
    )?;
    tx.commit()?;
    Ok(())
}

fn open_recovery_database(root: &Path) -> Result<Connection> {
    let opened = swarm_store::open_existing_writer(
        &root.join("swarm.db"),
        swarm_store::SchemaIdentity {
            application_id: APPLICATION_ID,
            user_version: 1,
            base_schema: SCHEMA,
        },
        swarm_store::WriterOptions::default(),
        |_, is_new| {
            if is_new {
                Err(Error::new(
                    "STORE_CLOSED",
                    "lifecycle recovery requires the existing kernel database",
                ))
            } else {
                Ok(())
            }
        },
    );
    let (db, ()) = match opened {
        Ok(value) => value,
        Err(swarm_store::OpenError::Store(error)) => return Err(error.into()),
        Err(swarm_store::OpenError::Initializer(error)) => return Err(error),
    };
    Ok(db)
}

fn open_database(
    root: &Path,
    credential: &Credential,
    module_supervisor_credential: &Credential,
    config: &Config,
    automation_scheduler_credential: Option<&Credential>,
    retained_scheduler_scope: Option<&swarm_contracts::DeclaredServiceScope>,
    automation_scheduler_config_sha256: Option<&str>,
) -> Result<Connection> {
    let opened = swarm_store::open_writer(
        &root.join("swarm.db"),
        swarm_store::SchemaIdentity {
            application_id: APPLICATION_ID,
            user_version: 1,
            base_schema: SCHEMA,
        },
        swarm_store::WriterOptions::default(),
        |tx, is_new| {
            initialize_database(
                tx,
                is_new,
                credential,
                module_supervisor_credential,
                config,
                automation_scheduler_credential,
                retained_scheduler_scope,
                automation_scheduler_config_sha256,
            )
        },
    );
    let (db, ()) = match opened {
        Ok(value) => value,
        Err(swarm_store::OpenError::Store(error)) => return Err(error.into()),
        Err(swarm_store::OpenError::Initializer(error)) => return Err(error),
    };
    Ok(db)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Database initialization keeps the transaction, existing/new state, bootstrap credentials, config, and retained scheduler pins explicit at one boundary."
)]
fn initialize_database(
    tx: &Transaction<'_>,
    is_new: bool,
    credential: &Credential,
    module_supervisor_credential: &Credential,
    config: &Config,
    automation_scheduler_credential: Option<&Credential>,
    retained_scheduler_scope: Option<&swarm_contracts::DeclaredServiceScope>,
    automation_scheduler_config_sha256: Option<&str>,
) -> Result<()> {
    if is_new {
        set_meta(tx, "controller_id", &json!(model::new_id()))?;
        set_meta(tx, "host_epoch", &json!(0))?;
        set_meta(tx, "execution_mode", &json!({"new_work":"enabled"}))?;
        set_meta(
            tx,
            LOCAL_OPERATOR_CLIENT_ID_KEY,
            &json!(credential.client_id),
        )?;
        set_meta(
            tx,
            &format!("client:{}", credential.client_id),
            &json!({"role":"operator","token_hash":model::digest(credential.token.as_bytes()),"disabled":false}),
        )?;
    } else {
        let local_operator = meta(tx, LOCAL_OPERATOR_CLIENT_ID_KEY)?;
        if let Some(local_operator) = &local_operator
            && local_operator.as_str() != Some(credential.client_id.as_str())
        {
            return Err(Error::new(
                "LOCAL_OPERATOR_MISMATCH",
                "bootstrap credential does not match the pinned local operator identity",
            ));
        }
        let record = meta(tx, &format!("client:{}", credential.client_id))?.ok_or_else(|| {
            Error::new(
                "UNAUTHORIZED",
                "operator credential does not match database",
            )
        })?;
        if record["role"] != "operator"
            || record["token_hash"] != model::digest(credential.token.as_bytes())
            || record["disabled"] == true
        {
            return Err(Error::new(
                "UNAUTHORIZED",
                "operator credential does not match database",
            ));
        }
        if local_operator.is_none() {
            set_meta(
                tx,
                LOCAL_OPERATOR_CLIENT_ID_KEY,
                &json!(credential.client_id),
            )?;
        }
    }
    let workspace_digest = json!(model::digest(WORKSPACE_SCHEMA.as_bytes()));
    match meta(tx, "schema_extension:workspace:v1")? {
        Some(digest) if digest == workspace_digest => {}
        Some(_) => {
            return Err(Error::new(
                "SCHEMA_MISMATCH",
                "workspace extension content differs",
            ));
        }
        None => {
            // Do not adopt arbitrary pre-existing tables as our authority.
            let occupied: i64 = tx.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('workspace_registrations','workspace_leases')",
                [], |row| row.get(0),
            )?;
            if occupied != 0 {
                return Err(Error::new(
                    "SCHEMA_MISMATCH",
                    "unregistered workspace extension tables already exist",
                ));
            }
            tx.execute_batch(WORKSPACE_SCHEMA)?;
            set_meta(tx, "schema_extension:workspace:v1", &workspace_digest)?;
        }
    }
    let owned_digest = json!(model::digest(OWNED_SERVICE_SCHEMA.as_bytes()));
    match meta(tx, "schema_extension:owned_services:v1")? {
        Some(digest) if digest == owned_digest => {}
        Some(_) => {
            return Err(Error::new(
                "SCHEMA_MISMATCH",
                "owned service extension content differs",
            ));
        }
        None => {
            let occupied: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='owned_service_starts')",
                [],
                |row| row.get(0),
            )?;
            if occupied {
                return Err(Error::new(
                    "SCHEMA_MISMATCH",
                    "unregistered owned service table already exists",
                ));
            }
            tx.execute_batch(OWNED_SERVICE_SCHEMA)?;
            set_meta(tx, "schema_extension:owned_services:v1", &owned_digest)?;
        }
    }
    install_schema_extension(
        tx,
        "schema_extension:scripts:v1",
        SCRIPT_SCHEMA,
        &["scripts", "script_revisions", "script_runs"],
    )?;
    script_event_schema::install(tx)?;
    operation_failure_event_schema::install(tx)?;
    operation_cancel_event_schema::install(tx)?;
    install_schema_extension(
        tx,
        "schema_extension:github:v1",
        GITHUB_SCHEMA,
        &[
            "github_sources",
            "github_issue_items",
            "github_issue_facts",
            "github_work_pool_members",
            "github_poll_leases",
        ],
    )?;
    install_schema_extension(
        tx,
        "schema_extension:github_label_effects:v1",
        GITHUB_EFFECTS_SCHEMA,
        &["github_label_effect_slots"],
    )?;
    install_schema_extension(
        tx,
        "schema_extension:github_pr_effects:v1",
        GITHUB_PR_EFFECTS_SCHEMA,
        &["github_pr_effect_slots"],
    )?;
    let scheduler_key = format!("client:{}", model::INTERNAL_SCHEDULER_CLIENT_ID);
    match meta(tx, &scheduler_key)? {
        None => set_meta(
            tx,
            &scheduler_key,
            &json!({
                "role": "scheduler", "internal_only": true, "disabled": false
            }),
        )?,
        Some(record) if record["role"] == "scheduler" && record["internal_only"] == true => {}
        Some(_) => {
            return Err(Error::new(
                "INTERNAL_CLIENT_CONFLICT",
                "reserved scheduler identity already has a transport registration",
            ));
        }
    }
    let c8_native_mcp_key = "client:swarm.internal.c8.native_mcp";
    let c8_native_mcp_record = json!({
        "role": "module",
        "internal_only": true,
        "disabled": false,
    });
    match meta(tx, c8_native_mcp_key)? {
        None => set_meta(tx, c8_native_mcp_key, &c8_native_mcp_record)?,
        Some(existing) if existing == c8_native_mcp_record => {}
        Some(_) => {
            return Err(Error::new(
                "INTERNAL_CLIENT_CONFLICT",
                "reserved native MCP caller identity has incompatible registration",
            ));
        }
    }
    if module_supervisor_credential.client_id != model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID {
        return Err(Error::new(
            "INTERNAL_CLIENT_CONFLICT",
            "reserved module supervisor credential has the wrong identity",
        ));
    }
    let supervisor_key = format!("client:{}", model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID);
    let supervisor_record = json!({
        "role":"module_supervisor",
        "token_hash":model::digest(module_supervisor_credential.token.as_bytes()),
        "disabled":false,
        "internal_only":false,
        "module_scope":"descriptor_catalog",
        "capabilities":[
            "module.descriptor.register",
            "module.supervisor.admission",
            "module.supervisor.demand.page",
            "module.supervisor.scope.readback",
            "module.supervisor.credential.ensure",
            "module.supervisor.credential.ready",
            "module.supervisor.recovery.reconcile",
            "module.supervisor.observation.record",
            "module.supervisor.health.record",
            "module.supervisor.health.read"
        ],
    });
    match meta(tx, &supervisor_key)? {
        None => set_meta(tx, &supervisor_key, &supervisor_record)?,
        Some(existing)
            if module_handshake::supervisor_scope_matches(
                model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
                &existing,
            ) && existing["disabled"] != true =>
        {
            set_meta(tx, &supervisor_key, &supervisor_record)?;
        }
        Some(_) => {
            return Err(Error::new(
                "INTERNAL_CLIENT_CONFLICT",
                "reserved module supervisor identity has incompatible scope",
            ));
        }
    }
    if config.automation_scheduler.enabled {
        let service_credential = automation_scheduler_credential.ok_or_else(|| {
            Error::new(
                "AUTOMATION_SERVICE_IDENTITY_INVALID",
                "enabled scheduler has no host-issued Module credential",
            )
        })?;
        let config_sha256 = automation_scheduler_config_sha256.ok_or_else(|| {
            Error::new(
                "AUTOMATION_SERVICE_CONFIG_INVALID",
                "enabled scheduler has no canonical config digest",
            )
        })?;
        automation_scheduler::provision(
            tx,
            service_credential,
            retained_scheduler_scope,
            config_sha256,
        )?;
    }
    let epoch = meta(tx, "host_epoch")?
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("EPOCH_OVERFLOW", "host epoch exhausted"))?;
    set_meta(tx, "host_epoch", &json!(epoch))?;
    tx.execute(
        "UPDATE operations SET state='outcome_unknown',result_json=CASE \
         WHEN method='github.effect.managed_label' AND state='sending' THEN \
           json_set(COALESCE(result_json,'{}'), \
             '$.outcome','outcome_unknown', \
             '$.readback','required', \
             '$.write_attempted',json('true'), \
             '$.current_state_read_method','operation.get') \
         WHEN method='github.pull_request.update_description' AND state='sending' THEN \
           json_set(COALESCE(result_json,'{}'), \
             '$.outcome','outcome_unknown', \
             '$.readback','required', \
             '$.write_attempted',json('true'), \
             '$.current_state_read_method','operation.get') \
         WHEN method='forge.publish_ref' AND state='sending' THEN \
           json_set(COALESCE(result_json,'{}'), \
             '$.outcome','unknown', \
             '$.publication','operator_intervention_required', \
             '$.process_tree_unconfirmed',json('true'), \
             '$.process_tree_status','unconfirmed_after_restart', \
             '$.process_tree_cleanup','host_lifecycle_interrupted_before_confirmation', \
             '$.resolution','manual_operator_intervention_required', \
             '$.reason','process_tree_unconfirmed') \
         ELSE result_json END,updated_at_ms=?1 \
         WHERE state IN ('sending','native_accepted')",
        [model::now_ms()?],
    )?;
    tx.execute(
        "UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.connection','disconnected') WHERE state='ready' AND released_at_ms IS NULL",
        [],
    )?;
    tx.execute(
        "UPDATE check_runs SET state='reconciling' WHERE state='running'",
        [],
    )?;
    tx.execute(
        "UPDATE script_runs SET state='reconciling' WHERE state='running'",
        [],
    )?;
    Ok(())
}
pub(super) fn meta(db: &Connection, key: &str) -> Result<Option<Value>> {
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |r| {
            r.get(0)
        })
        .optional()?;
    raw.map(|s| serde_json::from_str(&s).map_err(Into::into))
        .transpose()
}
pub(super) fn set_meta(db: &Connection, key: &str, value: &Value) -> Result<()> {
    db.execute("INSERT INTO meta(key,value_json) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json", params![key,model::canonical(value)?])?;
    Ok(())
}
/// Add a known, source-pinned extension without adopting unrelated existing tables.
fn install_schema_extension(
    tx: &Transaction<'_>,
    key: &str,
    schema: &str,
    tables: &[&str],
) -> Result<()> {
    let expected = json!(model::digest(schema.as_bytes()));
    match meta(tx, key)? {
        Some(digest) if digest == expected => Ok(()),
        Some(_) => Err(Error::new(
            "SCHEMA_MISMATCH",
            format!("{key} extension content differs"),
        )),
        None => {
            for table in tables {
                let occupied: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                    [table],
                    |row| row.get(0),
                )?;
                if occupied {
                    return Err(Error::new(
                        "SCHEMA_MISMATCH",
                        format!("unregistered {key} table exists"),
                    ));
                }
            }
            tx.execute_batch(schema)?;
            set_meta(tx, key, &expected)
        }
    }
}

fn is_read(method: &str) -> bool {
    swarm_contracts::method_policy::is_read_only(method)
}

fn participant_method_allowed(method: &str) -> bool {
    swarm_contracts::method_policy::participant_allowed(method)
}
// Watch ownership is checked against the live assignment in its handler;
// these two methods are shared with Manager/Operator identities.
fn participant_only_mutation(method: &str) -> bool {
    swarm_contracts::method_policy::participant_coordination_mutation(method)
        && !matches!(
            method,
            "coordination.watch.create" | "coordination.watch.cancel"
        )
}

fn page(params: &Value) -> Result<(i64, i64)> {
    let integer = |name, default| -> Result<i64> {
        match params.get(name) {
            None => Ok(default),
            Some(value) => value
                .as_i64()
                .ok_or_else(|| Error::invalid(format!("{name} must be an integer"))),
        }
    };
    let limit = integer("limit", 50)?;
    let after = integer("after", 0)?;
    if !(1..=200).contains(&limit) || after < 0 {
        return Err(Error::invalid("limit must be 1..200 and after nonnegative"));
    }
    Ok((limit, after))
}

// Public Operation/report reads share this visibility rule for directed
// message receipts. The caller and the exact original recipient may read a
// send; cancellation recipients are resolved against the immutable settled
// send identified by both delivery ID and payload digest. Only the verified
// local operator receives the global diagnostic view.
const OPERATION_VISIBILITY_SQL: &str = r#"(
    :operator = 1
    OR (
        op.method NOT IN ('message.send', 'message.cancel', 'coordination.send', 'coordination.message.send',
            'task.request_changes', 'check.run', 'check.cancel', 'event.emit')
        AND op.method NOT LIKE 'coordination.%'
        AND op.method NOT LIKE 'concilium.%'
        AND op.method NOT LIKE 'review.%'
        AND op.method NOT LIKE 'automation.%'
        AND op.method NOT LIKE 'script.%'
        AND op.method NOT LIKE 'goal.%'
        AND op.method NOT LIKE 'hook.%'
        AND op.method NOT LIKE 'github.%'
        AND op.caller_id != 'eliot-internal-automation-v1'
    )
    OR op.caller_id = :client
    OR (
        op.method LIKE 'concilium.%'
        AND EXISTS (
            SELECT 1 FROM meta AS link
            JOIN meta AS manager ON manager.key='client:' || :client
            WHERE link.key='concilium:v1:operation:' || op.operation_id
              AND json_extract(link.value_json,'$.schema_version')=1
              AND json_extract(link.value_json,'$.task_id')=op.task_id
              AND json_extract(link.value_json,'$.attempt_id')=op.attempt_id
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
              AND (
                  json_extract(link.value_json,'$.manager_id')=:client
                  OR EXISTS (
                      SELECT 1 FROM meta AS current_gm
                      WHERE current_gm.key='gm'
                        AND json_extract(current_gm.value_json,'$.client_id')=:client
                  )
                  OR EXISTS (
                      SELECT 1 FROM tasks AS target
                      JOIN attempts AS current_attempt ON current_attempt.task_id=target.task_id
                      WHERE target.task_id=op.task_id
                        AND current_attempt.task_id=target.task_id
                        AND current_attempt.attempt_id=op.attempt_id
                        AND current_attempt.owner_id=:client
                        AND current_attempt.released_at_ms IS NULL
                  )
              )
        )
    )
    OR (
        ((op.method LIKE 'script.%' AND op.method != 'script.run') OR op.method LIKE 'goal.%'
         OR op.method LIKE 'hook.%' OR op.method LIKE 'github.%')
        AND EXISTS (
            SELECT 1 FROM meta AS manager JOIN meta AS current_gm ON current_gm.key='gm'
            WHERE manager.key='client:' || :client
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
              AND json_extract(current_gm.value_json,'$.client_id')=:client
        )
    )
    OR (
        op.method = 'script.run'
        AND EXISTS (
            SELECT 1 FROM script_runs AS run
            JOIN attempts AS target_attempt ON target_attempt.attempt_id=run.attempt_id
            JOIN tasks AS target ON target.task_id=run.task_id AND target.task_id=target_attempt.task_id
            JOIN meta AS manager ON manager.key='client:' || :client
            JOIN meta AS current_gm ON current_gm.key='gm'
            WHERE run.operation_id=op.operation_id AND run.task_id=op.task_id AND run.attempt_id=op.attempt_id
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
              AND json_extract(current_gm.value_json,'$.client_id')=:client
        )
    )
    OR (
        op.method = 'script.run'
        AND op.task_id IS NOT NULL AND op.attempt_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM script_runs AS run
            JOIN meta AS link ON link.key='automation:v1:operation:' || op.operation_id
            JOIN meta AS manager ON manager.key='client:' || :client
            WHERE run.operation_id=op.operation_id
              AND run.task_id=op.task_id AND run.attempt_id=op.attempt_id
              AND run.task_revision IS NOT NULL AND run.task_revision > 0
              AND json_extract(link.value_json,'$.record.operation_id')=op.operation_id
              AND json_extract(link.value_json,'$.record.action')='script.run'
              AND json_extract(link.value_json,'$.record.technical_requester_id')='eliot-internal-automation-v1'
              AND json_extract(link.value_json,'$.record.effective_manager_id')=:client
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
        )
    )
    OR (
        op.method = 'script.run'
        AND op.task_id IS NULL AND op.attempt_id IS NULL
        AND EXISTS (
            SELECT 1 FROM script_runs AS run
            JOIN meta AS link ON link.key='automation:v1:operation:' || op.operation_id
            JOIN meta AS manager ON manager.key='client:' || :client
            WHERE run.operation_id=op.operation_id
              AND run.task_id IS NULL AND run.task_revision IS NULL AND run.attempt_id IS NULL
              AND json_extract(link.value_json,'$.record.operation_id')=op.operation_id
              AND json_extract(link.value_json,'$.record.action')='script.run'
              AND json_extract(link.value_json,'$.record.technical_requester_id')='eliot-internal-automation-v1'
              AND json_extract(link.value_json,'$.record.cause.kind')='system_event'
              AND json_extract(link.value_json,'$.record.effective_manager_id')=:client
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
        )
    )
    OR (
        op.method LIKE 'goal.%'
        AND EXISTS (
            SELECT 1 FROM attempts AS target JOIN meta AS manager ON manager.key='client:' || :client
            WHERE target.attempt_id=op.attempt_id AND target.task_id=op.task_id
              AND target.owner_id=:client
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
        )
    )
    OR (
        op.method IN ('task.request_changes', 'check.run', 'check.cancel')
        AND EXISTS (
            SELECT 1 FROM attempts AS target
            WHERE target.attempt_id = op.attempt_id AND target.owner_id = :client
        )
    )
    OR (
        op.method IN ('check.run', 'check.cancel')
        AND op.task_id IS NOT NULL
        AND op.attempt_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM check_runs AS check_run
            JOIN attempts AS target_attempt
              ON target_attempt.attempt_id = check_run.attempt_id
            JOIN tasks AS target ON target.task_id = target_attempt.task_id
            JOIN meta AS manager ON manager.key = 'client:' || :client
            JOIN meta AS current_gm ON current_gm.key = 'gm'
            WHERE target_attempt.attempt_id = op.attempt_id
              AND target_attempt.task_id = op.task_id
              AND target_attempt.released_at_ms IS NULL
              AND (SELECT latest.attempt_id FROM attempts AS latest
                   WHERE latest.task_id = target.task_id AND latest.released_at_ms IS NULL
                   ORDER BY latest.created_at_ms DESC, latest.attempt_id DESC LIMIT 1) = op.attempt_id
              AND (check_run.operation_id = op.operation_id
                   OR check_run.check_id = json_extract(op.original_request_json, '$.check_id'))
              AND json_extract(manager.value_json, '$.role') = 'manager'
              AND COALESCE(json_extract(manager.value_json, '$.disabled'), 0) = 0
              AND json_extract(current_gm.value_json, '$.client_id') = :client
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method != 'task.accept'
        AND op.method != 'forge.publish_ref'
        AND EXISTS (
            SELECT 1 FROM meta AS link
            WHERE link.key = 'automation:v1:operation:' || op.operation_id
              AND json_extract(link.value_json, '$.record.operation_id') = op.operation_id
              AND json_extract(link.value_json, '$.record.action') = op.method
              AND json_extract(link.value_json, '$.record.technical_requester_id') = op.caller_id
              AND json_extract(op.effective_request_json, CASE op.method
                    WHEN 'review.assign' THEN '$.on_behalf.effective_manager_id'
                    ELSE '$.automation_on_behalf.effective_manager_id' END)
                  = json_extract(link.value_json, '$.record.effective_manager_id')
              AND (op.method != 'agent.send'
                   OR json_extract(link.value_json, '$.record.cause.kind') = 'goal_progression')
              AND NOT (op.method='message.send'
                       AND json_extract(link.value_json,'$.record.cause.kind')='script_controller_effect')
              AND (
                  json_extract(link.value_json, '$.record.effective_manager_id') = :client
                  OR EXISTS (
                      SELECT 1 FROM meta AS successor_manager JOIN meta AS current_gm ON current_gm.key='gm'
                      JOIN tasks AS target ON target.task_id=op.task_id
                      WHERE successor_manager.key='client:' || :client
                        AND json_extract(successor_manager.value_json,'$.role')='manager'
                        AND COALESCE(json_extract(successor_manager.value_json,'$.disabled'),0)=0
                        AND json_extract(current_gm.value_json,'$.client_id')=:client
                        AND target.project_id=json_extract(link.value_json,'$.record.project_id')
                  )
              )
        )
    )
    OR (
        op.caller_id='eliot-internal-automation-v1'
        AND op.method='message.send'
        AND EXISTS (
            SELECT 1 FROM meta AS link
            JOIN meta AS manager ON manager.key='client:' || :client
            LEFT JOIN meta AS current_gm ON current_gm.key='gm'
            JOIN tasks AS target
              ON target.task_id=json_extract(link.value_json,'$.record.cause.task_id')
            JOIN attempts AS subject
              ON subject.task_id=target.task_id
             AND subject.attempt_id=json_extract(link.value_json,'$.record.cause.attempt_id')
            WHERE link.key='automation:v1:operation:' || op.operation_id
              AND json_extract(link.value_json,'$.record.operation_id')=op.operation_id
              AND json_extract(link.value_json,'$.record.technical_requester_id')=op.caller_id
              AND json_extract(link.value_json,'$.record.action')='message.send'
              AND json_extract(link.value_json,'$.record.cause.kind')='script_controller_effect'
              AND (json_extract(link.value_json,'$.record.effective_manager_id')=:client
                   OR json_extract(current_gm.value_json,'$.client_id')=:client)
              AND json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id')
                  =json_extract(link.value_json,'$.record.effective_manager_id')
              AND json_extract(op.effective_request_json,'$.script_invocation.grant')='task_owner_message'
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.kind')='script_invocation'
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.script_run_operation_id')
                  =json_extract(link.value_json,'$.record.cause.script_run_operation_id')
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.script_run_id')
                  =json_extract(link.value_json,'$.record.cause.script_run_id')
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.identity.task_id')=target.task_id
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.identity.task_revision')=subject.task_revision
              AND json_extract(op.effective_request_json,'$.script_invocation.cause.identity.attempt_id')=subject.attempt_id
              AND json_extract(link.value_json,'$.record.project_id')=target.project_id
              AND subject.task_revision=json_extract(link.value_json,'$.record.cause.task_revision')
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method = 'forge.publish_ref'
        AND EXISTS (
            SELECT 1
            FROM meta AS link
            JOIN operations AS accepted
              ON accepted.operation_id = json_extract(link.value_json, '$.record.cause.operation_id')
            JOIN tasks AS target
              ON target.task_id = json_extract(link.value_json, '$.record.cause.task_id')
            JOIN meta AS manager ON manager.key = 'client:' || :client
            JOIN observations AS accepted_event
              ON accepted_event.observation_id = json_extract(link.value_json, '$.record.cause.observation_id')
            JOIN attempts AS source_attempt
              ON source_attempt.attempt_id = json_extract(link.value_json, '$.record.cause.attempt_id')
            WHERE link.key = 'automation:v1:operation:' || op.operation_id
              AND json_type(link.value_json, '$.record') = 'object'
              AND json_type(link.value_json, '$.record.schema_version') = 'integer'
              AND json_extract(link.value_json, '$.record.schema_version') = 1
              AND json_type(link.value_json, '$.record.operation_id') = 'text'
              AND json_extract(link.value_json, '$.record.operation_id') = op.operation_id
              AND json_type(link.value_json, '$.record.technical_requester_id') = 'text'
              AND json_extract(link.value_json, '$.record.technical_requester_id') = op.caller_id
              AND json_type(link.value_json, '$.record.effective_manager_id') = 'text'
              AND json_type(op.effective_request_json, '$.automation_on_behalf.effective_manager_id') = 'text'
              AND json_extract(op.effective_request_json, '$.automation_on_behalf.effective_manager_id') = json_extract(link.value_json, '$.record.effective_manager_id')
              AND (json_extract(link.value_json, '$.record.effective_manager_id') = :client
                   OR json_extract((SELECT value_json FROM meta WHERE key='gm'), '$.client_id') = :client)
              AND json_type(link.value_json, '$.record.automation_id') = 'text'
              AND length(json_extract(link.value_json, '$.record.automation_id')) > 0
              AND json_type(link.value_json, '$.record.automation_revision') = 'integer'
              AND json_extract(link.value_json, '$.record.automation_revision') > 0
              AND json_type(link.value_json, '$.record.project_id') = 'text'
              AND json_extract(link.value_json, '$.record.project_id') = target.project_id
              AND json_type(link.value_json, '$.record.action') = 'text'
              AND json_extract(link.value_json, '$.record.action') = op.method
              AND json_type(link.value_json, '$.record.linked_at_ms') = 'integer'
              AND json_extract(link.value_json, '$.record.linked_at_ms') >= 0
              AND json_type(link.value_json, '$.record.cause') = 'object'
              AND json_type(link.value_json, '$.record.cause.kind') = 'text'
              AND json_extract(link.value_json, '$.record.cause.kind') = 'task.acceptance'
              AND json_type(link.value_json, '$.record.cause.id') = 'text'
              AND json_extract(link.value_json, '$.record.cause.id') = accepted.operation_id
              AND json_type(link.value_json, '$.record.cause.operation_id') = 'text'
              AND json_extract(link.value_json, '$.record.cause.operation_id') = accepted.operation_id
              AND json_extract(link.value_json, '$.record.cause.task_id') = accepted.task_id
              AND accepted.task_id = target.task_id
              AND accepted.attempt_id = json_extract(link.value_json, '$.record.cause.attempt_id')
              AND json_type(link.value_json, '$.record.cause.observation_id') = 'integer'
              AND json_extract(link.value_json, '$.record.cause.observation_id') > 0
              AND json_type(link.value_json, '$.record.cause.task_id') = 'text'
              AND json_type(link.value_json, '$.record.cause.task_revision') = 'integer'
              AND json_type(link.value_json, '$.record.cause.canonical_repository') = 'text'
              AND length(json_extract(link.value_json, '$.record.cause.canonical_repository')) > 0
              AND json_extract(link.value_json, '$.record.cause.task_revision') > 0
              AND json_type(link.value_json, '$.record.cause.attempt_id') = 'text'
              AND json_type(link.value_json, '$.record.cause.submission_ref') = 'text'
              AND json_type(link.value_json, '$.record.cause.candidate_ref') = 'text'
              AND json_type(link.value_json, '$.record.cause.gm_epoch') = 'integer'
              AND json_extract(link.value_json, '$.record.cause.gm_epoch') > 0
              AND json_type(link.value_json, '$.record.cause.policy_revision') = 'text'
              AND json_type(link.value_json, '$.record.cause.target_ref') = 'text'
              AND json_type(link.value_json, '$.record.cause.expected_old_ref') IN ('null', 'text')
              AND json_type(link.value_json, '$.record.cause.expected_create') IN ('true', 'false')
              AND (
                  (json_extract(link.value_json, '$.record.cause.expected_create') = 1
                   AND json_type(link.value_json, '$.record.cause.expected_old_ref') = 'null')
                  OR (json_extract(link.value_json, '$.record.cause.expected_create') = 0
                      AND json_type(link.value_json, '$.record.cause.expected_old_ref') = 'text')
              )
              AND json_type(link.value_json, '$.record.cause.activation_cut') = 'integer'
              AND json_extract(link.value_json, '$.record.cause.activation_cut') >= 0
              AND json_type(link.value_json, '$.record.cause.historical_replay_authorized') IN ('true', 'false')
              AND json_extract(link.value_json, '$.record.cause.historical_replay_authorized') =
                  CASE WHEN json_extract(link.value_json, '$.record.cause.observation_id')
                                 <= json_extract(link.value_json, '$.record.cause.activation_cut')
                       THEN 1 ELSE 0 END
              AND op.task_id = target.task_id
              AND op.attempt_id = json_extract(link.value_json, '$.record.cause.attempt_id')
              AND json_extract(op.original_request_json, '$.accepted_operation_id') = accepted.operation_id
              AND json_type(op.original_request_json, '$.expected_revision') = 'integer'
              AND json_extract(op.original_request_json, '$.attempt_id') = json_extract(link.value_json, '$.record.cause.attempt_id')
              AND json_extract(op.original_request_json, '$.attempt_id') = accepted.attempt_id
              AND json_extract(op.original_request_json, '$.expected_revision') = json_extract(link.value_json, '$.record.cause.task_revision')
              AND json_extract(op.original_request_json, '$.submission_ref') = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND json_extract(op.original_request_json, '$.candidate_ref') = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND json_extract(op.original_request_json, '$.expected_policy_revision') = json_extract(link.value_json, '$.record.cause.policy_revision')
              AND json_extract(op.original_request_json, '$.target_ref') = json_extract(link.value_json, '$.record.cause.target_ref')
              AND json_extract(op.original_request_json, '$.expected_old_ref') IS json_extract(link.value_json, '$.record.cause.expected_old_ref')
              AND json_type(op.original_request_json, '$.expected_create') IN ('true', 'false')
              AND json_extract(op.original_request_json, '$.expected_create') IS json_extract(link.value_json, '$.record.cause.expected_create')
              AND json_extract(op.effective_request_json, '$.automation_on_behalf.effective_manager_id') = json_extract(link.value_json, '$.record.effective_manager_id')
              AND json_type(op.effective_request_json, '$.publication_intent') = 'object'
              AND json_extract(op.effective_request_json, '$.publication_intent.operation_id') = op.operation_id
              AND json_extract(op.effective_request_json, '$.publication_intent.project_id') = target.project_id
              AND json_extract(op.effective_request_json, '$.publication_intent.canonical_repository') = json_extract(link.value_json, '$.record.cause.canonical_repository')
              AND json_extract(op.effective_request_json, '$.publication_intent.attempt_id') = op.attempt_id
              AND json_extract(op.effective_request_json, '$.publication_intent.task_revision') = json_extract(link.value_json, '$.record.cause.task_revision')
              AND json_extract(op.effective_request_json, '$.publication_intent.admitted_gm_epoch') = json_extract(link.value_json, '$.record.cause.gm_epoch')
              AND json_extract(op.effective_request_json, '$.publication_intent.submission_ref') = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND json_extract(op.effective_request_json, '$.publication_intent.accepted_operation_id') = accepted.operation_id
              AND json_extract(op.effective_request_json, '$.publication_intent.candidate_ref') = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND json_extract(op.effective_request_json, '$.publication_intent.policy_revision') = json_extract(link.value_json, '$.record.cause.policy_revision')
              AND json_extract(op.effective_request_json, '$.publication_intent.target_ref') = json_extract(link.value_json, '$.record.cause.target_ref')
              AND json_extract(op.effective_request_json, '$.publication_intent.expected_old_ref') IS json_extract(link.value_json, '$.record.cause.expected_old_ref')
              AND json_extract(op.effective_request_json, '$.publication_intent.expected_create') IS json_extract(link.value_json, '$.record.cause.expected_create')
              AND json_type(op.effective_request_json, '$.publication_intent.force') = 'false'
              AND json_extract(op.effective_request_json, '$.publication_intent.force') = 0
              AND accepted.method = 'task.accept'
              AND accepted.state = 'settled'
              AND json_extract(accepted.result_json, '$.outcome') = 'applied'
              AND json_type(accepted.result_json, '$.task_accepted') = 'true'
              AND json_extract(accepted.result_json, '$.task_accepted') = 1
              AND json_extract(accepted.result_json, '$.acceptance_operation_id') = accepted.operation_id
              AND json_extract(accepted.result_json, '$.task_id') = target.task_id
              AND json_extract(accepted.result_json, '$.attempt_id') = accepted.attempt_id
              AND json_extract(accepted.result_json, '$.task_revision') = json_extract(link.value_json, '$.record.cause.task_revision')
              AND json_extract(accepted.result_json, '$.submission_ref') = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND json_extract(accepted.result_json, '$.candidate_ref') = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND json_extract(accepted.original_request_json, '$.attempt_id') = accepted.attempt_id
              AND json_extract(accepted.original_request_json, '$.expected_revision') = json_extract(link.value_json, '$.record.cause.task_revision')
              AND json_extract(accepted.original_request_json, '$.submission_ref') = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND json_extract(accepted.original_request_json, '$.candidate_ref') = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND accepted_event.source_stream_id = 'controller:acceptance'
              AND accepted_event.source_event_key = 'accept:' || accepted.operation_id
              AND accepted_event.kind = 'task.acceptance'
              AND accepted_event.operation_id = accepted.operation_id
              AND json_extract(accepted_event.payload_json, '$.outcome') = 'applied'
              AND json_type(accepted_event.payload_json, '$.task_accepted') = 'true'
              AND json_extract(accepted_event.payload_json, '$.task_accepted') = 1
              AND json_extract(accepted_event.payload_json, '$.acceptance_operation_id') = accepted.operation_id
              AND json_extract(accepted_event.payload_json, '$.task_id') = target.task_id
              AND json_extract(accepted_event.payload_json, '$.attempt_id') = accepted.attempt_id
              AND json_extract(accepted_event.payload_json, '$.task_revision') = json_extract(link.value_json, '$.record.cause.task_revision')
              AND json_extract(accepted_event.payload_json, '$.submission_ref') = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND json_extract(accepted_event.payload_json, '$.candidate_ref') = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND source_attempt.task_id = target.task_id
              AND source_attempt.task_revision = json_extract(link.value_json, '$.record.cause.task_revision')
              AND source_attempt.submission_ref = json_extract(link.value_json, '$.record.cause.submission_ref')
              AND source_attempt.candidate_ref = json_extract(link.value_json, '$.record.cause.candidate_ref')
              AND json_extract(manager.value_json, '$.role') = 'manager'
              AND COALESCE(json_extract(manager.value_json, '$.disabled'), 0) = 0
              AND (
                  (SELECT owned.owner_id FROM attempts AS owned
                   WHERE owned.task_id = target.task_id AND owned.released_at_ms IS NULL
                   ORDER BY owned.created_at_ms DESC, owned.attempt_id DESC LIMIT 1) = :client
                  OR EXISTS (
                      SELECT 1 FROM meta AS gm
                      WHERE gm.key = 'gm' AND json_extract(gm.value_json, '$.client_id') = :client
                  )
              )
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method = 'task.accept'
        AND EXISTS (
            SELECT 1 FROM meta AS link JOIN tasks AS target ON target.task_id=op.task_id
              JOIN meta AS manager ON manager.key='client:' || :client
            WHERE link.key='automation:v1:operation:' || op.operation_id
              AND json_extract(link.value_json,'$.record.operation_id')=op.operation_id
              AND json_extract(link.value_json,'$.record.action')='task.accept'
              AND json_extract(link.value_json,'$.record.technical_requester_id')=op.caller_id
              AND json_extract(link.value_json,'$.record.effective_manager_id')=json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id')
              AND (json_extract(link.value_json,'$.record.effective_manager_id')=:client
                   OR EXISTS (SELECT 1 FROM meta AS current_gm WHERE current_gm.key='gm' AND json_extract(current_gm.value_json,'$.client_id')=:client))
              AND json_extract(link.value_json,'$.record.project_id')=target.project_id
              AND json_extract(link.value_json,'$.record.cause.kind')='review_result'
              AND json_extract(link.value_json,'$.record.cause.identity.task_id')=op.task_id
              AND json_extract(link.value_json,'$.record.cause.identity.attempt_id')=op.attempt_id
              AND json_extract(op.original_request_json,'$.attempt_id')=op.attempt_id
              AND json_extract(op.original_request_json,'$.expected_revision')=json_extract(link.value_json,'$.record.cause.identity.task_revision')
              AND json_extract(op.original_request_json,'$.submission_ref')=json_extract(link.value_json,'$.record.cause.identity.submission_ref')
              AND json_extract(op.original_request_json,'$.candidate_ref')=json_extract(link.value_json,'$.record.cause.identity.candidate_ref')
              AND json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id')=json_extract(link.value_json,'$.record.effective_manager_id')
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
              AND (EXISTS (SELECT 1 FROM attempts AS owned WHERE owned.task_id=target.task_id AND owned.owner_id=:client AND owned.released_at_ms IS NULL)
                   OR EXISTS (SELECT 1 FROM meta AS gm WHERE gm.key='gm' AND json_extract(gm.value_json,'$.client_id')=:client))
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method = 'swarm.launch'
        AND EXISTS (
            SELECT 1 FROM meta AS link
            WHERE link.key = 'work-dispatch:v1:operation-link:' || op.operation_id
              AND json_extract(link.value_json, '$.record.operation_id') = op.operation_id
              AND json_extract(link.value_json, '$.record.action') = op.method
              AND json_extract(link.value_json, '$.record.technical_requester_id') = op.caller_id
              AND json_extract(link.value_json, '$.record.effective_manager_id') = json_extract(op.effective_request_json, '$.launch_manifest.actor.effective_manager_id')
              AND (json_extract(link.value_json, '$.record.effective_manager_id') = :client
                   OR EXISTS (SELECT 1 FROM meta AS current_gm WHERE current_gm.key='gm' AND json_extract(current_gm.value_json,'$.client_id')=:client))
              AND json_extract(link.value_json, '$.record.automation_id') = json_extract(op.effective_request_json, '$.launch_manifest.actor.automation_id')
              AND json_extract(link.value_json, '$.record.automation_revision') = json_extract(op.effective_request_json, '$.launch_manifest.actor.automation_revision')
              AND json_extract(link.value_json, '$.record.semantic_slot_id') = json_extract(op.effective_request_json, '$.launch_manifest.actor.semantic_slot_id')
              AND EXISTS (
                  SELECT 1 FROM tasks AS target JOIN meta AS manager
                    ON manager.key = 'client:' || :client
                  WHERE target.task_id = json_extract(link.value_json, '$.record.task_id')
                    AND target.project_id = json_extract(link.value_json, '$.record.project_id')
                    AND json_extract(manager.value_json, '$.role') = 'manager'
                    AND COALESCE(json_extract(manager.value_json, '$.disabled'),0) = 0
                    AND (EXISTS (SELECT 1 FROM attempts AS owned WHERE owned.task_id=target.task_id AND owned.owner_id=:client AND owned.released_at_ms IS NULL)
                         OR EXISTS (SELECT 1 FROM meta AS gm WHERE gm.key='gm' AND json_extract(gm.value_json,'$.client_id')=:client))
              )
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method = 'agent.send'
        AND EXISTS (
            SELECT 1 FROM meta AS link JOIN tasks AS target ON target.task_id=op.task_id
              JOIN meta AS manager ON manager.key='client:' || :client
            WHERE link.key='repair:v1:operation-link:' || op.operation_id
              AND json_extract(link.value_json,'$.record.operation_id')=op.operation_id
              AND json_extract(link.value_json,'$.record.action')='agent.send'
              AND json_extract(link.value_json,'$.record.technical_requester_id')=op.caller_id
              AND json_extract(link.value_json,'$.record.effective_manager_id')=json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id')
              AND (json_extract(link.value_json,'$.record.effective_manager_id')=:client
                   OR EXISTS (SELECT 1 FROM meta AS current_gm WHERE current_gm.key='gm' AND json_extract(current_gm.value_json,'$.client_id')=:client))
              AND json_extract(link.value_json,'$.record.project_id')=target.project_id
              AND json_extract(link.value_json,'$.record.task_id')=op.task_id
              AND json_extract(link.value_json,'$.record.attempt_id')=op.attempt_id
              AND json_extract(link.value_json,'$.record.binding_id')=op.binding_id
              AND json_extract(link.value_json,'$.record.binding_generation')=op.binding_generation
              AND json_extract(op.original_request_json,'$.binding_id')=op.binding_id
              AND json_extract(op.original_request_json,'$.generation')=op.binding_generation
              AND json_extract(op.original_request_json,'$.delivery')='next_turn'
              AND json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id')=json_extract(link.value_json,'$.record.effective_manager_id')
              AND json_extract(op.effective_request_json,'$.automation_on_behalf.semantic_slot_id')=json_extract(link.value_json,'$.record.semantic_slot_id')
              AND json_extract(manager.value_json,'$.role')='manager'
              AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
              AND ((SELECT owned.owner_id FROM attempts AS owned
                    WHERE owned.task_id=target.task_id AND owned.released_at_ms IS NULL
                    ORDER BY owned.created_at_ms DESC,owned.attempt_id DESC LIMIT 1)=:client
                   OR EXISTS (SELECT 1 FROM meta AS gm WHERE gm.key='gm' AND json_extract(gm.value_json,'$.client_id')=:client))
        )
    )
    OR (
        op.caller_id = 'eliot-internal-automation-v1'
        AND op.method IN ('task.claim','coordination.participant.register','agent.open')
        AND EXISTS (
            SELECT 1 FROM operations AS parent JOIN meta AS link
              ON link.key='work-dispatch:v1:operation-link:' || parent.operation_id
            WHERE parent.operation_id = CASE op.method
                WHEN 'task.claim' THEN json_extract(op.effective_request_json,'$.launch_child.launch_operation_id')
                WHEN 'coordination.participant.register' THEN json_extract(op.effective_request_json,'$.launch_registration.launch_operation_id')
                ELSE json_extract(op.effective_request_json,'$.operation_contract.parent_launch_operation_id') END
              AND parent.method='swarm.launch' AND parent.caller_id=op.caller_id
              AND json_extract(link.value_json,'$.record.operation_id')=parent.operation_id
              AND json_extract(link.value_json,'$.record.effective_manager_id')=json_extract(parent.effective_request_json,'$.launch_manifest.actor.effective_manager_id')
              AND (json_extract(link.value_json,'$.record.effective_manager_id')=:client
                   OR EXISTS (SELECT 1 FROM meta AS current_gm WHERE current_gm.key='gm' AND json_extract(current_gm.value_json,'$.client_id')=:client))
              AND json_extract(link.value_json,'$.record.technical_requester_id')=op.caller_id
              AND parent.task_id=json_extract(link.value_json,'$.record.task_id')
              AND json_extract(parent.effective_request_json,'$.launch_manifest.task.task_id')=parent.task_id
              AND json_extract(parent.effective_request_json,'$.launch_manifest.task.observed_revision')=json_extract(link.value_json,'$.record.task_revision')
              AND parent.attempt_id IS json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_id')
              AND CASE WHEN json_extract(link.value_json,'$.record.attempt_id') IS NULL
                       THEN json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_action')='claim_new'
                       ELSE json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_action')='use_existing' END
              AND (json_extract(link.value_json,'$.record.attempt_id') IS NULL OR json_extract(link.value_json,'$.record.attempt_id')=parent.attempt_id)
              AND (parent.attempt_id IS NULL OR EXISTS (SELECT 1 FROM attempts AS bound_attempt
                    WHERE bound_attempt.attempt_id=parent.attempt_id
                      AND bound_attempt.task_id=parent.task_id
                      AND bound_attempt.task_revision=json_extract(link.value_json,'$.record.task_revision')
                      AND bound_attempt.owner_id=json_extract(link.value_json,'$.record.effective_manager_id')))
              AND op.client_request_id='launch:' || parent.operation_id || CASE op.method WHEN 'task.claim' THEN ':claim' WHEN 'coordination.participant.register' THEN ':participant' ELSE ':open' END
              AND CASE op.method
                  WHEN 'task.claim' THEN json_extract(op.original_request_json,'$.task_id')=parent.task_id
                    AND json_extract(op.original_request_json,'$.expected_revision')=json_extract(link.value_json,'$.record.task_revision')
                    AND json_extract(op.original_request_json,'$.owner_id')=json_extract(link.value_json,'$.record.effective_manager_id')
                    AND json_extract(op.original_request_json,'$.start_owner')='controller'
                    AND json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_action')='claim_new'
                    AND ((op.state='rejected' AND op.task_id IS NULL AND op.attempt_id IS NULL
                          AND json_type(op.result_json,'$.code')='text'
                          AND json_extract(op.effective_request_json,'$.receipt.ok')=0
                          AND json_extract(op.effective_request_json,'$.receipt.error.code')=json_extract(op.result_json,'$.code')
                          AND json_extract(op.effective_request_json,'$.receipt.error.message')=json_extract(op.result_json,'$.message'))
                      OR (op.state<>'rejected' AND op.task_id=parent.task_id
                          AND op.attempt_id=json_extract(op.result_json,'$.attempt_id')
                          AND json_extract(op.result_json,'$.operation_id')=op.operation_id
                          AND json_extract(op.result_json,'$.task_id')=op.task_id
                          AND json_extract(op.result_json,'$.created')=1
                          AND json_extract(op.effective_request_json,'$.receipt.ok')=1
                          AND json_extract(op.effective_request_json,'$.receipt.value.operation_id')=op.operation_id
                          AND json_extract(op.effective_request_json,'$.receipt.value.task_id')=op.task_id
                          AND json_extract(op.effective_request_json,'$.receipt.value.attempt_id')=op.attempt_id
                          AND parent.attempt_id=op.attempt_id
                          AND json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_id')=op.attempt_id))
                  WHEN 'coordination.participant.register' THEN json_extract(op.original_request_json,'$.task_id')=parent.task_id
                    AND json_extract(op.original_request_json,'$.task_revision')=json_extract(link.value_json,'$.record.task_revision')
                    AND json_extract(op.original_request_json,'$.attempt_id')=parent.attempt_id
                    AND json_extract(parent.effective_request_json,'$.launch_manifest.task.attempt_id')=parent.attempt_id
                    AND ((op.state='rejected' AND op.task_id IS NULL AND op.attempt_id IS NULL
                          AND json_type(op.result_json,'$.code')='text'
                          AND json_extract(op.effective_request_json,'$.receipt.ok')=0
                          AND json_extract(op.effective_request_json,'$.receipt.error.code')=json_extract(op.result_json,'$.code')
                          AND json_extract(op.effective_request_json,'$.receipt.error.message')=json_extract(op.result_json,'$.message'))
                      OR (op.state<>'rejected' AND op.task_id=parent.task_id AND op.attempt_id=parent.attempt_id
                          AND op.binding_id=parent.binding_id AND op.binding_generation=parent.binding_generation
                          AND json_extract(op.result_json,'$.operation_id')=op.operation_id
                          AND json_extract(op.result_json,'$.task_id')=op.task_id
                          AND json_extract(op.result_json,'$.task_revision')=json_extract(link.value_json,'$.record.task_revision')
                          AND json_extract(op.result_json,'$.attempt_id')=op.attempt_id
                          AND json_extract(op.effective_request_json,'$.receipt.ok')=1
                          AND json_extract(op.effective_request_json,'$.receipt.value.operation_id')=op.operation_id
                          AND json_extract(op.effective_request_json,'$.receipt.value.task_id')=op.task_id
                          AND json_extract(op.effective_request_json,'$.receipt.value.task_revision')=json_extract(link.value_json,'$.record.task_revision')
                          AND json_extract(op.effective_request_json,'$.receipt.value.attempt_id')=op.attempt_id
                          AND EXISTS (SELECT 1 FROM meta AS registration WHERE registration.key='client:' || json_extract(op.original_request_json,'$.client_id')
                            AND json_extract(registration.value_json,'$.created_operation_id')=op.operation_id
                            AND json_extract(registration.value_json,'$.task_id')=op.task_id
                            AND json_extract(registration.value_json,'$.task_revision')=json_extract(link.value_json,'$.record.task_revision')
                            AND json_extract(registration.value_json,'$.attempt_id')=op.attempt_id
                            AND json_extract(registration.value_json,'$.binding_id')=op.binding_id
                            AND json_extract(registration.value_json,'$.binding_generation')=op.binding_generation)))
                  ELSE json_extract(op.original_request_json,'$.route')=json_extract(parent.original_request_json,'$.route')
                    AND json_extract(op.original_request_json,'$.lane_id') LIKE 'launch-%'
                    AND op.prerequisite_operation_id=parent.operation_id
                    AND EXISTS (SELECT 1 FROM workspace_leases AS lease WHERE lease.operation_id=parent.operation_id
                      AND 'launch-' || lease.lease_id=json_extract(op.original_request_json,'$.lane_id')
                      AND lease.project_id=json_extract(link.value_json,'$.record.project_id')
                      AND lease.task_id=parent.task_id
                      AND lease.task_revision=json_extract(link.value_json,'$.record.task_revision')
                      AND lease.attempt_id=parent.attempt_id
                      AND lease.owner_client_id=json_extract(link.value_json,'$.record.effective_manager_id')
                      AND ((op.state='rejected' AND json_type(op.effective_request_json,'$.operation_contract') IS NULL
                            AND op.task_id IS NULL AND op.attempt_id IS NULL
                            AND op.binding_id IS NULL AND op.binding_generation IS NULL
                            AND json_extract(op.effective_request_json,'$.receipt.ok')=0
                            AND json_extract(op.effective_request_json,'$.receipt.error.code')=json_extract(op.result_json,'$.code')
                            AND json_extract(op.effective_request_json,'$.receipt.error.message')=json_extract(op.result_json,'$.message')
                            AND json_type(op.result_json,'$.code')='text')
                        OR (json_type(op.effective_request_json,'$.operation_contract')='object'
                            AND op.task_id=parent.task_id AND op.attempt_id=parent.attempt_id
                            AND op.binding_id=parent.binding_id AND op.binding_generation=parent.binding_generation
                            AND json_extract(op.effective_request_json,'$.operation_contract.effect_scope')='one_exact_launch_binding'
                            AND json_extract(op.effective_request_json,'$.operation_contract.completion_condition')='binding_ready_readback'
                            AND json_extract(op.effective_request_json,'$.operation_contract.replay_policy')='exact_binding_readback_only_after_unknown'
                            AND json_extract(op.effective_request_json,'$.operation_contract.parent_launch_operation_id')=parent.operation_id
                            AND json_extract(op.effective_request_json,'$.workspace_lease.lease_id')=json_extract(parent.effective_request_json,'$.launch_manifest.workspace.lease_authority.lease_id')
                            AND json_extract(op.effective_request_json,'$.workspace_lease.generation')=json_extract(parent.effective_request_json,'$.launch_manifest.workspace.lease_authority.generation')
                            AND json_extract(op.effective_request_json,'$.workspace_lease.binding_digest')=json_extract(parent.effective_request_json,'$.launch_manifest.workspace.lease_authority.binding_digest')
                            AND json_extract(op.effective_request_json,'$.workspace_lease.lease_id')=lease.lease_id
                            AND json_extract(op.effective_request_json,'$.workspace_lease.generation')=lease.generation
                            AND json_extract(op.effective_request_json,'$.workspace_lease.binding_digest')=lease.binding_digest
                            AND json_extract(op.effective_request_json,'$.receipt.ok')=1
                            AND json_extract(op.effective_request_json,'$.receipt.value.binding_id')=op.binding_id
                            AND json_extract(op.effective_request_json,'$.receipt.value.generation')=op.binding_generation
                            AND json_extract(op.effective_request_json,'$.receipt.value.operation_id')=op.operation_id
                            AND json_extract(parent.effective_request_json,'$.launch_manifest.binding.operation_id')=op.operation_id
                            AND json_extract(parent.effective_request_json,'$.launch_manifest.binding.binding_id')=op.binding_id
                            AND json_extract(parent.effective_request_json,'$.launch_manifest.binding.generation')=op.binding_generation))) END
              AND EXISTS (
                  SELECT 1 FROM tasks AS target JOIN meta AS manager ON manager.key='client:' || :client
                  WHERE target.task_id=json_extract(link.value_json,'$.record.task_id')
                    AND target.project_id=json_extract(link.value_json,'$.record.project_id')
                    AND json_extract(manager.value_json,'$.role')='manager'
                    AND COALESCE(json_extract(manager.value_json,'$.disabled'),0)=0
                    AND (EXISTS (SELECT 1 FROM attempts AS owned WHERE owned.task_id=target.task_id AND owned.owner_id=:client AND owned.released_at_ms IS NULL)
                         OR EXISTS (SELECT 1 FROM meta AS gm WHERE gm.key='gm' AND json_extract(gm.value_json,'$.client_id')=:client))
              )
        )
    )
    OR (
        op.method = 'review.submit'
        AND EXISTS (
            SELECT 1 FROM observations AS assignment
            JOIN tasks AS target
              ON target.task_id = json_extract(assignment.payload_json, '$.identity.task_id')
            WHERE assignment.source_stream_id = 'controller:review'
              AND assignment.kind = 'review.assignment'
              AND json_extract(assignment.payload_json, '$.review_assignment_id') =
                  json_extract(op.result_json, '$.review_assignment_id')
              AND json_extract(assignment.payload_json, '$.review_assignment_id') = json_extract(op.result_json, '$.review_assignment_id')
              AND json_extract(assignment.payload_json, '$.reviewer_client_id') = op.caller_id
              AND json_extract(assignment.payload_json, '$.identity.task_id') = op.task_id
              AND json_extract(assignment.payload_json, '$.identity.attempt_id') = op.attempt_id
              AND (
                  (json_extract(op.result_json, '$.sponsor_client_id') = :client
                   AND json_extract(assignment.payload_json, '$.sponsor_client_id') = :client)
                  OR (
                      target.project_id IS NOT NULL
                      AND json_extract(assignment.payload_json, '$.on_behalf.effective_manager_id') = json_extract(assignment.payload_json, '$.sponsor_client_id')
                      AND json_extract(assignment.payload_json, '$.on_behalf.project_id') = target.project_id
                      AND EXISTS (
                          SELECT 1 FROM meta AS manager JOIN meta AS current_gm ON current_gm.key='gm'
                          WHERE manager.key='client:' || :client
                            AND json_extract(manager.value_json, '$.role')='manager'
                            AND COALESCE(json_extract(manager.value_json, '$.disabled'), 0)=0
                            AND json_extract(current_gm.value_json, '$.client_id')=:client
                      )
                  )
              )
        )
    )
    OR (
        op.method IN ('message.send', 'coordination.message.send', 'coordination.send', 'coordination.consult')
        AND op.state = 'settled'
        AND json_type(op.result_json, '$.sender') = 'text'
        AND json_extract(op.result_json, '$.sender') = op.caller_id
        AND json_type(op.result_json, '$.recipient') = 'text'
        AND length(json_extract(op.result_json, '$.recipient')) > 0
        AND json_extract(op.result_json, '$.recipient') = :client
    )
    OR (
        op.method = 'message.cancel'
        AND op.state = 'settled'
        AND EXISTS (
            SELECT 1 FROM operations AS original
            WHERE original.method IN ('message.send', 'coordination.message.send', 'coordination.send', 'coordination.consult')
              AND original.state = 'settled'
              AND json_extract(original.result_json, '$.delivery_id') =
                  json_extract(op.result_json, '$.cancellation.delivery_id')
              AND json_extract(original.result_json, '$.payload_digest') =
                  json_extract(op.result_json, '$.cancellation.payload_digest')
              AND original.caller_id = op.caller_id
              AND json_type(original.result_json, '$.sender') = 'text'
              AND json_extract(original.result_json, '$.sender') = op.caller_id
              AND json_type(original.result_json, '$.recipient') = 'text'
              AND length(json_extract(original.result_json, '$.recipient')) > 0
              AND json_extract(original.result_json, '$.recipient') = :client
              AND (
                  SELECT count(*) FROM operations AS same_identity
                  WHERE same_identity.method IN ('message.send', 'coordination.message.send', 'coordination.send', 'coordination.consult')
                    AND same_identity.state = 'settled'
                    AND json_extract(same_identity.result_json, '$.delivery_id') =
                        json_extract(op.result_json, '$.cancellation.delivery_id')
                    AND json_extract(same_identity.result_json, '$.payload_digest') =
                        json_extract(op.result_json, '$.cancellation.payload_digest')
              ) = 1
        )
    )
)"#;

fn timeline_visibility_sql() -> String {
    format!(
        r#"(
            -- Normalized message lifecycle facts feed the automation bus;
            -- the public timeline retains one raw mailbox delivery per send.
            o.source_stream_id != 'controller:messages'
            AND (
                o.operation_id IS NULL
                OR EXISTS (
                    SELECT 1 FROM operations AS op
                    WHERE op.operation_id = o.operation_id
                      AND {OPERATION_VISIBILITY_SQL}
                )
            )
            AND (
            (
                :mailbox_only = 1
                AND o.kind IN ('message.send', 'coordination.message.send', 'task.feedback', 'check.completed')
                AND json_extract(o.payload_json, '$.recipient') = :client
            )
            OR (
                :mailbox_only = 0
                AND (
                    (
                        o.kind NOT IN ('message.send', 'message.cancel', 'coordination.send', 'coordination.message.send',
                            'task.request_changes', 'task.feedback', 'task.review_stale',
                            'check.run', 'check.cancel', 'check.completed')
                        AND o.kind NOT LIKE 'coordination.%'
                        AND o.kind NOT LIKE 'review.%'
                        AND o.kind NOT LIKE 'automation.%'
                        AND NOT EXISTS (SELECT 1 FROM operations AS automatic WHERE automatic.operation_id=o.operation_id AND automatic.caller_id='eliot-internal-automation-v1')
                    )
                    OR (:operator = 1 AND o.kind IN ('message.send', 'message.cancel', 'coordination.send', 'coordination.message.send'))
                    OR (
                        (
                            o.kind IN ('message.send', 'message.cancel', 'coordination.send', 'coordination.message.send',
                                'task.request_changes', 'task.feedback', 'task.review_stale',
                                'check.run', 'check.cancel', 'check.completed')
                            OR o.kind LIKE 'coordination.%'
                            OR o.kind LIKE 'review.%'
                            OR o.kind LIKE 'automation.%'
                            OR EXISTS (SELECT 1 FROM operations AS automatic WHERE automatic.operation_id=o.operation_id AND automatic.caller_id='eliot-internal-automation-v1')
                        )
                        AND EXISTS (
                            SELECT 1 FROM operations AS op
                            WHERE op.operation_id = o.operation_id
                              AND {OPERATION_VISIBILITY_SQL}
                        )
                    )
                )
            )
            )
        )"#
    )
}

fn operation_visible_to(db: &Connection, p: &Principal, id: &str) -> Result<bool> {
    let caller: Option<String> = db
        .query_row(
            "SELECT caller_id FROM operations WHERE operation_id=?1",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    if caller.as_deref()
        == Some(crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID)
    {
        // SQL performs visibility filtering before page/count decisions;
        // verify the retained link's digest and requester before projection.
        let target =
            if crate::automation::authorization::any_on_behalf_operation_link(db, id)?.is_some() {
                id.to_owned()
            } else if let Some(parent) = launch_child_parent(db, id)? {
                parent
            } else {
                return Ok(false);
            };
        return if p.role == Role::Operator {
            Ok(
                crate::automation::authorization::any_on_behalf_operation_link(db, &target)?
                    .is_some(),
            )
        } else {
            crate::automation::authorization::on_behalf_visible_to(db, p, &target)
        };
    }
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM operations AS op WHERE op.operation_id=:operation_id AND {OPERATION_VISIBILITY_SQL})"
    );
    Ok(db.query_row(
        &sql,
        named_params! {
            ":operation_id": id,
            ":operator": p.role == Role::Operator,
            ":client": &p.client_id,
        },
        |row| row.get(0),
    )?)
}

/// Internal child receipts inherit only their validated exact launch's scope.
/// Public request parameters cannot supply any of these admission contexts.
fn launch_child_parent(db: &Connection, id: &str) -> Result<Option<String>> {
    type RetainedLaunchLeaseRow = (
        String,
        String,
        i64,
        String,
        String,
        String,
        Option<String>,
        String,
        i64,
        String,
    );
    type ChildRow = (
        String,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    );
    let row: Option<ChildRow> = db.query_row(
        "SELECT caller_id,method,client_request_id,effective_request_json,prerequisite_operation_id,
                original_request_json,state,task_id,attempt_id,binding_id,binding_generation,result_json
         FROM operations WHERE operation_id=?1",
        [id],
        |row| {
            Ok((
                row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?,
            ))
        },
    ).optional()?;
    let Some((
        caller,
        method,
        request_id,
        effective,
        prerequisite,
        original,
        state,
        child_task,
        child_attempt,
        child_binding_id,
        child_binding_generation,
        result_raw,
    )) = row
    else {
        return Ok(None);
    };
    if caller != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        return Ok(None);
    }
    let effective: Value = serde_json::from_str(&effective)?;
    let original: Value = serde_json::from_str(&original)?;
    let (context, phase) = match method.as_str() {
        "task.claim" => (&effective["launch_child"], "claim"),
        "coordination.participant.register" => (&effective["launch_registration"], "participant"),
        "agent.open" => (&effective["operation_contract"], "open"),
        _ => return Ok(None),
    };
    let key = if method == "agent.open" {
        "parent_launch_operation_id"
    } else {
        "launch_operation_id"
    };
    let Some(parent_id) = context[key].as_str() else {
        return Ok(None);
    };
    if request_id != format!("launch:{parent_id}:{phase}")
        || (method == "agent.open" && prerequisite.as_deref() != Some(parent_id))
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "launch child linkage is inconsistent",
        ));
    }
    let Some(link) = automation_work_dispatch::operation_link(db, parent_id)? else {
        return Ok(None);
    };
    let parent = operations::get_operation(db, parent_id)?;
    let (parent_original_raw, parent_effective_raw): (String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [parent_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let parent_original: Value = serde_json::from_str(&parent_original_raw)?;
    let parent_effective: Value = serde_json::from_str(&parent_effective_raw)?;
    let manifest = &parent_effective["launch_manifest"];
    let parent_task_id = model::text(&manifest["task"], "task_id")?;
    let parent_revision = model::positive(&manifest["task"], "observed_revision")?;
    let parent_attempt = manifest["task"]["attempt_id"].as_str();
    if parent["method"] != "swarm.launch"
        || parent["caller_id"] != caller
        || parent["task_id"] != link.task_id
        || parent_task_id != link.task_id
        || manifest["task"]["project_id"] != link.project_id
        || parent_revision != link.task_revision
        || (link
            .attempt_id
            .as_deref()
            .is_some_and(|attempt| Some(attempt) != parent_attempt))
        || (link.attempt_id.is_none() && manifest["task"]["attempt_action"] != "claim_new")
        || parent["attempt_id"].as_str() != parent_attempt
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "launch child parent is inconsistent",
        ));
    }
    if let Some(attempt_id) = parent_attempt {
        let attempt = tasks::get_attempt(db, attempt_id)?;
        if attempt["task_id"] != link.task_id
            || attempt["task_revision"] != link.task_revision
            || attempt["owner_id"] != link.effective_manager_id
        {
            return Err(Error::new(
                "INVALID_RECEIPT",
                "launch child Attempt differs from its retained parent scope",
            ));
        }
    }
    let result: Value = result_raw
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or(Value::Null);
    let rejected = state == "rejected";
    let subject_matches = match method.as_str() {
        "task.claim" => {
            effective["launch_child"]
                .as_object()
                .is_some_and(|fields| fields.len() == 1)
                && original["client_request_id"] == request_id
                && original["task_id"] == link.task_id
                && original["expected_revision"] == link.task_revision
                && original["owner_id"] == link.effective_manager_id
                && original["start_owner"] == "controller"
                && original.as_object().is_some_and(|fields| fields.len() == 5)
                && manifest["task"]["attempt_action"] == "claim_new"
                && if rejected {
                    child_task.is_none()
                        && child_attempt.is_none()
                        && child_binding_id.is_none()
                        && child_binding_generation.is_none()
                        && effective["receipt"]["ok"] == false
                        && effective["receipt"]["error"] == result
                        && result.get("code").and_then(Value::as_str).is_some()
                } else {
                    let attempt_id = result.get("attempt_id").and_then(Value::as_str);
                    attempt_id.is_some()
                        && child_task.as_deref() == Some(link.task_id.as_str())
                        && child_attempt.as_deref() == attempt_id
                        && child_binding_id.is_none()
                        && child_binding_generation.is_none()
                        && parent_attempt == attempt_id
                        && manifest["attempt"]["action"] == "claim_new"
                        && result["operation_id"] == id
                        && result["task_id"] == link.task_id
                        && result["created"] == true
                        && effective["receipt"]["ok"] == true
                        && effective["receipt"]["value"] == result
                        && if let Some(attempt_id) = attempt_id {
                            let attempt = tasks::get_attempt(db, attempt_id)?;
                            attempt["task_id"] == link.task_id
                                && attempt["task_revision"] == link.task_revision
                                && attempt["owner_id"] == link.effective_manager_id
                                && attempt["start_owner"] == "controller"
                        } else {
                            false
                        }
                }
        }
        "coordination.participant.register" => {
            let exact_attempt = parent_attempt.is_some_and(|attempt| {
                original["attempt_id"] == attempt
                    && parent["attempt_id"] == attempt
                    && manifest["task"]["attempt_action"]
                        == if link.attempt_id.is_some() {
                            "use_existing"
                        } else {
                            "claim_new"
                        }
            });
            let exact_request = effective["launch_registration"]
                .as_object()
                .is_some_and(|fields| fields.len() == 1)
                && original["client_request_id"] == request_id
                && original["task_id"] == link.task_id
                && original["task_revision"] == link.task_revision
                && exact_attempt;
            if rejected {
                exact_request
                    && child_task.is_none()
                    && child_attempt.is_none()
                    && child_binding_id.is_none()
                    && child_binding_generation.is_none()
                    && effective["receipt"]["ok"] == false
                    && effective["receipt"]["error"] == result
                    && result.get("code").and_then(Value::as_str).is_some()
            } else {
                let client_id = original.get("client_id").and_then(Value::as_str);
                let registration = match client_id {
                    Some(client) => meta(db, &format!("client:{client}"))?,
                    None => None,
                };
                exact_request
                    && child_task.as_deref() == Some(link.task_id.as_str())
                    && child_attempt.as_deref() == parent_attempt
                    && child_binding_id.as_deref() == parent["binding_id"].as_str()
                    && child_binding_id.as_deref() == manifest["binding"]["binding_id"].as_str()
                    && child_binding_generation == parent["binding_generation"].as_i64()
                    && child_binding_generation == manifest["binding"]["generation"].as_i64()
                    && result["operation_id"] == id
                    && result["client_id"].as_str() == client_id
                    && result["task_id"] == link.task_id
                    && result["task_revision"] == link.task_revision
                    && result["attempt_id"].as_str() == parent_attempt
                    && registration.is_some_and(|record| {
                        record["role"] == "participant"
                            && record["created_operation_id"] == id
                            && record["task_id"] == link.task_id
                            && record["task_revision"] == link.task_revision
                            && record["attempt_id"].as_str() == parent_attempt
                            && record["binding_id"] == manifest["binding"]["binding_id"]
                            && record["binding_generation"] == manifest["binding"]["generation"]
                    })
            }
        }
        "agent.open" => {
            let child = operations::get_operation(db, id)?;
            let parent_request = crate::launcher::LaunchRequest::parse(&parent_original)?;
            let lease_id = original["lane_id"]
                .as_str()
                .and_then(|lane| lane.strip_prefix("launch-"));
            let lease_id = lease_id.unwrap_or_default();
            let lease: Option<RetainedLaunchLeaseRow> = db.query_row(
                "SELECT lease_id,project_id,task_revision,task_id,operation_id,owner_client_id,attempt_id,binding_digest,generation,state
                 FROM workspace_leases WHERE operation_id=?1 AND lease_id=?2",
                params![parent_id,lease_id],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?)),
            ).optional()?;
            let Some((
                stored_lease_id,
                project_id,
                lease_revision,
                lease_task,
                lease_operation,
                lease_owner,
                lease_attempt,
                lease_digest,
                lease_generation,
                _lease_state,
            )) = lease
            else {
                return Err(Error::new(
                    "INVALID_RECEIPT",
                    "launch open child has no exact retained workspace lease",
                ));
            };
            let lease = &effective["workspace_lease"];
            let rejected_before_admission =
                state == "rejected" && effective.get("operation_contract").is_none();
            let lease_tuple_matches = stored_lease_id == lease_id
                && project_id == link.project_id
                && lease_revision == link.task_revision
                && lease_task == link.task_id
                && lease_operation == parent_id
                && lease_owner == link.effective_manager_id
                && lease_attempt.as_deref() == parent_attempt
                && original["client_request_id"] == request_id
                && original.as_object().is_some_and(|fields| fields.len() == 3)
                && original["lane_id"] == format!("launch-{stored_lease_id}")
                && original["route"].as_str() == Some(parent_request.preview.route.as_str())
                && parent_attempt.is_some();
            if rejected_before_admission {
                lease_tuple_matches
                    && result.get("code").and_then(Value::as_str).is_some()
                    && child_binding_generation.is_none()
                    && child["task_id"].is_null()
                    && child["attempt_id"].is_null()
                    && child["binding_id"].is_null()
                    && effective["receipt"]["ok"] == false
                    && effective["receipt"]["error"] == result
            } else {
                let contract = &effective["operation_contract"];
                let manifest_lease = &manifest["workspace"]["lease_authority"];
                let manifest_binding = &manifest["binding"];
                let retained_receipt = &effective["receipt"];
                lease_tuple_matches
                    && child_task.as_deref() == Some(link.task_id.as_str())
                    && child_attempt.as_deref() == parent_attempt
                    && child["task_id"] == parent["task_id"]
                    && child["attempt_id"] == parent["attempt_id"]
                    && child["binding_id"] == parent["binding_id"]
                    && child_binding_id.as_deref() == parent["binding_id"].as_str()
                    && child["binding_generation"] == parent["binding_generation"]
                    && child_binding_generation == parent["binding_generation"].as_i64()
                    && child["prerequisite_operation_id"] == parent_id
                    && original["route"].as_str() == Some(parent_request.preview.route.as_str())
                    && lease["lease_id"] == stored_lease_id
                    && lease["generation"] == lease_generation
                    && lease["binding_digest"] == lease_digest
                    && lease["lease_id"] == manifest_lease["lease_id"]
                    && lease["generation"] == manifest_lease["generation"]
                    && lease["binding_digest"] == manifest_lease["binding_digest"]
                    && Some(lease_task.as_str()) == manifest_lease["task_id"].as_str()
                    && lease_revision == manifest_lease["task_revision"]
                    && lease_attempt.as_deref() == manifest_lease["attempt_id"].as_str()
                    && Some(lease_owner.as_str()) == manifest_lease["owner_client_id"].as_str()
                    && lease_digest.len() == 64
                    && child["binding_id"] == manifest_binding["binding_id"]
                    && child["binding_generation"] == manifest_binding["generation"]
                    && manifest_binding["operation_id"] == id
                    && contract["effect_scope"] == "one_exact_launch_binding"
                    && contract["completion_condition"] == "binding_ready_readback"
                    && contract["replay_policy"] == "exact_binding_readback_only_after_unknown"
                    && contract["parent_launch_operation_id"] == parent_id
                    && contract.as_object().is_some_and(|fields| fields.len() == 4)
                    && lease.as_object().is_some_and(|fields| fields.len() == 3)
                    && retained_receipt["ok"] == true
                    && retained_receipt["value"]["binding_id"] == child["binding_id"]
                    && retained_receipt["value"]["generation"] == child["binding_generation"]
                    && retained_receipt["value"]["operation_id"] == id
            }
        }
        _ => false,
    };
    if !subject_matches {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "launch child subject differs from its exact retained launch",
        ));
    }
    Ok(Some(parent_id.to_owned()))
}

/// Internal MCP discovery context. This is not an execution grant: the
/// hard facade profile and the target handler still enforce their guards.
fn mcp_authorization(db: &Connection, p: &Principal, value: &Value) -> Result<Value> {
    model::fields(value, &["task_id"])?;
    let requested_task = match value.get("task_id") {
        None | Some(Value::Null) => None,
        Some(_) => Some(model::text(value, "task_id")?),
    };
    let mut revision_context = json!({"client_id":p.client_id,"role":p.role,"gm":gm::record(db)?});
    let mut allowed = vec!["swarm.tools.search"];
    let methods = crate::mcp::registered_application_methods();
    match p.role {
        Role::Participant => {
            let registration = meta(db, &format!("client:{}", p.client_id))?
                .ok_or_else(|| Error::new("UNAUTHORIZED", "participant is not registered"))?;
            if requested_task.is_some_and(|task| registration["task_id"] != task) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "discovery Task is outside the participant assignment",
                ));
            }
            revision_context["grant_revision"] = registration["grant_revision"].clone();
            revision_context["participation_basis"] = registration["participation_basis"].clone();
            revision_context["scope_state"] = json!("unavailable");
            // Retained Concilium views and own Operation receipts do not
            // acquire a new action grant. Their handlers resolve the exact
            // historical actor/scope even after this assignment has ended.
            allowed.extend(["concilium.get", "concilium.list", "operation.get"]);
            allowed.extend([
                "coordination.thread.get",
                "coordination.thread.list",
                "coordination.contract.get",
                "coordination.contract.list",
                "coordination.agreement.get",
            ]);
            match coordination::current_scope(db, p) {
                Ok(scope) => {
                    revision_context["scope_state"] = json!("current");
                    revision_context["scope"] = scope;
                    allowed.extend(methods.into_iter().filter(|method| {
                        (swarm_contracts::method_policy::participant_coordination_read(method)
                            && !matches!(
                                *method,
                                "coordination.participant.get" | "coordination.participant.list"
                            ))
                            || swarm_contracts::method_policy::participant_coordination_mutation(
                                method,
                            )
                    }));
                    if matches!(
                        registration["participation_basis"]["kind"].as_str(),
                        Some("attempt_owner" | "producer_ref")
                    ) {
                        allowed.extend(["task.submit", "artifact.read"]);
                    } else if registration["participation_basis"]["kind"] == "sponsored_reviewer" {
                        allowed.extend([
                            "review.submit",
                            "review.get",
                            "review.list",
                            "swarm.review.context",
                            "artifact.read",
                            "task.submission",
                            "check.get",
                        ]);
                    }
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "STALE_PARTICIPANT" | "FORBIDDEN" | "NOT_FOUND"
                    ) =>
                {
                    let scope = &registration["participation_basis"]["review_scope"];
                    if let Some(assignment) = scope["review_assignment_id"].as_str()
                        && coordination::require_historical_review_result_scope(
                            db, p, assignment, scope,
                        )
                        .is_ok()
                    {
                        allowed.extend(["review.submit", "review.get", "operation.get"]);
                        revision_context["scope_state"] = json!("historical_result_only");
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Role::Operator => {
            require_local_operator(db, &p.client_id)?;
            allowed.extend(methods.into_iter().filter(|method| {
                !participant_only_mutation(method)
                    && *method != "review.submit"
                    && !method.starts_with("bus.")
            }));
        }
        Role::Manager => {
            let gm_authority = gm::require_authority(db, p).is_ok();
            revision_context["current_gm_authority"] = json!(gm_authority);
            allowed.extend(methods.into_iter().filter(|method| {
                !participant_only_mutation(method)
                    && *method != "review.submit"
                    && (gm_authority || !method.starts_with("hook."))
                    && (gm_authority
                        || !matches!(
                            *method,
                            "client.register"
                                | "client.list"
                                | "host.mode"
                                | "task.accept"
                                | "task.invalidate_acceptance"
                                | "forge.publish_ref"
                                | "gm.handover"
                        ))
            }));
        }
        Role::Observer => allowed.extend(methods.into_iter().filter(|method| {
            (is_read(method) || matches!(*method, "host.status" | "artifact.read"))
                && !matches!(
                    *method,
                    "client.list"
                        | "module.catalog.get"
                        | "swarm.context.get"
                        | "swarm.queue.get"
                        | "swarm.agent.inspect"
                        | "swarm.exceptions.get"
                        | "monitor.snapshot"
                        | "monitor.follow"
                        | "swarm.launch.preview"
                        | "review.get"
                        | "review.list"
                        | "swarm.review.context"
                        | "automation.config.get"
                        | "automation.config.preview"
                        | "automation.config.explain"
                )
                && !method.starts_with("coordination.")
                && !method.starts_with("code.scope.")
                && !method.starts_with("script.")
                && !method.starts_with("goal.")
                && !method.starts_with("hook.")
                && !method.starts_with("bus.")
                && !method.starts_with("github.")
        })),
        Role::Module | Role::Scheduler | Role::HookSource => {
            return Err(Error::new(
                "FORBIDDEN",
                "this role has no MCP discovery surface",
            ));
        }
        Role::ModuleSupervisor => {
            return Err(Error::new(
                "FORBIDDEN",
                "module supervisor has no MCP discovery surface",
            ));
        }
    }
    if p.role != Role::Participant
        && let Some(task_id) = requested_task
    {
        let task = tasks::get_task(db, task_id)?;
        revision_context["task"] =
            json!({"task_id":task_id,"revision":task["revision"],"state":task["state"]});
    }
    allowed.sort_unstable();
    allowed.dedup();
    revision_context["allowed_methods"] = json!(allowed);
    let authorization_revision = model::digest(model::canonical(&revision_context)?.as_bytes());
    Ok(
        json!({"authorization_revision":authorization_revision,"basis":"authenticated_store_scope",
        "allowed_methods":allowed,"role":p.role,"task_id":requested_task}),
    )
}

fn read(db: &Connection, p: &Principal, method: &str, v: &Value, config: &Config) -> Result<Value> {
    match method {
        "concilium.preview" | "concilium.get" | "concilium.list" => {
            concilium::read(db, p, method, v)
        }
        "coordination.thread.get" | "coordination.thread.list" => {
            coordination_threads::read(db, p, method, v)
        }
        "code.scope.inspect" | "code.scope.conflicts" => code_scopes::read(db, p, method, v),
        "coordination.agreement.get" => integration::read(db, p, method, v),
        "goal.get" | "goal.list" => goals::read(db, p, method, v),
        "mcp.authorization" => mcp_authorization(db, p, v),
        "swarm.context.get"
        | "coordination.participant.get"
        | "coordination.participant.list"
        | "coordination.peer.find"
        | "coordination.work_card.get"
        | "coordination.work_card.list"
        | "coordination.contract_card.get"
        | "coordination.contract_card.list"
        | "coordination.contract.get"
        | "coordination.contract.list"
        | "coordination.inbox" => coordination::read(db, p, method, v),
        "coordination.watch.list" => coordination_watch::read(db, p, v),
        "bus.events.page" => bus_kernel::read(db, p, method, v, config),
        "review.get" | "review.list" | "swarm.review.context" => reviews::read(db, p, method, v),
        "automation.config.get" => automation::get(db, p, v),
        "automation.config.preview" => automation::preview(db, p, v),
        "automation.config.explain" => automation::explain(db, p, v),
        "logging.get" => {
            model::fields(
                v,
                &[
                    "client_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "operation_id",
                    "binding_id",
                    "binding_generation",
                    "module_id",
                ],
            )?;
            logging::get(db, p, v)
        }
        "swarm.dashboard" => launcher::dashboard(db, p, v),
        "monitor.snapshot" => monitor::snapshot(db, p, v, config),
        "monitor.follow" => monitor::follow(db, p, v, config),
        "swarm.queue.get" => launcher::queue_get(db, p, v),
        "swarm.agent.inspect" => launcher::agent_inspect(db, p, v),
        "swarm.exceptions.get" => launcher::exceptions_get(db, p, v),
        "swarm.launch.preview" => launcher::launch_preview(db, p, v, config),
        "swarm.overlap.check" => integration::read(db, p, method, v),
        "check.get" => checks::describe(db, v),
        "check.profiles" => {
            model::fields(v, &[])?;
            let profiles = config
                .checks
                .profiles
                .iter()
                .map(|profile| {
                    json!({
                        "profile_id":profile.profile_id,
                        "profile_revision":profile.profile_revision,
                        "parser":profile.parser,
                        "resource":profile.resource,
                        "reproducible_opt_in":profile.reproducible,
                        "configured_environment_names":profile.environment.keys().collect::<Vec<_>>(),
                        "inherited_environment_names":profile.inherit_env,
                        "versioned_input_names":profile.versioned_inputs.keys().collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            Ok(json!({
                "enabled":config.checks.enabled,
                "profiles":profiles,
                "cache_reuse":"conditional",
                "cache_reuse_policy":"per_check_requires_verified_versioned_inputs"
            }))
        }

        "artifact.get" => results::describe(db, p, v),
        "artifact.parts" => assembly::parts(db, v),
        "task.acceptance" => acceptance::describe(db, v),
        "host.status" => {
            model::fields(v, &[])?;
            let tasks: i64 = db.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))?;
            let owners: i64 = db.query_row(
                "SELECT count(*) FROM attempts WHERE released_at_ms IS NULL",
                [],
                |r| r.get(0),
            )?;
            let queued: i64 = db.query_row(
                "SELECT count(*) FROM operations WHERE state='queued'",
                [],
                |r| r.get(0),
            )?;
            Ok(
                json!({"version":env!("CARGO_PKG_VERSION"),"controller_id":meta(db,"controller_id")?,"host_epoch":meta(db,"host_epoch")?,"host_lifecycle":host_lifecycle::status(db)?,"gm":gm::record(db)?,"gm_wake_mode":gm::WAKE_MODE,"sqlite":rusqlite::version(),"tasks":tasks,"unreleased_attempts":owners,"queued_operations":queued,"execution_mode":meta(db,"execution_mode")?,"native_modules_connected":db.query_row("SELECT count(*) FROM bindings WHERE released_at_ms IS NULL AND json_extract(state_json, '$.connection')='connected'",[],|r|r.get::<_,i64>(0))?,"native_execution":"scoped_runtime_protocol","schedules":schedules::status(db,&config.schedules,model::now_ms()?)?,"managed_bus_services":bus_kernel::managed_health_projection(db)?}),
            )
        }
        "agent.family" => producers::family(db, v),
        "task.submission" => submissions::describe(db, v),
        "task.get" => {
            model::fields(v, &["task_id"])?;
            tasks::get_task(db, model::text(v, "task_id")?)
        }
        "attempt.get" => {
            model::fields(v, &["attempt_id"])?;
            tasks::get_attempt(db, model::text(v, "attempt_id")?)
        }
        "task.list" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            let mut s = db.prepare(
                "SELECT task_id FROM tasks ORDER BY created_at_ms,task_id LIMIT ?1 OFFSET ?2",
            )?;
            let ids = s
                .query_map(params![limit, after], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let values = ids
                .iter()
                .map(|id| tasks::get_task(db, id))
                .collect::<Result<Vec<_>>>()?;
            Ok(
                json!({"items":values,"next_after":after+ids.len() as i64,"pagination":"offset_snapshot_not_inventory_proof"}),
            )
        }
        "operation.get" => {
            model::fields(v, &["operation_id"])?;
            let id = model::text(v, "operation_id")?;
            if !operation_visible_to(db, p, id)? {
                return Err(Error::new("NOT_FOUND", format!("Operation {id}")));
            }
            operations::get_operation_for_current_manager(db, p, id)
        }
        "agent.state" => {
            model::fields(v, &["binding_id", "generation"])?;
            let binding_id = model::text(v, "binding_id")?;
            let generation = model::positive(v, "generation")?;
            if p.role == Role::Operator {
                require_local_operator(db, &p.client_id)?;
                operations::get_binding(db, binding_id, generation)
            } else {
                operations::get_binding_public(db, binding_id, generation)
            }
        }
        "route.list" => {
            model::fields(v, &[])?;
            Ok(json!({"routes":config.routes,"live_qualification":false}))
        }
        "module.catalog.get" => module_handshake::read_catalog(p, v, db),
        "client.list" => {
            gm::require_authority(db, p)?;
            model::fields(v, &[])?;
            let mut s = db.prepare(
                "SELECT key,value_json FROM meta WHERE key LIKE 'client:%' ORDER BY key",
            )?;
            let items = s
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut result = Vec::new();
            for (key, raw) in items {
                let v: Value = serde_json::from_str(&raw)?;
                result
                    .push(json!({"client_id":&key[7..],"role":v["role"],"disabled":v["disabled"]}));
            }
            Ok(json!({"items":result}))
        }
        "agent.list" => {
            model::fields(v, &["limit", "after"])?;
            let (limit, after) = page(v)?;
            let private = p.role == Role::Operator;
            if private {
                require_local_operator(db, &p.client_id)?;
            }
            let mut s=db.prepare("SELECT binding_id,generation FROM bindings ORDER BY created_at_ms,binding_id,generation LIMIT ?1 OFFSET ?2")?;
            let ids = s
                .query_map(params![limit, after], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|(id, g)| {
                    if private {
                        operations::get_binding(db, id, *g)
                    } else {
                        operations::get_binding_public(db, id, *g)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"items":items,"next_after":after+ids.len() as i64}))
        }
        "operation.list" => {
            model::fields(v, &["after", "limit", "state"])?;
            let (limit, after) = page(v)?;
            let state = v.get("state").and_then(Value::as_str);
            let sql = format!(
                "SELECT op.operation_id FROM operations AS op WHERE (:state IS NULL OR op.state=:state) AND {OPERATION_VISIBILITY_SQL} ORDER BY op.created_at_ms,op.operation_id LIMIT :limit OFFSET :after"
            );
            let mut s = db.prepare(&sql)?;
            let ids = s
                .query_map(
                    named_params! {
                        ":state": state,
                        ":limit": limit,
                        ":after": after,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                    },
                    |r| r.get::<_, String>(0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|id| {
                    if !operation_visible_to(db, p, id)? {
                        return Err(Error::new("NOT_FOUND", format!("Operation {id}")));
                    }
                    operations::get_operation(db, id)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"items":items,"next_after":after+ids.len() as i64}))
        }
        "report.capacity" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            capacity::capacity_report(db, limit, after)
        }
        "report.attention" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            capacity::attention_report(db, limit, after)
        }
        "report.delta" | "message.read" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            let mailbox_only = method == "message.read";
            // Reports subscriptions read this same scoped cursor, so hidden
            // mail cannot leak through either payloads or pagination flags.
            let visibility = timeline_visibility_sql();
            let mut s = db.prepare(&format!(
                "SELECT o.observation_id,o.kind,o.payload_json,o.recorded_at_ms,o.operation_id FROM observations AS o WHERE o.observation_id>:after AND {visibility} ORDER BY o.observation_id LIMIT :limit"
            ))?;
            let rows = s
                .query_map(
                    named_params! {
                        ":after": after,
                        ":mailbox_only": mailbox_only,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                        ":limit": limit,
                    },
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, Option<String>>(4)?,
                        ))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            // Projection first, limits after projection (§8.1): an
            // oversized item becomes an explicit gap reference at its
            // own cursor; the cursor below advances only over entries
            // actually returned, so a limited page never replays or
            // silently skips a source row.
            let mut projected = Vec::with_capacity(rows.len());
            for (id, kind, raw, time, operation_id) in rows {
                // The SQL predicate scopes every linked fact before paging;
                // verify retained internal admission links before projecting it.
                if let Some(operation_id) = operation_id.as_deref()
                    && !operation_visible_to(db, p, operation_id)?
                {
                    return Err(Error::new(
                        "NOT_FOUND",
                        "scoped observation is not visible to this client",
                    ));
                }
                projected.push(json!({"cursor":id,"kind":kind,"payload":serde_json::from_str::<Value>(&raw)?,"recorded_at_ms":time,"operation_id":operation_id}));
            }
            let limited = projection::limit_items(projected, projection::timeline_gap_reference)?;
            let next = limited
                .items
                .last()
                .and_then(|item| item["cursor"].as_i64())
                .unwrap_or(after);
            // Newer source rows exist when a budget stopped this page
            // early (fetched rows were left unemitted) or when the
            // source itself continues past the last returned cursor.
            let has_newer: bool = limited.stopped_early
                || db.query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM observations AS o WHERE o.observation_id>:after AND {visibility})"
                    ),
                    named_params! {
                        ":after": next,
                        ":mailbox_only": mailbox_only,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                    },
                    |r| r.get::<_, bool>(0),
                )?;
            let has_older: bool = db.query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM observations AS o WHERE o.observation_id<=:after AND {visibility})"
                ),
                named_params! {
                    ":after": after,
                    ":mailbox_only": mailbox_only,
                    ":operator": p.role == Role::Operator,
                    ":client": &p.client_id,
                },
                |r| r.get(0),
            )?;
            let frame = projection::frame(
                if mailbox_only {
                    "mailbox"
                } else {
                    "observation_timeline"
                },
                json!({"after": after, "next_cursor": next}),
                &limited,
                limit,
                has_older,
                has_newer,
                limited.gap_count == 0,
                Vec::new(),
            )?;
            let goal_reminders = if mailbox_only {
                goals::notifications(db, p, limit)?
            } else {
                Value::Null
            };
            Ok(
                json!({"items":limited.items,"next_cursor":next,"projection":frame,"goal_reminders":goal_reminders}),
            )
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

fn mutate(
    db: &mut Connection,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = mutate_in_transaction(&tx, p, method, v, config, model::now_ms()?)?;
    // Rejected requests retain the same durable receipt as successful requests.
    tx.commit()?;
    result
}

fn mutate_in_transaction(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_check_plan(tx, p, method, v, config, now, None)
}

/// Admit the single closed ScriptRun controller effect through the normal
/// Operation, observation, capacity, and receipt path. Its Store-derived
/// context is not a Principal and is revalidated before and after apply.
fn mutate_script_effect_in_transaction(
    tx: &Transaction<'_>,
    admission: &scripts::ScriptEffectAdmission,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_authority(
        tx,
        MutationAuthority::ScriptEffect(admission),
        method,
        v,
        config,
        now,
        MutationPlan {
            check_plan: None,
            launch_operation_id: None,
            forge_execution: None,
        },
    )
}

pub(super) struct CronAdmission {
    pub(super) receipt: Result<Value>,
    pub(super) wake_check_worker: bool,
}

/// Admit one closed cron CheckRun through the same Operation receipt and
/// CheckRunner path as a manual check. The context is DB-derived and cannot be
/// supplied by a Principal or the legacy Scheduler identity.
fn mutate_cron_check_in_transaction(
    tx: &Transaction<'_>,
    context: &crate::automation::authorization::CronExecutionContext,
    config: &Config,
    now: i64,
    resolution: &checks::CheckPlanResolution,
) -> Result<CronAdmission> {
    context.require_current_check_target(tx)?;
    let params = context.request_params()?;
    let receipt = mutate_in_transaction_with_authority(
        tx,
        MutationAuthority::Cron(context),
        "check.run",
        &params,
        config,
        now,
        MutationPlan {
            check_plan: Some(resolution),
            launch_operation_id: None,
            forge_execution: None,
        },
    )?;
    let check_id = receipt
        .as_ref()
        .ok()
        .and_then(|value| value["check_id"].as_str())
        .map(str::to_owned);
    let wake_check_worker = if let Some(check_id) = check_id {
        tx.query_row(
            "SELECT state='queued' FROM check_runs WHERE check_id=?1",
            [check_id],
            |row| row.get::<_, bool>(0),
        )
        .optional()?
        .unwrap_or(false)
    } else {
        false
    };
    Ok(CronAdmission {
        receipt,
        wake_check_worker,
    })
}

/// Admit one Goal follow-up through the regular mutation receipt and Runtime
/// Operation ledger, bound to the manager-owned entry and authenticated
/// terminal EventRef that selected it.
fn admit_goal_progression_operation(
    tx: &Transaction<'_>,
    admission: &automation_goal_progression::GoalProgressionAdmission,
    config: &Config,
    now: i64,
) -> Result<automation_goal_progression::AdmissionResult> {
    admission.require_current_for_admission(tx, now)?;
    let caller = crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    let request = admission.request();
    let method = admission.continuation_method();
    let request_id = model::text(request, "client_request_id")?;
    let original = model::canonical(request)?;
    let prior: Option<(String, String)> = tx
        .query_row(
            "SELECT method,original_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if prior
        .as_ref()
        .is_some_and(|(prior_method, body)| prior_method != method || body != &original)
    {
        return Ok(automation_goal_progression::AdmissionResult::Conflict {
            code: "REQUEST_ID_CONFLICT".to_owned(),
            reason:
                "terminal-event request ID already belongs to another Goal continuation request"
                    .to_owned(),
        });
    }
    let receipt = mutate_in_transaction_with_authority(
        tx,
        MutationAuthority::GoalProgression(admission),
        method,
        request,
        config,
        now,
        MutationPlan {
            check_plan: None,
            launch_operation_id: None,
            forge_execution: None,
        },
    );
    let value = match receipt {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            return Ok(automation_goal_progression::AdmissionResult::Conflict {
                code: error.code,
                reason: error.message,
            });
        }
        Err(error) if goal_progression_admission_conflict(&error.code) => {
            return Ok(automation_goal_progression::AdmissionResult::Conflict {
                code: error.code,
                reason: error.message,
            });
        }
        Err(error) => return Err(error),
    };
    let operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2 AND method=?3 AND original_request_json=?4",
            params![caller, request_id, method, original],
            |row| row.get(0),
        )
        .optional()?;
    let operation_id = operation_id
        .or_else(|| value["operation_id"].as_str().map(str::to_owned))
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_GOAL_ADMISSION_LOST",
                "normal Goal admission returned no retained Operation ID",
            )
        })?;
    if prior.is_some() {
        Ok(automation_goal_progression::AdmissionResult::Reused { operation_id })
    } else {
        Ok(automation_goal_progression::AdmissionResult::Admitted { operation_id })
    }
}

fn goal_progression_admission_conflict(code: &str) -> bool {
    code.contains("CONFLICT")
        || code == "FORBIDDEN"
        || code == "AUTOMATION_ACTION_CHANGED"
        || code.starts_with("GOAL_")
        || code.starts_with("BINDING_")
}

fn mutate_in_transaction_with_check_plan(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    check_plan: Option<&checks::CheckPlanResolution>,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_plan(
        tx,
        p,
        method,
        v,
        config,
        now,
        MutationPlan {
            check_plan,
            launch_operation_id: None,
            forge_execution: None,
        },
    )
}

pub(in crate::store) fn mutate_in_transaction_with_forge_execution(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    preparation: &forge::ForgeExecutionPreparation,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_plan(
        tx,
        p,
        method,
        v,
        config,
        now,
        MutationPlan {
            check_plan: None,
            launch_operation_id: None,
            forge_execution: Some(preparation),
        },
    )
}

pub(crate) fn mutate_launch_child_in_transaction(
    tx: &Transaction<'_>,
    actor: &launcher::LaunchActor,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    launch_operation_id: &str,
) -> Result<Result<Value>> {
    if !matches!(method, "task.claim" | "coordination.participant.register") {
        return Err(Error::new("FORBIDDEN", "unsupported launch child action"));
    }
    let current = launcher::launch_actor(tx, launch_operation_id)?;
    if !current.same_authority_identity(actor) {
        return Err(Error::new("STALE_LAUNCH", "launch child authority changed"));
    }
    mutate_in_transaction_with_authority(
        tx,
        MutationAuthority::Launch(actor),
        method,
        v,
        config,
        now,
        MutationPlan {
            check_plan: None,
            launch_operation_id: Some(launch_operation_id),
            forge_execution: None,
        },
    )
}

#[derive(Clone, Copy)]
struct MutationPlan<'a> {
    check_plan: Option<&'a checks::CheckPlanResolution>,
    launch_operation_id: Option<&'a str>,
    forge_execution: Option<&'a forge::ForgeExecutionPreparation>,
}

enum MutationAuthority<'a> {
    Direct(&'a Principal),
    Launch(&'a launcher::LaunchActor),
    Cron(&'a crate::automation::authorization::CronExecutionContext),
    GoalProgression(&'a automation_goal_progression::GoalProgressionAdmission),
    ScriptEffect(&'a scripts::ScriptEffectAdmission),
}

impl MutationAuthority<'_> {
    fn caller_id(&self) -> &str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::Launch(actor) => actor.technical_requester_id(),
            Self::Cron(_) => crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
            Self::GoalProgression(_) => {
                crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
            }
            Self::ScriptEffect(admission) => admission.technical_requester_id(),
        }
    }
}

fn mutate_in_transaction_with_plan(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    plan: MutationPlan<'_>,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_authority(
        tx,
        MutationAuthority::Direct(p),
        method,
        v,
        config,
        now,
        plan,
    )
}

fn mutate_in_transaction_with_authority(
    tx: &Transaction<'_>,
    authority: MutationAuthority<'_>,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    plan: MutationPlan<'_>,
) -> Result<Result<Value>> {
    if let MutationAuthority::Direct(p) = &authority
        && p.role == Role::Scheduler
        && (p.client_id != model::INTERNAL_SCHEDULER_CLIENT_ID || method != "check.run")
    {
        return Err(Error::new(
            "FORBIDDEN",
            "scheduler may admit only configured checks",
        ));
    }
    if let MutationAuthority::Cron(context) = &authority {
        if method != "check.run" || plan.launch_operation_id.is_some() {
            return Err(Error::new(
                "FORBIDDEN",
                "cron authority permits only its closed CheckRun action",
            ));
        }
        context.require_current_check_target(tx)?;
    }
    if let MutationAuthority::GoalProgression(context) = &authority {
        if method != context.continuation_method()
            || plan.launch_operation_id.is_some()
            || plan.check_plan.is_some()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "Goal progression authority permits only its exact continuation action",
            ));
        }
        context.require_current_for_admission(tx, now)?;
    }
    if let MutationAuthority::ScriptEffect(admission) = &authority {
        if !matches!(method, "message.send" | "task.create")
            || plan.check_plan.is_some()
            || plan.launch_operation_id.is_some()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "ScriptRun effect authority permits only its exact message or taskless task.create action",
            ));
        }
        admission.require_current(tx, config, method, v)?;
    }
    model::validate_mutation(method, v)?;
    let request_id = model::text(v, "client_request_id")?;
    let original = model::canonical(v)?;
    let caller_id = authority.caller_id();
    let admission_key = if method == "coordination.participant.register" {
        "launch_registration"
    } else {
        "launch_child"
    };
    if let Some(receipt) = launch_alias_receipt(tx, caller_id, request_id, method, &original)? {
        return Ok(receipt_result(&receipt));
    }
    if let Some(receipt) = repair_alias_receipt(tx, caller_id, request_id, method, &original)? {
        return Ok(receipt_result(&receipt));
    }
    let old:Option<(String,String,String)> = tx.query_row("SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2", params![caller_id,request_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if let Some((old_method, body, effective)) = old {
        if old_method != method || body != original {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "request ID was used with a different method or payload",
            ));
        }
        let receipt: Value = serde_json::from_str(&effective)?;
        let retained_launch_id = match receipt.get(admission_key) {
            None => None,
            Some(value) if value.as_object().is_some_and(|object| object.len() == 1) => {
                Some(model::text(value, "launch_operation_id").map_err(|_| {
                    Error::new(
                        "INVALID_RECEIPT",
                        "retained launch admission context is invalid",
                    )
                })?)
            }
            Some(_) => {
                return Err(Error::new(
                    "INVALID_RECEIPT",
                    "retained launch admission context is invalid",
                ));
            }
        };
        if retained_launch_id != plan.launch_operation_id {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "request ID has a different launch admission context",
            ));
        }
        if let Some(launch_operation_id) = plan.launch_operation_id {
            // A retained successful registration cannot become a new rejected
            // credential. Stale retry authority is an outer error, leaving
            // any accepted secret intact for exact recovery.
            if method == "coordination.participant.register" && receipt["receipt"]["ok"] == true {
                let actor = match &authority {
                    MutationAuthority::Direct(p) => launcher::LaunchActor::Direct((*p).clone()),
                    MutationAuthority::Launch(actor) => (*actor).clone(),
                    MutationAuthority::Cron(_) => {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "cron authority cannot register a participant",
                        ));
                    }
                    MutationAuthority::GoalProgression(_) => {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "Goal progression cannot admit a launch child",
                        ));
                    }
                    MutationAuthority::ScriptEffect(_) => {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "ScriptRun effect cannot admit a launch child",
                        ));
                    }
                };
                participant_credentials::validate_launch_registration(
                    tx,
                    &actor,
                    launch_operation_id,
                    config,
                    v,
                )?;
            }
        }
        return Ok(receipt_result(&receipt["receipt"]));
    }
    if method == "swarm.launch"
        && let MutationAuthority::Direct(principal) = &authority
    {
        let request = crate::launcher::LaunchRequest::parse(v)?;
        let actor = launcher::LaunchActor::Direct((*principal).clone());
        match launcher::resolve_launch_slot(tx, &actor, &request.preview)? {
            automation_work_dispatch::LaunchSlotResolution::Vacant => {}
            automation_work_dispatch::LaunchSlotResolution::Reuse {
                operation_id,
                operation_state,
            } => {
                // Mutation retries return the immutable admission receipt.
                // Mutable launch progress is read through operation.get; do
                // not copy its result/state into a success-shaped snapshot.
                let value = json!({
                    "operation_id": operation_id,
                    "operation_state_at_receipt": operation_state,
                    "receipt_recorded_at_ms": now,
                    "current_state_read_method": "operation.get",
                    "semantic_reuse": true,
                });
                save_launch_alias(
                    tx,
                    caller_id,
                    request_id,
                    &original,
                    &operation_id,
                    &value,
                    now,
                )?;
                return Ok(Ok(value));
            }
            automation_work_dispatch::LaunchSlotResolution::Conflict {
                operation_id,
                operation_state,
            } => {
                return Err(Error::new(
                    "LAUNCH_SLOT_CONFLICT",
                    format!(
                        "launch slot is retained by Operation {operation_id} ({operation_state})"
                    ),
                ));
            }
        }
    }
    let direct_repair_slot = if method == "agent.send"
        && let MutationAuthority::Direct(principal) = &authority
    {
        let slot = automation_repair::recognize_direct_correction_request(tx, principal, v)?;
        if let Some(slot) = &slot {
            match automation_repair::resolve_direct_slot(tx, slot)? {
                automation_repair::RepairSlotResolution::Vacant => {}
                automation_repair::RepairSlotResolution::Existing {
                    operation_id,
                    operation_state,
                } => {
                    let value = json!({
                        "operation_id":operation_id,
                        "operation_state_at_receipt":operation_state,
                        "receipt_recorded_at_ms":now,
                        "current_state_read_method":"operation.get",
                        "semantic_reuse":true
                    });
                    save_repair_alias(
                        tx,
                        caller_id,
                        request_id,
                        &original,
                        &operation_id,
                        &value,
                        now,
                    )?;
                    return Ok(Ok(value));
                }
                automation_repair::RepairSlotResolution::Conflict {
                    operation_id,
                    operation_state,
                } => {
                    return Err(Error::new(
                        "REPAIR_SLOT_CONFLICT",
                        format!(
                            "correction slot is retained by Operation {operation_id} ({operation_state})"
                        ),
                    ));
                }
            }
        }
        slot
    } else {
        None
    };
    let id = model::new_id();
    tx.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'{}','queued',?6,?6,?6)",params![id,caller_id,request_id,method,original,now])?;
    if let MutationAuthority::Cron(context) = &authority {
        crate::automation::authorization::save_cron_operation_link(tx, &id, context, now)?;
    }
    if let Some(launch_operation_id) = plan.launch_operation_id {
        let admission_path = format!("$.{admission_key}");
        tx.execute("UPDATE operations SET effective_request_json=json_set(effective_request_json,?3,json(?2)) WHERE operation_id=?1", params![id, model::canonical(&json!({"launch_operation_id":launch_operation_id}))?, admission_path])?;
    }
    if let MutationAuthority::Direct(principal) = &authority
        && matches!(
            method,
            "coordination.thread.open"
                | "coordination.message.send"
                | "coordination.thread.resolve"
                | "coordination.thread.withdraw"
                | "coordination.thread.supersede"
                | "coordination.contract.propose"
                | "coordination.contract.respond"
        )
        && let Some((task_id, task_revision, attempt_id)) =
            coordination_threads::admitted_thread_operation_scope(tx, principal, method, v)?
    {
        let changed = if matches!(
            method,
            "coordination.contract.propose" | "coordination.contract.respond"
        ) {
            let thread_id = model::text(v, "thread_id")?;
            let retained_scope = json!({
                "kind": "contract_thread",
                "thread_id": thread_id,
                "task_id": task_id,
                "task_revision": task_revision,
                "attempt_id": attempt_id,
            });
            tx.execute(
                "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=json_set(effective_request_json,'$.coordination_scope',json(?7)) WHERE operation_id=?1 AND caller_id=?4 AND method=?5 AND client_request_id=?6",
                params![
                    id,
                    task_id,
                    attempt_id,
                    principal.client_id,
                    method,
                    request_id,
                    model::canonical(&retained_scope)?,
                ],
            )?
        } else {
            tx.execute(
                "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND caller_id=?4 AND method=?5 AND client_request_id=?6",
                params![id, task_id, attempt_id, principal.client_id, method, request_id],
            )?
        };
        if changed != 1 {
            return Err(Error::new(
                "STORE_INVARIANT",
                "Thread-backed Operation scope could not be attached to its admitted receipt",
            ));
        }
    }
    tx.execute_batch("SAVEPOINT mutation_effect")?;
    let result = match &authority {
        MutationAuthority::Direct(p) => apply(
            tx,
            p,
            method,
            v,
            config,
            ApplyContext {
                operation_id: &id,
                now,
                plan,
            },
        ),
        MutationAuthority::Launch(actor) => apply_launch_child(
            tx,
            actor,
            method,
            v,
            config,
            ApplyContext {
                operation_id: &id,
                now,
                plan,
            },
        ),
        MutationAuthority::Cron(context) => apply_cron(
            tx,
            context,
            method,
            v,
            config,
            ApplyContext {
                operation_id: &id,
                now,
                plan,
            },
        ),
        MutationAuthority::GoalProgression(context) => apply_goal_progression(
            tx,
            context,
            method,
            v,
            config,
            ApplyContext {
                operation_id: &id,
                now,
                plan,
            },
        ),
        MutationAuthority::ScriptEffect(admission) => match method {
            "message.send" => {
                mailbox::apply_message_send(tx, admission.effective_manager_id(), v, &id)
                    .map(|value| (value, false))
            }
            "task.create" => {
                tasks::create_for_script_effect(tx, admission.project_id(), v, &id, now)
                    .map(|value| (value, false))
            }
            _ => Err(Error::new(
                "FORBIDDEN",
                "ScriptRun effect authority cannot dispatch this method",
            )),
        },
    };
    let result = result.and_then(|(value, queued)| {
        if let MutationAuthority::ScriptEffect(admission) = &authority {
            admission.require_current(tx, config, method, v)?;
        }
        if let Some(slot) = &direct_repair_slot
            && let MutationAuthority::Direct(principal) = &authority
        {
            if !queued {
                return Err(Error::new(
                    "REPAIR_OPERATION_MISMATCH",
                    "correction admission did not queue delivery",
                ));
            }
            automation_repair::retain_direct_admission(tx, principal, slot, &id, now)?;
        }
        Ok((value, queued))
    });
    let receipt = match &result {
        Ok((value, queued)) => {
            tx.execute_batch("RELEASE mutation_effect")?;
            let state = if *queued { "queued" } else { "settled" };
            tx.execute("UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1",params![id,state,model::canonical(value)?,if *queued{None}else{Some(now)},now])?;
            // The admitted operation now holds (or releases) native
            // capacity; record that in the durable ledger (R23).
            capacity::sync_operation(tx, &id, now)?;
            if method != "event.emit" {
                tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?1,?2,?3,?4)",params![id,method,model::canonical(value)?,now])?;
            }
            record_safe_system_events(tx, method, &id, value, *queued, now)?;
            json!({"ok":true,"value":value})
        }
        Err(error) => {
            tx.execute_batch("ROLLBACK TO mutation_effect; RELEASE mutation_effect")?;
            tx.execute("UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3 WHERE operation_id=?1",params![id,model::canonical(&json!(error))?,now])?;
            record_operation_failure_event(tx, &id, "rejected", now)?;
            json!({"ok":false,"error":error})
        }
    };
    tx.execute("UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.receipt',json(?2)) WHERE operation_id=?1",params![id,model::canonical(&receipt)?])?;
    Ok(result.map(|(v, _)| v))
}
fn record_safe_system_events(
    tx: &Transaction<'_>,
    method: &str,
    operation_id: &str,
    value: &Value,
    queued: bool,
    now: i64,
) -> Result<()> {
    if queued {
        return Ok(());
    }

    let message_sent = matches!(
        method,
        "message.send" | "coordination.send" | "coordination.message.send"
    ) || (method == "coordination.consult" && value["delivery_created"] == true);
    if message_sent {
        insert_safe_system_event(
            tx,
            "controller:messages",
            &format!("sent:{operation_id}"),
            Some(operation_id),
            "message.sent",
            "message_send_committed",
            "sent",
            Some(&format!("operation:{operation_id}:message_send_committed")),
            None,
            None,
            now,
        )?;
    }
    if method == "message.send" && value["reply_to"].is_object() {
        insert_safe_system_event(
            tx,
            "controller:messages",
            &format!("reply:{operation_id}"),
            Some(operation_id),
            "message.reply_sent",
            "message_send_committed",
            "sent",
            Some(&format!("operation:{operation_id}:message_send_committed")),
            None,
            None,
            now,
        )?;
    }
    if method == "coordination.consult" && value["status"] == "answered_from_card" {
        insert_safe_system_event(
            tx,
            "controller:coordination",
            &format!("answer:{operation_id}"),
            Some(operation_id),
            "coordination.answer",
            "coordination_answered",
            "answered",
            Some(&format!("operation:{operation_id}:coordination_answered")),
            None,
            None,
            now,
        )?;
    }
    Ok(())
}

/// Retain a closed failure fact in the same transaction as its Operation.
/// The category is public metadata; detailed errors remain in operation.get.
pub(super) fn record_operation_failure_event(
    tx: &Transaction<'_>,
    operation_id: &str,
    state: &str,
    now: i64,
) -> Result<()> {
    let (kind, phase, status, error_code) = match state {
        "rejected" => (
            "operation.rejected",
            "operation_rejected",
            "rejected",
            "OPERATION_REJECTED",
        ),
        "outcome_unknown" => (
            "operation.outcome_unknown",
            "operation_outcome_unknown",
            "unknown",
            "OUTCOME_UNKNOWN",
        ),
        _ => {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "invalid Operation failure state",
            ));
        }
    };
    let current: String = tx.query_row(
        "SELECT state FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    if current != state {
        return Err(Error::new(
            "SYSTEM_EVENT_INVALID",
            "Operation failure fact differs from retained state",
        ));
    }
    let occurrence_id = format!("operation:{operation_id}:{phase}");
    let payload = json!({
        "schema_version":1, "phase":phase, "status":status,
        "occurrence_id":occurrence_id, "error_code":error_code
    });
    // Repeated uncertain readbacks retain the first immutable failure fact.
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:operations',?1,?2,?3,?4,?5)",
        rusqlite::params![occurrence_id, operation_id, kind, model::canonical(&payload)?, now],
    )?;
    Ok(())
}

pub(super) fn is_safe_native_result_error_code(value: &str) -> bool {
    matches!(
        value,
        "MODULE_RECEIPT_INVALID"
            | "MODULE_RESULT_ACK_INVALID"
            | "RESULT_BODY_UNAVAILABLE"
            | "RESULT_ORIGIN_INVALID"
            | "RESULT_PAGE_UNAVAILABLE"
            | "RESULT_PROVENANCE_INVALID"
            | "RESULT_RANGE_INVALID"
            | "RESULT_SELECTOR_UNSUPPORTED"
            | "RESULT_TARGET_NOT_ADMITTED"
            | "RESULT_TARGET_NOT_TERMINAL"
            | "RESULT_TARGET_ORIGIN_INVALID"
            | "RESULT_TARGET_RECEIPT_INVALID"
            | "RESULT_TARGET_SCOPE_INVALID"
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_safe_system_event(
    tx: &Transaction<'_>,
    source_stream_id: &str,
    source_event_key: &str,
    operation_id: Option<&str>,
    kind: &str,
    phase: &str,
    status: &str,
    occurrence_id: Option<&str>,
    host_epoch_pair: Option<(i64, i64)>,
    error_code: Option<&str>,
    recorded_at_ms: i64,
) -> Result<()> {
    let source_kind_phase_valid = matches!(
        (source_stream_id, kind, phase),
        (
            "controller:messages",
            "message.sent" | "message.reply_sent",
            "message_send_committed"
        ) | (
            "controller:coordination",
            "coordination.answer",
            "coordination_answered"
        ) | (
            "controller:runtime",
            "native.operation.completed",
            "native_outcome_terminal"
        ) | (
            "controller:runtime",
            "native.result.available",
            "native_result_page_recorded"
        ) | (
            "controller:host-lifecycle",
            "host.interrupted",
            "host_interruption_observed"
        )
    );
    let status_valid = match phase {
        "message_send_committed" => status == "sent",
        "coordination_answered" => status == "answered",
        "native_outcome_terminal" => matches!(status, "applied" | "rejected"),
        "native_result_page_recorded" => matches!(status, "completed" | "incomplete"),
        "host_interruption_observed" => status == "unknown",
        _ => false,
    };
    if !source_kind_phase_valid || !status_valid {
        return Err(Error::new(
            "SYSTEM_EVENT_INVALID",
            "safe system event kind, phase, or status is invalid",
        ));
    }
    if phase == "host_interruption_observed" {
        let Some((previous_epoch, current_epoch)) = host_epoch_pair else {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "host interruption is missing its persisted epoch pair",
            ));
        };
        let expected_id = format!("host-interruption:{previous_epoch}:{current_epoch}");
        if previous_epoch <= 0
            || current_epoch <= previous_epoch
            || operation_id.is_some()
            || occurrence_id != Some(expected_id.as_str())
            || error_code != Some("HOST_INTERRUPTED")
        {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "host interruption identity is invalid",
            ));
        }
    } else {
        let Some(operation_id) = operation_id else {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "operation event is missing its Operation identity",
            ));
        };
        let expected_id = format!("operation:{operation_id}:{phase}");
        let native_result_failure = source_stream_id == "controller:runtime"
            && kind == "native.operation.completed"
            && phase == "native_outcome_terminal";
        if occurrence_id != Some(expected_id.as_str())
            || host_epoch_pair.is_some()
            || (!native_result_failure && error_code.is_some())
            || (native_result_failure && error_code.is_some() && status != "rejected")
            || error_code.is_some_and(|code| {
                native_result_failure && !is_safe_native_result_error_code(code)
            })
        {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "operation event identity is invalid",
            ));
        }
    }

    let mut payload = json!({
        "schema_version":1,
        "phase":phase,
        "status":status,
    });
    if let Some(occurrence_id) = occurrence_id {
        payload["occurrence_id"] = json!(occurrence_id);
    }
    if let Some((previous_epoch, current_epoch)) = host_epoch_pair {
        payload["previous_host_epoch"] = json!(previous_epoch);
        payload["current_host_epoch"] = json!(current_epoch);
    }
    if let Some(error_code) = error_code {
        payload["error_code"] = json!(error_code);
    }
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,?3,?4,?5,?6)",
        rusqlite::params![
            source_stream_id,
            source_event_key,
            operation_id,
            kind,
            model::canonical(&payload)?,
            recorded_at_ms
        ],
    )?;
    Ok(())
}
fn receipt_result(value: &Value) -> Result<Value> {
    if value["ok"] == true {
        Ok(value["value"].clone())
    } else {
        Err(Error::new(
            value["error"]["code"].as_str().unwrap_or("INVALID_RECEIPT"),
            value["error"]["message"]
                .as_str()
                .unwrap_or("stored request has no valid receipt"),
        ))
    }
}

fn repair_alias_key(caller: &str, request_id: &str) -> Result<String> {
    Ok(format!(
        "repair:request-alias:v1:{}",
        model::digest(model::canonical(&json!([caller, request_id]))?.as_bytes())
    ))
}

fn repair_alias_receipt(
    db: &Connection,
    caller: &str,
    request_id: &str,
    method: &str,
    original: &str,
) -> Result<Option<Value>> {
    let key = repair_alias_key(caller, request_id)?;
    let Some(alias) = crate::automation::config::read_record(db, &key, "repair request alias")?
    else {
        return Ok(None);
    };
    if alias["schema_version"] != 1
        || alias["caller_id"] != caller
        || alias["client_request_id"] != request_id
        || alias["method"] != "agent.send"
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "repair request alias identity is invalid",
        ));
    }
    if alias["method"] != method || alias["original_request_json"] != original {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "request ID was used with a different method or payload",
        ));
    }
    let operation_id = model::text(&alias, "operation_id")?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "agent.send"
        || alias["receipt"]["ok"] != true
        || alias["receipt"]["value"]["operation_id"] != operation_id
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "repair request alias target is invalid",
        ));
    }
    Ok(Some(alias["receipt"].clone()))
}

fn save_repair_alias(
    tx: &Transaction<'_>,
    caller: &str,
    request_id: &str,
    original: &str,
    operation_id: &str,
    value: &Value,
    now: i64,
) -> Result<()> {
    let key = repair_alias_key(caller, request_id)?;
    if value["operation_id"] != operation_id
        || crate::automation::config::read_record(tx, &key, "repair request alias")?.is_some()
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "repair request alias cannot replace a retained receipt",
        ));
    }
    crate::automation::config::write_record(
        tx,
        &key,
        &json!({
            "schema_version":1,"caller_id":caller,"client_request_id":request_id,
            "method":"agent.send","original_request_json":original,"operation_id":operation_id,
            "receipt":{"ok":true,"value":value},"created_at_ms":now
        }),
    )
}

fn launch_alias_key(caller: &str, request_id: &str) -> Result<String> {
    Ok(format!(
        "launcher:request-alias:v1:{}",
        model::digest(model::canonical(&json!([caller, request_id]))?.as_bytes())
    ))
}

fn launch_alias_receipt(
    db: &Connection,
    caller: &str,
    request_id: &str,
    method: &str,
    original: &str,
) -> Result<Option<Value>> {
    let key = launch_alias_key(caller, request_id)?;
    let Some(alias) = crate::automation::config::read_record(db, &key, "launch request alias")?
    else {
        return Ok(None);
    };
    if alias["schema_version"] != 1
        || alias["caller_id"] != caller
        || alias["client_request_id"] != request_id
        || alias["method"] != "swarm.launch"
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "launch request alias identity is invalid",
        ));
    }
    if alias["method"] != method || alias["original_request_json"] != original {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "request ID was used with a different method or payload",
        ));
    }
    let target = model::text(&alias, "operation_id")?;
    let operation = operations::get_operation(db, target)?;
    if operation["method"] != "swarm.launch"
        || alias["receipt"]["ok"] != true
        || alias["receipt"]["value"]["operation_id"] != target
    {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "launch request alias target is invalid",
        ));
    }
    Ok(Some(alias["receipt"].clone()))
}

fn save_launch_alias(
    tx: &Transaction<'_>,
    caller: &str,
    request_id: &str,
    original: &str,
    operation_id: &str,
    value: &Value,
    now: i64,
) -> Result<()> {
    if value["operation_id"] != operation_id {
        return Err(Error::new(
            "INVALID_RECEIPT",
            "retained launch receipt has a different Operation",
        ));
    }
    let key = launch_alias_key(caller, request_id)?;
    let alias = json!({"schema_version":1,"caller_id":caller,"client_request_id":request_id,"method":"swarm.launch","original_request_json":original,"operation_id":operation_id,"receipt":{"ok":true,"value":value},"created_at_ms":now});
    if crate::automation::config::read_record(tx, &key, "launch request alias")?.is_some() {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "launch request alias already exists",
        ));
    }
    crate::automation::config::write_record(tx, &key, &alias)
}
struct ApplyContext<'a> {
    operation_id: &'a str,
    now: i64,
    plan: MutationPlan<'a>,
}

fn apply_launch_child(
    tx: &Transaction<'_>,
    actor: &launcher::LaunchActor,
    method: &str,
    value: &Value,
    config: &Config,
    context: ApplyContext<'_>,
) -> Result<(Value, bool)> {
    let parent = context
        .plan
        .launch_operation_id
        .ok_or_else(|| Error::new("FORBIDDEN", "launch child requires its admitted parent"))?;
    match method {
        "task.claim" => {
            tasks::claim_for_launch(tx, actor, value, context.operation_id, context.now)
                .map(|value| (value, false))
        }
        "coordination.participant.register" => {
            participant_credentials::validate_launch_registration(
                tx, actor, parent, config, value,
            )?;
            coordination::register_participant_for_launch(
                tx,
                actor,
                value,
                context.operation_id,
                context.now,
            )
            .map(|value| (value, false))
        }
        _ => Err(Error::new("FORBIDDEN", "unsupported launch child action")),
    }
}

fn apply_cron(
    tx: &Transaction<'_>,
    context: &crate::automation::authorization::CronExecutionContext,
    method: &str,
    value: &Value,
    config: &Config,
    apply: ApplyContext<'_>,
) -> Result<(Value, bool)> {
    if method != "check.run" || apply.plan.launch_operation_id.is_some() {
        return Err(Error::new(
            "FORBIDDEN",
            "cron authority permits only its closed CheckRun action",
        ));
    }
    if model::canonical(value)? != model::canonical(&context.request_params()?)? {
        return Err(Error::new(
            "FORBIDDEN",
            "cron CheckRun request does not match its committed action context",
        ));
    }
    checks::reserve_cron(
        tx,
        context,
        apply.operation_id,
        config,
        apply.plan.check_plan,
    )
}

fn apply_goal_progression(
    tx: &Transaction<'_>,
    context: &automation_goal_progression::GoalProgressionAdmission,
    method: &str,
    value: &Value,
    config: &Config,
    apply: ApplyContext<'_>,
) -> Result<(Value, bool)> {
    if method != context.continuation_method()
        || apply.plan.launch_operation_id.is_some()
        || apply.plan.check_plan.is_some()
        || model::canonical(value)? != model::canonical(context.request())?
    {
        return Err(Error::new(
            "FORBIDDEN",
            "Goal progression permits only its exact admitted continuation request",
        ));
    }
    let result = runtime::user_command_for_goal_progression(
        tx,
        context,
        apply.operation_id,
        config,
        apply.now,
    )?;
    automation_goal_progression::retain_operation_link(tx, apply.operation_id, context, apply.now)?;
    Ok((result, true))
}

fn operation_project_id(tx: &Connection, operation_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>, Option<String>, String, String)> = tx
        .query_row(
            "SELECT task_id,attempt_id,original_request_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((task_id, attempt_id, original_json, effective_json)) = row else {
        return Ok(None);
    };
    if let Some(task_id) = task_id {
        return tx
            .query_row(
                "SELECT project_id FROM tasks WHERE task_id=?1",
                [task_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into);
    }
    if let Some(attempt_id) = attempt_id {
        let task_id: Option<String> = tx
            .query_row(
                "SELECT task_id FROM attempts WHERE attempt_id=?1",
                [attempt_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(task_id) = task_id {
            return tx
                .query_row(
                    "SELECT project_id FROM tasks WHERE task_id=?1",
                    [task_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into);
        }
    }
    for encoded in [effective_json, original_json] {
        let value: Value = serde_json::from_str(&encoded).map_err(|_| {
            Error::new(
                "EVENT_CAUSE_INVALID",
                "cause Operation authority record is malformed",
            )
        })?;
        for candidate in [&value["event_emit"]["project_id"], &value["project_id"]] {
            if let Some(project_id) = candidate.as_str() {
                return Ok(Some(project_id.to_owned()));
            }
        }
    }
    Ok(None)
}

fn validate_manager_event_cause(
    tx: &Connection,
    principal: &Principal,
    project_id: &str,
    cause: &Value,
) -> Result<()> {
    let Some(cause) = cause.as_object() else {
        return Ok(());
    };
    let operation_id = cause
        .get("operation_id")
        .filter(|value| !value.is_null())
        .and_then(Value::as_str);
    let observation_id = cause
        .get("observation_id")
        .filter(|value| !value.is_null())
        .and_then(Value::as_i64);
    if operation_id.is_none() && observation_id.is_none() {
        return Err(Error::new(
            "EVENT_CAUSE_INVALID",
            "cause must retain an existing operation_id or observation_id",
        ));
    }
    let observed_operation_id: Option<String> = if let Some(observation_id) = observation_id {
        tx.query_row(
            "SELECT operation_id FROM observations WHERE observation_id=?1",
            [observation_id],
            |row| row.get(0),
        )
        .optional()?
    } else {
        None
    };
    if observation_id.is_some() && observed_operation_id.is_none() {
        return Err(Error::new(
            "EVENT_CAUSE_INVALID",
            "cause observation is missing or is not linked to an Operation",
        ));
    }
    if let (Some(operation_id), Some(observed_operation_id)) =
        (operation_id, observed_operation_id.as_deref())
        && operation_id != observed_operation_id
    {
        return Err(Error::new(
            "EVENT_CAUSE_INVALID",
            "cause operation_id does not match its observation",
        ));
    }
    let operation_id = operation_id
        .or(observed_operation_id.as_deref())
        .ok_or_else(|| Error::new("EVENT_CAUSE_INVALID", "cause Operation is unavailable"))?;
    if !operation_visible_to(tx, principal, operation_id)? {
        return Err(Error::new(
            "EVENT_CAUSE_UNAUTHORIZED",
            "cause Operation is outside the current Manager's visibility",
        ));
    }
    let caller_id: String = tx.query_row(
        "SELECT caller_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    if caller_id != principal.client_id {
        return Err(Error::new(
            "EVENT_CAUSE_UNAUTHORIZED",
            "cause Operation is not owned by the current Manager",
        ));
    }
    if operation_project_id(tx, operation_id)?.as_deref() != Some(project_id) {
        return Err(Error::new(
            "EVENT_CAUSE_SCOPE_MISMATCH",
            "cause Operation is outside the emitted event project",
        ));
    }
    Ok(())
}

fn emit_manager_event(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    if principal.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "event.emit is owned by an authenticated Manager",
        ));
    }
    let project_id = model::text(value, "project_id")?;
    let name = model::text(value, "name")?;
    let dedupe_key = model::text(value, "dedupe_key")?;
    let payload = value
        .get("payload")
        .filter(|payload| !payload.is_null())
        .ok_or_else(|| Error::invalid("event payload must be non-null"))?;
    let cause = value.get("cause").cloned().unwrap_or(Value::Null);
    validate_manager_event_cause(tx, principal, project_id, &cause)?;
    let identity = json!({
        "owner_manager_id":principal.client_id.clone(),
        "project_id":project_id,
        "name":name,
        "dedupe_key":dedupe_key,
    });
    let source_event_key = format!(
        "emit:{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    );
    let payload_digest = model::digest(model::canonical(payload)?.as_bytes());
    let scope = json!({
        "schema_version":1,
        "source_stream_id":MANAGER_EVENT_SOURCE_STREAM,
        "source_event_key":source_event_key.clone(),
        "owner_manager_id":principal.client_id.clone(),
        "project_id":project_id.to_owned(),
        "name":name.to_owned(),
        "dedupe_key":dedupe_key.to_owned(),
        "payload_digest":payload_digest.clone(),
    });
    tx.execute(
        "UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.event_emit',json(?2)) WHERE operation_id=?1",
        params![operation_id, model::canonical(&scope)?],
    )?;
    let event_payload = json!({
        "schema_version":1,
        "source_stream_id":MANAGER_EVENT_SOURCE_STREAM,
        "source_event_key":source_event_key.clone(),
        "owner_manager_id":principal.client_id.clone(),
        "project_id":project_id.to_owned(),
        "name":name.to_owned(),
        "dedupe_key":dedupe_key.to_owned(),
        "payload":payload.clone(),
        "cause":cause.clone(),
    });
    let encoded = model::canonical(&event_payload)?;
    if encoded.len()
        > model::MAX_MANAGER_EVENT_PAYLOAD_BYTES + model::MAX_MANAGER_EVENT_CAUSE_BYTES + 2048
    {
        return Err(Error::invalid("manager event exceeds its storage bound"));
    }
    let existing: Option<(i64, Option<String>, String)> = tx
        .query_row(
            "SELECT observation_id,operation_id,payload_json FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2",
            params![MANAGER_EVENT_SOURCE_STREAM, &source_event_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((observation_id, existing_operation_id, old_payload)) = existing {
        if old_payload != encoded {
            return Err(Error::new(
                "EVENT_DEDUPE_CONFLICT",
                "event dedupe identity already has different immutable content",
            ));
        }
        let existing_operation_id = existing_operation_id.ok_or_else(|| {
            Error::new(
                "EVENT_LEDGER_CORRUPT",
                "manager event observation has no linked Operation",
            )
        })?;
        return Ok(json!({
            "operation_id":existing_operation_id,
            "observation_id":observation_id,
            "source_id":MANAGER_EVENT_SOURCE_STREAM,
            "source_event_key":source_event_key,
            "event_kind":name,
            "project_id":project_id,
            "dedupe_key":dedupe_key,
            "duplicate":true,
            "recorded_at_ms":now_ms,
        }));
    }
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            MANAGER_EVENT_SOURCE_STREAM,
            &source_event_key,
            operation_id,
            name,
            encoded,
            now_ms,
        ],
    )?;
    let observation_id = tx.last_insert_rowid();
    Ok(json!({
        "operation_id":operation_id,
        "observation_id":observation_id,
        "source_id":MANAGER_EVENT_SOURCE_STREAM,
        "source_event_key":source_event_key,
        "event_kind":name,
        "project_id":project_id,
        "dedupe_key":dedupe_key,
        "duplicate":false,
        "recorded_at_ms":now_ms,
    }))
}

fn apply(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    context: ApplyContext<'_>,
) -> Result<(Value, bool)> {
    let ApplyContext {
        operation_id: id,
        now,
        plan,
    } = context;
    if let Some(launch_operation_id) = plan.launch_operation_id {
        if method != "coordination.participant.register" {
            return Err(Error::new(
                "FORBIDDEN",
                "launch admission permits only scoped Participant registration",
            ));
        }
        participant_credentials::validate_launch_registration(
            tx,
            &launcher::LaunchActor::Direct(p.clone()),
            launch_operation_id,
            config,
            v,
        )?;
    }
    match method {
        "goal.create" | "goal.revise" | "goal.enable" | "goal.disable" | "goal.readback" => {
            goals::apply(tx, p, method, v, id, now)
        }
        "github.pull_request.update_description" => {
            github_pr_effects::apply(tx, p, v, id, now, config)
        }
        "github.pull_request.reconcile_description" => {
            github_pr_effects::apply_reconcile(tx, p, v, id, now)
        }
        "github.source.setup"
        | "github.source.poll"
        | "github.work_pool.apply"
        | "github.effect.managed_label"
        | "github.effect.reconcile_managed_label" => {
            github::apply(tx, p, method, v, config, id, now)
        }
        "hook.source.revoke" => hooks::revoke(
            tx,
            p,
            model::text(v, "source_id")?,
            model::positive(v, "expected_revision")?,
            now,
        )
        .map(|mut value| {
            value["operation_id"] = json!(id);
            (value, false)
        }),
        "coordination.sync_integration" | "coordination.integration.ack" => {
            integration::apply(tx, p, method, v, config, id, now)
        }
        "coordination.thread.open"
        | "coordination.message.send"
        | "coordination.thread.resolve"
        | "coordination.thread.withdraw"
        | "coordination.thread.supersede" => {
            coordination_threads::apply(tx, p, method, v, id, now).map(|value| (value, false))
        }
        "code.scope.propose" | "code.scope.accept" | "code.scope.release" => {
            code_scopes::apply(tx, p, method, v, id, now)
        }
        "concilium.propose"
        | "concilium.open"
        | "concilium.position.submit"
        | "concilium.round.advance"
        | "concilium.close" => concilium::apply(tx, p, method, v, id, now),
        "swarm.launch" => launcher::launch(tx, p, v, config, id, now),
        "coordination.watch.create" | "coordination.watch.cancel" => {
            coordination_watch::apply(tx, p, method, v, id, now)
        }
        "bus.consumer.register" | "bus.consumer.revoke" | "bus.consumer.admit" => {
            bus_kernel::apply(tx, p, method, v, id, config, now)
        }
        "coordination.participant.register"
        | "coordination.participant.disable"
        | "coordination.work_card.publish"
        | "coordination.work_card.withdraw"
        | "coordination.contract_card.publish"
        | "coordination.contract_card.withdraw"
        | "coordination.contract.propose"
        | "coordination.contract.respond"
        | "coordination.send"
        | "coordination.consult" => coordination::apply(tx, p, method, v, config, id, now),
        "review.assign" => {
            let request = crate::review::ReviewAssignRequest::parse(v)?;
            reviews::reserve_assign(tx, reviews::ReviewActor::Direct(p), &request, id, now)
                .map(|value| (value, false))
        }
        "review.submit" => {
            let request = crate::review::ReviewSubmitRequest::parse(v)?;
            reviews::reserve_submit(tx, p, &request, id, now).map(|value| (value, false))
        }
        "automation.config.apply" => {
            automation::apply(tx, p, v, id, config, now).map(|value| (value, false))
        }
        "logging.set" => logging::set(tx, p, v, id, now).map(|value| (value, false)),
        "event.emit" => emit_manager_event(tx, p, v, id, now).map(|value| (value, false)),
        "automation.config.transfer" => {
            automation_transfer::apply(tx, p, v, id, now).map(|value| (value, false))
        }
        "forge.publish_ref" => {
            forge::reserve(tx, p, v, id, config, plan.forge_execution).map(|value| {
                let queued = value.get("coalesced") != Some(&Value::Bool(true));
                (value, queued)
            })
        }
        "source.capture" => checks::reserve_source(tx, p, v, id, config).map(|v| (v, true)),
        "check.run" => checks::reserve(tx, p, v, id, config, plan.check_plan),
        "check.cancel" => checks::cancel(tx, p, v, id).map(|v| (v, false)),

        "artifact.assemble" => assembly::reserve(tx, p, v, id).map(|v| (v, true)),
        "task.submit" => submissions::reserve(tx, p, v, id).map(|v| (v, true)),
        "task.submit.recover" => submissions::reserve_recovery(tx, p, v, id, now),
        "task.accept" => acceptance::reserve(tx, p, v, id).map(|v| {
            let queued = v.get("coalesced") != Some(&Value::Bool(true));
            (v, queued)
        }),
        "task.invalidate_acceptance" => {
            acceptance::invalidate(tx, p, v, id, now).map(|v| (v, false))
        }
        "task.request_changes" => {
            submissions::request_changes(tx, p, v, id, now).map(|v| (v, false))
        }
        "task.create" => tasks::create(tx, p, v, id, now).map(|v| (v, false)),
        "task.revise" => tasks::revise(tx, p, v, id, now).map(|v| (v, false)),
        "task.claim" => tasks::claim(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.bind_producer" => producers::bind(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.release" => tasks::release(tx, p, v, id, now).map(|v| (v, false)),
        "task.dispatch" => operations::dispatch(tx, p, v, id, now, config),
        "agent.send" | "agent.reply" | "agent.configure" | "agent.goal" | "agent.background"
        | "agent.refresh" | "agent.reconcile" | "agent.result" | "agent.recover" => {
            runtime::user_command(tx, p, method, v, id, config).map(|v| (v, true))
        }
        "agent.open" => operations::open(tx, p, v, config, id, now).map(|v| (v, true)),
        "operation.cancel" => operations::cancel(tx, p, v, id, now).map(|v| (v, false)),
        "host.mode" => {
            gm::require_authority(tx, p)?;
            model::fields(v, &["client_request_id", "new_work"])?;
            let mode = model::text(v, "new_work")?;
            if !["enabled", "disabled"].contains(&mode) {
                return Err(Error::invalid("new_work must be enabled or disabled"));
            }
            set_meta(tx, "execution_mode", &json!({"new_work":mode}))?;
            Ok((
                json!({"operation_id":id,"new_work":mode,"running_work_cancelled":false}),
                false,
            ))
        }
        "gm.handover" => gm::handover(tx, p, v, id).map(|v| (v, false)),
        "module.route.select" => {
            module_handshake::select_route(tx, p, v, config, id).map(|value| (value, false))
        }
        "client.register" => {
            gm::require_authority(tx, p)?;
            model::fields(
                v,
                &[
                    "client_request_id",
                    "client_id",
                    "role",
                    "token_hash",
                    "binding_id",
                    "binding_generation",
                ],
            )?;
            let client = model::text(v, "client_id")?;
            let role: Role = serde_json::from_value(v["role"].clone())?;
            if role == Role::Participant {
                return Err(Error::new(
                    "FORBIDDEN",
                    "participants require assignment-bound coordination registration",
                ));
            }
            if role == Role::Operator {
                return Err(Error::new(
                    "FORBIDDEN",
                    "operator role is reserved for the bootstrap credential",
                ));
            }
            if role == Role::HookSource {
                return Err(Error::new(
                    "FORBIDDEN",
                    "hook credentials require repository-scoped source setup",
                ));
            }
            if role == Role::ModuleSupervisor
                || client == model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID
                || client == automation_scheduler::SERVICE_ID
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "reserved host Module identities cannot be registered by a caller",
                ));
            }
            if role == Role::Scheduler || client == model::INTERNAL_SCHEDULER_CLIENT_ID {
                return Err(Error::new(
                    "FORBIDDEN",
                    "scheduler identity is reserved for in-process admission",
                ));
            }
            let hash = model::text(v, "token_hash")?;
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::invalid("token_hash must be SHA-256 hex"));
            }
            if meta(tx, &format!("client:{client}"))?.is_some() {
                return Err(Error::conflict(
                    "client already registered; no implicit credential rotation",
                ));
            }
            let scope = if role == Role::Module {
                runtime::register(tx, v, client)?
            } else {
                if v.get("binding_id").is_some() || v.get("binding_generation").is_some() {
                    return Err(Error::invalid(
                        "binding scope only belongs to module credentials",
                    ));
                }
                json!({})
            };
            let mut registration =
                json!({"role":role,"token_hash":hash.to_lowercase(),"disabled":false});
            if let Some(fields) = scope.as_object() {
                for (k, v) in fields {
                    registration[k] = v.clone();
                }
            }
            set_meta(tx, &format!("client:{client}"), &registration)?;
            Ok((
                json!({"operation_id":id,"client_id":client,"role":role}),
                false,
            ))
        }
        "message.send" => {
            mailbox::apply_message_send(tx, &p.client_id, v, id).map(|value| (value, false))
        }
        "message.cancel" => mailbox::cancel_message(tx, p, v, id).map(|v| (v, false)),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not implemented; no native effect was attempted"),
        )),
    }
}

#[cfg(test)]
mod automation_transfer_tests;
#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod core_failure_tests;
#[cfg(test)]
mod gm_automation_recovery_tests;
#[cfg(test)]
mod gm_continuation_tests;
#[cfg(test)]
mod gm_submission_recovery_tests;
#[cfg(test)]
mod mailbox_tests;
#[cfg(test)]
mod program_tests;
#[cfg(test)]
mod receipt_tests;
#[cfg(test)]
mod runtime_admission_tests;
#[cfg(test)]
mod security_tests;
#[cfg(test)]
mod system_event_producer_tests;
