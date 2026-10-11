//! Standalone Store scheduler client. It never holds Manager credentials.

use std::{env, path::PathBuf, time::Duration};
use swarm_automation::{
    ADMIT_METHOD, DuePage, DueSourceKind, PAGE_METHOD, READY_FRAME, WorkerConfig, has_due_source,
    read_worker_config_file, wait_ms,
};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    DeclaredServiceScope,
    error::{Error, Result},
};
use tokio::time::sleep;

const INITIAL_NO_PROGRESS_BACKOFF: Duration = Duration::from_millis(250);
const MAX_NO_PROGRESS_BACKOFF: Duration = Duration::from_secs(30);

/// One worker-local pacing state for page-read failures, stale admission and
/// unchanged authoritative readback. Source cursor digests identify retained
/// Store facts; wall-clock due transitions do not reset accumulated pacing.
struct NoProgressBackoff {
    delay: Duration,
    last_source_progress: Option<Vec<(DueSourceKind, String)>>,
    awaiting_readback_sha256: Option<String>,
}

impl NoProgressBackoff {
    fn new() -> Self {
        Self {
            delay: INITIAL_NO_PROGRESS_BACKOFF,
            last_source_progress: None,
            awaiting_readback_sha256: None,
        }
    }

    fn note_admission_attempt(&mut self, snapshot_sha256: &str) {
        self.awaiting_readback_sha256 = Some(snapshot_sha256.to_owned());
    }

    /// Return a delay only when the authoritative page still represents the
    /// exact cut submitted by the preceding admission attempt. A new cut can
    /// be considered immediately, but only changed retained cursor identities
    /// reset the accumulated delay.
    fn observe_page(&mut self, page: &DuePage) -> Option<Duration> {
        let mut source_progress = page
            .sources
            .iter()
            .map(|source| (source.kind, source.cursor_digest.clone()))
            .collect::<Vec<_>>();
        source_progress.sort_by_key(|(kind, _)| *kind);
        let progressed = self.last_source_progress.as_ref() != Some(&source_progress);
        self.last_source_progress = Some(source_progress);
        if progressed {
            self.delay = INITIAL_NO_PROGRESS_BACKOFF;
        }
        match self.awaiting_readback_sha256.take() {
            Some(expected) if expected == page.snapshot_sha256 => Some(self.next_delay()),
            Some(_) | None => None,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let current = self.delay;
        self.delay = self.delay.saturating_mul(2).min(MAX_NO_PROGRESS_BACKOFF);
        current
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_cli().await {
        eprintln!("swarm-automation-worker: {}", error.code);
        std::process::exit(2);
    }
}

/// Name of the only optional worker flag. It is recognized only when the
/// crate is compiled with the qualification-faults feature.
#[cfg(feature = "qualification-faults")]
const QUALIFICATION_DROP_REPLY_FLAG: &str = "--qualification-drop-first-admit-reply";

/// Whether this process records admission intent before each admit request.
///
/// Worker-owned state. It is set only after the qualification flag successfully
/// arms the fault, so the marker and the fault always travel together.
#[cfg(feature = "qualification-faults")]
static QUALIFICATION_ADMIT_INTENT_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

async fn run_cli() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let mode = args
        .next()
        .and_then(|arg| arg.into_string().ok())
        .ok_or_else(|| Error::invalid("expected run --config <private-file>"))?;
    if mode != "run" {
        return Err(Error::invalid("expected run --config <private-file>"));
    }
    let flag = args
        .next()
        .and_then(|arg| arg.into_string().ok())
        .ok_or_else(|| Error::invalid("missing --config"))?;
    if flag != "--config" {
        return Err(Error::invalid("expected --config"));
    }
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::invalid("missing worker config path"))?;
    // The sole optional flag, accepted only with the qualification-faults
    // feature. Every other argument is still rejected, and the flag is an
    // unrecognized token in a release-default build.
    #[cfg(not(feature = "qualification-faults"))]
    if args.next().is_some() {
        return Err(Error::invalid("unexpected worker argument"));
    }
    #[cfg(feature = "qualification-faults")]
    let qualification_flag = {
        let mut fault = false;
        for arg in args {
            if arg == QUALIFICATION_DROP_REPLY_FLAG && !fault {
                fault = true;
            } else {
                return Err(Error::invalid("unexpected worker argument"));
            }
        }
        fault
    };
    // Arm once per process. The arming is exclusive and becomes terminal when
    // consumed, so exactly one admit reply can be withheld in this lifetime.
    #[cfg(feature = "qualification-faults")]
    if qualification_flag {
        let armed = swarm_client::qualification_faults::arm_single_reply_drop_for_admit();
        // Bounded evidence that the process accepted the qualification flag.
        // The line carries no credential, request, or Store decision.
        eprintln!("swarm-automation-worker: qualification reply fault armed={armed}");
        // The admission-intent observer is activated only by a successful arm, so
        // an observer can never exist without the fault that exercises it.
        if armed {
            QUALIFICATION_ADMIT_INTENT_ACTIVE.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let config = read_worker_config_file(&path)?;
    let _service_owner =
        swarm_process::Group::enter_service(&config.service_owner_token, config.scope.purpose)?;
    {
        use std::io::Write;
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(READY_FRAME)
            .and_then(|()| stdout.flush())
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_WORKER_READY_SIGNAL_FAILED",
                    "scheduler service readiness could not be signaled",
                )
            })?;
    }
    run_worker(config).await
}

async fn run_worker(config: WorkerConfig) -> Result<()> {
    // Run one shared timer/queue. Reconnect/readback is the only recovery path;
    // no action-specific task or client retry loop is created.
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    let mut pacing = NoProgressBackoff::new();
    loop {
        let page_result = tokio::select! {
            result = &mut signal => {
                result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                return Ok(());
            }
            page = read_page(&config) => page,
        };
        let page = match page_result {
            Ok(page) => page,
            Err(error) if retryable(&error.code) => {
                let delay = pacing.next_delay();
                eprintln!(
                    "swarm-automation-worker: {}; Store page retry in {} ms",
                    error.code,
                    delay.as_millis()
                );
                tokio::select! {
                    result = &mut signal => {
                        result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                        return Ok(());
                    }
                    _ = sleep(delay) => {}
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        page.validate(&config.scope).map_err(|_| {
            Error::new(
                "AUTOMATION_PAGE_INVALID",
                "Store returned an invalid scheduler page",
            )
        })?;
        if let Some(delay) = pacing.observe_page(&page) {
            eprintln!(
                "swarm-automation-worker: Store cut unchanged; admission retry in {} ms",
                delay.as_millis()
            );
            tokio::select! {
                result = &mut signal => {
                    result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                    return Ok(());
                }
                _ = sleep(delay) => {}
            }
        }
        if has_due_source(&page) {
            let params = page.admit_params().map_err(|_| {
                Error::new(
                    "AUTOMATION_PAGE_INVALID",
                    "scheduler page could not form a bounded admission request",
                )
            })?;
            pacing.note_admission_attempt(&page.snapshot_sha256);
            let result = tokio::select! {
                signal_result = &mut signal => {
                    signal_result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                    return Ok(());
                }
                result = call_once(&config, ADMIT_METHOD, params) => result,
            };
            match result {
                Ok(value) if valid_admit_receipt(&value, &config.scope) => {
                    // Always read the authoritative page before another
                    // admission. Source cursor progress resets pacing; an
                    // unchanged exact cut receives the shared backoff.
                    continue;
                }
                Ok(_) => {
                    return Err(Error::new(
                        "AUTOMATION_ADMIT_RECEIPT_INVALID",
                        "Store returned an invalid scheduler receipt",
                    ));
                }
                // Do not replay the previous page on an uncertain reply. The
                // next iteration reads authoritative source cursors first;
                // each domain's stable slot/receipt coalesces a committed cut.
                Err(error) if retryable(&error.code) || error.code == "AUTOMATION_PAGE_STALE" => {
                    eprintln!(
                        "swarm-automation-worker: {}; rereading Store page before retry",
                        error.code
                    );
                    // Qualification-only applied-fault witness. This launch
                    // sends stderr to a null sink and the host stops reading
                    // stdout after the readiness frame, so worker logging is
                    // unreachable. The only observable trace of the withheld
                    // reply is a bounded create-only marker beside the exact
                    // private worker config, bound to the live PID and the
                    // config digest. No Store state or credential is touched.
                    #[cfg(feature = "qualification-faults")]
                    if error.code == swarm_client::qualification_faults::QUALIFICATION_REPLY_DROPPED
                    {
                        // The exact withheld frame length is reported by the
                        // transport seam inside the error message. It is parsed
                        // strictly and a failure to parse is reported rather than
                        // defaulting to a fabricated value.
                        match swarm_client::qualification_faults::parse_withheld_frame_bytes(
                            &error.message,
                        ) {
                            Some(withheld_frame_bytes) => {
                                record_qualification_fault_witness(&config, withheld_frame_bytes);
                            }
                            None => {
                                eprintln!(
                                    "swarm-automation-worker: qualification witness refused; withheld frame length was absent from the transport error"
                                );
                            }
                        }
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        tokio::select! {
            result = &mut signal => {
                result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                return Ok(());
            }
            _ = sleep(Duration::from_millis(wait_ms(&page))) => {}
        }
    }
}

async fn read_page(config: &WorkerConfig) -> Result<DuePage> {
    let value = call_once(
        config,
        PAGE_METHOD,
        serde_json::json!({
            "scope":config.scope,
        }),
    )
    .await?;
    serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_PAGE_INVALID",
            "Store scheduler page does not match its fixed schema",
        )
    })
}

async fn call_once(
    config: &WorkerConfig,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    let mut client = Client::connect(
        &PathBuf::from(&config.store_root),
        &config.credential,
        &IpcConfig::default(),
    )
    .await?;
    // Qualification-only: record this admission intent before the request is
    // sent, so a replay leaves a marker the harness can refute. Any marker failure
    // refuses the request rather than falling back to sending it unobserved.
    #[cfg(feature = "qualification-faults")]
    if method == ADMIT_METHOD
        && QUALIFICATION_ADMIT_INTENT_ACTIVE.load(std::sync::atomic::Ordering::Acquire)
    {
        swarm_automation::write_qualification_admit_attempt(
            std::path::Path::new(&config.store_root),
            std::process::id(),
            &config.config_sha256,
        )?;
    }
    client.request(method, params).await
}

fn valid_admit_receipt(value: &serde_json::Value, scope: &DeclaredServiceScope) -> bool {
    value["schema_version"] == 1
        && value["scope"] == serde_json::json!(scope)
        && matches!(
            value["disposition"].as_str(),
            Some("reconcilers_completed" | "already_observed" | "page_stale")
        )
        && value["next_due_at_ms"].as_i64().is_none_or(|due| due >= 0)
}

fn retryable(code: &str) -> bool {
    // The qualification code cannot be produced by a release-default build, so
    // it is recognized only when that build requested the fault feature. The
    // reference is compiled out entirely otherwise.
    #[cfg(feature = "qualification-faults")]
    if code == swarm_client::qualification_faults::QUALIFICATION_REPLY_DROPPED {
        return true;
    }
    matches!(
        code,
        "HOST_UNAVAILABLE"
            | "IO_ERROR"
            | "OUTCOME_UNKNOWN"
            | "STORE_CLOSED"
            | "DISCONNECTED"
            | "AUTOMATION_SERVICE_OWNER_PENDING"
    )
}

/// Record the bounded qualification witness for one withheld admit reply.
///
/// Called only from the qualification fault path, which is compiled out of any
/// build that did not request the feature. The worker is the only place that
/// knows both the live process identity and the exact private config digest the
/// Store issued, so it is the only place the witness can be bound correctly.
///
/// The Store-owned launch sends stderr to a null sink and the host stops
/// reading stdout after the readiness frame, so no worker output is reachable.
/// This marker beside the private worker config is the only observable trace
/// that the fault actually fired in the live Store-owned process.
///
/// Failure to write the witness is reported on stderr and never affects the
/// Store, the credential, or the uncertain branch that follows.
#[cfg(feature = "qualification-faults")]
fn record_qualification_fault_witness(config: &WorkerConfig, withheld_frame_bytes: usize) {
    let witness = swarm_automation::QualificationFaultWitness {
        owner_launch_pid: std::process::id(),
        owner_config_sha256: config.config_sha256.clone(),
        method: "automation.scheduler.admit",
        withheld_frame_bytes,
    };

    // The service directory is beside the exact private config this process
    // loaded, so the witness can never land outside it.
    let data_root = std::path::Path::new(&config.store_root);
    if let Err(error) = swarm_automation::write_qualification_fault_witness(data_root, &witness) {
        eprintln!(
            "swarm-automation-worker: qualification witness write failed: {}",
            error.code
        );
        return;
    }
    eprintln!(
        "swarm-automation-worker: QUALIFICATION_FAULT_WITNESS_RECORDED method=automation.scheduler.admit reply=withheld replay=refused"
    );
}
