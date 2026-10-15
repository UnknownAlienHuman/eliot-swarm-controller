#[cfg(test)]
mod app299_connected_consumer;
#[cfg(test)]
mod app320_connected_consumer;
mod config;
mod journal;
mod mcp_plugin;
mod module_link;
mod module_receipt;
mod module_runtime;
mod native;
mod native_interactions;
mod native_mcp;
mod native_mcp_intake;
mod native_owner;
mod provider_auth;

pub use config::{
    ARTIFACT_ID, ARTIFACT_VERSION, AdapterConfig, ModelRef, NativeOptions, OwnedNativeOptions,
    RUNTIME,
};
pub use module_runtime::OwnedBootstrap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
#[cfg(test)]
use journal::operation_key;
use journal::{
    Journal, OperationIntent, PendingItem, ResultAssistantIntent, ResultInputStatusIntent,
    digest_json,
};
use native::{
    AssistantResultEvidence, InputEvidence, NativeClient, canonical_json, input_id, intent_for,
    root_id,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{
        EffectOutcome, NormalizedResultOriginContext, NormalizedResultPageSource, RuntimeCommand,
        RuntimeOutcome, TaskDispatchAdmissionReceipt, TaskDispatchContext,
    },
    task_prompt::TaskPromptEnvelopeV1,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Instant as TokioInstant, sleep};

const ACK_ATTEMPTS: usize = 4;
const ACK_BACKOFF_MS: [u64; ACK_ATTEMPTS] = [100, 250, 500, 1000];
const IDLE_POLL_MS: u64 = 500;
const JOURNAL_RETENTION_RETRY_MS: u64 = 10_000;
const STARTUP_RECOVERY_TICK_MS: u64 = 250;

enum StartupPoll<T> {
    ModuleNext(T),
    RecoveryTick,
}

async fn wait_for_module_next_or_recovery<F: Future>(
    next: Pin<&mut F>,
    recovery_tick: &mut tokio::time::Interval,
    recovery_enabled: bool,
) -> StartupPoll<F::Output> {
    tokio::select! {
        result = next => StartupPoll::ModuleNext(result),
        _ = recovery_tick.tick(), if recovery_enabled => StartupPoll::RecoveryTick,
    }
}

#[derive(Clone, Copy)]
enum StartupRecoveryStep {
    WaitingForInventory,
    Progressed,
    Complete,
    Blocked,
}

struct StartupRecovery {
    inventory: Option<tokio::task::JoinHandle<Result<()>>>,
    blocked: bool,
}

impl StartupRecovery {
    fn start(journal: Arc<Journal>) -> Result<Self> {
        let inventory = if journal.startup_inventory_required()? {
            Some(tokio::task::spawn_blocking(move || {
                journal.prepare_startup_inventory()
            }))
        } else {
            None
        };
        Ok(Self {
            inventory,
            blocked: false,
        })
    }

    async fn advance(
        &mut self,
        journal: &Journal,
        recovery_sample: &mut Vec<journal::OperationRecoveryState>,
        recovery_count: &mut usize,
    ) -> StartupRecoveryStep {
        if self.blocked {
            return StartupRecoveryStep::Blocked;
        }
        if journal.startup_recovery_complete() {
            return StartupRecoveryStep::Complete;
        }
        if let Some(inventory) = self.inventory.as_ref()
            && !inventory.is_finished()
        {
            return StartupRecoveryStep::WaitingForInventory;
        }
        if let Some(inventory) = self.inventory.take() {
            match inventory.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!(
                        "OpenCode startup name inventory remains incomplete: {}",
                        error.code
                    );
                    self.blocked = true;
                    return StartupRecoveryStep::Blocked;
                }
                Err(_) => {
                    eprintln!("OpenCode startup name inventory worker failed");
                    self.blocked = true;
                    return StartupRecoveryStep::Blocked;
                }
            }
        }
        match journal.recover_outbox() {
            Ok(window) => {
                *recovery_count = recovery_count.saturating_add(window.len());
                let remaining = 32usize.saturating_sub(recovery_sample.len());
                recovery_sample.extend(window.into_iter().take(remaining));
            }
            Err(error) => {
                // Damage and storage uncertainty hold the index closed. Keep
                // servicing exact Operation reads; never clear custody or
                // authorize a retry because census progress failed.
                eprintln!(
                    "OpenCode startup journal recovery remains incomplete: {}",
                    error.code
                );
                self.blocked = true;
                return StartupRecoveryStep::Blocked;
            }
        }
        if journal.startup_recovery_complete() {
            StartupRecoveryStep::Complete
        } else {
            StartupRecoveryStep::Progressed
        }
    }
}

#[cfg(test)]
mod startup_poll_tests {
    use super::*;

    #[tokio::test]
    async fn recovery_ticks_keep_an_inflight_module_next_available() {
        let mut recovery_tick = tokio::time::interval(Duration::from_millis(5));
        recovery_tick.tick().await;
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut module_next = Box::pin(async move { receive.await.unwrap() });

        // A large legacy census can require many bounded windows. Keep the
        // same module.next future selectable through every recovery tick.
        for _ in 0..32 {
            assert!(matches!(
                wait_for_module_next_or_recovery(module_next.as_mut(), &mut recovery_tick, true)
                    .await,
                StartupPoll::RecoveryTick
            ));
        }
        assert!(send.send("agent.reconcile").is_ok());

        let response = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let StartupPoll::ModuleNext(response) =
                    wait_for_module_next_or_recovery(module_next.as_mut(), &mut recovery_tick, true)
                        .await
                {
                    break response;
                }
            }
        })
        .await
        .expect("the retained module.next future must complete");
        assert_eq!(response, "agent.reconcile");
    }
}

struct HostSession<'a> {
    config: &'a AdapterConfig,
    credential: &'a Credential,
    journal: &'a Journal,
    claim: &'a ModuleContractClaim,
    boot_id: String,
    hello_base: Value,
    root_hint: Mutex<Option<String>>,
    ipc_link: AsyncMutex<Option<swarm_client::ModuleLink>>,
    #[cfg(test)]
    outcome_observer: Option<std::sync::Arc<Mutex<Vec<Value>>>>,
    // A partial batch or transient sweep error is retried from the main loop.
    retention_retry_pending: AtomicBool,
}

pub async fn run_owned(bootstrap: OwnedBootstrap) -> Result<()> {
    let OwnedBootstrap {
        config,
        credential,
        worker,
        contract,
    } = bootstrap;
    config.validate()?;
    let route_sha256 = digest_json(&serde_json::to_value(&config.native_options)?)?;
    let journal = Arc::new(Journal::open(
        &config.state_dir,
        &config.binding_id,
        config.generation,
        &config.native_options.scope_key(),
        &route_sha256,
    )?);
    // Before hello, visit at most one sparse durable-closure window. A cold
    // legacy name census starts on a blocking worker and never gates service
    // polling; exact requested-operation reads remain available meanwhile.
    let mut journal_recovery = journal.recover_outbox_before_hello()?;
    let mut journal_recovery_count = journal_recovery.len();
    let mut startup_recovery = StartupRecovery::start(journal.clone())?;
    let mut native_owner = native_owner::NativeOwnerController::new(config.owned_native.clone());

    // The verified owner and boot identity come from the per-scope helper.
    // The helper owns this adapter process; any OpenCode child is a separate
    // native owner with its own pinned image and state-root receipts.
    let boot_id = worker.boot_id.clone();
    let scope = config.native_options.scope_key();
    let root_hint = if journal.startup_recovery_complete() {
        journal.native_root_for_hello(&config.binding_id, config.generation, &scope)?
    } else {
        None
    };

    let native_probe = match NativeClient::connect(&config.native_options).await {
        Ok((service, info)) => match service.probe_route(&config.native_options).await {
            Ok(()) => Some(info),
            Err(_) => None,
        },
        Err(_) => None,
    };
    let owner = &worker.owner_record;
    let hello_base = json!({
        "boot_id":boot_id,
        "module_artifact_id":ARTIFACT_ID,
        "native_ready":native_probe.is_some(),
        "managed_owner":{"token":owner["token"],"process":owner["process"]}
    });
    let host = HostSession {
        config: &config,
        credential: &credential,
        journal: &journal,
        claim: &contract,
        boot_id: boot_id.clone(),
        hello_base,
        root_hint: Mutex::new(root_hint),
        ipc_link: AsyncMutex::new(None),
        #[cfg(test)]
        outcome_observer: None,
        retention_retry_pending: AtomicBool::new(false),
    };
    let run_result = async {
        host.hello_retry().await?;
        if let Some(probe) = native_probe {
            let event_id = uuid::Uuid::new_v4().to_string();
            let observation = json!({
                "event_id":event_id,
                "state":{
                    "boot_id":boot_id,
                    "native_scope_key":scope,
                    "service_id":config.native_options.service_id,
                    "service_version":probe.version,
                    "service_pid":probe.pid,
                    "route_model":config.native_options.model
                }
            });
            journal.queue_observation(&event_id, observation)?;
        }

        let mut next_retention_retry = TokioInstant::now()
            + Duration::from_millis(JOURNAL_RETENTION_RETRY_MS);
        let mut recovery_tick = tokio::time::interval_at(
            TokioInstant::now() + Duration::from_millis(STARTUP_RECOVERY_TICK_MS),
            Duration::from_millis(STARTUP_RECOVERY_TICK_MS),
        );
        recovery_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut module_next = Box::pin(host.call("module.next", json!({})));
        let mut root_checked = false;
        let mut retention_started = false;
        let mut recovery_report_queued = false;
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        loop {
            let event = tokio::select! {
                signal = &mut ctrl_c => {
                    signal.map_err(|_| Error::new("ADAPTER_SIGNAL", "shutdown signal could not be installed"))?;
                    break;
                }
                event = wait_for_module_next_or_recovery(
                    module_next.as_mut(),
                    &mut recovery_tick,
                    !startup_recovery.blocked
                        && !journal.startup_recovery_complete(),
                ) => event,
            };
            if let StartupPoll::ModuleNext(result) = event {
                let response = result?;
                let command_value = response.get("command").ok_or_else(|| {
                    Error::new(
                        "HOST_COMMAND_SCHEMA",
                        "module.next response lacks command state",
                    )
                })?;
                if !command_value.is_null() {
                    let command: RuntimeCommand =
                        serde_json::from_value(command_value.clone()).map_err(|_| {
                            Error::new(
                                "HOST_COMMAND_SCHEMA",
                                "module command does not match the shared contract",
                            )
                        })?;
                    verify_command_scope(host.config, &command, host.claim)?;
                    handle_command(&host, &command, &mut native_owner).await?;
                } else {
                    sleep(Duration::from_millis(IDLE_POLL_MS)).await;
                }
                // The first module.next is issued immediately after hello.
                // Recovery ticks keep this same RPC pinned and process exact
                // command reads before advancing another body window.
                flush_outbox(&host).await?;
                module_next = Box::pin(host.call("module.next", json!({})));

                // A failed module.next may have admitted work before its response was
                // lost. It is intentionally not retried on this boot; startup recovery
                // will make that Operation unknown and offer readback-only reconcile.
                if host.retention_retry_pending.load(Ordering::Acquire)
                    && TokioInstant::now() >= next_retention_retry
                {
                    attempt_journal_retention(&host);
                    next_retention_retry =
                        TokioInstant::now() + Duration::from_millis(JOURNAL_RETENTION_RETRY_MS);
                }
            }

            let step = startup_recovery
                .advance(
                    &journal,
                    &mut journal_recovery,
                    &mut journal_recovery_count,
                )
                .await;
            if matches!(step, StartupRecoveryStep::Complete) {
                if !root_checked {
                    if let Some(root) = journal.native_root_for_hello(
                        &config.binding_id,
                        config.generation,
                        &scope,
                    )? {
                        host.set_root_hint(&root)?;
                    }
                    let startup_scan = journal.startup_scan_counts();
                    eprintln!(
                        "OpenCode startup recovery scan: operation_names={}, parsed_bodies={}, bytes={}, outbox_entries={}, unresolved={}",
                        startup_scan.operations_listed,
                        startup_scan.operation_bodies_parsed,
                        startup_scan.operation_bytes_read,
                        startup_scan.outbox_entries_scanned,
                        startup_scan.unresolved_found,
                    );
                    root_checked = true;
                }
                if !retention_started {
                    attempt_journal_retention(&host);
                    retention_started = true;
                }
            }
            if !recovery_report_queued
                && !journal_recovery.is_empty()
                && matches!(step, StartupRecoveryStep::Complete | StartupRecoveryStep::Blocked)
            {
                let entries = journal_recovery.iter().take(32).map(|recovered| json!({
                    "operation_key":recovered.operation_key,
                    "operation_id":recovered.operation_id,
                    "native_effect_unknown":recovered.history.recovery_unknown(),
                    "torn_tail":recovered.history.torn_tail_evidence.as_ref().map(|tail| json!({
                        "valid_prefix_bytes":tail.valid_prefix_bytes,
                        "tail_sha256":tail.tail_sha256,
                        "tail_bytes":tail.tail_bytes,
                    })),
                    "native_replay_permitted":false,
                })).collect::<Vec<_>>();
                let event_id = uuid::Uuid::new_v4().to_string();
                journal.queue_observation(&event_id, json!({
                    "event_id":event_id,
                    "state":{
                        "boot_id":boot_id,
                        "native_scope_key":scope,
                        "journal_recovery":{
                            "count":journal_recovery_count,
                            "entries":entries,
                            "coverage":if journal.startup_recovery_complete() {"complete"} else {"partial"},
                            "native_replay_permitted":false,
                        },
                    },
                }))?;
                recovery_report_queued = true;
            }
        }
        Ok(())
    }
    .await;
    let stop_result = shutdown_native_owner_until_confirmed(&mut native_owner).await;
    match (run_result, stop_result) {
        (Ok(()), stop) => stop,
        (Err(run_error), Ok(())) => Err(run_error),
        (Err(run_error), Err(stop_error)) => Err(Error::new(
            run_error.code,
            format!(
                "adapter loop failed; native owner stop also reported {}",
                stop_error.code
            ),
        )),
    }
}

async fn shutdown_native_owner_until_confirmed(
    native_owner: &mut native_owner::NativeOwnerController,
) -> Result<()> {
    loop {
        match native_owner.shutdown().await {
            Ok(()) => return Ok(()),
            Err(_) if native_owner.owns_child() => {
                // The owner and Child remain in this process across each
                // cancel-safe wait timeout. No new native work is admitted.
                sleep(Duration::from_millis(IDLE_POLL_MS)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn handle_command(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    native_owner: &mut native_owner::NativeOwnerController,
) -> Result<()> {
    let config = host.config;
    let journal = host.journal;
    let claim = host.claim;
    if let Some(root) = command.native_root_id.as_deref() {
        host.set_root_hint(root)?;
    }
    let history = journal.load(&command.operation_id)?;
    if history.torn_tail_evidence.is_some() {
        return Err(Error::new(
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
            "torn operation evidence requires readback; native action and journal append are withheld",
        ));
    }
    let expected_receipt = module_receipt::for_command(claim, command)?;
    let expected_receipt_value = serde_json::to_value(&expected_receipt)?;
    if history
        .intent
        .as_ref()
        .is_some_and(|intent| intent.module_receipt != expected_receipt)
        || history
            .outcome
            .as_ref()
            .is_some_and(|saved| saved["details"]["module_receipt"] != expected_receipt_value)
        || history.result_params.as_ref().is_some_and(|saved| {
            saved["page"]["source"]["result_module_receipt"] != expected_receipt_value
        })
    {
        return Err(Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "saved operation receipt differs from the authenticated command identity",
        ));
    }
    if let Some(saved) = history.outcome.as_ref() {
        validate_saved_dispatch_outcome(host, command, &history, saved)?;
        let digest = digest_json(saved)?;
        if history.acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        journal.queue_outcome_value(saved)?;
        return flush_outbox(host).await;
    }
    if command.method != "agent.reconcile"
        && command.method != "agent.result"
        && let Some(intent) = history.intent.as_ref()
    {
        // A durable pre-send intent without a saved outcome is ambiguous. It
        // is never permission to repeat its native POST, even if this command
        // arrives again on the same adapter boot.
        let unknown = unknown_from_intent(intent, "NATIVE_EFFECT_MAY_HAVE_OCCURRED")?;
        journal.queue_outcome(&unknown)?;
        return flush_outbox(host).await;
    }

    let options = match route_options(config, command) {
        Ok(options) => options,
        Err(error) => {
            let outcome = outcome(
                command,
                claim,
                EffectOutcome::Rejected,
                &config.native_options,
                command.native_root_id.clone(),
                None,
                diagnostic(&error),
            )?;
            journal.queue_outcome(&outcome)?;
            return flush_outbox(host).await;
        }
    };

    match command.method.as_str() {
        "agent.open" => handle_open(host, command, &options, native_owner).await,
        "task.dispatch" | "agent.send" | "native.opencode.loop_step" => {
            handle_send(host, command, &options).await
        }
        "agent.reply" => handle_reply(host, command, &options).await,
        "agent.background" => handle_background(host, command, &options).await,
        "agent.refresh" => handle_refresh(host, command, &options).await,
        "agent.reconcile" => handle_reconcile(host, command, &options).await,
        "agent.result" => handle_result(host, command, &options).await,
        "native.mcp.install" | "native.mcp.observe" | "native.mcp.arm" | "native.mcp.read" => {
            handle_native_mcp(host, command, &options).await
        }
        _ => {
            let error = Error::new(
                "UNSUPPORTED_CAPABILITY",
                "the requested native method is not implemented by this OpenCode artifact",
            );
            let outcome = outcome(
                command,
                claim,
                EffectOutcome::Rejected,
                &options,
                command.native_root_id.clone(),
                None,
                diagnostic(&error),
            )?;
            journal.queue_outcome(&outcome)?;
            flush_outbox(host).await
        }
    }
}

async fn handle_native_mcp(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let admitted = match native_mcp_intake::admit(command, claim) {
        Ok(admitted) => admitted,
        Err(error) => {
            let outcome = outcome(
                command,
                claim,
                EffectOutcome::Rejected,
                options,
                command.native_root_id.clone(),
                None,
                diagnostic(&error),
            )?;
            journal.queue_outcome(&outcome)?;
            return flush_outbox(host).await;
        }
    };
    let intent = intent_for(command, claim, options, None, None)?;
    journal.write_intent(&intent)?;
    let result = match NativeClient::connect(options).await {
        Ok((native, _)) => {
            match native_mcp::execute(&native, &admitted.effect_command, options).await {
                Ok(receipt) => outcome(
                    command,
                    claim,
                    EffectOutcome::Applied,
                    options,
                    command.native_root_id.clone(),
                    None,
                    json!({
                        "native_mcp":receipt,
                        "native_replay":false,
                    }),
                )?,
                Err(failure) => outcome(
                    command,
                    claim,
                    failure.outcome,
                    options,
                    command.native_root_id.clone(),
                    None,
                    {
                        let mut details = diagnostic(&failure.error);
                        details["native_mcp_action"] = json!(command.method);
                        details["native_replay"] = json!(false);
                        details
                    },
                )?,
            }
        }
        Err(error) => outcome(
            command,
            claim,
            EffectOutcome::Rejected,
            options,
            command.native_root_id.clone(),
            None,
            {
                let mut details = diagnostic(&error);
                details["native_mcp_action"] = json!(command.method);
                details["native_replay"] = json!(false);
                details
            },
        )?,
    };
    journal.queue_outcome(&result)?;
    flush_outbox(host).await
}

async fn handle_result(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let selector = &command.input["selector"];
    if selector["kind"] == "opencode_assistant_result" {
        return handle_assistant_result(host, command, options).await;
    }
    let selector_fields = ["kind", "input_operation_id", "session_id"];
    if selector.as_object().is_none_or(|object| {
        object.len() != selector_fields.len()
            || object
                .keys()
                .any(|field| !selector_fields.contains(&field.as_str()))
    }) || selector["kind"] != "input_status"
    {
        return queue_rejected(
            host,
            command,
            options,
            command.native_root_id.clone(),
            &Error::new(
                "UNSUPPORTED_RESULT_SELECTOR",
                "OpenCode result delivery supports only exact input_status selectors",
            ),
        )
        .await;
    }
    let input_operation_id = selector["input_operation_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| Error::invalid("input_status selector lacks its Operation ID"))?;
    let session_id = selector["session_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| Error::invalid("input_status selector lacks its native session ID"))?;
    let root = command.native_root_id.as_deref().ok_or_else(|| {
        Error::new(
            "NATIVE_ROOT_MISSING",
            "input status requires the exact binding session",
        )
    })?;
    if session_id != root {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.into()),
            &Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "input_status selector names another native session",
            ),
        )
        .await;
    }

    let target_history = host.journal.load(input_operation_id)?;
    let Some(target_intent) = target_history.intent.as_ref() else {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.into()),
            &Error::new(
                "RESULT_TARGET_INTENT_MISSING",
                "exact input status requires the saved dispatch or send intent",
            ),
        )
        .await;
    };
    if !matches!(
        target_intent.method.as_str(),
        "task.dispatch" | "agent.send" | "native.opencode.loop_step"
    ) {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.into()),
            &Error::new(
                "RESULT_TARGET_METHOD",
                "input status target must be a saved dispatch or send Operation",
            ),
        )
        .await;
    }
    let target_receipt = module_receipt::for_target_intent(
        host.claim,
        target_intent,
        input_operation_id,
        command.target_input_sha256.as_deref(),
        &command.binding_id,
        command.generation,
    )?;
    let target_native_input_id = input_id(input_operation_id);
    if target_intent.native_root_id.as_deref() != Some(root)
        || target_intent.native_input_id.as_deref() != Some(target_native_input_id.as_str())
        || target_intent.native_scope_key != options.scope_key()
    {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.into()),
            &Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved target intent differs from the exact selected session or input",
            ),
        )
        .await;
    }

    let mut result_intent = intent_for(command, host.claim, options, None, None)?;
    result_intent.result_input_status = Some(ResultInputStatusIntent {
        input_operation_id: input_operation_id.into(),
        native_session_id: session_id.into(),
        native_input_id: target_native_input_id.clone(),
        target_module_receipt: target_receipt.clone(),
    });
    let own_history = host.journal.load(&command.operation_id)?;
    if let Some(saved_intent) = own_history.intent.as_ref() {
        if saved_intent != &result_intent {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved result selector differs from the exact admitted readback request",
            ));
        }
    } else {
        // Persist the exact target/session/receipt before any native read.
        host.journal.write_intent(&result_intent)?;
    }

    if let Some(saved) = own_history.result_params.as_ref() {
        if saved["page"]["source"]["input_operation_id"] != input_operation_id
            || saved["page"]["source"]["target_module_receipt"]
                != serde_json::to_value(&target_receipt)?
            || saved["page"]["source"]["native_session_id"] != session_id
            || saved["page"]["source"]["native_input_id"] != target_native_input_id
        {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved result page differs from its exact target receipt or selector",
            ));
        }
        let digest = digest_json(saved)?;
        if own_history.result_acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        host.journal.queue_result(saved)?;
        return flush_outbox(host).await;
    }

    let native = match NativeClient::connect(options).await {
        Ok((native, _)) => native,
        Err(error) => return queue_unknown_result(host, &result_intent, &error).await,
    };
    let evidence = match native.read_input_status(target_intent, options).await {
        Ok(evidence) => evidence,
        Err(error) => return queue_unknown_result(host, &result_intent, &error).await,
    };
    let params = match input_status_result_page(
        command,
        &result_intent,
        target_intent,
        &target_receipt,
        &evidence.input_message_sha256,
    ) {
        Ok(params) => params,
        Err(error) => {
            return queue_rejected(host, command, options, Some(root.into()), &error).await;
        }
    };
    host.journal.queue_result(&params)?;
    flush_outbox(host).await
}

async fn handle_assistant_result(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    if !module_runtime::normalized_result_enabled(host.claim) {
        return queue_rejected(
            host,
            command,
            options,
            command.native_root_id.clone(),
            &Error::new(
                "NORMALIZED_RESULT_CONTRACT_UNAVAILABLE",
                "OpenCode assistant pages require the exact normalized-result descriptor opt-in",
            ),
        )
        .await;
    }
    let selector = &command.input["selector"];
    let selector_fields = ["kind", "input_operation_id", "session_id", "message_id"];
    if selector.as_object().is_none_or(|object| {
        object.len() != selector_fields.len()
            || object
                .keys()
                .any(|field| !selector_fields.contains(&field.as_str()))
    }) || selector["kind"] != "opencode_assistant_result"
    {
        return queue_rejected(
            host,
            command,
            options,
            command.native_root_id.clone(),
            &Error::new(
                "UNSUPPORTED_RESULT_SELECTOR",
                "normalized OpenCode result delivery requires one exact assistant message selector",
            ),
        )
        .await;
    }
    let input_operation_id = selector["input_operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("assistant result selector lacks its input Operation ID"))?;
    let session_id = selector["session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("assistant result selector lacks its native session ID"))?;
    let assistant_message_id = selector["message_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("assistant result selector lacks its message ID"))?;
    let root = command.native_root_id.as_deref().ok_or_else(|| {
        Error::new(
            "NATIVE_ROOT_MISSING",
            "assistant result requires the exact binding session",
        )
    })?;
    if session_id != root {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.to_owned()),
            &Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "assistant result selector names another native session",
            ),
        )
        .await;
    }

    let target_history = host.journal.load(input_operation_id)?;
    let Some(target_intent) = target_history.intent.as_ref() else {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.to_owned()),
            &Error::new(
                "RESULT_TARGET_INTENT_MISSING",
                "assistant result requires the saved task.dispatch intent",
            ),
        )
        .await;
    };
    if target_intent.method != "task.dispatch" {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.to_owned()),
            &Error::new(
                "RESULT_TARGET_METHOD",
                "normalized assistant result target must be task.dispatch",
            ),
        )
        .await;
    }
    let target_receipt = module_receipt::for_target_intent(
        host.claim,
        target_intent,
        input_operation_id,
        command.target_input_sha256.as_deref(),
        &command.binding_id,
        command.generation,
    )?;
    let target_native_input_id = input_id(input_operation_id);
    if target_intent.native_root_id.as_deref() != Some(root)
        || target_intent.native_input_id.as_deref() != Some(target_native_input_id.as_str())
        || target_intent.native_scope_key != options.scope_key()
    {
        return queue_rejected(
            host,
            command,
            options,
            Some(root.to_owned()),
            &Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved task.dispatch intent differs from the selected assistant session or input",
            ),
        )
        .await;
    }

    let admission = target_intent.dispatch_admission.clone().ok_or_else(|| {
        Error::new(
            "RESULT_DISPATCH_ADMISSION_MISSING",
            "normalized assistant result target lacks its typed dispatch admission",
        )
    })?;
    admission.validate().map_err(|_| {
        Error::new(
            "RESULT_DISPATCH_ADMISSION_INVALID",
            "saved task.dispatch admission receipt is invalid",
        )
    })?;
    let target_outcome = target_history.outcome.as_ref().ok_or_else(|| {
        Error::new(
            "RESULT_DISPATCH_OUTCOME_MISSING",
            "normalized assistant result target has no retained dispatch outcome",
        )
    })?;
    let target_outcome: RuntimeOutcome =
        serde_json::from_value(target_outcome.clone()).map_err(|_| {
            Error::new(
                "RESULT_DISPATCH_OUTCOME_INVALID",
                "saved task.dispatch outcome is not a runtime outcome",
            )
        })?;
    let saved_admission: TaskDispatchAdmissionReceipt = serde_json::from_value(
        target_outcome.details["dispatch_admission"].clone(),
    )
    .map_err(|_| {
        Error::new(
            "RESULT_DISPATCH_ADMISSION_INVALID",
            "saved task.dispatch outcome lacks its typed admission receipt",
        )
    })?;
    if !matches!(
        target_outcome.outcome,
        EffectOutcome::Applied | EffectOutcome::Accepted
    ) || target_outcome.operation_id != input_operation_id
        || target_outcome.native_input_id.as_deref() != Some(target_native_input_id.as_str())
        || target_outcome.details["module_receipt"] != serde_json::to_value(&target_receipt)?
        || saved_admission != admission
        || admission.module_receipt != target_receipt
        || admission.operation_id != input_operation_id
        || admission.binding_id != command.binding_id
        || admission.binding_generation != command.generation
        || admission.native_input_id.as_deref() != Some(target_native_input_id.as_str())
    {
        return Err(Error::new(
            "RESULT_DISPATCH_ADMISSION_MISMATCH",
            "saved dispatch outcome does not prove the exact admitted input receipt",
        ));
    }

    let origin: NormalizedResultOriginContext = serde_json::from_value(
        command.input["normalized_result_origin"].clone(),
    )
    .map_err(|_| {
        Error::new(
            "NORMALIZED_RESULT_ORIGIN_INVALID",
            "Store-supplied normalized result origin is malformed",
        )
    })?;
    origin.validate().map_err(|_| {
        Error::new(
            "NORMALIZED_RESULT_ORIGIN_INVALID",
            "Store-supplied normalized result origin is invalid",
        )
    })?;
    let selector_sha256 = sha256(canonical_json(selector)?.as_bytes());
    let context = admission.context();
    if origin.binding_id != command.binding_id
        || origin.binding_generation != command.generation
        || origin.target_operation_id != input_operation_id
        || origin.target_input_sha256 != target_receipt.input_sha256
        || origin.selector_sha256 != selector_sha256
        || origin.attempt_id != context.attempt_id
        || origin.task_id != context.task_id
        || origin.task_revision != context.task_revision
        || origin.task_snapshot_sha256 != context.task_snapshot_sha256
        || origin.producer.assignment_id != input_operation_id
        || origin.producer.dispatch_operation_id != input_operation_id
        || origin.producer.attempt_id != context.attempt_id
        || origin.producer.task_id != context.task_id
        || origin.producer.task_revision != context.task_revision
        || origin.producer.source_text_sha256 != context.source_text_sha256
        || origin.producer.source_text_bytes != context.source_text_bytes
        || origin.producer.native_payload_sha256 != admission.native_payload_sha256
        || origin.producer.native_payload_bytes != admission.native_payload_bytes
        || origin.producer.native_input_id.as_deref() != Some(target_native_input_id.as_str())
        || origin.producer.module_receipt != target_receipt
    {
        return Err(Error::new(
            "NORMALIZED_RESULT_ORIGIN_MISMATCH",
            "normalized result origin differs from the retained dispatch admission",
        ));
    }

    let mut result_intent = intent_for(command, host.claim, options, None, None)?;
    result_intent.result_assistant = Some(ResultAssistantIntent {
        input_operation_id: input_operation_id.to_owned(),
        native_session_id: session_id.to_owned(),
        native_input_id: target_native_input_id.clone(),
        assistant_message_id: assistant_message_id.to_owned(),
        assistant_parent_id: target_native_input_id.clone(),
        target_input_sha256: target_receipt.input_sha256.clone(),
        selector_sha256: selector_sha256.clone(),
        target_module_receipt: target_receipt.clone(),
    });
    let own_history = host.journal.load(&command.operation_id)?;
    if let Some(saved_intent) = own_history.intent.as_ref() {
        if saved_intent != &result_intent {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved assistant result selector differs from the exact admitted readback request",
            ));
        }
    } else {
        host.journal.write_intent(&result_intent)?;
    }
    let native_identity = assistant_identity(assistant_message_id, &target_native_input_id);
    if let Some(saved) = own_history.result_params.as_ref() {
        let source: NormalizedResultPageSource =
            serde_json::from_value(saved["page"]["source"].clone()).map_err(|_| {
                Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "saved normalized assistant result page is malformed",
                )
            })?;
        if source.origin != origin
            || source.result_module_receipt != result_intent.module_receipt
            || source.native_response_identity.as_deref() != Some(native_identity.as_str())
        {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved normalized assistant page differs from its exact source identity",
            ));
        }
        let digest = digest_json(saved)?;
        if own_history.result_acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        host.journal.queue_result(saved)?;
        return flush_outbox(host).await;
    }

    let native = match NativeClient::connect(options).await {
        Ok((native, _)) => native,
        Err(error) => return queue_unknown_result(host, &result_intent, &error).await,
    };
    let evidence = match native
        .read_assistant_result(target_intent, options, assistant_message_id)
        .await
    {
        Ok(evidence) => evidence,
        Err(error) => return queue_unknown_result(host, &result_intent, &error).await,
    };
    let params = normalized_assistant_result_page(
        command,
        &result_intent,
        origin,
        target_receipt,
        &evidence,
        &native_identity,
    )?;
    host.journal.queue_result(&params)?;
    flush_outbox(host).await
}

fn assistant_identity(message_id: &str, parent_id: &str) -> String {
    format!("opencode:assistant:{message_id}:parent:{parent_id}")
}

fn normalized_assistant_result_page(
    command: &RuntimeCommand,
    result_intent: &OperationIntent,
    origin: NormalizedResultOriginContext,
    target_receipt: swarm_contracts::runtime::ModuleReceiptIdentity,
    evidence: &AssistantResultEvidence,
    native_identity: &str,
) -> Result<Value> {
    let target = result_intent.result_assistant.as_ref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "assistant result intent has no exact target",
        )
    })?;
    if target.target_module_receipt != target_receipt
        || target.input_operation_id != origin.target_operation_id
        || origin.producer.native_input_id.as_deref() != Some(target.native_input_id.as_str())
        || target.assistant_message_id != evidence.message_id
        || target.assistant_parent_id != evidence.parent_id
        || native_identity != assistant_identity(&evidence.message_id, &evidence.parent_id)
    {
        return Err(Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "assistant result evidence differs from the sealed target identity",
        ));
    }
    let body = canonical_json(&evidence.message)?.into_bytes();
    let payload_sha256 = sha256(&body);
    if payload_sha256 != evidence.payload_sha256 || body.len() as u64 != evidence.payload_bytes {
        return Err(Error::new(
            "NATIVE_ASSISTANT_PAYLOAD_MISMATCH",
            "assistant result body digest changed during normalization",
        ));
    }
    if let Some(expected) = command
        .input
        .get("normalized_result_payload_identity")
        .filter(|value| value.is_object())
        && (expected["sha256"].as_str() != Some(payload_sha256.as_str())
            || expected["byte_length"].as_u64() != Some(body.len() as u64)
            || expected["complete"] == false)
    {
        return Err(Error::new(
            "NORMALIZED_RESULT_PAYLOAD_MISMATCH",
            "assistant result body differs from the Store-admitted payload identity",
        ));
    }
    let offset = command.input["offset_bytes"].as_u64().unwrap_or(0);
    let requested = command.input["length_bytes"]
        .as_u64()
        .unwrap_or(65_536)
        .min(65_536);
    let total = body.len() as u64;
    if offset > total || (requested == 0 && offset < total) {
        return Err(Error::invalid("assistant result page range is invalid"));
    }
    let end = offset.saturating_add(requested).min(total);
    let start = usize::try_from(offset)
        .map_err(|_| Error::invalid("assistant result page offset is invalid"))?;
    let stop =
        usize::try_from(end).map_err(|_| Error::invalid("assistant result page end is invalid"))?;
    let page = &body[start..stop];
    let source = NormalizedResultPageSource {
        schema_id: "swarm.normalized_result_page".to_owned(),
        schema_version: 1,
        origin,
        result_operation_id: command.operation_id.clone(),
        result_input_sha256: result_intent.module_receipt.input_sha256.clone(),
        result_module_receipt: result_intent.module_receipt.clone(),
        payload_sha256,
        payload_bytes: total,
        native_response_identity: Some(native_identity.to_owned()),
        execution_complete: false,
        task_completion: "unknown".to_owned(),
        native_replay: false,
    };
    source.validate().map_err(|_| {
        Error::new(
            "NORMALIZED_RESULT_SOURCE_INVALID",
            "normalized assistant result source failed the shared contract",
        )
    })?;
    Ok(json!({
        "operation_id":command.operation_id,
        "page":{
            "source":source,
            "offset_bytes":offset,
            "byte_length":page.len(),
            "total_bytes":total,
            "eof":end == total,
            "media_type":"application/json",
            "content_base64":STANDARD.encode(page),
            "page_sha256":sha256(page),
        }
    }))
}

async fn queue_unknown_result(
    host: &HostSession<'_>,
    intent: &OperationIntent,
    error: &Error,
) -> Result<()> {
    let mut unknown = unknown_from_intent(intent, &error.code)?;
    attach_native_http_failure(&mut unknown.details, error);
    if let Some(target) = intent.result_input_status.as_ref() {
        unknown.details["completion_condition"] = json!("input_status_unavailable");
        unknown.details["input_operation_id"] = json!(target.input_operation_id);
        unknown.details["target_module_receipt"] = json!(target.target_module_receipt);
        unknown.details["assistant_result_correlation"] = json!("not_exposed");
        unknown.details["assistant_result_correlation_reason"] =
            json!("assistant_message_has_no_input_parent_in_public_projection");
    } else if let Some(target) = intent.result_assistant.as_ref() {
        unknown.details["completion_condition"] = json!("assistant_result_unavailable");
        unknown.details["input_operation_id"] = json!(target.input_operation_id);
        unknown.details["native_session_id"] = json!(target.native_session_id);
        unknown.details["native_input_id"] = json!(target.native_input_id);
        unknown.details["assistant_message_id"] = json!(target.assistant_message_id);
        unknown.details["expected_assistant_parent_id"] = json!(target.assistant_parent_id);
        unknown.details["target_input_sha256"] = json!(target.target_input_sha256);
        unknown.details["target_module_receipt"] = json!(target.target_module_receipt);
        unknown.details["assistant_result_correlation"] = json!("unknown");
        unknown.details["assistant_result_correlation_reason"] =
            json!("exact_parent_identity_unavailable_or_native_readback_failed");
        unknown.details["native_response_identity"] = json!("unavailable");
    } else {
        return Err(Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "result intent is missing its exact target",
        ));
    }
    unknown.details["task_completion"] = json!("unknown");
    unknown.details["execution_complete"] = json!(false);
    unknown.details["native_replay"] = json!(false);
    host.journal.queue_outcome(&unknown)?;
    flush_outbox(host).await
}

fn input_status_result_page(
    command: &RuntimeCommand,
    result_intent: &OperationIntent,
    target_intent: &OperationIntent,
    target_receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    input_message_sha256: &str,
) -> Result<Value> {
    let result_receipt = &result_intent.module_receipt;
    let target_status = result_intent.result_input_status.as_ref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "saved result intent has no input status target",
        )
    })?;
    if target_status.target_module_receipt != *target_receipt
        || target_status.input_operation_id != target_intent.operation_id
        || target_status.native_input_id != input_id(&target_intent.operation_id)
    {
        return Err(Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "result page target differs from its saved Operation receipt",
        ));
    }
    let content = serde_json::to_vec(&json!({
        "status":"native_input_admitted",
        "input_operation_id":target_intent.operation_id,
        "native_session_id":target_status.native_session_id,
        "native_input_id":target_status.native_input_id,
        "input_message_sha256":input_message_sha256,
        "assistant_result_correlation":"not_exposed",
        "assistant_result_correlation_reason":"assistant_message_has_no_input_parent_in_public_projection",
        "task_completion":"unknown",
        "execution_complete":false
    }))?;
    let total = content.len();
    let offset = match command.input.get("offset_bytes") {
        None => 0,
        Some(value) => usize::try_from(
            value
                .as_u64()
                .ok_or_else(|| Error::invalid("result offset must be a nonnegative integer"))?,
        )
        .map_err(|_| Error::invalid("result offset is too large"))?,
    };
    let length = match command.input.get("length_bytes") {
        None => 65_536usize,
        Some(value) => usize::try_from(
            value
                .as_u64()
                .ok_or_else(|| Error::invalid("result length must be a nonnegative integer"))?,
        )
        .ok()
        .filter(|value| (1..=65_536).contains(value))
        .ok_or_else(|| Error::invalid("result length must be 1..65536"))?,
    };
    if offset > total {
        return Err(Error::invalid("result offset exceeds status page length"));
    }
    let end = total.min(offset.saturating_add(length));
    let page = &content[offset..end];
    let source = json!({
        "kind":"input_status",
        "result_operation_id":command.operation_id,
        "result_input_sha256":result_receipt.input_sha256,
        "result_module_receipt":result_receipt,
        "input_operation_id":target_intent.operation_id,
        "target_method":target_intent.method,
        "target_input_sha256":target_receipt.input_sha256,
        "target_module_receipt":target_receipt,
        "native_session_id":target_status.native_session_id,
        "native_input_id":target_status.native_input_id,
        "input_message_sha256":input_message_sha256,
        "evidence":"exact_user_message_projection",
        "assistant_result_correlation":"not_exposed",
        "assistant_result_correlation_reason":"assistant_message_has_no_input_parent_in_public_projection",
        "read_method":"session.message.get",
        "read_consistency":"repeated_equal_projection_not_atomic_snapshot",
        "task_completion":"unknown",
        "execution_complete":false,
        "native_replay":false
    });
    let page_sha256 = format!("{:x}", Sha256::digest(page));
    Ok(json!({
        "operation_id":command.operation_id,
        "page":{
            "source":source,
            "offset_bytes":offset,
            "byte_length":page.len(),
            "total_bytes":total,
            "eof":end==total,
            "media_type":"application/json",
            "content_base64":STANDARD.encode(page),
            "page_sha256":page_sha256
        }
    }))
}

async fn handle_open(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
    native_owner: &mut native_owner::NativeOwnerController,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let root = root_id(&command.binding_id, command.generation);
    host.set_root_hint(&root)?;
    let intent = intent_for(command, claim, options, None, None)?;
    // The adapter-owned service is a native effect too.  Persist the exact
    // operation marker before either starting Bun or sending HTTP.
    journal.write_intent(&intent)?;
    let owner_ready = match native_owner.ensure_started(options).await {
        Ok(receipt) => receipt,
        Err(failure) => {
            let state = if failure.effect_attempted {
                EffectOutcome::Unknown
            } else {
                EffectOutcome::Rejected
            };
            let mut details = failure.details;
            details["diagnostic_code"] = json!(failure.error.code);
            let outcome = outcome(command, claim, state, options, Some(root), None, details)?;
            journal.queue_outcome(&outcome)?;
            return flush_outbox(host).await;
        }
    };
    let owner_ready = owner_ready.map(serde_json::to_value).transpose()?;
    let native = match NativeClient::connect(options).await {
        Ok((native, _)) => native,
        Err(error) => {
            let mut rejected = outcome(
                command,
                claim,
                EffectOutcome::Rejected,
                options,
                None,
                None,
                diagnostic(&error),
            )?;
            attach_owned_service_ready_only(&mut rejected, owner_ready.as_ref(), options);
            journal.queue_outcome(&rejected)?;
            return flush_outbox(host).await;
        }
    };
    let provider_auth = match native_owner.bootstrap_provider_auth(&native, options).await {
        Ok(proof) => proof,
        Err(error) => {
            let mut failure =
                effect_failure(command, claim, options, Some(root), None, &error, true)?;
            attach_owned_service_ready_only(&mut failure, owner_ready.as_ref(), options);
            journal.queue_outcome(&failure)?;
            return flush_outbox(host).await;
        }
    };
    let location = match native.preflight_open(options).await {
        Ok(location) => location,
        Err(failure) => {
            let mut details = diagnostic(&failure.error);
            if failure.error.code == "NATIVE_MODEL_UNAVAILABLE"
                && let Some(category) = failure.catalog_failure_category
            {
                details["catalog_failure_category"] = json!(category.as_str());
            }
            attach_provider_auth_proof(&mut details, provider_auth.as_ref());
            let mut rejected = outcome(
                command,
                claim,
                EffectOutcome::Rejected,
                options,
                None,
                None,
                details,
            )?;
            attach_owned_service_ready_only(&mut rejected, owner_ready.as_ref(), options);
            journal.queue_outcome(&rejected)?;
            return flush_outbox(host).await;
        }
    };
    // This fsync is the barrier before the one native create POST. On any
    // later crash this operation becomes Unknown and can only be read back.
    let outcome = match native.create_root(command, options, &root, location).await {
        Ok(()) => {
            let mut details = json!({
                "completion_condition":"native_session_created",
                "durable_origin":"exact_session_created_event",
                "native_replay":false,
                "model":options.model
            });
            if let Some(receipt) = owner_ready.as_ref() {
                details["owned_service_ready"] = receipt.clone();
            }
            if let Some(proof) = provider_auth {
                details["provider_auth"] = proof;
            }
            outcome(
                command,
                claim,
                EffectOutcome::Applied,
                options,
                Some(root),
                None,
                details,
            )?
        }
        Err(error) => {
            let mut failure =
                effect_failure(command, claim, options, Some(root), None, &error, true)?;
            attach_owned_service_ready_only(&mut failure, owner_ready.as_ref(), options);
            attach_provider_auth_proof(&mut failure.details, provider_auth.as_ref());
            failure
        }
    };
    journal.queue_outcome(&outcome)?;
    flush_outbox(host).await
}

fn attach_provider_auth_proof(details: &mut Value, proof: Option<&Value>) {
    if let Some(proof) = proof {
        details["provider_auth"] = proof.clone();
    }
}

fn attach_owned_service_ready_only(
    outcome: &mut RuntimeOutcome,
    receipt: Option<&Value>,
    options: &NativeOptions,
) {
    let Some(receipt) = receipt else {
        return;
    };
    outcome.details["owned_service_ready"] = receipt.clone();
    outcome.details["owned_service_ready_only"] = json!(true);
    outcome.details["native_replay"] = json!(false);
    outcome.details["model"] = json!(options.model);
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn task_prompt_for(
    command: &RuntimeCommand,
    options: &NativeOptions,
    claim: &ModuleContractClaim,
) -> Result<Option<String>> {
    if command.method != "task.dispatch" {
        if command.input.get("task_prompt").is_some() {
            return Err(Error::new(
                "TASK_PROMPT_INVALID",
                "TaskPrompt envelope is only valid for task.dispatch",
            ));
        }
        return Ok(None);
    }
    if !module_runtime::task_prompt_v1_selected(claim) {
        return Err(Error::new(
            "TASK_PROMPT_SCHEMA_NOT_SELECTED",
            "this OpenCode artifact requires the exact TaskPrompt v1 descriptor",
        ));
    }
    let value = command.input.get("task_prompt").ok_or_else(|| {
        Error::new(
            "TASK_PROMPT_REQUIRED",
            "selected TaskPrompt v1 dispatch has no Store-produced envelope",
        )
    })?;
    let envelope: TaskPromptEnvelopeV1 = serde_json::from_value(value.clone())
        .map_err(|_| Error::new("TASK_PROMPT_INVALID", "TaskPrompt envelope is malformed"))?;
    envelope
        .validate_shape()
        .map_err(|_| Error::new("TASK_PROMPT_INVALID", "TaskPrompt envelope is invalid"))?;
    let context: TaskDispatchContext = serde_json::from_value(
        command
            .input
            .get("task_dispatch_context")
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "TASK_PROMPT_CONTEXT_REQUIRED",
                    "TaskPrompt requires its Store dispatch context",
                )
            })?,
    )
    .map_err(|_| {
        Error::new(
            "TASK_PROMPT_CONTEXT_INVALID",
            "TaskPrompt dispatch context is malformed",
        )
    })?;
    context.validate().map_err(|_| {
        Error::new(
            "TASK_PROMPT_CONTEXT_INVALID",
            "TaskPrompt dispatch context is invalid",
        )
    })?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || envelope.attempt_id != context.attempt_id
        || envelope.task_id != context.task_id
        || envelope.task_revision != context.task_revision
        || envelope.task_snapshot_sha256 != context.task_snapshot_sha256
        || envelope.prompt_sha256 != sha256(envelope.prompt.as_bytes())
    {
        return Err(Error::new(
            "TASK_PROMPT_IDENTITY_MISMATCH",
            "TaskPrompt differs from its authenticated dispatch identity or digest",
        ));
    }
    let expected_snapshot_digest = format!("sha256:{}", context.task_snapshot_sha256);
    if let Some(packet) = command.input.get("launch_dispatch_packet")
        && (packet["task"]["task_id"].as_str() != Some(context.task_id.as_str())
            || packet["task"]["revision"].as_i64() != Some(context.task_revision)
            || packet["task"]["attempt_id"].as_str() != Some(context.attempt_id.as_str())
            || packet["task"]["snapshot_digest"].as_str()
                != Some(expected_snapshot_digest.as_str())
            || packet["selection"]["provider"].as_str() != Some(options.model.provider_id.as_str())
            || packet["selection"]["model"].as_str() != Some(options.model.id.as_str())
            || packet["selection"]["variant"].as_str() != Some(options.model.variant.as_str()))
    {
        return Err(Error::new(
            "TASK_PROMPT_LAUNCH_PACKET_MISMATCH",
            "launch packet differs from the frozen dispatch or pinned native model",
        ));
    }
    Ok(Some(envelope.prompt))
}

fn task_prompt_preflight_command(command: &RuntimeCommand, prompt: &str) -> RuntimeCommand {
    RuntimeCommand {
        operation_id: command.operation_id.clone(),
        method: String::from("agent.send"),
        created_at_ms: command.created_at_ms,
        binding_id: command.binding_id.clone(),
        generation: command.generation,
        native_root_id: command.native_root_id.clone(),
        route: command.route.clone(),
        input: json!({"delivery":"next_turn","text":prompt}),
        input_sha256: command.input_sha256.clone(),
        target_input_sha256: command.target_input_sha256.clone(),
    }
}

fn normalized_dispatch_admission(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    intent: &OperationIntent,
    input_id: &str,
    prompt_text: &str,
) -> Result<Option<TaskDispatchAdmissionReceipt>> {
    if command.method != "task.dispatch" || !module_runtime::normalized_dispatch_enabled(host.claim)
    {
        return Ok(None);
    }
    let context: TaskDispatchContext =
        serde_json::from_value(command.input["task_dispatch_context"].clone()).map_err(|_| {
            Error::new(
                "TASK_DISPATCH_CONTEXT_INVALID",
                "dispatch context is malformed",
            )
        })?;
    context.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch context is invalid",
        )
    })?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != host.boot_id
    {
        return Err(Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch context does not match this operation, binding, or boot",
        ));
    }
    let source_text = command.input["text"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::new("TASK_DISPATCH_CONTEXT_INVALID", "source text is missing"))?;
    let source_text_sha256 = sha256(source_text.as_bytes());
    if context.source_text_sha256 != source_text_sha256
        || context.source_text_bytes != source_text.len() as u64
    {
        return Err(Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch context source identity differs from the command",
        ));
    }
    if intent.module_receipt.operation_id != command.operation_id
        || intent.native_input_id.as_deref() != Some(input_id)
    {
        return Err(Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "native input identity differs from the durable module intent",
        ));
    }
    if !module_runtime::task_prompt_v1_selected(host.claim) {
        return Err(Error::new(
            "TASK_PROMPT_SCHEMA_NOT_SELECTED",
            "dispatch admission requires the exact TaskPrompt v1 descriptor",
        ));
    }
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt: intent.module_receipt.clone(),
        operation_id: context.operation_id,
        binding_id: context.binding_id,
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id,
        attempt_id: context.attempt_id,
        task_id: context.task_id,
        task_revision: context.task_revision,
        task_snapshot_sha256: context.task_snapshot_sha256,
        source_text_sha256: context.source_text_sha256,
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: sha256(prompt_text.as_bytes()),
        native_payload_bytes: prompt_text.len() as u64,
        native_input_id: Some(input_id.to_owned()),
    };
    receipt.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch admission is invalid",
        )
    })?;
    Ok(Some(receipt))
}

fn validate_saved_dispatch_outcome(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    history: &journal::OperationHistory,
    saved: &Value,
) -> Result<()> {
    if command.method != "task.dispatch" || !module_runtime::normalized_dispatch_enabled(host.claim)
    {
        return Ok(());
    }
    let outcome: RuntimeOutcome = serde_json::from_value(saved.clone()).map_err(|_| {
        Error::new(
            "ADAPTER_JOURNAL",
            "saved outcome does not match the shared runtime outcome",
        )
    })?;
    let admission = saved["details"]["dispatch_admission"].clone();
    match outcome.outcome {
        EffectOutcome::Applied | EffectOutcome::Accepted => {
            let receipt: TaskDispatchAdmissionReceipt =
                serde_json::from_value(admission).map_err(|_| {
                    Error::new(
                        "ADAPTER_INTENT_MISMATCH",
                        "known dispatch outcome lacks a typed admission receipt",
                    )
                })?;
            receipt.validate().map_err(|_| {
                Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "saved dispatch admission receipt is invalid",
                )
            })?;
            let context = receipt.context();
            let source_text = command.input["text"].as_str().ok_or_else(|| {
                Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "dispatch source text is missing while validating saved receipt",
                )
            })?;
            let prompt_text = task_prompt_for(command, &host.config.native_options, host.claim)?
                .ok_or_else(|| {
                    Error::new(
                        "ADAPTER_INTENT_MISMATCH",
                        "selected TaskPrompt v1 dispatch has no immutable prompt",
                    )
                })?;
            let expected = module_receipt::for_command(host.claim, command)?;
            if receipt.module_receipt != expected
                || context.operation_id != command.operation_id
                || context.binding_id != command.binding_id
                || context.binding_generation != command.generation
                || context.worker_boot_id != host.boot_id
                || context.source_text_sha256 != sha256(source_text.as_bytes())
                || context.source_text_bytes != source_text.len() as u64
                || receipt.native_payload_sha256 != sha256(prompt_text.as_bytes())
                || receipt.native_payload_bytes != prompt_text.len() as u64
                || receipt.native_input_id.as_deref() != outcome.native_input_id.as_deref()
                || history
                    .intent
                    .as_ref()
                    .and_then(|intent| intent.dispatch_admission.as_ref())
                    .is_none_or(|saved| saved != &receipt)
            {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "saved dispatch outcome differs from its durable admission intent",
                ));
            }
        }
        EffectOutcome::Rejected | EffectOutcome::Unknown => {
            if !saved["details"]["dispatch_admission"].is_null() {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "rejected or unknown dispatch outcome cannot carry admission evidence",
                ));
            }
        }
    }
    Ok(())
}

async fn handle_send(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let root = match command.native_root_id.as_deref() {
        Some(root) => root,
        None => {
            let error = Error::new(
                "NATIVE_ROOT_MISSING",
                "send requires the exact owned native root",
            );
            return queue_rejected(host, command, options, None, &error).await;
        }
    };
    host.set_root_hint(root)?;
    let native = match NativeClient::connect(options).await {
        Ok((native, _)) => native,
        Err(error) => {
            return queue_rejected(host, command, options, Some(root.into()), &error).await;
        }
    };
    let prompt_text = match task_prompt_for(command, options, claim) {
        Err(error) => {
            return queue_rejected(host, command, options, Some(root.into()), &error).await;
        }
        Ok(Some(prompt)) => {
            let preflight_command = task_prompt_preflight_command(command, &prompt);
            match native
                .preflight_send(&preflight_command, options, root)
                .await
            {
                Ok(preflight_prompt) if preflight_prompt == prompt => prompt,
                Ok(_) => {
                    let error = Error::new(
                        "TASK_PROMPT_PREFLIGHT_MISMATCH",
                        "OpenCode preflight changed the Store-produced TaskPrompt",
                    );
                    return queue_rejected(host, command, options, Some(root.into()), &error).await;
                }
                Err(error) => {
                    return queue_rejected(host, command, options, Some(root.into()), &error).await;
                }
            }
        }
        Ok(None) => match native.preflight_send(command, options, root).await {
            Ok(text) => text,
            Err(error) => {
                return queue_rejected(host, command, options, Some(root.into()), &error).await;
            }
        },
    };
    let input = input_id(&command.operation_id);
    let mut intent = intent_for(
        command,
        claim,
        options,
        Some(input.clone()),
        Some(&prompt_text),
    )?;
    intent.dispatch_admission =
        normalized_dispatch_admission(host, command, &intent, &input, &prompt_text)?;
    // Only the digest and byte count are retained; prompt text stays in this
    // stack frame and the HTTP request body.
    journal.write_intent(&intent)?;
    let outcome = match native
        .admit_input(command, root, &input, &prompt_text)
        .await
    {
        Ok(admission) => {
            let delivery =
                native::native_delivery_for_method(&command.method).ok_or_else(|| {
                    Error::new(
                        "ADAPTER_INTENT_MISMATCH",
                        "admitted input method has no native delivery",
                    )
                })?;
            let mut details = json!({
                "completion_condition":"native_input_admitted",
                "delivery":delivery,
                "execution_boundary":if admission.promoted_sequence.is_some() { "native_input_promoted" } else if delivery == "steer" { "native_safe_next_loop_step" } else { "queued_next_turn" },
                "admitted_sequence":admission.admitted_sequence,
                "promoted_sequence":admission.promoted_sequence,
                "evidence":"prompt_response",
                "assistant_result_correlation":"not_exposed",
                "assistant_result_correlation_reason":"assistant_message_has_no_input_parent_in_public_projection",
                "execution_complete":false,
                "native_replay":false
            });
            if let Some(admission) = intent.dispatch_admission.as_ref() {
                if admission.native_input_id.as_deref() != Some(input.as_str()) {
                    return Err(Error::new(
                        "ADAPTER_INTENT_MISMATCH",
                        "native admission receipt does not match the submitted input ID",
                    ));
                }
                details["dispatch_admission"] = serde_json::to_value(admission)?;
            }
            outcome(
                command,
                claim,
                EffectOutcome::Applied,
                options,
                Some(root.into()),
                Some(input),
                details,
            )?
        }
        Err(error) => effect_failure(
            command,
            claim,
            options,
            Some(root.into()),
            Some(input),
            &error,
            true,
        )?,
    };
    journal.queue_outcome(&outcome)?;
    flush_outbox(host).await
}

async fn handle_reply(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let root = command.native_root_id.clone();
    let (native, _) = match NativeClient::connect(options).await {
        Ok(value) => value,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    let prepared = match native_interactions::prepare_reply(&native, command, options).await {
        Ok(prepared) => prepared,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    let mut intent = intent_for(command, claim, options, None, None)?;
    intent.marker["native_opencode_reply"] = prepared.intent_identity();
    journal.write_intent(&intent)?;

    let response = match prepared.body.clone() {
        Some(body) => native.control_post(&prepared.path, body).await,
        None => native.control_post_empty(&prepared.path).await,
    };
    let result = match response {
        Ok(Value::Null) => outcome(
            command,
            claim,
            EffectOutcome::Applied,
            options,
            root,
            None,
            json!({
                "completion_condition":"native_reply_acknowledged",
                "evidence":"exact_native_204",
                "kind":prepared.kind,
                "action":prepared.action,
                "session_id":prepared.session_id,
                "request_id":prepared.request_id,
                "request_fingerprint":prepared.request_fingerprint,
                "affected_request_ids":prepared.affected_request_ids,
                "native_replay":false
            }),
        )?,
        Ok(_) => {
            let error = Error::new(
                "NATIVE_OUTCOME_UNKNOWN",
                "native reply returned an unsupported response body",
            );
            effect_failure(command, claim, options, root, None, &error, true)?
        }
        Err(error) => effect_failure(command, claim, options, root, None, &error, true)?,
    };
    journal.queue_outcome(&result)?;
    flush_outbox(host).await
}

async fn handle_background(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let root = command.native_root_id.clone();
    let target = match native_interactions::validate_target(command, "session_id") {
        Ok(target) => target,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    let (native, _) = match NativeClient::connect(options).await {
        Ok(value) => value,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    if let Err(error) = native.verify_control_scope(command, options, &target).await {
        return queue_rejected(host, command, options, root, &error).await;
    }
    let supported = match native_interactions::background_capability(&native).await {
        Ok(supported) => supported,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    if !supported {
        let error = Error::new(
            "UNSUPPORTED_CAPABILITY",
            "OpenCode does not advertise backgroundSubagents for this service",
        );
        return queue_rejected(host, command, options, root, &error).await;
    }

    let mut intent = intent_for(command, claim, options, None, None)?;
    intent.marker["native_opencode_background"] = json!({
        "session_id":target,
        "capability":"backgroundSubagents",
        "capability_value":true
    });
    journal.write_intent(&intent)?;
    let path = format!("/experimental/session/{target}/background");
    let result = match native.control_post_empty(&path).await {
        Ok(Value::Bool(changed)) => outcome(
            command,
            claim,
            EffectOutcome::Applied,
            options,
            root,
            None,
            json!({
                "completion_condition":if changed { "native_background_changed" } else { "native_background_noop" },
                "native_endpoint":"/experimental/session/:sessionID/background",
                "capability_observation":{"backgroundSubagents":true},
                "session_id":target,
                "changed":changed,
                "execution_complete":false,
                "native_replay":false
            }),
        )?,
        Ok(_) => {
            let error = Error::new(
                "NATIVE_OUTCOME_UNKNOWN",
                "background endpoint did not return its documented boolean",
            );
            effect_failure(command, claim, options, root, None, &error, true)?
        }
        Err(error) => effect_failure(command, claim, options, root, None, &error, true)?,
    };
    journal.queue_outcome(&result)?;
    flush_outbox(host).await
}

async fn handle_refresh(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let root = command.native_root_id.clone();
    let session_id = match native_interactions::validate_target(command, "session_id") {
        Ok(session_id) => session_id,
        Err(error) => return queue_rejected(host, command, options, root, &error).await,
    };
    let after_sequence = match command.input.get("after_sequence") {
        None => 0,
        Some(value) => match value.as_u64() {
            Some(sequence) => sequence,
            None => {
                let error = Error::new(
                    "INVALID_NATIVE_INTERACTION",
                    "after_sequence must be a nonnegative integer",
                );
                return queue_rejected(host, command, options, root, &error).await;
            }
        },
    };
    let (native, _) = match NativeClient::connect(options).await {
        Ok(value) => value,
        Err(error) => {
            let result = outcome(
                command,
                claim,
                EffectOutcome::Unknown,
                options,
                root,
                None,
                diagnostic(&error),
            )?;
            journal.queue_outcome(&result)?;
            return flush_outbox(host).await;
        }
    };
    let page = match native_interactions::read_interaction_page(
        &native,
        command,
        options,
        &session_id,
        after_sequence,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            let result = outcome(
                command,
                claim,
                EffectOutcome::Unknown,
                options,
                root,
                None,
                diagnostic(&error),
            )?;
            journal.queue_outcome(&result)?;
            return flush_outbox(host).await;
        }
    };
    journal.queue_observation(
        &page.event_id,
        json!({
            "event_id":page.event_id,
            "state":page.observation
        }),
    )?;
    // A successful module.observe call is the cursor-advance boundary. The
    // Store supplies this acknowledged position on the next refresh command.
    flush_outbox(host).await?;
    let state = if page.coverage == "gap" {
        EffectOutcome::Unknown
    } else {
        EffectOutcome::Applied
    };
    let result = outcome(
        command,
        claim,
        state,
        options,
        root,
        None,
        json!({
            "completion_condition":"native_interaction_observation",
            "observation_event_id":page.event_id,
            "session_id":session_id,
            "after_sequence":page.after_sequence,
            "through_sequence":page.through_sequence,
            "next_after_sequence":page.through_sequence,
            "has_more":page.has_more,
            "coverage":page.coverage,
            "pending_question_count":page.pending_questions,
            "pending_permission_count":page.pending_permissions,
            "native_replay":false
        }),
    )?;
    journal.queue_outcome(&result)?;
    flush_outbox(host).await
}

async fn handle_reconcile(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| Error::invalid("reconcile target operation ID is missing"))?;
    let own_intent = intent_for(command, claim, options, None, None)?;
    let own_history = journal.load(&command.operation_id)?;
    if let Some(saved_intent) = own_history.intent.as_ref()
        && saved_intent.reconcile_target_operation_id.as_deref() != Some(target_id)
    {
        let error = Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "reconcile target differs from its saved operation identity",
        );
        let rejected = outcome(
            command,
            claim,
            EffectOutcome::Rejected,
            options,
            command.native_root_id.clone(),
            None,
            diagnostic(&error),
        )?;
        journal.queue_outcome(&rejected)?;
        return flush_outbox(host).await;
    }
    if own_history.intent.is_none() {
        journal.write_intent(&own_intent)?;
    }

    let target_history = journal.load(target_id)?;
    if let Some(target_intent) = target_history.intent.as_ref() {
        let target_receipt = module_receipt::for_target_intent(
            claim,
            target_intent,
            target_id,
            command.target_input_sha256.as_deref(),
            &command.binding_id,
            command.generation,
        )?;
        if let Some(saved) = target_history.outcome.as_ref()
            && saved["details"]["module_receipt"] != serde_json::to_value(&target_receipt)?
        {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved target outcome differs from its original Operation receipt",
            ));
        }
    }
    let target_can_settle = target_history
        .outcome
        .as_ref()
        .is_none_or(|saved| saved["outcome"] == "unknown");
    let target_already_applied = target_history
        .outcome
        .as_ref()
        .is_some_and(|saved| saved["outcome"] == "applied");
    let mut resolved = false;
    if let Some(target_intent) = target_history.intent.as_ref() {
        if target_intent.binding_id == command.binding_id
            && target_intent.generation == command.generation
            && target_intent.native_scope_key == options.scope_key()
            && target_intent.route_sha256 == digest_json(&serde_json::to_value(options)?)?
        {
            let evidence = match NativeClient::connect(options).await {
                Ok((native, _)) => match target_intent.method.as_str() {
                    "agent.open" => native
                        .reconcile_open(target_intent, options)
                        .await
                        .map(|()| None),
                    "task.dispatch" | "agent.send" | "native.opencode.loop_step" => native
                        .reconcile_input(target_intent, options)
                        .await
                        .map(Some),
                    "agent.reply" => native_interactions::reconcile_reply(
                        &native,
                        command,
                        options,
                        target_intent,
                    )
                    .await
                    .map(|()| None),
                    _ => Err(Error::new(
                        "NATIVE_EVIDENCE_UNAVAILABLE",
                        "saved method is outside the readback subset",
                    )),
                },
                Err(error) => Err(error),
            };
            match evidence {
                Ok(input_evidence) => {
                    if target_can_settle {
                        let target_outcome = link_reconcile_target(
                            applied_from_intent(target_intent, input_evidence)?,
                            &command.operation_id,
                        )?;
                        journal.queue_outcome(&target_outcome)?;
                        resolved = true;
                    } else {
                        resolved = target_already_applied;
                    }
                }
                Err(error) => {
                    if target_history.outcome.is_none() {
                        let target_outcome = link_reconcile_target(
                            unknown_from_intent(target_intent, &error.code)?,
                            &command.operation_id,
                        )?;
                        journal.queue_outcome(&target_outcome)?;
                    }
                }
            }
        } else {
            if target_history.outcome.is_none() {
                let error = Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "saved native intent no longer matches this binding route",
                );
                let target_outcome = link_reconcile_target(
                    unknown_from_intent(target_intent, &error.code)?,
                    &command.operation_id,
                )?;
                journal.queue_outcome(&target_outcome)?;
            }
        }
    }

    let reconcile_outcome = outcome(
        command,
        claim,
        EffectOutcome::Applied,
        options,
        command.native_root_id.clone(),
        None,
        json!({
            "completion_condition":"native_readback_recorded",
            "target_operation_id":target_id,
            "resolved":resolved,
            "native_replay":false
        }),
    )?;
    journal.queue_outcome(&reconcile_outcome)?;
    flush_outbox(host).await
}

async fn queue_rejected(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
    root: Option<String>,
    error: &Error,
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let outcome = outcome(
        command,
        claim,
        EffectOutcome::Rejected,
        options,
        root,
        None,
        diagnostic(error),
    )?;
    journal.queue_outcome(&outcome)?;
    flush_outbox(host).await
}

fn effect_failure(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    options: &NativeOptions,
    root: Option<String>,
    input: Option<String>,
    error: &Error,
    native_post_attempted: bool,
) -> Result<RuntimeOutcome> {
    let state = if !native_post_attempted || error.code == "NATIVE_REJECTED" {
        EffectOutcome::Rejected
    } else {
        EffectOutcome::Unknown
    };
    let mut details = diagnostic(error);
    details["native_replay"] = json!(false);
    outcome(command, claim, state, options, root, input, details)
}

fn applied_from_intent(
    intent: &OperationIntent,
    input_evidence: Option<InputEvidence>,
) -> Result<RuntimeOutcome> {
    let reply_identity = if intent.method == "agent.reply" {
        let identity = intent
            .marker
            .get("native_opencode_reply")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "saved reply intent has no exact native reply identity",
                )
            })?;
        if identity["feedback_present"] != false
            || intent.native_input_id.is_some()
            || intent.prompt_sha256.is_some()
            || intent.prompt_bytes.is_some()
        {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved reply intent cannot prove readback without replayable input",
            ));
        }
        Some(identity)
    } else {
        None
    };
    let (completion_condition, evidence, input_id, admitted_sequence, prompted_sequence) =
        match (input_evidence, reply_identity.is_some()) {
            (None, true) => (
                "native_reply_acknowledged",
                "complete_contiguous_native_reply_history",
                None,
                None,
                None,
            ),
            (None, false) => (
                "native_session_created",
                "exact_session_readback",
                None,
                None,
                None,
            ),
            (Some(_), true) => {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "reply readback cannot carry native input admission evidence",
                ));
            }
            (Some(evidence), false) => (
                "native_input_admitted",
                "durable_session_history",
                intent.native_input_id.clone(),
                evidence.admitted_sequence,
                evidence.prompted_sequence,
            ),
        };
    let is_input = input_id.is_some();
    let delivery = if is_input {
        Some(
            native::native_delivery_for_method(&intent.method).ok_or_else(|| {
                Error::new(
                    "ADAPTER_INTENT_MISMATCH",
                    "saved input intent has no recognized native delivery",
                )
            })?,
        )
    } else {
        None
    };
    let mut details = if is_input {
        json!({
            "completion_condition":completion_condition,
            "evidence":evidence,
            "selected_model":intent.model.clone(),
            "delivery":delivery,
            "execution_boundary":if prompted_sequence.is_some() { "native_input_promoted" } else if delivery == Some("steer") { "native_safe_next_loop_step" } else { "queued_next_turn" },
            "admitted_sequence":admitted_sequence,
            "promoted_sequence":prompted_sequence,
            "execution_complete":false,
            "native_replay":false
        })
    } else {
        json!({
            "completion_condition":completion_condition,
            "evidence":evidence,
            "selected_model":intent.model.clone(),
            "native_replay":false
        })
    };
    if let Some(admission) = intent.dispatch_admission.as_ref() {
        if input_id.as_deref() != admission.native_input_id.as_deref() {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved native input readback differs from its dispatch admission receipt",
            ));
        }
        details["dispatch_admission"] = serde_json::to_value(admission)?;
    }
    if let Some(identity) = reply_identity {
        details["reply_readback_identity"] = identity;
    }
    module_receipt::insert_into_details(&mut details, &intent.module_receipt)?;
    Ok(RuntimeOutcome {
        operation_id: intent.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some(intent.native_scope_key.clone()),
        native_root_id: intent.native_root_id.clone(),
        turn_id: None,
        native_input_id: input_id,
        details,
    })
}

fn unknown_from_intent(intent: &OperationIntent, code: &str) -> Result<RuntimeOutcome> {
    let mut details = json!({
        "diagnostic_code":code,
        "native_replay":false,
        "selected_model":intent.model.clone()
    });
    module_receipt::insert_into_details(&mut details, &intent.module_receipt)?;
    Ok(RuntimeOutcome {
        operation_id: intent.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: Some(intent.native_scope_key.clone()),
        native_root_id: intent.native_root_id.clone(),
        turn_id: None,
        native_input_id: intent.native_input_id.clone(),
        details,
    })
}

fn link_reconcile_target(
    mut target: RuntimeOutcome,
    reconcile_operation_id: &str,
) -> Result<RuntimeOutcome> {
    if reconcile_operation_id.trim().is_empty() || target.operation_id == reconcile_operation_id {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "reconcile target must name a distinct admitted operation",
        ));
    }
    let details = target.details.as_object_mut().ok_or_else(|| {
        Error::new(
            "ADAPTER_RECEIPT",
            "target outcome details must be a JSON object",
        )
    })?;
    details.insert(
        "reconcile_operation_id".to_owned(),
        json!(reconcile_operation_id),
    );
    Ok(target)
}

fn outcome(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    state: EffectOutcome,
    options: &NativeOptions,
    root: Option<String>,
    input: Option<String>,
    mut details: Value,
) -> Result<RuntimeOutcome> {
    if let Some(object) = details.as_object_mut() {
        object.insert("selected_model".into(), json!(options.model));
    }
    let identity = module_receipt::for_command(claim, command)?;
    module_receipt::insert_into_details(&mut details, &identity)?;
    Ok(RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: state,
        native_scope_key: Some(options.scope_key()),
        native_root_id: root,
        turn_id: None,
        native_input_id: input,
        details,
    })
}

fn diagnostic(error: &Error) -> Value {
    let mut details = json!({"diagnostic_code":error.code});
    attach_native_http_failure(&mut details, error);
    details
}

fn attach_native_http_failure(details: &mut Value, error: &Error) {
    if let Some(failure) = error.native_http_failure {
        details["native_http_failure"] = json!({
            "status":failure.status,
            "kind":failure.kind
        });
    }
}

impl<'a> HostSession<'a> {
    fn root_hint(&self) -> Result<Option<String>> {
        self.root_hint
            .lock()
            .map(|root| root.clone())
            .map_err(|_| Error::new("ADAPTER_STATE", "native root cache is unavailable"))
    }

    fn set_root_hint(&self, root: &str) -> Result<()> {
        if root != root_id(&self.config.binding_id, self.config.generation) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native root differs from the deterministic binding identity",
            ));
        }
        let mut saved = self
            .root_hint
            .lock()
            .map_err(|_| Error::new("ADAPTER_STATE", "native root cache is unavailable"))?;
        if saved.as_deref().is_some_and(|previous| previous != root) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native root changed for this binding generation",
            ));
        }
        *saved = Some(root.to_owned());
        Ok(())
    }

    fn save_store_root(&self, root: &str) -> Result<()> {
        self.journal.remember_native_root(
            &self.config.binding_id,
            self.config.generation,
            &self.config.native_options.scope_key(),
            root,
        )?;
        self.set_root_hint(root)
    }

    async fn open_link(&self) -> Result<swarm_client::ModuleLink> {
        let mut link = module_link::connect(
            &self.config.host_data_dir,
            self.credential,
            &self.config.ipc,
        )
        .await?;
        let mut params = self.hello_base.clone();
        if let Some(root) = self.root_hint()? {
            params["native_root_id"] = json!(root);
            params["native_scope_key"] = json!(self.config.native_options.scope_key());
        } else if let Some(object) = params.as_object_mut() {
            object.remove("native_root_id");
            object.remove("native_scope_key");
        }
        let response = module_link::hello(&mut link, params, self.claim).await?;
        let expected_checkpoint = self.journal.native_root_checkpoint_for_hello(
            &self.config.binding_id,
            self.config.generation,
            &self.config.native_options.scope_key(),
        )?;
        if let Some(root) = verify_hello(
            self.config,
            self.claim,
            &self.config.native_options.scope_key(),
            expected_checkpoint.as_deref(),
            &response,
        )? {
            self.save_store_root(&root)?;
        }
        Ok(link)
    }

    async fn hello_retry(&self) -> Result<()> {
        let mut last_error = Error::new("HOST_UNAVAILABLE", "module hello was not acknowledged");
        for (attempt, backoff_ms) in ACK_BACKOFF_MS.into_iter().enumerate() {
            match self.open_link().await {
                Ok(link) => {
                    *self.ipc_link.lock().await = Some(link);
                    return Ok(());
                }
                Err(error) => last_error = error,
            }
            if attempt + 1 < ACK_ATTEMPTS {
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
        Err(Error::new(
            "HOST_ACK_PENDING",
            format!("module hello remains unacknowledged: {}", last_error.code),
        ))
    }

    /// Reuse the authenticated, hello-bound link for ordinary RPCs. A
    /// transport-ambiguous exchange retires it; a later saved-receipt retry
    /// establishes and hello-binds a fresh link before resending those bytes.
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        #[cfg(test)]
        if method == "module.outcome"
            && let Some(observer) = self.outcome_observer.as_ref()
            && let Ok(mut calls) = observer.lock()
        {
            calls.push(params.clone());
        }
        let mut saved = self.ipc_link.lock().await;
        if saved.is_none() {
            *saved = Some(self.open_link().await?);
        }
        let link = saved
            .as_mut()
            .ok_or_else(|| Error::new("HOST_UNAVAILABLE", "module IPC link was not opened"))?;
        match module_link::call(link, method, params).await {
            Ok(response) => Ok(response),
            Err(error) => {
                if matches!(
                    error.code.as_str(),
                    "OUTCOME_UNKNOWN" | "PROTOCOL_ERROR" | "DISCONNECTED"
                ) {
                    *saved = None;
                }
                Err(error)
            }
        }
    }

    async fn call_recorded_retry(&self, method: &str, params: Value) -> Result<()> {
        let mut last_error = Error::new("HOST_ACK_SCHEMA", "host acknowledgement was not recorded");
        for (attempt, backoff_ms) in ACK_BACKOFF_MS.into_iter().enumerate() {
            match self.call(method, params.clone()).await {
                Ok(response) if response["recorded"] == true => return Ok(()),
                Ok(_) => {
                    last_error = Error::new(
                        "HOST_ACK_SCHEMA",
                        "host did not acknowledge the exact saved result",
                    )
                }
                Err(error) => last_error = error,
            }
            if attempt + 1 < ACK_ATTEMPTS {
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
        Err(Error::new(
            "HOST_ACK_PENDING",
            format!("host acknowledgement remains pending: {}", last_error.code),
        ))
    }
}

struct PreparedOutboxItem {
    path: PathBuf,
    item: PendingItem,
}

fn prepare_outbox_batch(
    journal: &Journal,
    items: Vec<(PathBuf, PendingItem)>,
) -> Result<(Vec<PreparedOutboxItem>, Vec<PathBuf>)> {
    let mut prepared = Vec::with_capacity(items.len());
    let mut deferred_paths = Vec::new();
    for (path, item) in items {
        let repair = match item.kind.as_str() {
            "outcome" => journal.repair_torn_outcome_ack(&item.payload),
            "result" => journal.repair_torn_result_ack(&item.payload),
            _ => Ok(false),
        };
        match repair {
            Ok(_) => {}
            Err(error) if error.code == "ADAPTER_JOURNAL_RECOVERY_UNKNOWN" => {
                deferred_paths.push(path);
                continue;
            }
            Err(error) => return Err(error),
        }
        if item.kind == "outcome" {
            journal.validate_pending_outcome(&path, &item)?;
        }
        prepared.push(PreparedOutboxItem { path, item });
    }
    Ok((prepared, deferred_paths))
}

async fn flush_outbox(host: &HostSession<'_>) -> Result<()> {
    let journal = host.journal;
    flush_outbox_with_sender(
        journal,
        |method, params| host.call_recorded_retry(method, params),
        |item| {
            match item.kind.as_str() {
                "outcome" => {
                    remember_root_from_outcome(host.config, journal, &item.payload)?;
                    if let Some(root) = item.payload["native_root_id"].as_str() {
                        host.set_root_hint(root)?;
                    }
                }
                "result" => {
                    if let Some(root) = item.payload["page"]["source"]["native_session_id"].as_str()
                    {
                        host.set_root_hint(root)?;
                    }
                }
                _ => {}
            }
            Ok(())
        },
        || {
            if journal.retention_pass_due_after_ack() {
                attempt_journal_retention(host);
            }
        },
    )
    .await
}

async fn flush_outbox_with_sender<F, Fut, D, R>(
    journal: &Journal,
    mut send_recorded: F,
    mut after_delivery: D,
    mut after_acknowledged: R,
) -> Result<()>
where
    F: FnMut(&'static str, Value) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
    D: FnMut(&PendingItem) -> Result<()>,
    R: FnMut(),
{
    let mut deferred_paths = HashSet::new();
    let mut deferred_reconcile_summaries = Vec::<(PathBuf, String)>::new();
    loop {
        let mut ready_summaries = Vec::new();
        for (path, target_operation_id) in &deferred_reconcile_summaries {
            if operation_outcome_is_acknowledged(journal, target_operation_id)? {
                ready_summaries.push(path.clone());
            }
        }
        for path in &ready_summaries {
            deferred_paths.remove(path);
        }
        deferred_reconcile_summaries.retain(|(path, _)| !ready_summaries.contains(path));

        let items = journal.pending_items_excluding(&deferred_paths)?;
        if items.is_empty() {
            return Ok(());
        }
        let (items, newly_deferred_paths) = prepare_outbox_batch(journal, items)?;
        deferred_paths.extend(newly_deferred_paths);
        for PreparedOutboxItem { path, item } in items {
            if let Some(target_operation_id) = resolved_reconcile_target_id(journal, &item)?
                && !operation_outcome_is_acknowledged(journal, &target_operation_id)?
            {
                deferred_paths.insert(path.clone());
                deferred_reconcile_summaries.push((path, target_operation_id));
                continue;
            }
            match item.kind.as_str() {
                "outcome" => {
                    let operation_id = item.payload["operation_id"].as_str().ok_or_else(|| {
                        Error::new("ADAPTER_OUTBOX", "saved outcome has no operation ID")
                    })?;
                    let digest = digest_json(&item.payload)?;
                    let history = match journal.load(operation_id) {
                        Ok(history) => history,
                        Err(error)
                            if matches!(
                                error.code.as_str(),
                                "ADAPTER_JOURNAL" | "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
                            ) =>
                        {
                            deferred_paths.insert(path);
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let mut acknowledged_now = false;
                    if history.acknowledged_sha256.as_deref() != Some(digest.as_str()) {
                        send_recorded("module.outcome", item.payload.clone()).await?;
                        journal.acknowledge_outcome(&item.payload)?;
                        acknowledged_now = true;
                    }
                    after_delivery(&item)?;
                    let cleanup = match journal.remove_pending(&path) {
                        Ok(cleanup) => cleanup,
                        Err(_)
                            if acknowledged_now
                                || history.acknowledged_sha256.as_deref()
                                    == Some(digest.as_str()) =>
                        {
                            eprintln!(
                                "OpenCode outcome ACK is durable; outbox removal is unknown and deferred"
                            );
                            deferred_paths.insert(path.clone());
                            if acknowledged_now {
                                after_acknowledged();
                            }
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if cleanup.durability == swarm_process::RemovalDurability::Unknown {
                        eprintln!(
                            "OpenCode outcome ACK is durable; outbox removal durability is unknown"
                        );
                        deferred_paths.insert(path.clone());
                    }
                    if acknowledged_now {
                        after_acknowledged();
                    }
                }
                "observation" => {
                    send_recorded("module.observe", item.payload.clone()).await?;
                    after_delivery(&item)?;
                    let cleanup = journal.remove_pending(&path)?;
                    if cleanup.durability == swarm_process::RemovalDurability::Unknown {
                        eprintln!("OpenCode observation cleanup durability is unknown");
                        deferred_paths.insert(path.clone());
                    }
                }
                "result" => {
                    let operation_id = item.payload["operation_id"].as_str().ok_or_else(|| {
                        Error::new("ADAPTER_OUTBOX", "saved result page has no operation ID")
                    })?;
                    let digest = digest_json(&item.payload)?;
                    let history = journal.load(operation_id)?;
                    let mut acknowledged_now = false;
                    if history.result_acknowledged_sha256.as_deref() != Some(digest.as_str()) {
                        send_recorded("module.result", item.payload.clone()).await?;
                        journal.acknowledge_result(&item.payload)?;
                        acknowledged_now = true;
                    }
                    after_delivery(&item)?;
                    let cleanup = match journal.remove_pending(&path) {
                        Ok(cleanup) => cleanup,
                        Err(_)
                            if acknowledged_now
                                || history.result_acknowledged_sha256.as_deref()
                                    == Some(digest.as_str()) =>
                        {
                            eprintln!(
                                "OpenCode result ACK is durable; outbox removal is unknown and deferred"
                            );
                            deferred_paths.insert(path.clone());
                            if acknowledged_now {
                                after_acknowledged();
                            }
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if cleanup.durability == swarm_process::RemovalDurability::Unknown {
                        eprintln!(
                            "OpenCode result ACK is durable; outbox removal durability is unknown"
                        );
                        deferred_paths.insert(path.clone());
                    }
                    if acknowledged_now {
                        after_acknowledged();
                    }
                }
                _ => {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "unsupported pending host message",
                    ));
                }
            }
        }
    }
}

fn attempt_journal_retention(host: &HostSession<'_>) {
    if !host.journal.startup_recovery_complete() {
        host.retention_retry_pending.store(true, Ordering::Release);
        return;
    }
    let queue_drained = match host.journal.reclaim_acknowledged(
        &host.config.binding_id,
        host.config.generation,
        &host.config.native_options.scope_key(),
    ) {
        Ok(summary) => {
            if summary.namespace_removed > 0 {
                eprintln!(
                    "OpenCode acknowledged journal retention removed {} operation(s) from the namespace",
                    summary.namespace_removed
                );
            }
            if !summary.queue_drained {
                eprintln!("OpenCode journal retention deferred at its bounded delete batch");
            }
            if summary.durability_unknown {
                eprintln!(
                    "OpenCode journal removal durability remains unknown after namespace work"
                );
            }
            summary.queue_drained
        }
        Err(error) => {
            eprintln!(
                "OpenCode journal retention did not complete: {}",
                error.code
            );
            false
        }
    };
    host.retention_retry_pending
        .store(!queue_drained, Ordering::Release);
}

fn remember_root_from_outcome(
    config: &AdapterConfig,
    journal: &Journal,
    value: &Value,
) -> Result<()> {
    let (Some(root), Some(scope)) = (
        value["native_root_id"].as_str(),
        value["native_scope_key"].as_str(),
    ) else {
        return Ok(());
    };
    if scope != config.native_options.scope_key() {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "saved outcome names another native service scope",
        ));
    }
    let operation_id = value["operation_id"]
        .as_str()
        .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "outcome operation ID is missing"))?;
    let history = journal.load(operation_id)?;
    if let Some(intent) = history.intent
        && (intent.binding_id != config.binding_id
            || intent.generation != config.generation
            || intent.native_scope_key != scope)
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "outcome intent belongs to another binding",
        ));
    }
    journal.remember_native_root(&config.binding_id, config.generation, scope, root)
}

fn verify_command_scope(
    config: &AdapterConfig,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<()> {
    if command.binding_id != config.binding_id || command.generation != config.generation {
        return Err(Error::new(
            "HOST_SCOPE_MISMATCH",
            "module command belongs to another binding generation",
        ));
    }
    module_receipt::for_command(claim, command)?;
    Ok(())
}

fn route_options(config: &AdapterConfig, command: &RuntimeCommand) -> Result<NativeOptions> {
    if command.route["runtime"] != RUNTIME
        || command.route["module_artifact_id"] != ARTIFACT_ID
        || command.route["workspace_option"] != "directory"
    {
        return Err(Error::new(
            "ARTIFACT_MISMATCH",
            "module command route names another runtime artifact",
        ));
    }
    let options: NativeOptions = serde_json::from_value(command.route["native_options"].clone())
        .map_err(|_| Error::new("CONFIG_ERROR", "module command route options are invalid"))?;
    options.validate()?;
    if options != config.native_options {
        return Err(Error::new(
            "NATIVE_ROUTE_CHANGED",
            "module command options differ from the exact local launch descriptor",
        ));
    }
    Ok(options)
}

fn verify_hello(
    config: &AdapterConfig,
    claim: &ModuleContractClaim,
    scope: &str,
    expected_root: Option<&str>,
    result: &Value,
) -> Result<Option<String>> {
    if result["binding_id"] != config.binding_id
        || result["generation"] != config.generation
        || result["route"]["runtime"] != RUNTIME
        || result["route"]["module_artifact_id"] != ARTIFACT_ID
        || result["route"]["workspace_option"] != "directory"
        || result["route"]["native_options"] != serde_json::to_value(&config.native_options)?
    {
        return Err(Error::new(
            "HOST_SCOPE_MISMATCH",
            "host module binding differs from the launch descriptor",
        ));
    }
    let host_root = optional_identity_field(result, "native_root_id")?;
    let host_scope = optional_identity_field(result, "native_scope_key")?;
    let store_root = match (host_root, host_scope) {
        (None, None) if expected_root.is_none() => None,
        (None, None) => {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "host omitted its retained native identity on reconnect",
            ));
        }
        (Some(root), Some(host_scope)) => {
            let expected = expected_root
                .map(str::to_owned)
                .unwrap_or_else(|| root_id(&config.binding_id, config.generation));
            if host_scope != scope || root != expected {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "host native identity differs from the exact binding root and service scope",
                ));
            }
            Some(root.to_owned())
        }
        _ => {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "host native root and scope must be supplied together",
            ));
        }
    };
    let negotiation = &result["module_contract_negotiation"];
    if negotiation["status"] != "negotiated"
        || negotiation["module_id"] != claim.module_id.as_str()
        || negotiation["artifact"] != serde_json::to_value(&claim.artifact)?
        || negotiation["protocol"] != serde_json::to_value(claim.protocol)?
        || negotiation["capabilities"] != serde_json::to_value(&claim.capabilities)?
        || negotiation["config_schema"] != serde_json::to_value(&claim.config_schema)?
        || negotiation["command_schemas"] != serde_json::to_value(&claim.command_schemas)?
        || negotiation["event_schemas"] != serde_json::to_value(&claim.event_schemas)?
        || negotiation["effects_authorized_by_descriptor"] != false
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "Store negotiation differs from the exact supervisor descriptor claim",
        ));
    }
    Ok(store_root)
}

fn optional_identity_field<'a>(value: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.is_empty() => Ok(Some(text)),
        Some(_) => Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "host native identity field must be a nonempty string or null",
        )),
    }
}

fn resolved_reconcile_target_id(journal: &Journal, item: &PendingItem) -> Result<Option<String>> {
    if item.kind != "outcome" || item.payload["details"]["resolved"] != true {
        return Ok(None);
    }
    let operation_id = item.payload["operation_id"].as_str().ok_or_else(|| {
        Error::new(
            "ADAPTER_OUTBOX",
            "saved reconcile summary has no operation ID",
        )
    })?;
    let history = journal.load(operation_id)?;
    let Some(intent) = history
        .intent
        .as_ref()
        .filter(|intent| intent.method == "agent.reconcile")
    else {
        return Ok(None);
    };
    let target_operation_id = intent
        .reconcile_target_operation_id
        .as_deref()
        .filter(|target| !target.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                "resolved reconcile summary has no exact target intent",
            )
        })?;
    if item.payload["details"]["target_operation_id"] != target_operation_id {
        return Err(Error::new(
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
            "resolved reconcile summary differs from its exact target intent",
        ));
    }
    Ok(Some(target_operation_id.to_owned()))
}

fn operation_outcome_is_acknowledged(journal: &Journal, operation_id: &str) -> Result<bool> {
    let history = journal.load(operation_id)?;
    if history.torn_tail_evidence.is_some() {
        return Ok(false);
    }
    let Some(digest) = history.outcome_sha256.as_deref() else {
        return Ok(false);
    };
    Ok(history
        .outcome
        .as_ref()
        .is_some_and(|outcome| outcome["outcome"] == "applied")
        && history.acknowledged_sha256.as_deref() == Some(digest))
}

#[cfg(test)]
mod outbox_recovery_tests {
    use super::*;
    use serde_json::json;
    use std::{collections::HashMap, io::Write};

    fn fixture_effect_intent(operation_id: &str) -> OperationIntent {
        let module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
            serde_json::from_value(json!({
            "schema_version":1,
            "module_id":"eliot.opencode",
            "artifact":{"artifact_id":ARTIFACT_ID,"version":ARTIFACT_VERSION},
            "protocol":{"major":1,"minor":0},
            "binding_id":"fixture-binding",
            "binding_generation":1,
            "operation_id":operation_id,
            "input_sha256":"c".repeat(64)
            }))
            .unwrap();
        OperationIntent {
            version: 2,
            operation_id: operation_id.to_owned(),
            module_receipt,
            method: "agent.send".to_owned(),
            binding_id: "fixture-binding".to_owned(),
            generation: 1,
            native_scope_key: "fixture-scope".to_owned(),
            native_root_id: None,
            native_input_id: Some("native-input".to_owned()),
            prompt_sha256: Some(format!("sha256:{}", "d".repeat(64))),
            prompt_bytes: Some(12),
            reconcile_target_operation_id: None,
            result_input_status: None,
            result_assistant: None,
            dispatch_admission: None,
            route_sha256: format!("sha256:{}", "a".repeat(64)),
            model: json!({"id":"fixture-model"}),
            marker: json!({"fixture_native_effect_was_already_attempted":true}),
        }
    }

    #[tokio::test]
    async fn consumer_recovers_result_page_ack_without_outcome_and_drains_past_corruption() {
        let root = std::env::temp_dir().join(format!(
            "swarm-opencode-result-ack-{}",
            uuid::Uuid::new_v4()
        ));
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        let page = |operation_id: &str| {
            json!({
                "operation_id":operation_id,
                "page":{"source":{"native_session_id":"fixture-root"},
                    "text":format!("exact saved page {operation_id}"),"complete":true}
            })
        };
        let recovered_id = "recovered-result";
        let recovered = page(recovered_id);
        let mut intent = fixture_effect_intent(recovered_id);
        intent.method = "agent.result".to_owned();
        journal.write_intent(&intent).unwrap();
        journal.queue_result(&recovered).unwrap();
        let mut expected_pages = HashMap::new();
        expected_pages.insert(recovered_id.to_owned(), recovered.clone());
        for index in 0..33 {
            let operation_id = format!("independent-result-{index:02}");
            let params = page(&operation_id);
            journal.queue_result(&params).unwrap();
            expected_pages.insert(operation_id, params);
        }
        let corrupt_id = "corrupt-result";
        journal.queue_result(&page(corrupt_id)).unwrap();
        let acknowledged_id = "already-acknowledged-result";
        let acknowledged = page(acknowledged_id);
        journal.queue_result(&acknowledged).unwrap();
        journal.acknowledge_result(&acknowledged).unwrap();
        let path = |operation_id: &str| {
            root.join("operations")
                .join(format!("{}.jsonl", operation_key(operation_id)))
        };
        let recovered_path = path(recovered_id);
        let recovered_prefix = std::fs::read(&recovered_path).unwrap();
        let acknowledged_path = path(acknowledged_id);
        let acknowledged_prefix = std::fs::read(&acknowledged_path).unwrap();
        for torn_path in [&recovered_path, &acknowledged_path] {
            std::fs::OpenOptions::new()
                .append(true)
                .open(torn_path)
                .unwrap()
                .write_all(b"{\"kind\":\"result_acknowledged\"")
                .unwrap();
        }
        let corrupt_path = path(corrupt_id);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&corrupt_path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
        let unrepaired_bytes = std::fs::read(&recovered_path).unwrap();
        let mut altered = recovered.clone();
        altered["page"]["text"] = json!("altered page");
        assert_eq!(
            journal.repair_torn_result_ack(&altered).unwrap_err().code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        assert_eq!(std::fs::read(&recovered_path).unwrap(), unrepaired_bytes);
        assert!(journal.load(recovered_id).unwrap().outcome.is_none());
        drop(journal);

        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        journal.recover_outbox().unwrap();
        let mut deliveries = Vec::new();
        flush_outbox_with_sender(
            &journal,
            |method, payload| {
                let operation_id = payload["operation_id"].as_str().unwrap();
                let history = journal.load(operation_id).unwrap();
                assert!(history.torn_tail_evidence.is_none());
                assert!(
                    history.outcome.is_none(),
                    "a result page needs no outcome ACK"
                );
                assert!(history.result_acknowledged_sha256.is_none());
                assert!(!history.native_replay_permitted());
                if operation_id == recovered_id {
                    assert_eq!(std::fs::read(&recovered_path).unwrap(), recovered_prefix);
                }
                let recorded =
                    method == "module.result" && expected_pages.get(operation_id) == Some(&payload);
                deliveries.push((method.to_owned(), payload));
                async move {
                    if recorded {
                        Ok(())
                    } else {
                        Err(Error::new(
                            "HOST_ACK_SCHEMA",
                            "fixture rejected an unsaved result page",
                        ))
                    }
                }
            },
            |_| Ok(()),
            || {},
        )
        .await
        .unwrap();
        assert_eq!(deliveries.len(), expected_pages.len());
        assert!(
            deliveries
                .iter()
                .all(|(method, _)| method == "module.result")
        );
        assert_eq!(
            deliveries
                .iter()
                .filter(|(_, payload)| payload["operation_id"] == recovered_id)
                .count(),
            1
        );
        assert!(
            !deliveries
                .iter()
                .any(|(_, payload)| payload["operation_id"] == corrupt_id
                    || payload["operation_id"] == acknowledged_id)
        );
        for (operation_id, params) in &expected_pages {
            let history = journal.load(operation_id).unwrap();
            assert!(history.outcome.is_none());
            assert_eq!(history.result_params.as_ref(), Some(params));
            assert_eq!(
                history.result_acknowledged_sha256.as_deref(),
                Some(digest_json(params).unwrap().as_str())
            );
        }
        let repaired_bytes = std::fs::read(&recovered_path).unwrap();
        let ack_suffix = repaired_bytes
            .strip_prefix(recovered_prefix.as_slice())
            .unwrap();
        assert_eq!(ack_suffix.last(), Some(&b'\n'));
        let ack_frame: Value = serde_json::from_slice(&ack_suffix[..ack_suffix.len() - 1]).unwrap();
        assert_eq!(ack_frame["kind"], "result_acknowledged");
        assert_eq!(ack_frame["result_sha256"], digest_json(&recovered).unwrap());
        assert_eq!(
            std::fs::read(&acknowledged_path).unwrap(),
            acknowledged_prefix
        );
        journal.acknowledge_result(&acknowledged).unwrap();
        assert_eq!(
            std::fs::read(&acknowledged_path).unwrap(),
            acknowledged_prefix
        );
        assert_eq!(std::fs::read(&corrupt_path).unwrap(), corrupt_bytes);
        let pending = journal.pending_items().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.key, corrupt_id);
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn consumer_resends_recovered_outcome_without_native_replay_and_drains_past_corruption() {
        let root =
            std::env::temp_dir().join(format!("swarm-opencode-torn-ack-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        let recovered = json!({"operation_id":"recovered-operation","outcome":"unknown"});
        let corrupt = json!({"operation_id":"corrupt-operation","outcome":"unknown"});
        let mut expected_outcomes = HashMap::new();
        let recovered_id = "recovered-operation";
        journal
            .write_intent(&fixture_effect_intent(recovered_id))
            .unwrap();
        journal.queue_outcome_value(&recovered).unwrap();
        expected_outcomes.insert(recovered_id.to_owned(), recovered.clone());
        for index in 0..33 {
            let operation_id = format!("independent-{index:02}");
            let outcome = json!({"operation_id":operation_id.clone(),"outcome":"unknown"});
            journal.queue_outcome_value(&outcome).unwrap();
            expected_outcomes.insert(operation_id, outcome);
        }
        journal.queue_outcome_value(&corrupt).unwrap();
        expected_outcomes.insert("corrupt-operation".to_owned(), corrupt.clone());

        let recovered_path = root
            .join("operations")
            .join(format!("{}.jsonl", operation_key(recovered_id)));
        let recovered_prefix = std::fs::read(&recovered_path).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&recovered_path)
            .unwrap()
            .write_all(b"{")
            .unwrap();

        let corrupt_path = root
            .join("operations")
            .join(format!("{}.jsonl", operation_key("corrupt-operation")));
        std::fs::OpenOptions::new()
            .append(true)
            .open(&corrupt_path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
        drop(journal);

        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        journal.recover_outbox().unwrap();
        assert_eq!(
            journal
                .native_root_for_hello("fixture-binding", 1, "fixture-scope")
                .unwrap(),
            None
        );

        let mut host_deliveries = Vec::new();
        let mut native_effect_calls = 0;
        flush_outbox_with_sender(
            &journal,
            |method, payload| {
                if method.starts_with("native.") {
                    native_effect_calls += 1;
                }
                if payload["operation_id"] == recovered_id {
                    let history = journal.load(recovered_id).unwrap();
                    assert!(history.torn_tail_evidence.is_none());
                    assert!(history.acknowledged_sha256.is_none());
                    assert!(!history.native_replay_permitted());
                    assert_eq!(std::fs::read(&recovered_path).unwrap(), recovered_prefix);
                }
                let exact_saved_outcome = method == "module.outcome"
                    && payload["operation_id"]
                        .as_str()
                        .and_then(|operation_id| expected_outcomes.get(operation_id))
                        == Some(&payload);
                let host_response = json!({"recorded":exact_saved_outcome});
                host_deliveries.push((
                    method.to_owned(),
                    payload.clone(),
                    host_response["recorded"] == true,
                ));
                let recorded = host_response["recorded"] == true;
                async move {
                    if recorded {
                        Ok(())
                    } else {
                        Err(Error::new(
                            "HOST_ACK_SCHEMA",
                            "fixture host did not record the exact saved outcome",
                        ))
                    }
                }
            },
            |_| Ok(()),
            || {},
        )
        .await
        .unwrap();

        assert_eq!(host_deliveries.len(), 34);
        assert!(
            host_deliveries
                .iter()
                .all(|(method, _, recorded)| { method == "module.outcome" && *recorded })
        );
        assert_eq!(native_effect_calls, 0);
        assert_eq!(
            host_deliveries
                .iter()
                .filter(|(_, payload, _)| payload["operation_id"] == recovered_id)
                .count(),
            1
        );
        assert!(
            !host_deliveries
                .iter()
                .any(|(_, payload, _)| payload["operation_id"] == "corrupt-operation")
        );

        let recovered_history = journal.load(recovered_id).unwrap();
        let recovered_digest = digest_json(&recovered).unwrap();
        assert!(recovered_history.torn_tail_evidence.is_none());
        assert_eq!(
            recovered_history.acknowledged_sha256.as_deref(),
            Some(recovered_digest.as_str())
        );
        let repaired_bytes = std::fs::read(&recovered_path).unwrap();
        let ack_suffix = repaired_bytes
            .strip_prefix(recovered_prefix.as_slice())
            .unwrap();
        assert_eq!(ack_suffix.last(), Some(&b'\n'));
        let ack_frame: Value = serde_json::from_slice(&ack_suffix[..ack_suffix.len() - 1]).unwrap();
        assert_eq!(ack_frame["kind"], "acknowledged");
        assert_eq!(ack_frame["operation_id"], recovered_id);
        assert_eq!(ack_frame["outcome_sha256"], recovered_digest);

        for operation_id in expected_outcomes.keys() {
            if operation_id.as_str() == "corrupt-operation" {
                continue;
            }
            let history = journal.load(operation_id).unwrap();
            assert_eq!(
                history.acknowledged_sha256.as_deref(),
                Some(
                    digest_json(&expected_outcomes[operation_id])
                        .unwrap()
                        .as_str()
                )
            );
        }
        assert_eq!(std::fs::read(&corrupt_path).unwrap(), corrupt_bytes);
        assert_eq!(
            journal.load("corrupt-operation").unwrap_err().code,
            "ADAPTER_JOURNAL"
        );
        let pending = journal.pending_items().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.key, "corrupt-operation");

        std::fs::remove_dir_all(root).unwrap();
    }

    fn fixture_target_intent(operation_id: &str) -> OperationIntent {
        let mut intent = fixture_effect_intent(operation_id);
        intent.native_root_id = Some("fixture-root".to_owned());
        intent
    }

    #[test]
    fn verified_reply_readback_preserves_exact_identity_without_input_replay() {
        let mut target = fixture_target_intent("reply-target");
        target.method = "agent.reply".to_owned();
        target.native_root_id = Some("ses_fixture".to_owned());
        target.native_input_id = None;
        target.prompt_sha256 = None;
        target.prompt_bytes = None;
        let reply_identity = json!({
            "kind":"question",
            "action":"reply",
            "session_id":"ses_fixture",
            "request_id":"que_fixture",
            "request_fingerprint":"a".repeat(64),
            "affected_request_ids":["que_fixture"],
            "effect_payload_sha256":format!("sha256:{}", "b".repeat(64)),
            "feedback_present":false
        });
        target.marker = json!({"native_opencode_reply":reply_identity.clone()});

        let applied = applied_from_intent(&target, None).unwrap();
        let linked = link_reconcile_target(applied, "reply-reconcile").unwrap();

        assert_eq!(linked.operation_id, "reply-target");
        assert_eq!(linked.native_root_id.as_deref(), Some("ses_fixture"));
        assert_eq!(linked.native_input_id, None);
        assert_eq!(
            linked.details["completion_condition"],
            "native_reply_acknowledged"
        );
        assert_eq!(
            linked.details["evidence"],
            "complete_contiguous_native_reply_history"
        );
        assert_eq!(linked.details["reply_readback_identity"], reply_identity);
        assert_eq!(linked.details["reconcile_operation_id"], "reply-reconcile");
        assert_eq!(linked.details["native_replay"], false);
        assert_eq!(
            linked.details["module_receipt"]["input_sha256"],
            target.module_receipt.input_sha256
        );
    }
    fn fixture_reconcile_intent(operation_id: &str, target_id: &str) -> OperationIntent {
        let mut intent = fixture_effect_intent(operation_id);
        intent.method = "agent.reconcile".to_owned();
        intent.native_root_id = Some("fixture-root".to_owned());
        intent.native_input_id = None;
        intent.prompt_sha256 = None;
        intent.prompt_bytes = None;
        intent.reconcile_target_operation_id = Some(target_id.to_owned());
        intent
    }

    fn fixture_linked_applied(
        target: &OperationIntent,
        reconcile_operation_id: &str,
    ) -> RuntimeOutcome {
        let applied = applied_from_intent(
            target,
            Some(InputEvidence {
                admitted_sequence: Some(4),
                prompted_sequence: None,
            }),
        )
        .unwrap();
        link_reconcile_target(applied, reconcile_operation_id).unwrap()
    }

    fn fixture_reconcile_outcome(
        intent: &OperationIntent,
        target_operation_id: &str,
    ) -> RuntimeOutcome {
        let mut details = json!({
            "completion_condition":"native_readback_recorded",
            "target_operation_id":target_operation_id,
            "resolved":true,
            "native_replay":false
        });
        module_receipt::insert_into_details(&mut details, &intent.module_receipt).unwrap();
        RuntimeOutcome {
            operation_id: intent.operation_id.clone(),
            outcome: EffectOutcome::Applied,
            native_scope_key: Some(intent.native_scope_key.clone()),
            native_root_id: intent.native_root_id.clone(),
            turn_id: None,
            native_input_id: None,
            details,
        }
    }

    /// REG-09: a pending Unknown can be replaced only by the typed, linked
    /// readback successor, and restart drains the exact latest outcome once.
    #[tokio::test]
    async fn unacknowledged_unknown_outcome_is_superseded_by_verified_applied() {
        let root = std::env::temp_dir().join(format!("reg09-supersede-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        let superseded_id = "superseded-unknown-operation";
        let target_intent = fixture_target_intent(superseded_id);
        let reconcile_id = "superseded-unknown-reconcile";
        let reconcile_intent = fixture_reconcile_intent(reconcile_id, superseded_id);
        journal.write_intent(&target_intent).unwrap();
        journal.write_intent(&reconcile_intent).unwrap();
        let unknown =
            unknown_from_intent(&target_intent, "NATIVE_EFFECT_MAY_HAVE_OCCURRED").unwrap();
        journal.queue_outcome(&unknown).unwrap();
        let slot = root
            .join("outbox")
            .join(format!("outcome-{}.json", operation_key(superseded_id)));
        let unknown_bytes = std::fs::read(&slot).unwrap();
        assert_eq!(pending_outcome_of(&unknown_bytes), json!("unknown"));

        let applied = fixture_linked_applied(&target_intent, reconcile_id);
        let reconcile_outcome = fixture_reconcile_outcome(&reconcile_intent, superseded_id);
        journal.queue_outcome(&applied).unwrap();
        journal.queue_outcome(&reconcile_outcome).unwrap();
        let applied_bytes = std::fs::read(&slot).unwrap();
        assert_eq!(pending_outcome_of(&applied_bytes), json!("applied"));
        let history = journal.load(superseded_id).unwrap();
        assert_eq!(history.outcome_revisions.len(), 2);
        assert!(!history.outcome_revisions[0].acknowledged);
        assert!(!history.outcome_revisions[1].acknowledged);

        drop(journal);
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        journal.recover_outbox().unwrap();
        let pending = journal.pending_items().unwrap();
        assert_eq!(pending.len(), 2);
        assert!(
            pending
                .iter()
                .all(|(_, item)| item.payload["outcome"] == "applied")
        );

        let mut deliveries = Vec::new();
        flush_outbox_with_sender(
            &journal,
            |method, payload| {
                assert_eq!(method, "module.outcome");
                assert!(!method.starts_with("native."));
                assert_eq!(payload["outcome"], json!("applied"));
                deliveries.push(payload);
                async move { Ok(()) }
            },
            |_| Ok(()),
            || {},
        )
        .await
        .unwrap();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(
            deliveries
                .iter()
                .map(|payload| payload["operation_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![superseded_id, reconcile_id]
        );
        let applied_value = serde_json::to_value(&applied).unwrap();
        let reconcile_value = serde_json::to_value(&reconcile_outcome).unwrap();
        assert!(deliveries.contains(&applied_value));
        assert!(deliveries.contains(&reconcile_value));

        let history = journal.load(superseded_id).unwrap();
        let applied_value = serde_json::to_value(&applied).unwrap();
        let applied_digest = digest_json(&applied_value).unwrap();
        assert!(history.torn_tail_evidence.is_none());
        assert_eq!(
            history.outcome_sha256.as_deref(),
            Some(applied_digest.as_str())
        );
        assert_eq!(
            history.acknowledged_sha256.as_deref(),
            Some(applied_digest.as_str())
        );
        assert_eq!(history.outcome_revisions.len(), 2);
        assert_eq!(history.outcome_revisions[0].outcome["outcome"], "unknown");
        assert!(!history.outcome_revisions[0].acknowledged);
        assert!(history.outcome_revisions[1].acknowledged);
        journal.queue_outcome(&applied).unwrap();
        assert_eq!(std::fs::read(&slot).unwrap(), applied_bytes);

        std::fs::remove_dir_all(root).unwrap();
    }

    /// An acknowledged Unknown remains history, while restart repairs a
    /// missing publication from the exact durable linked Applied revision.
    #[tokio::test]
    async fn acknowledged_unknown_is_retained_and_latest_revision_recovers_after_restart() {
        let root =
            std::env::temp_dir().join(format!("reg09-acknowledged-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        let operation_id = "acknowledged-unknown-operation";
        let reconcile_id = "acknowledged-unknown-reconcile";
        let target_intent = fixture_target_intent(operation_id);
        let reconcile_intent = fixture_reconcile_intent(reconcile_id, operation_id);
        journal.write_intent(&target_intent).unwrap();
        journal.write_intent(&reconcile_intent).unwrap();
        let unknown =
            unknown_from_intent(&target_intent, "NATIVE_EFFECT_MAY_HAVE_OCCURRED").unwrap();
        journal.queue_outcome(&unknown).unwrap();
        let slot = root
            .join("outbox")
            .join(format!("outcome-{}.json", operation_key(operation_id)));
        let unknown_bytes = std::fs::read(&slot).unwrap();
        assert_eq!(pending_outcome_of(&unknown_bytes), json!("unknown"));
        let mut first_deliveries = Vec::new();
        flush_outbox_with_sender(
            &journal,
            |method, payload| {
                assert_eq!(method, "module.outcome");
                first_deliveries.push(payload);
                async move { Ok(()) }
            },
            |_| Ok(()),
            || {},
        )
        .await
        .unwrap();
        assert_eq!(first_deliveries.len(), 1);
        assert_eq!(first_deliveries[0], serde_json::to_value(&unknown).unwrap());
        assert!(!slot.exists());
        let history = journal.load(operation_id).unwrap();
        assert!(history.outcome_revisions[0].acknowledged);

        let applied = fixture_linked_applied(&target_intent, reconcile_id);
        let reconcile_outcome = fixture_reconcile_outcome(&reconcile_intent, operation_id);
        journal.queue_outcome(&applied).unwrap();
        journal.queue_outcome(&reconcile_outcome).unwrap();
        let applied_value = serde_json::to_value(&applied).unwrap();
        let reconcile_value = serde_json::to_value(&reconcile_outcome).unwrap();
        let applied_bytes = std::fs::read(&slot).unwrap();
        let history = journal.load(operation_id).unwrap();
        assert_eq!(history.acknowledged_sha256, None);
        assert!(history.outcome_revisions[0].acknowledged);
        assert!(!history.outcome_revisions[1].acknowledged);

        // Model a crash after the append-only outcome record but before its
        // deterministic outbox publication.
        std::fs::remove_file(&slot).unwrap();
        drop(journal);

        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        journal.recover_outbox().unwrap();
        assert_eq!(std::fs::read(&slot).unwrap(), applied_bytes);
        let mut second_deliveries = Vec::new();
        let mut native_calls = 0;
        flush_outbox_with_sender(
            &journal,
            |method, payload| {
                if method.starts_with("native.") {
                    native_calls += 1;
                }
                assert_eq!(method, "module.outcome");
                second_deliveries.push(payload);
                async move { Ok(()) }
            },
            |_| Ok(()),
            || {},
        )
        .await
        .unwrap();
        assert_eq!(second_deliveries.len(), 2);
        assert_eq!(
            second_deliveries
                .iter()
                .map(|payload| payload["operation_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![operation_id, reconcile_id]
        );
        assert!(second_deliveries.contains(&applied_value));
        assert!(second_deliveries.contains(&reconcile_value));
        assert_eq!(native_calls, 0);
        let history = journal.load(operation_id).unwrap();
        assert_eq!(history.outcome_revisions.len(), 2);
        assert!(history.outcome_revisions[0].acknowledged);
        assert!(history.outcome_revisions[1].acknowledged);
        assert_eq!(
            history.outcome_revisions[0].outcome,
            serde_json::to_value(&unknown).unwrap()
        );
        assert_eq!(history.outcome_revisions[1].outcome, applied_value);
        let applied_digest = digest_json(&serde_json::to_value(&applied).unwrap()).unwrap();
        assert_eq!(
            history.acknowledged_sha256.as_deref(),
            Some(applied_digest.as_str())
        );
        let reconcile_history = journal.load(reconcile_id).unwrap();
        assert_eq!(reconcile_history.outcome_revisions.len(), 1);
        assert!(reconcile_history.outcome_revisions[0].acknowledged);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn invalid_reconcile_link_and_post_applied_payload_are_rejected_before_append() {
        let root = std::env::temp_dir().join(format!("reg09-guards-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(
            &root,
            "fixture-binding",
            1,
            "fixture-scope",
            &"a".repeat(64),
        )
        .unwrap();
        let operation_id = "guarded-reconcile-target";
        let reconcile_id = "guarded-reconcile-operation";
        let target_intent = fixture_target_intent(operation_id);
        journal.write_intent(&target_intent).unwrap();
        let wrong_route_id = "wrong-route-reconcile-operation";
        let mut wrong_route_intent = fixture_reconcile_intent(wrong_route_id, operation_id);
        wrong_route_intent.route_sha256 = format!("sha256:{}", "b".repeat(64));
        journal.write_intent(&wrong_route_intent).unwrap();
        journal
            .write_intent(&fixture_reconcile_intent(reconcile_id, operation_id))
            .unwrap();
        let unknown =
            unknown_from_intent(&target_intent, "NATIVE_EFFECT_MAY_HAVE_OCCURRED").unwrap();
        journal.queue_outcome(&unknown).unwrap();
        let operation_path = root
            .join("operations")
            .join(format!("{}.jsonl", operation_key(operation_id)));
        let slot = root
            .join("outbox")
            .join(format!("outcome-{}.json", operation_key(operation_id)));
        let operation_bytes = std::fs::read(&operation_path).unwrap();
        let slot_bytes = std::fs::read(&slot).unwrap();

        let wrong_route = fixture_linked_applied(&target_intent, wrong_route_id);
        assert_eq!(
            journal.queue_outcome(&wrong_route).unwrap_err().code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        let mut wrong_receipt = fixture_linked_applied(&target_intent, reconcile_id);
        wrong_receipt.details["module_receipt"]["binding_id"] = json!("foreign-binding");
        assert_eq!(
            journal.queue_outcome(&wrong_receipt).unwrap_err().code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        let mut wrong_link = fixture_linked_applied(&target_intent, reconcile_id);
        wrong_link.details["reconcile_operation_id"] = json!("foreign-reconcile-operation");
        assert_eq!(
            journal.queue_outcome(&wrong_link).unwrap_err().code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        assert_eq!(std::fs::read(&operation_path).unwrap(), operation_bytes);
        assert_eq!(std::fs::read(&slot).unwrap(), slot_bytes);

        let applied = fixture_linked_applied(&target_intent, reconcile_id);
        journal.queue_outcome(&applied).unwrap();
        let applied_bytes = std::fs::read(&operation_path).unwrap();
        let mut conflicting = applied;
        conflicting.details["diagnostic_code"] = json!("different-payload");
        assert_eq!(
            journal.queue_outcome(&conflicting).unwrap_err().code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        assert_eq!(std::fs::read(&operation_path).unwrap(), applied_bytes);

        std::fs::remove_dir_all(root).unwrap();
    }

    fn pending_outcome_of(bytes: &[u8]) -> Value {
        serde_json::from_slice::<PendingItem>(bytes)
            .unwrap()
            .payload["outcome"]
            .clone()
    }
}
