mod config;
mod journal;
mod module_link;
mod module_receipt;
mod module_runtime;
mod native;

pub use config::{ARTIFACT_ID, ARTIFACT_VERSION, AdapterConfig, ModelRef, NativeOptions, RUNTIME};
pub use module_runtime::OwnedBootstrap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use journal::{Journal, OperationIntent, ResultInputStatusIntent, digest_json};
use native::{InputEvidence, NativeClient, input_id, intent_for, root_id};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Mutex, time::Duration};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use tokio::time::sleep;

const ACK_ATTEMPTS: usize = 4;
const ACK_BACKOFF_MS: [u64; ACK_ATTEMPTS] = [100, 250, 500, 1000];
const IDLE_POLL_MS: u64 = 500;

struct HostSession<'a> {
    config: &'a AdapterConfig,
    credential: &'a Credential,
    journal: &'a Journal,
    claim: &'a ModuleContractClaim,
    hello_base: Value,
    root_hint: Mutex<Option<String>>,
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
    let journal = Journal::open(
        &config.state_dir,
        &config.binding_id,
        config.generation,
        &config.native_options.scope_key(),
        &route_sha256,
    )?;
    journal.recover_outbox()?;

    // The verified owner and boot identity come from the per-scope helper.
    // That group owns this adapter process tree only; OpenCode stays external.
    let boot_id = worker.boot_id.clone();
    let scope = config.native_options.scope_key();
    let root_hint = journal.native_root_for_hello(&config.binding_id, config.generation, &scope)?;

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
        hello_base,
        root_hint: Mutex::new(root_hint),
    };
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
        flush_outbox(&host).await?;
    }

    flush_outbox(&host).await?;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        let response = tokio::select! {
            signal = &mut ctrl_c => {
                signal.map_err(|_| Error::new("ADAPTER_SIGNAL", "shutdown signal could not be installed"))?;
                return Ok(());
            }
            result = host.call("module.next", json!({})) => result?,
        };
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
            handle_command(&host, &command).await?;
            flush_outbox(&host).await?;
        } else {
            sleep(Duration::from_millis(IDLE_POLL_MS)).await;
        }
        // A failed module.next may have admitted work before its response was
        // lost. It is intentionally not retried on this boot; startup recovery
        // will make that Operation unknown and offer readback-only reconcile.
    }
}

async fn handle_command(host: &HostSession<'_>, command: &RuntimeCommand) -> Result<()> {
    let config = host.config;
    let journal = host.journal;
    let claim = host.claim;
    if let Some(root) = command.native_root_id.as_deref() {
        host.set_root_hint(root)?;
    }
    let history = journal.load(&command.operation_id)?;
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
        "agent.open" => handle_open(host, command, &options).await,
        "task.dispatch" | "agent.send" => handle_send(host, command, &options).await,
        "agent.reconcile" => handle_reconcile(host, command, &options).await,
        "agent.result" => handle_result(host, command, &options).await,
        _ => {
            let error = Error::new(
                "UNSUPPORTED_CAPABILITY",
                "this Rust artifact implements only OpenCode session open, next-turn input and saved readback",
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

async fn handle_result(
    host: &HostSession<'_>,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<()> {
    let selector = &command.input["selector"];
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
        "task.dispatch" | "agent.send"
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

async fn queue_unknown_result(
    host: &HostSession<'_>,
    intent: &OperationIntent,
    error: &Error,
) -> Result<()> {
    let target = intent.result_input_status.as_ref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTENT_MISMATCH",
            "result status intent is missing its exact target",
        )
    })?;
    let mut unknown = unknown_from_intent(intent, &error.code)?;
    unknown.details["completion_condition"] = json!("input_status_unavailable");
    unknown.details["input_operation_id"] = json!(target.input_operation_id);
    unknown.details["target_module_receipt"] = json!(target.target_module_receipt);
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
) -> Result<()> {
    let journal = host.journal;
    let claim = host.claim;
    let native = match NativeClient::connect(options).await {
        Ok((native, _)) => native,
        Err(error) => {
            return queue_rejected(host, command, options, None, &error).await;
        }
    };
    let location = match native.preflight_open(options).await {
        Ok(location) => location,
        Err(error) => {
            return queue_rejected(host, command, options, None, &error).await;
        }
    };
    let root = root_id(&command.binding_id, command.generation);
    host.set_root_hint(&root)?;
    let intent = intent_for(command, claim, options, None, None)?;
    // This fsync is the barrier before the one native create POST. On any
    // later crash this operation becomes Unknown and can only be read back.
    journal.write_intent(&intent)?;
    let outcome = match native.create_root(command, options, &root, location).await {
        Ok(()) => outcome(
            command,
            claim,
            EffectOutcome::Applied,
            options,
            Some(root),
            None,
            json!({
                "completion_condition":"native_session_created",
                "durable_origin":"exact_session_created_event",
                "native_replay":false,
                "model":options.model
            }),
        )?,
        Err(error) => effect_failure(command, claim, options, Some(root), None, &error, true)?,
    };
    journal.queue_outcome(&outcome)?;
    flush_outbox(host).await
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
    let prompt_text = match native.preflight_send(command, options, root).await {
        Ok(text) => text,
        Err(error) => {
            return queue_rejected(host, command, options, Some(root.into()), &error).await;
        }
    };
    let input = input_id(&command.operation_id);
    let intent = intent_for(
        command,
        claim,
        options,
        Some(input.clone()),
        Some(&prompt_text),
    )?;
    // Only the digest and byte count are retained; prompt text stays in this
    // stack frame and the HTTP request body.
    journal.write_intent(&intent)?;
    let outcome = match native
        .admit_input(command, options, root, &input, &prompt_text)
        .await
    {
        Ok(()) => outcome(
            command,
            claim,
            EffectOutcome::Applied,
            options,
            Some(root.into()),
            Some(input),
            json!({
                "completion_condition":"native_input_admitted",
                "delivery":"queue",
                "evidence":"prompt_response",
                "execution_complete":false,
                "native_replay":false
            }),
        )?,
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
            if let Ok((native, _)) = NativeClient::connect(options).await {
                let evidence = match target_intent.method.as_str() {
                    "agent.open" => native
                        .reconcile_open(target_intent, options)
                        .await
                        .map(|()| None),
                    "task.dispatch" | "agent.send" => native
                        .reconcile_input(target_intent, options)
                        .await
                        .map(Some),
                    _ => Err(Error::new(
                        "NATIVE_EVIDENCE_UNAVAILABLE",
                        "saved method is outside the readback subset",
                    )),
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
                        "NATIVE_READ_FAILED",
                        "native service was unavailable for readback",
                    );
                    let target_outcome = link_reconcile_target(
                        unknown_from_intent(target_intent, &error.code)?,
                        &command.operation_id,
                    )?;
                    journal.queue_outcome(&target_outcome)?;
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
    let (completion_condition, evidence, input_id) = match input_evidence {
        None => ("native_session_created", "exact_session_readback", None),
        Some(InputEvidence::InboxReadback) => (
            "native_input_admitted",
            "inbox_readback",
            intent.native_input_id.clone(),
        ),
        Some(InputEvidence::ProjectedMessageReadback) => (
            "native_input_admitted",
            "projected_message_readback",
            intent.native_input_id.clone(),
        ),
    };
    let is_input = input_id.is_some();
    let mut details = if is_input {
        json!({
            "completion_condition":completion_condition,
            "evidence":evidence,
            "selected_model":intent.model.clone(),
            "delivery":"queue",
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
    json!({"diagnostic_code":error.code})
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
        for attempt in 0..ACK_ATTEMPTS {
            match self.open_link().await {
                Ok(_) => return Ok(()),
                Err(error) => last_error = error,
            }
            if attempt + 1 < ACK_ATTEMPTS {
                sleep(Duration::from_millis(ACK_BACKOFF_MS[attempt])).await;
            }
        }
        Err(Error::new(
            "HOST_ACK_PENDING",
            format!("module hello remains unacknowledged: {}", last_error.code),
        ))
    }

    /// Each authenticated connection receives a fresh principal link ID.
    /// Rebind it with typed module.hello and perform the application RPC on
    /// that same connection so Store's link-scoped admission remains valid.
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let mut link = self.open_link().await?;
        module_link::call(&mut link, method, params).await
    }

    async fn call_recorded_retry(&self, method: &str, params: Value) -> Result<()> {
        let mut last_error = Error::new("HOST_ACK_SCHEMA", "host acknowledgement was not recorded");
        for attempt in 0..ACK_ATTEMPTS {
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
                sleep(Duration::from_millis(ACK_BACKOFF_MS[attempt])).await;
            }
        }
        Err(Error::new(
            "HOST_ACK_PENDING",
            format!("host acknowledgement remains pending: {}", last_error.code),
        ))
    }
}

async fn flush_outbox(host: &HostSession<'_>) -> Result<()> {
    let journal = host.journal;
    loop {
        let items = journal.pending_items()?;
        if items.is_empty() {
            return Ok(());
        }
        for (path, item) in items {
            match item.kind.as_str() {
                "outcome" => {
                    let operation_id = item.payload["operation_id"].as_str().ok_or_else(|| {
                        Error::new("ADAPTER_OUTBOX", "saved outcome has no operation ID")
                    })?;
                    let digest = digest_json(&item.payload)?;
                    let history = journal.load(operation_id)?;
                    if history.acknowledged_sha256.as_deref() != Some(digest.as_str()) {
                        host.call_recorded_retry("module.outcome", item.payload.clone())
                            .await?;
                        journal.acknowledge_outcome(&item.payload)?;
                    }
                    remember_root_from_outcome(host.config, journal, &item.payload)?;
                    if let Some(root) = item.payload["native_root_id"].as_str() {
                        host.set_root_hint(root)?;
                    }
                    journal.remove_pending(&path)?;
                }
                "observation" => {
                    host.call_recorded_retry("module.observe", item.payload.clone())
                        .await?;
                    journal.remove_pending(&path)?;
                }
                "result" => {
                    let operation_id = item.payload["operation_id"].as_str().ok_or_else(|| {
                        Error::new("ADAPTER_OUTBOX", "saved result page has no operation ID")
                    })?;
                    let digest = digest_json(&item.payload)?;
                    let history = journal.load(operation_id)?;
                    if history.result_acknowledged_sha256.as_deref() != Some(digest.as_str()) {
                        host.call_recorded_retry("module.result", item.payload.clone())
                            .await?;
                        journal.acknowledge_result(&item.payload)?;
                    }
                    if let Some(root) = item.payload["page"]["source"]["native_session_id"].as_str()
                    {
                        host.set_root_hint(root)?;
                    }
                    journal.remove_pending(&path)?;
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
    if let Some(intent) = history.intent {
        if intent.binding_id != config.binding_id
            || intent.generation != config.generation
            || intent.native_scope_key != scope
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "outcome intent belongs to another binding",
            ));
        }
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
