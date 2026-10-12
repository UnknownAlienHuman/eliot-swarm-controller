use crate::Profile;
use crate::acp_prompt::{self, PreparedAcpDispatch};
use crate::module_host::{self, ModuleHostIdentity};
use crate::{ARTIFACT_ID, EXECUTION_SHAPE, RUNTIME};
use crate::{
    journal::{DispatchIdentity, NativeCaptureEvidence, NativeStreamFacts, RunStore, digest},
    native, result_page,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use swarm_client::{HostConnectionConfig, ModuleLink};
use swarm_contracts::{
    Credential, EffectOutcome, RuntimeCommand, RuntimeOutcome,
    error::{Error, Result},
    module_command::CommandCaptureReleaseEvidence,
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};
use tokio::time;

const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_CREDENTIAL_BYTES: u64 = 16_384;
const MAX_FIXED_ARGS: usize = 32;
const MAX_FIXED_ARG_BYTES: usize = 4096;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    module_artifact_id: String,
    command: PathBuf,
    #[serde(default)]
    command_args: Vec<String>,
    #[serde(default)]
    mod_path: Option<PathBuf>,
    #[serde(default)]
    run_timeout_ms: Option<u64>,
    #[serde(skip)]
    host_connection: Option<HostConnectionConfig>,
}

#[derive(Clone)]
struct Owner {
    state_dir: PathBuf,
    token: String,
    record: Value,
    host: ModuleHostIdentity,
}

struct Link {
    client: ModuleLink,
    binding_id: String,
    generation: i64,
    route: Value,
    recovery_required: bool,
}

pub(crate) fn pending_startup_outcomes(store: &RunStore) -> Result<Vec<(RuntimeOutcome, String)>> {
    // Resume ACK-authorized cleanup namespaces before the broad outcome scan;
    // cleanup directories and intentless tombstones are not op-* records.
    store.finalize_acknowledged_releases()?;
    store.pending_outcomes()
}

pub async fn run() -> Result<()> {
    let (host_config_path, config_path) = config_arguments()?;
    let owner = load_owner()?;
    let config = load_config(&config_path, owner.host.profile)?;
    let host_connection = load_host_connection_config(&host_config_path, &owner.state_dir)?;
    let mut config = config;
    config.host_connection = Some(host_connection);
    let credential = load_credential(&owner.host.credential_file)?;
    if credential.client_id != owner.host.module_client_id {
        return Err(Error::new(
            "CREDENTIAL_INVALID",
            "module credential client differs from the supervisor binding identity",
        ));
    }
    if owner.host.profile == Profile::AcpV1 {
        return acp::run(config, owner, credential).await;
    }
    let store = RunStore::new_for_profile(&owner.state_dir, owner.host.profile)?;
    let mut next_observation_sequence = 1_u64;
    let mut pending_observation: Option<(u64, Value)> = None;
    let mut expected_route: Option<Value> = None;
    let mut last_operation = Value::Null;
    let mut stop_after_observation = false;

    loop {
        let mut link = match connect_module(&config, &credential, &owner).await {
            Ok(link) => link,
            Err(error) if is_connect_retryable(&error) => {
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        if expected_route
            .as_ref()
            .is_some_and(|route| route != &link.route)
        {
            return Err(Error::new(
                "ROUTE_CHANGED",
                "module route changed while this owner was live",
            ));
        }
        expected_route = Some(link.route.clone());

        // A pending observation is the same immutable event ID and state after
        // reconnect. A pending outcome is also resent only as its exact saved
        // envelope; native input is never inferred from a transport retry.
        if let Some((sequence, state)) = pending_observation.clone() {
            match send_observation(&mut link.client, &owner, sequence, state).await {
                Ok(()) => {
                    pending_observation = None;
                    next_observation_sequence = next_sequence(sequence)?;
                    if stop_after_observation {
                        return Ok(());
                    }
                }
                Err(error) if is_transport_uncertain(&error) => {
                    drop(link);
                    time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(error) => return Err(error),
            }
        } else {
            let state = module_state(&link, &owner, &last_operation, 0);
            let sequence = next_observation_sequence;
            pending_observation = Some((sequence, state.clone()));
            match send_observation(&mut link.client, &owner, sequence, state).await {
                Ok(()) => {
                    pending_observation = None;
                    next_observation_sequence = next_sequence(sequence)?;
                }
                Err(error) if is_transport_uncertain(&error) => {
                    drop(link);
                    time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        // A saved terminal receipt may have reached Store while its RPC ack
        // was lost. The only retry permitted here is the same bounded outcome.
        for (outcome, hash) in pending_startup_outcomes(&store)? {
            module_host::validate_saved_receipt(
                &outcome,
                &owner.host.claim,
                &link.binding_id,
                link.generation,
            )?;
            let (_, admitted) = store.admit(
                &outcome.operation_id,
                None,
                &link.binding_id,
                link.generation,
                &link.route,
            )?;
            if !admitted {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "pending outcome has no matching immutable admission",
                ));
            }
            let expected_binding_id = link.binding_id.clone();
            let expected_generation = link.generation;
            let expected_route = link.route.clone();
            link.client = deliver_outcome(
                link.client,
                &config,
                &credential,
                &owner,
                &store,
                &expected_binding_id,
                expected_generation,
                &expected_route,
                outcome,
                hash,
            )
            .await?;
        }
        store.finalize_acknowledged_releases()?;
        for (params, hash) in store.pending_result_pages()? {
            result_page::validate_saved(
                &params,
                &owner.host.claim,
                &link.binding_id,
                link.generation,
            )?;
            let operation_id = params["operation_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    Error::new("ADAPTER_EVIDENCE_INVALID", "result Operation is missing")
                })?
                .to_owned();
            let (_, admitted) = store.admit(
                &operation_id,
                None,
                &link.binding_id,
                link.generation,
                &link.route,
            )?;
            if !admitted {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "pending result page has no matching immutable admission",
                ));
            }
            deliver_result_page(
                &mut link,
                &config,
                &credential,
                &owner,
                &store,
                params,
                hash,
            )
            .await?;
        }

        loop {
            let next = match link.client.next().await {
                Ok(value) => value,
                Err(error) if is_transport_uncertain(&error) => break,
                Err(error) => return Err(error),
            };
            if next.get("rejected_operation_id").is_some() {
                last_operation = json!({
                    "operation_id":next["rejected_operation_id"],
                    "state":"rejected_before_module_effect"
                });
                pending_observation = Some((
                    next_observation_sequence,
                    module_state(&link, &owner, &last_operation, 0),
                ));
                break;
            }
            if next["command"].is_null() {
                continue;
            }
            let command: RuntimeCommand =
                serde_json::from_value(next["command"].clone()).map_err(|_| {
                    Error::new(
                        "MODULE_COMMAND_INVALID",
                        "module command envelope is invalid",
                    )
                })?;
            validate_command(&command, &link)?;
            let (receipts, stop_bridge) =
                process_command(&config, &owner, &store, &mut link, &credential, &command).await?;
            stop_after_observation |= stop_bridge;
            for mut outcome in receipts {
                module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim)?;
                let hash = store.save_outcome(&outcome)?;
                let (saved, saved_hash) =
                    store.read_outcome(&outcome.operation_id)?.ok_or_else(|| {
                        Error::new("ADAPTER_EVIDENCE_INVALID", "durable outcome disappeared")
                    })?;
                if saved_hash != hash {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "outcome digest changed after save",
                    ));
                }
                let expected_binding_id = link.binding_id.clone();
                let expected_generation = link.generation;
                let expected_route = link.route.clone();
                link.client = deliver_outcome(
                    link.client,
                    &config,
                    &credential,
                    &owner,
                    &store,
                    &expected_binding_id,
                    expected_generation,
                    &expected_route,
                    saved,
                    hash,
                )
                .await?;
            }
            last_operation = json!({
                "operation_id":command.operation_id,
                "method":command.method,
                "completion_condition":receipts_completion(&store, &command.operation_id).unwrap_or(Value::Null)
            });
            let sequence = next_observation_sequence;
            let state = module_state(&link, &owner, &last_operation, 0);
            match send_observation(&mut link.client, &owner, sequence, state.clone()).await {
                Ok(()) => next_observation_sequence = next_sequence(sequence)?,
                Err(error) if is_transport_uncertain(&error) => {
                    pending_observation = Some((sequence, state));
                    break;
                }
                Err(error) => return Err(error),
            }
            if stop_after_observation {
                return Ok(());
            }
        }
        drop(link);
        time::sleep(Duration::from_millis(250)).await;
    }
}

mod acp;

fn normalized_dispatch_admission(
    owner: &Owner,
    command: &RuntimeCommand,
    identity: &DispatchIdentity,
    prompt: &str,
) -> Result<Option<TaskDispatchAdmissionReceipt>> {
    if command.method != "task.dispatch"
        || !module_host::normalized_dispatch_enabled(&owner.host.claim)
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
        || context.worker_boot_id != owner.host.boot_id
        || identity.operation_id != command.operation_id
        || context.task_snapshot_sha256 != identity.task_snapshot_sha256
    {
        return Err(Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch context does not match the durable Command identity",
        ));
    }
    let source_text = command.input["text"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::new("TASK_DISPATCH_CONTEXT_INVALID", "source text is missing"))?;
    if context.source_text_sha256 != digest(source_text.as_bytes())
        || context.source_text_bytes != source_text.len() as u64
    {
        return Err(Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch context source identity differs from the command",
        ));
    }
    if prompt.len() != identity.prompt_bytes || digest(prompt.as_bytes()) != identity.prompt_sha256
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "native prompt identity differs from its durable admission identity",
        ));
    }
    let module_receipt = module_host::receipt_identity_for_input(
        &owner.host.claim,
        command,
        &command.operation_id,
        &identity.input_sha256,
    )?;
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt,
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
        // Command passes the prompt as the final argv argument. This is the
        // exact native payload available before the subprocess starts; the
        // CLI exposes no native input ID or echo channel.
        native_payload_sha256: identity.prompt_sha256.clone(),
        native_payload_bytes: identity.prompt_bytes as u64,
        native_input_id: None,
    };
    receipt.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "dispatch admission is invalid",
        )
    })?;
    Ok(Some(receipt))
}

fn acp_dispatch_admission(
    owner: &Owner,
    command: &RuntimeCommand,
    dispatch: &PreparedAcpDispatch,
) -> Result<TaskDispatchAdmissionReceipt> {
    if !matches!(owner.host.profile, Profile::AcpV1 | Profile::BatchV4)
        || !module_host::task_prompt_v1_enabled(&owner.host.claim)
        || dispatch.identity.operation_id != command.operation_id
        || dispatch.identity.input_sha256 != command.input_sha256.as_deref().unwrap_or_default()
        || dispatch.identity.prompt_sha256 != dispatch.envelope.prompt_sha256
        || dispatch.identity.prompt_bytes != dispatch.envelope.prompt_bytes
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "ACP dispatch is not bound to the selected TaskPrompt artifact and request",
        ));
    }
    let context = &dispatch.context;
    let module_receipt = module_host::receipt_identity_for_input(
        &owner.host.claim,
        command,
        &command.operation_id,
        &dispatch.identity.input_sha256,
    )?;
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt,
        operation_id: context.operation_id.clone(),
        binding_id: context.binding_id.clone(),
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id.clone(),
        attempt_id: dispatch.identity.attempt_id.clone(),
        task_id: dispatch.identity.task_id.clone(),
        task_revision: dispatch.identity.task_revision,
        task_snapshot_sha256: dispatch.identity.task_snapshot_sha256.clone(),
        source_text_sha256: context.source_text_sha256.clone(),
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: dispatch.identity.prompt_sha256.clone(),
        native_payload_bytes: dispatch.identity.prompt_bytes,
        native_input_id: None,
    };
    receipt.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "ACP dispatch admission receipt is invalid",
        )
    })?;
    Ok(receipt)
}

fn validate_dispatch_outcome(
    outcome: &RuntimeOutcome,
    admission: Option<&TaskDispatchAdmissionReceipt>,
) -> Result<()> {
    let saved = outcome.details["dispatch_admission"].clone();
    match outcome.outcome {
        EffectOutcome::Applied | EffectOutcome::Accepted => {
            if let Some(expected) = admission {
                let actual: TaskDispatchAdmissionReceipt =
                    serde_json::from_value(saved).map_err(|_| {
                        Error::new(
                            "ADAPTER_EVIDENCE_INVALID",
                            "known dispatch outcome lacks its typed admission receipt",
                        )
                    })?;
                actual.validate().map_err(|_| {
                    Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "saved dispatch admission receipt is invalid",
                    )
                })?;
                if &actual != expected
                    || actual.native_input_id.as_deref() != outcome.native_input_id.as_deref()
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_CONFLICT",
                        "known dispatch outcome differs from its durable admission marker",
                    ));
                }
            }
        }
        EffectOutcome::Rejected | EffectOutcome::Unknown => {
            if !saved.is_null() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "rejected or unknown dispatch outcome cannot carry admission evidence",
                ));
            }
        }
    }
    Ok(())
}

fn retry_admitted_dispatch_outcome(
    store: &RunStore,
    identity: &DispatchIdentity,
    binding_id: &str,
    generation: i64,
    route: &Value,
    dispatch_admission: Option<&TaskDispatchAdmissionReceipt>,
) -> Result<RuntimeOutcome> {
    if let Some((saved, _)) = store.read_outcome(&identity.operation_id)? {
        if !native::result_identity_matches(&serde_json::to_value(&saved)?, identity) {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "saved terminal receipt differs from the core-frozen Operation",
            ));
        }
        validate_dispatch_outcome(&saved, dispatch_admission)?;
        return Ok(saved);
    }

    let mut unknown = native::core_bound_unknown(
        &identity.operation_id,
        identity,
        "native_result_missing_after_admission",
    );
    unknown.details["native_replay"] = json!(false);
    match store.read_native_evidence(identity, binding_id, generation, route, dispatch_admission) {
        Ok(Some(evidence)) => {
            apply_retained_capture_facts(&mut unknown, &evidence);
            unknown.details["diagnostic_code"] =
                json!("native_outcome_missing_after_capture_persisted");
            unknown.details["native_capture_recovery_status"] = json!("validated");
            unknown.details["local_evidence_available"] = json!(true);
        }
        Ok(None) => mark_capture_recovery_unavailable(
            &mut unknown,
            "not_retained",
            "native_result_missing_after_admission",
        ),
        Err(_) => mark_capture_recovery_unavailable(
            &mut unknown,
            "proof_unavailable",
            "native_capture_proof_unavailable",
        ),
    }
    validate_dispatch_outcome(&unknown, dispatch_admission)?;
    Ok(unknown)
}

fn apply_retained_capture_facts(outcome: &mut RuntimeOutcome, evidence: &NativeCaptureEvidence) {
    set_native_stream_facts(&mut outcome.details, "stdout", &evidence.stdout_facts);
    set_native_stream_facts(&mut outcome.details, "stderr", &evidence.stderr_facts);
    outcome.details["native_child"] = evidence.native_child.clone();
    if let Some(module_receipt) = &evidence.module_receipt {
        outcome.details["module_receipt"] = module_receipt.clone();
    }
}

fn set_native_stream_facts(details: &mut Value, stream: &str, facts: &NativeStreamFacts) {
    let Some(details) = details.as_object_mut() else {
        return;
    };
    details.insert(format!("{stream}_bytes"), json!(facts.bytes));
    details.insert(format!("{stream}_stored_bytes"), json!(facts.stored_bytes));
    details.insert(format!("{stream}_sha256"), json!(facts.sha256));
    details.insert(
        format!("{stream}_stored_sha256"),
        json!(facts.stored_sha256),
    );
    details.insert(format!("{stream}_truncated"), json!(facts.truncated));
    details.insert(format!("{stream}_read_error"), json!(facts.read_error));
}

fn mark_capture_recovery_unavailable(outcome: &mut RuntimeOutcome, status: &str, diagnostic: &str) {
    outcome.details["diagnostic_code"] = json!(diagnostic);
    outcome.details["native_capture_recovery_status"] = json!(status);
    outcome.details["local_evidence_available"] = json!(false);
    outcome.details["native_replay"] = json!(false);
}

async fn process_command(
    config: &Config,
    owner: &Owner,
    store: &RunStore,
    link: &mut Link,
    credential: &Credential,
    command: &RuntimeCommand,
) -> Result<(Vec<RuntimeOutcome>, bool)> {
    match command.method.as_str() {
        "agent.open" => {
            let model_id = route_model(command)?;
            let workspace = native::route_workspace(command)?;
            let (_, existing) = store.admit(
                &command.operation_id,
                None,
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            let outcome = if let Some((saved, _)) = store.read_outcome(&command.operation_id)? {
                if saved.details["requested_model"] != model_id {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_CONFLICT",
                        "saved preflight receipt differs from the current route model",
                    ));
                }
                saved
            } else if existing {
                RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Unknown,
                    native_scope_key: None,
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "execution_shape":EXECUTION_SHAPE,
                        "completion_condition":"executor_preflight_unconfirmed",
                        "requested_model":model_id,
                        "effective_model":Value::Null,
                        "effective_model_status":"unknown",
                        "native_session_state":"not_started",
                        "diagnostic_code":"preflight_admission_without_receipt",
                        "native_replay":false
                    }),
                }
            } else {
                let mod_check = check_mod(batch_mod_path(config)?);
                let executable_ready = config.command.is_file();
                let workspace_ready = workspace.is_dir();
                let ready = mod_check.is_ok() && executable_ready && workspace_ready;
                let code = if mod_check.is_err() {
                    Some("COMMAND_MOD_MISMATCH")
                } else if !executable_ready {
                    Some("NATIVE_EXECUTABLE_UNAVAILABLE")
                } else if !workspace_ready {
                    Some("COMMAND_WORKSPACE_INVALID")
                } else {
                    None
                };
                let outcome = native::open_preflight_outcome(command, model_id, ready, code);
                store.save_native_evidence(
                    &command.operation_id,
                    b"",
                    b"",
                    &json!({
                        "module_artifact_id":ARTIFACT_ID,
                        "preflight_only":true,
                        "native_version_probe_performed":false,
                        "native_model_probe_performed":false,
                        "program_present":executable_ready,
                        "workspace_present":workspace_ready,
                        "mod_sha256":mod_check.ok()
                    }),
                )?;
                outcome
            };
            Ok((vec![outcome], false))
        }
        "task.dispatch" => {
            let (prompt, identity, dispatch_admission) = if owner.host.profile == Profile::BatchV4 {
                let dispatch = acp_prompt::prepare(command)?;
                let identity = native::task_prompt_identity(command, &dispatch)?;
                let admission = acp_dispatch_admission(owner, command, &dispatch)?;
                (dispatch.envelope.prompt.clone(), identity, Some(admission))
            } else {
                let (prompt, identity) = native::prompt_for(command)?;
                let admission = normalized_dispatch_admission(owner, command, &identity, &prompt)?;
                (prompt, identity, admission)
            };
            let workspace = native::route_workspace(command)?;
            let (_dir, existing) = store.admit(
                &command.operation_id,
                Some(&identity),
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            if existing {
                if dispatch_admission.is_some()
                    && store
                        .read_dispatch_admission(&command.operation_id)?
                        .as_ref()
                        != dispatch_admission.as_ref()
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_CONFLICT",
                        "saved dispatch admission differs from the current Store context",
                    ));
                }
                let outcome = retry_admitted_dispatch_outcome(
                    store,
                    &identity,
                    &command.binding_id,
                    command.generation,
                    &command.route,
                    dispatch_admission.as_ref(),
                )?;
                return Ok((vec![outcome], false));
            }
            if let Some(admission) = dispatch_admission.as_ref() {
                store.save_dispatch_admission(&command.operation_id, admission)?;
            }
            let invocation = native::invoke(
                &native::InvocationConfig {
                    program: config.command.clone(),
                    fixed_args: config.command_args.clone(),
                    mod_path: batch_mod_path(config)?.to_path_buf(),
                    run_timeout: batch_run_timeout(config)?,
                    owner_token: owner.token.clone(),
                },
                store,
                command,
                &identity,
                &prompt,
                &workspace,
            )
            .await?;
            let mut outcome = invocation.outcome;
            if matches!(outcome.outcome, EffectOutcome::Applied)
                && let Some(admission) = dispatch_admission.as_ref()
            {
                outcome.details["dispatch_admission"] = serde_json::to_value(admission)?;
            }
            validate_dispatch_outcome(&outcome, dispatch_admission.as_ref())?;
            Ok((vec![outcome], invocation.stop_bridge))
        }
        "agent.refresh" => {
            let (_, existing) = store.admit(
                &command.operation_id,
                None,
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            if let Some((saved, _)) = store.read_outcome(&command.operation_id)? {
                return Ok((vec![saved], false));
            }
            if existing {
                return Ok((
                    vec![readback_unknown(
                        command,
                        "snapshot_admission_without_receipt",
                    )],
                    false,
                ));
            }
            let outcome = RuntimeOutcome {
                operation_id: command.operation_id.clone(),
                outcome: EffectOutcome::Applied,
                native_scope_key: None,
                native_root_id: None,
                turn_id: None,
                native_input_id: None,
                details: json!({
                    "execution_shape":EXECUTION_SHAPE,
                    "completion_condition":"batch_snapshot_readback",
                    "binding_id":command.binding_id,
                    "binding_generation":command.generation,
                    "native_session_state":"not_started",
                    "family_complete":false,
                    "task_acceptance_claimed":false
                }),
            };
            store.save_native_evidence(
                &command.operation_id,
                b"",
                b"",
                &json!({"snapshot_readback":true,"family_departure_claimed":false}),
            )?;
            Ok((vec![outcome], false))
        }
        "agent.reconcile" => reconcile(store, command).await,
        "agent.result" => {
            let (_, _existing) = store.admit(
                &command.operation_id,
                None,
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            let (params, hash) =
                if let Some(saved) = store.read_result_page(&command.operation_id)? {
                    result_page::validate_saved(
                        &saved.0,
                        &owner.host.claim,
                        &command.binding_id,
                        command.generation,
                    )?;
                    saved
                } else {
                    // A result-page Operation has no native effect. When the
                    // first page was not sealed before a crash, regenerate it
                    // from the exact current Store snapshot in this command.
                    let params = if command.input["selector"]["kind"] == "command_output" {
                        if module_host::normalized_result_enabled(&owner.host.claim) {
                            result_page::build_normalized_output(command, &owner.host.claim, store)?
                        } else {
                            result_page::build_output(command, &owner.host.claim, store)?
                        }
                    } else {
                        result_page::build(command, &owner.host.claim)?
                    };
                    let hash = store.save_result_page(&command.operation_id, &params)?;
                    store
                        .read_result_page(&command.operation_id)?
                        .filter(|(_, saved_hash)| saved_hash == &hash)
                        .ok_or_else(|| {
                            Error::new(
                                "ADAPTER_EVIDENCE_INVALID",
                                "saved Command status page disappeared",
                            )
                        })?
                };
            result_page::validate_saved(
                &params,
                &owner.host.claim,
                &link.binding_id,
                link.generation,
            )?;
            deliver_result_page(link, config, credential, owner, store, params, hash).await?;
            Ok((Vec::new(), false))
        }
        "native.command.release_evidence" => {
            let (_, _) = store.admit(
                &command.operation_id,
                None,
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            let evidence_value = command
                .input
                .get(swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD)
                .cloned()
                .ok_or_else(|| {
                    Error::new(
                        "COMMAND_RELEASE_EVIDENCE_INVALID",
                        "Store release Operation omitted its artifact coverage proof",
                    )
                })?;
            let evidence: CommandCaptureReleaseEvidence = serde_json::from_value(evidence_value)
                .map_err(|_| {
                    Error::new(
                        "COMMAND_RELEASE_EVIDENCE_INVALID",
                        "Store release Operation has an invalid artifact coverage proof",
                    )
                })?;
            if command.input["target_operation_id"].as_str()
                != Some(evidence.target_operation_id.as_str())
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_EVIDENCE_INVALID",
                    "Store release Operation target differs from its artifact coverage proof",
                ));
            }
            let release_input_sha256 = command.input_sha256.as_deref().ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_EVIDENCE_INVALID",
                    "Store release Operation omitted its exact input digest",
                )
            })?;
            let outcome = store.release_command_captures(
                &command.operation_id,
                release_input_sha256,
                &command.binding_id,
                command.generation,
                &command.route,
                &evidence,
            )?;
            Ok((vec![outcome], false))
        }
        _ => Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "Command headless module received an unsupported operation",
        )),
    }
}

async fn reconcile(
    store: &RunStore,
    command: &RuntimeCommand,
) -> Result<(Vec<RuntimeOutcome>, bool)> {
    let (_, _reconcile_existing) = store.admit(
        &command.operation_id,
        None,
        &command.binding_id,
        command.generation,
        &command.route,
    )?;
    if let Some((saved, _)) = store.read_outcome(&command.operation_id)? {
        return Ok((vec![saved], false));
    }
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::invalid("reconcile operation_id is missing"))?;
    let target_method = command.input["target_command_method"]
        .as_str()
        .ok_or_else(|| Error::invalid("Store target method is missing"))?;
    if !matches!(target_method, "agent.open" | "task.dispatch") {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "reconcile target is outside the Command readback contract",
        ));
    }
    let (target_outcome, resolved, target_receipt_existed) = if target_method == "task.dispatch" {
        let identity = target_dispatch_identity(command, target_id)?;
        let (_, target_existing) = store.admit(
            target_id,
            Some(&identity),
            &command.binding_id,
            command.generation,
            &command.route,
        )?;
        match store.read_outcome(target_id)? {
            Some((saved, _)) => {
                if !native::result_identity_matches(&serde_json::to_value(&saved)?, &identity) {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_CONFLICT",
                        "saved reconcile receipt differs from Store-injected target identity",
                    ));
                }
                let terminal = matches!(
                    saved.outcome,
                    EffectOutcome::Applied | EffectOutcome::Rejected
                );
                (saved, terminal, true)
            }
            None => {
                let unknown = native::core_bound_unknown(
                    target_id,
                    &identity,
                    if target_existing {
                        "reconcile_target_saved_terminal_evidence_missing"
                    } else {
                        "reconcile_target_has_no_adapter_evidence"
                    },
                );
                (unknown, false, false)
            }
        }
    } else {
        let (_, _target_existing) = store.admit(
            target_id,
            None,
            &command.binding_id,
            command.generation,
            &command.route,
        )?;
        match store.read_outcome(target_id)? {
            Some((saved, _)) => {
                if saved.details["requested_model"] != route_model(command)? {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_CONFLICT",
                        "saved preflight receipt differs from the binding model",
                    ));
                }
                let terminal = matches!(
                    saved.outcome,
                    EffectOutcome::Applied | EffectOutcome::Rejected
                );
                (saved, terminal, true)
            }
            None => (
                RuntimeOutcome {
                    operation_id: target_id.to_owned(),
                    outcome: EffectOutcome::Unknown,
                    native_scope_key: None,
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "execution_shape":EXECUTION_SHAPE,
                        "completion_condition":"executor_preflight_unconfirmed",
                        "requested_model":route_model(command)?,
                        "effective_model":Value::Null,
                        "effective_model_status":"unknown",
                        "native_session_state":"not_started",
                        "diagnostic_code":"reconcile_target_evidence_missing",
                        "native_replay":false
                    }),
                },
                false,
                false,
            ),
        }
    };

    // The Store-injected target identity is authoritative. Resending this exact
    // outcome is idempotent; a native prompt is never repeated during reconcile.
    let mut target_outcome = target_outcome;
    if !target_receipt_existed {
        target_outcome.details["reconcile_operation_id"] = json!(command.operation_id);
    }
    let target_hash = store.save_outcome(&target_outcome)?;
    let (saved_target, verified_hash) = store
        .read_outcome(target_id)?
        .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "target receipt disappeared"))?;
    if target_hash != verified_hash {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "target receipt digest changed",
        ));
    }
    let reconcile_outcome = RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({
            "execution_shape":EXECUTION_SHAPE,
            "completion_condition":"batch_readback_recorded",
            "target_operation_id":target_id,
            "target_record_state":if target_receipt_existed {"saved_receipt_observed"} else {"admission_only"},
            "target_outcome":saved_target,
            "native_replay":false,
            "resolved":resolved
        }),
    };
    let receipts = vec![saved_target, reconcile_outcome];
    Ok((receipts, false))
}

fn target_dispatch_identity(command: &RuntimeCommand, target_id: &str) -> Result<DispatchIdentity> {
    let model_id = route_model(command)?;
    if command.input["target_command_requested_model"] != model_id {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Store-injected requested model differs from the immutable binding route",
        ));
    }
    let core = &command.input["target_command_core_binding"];
    let input_sha256 = command
        .target_input_sha256
        .as_deref()
        .filter(|value| is_sha256(value))
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "target Operation digest is invalid",
            )
        })?;
    let prompt_sha256 = core["prompt_sha256"]
        .as_str()
        .filter(|value| is_sha256(value))
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "target prompt digest is invalid",
            )
        })?;
    let prompt_bytes = core["prompt_bytes"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0 && *value <= 1_048_576)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "target prompt length is invalid",
            )
        })?;
    let expected_run = format!("command-batch:{}", &digest(target_id.as_bytes())[..32]);
    let batch_run_id = core["batch_run_id"]
        .as_str()
        .filter(|value| *value == expected_run)
        .ok_or_else(|| Error::new("NATIVE_IDENTITY_MISMATCH", "target run identity is invalid"))?;
    if core.as_object().is_none_or(|object| {
        object.len() != 3
            || !object.contains_key("batch_run_id")
            || !object.contains_key("prompt_sha256")
            || !object.contains_key("prompt_bytes")
    }) {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "target core binding has an invalid field set",
        ));
    }
    Ok(DispatchIdentity {
        operation_id: target_id.to_owned(),
        input_sha256: input_sha256.to_owned(),
        batch_run_id: batch_run_id.to_owned(),
        requested_model: model_id.to_owned(),
        prompt_sha256: prompt_sha256.to_owned(),
        prompt_bytes,
        task_snapshot_sha256: String::new(),
    })
}

// Keep the transport, binding, route, outcome, and receipt hash together: the
// journal retry path validates every item before it sends the saved envelope.
#[allow(clippy::too_many_arguments)]
async fn deliver_outcome(
    mut client: ModuleLink,
    config: &Config,
    credential: &Credential,
    owner: &Owner,
    store: &RunStore,
    expected_binding_id: &str,
    expected_generation: i64,
    expected_route: &Value,
    outcome: RuntimeOutcome,
    hash: String,
) -> Result<ModuleLink> {
    if !store.outcome_pending(&outcome.operation_id, &hash)? {
        return Ok(client);
    }
    let payload = serde_json::to_value(&outcome)?;
    match client.outcome(payload.clone()).await {
        Ok(ack) => {
            store.acknowledge(&outcome.operation_id, &hash, ack)?;
            store.finalize_release_after_outcome_ack(&outcome.operation_id, &hash)?;
            Ok(client)
        }
        Err(error) if is_transport_uncertain(&error) => {
            // One reconnect and one exact payload retry are bounded here. The
            // adapter never retries the native prompt when its result ack is
            // uncertain.
            let replacement_link = connect_module(config, credential, owner).await?;
            if replacement_link.binding_id.as_str() != expected_binding_id
                || replacement_link.generation != expected_generation
                || &replacement_link.route != expected_route
            {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "module binding route changed before exact outcome retry",
                ));
            }
            let mut replacement = replacement_link.client;
            let ack = replacement.outcome(payload).await?;
            store.acknowledge(&outcome.operation_id, &hash, ack)?;
            store.finalize_release_after_outcome_ack(&outcome.operation_id, &hash)?;
            Ok(replacement)
        }
        Err(error) => Err(error),
    }
}

async fn deliver_result_page(
    link: &mut Link,
    config: &Config,
    credential: &Credential,
    owner: &Owner,
    store: &RunStore,
    params: Value,
    hash: String,
) -> Result<()> {
    let operation_id = params["operation_id"]
        .as_str()
        .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "result Operation is missing"))?
        .to_owned();
    result_page::validate_saved(
        &params,
        &owner.host.claim,
        &link.binding_id,
        link.generation,
    )?;
    if !store.result_page_pending(&operation_id, &hash)? {
        return Ok(());
    }
    let payload = params.clone();
    match link.client.result(payload.clone()).await {
        Ok(ack) => store.acknowledge_result_page(&operation_id, &hash, ack),
        Err(error) if is_transport_uncertain(&error) => {
            // A module.result retry resends only the exact sealed page. It
            // never reruns the native Command invocation.
            let mut replacement = connect_module(config, credential, owner).await?;
            if replacement.binding_id != link.binding_id
                || replacement.generation != link.generation
                || replacement.route != link.route
            {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "binding route changed before exact result-page retry",
                ));
            }
            let ack = replacement.client.result(payload).await?;
            store.acknowledge_result_page(&operation_id, &hash, ack)?;
            *link = replacement;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn connect_module(config: &Config, credential: &Credential, owner: &Owner) -> Result<Link> {
    connect_module_with_native_state(config, credential, owner, None, None).await
}

async fn connect_module_with_native_state(
    config: &Config,
    credential: &Credential,
    owner: &Owner,
    native_root_id: Option<&str>,
    native_scope_key: Option<&str>,
) -> Result<Link> {
    if native_root_id.is_some() != native_scope_key.is_some() {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "module hello requires a paired native root and scope",
        ));
    }
    let host_connection = config.host_connection.as_ref().ok_or_else(|| {
        Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host connection config is unavailable",
        )
    })?;
    let mut client = ModuleLink::connect(
        &host_connection.host_data_dir,
        credential,
        &host_connection.ipc,
    )
    .await?;
    let hello = client
        .hello(
            json!({
                "boot_id":owner.host.boot_id,
                "module_artifact_id":owner.host.profile.artifact_id(),
                "native_root_id":native_root_id,
                "native_scope_key":native_scope_key,
                "native_ready":false,
                "managed_owner":owner.record
            }),
            Some(&owner.host.claim),
        )
        .await?;
    module_host::require_negotiated(&hello, &owner.host)?;
    let route = hello.get("route").cloned().ok_or_else(|| {
        Error::new(
            "MODULE_HELLO_INVALID",
            "module.hello response omitted its route",
        )
    })?;
    if route["runtime"] != RUNTIME
        || route["module_artifact_id"] != owner.host.profile.artifact_id()
    {
        return Err(Error::new(
            "ARTIFACT_MISMATCH",
            "reserved route does not name this Rust Command adapter artifact",
        ));
    }
    route_model_from_value(&route)?;
    let binding_id = hello["binding_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::new("MODULE_HELLO_INVALID", "module binding id is missing"))?
        .to_owned();
    let generation = hello["generation"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::new("MODULE_HELLO_INVALID", "module generation is missing"))?;
    if binding_id != owner.host.binding_id || generation != owner.host.generation {
        return Err(Error::new(
            "MODULE_HELLO_INVALID",
            "module hello differs from the supervisor-selected binding scope",
        ));
    }
    Ok(Link {
        client,
        binding_id,
        generation,
        route,
        recovery_required: hello["recovery_required"] == true,
    })
}

async fn send_observation(
    client: &mut ModuleLink,
    owner: &Owner,
    sequence: u64,
    state: Value,
) -> Result<()> {
    let ack = client
        .observe(json!({
            "event_id":format!("{}:{sequence}", owner.token),
            "sequence":sequence,
            "state":state
        }))
        .await?;
    if ack["recorded"] != true {
        return Err(Error::new(
            "OBSERVATION_ACK_INVALID",
            "module.observe did not confirm recording",
        ));
    }
    Ok(())
}

fn module_state(
    link: &Link,
    owner: &Owner,
    last_operation: &Value,
    pending_results: usize,
) -> Value {
    let model = route_model_from_value(&link.route).unwrap_or("");
    json!({
        "phase":"sessionless",
        "native_root_id":Value::Null,
        "native_scope_key":Value::Null,
        "native_session_state":"not_started",
        "boot_id":owner.host.boot_id,
        "describe":{
            "runtime":RUNTIME,
            "module_artifact_id":owner.host.profile.artifact_id(),
            "module_artifact_version":owner.host.profile.artifact_version(),
            "contract_revision":owner.host.profile.contract_revision(),
            "entrypoint":"native_headless_cli",
            "execution_shape":owner.host.profile.execution_shape(),
            "installed_runtime_verified":false,
            "version_probe":"not_run",
            "requested_model":model,
            "effective_model":Value::Null,
            "effective_model_status":"unknown",
            "capabilities":{
                "open":"executor_preflight_only_no_native_session",
                "task_dispatch":"one_shot_sessionless_batch",
                "reconcile":"saved_evidence_readback_only",
                "refresh":"module_snapshot_read_only",
                "result_pages":"bounded_exact_command_status_and_output_pages",
                "send":"unavailable_sessionless_batch",
                "configure":"unavailable",
                "goal":"unavailable",
                "resume":"unavailable",
                "steer":"unavailable",
                "task_acceptance":"unavailable"
            }
        },
        "binding_id":link.binding_id,
        "generation":link.generation,
        "recovery_required":link.recovery_required,
        "last_operation":last_operation,
        "pending_result_acknowledgements":pending_results,
        "family_departure_claimed":false
    })
}

fn validate_command(command: &RuntimeCommand, link: &Link) -> Result<()> {
    if command.binding_id != link.binding_id
        || command.generation != link.generation
        || command.route != link.route
        || command.native_root_id.is_some()
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "module command differs from its authenticated binding route",
        ));
    }
    let acp_profile = command.route["module_artifact_id"] == crate::ACP_ARTIFACT_ID;
    let supported = if acp_profile {
        matches!(
            command.method.as_str(),
            "agent.open"
                | "task.dispatch"
                | "agent.send"
                | "agent.configure"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.reply"
                | "agent.result"
                | "native.command.cancel_turn"
                | "native.command.close_session"
        )
    } else {
        matches!(
            command.method.as_str(),
            "agent.open"
                | "task.dispatch"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.result"
                | "native.command.release_evidence"
        )
    };
    if !supported {
        return Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "Command sessionless module received an unsupported method",
        ));
    }
    let result_kind = command.input["selector"]["kind"].as_str();
    let acp_result = acp_profile
        && result_kind == Some("command_output")
        && command.input["selector"]["native_output"] == "acp.assistant_messages.txt";
    let missing_result_data = if acp_result {
        command.input["normalized_result_origin"]
            .as_object()
            .is_none()
            || command.input["normalized_result_payload_identity"]
                .as_object()
                .is_none()
    } else {
        (result_kind == Some("command_status")
            && command.input["target_operation_status"]
                .as_object()
                .is_none())
            || (result_kind == Some("command_output")
                && command.input["target_command_output"].as_object().is_none())
    };
    if command.method == "agent.result"
        && (!matches!(result_kind, Some("command_status" | "command_output"))
            || command
                .target_input_sha256
                .as_deref()
                .is_none_or(|digest| !is_sha256(digest))
            || missing_result_data)
    {
        return Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "Command result requires an exact Store status or output snapshot",
        ));
    }
    route_model(command)?;
    Ok(())
}

fn route_model(command: &RuntimeCommand) -> Result<&str> {
    route_model_from_value(&command.route)
}

fn route_model_from_value(route: &Value) -> Result<&str> {
    route["native_options"]["modelId"]
        .as_str()
        .filter(|value| !value.is_empty() && *value == value.trim() && value.len() <= 256)
        .ok_or_else(|| {
            Error::new(
                "COMMAND_MODEL_REQUIRED",
                "explicit route modelId is invalid",
            )
        })
}

fn load_config(path: &Path, profile: Profile) -> Result<Config> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("CONFIG_UNAVAILABLE", "adapter config file is unavailable"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES
    {
        return Err(Error::new(
            "CONFIG_INVALID",
            "adapter config is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "CONFIG_INVALID",
            "adapter config exceeds its size limit",
        ));
    }
    let config: Config = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("CONFIG_INVALID", "adapter config JSON is invalid"))?;
    let batch_config_valid = config.mod_path.as_deref().is_some_and(Path::is_absolute)
        && config
            .run_timeout_ms
            .is_some_and(|timeout| (100..=86_400_000).contains(&timeout))
        && config.command_args.len() <= MAX_FIXED_ARGS
        && config.command_args.iter().all(|arg| {
            Path::new(arg).is_absolute()
                && arg.len() <= MAX_FIXED_ARG_BYTES
                && !arg.chars().any(char::is_control)
                && !reserved_argument(arg)
        });
    let acp_args_valid = match config.command_args.as_slice() {
        [mode] => mode == "acp",
        [entrypoint, mode] => {
            mode == "acp"
                && Path::new(entrypoint).is_absolute()
                && entrypoint.len() <= MAX_FIXED_ARG_BYTES
                && !entrypoint.chars().any(char::is_control)
                && !reserved_argument(entrypoint)
        }
        _ => false,
    };
    let acp_config_valid =
        config.mod_path.is_none() && config.run_timeout_ms.is_none() && acp_args_valid;
    if config.module_artifact_id != profile.artifact_id()
        || !config.command.is_absolute()
        || match profile {
            Profile::BatchV3 | Profile::BatchV4 => !batch_config_valid,
            Profile::AcpV1 => !acp_config_valid,
        }
    {
        return Err(Error::new(
            "CONFIG_INVALID",
            "adapter config paths, timeout or fixed native arguments are invalid",
        ));
    }
    #[cfg(windows)]
    if config.command.extension().is_some_and(|extension| {
        extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
    }) {
        return Err(Error::new(
            "CONFIG_INVALID",
            "native executable path cannot select a shell wrapper",
        ));
    }
    Ok(config)
}

fn batch_mod_path(config: &Config) -> Result<&Path> {
    config
        .mod_path
        .as_deref()
        .ok_or_else(|| Error::new("CONFIG_INVALID", "batch Command mod path is missing"))
}

fn batch_run_timeout(config: &Config) -> Result<Duration> {
    config
        .run_timeout_ms
        .map(Duration::from_millis)
        .ok_or_else(|| Error::new("CONFIG_INVALID", "batch Command timeout is missing"))
}

fn load_host_connection_config(path: &Path, state_dir: &Path) -> Result<HostConnectionConfig> {
    if !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some("module-host-connection.json")
    {
        return Err(Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config path has an invalid location",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config path has no parent directory",
        )
    })?;
    let path_parent = fs::canonicalize(parent).map_err(|_| {
        Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config directory is unavailable",
        )
    })?;
    if path_parent != state_dir {
        return Err(Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config must be inside this binding's private state directory",
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "MODULE_HOST_CONFIG_UNAVAILABLE",
            "supervisor-provided host config file is unavailable",
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES
    {
        return Err(Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config exceeds its size limit",
        ));
    }
    let host: HostConnectionConfig = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config JSON is invalid",
        )
    })?;
    host.validate().map_err(|_| {
        Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host config is outside schema version 1",
        )
    })?;
    if !host.host_data_dir.is_dir()
        || host.ipc.max_connections == 0
        || host.ipc.max_inflight_per_connection == 0
        || host.ipc.max_frame_bytes < 1024
        || host.ipc.write_timeout_seconds == 0
    {
        return Err(Error::new(
            "MODULE_HOST_CONFIG_INVALID",
            "supervisor-provided host data or IPC settings are invalid",
        ));
    }
    Ok(host)
}

fn load_owner() -> Result<Owner> {
    let state_env = std::env::var_os("ELIOT_SWARM_MODULE_STATE");
    let owner_env = std::env::var_os("ELIOT_SWARM_MODULE_OWNER");
    let (Some(state_env), Some(owner_env)) = (state_env, owner_env) else {
        return Err(Error::new(
            "MODULE_OWNER_REQUIRED",
            "module must run under the local non-killing owner launcher",
        ));
    };
    let state_dir = PathBuf::from(state_env);
    let owner_path = PathBuf::from(owner_env);
    if !state_dir.is_absolute() || !owner_path.is_absolute() {
        return Err(Error::new(
            "MODULE_OWNER_INVALID",
            "module owner paths must be absolute",
        ));
    }
    let state_dir = fs::canonicalize(state_dir).map_err(|_| {
        Error::new(
            "MODULE_OWNER_INVALID",
            "module state directory is unavailable",
        )
    })?;
    let expected_owner = state_dir.join("owner.json");
    let owner_metadata = fs::symlink_metadata(&owner_path)
        .map_err(|_| Error::new("MODULE_OWNER_INVALID", "module owner record is unavailable"))?;
    if owner_metadata.file_type().is_symlink()
        || !owner_metadata.is_file()
        || fs::canonicalize(&owner_path)? != expected_owner
    {
        return Err(Error::new(
            "MODULE_OWNER_INVALID",
            "module owner record is not the exact child of its state directory",
        ));
    }
    if owner_metadata.len() > MAX_CREDENTIAL_BYTES {
        return Err(Error::new(
            "MODULE_OWNER_INVALID",
            "module owner record is too large",
        ));
    }
    let mut bytes = Vec::with_capacity(owner_metadata.len() as usize);
    File::open(&owner_path)?
        .take(MAX_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
        return Err(Error::new(
            "MODULE_OWNER_INVALID",
            "module owner record is too large",
        ));
    }
    let record: Value = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "MODULE_OWNER_INVALID",
            "module owner record is invalid JSON",
        )
    })?;
    let token = record["token"]
        .as_str()
        .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        .ok_or_else(|| Error::new("MODULE_OWNER_INVALID", "module owner token is invalid"))?
        .to_owned();
    if record["version"] != 1 || record["process"]["purpose"] != "module" {
        return Err(Error::new(
            "MODULE_OWNER_INVALID",
            "owner record does not name a managed module process group",
        ));
    }
    let host = module_host::load_claim_and_verify_owner(&record)?;
    Ok(Owner {
        state_dir,
        token,
        record,
        host,
    })
}

fn load_credential(path: &Path) -> Result<Credential> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "CREDENTIAL_UNAVAILABLE",
            "module credential file is unavailable",
        )
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_CREDENTIAL_BYTES
    {
        return Err(Error::new(
            "CREDENTIAL_INVALID",
            "module credential is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
        return Err(Error::new(
            "CREDENTIAL_INVALID",
            "module credential exceeds its size limit",
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("CREDENTIAL_INVALID", "module credential JSON is invalid"))
}

fn config_arguments() -> Result<(PathBuf, PathBuf)> {
    let mut args = std::env::args_os().skip(1);
    let host_flag = args.next();
    let host_path = args.next();
    let config_flag = args.next();
    let config_path = args.next();
    if host_flag.as_deref() != Some(std::ffi::OsStr::new("--module-host-config"))
        || config_flag.as_deref() != Some(std::ffi::OsStr::new("--config"))
        || args.next().is_some()
    {
        return Err(Error::invalid(
            "usage: swarm-adapter-command --module-host-config <supervisor-json> --config <native-json>",
        ));
    }
    let host_path = PathBuf::from(
        host_path.ok_or_else(|| Error::invalid("module host config path is required"))?,
    );
    let config_path =
        PathBuf::from(config_path.ok_or_else(|| Error::invalid("config path is required"))?);
    if !host_path.is_absolute() || !config_path.is_absolute() {
        return Err(Error::invalid("adapter config paths must be absolute"));
    }
    Ok((host_path, config_path))
}

fn reserved_argument(argument: &str) -> bool {
    let flag = argument.split_once('=').map_or(argument, |(flag, _)| flag);
    matches!(
        flag.to_ascii_lowercase().as_str(),
        "-p" | "--print"
            | "--output-format"
            | "--model"
            | "--mod"
            | "--cwd"
            | "--version"
            | "--resume"
            | "-r"
            | "--continue"
            | "-c"
    )
}

fn is_transport_uncertain(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "OUTCOME_UNKNOWN" | "DISCONNECTED" | "PROTOCOL_ERROR" | "HOST_UNAVAILABLE"
    )
}

fn is_connect_retryable(error: &Error) -> bool {
    matches!(error.code.as_str(), "HOST_UNAVAILABLE" | "IO_ERROR")
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn next_sequence(sequence: u64) -> Result<u64> {
    sequence.checked_add(1).ok_or_else(|| {
        Error::new(
            "OBSERVATION_SEQUENCE_EXHAUSTED",
            "module observation sequence reached its limit",
        )
    })
}

fn check_mod(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "COMMAND_MOD_UNAVAILABLE",
            "configured mod source is unavailable",
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 2 * 1024 * 1024
    {
        return Err(Error::new(
            "COMMAND_MOD_INVALID",
            "configured mod source is invalid",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(2 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(Error::new(
            "COMMAND_MOD_INVALID",
            "configured mod source exceeded its read boundary",
        ));
    }
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            normalized.push(b'\n');
            index += 2;
        } else {
            normalized.push(bytes[index]);
            index += 1;
        }
    }
    let hash = digest(&normalized);
    if hash != crate::MOD_SHA256 {
        return Err(Error::new(
            "COMMAND_MOD_MISMATCH",
            "configured mod differs from its artifact pin",
        ));
    }
    Ok(hash)
}

fn readback_unknown(command: &RuntimeCommand, diagnostic: &str) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({
            "execution_shape":EXECUTION_SHAPE,
            "completion_condition":"batch_readback_unconfirmed",
            "diagnostic_code":diagnostic,
            "native_replay":false,
            "family_departure_claimed":false
        }),
    }
}

fn receipts_completion(store: &RunStore, operation_id: &str) -> Option<Value> {
    let (outcome, _) = store.read_outcome(operation_id).ok().flatten()?;
    Some(
        outcome
            .details
            .get("completion_condition")
            .cloned()
            .unwrap_or(Value::Null),
    )
}

#[cfg(test)]
mod capture_release_fixture {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use swarm_contracts::module_command::{
        CommandCaptureArtifactPage, CommandCaptureOutputCoverage, CommandCaptureReleaseEvidence,
    };

    struct TempState(PathBuf);

    impl TempState {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("command-release-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).expect("create fixture state directory");
            Self(path)
        }
    }

    impl Drop for TempState {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct AdmittedCaptureFixture {
        _state: TempState,
        store: RunStore,
        identity: DispatchIdentity,
        binding_id: String,
        generation: i64,
        route: Value,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        native_child: Value,
    }

    impl AdmittedCaptureFixture {
        fn new() -> Result<Self> {
            let state = TempState::new();
            let store = RunStore::new_for_profile(&state.0, crate::Profile::BatchV3)?;
            let binding_id = "fixture-binding".to_owned();
            let generation = 3;
            let route = json!({
                "runtime":crate::RUNTIME,
                "module_artifact_id":crate::ARTIFACT_ID,
                "native_options":{"modelId":"fixture-model"}
            });
            let identity = DispatchIdentity {
                operation_id: "fixture-crash-window".to_owned(),
                input_sha256: "a".repeat(64),
                batch_run_id: "fixture-batch".to_owned(),
                requested_model: "fixture-model".to_owned(),
                prompt_sha256: "b".repeat(64),
                prompt_bytes: 19,
                task_snapshot_sha256: "c".repeat(64),
            };
            store.admit(
                &identity.operation_id,
                Some(&identity),
                &binding_id,
                generation,
                &route,
            )?;
            let stdout = b"partial stdout retained\n".to_vec();
            let stderr = b"partial stderr retained\n".to_vec();
            let native_child = json!({
                "spawn_returned_pid":17,
                "birth_identity":Value::Null,
                "exit_observed_through_child_handle":false,
                "exit":Value::Null,
                "family_departure_claimed":false,
                "manager_group_drain_required":true
            });
            store.save_native_evidence(
                &identity.operation_id,
                &stdout,
                &stderr,
                &json!({
                    "stream_drain_timed_out":true,
                    "native_stream_capture_status":"drain_deadline_exceeded",
                    "stdout":{
                        "bytes":stdout.len(),
                        "stored_bytes":stdout.len(),
                        "sha256":digest(&stdout),
                        "stored_sha256":digest(&stdout),
                        "truncated":false,
                        "read_error":true
                    },
                    "stderr":{
                        "bytes":stderr.len(),
                        "stored_bytes":stderr.len(),
                        "sha256":digest(&stderr),
                        "stored_sha256":digest(&stderr),
                        "truncated":false,
                        "read_error":true
                    },
                    "native_child":native_child
                }),
            )?;
            Ok(Self {
                _state: state,
                store,
                identity,
                binding_id,
                generation,
                route,
                stdout,
                stderr,
                native_child,
            })
        }
    }

    #[test]
    fn adapter_release_persists_intent_and_repeat_readback() -> Result<()> {
        let state = TempState::new();
        let store = RunStore::new_for_profile(&state.0, crate::Profile::BatchV3)?;
        let binding_id = "fixture-binding";
        let generation = 1;
        let route = json!({
            "runtime":crate::RUNTIME,
            "module_artifact_id":crate::ARTIFACT_ID,
            "native_options":{"modelId":"fixture-model"}
        });
        let target_id = "fixture-command-dispatch";
        let target_input_sha256 = "a".repeat(64);
        let stdout = b"partial stdout retained after timeout\n";
        let stderr = b"partial stderr retained after timeout\n";
        let native_child = json!({
            "spawn_returned_pid":17,
            "birth_identity":Value::Null,
            "exit_observed_through_child_handle":false,
            "exit":Value::Null,
            "family_departure_claimed":false,
            "manager_group_drain_required":true
        });
        let identity = DispatchIdentity {
            operation_id: target_id.to_owned(),
            input_sha256: target_input_sha256.clone(),
            batch_run_id: "fixture-batch".to_owned(),
            requested_model: "fixture-model".to_owned(),
            prompt_sha256: "b".repeat(64),
            prompt_bytes: 12,
            task_snapshot_sha256: "c".repeat(64),
        };
        store.admit(target_id, Some(&identity), binding_id, generation, &route)?;
        let target_outcome = RuntimeOutcome {
            operation_id: target_id.to_owned(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "completion_condition":"native_result_unconfirmed",
                "native_replay":false
            }),
        };
        let target_outcome_sha256 = store.save_outcome(&target_outcome)?;
        store.acknowledge(target_id, &target_outcome_sha256, json!({"recorded":true}))?;
        store.save_native_evidence(
            target_id,
            stdout,
            stderr,
            &json!({
                "stdout":{
                    "bytes":(stdout.len() + 9) as u64,
                    "stored_bytes":stdout.len() as u64,
                    "sha256":digest(b"partial stdout retained after timeout\nlate"),
                    "stored_sha256":digest(stdout),
                    "truncated":false,
                    "read_error":true
                },
                "stderr":{
                    "bytes":(stderr.len() + 9) as u64,
                    "stored_bytes":stderr.len() as u64,
                    "sha256":digest(b"partial stderr retained after timeout\nlate"),
                    "stored_sha256":digest(stderr),
                    "truncated":false,
                    "read_error":true
                },
                "native_child":native_child
            }),
        )?;

        let mut outputs = Vec::new();
        for (native_output, bytes, result_input_sha256) in [
            ("stdout.ndjson", stdout.as_slice(), "d".repeat(64)),
            ("stderr.txt", stderr.as_slice(), "e".repeat(64)),
        ] {
            let stream_sha256 = if native_output == "stdout.ndjson" {
                digest(b"partial stdout retained after timeout\nlate")
            } else {
                digest(b"partial stderr retained after timeout\nlate")
            };
            let snapshot = json!({
                "schema_version":1,
                "operation_id":target_id,
                "method":"task.dispatch",
                "operation_state":"outcome_unknown",
                "operation_outcome":"unknown",
                "completion_condition":"native_result_unconfirmed",
                "input_sha256":target_input_sha256,
                "module_receipt":Value::Null,
                "task_dispatch_context":Value::Null,
                "dispatch_admission":Value::Null,
                "native_output":native_output,
                "stream_bytes":(bytes.len() + 9) as u64,
                "stored_bytes":bytes.len() as u64,
                "stream_sha256":stream_sha256,
                "stored_sha256":digest(bytes),
                "truncated":false,
                "read_error":true,
                "native_child":native_child,
                "native_response_identity":"unavailable",
                "execution_complete":false,
                "task_completion":"unknown",
                "native_replay":false
            });
            assert_eq!(
                store.read_native_output(
                    target_id,
                    &target_input_sha256,
                    binding_id,
                    generation,
                    &route,
                    &snapshot,
                )?,
                bytes
            );
            let result_operation_id = format!("fixture-result-{native_output}");
            let artifact_ref = format!("artifact:{result_operation_id}");
            let params = json!({
                "operation_id":result_operation_id,
                "page":{
                    "source":{
                        "kind":"command_output",
                        "result_operation_id":result_operation_id,
                        "result_input_sha256":result_input_sha256,
                        "input_operation_id":target_id,
                        "target_input_sha256":target_input_sha256,
                        "target_command_output":snapshot
                    },
                    "offset_bytes":0,
                    "byte_length":bytes.len(),
                    "total_bytes":bytes.len(),
                    "eof":true,
                    "media_type":if native_output == "stdout.ndjson" {
                        "application/x-ndjson"
                    } else {
                        "text/plain; charset=utf-8"
                    },
                    "content_base64":STANDARD.encode(bytes),
                    "page_sha256":digest(bytes)
                }
            });
            store.admit(&result_operation_id, None, binding_id, generation, &route)?;
            let result_page_sha256 = store.save_result_page(&result_operation_id, &params)?;
            store.acknowledge_result_page(
                &result_operation_id,
                &result_page_sha256,
                json!({"recorded":true,"artifact_ref":artifact_ref}),
            )?;
            outputs.push(CommandCaptureOutputCoverage {
                native_output: native_output.to_owned(),
                target_command_output: snapshot,
                pages: vec![CommandCaptureArtifactPage {
                    result_operation_id,
                    result_input_sha256,
                    artifact_ref,
                    artifact_sha256: digest(bytes),
                    artifact_byte_length: bytes.len() as u64,
                    offset_bytes: 0,
                    byte_length: bytes.len() as u64,
                    total_bytes: bytes.len() as u64,
                    page_sha256: digest(bytes),
                    eof: true,
                }],
            });
        }
        let evidence = CommandCaptureReleaseEvidence {
            schema_version: 1,
            target_operation_id: target_id.to_owned(),
            target_input_sha256,
            target_outcome_sha256,
            outputs,
        };
        let release_input = json!({
            "target_operation_id":target_id,
            "capture_release_evidence":evidence
        });
        assert_eq!(
            swarm_contracts::module_command::classify_runtime_command(
                swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD,
                &release_input,
            ),
            Ok(swarm_contracts::module_command::RuntimeCommandKind::CommandReleaseEvidence)
        );
        let evidence: CommandCaptureReleaseEvidence = serde_json::from_value(
            release_input[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD].clone(),
        )?;
        let release_operation_id = "fixture-command-release";
        let release_input_sha256 = "f".repeat(64);
        store.admit(release_operation_id, None, binding_id, generation, &route)?;
        let (_, saved_target_hash) = store
            .read_outcome(target_id)?
            .ok_or_else(|| Error::new("FIXTURE_FAILED", "target outcome readback missing"))?;
        assert_eq!(saved_target_hash, evidence.target_outcome_sha256);
        let release_outcome = store.release_command_captures(
            release_operation_id,
            &release_input_sha256,
            binding_id,
            generation,
            &route,
            &evidence,
        )?;
        assert_eq!(
            release_outcome.details["completion_condition"],
            "command_capture_release_recorded"
        );
        let release_outcome_sha256 = store.save_outcome(&release_outcome)?;
        let unacknowledged_cleanup = store
            .finalize_release_after_outcome_ack(release_operation_id, &release_outcome_sha256)
            .expect_err("release cleanup must require the durable host ACK");
        assert_eq!(unacknowledged_cleanup.code, "ADAPTER_EVIDENCE_INVALID");
        store.finalize_acknowledged_releases()?;

        let (first_readback, first_hash) = store
            .read_outcome(release_operation_id)?
            .ok_or_else(|| Error::new("FIXTURE_FAILED", "release outcome missing"))?;
        let (repeat_readback, repeat_hash) = store
            .read_outcome(release_operation_id)?
            .ok_or_else(|| Error::new("FIXTURE_FAILED", "repeat release readback missing"))?;
        assert_eq!(
            first_readback.details["target_outcome_sha256"],
            evidence.target_outcome_sha256,
        );
        assert_eq!(
            first_readback.details["target_input_sha256"],
            evidence.target_input_sha256,
        );
        assert_eq!(
            serde_json::to_value(first_readback)?,
            serde_json::to_value(repeat_readback)?,
        );
        assert_eq!(first_hash, repeat_hash);
        assert_eq!(first_hash, release_outcome_sha256);
        assert!(store.outcome_pending(release_operation_id, &first_hash)?);
        assert_eq!(store.pending_outcomes()?.len(), 1);

        store.acknowledge(
            release_operation_id,
            &release_outcome_sha256,
            json!({"recorded":true}),
        )?;
        let mismatched_cleanup = store
            .finalize_release_after_outcome_ack(
                release_operation_id,
                &digest(b"different release outcome"),
            )
            .expect_err("a different outcome hash must not authorize cleanup");
        assert_eq!(mismatched_cleanup.code, "ADAPTER_EVIDENCE_INVALID");
        assert!(store.read_outcome(release_operation_id)?.is_some());
        store.finalize_release_after_outcome_ack(release_operation_id, &release_outcome_sha256)?;
        store.finalize_acknowledged_releases()?;
        assert!(store.read_outcome(release_operation_id)?.is_none());
        for output in &evidence.outputs {
            for page in &output.pages {
                assert!(store.read_result_page(&page.result_operation_id)?.is_none());
            }
        }
        assert!(store.pending_outcomes()?.is_empty());
        assert!(store.pending_result_pages()?.is_empty());
        assert!(
            store
                .read_native_output(
                    target_id,
                    &evidence.target_input_sha256,
                    binding_id,
                    generation,
                    &route,
                    &evidence.outputs[0].target_command_output,
                )
                .is_err()
        );
        assert!(
            !state
                .0
                .join("command-adapter-runs-v3")
                .join(format!("op-{}", digest(target_id.as_bytes())))
                .exists()
        );
        assert!(
            !state
                .0
                .join("command-adapter-runs-v3")
                .join(format!("releasing-{}", digest(target_id.as_bytes())))
                .exists()
        );
        assert!(
            !state
                .0
                .join("command-adapter-runs-v3")
                .join(format!("op-{}", digest(release_operation_id.as_bytes())))
                .exists()
        );
        assert_eq!(
            std::fs::read_dir(state.0.join("command-adapter-runs-v3"))?.count(),
            0,
        );
        Ok(())
    }

    #[test]
    fn admitted_retry_recovers_validated_capture_as_unknown_without_replay() -> Result<()> {
        let fixture = AdmittedCaptureFixture::new()?;
        let retry_store = RunStore::new_for_profile(&fixture._state.0, crate::Profile::BatchV3)?;
        assert!(
            retry_store
                .read_outcome(&fixture.identity.operation_id)?
                .is_none(),
            "reopened crash-window journal must have no persisted outcome"
        );

        let outcome = retry_admitted_dispatch_outcome(
            &retry_store,
            &fixture.identity,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
            None,
        )?;
        assert_eq!(outcome.outcome, EffectOutcome::Unknown);
        assert_eq!(outcome.operation_id, fixture.identity.operation_id);
        assert_eq!(
            outcome.details["input_sha256"],
            fixture.identity.input_sha256
        );
        assert_eq!(
            outcome.details["prompt_sha256"],
            fixture.identity.prompt_sha256
        );
        assert_eq!(
            outcome.details["task_snapshot_sha256"],
            fixture.identity.task_snapshot_sha256
        );
        assert_eq!(
            outcome.details["completion_condition"],
            "native_result_unconfirmed"
        );
        assert_eq!(
            outcome.details["diagnostic_code"],
            "native_outcome_missing_after_capture_persisted"
        );
        assert_eq!(
            outcome.details["native_capture_recovery_status"],
            "validated"
        );
        assert_eq!(outcome.details["local_evidence_available"], true);
        assert_eq!(outcome.details["native_replay"], false);
        assert_eq!(outcome.details["task_acceptance_claimed"], false);
        assert_eq!(outcome.details["native_result"], Value::Null);
        assert_eq!(outcome.details["result_subtype"], Value::Null);
        assert_eq!(outcome.details["stdout_bytes"], fixture.stdout.len() as u64);
        assert_eq!(
            outcome.details["stdout_stored_bytes"],
            fixture.stdout.len() as u64
        );
        assert_eq!(outcome.details["stdout_sha256"], digest(&fixture.stdout));
        assert_eq!(
            outcome.details["stdout_stored_sha256"],
            digest(&fixture.stdout)
        );
        assert_eq!(outcome.details["stdout_read_error"], true);
        assert_eq!(outcome.details["stderr_bytes"], fixture.stderr.len() as u64);
        assert_eq!(outcome.details["stderr_sha256"], digest(&fixture.stderr));
        assert_eq!(
            outcome.details["stderr_stored_sha256"],
            digest(&fixture.stderr)
        );
        assert_eq!(outcome.details["stderr_read_error"], true);
        assert_eq!(outcome.details["native_child"], fixture.native_child);

        let stdout_expected = json!({
            "task_dispatch_context":Value::Null,
            "module_receipt":Value::Null,
            "dispatch_admission":Value::Null,
            "native_output":"stdout.ndjson",
            "stream_bytes":outcome.details["stdout_bytes"],
            "stored_bytes":outcome.details["stdout_stored_bytes"],
            "stream_sha256":outcome.details["stdout_sha256"],
            "stored_sha256":outcome.details["stdout_stored_sha256"],
            "truncated":outcome.details["stdout_truncated"],
            "read_error":outcome.details["stdout_read_error"],
            "native_child":outcome.details["native_child"]
        });
        let stderr_expected = json!({
            "task_dispatch_context":Value::Null,
            "module_receipt":Value::Null,
            "dispatch_admission":Value::Null,
            "native_output":"stderr.txt",
            "stream_bytes":outcome.details["stderr_bytes"],
            "stored_bytes":outcome.details["stderr_stored_bytes"],
            "stream_sha256":outcome.details["stderr_sha256"],
            "stored_sha256":outcome.details["stderr_stored_sha256"],
            "truncated":outcome.details["stderr_truncated"],
            "read_error":outcome.details["stderr_read_error"],
            "native_child":outcome.details["native_child"]
        });
        assert_eq!(
            retry_store.read_native_output(
                &fixture.identity.operation_id,
                &fixture.identity.input_sha256,
                &fixture.binding_id,
                fixture.generation,
                &fixture.route,
                &stdout_expected,
            )?,
            fixture.stdout
        );
        assert_eq!(
            retry_store.read_native_output(
                &fixture.identity.operation_id,
                &fixture.identity.input_sha256,
                &fixture.binding_id,
                fixture.generation,
                &fixture.route,
                &stderr_expected,
            )?,
            fixture.stderr
        );
        Ok(())
    }

    #[test]
    fn admitted_retry_wrong_identity_and_corrupt_capture_stay_fenced() -> Result<()> {
        let fixture = AdmittedCaptureFixture::new()?;
        let mut wrong_identity = fixture.identity.clone();
        wrong_identity.input_sha256 = "d".repeat(64);
        assert_eq!(
            fixture
                .store
                .admit(
                    &wrong_identity.operation_id,
                    Some(&wrong_identity),
                    &fixture.binding_id,
                    fixture.generation,
                    &fixture.route,
                )
                .expect_err("changed request digest must not reuse admitted evidence")
                .code,
            "ADAPTER_EVIDENCE_CONFLICT"
        );

        let directory = fixture.store.directory(&fixture.identity.operation_id)?;
        fs::write(directory.join("stdout.ndjson"), b"corrupt capture")?;
        let outcome = retry_admitted_dispatch_outcome(
            &fixture.store,
            &fixture.identity,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
            None,
        )?;
        assert_eq!(outcome.outcome, EffectOutcome::Unknown);
        assert_eq!(
            outcome.details["input_sha256"],
            fixture.identity.input_sha256
        );
        assert_eq!(
            outcome.details["diagnostic_code"],
            "native_capture_proof_unavailable"
        );
        assert_eq!(
            outcome.details["native_capture_recovery_status"],
            "proof_unavailable"
        );
        assert_eq!(outcome.details["local_evidence_available"], false);
        assert_eq!(outcome.details["native_replay"], false);
        assert_eq!(outcome.details["task_acceptance_claimed"], false);
        assert!(outcome.details["stdout_bytes"].is_null());
        assert!(
            fixture
                .store
                .read_outcome(&fixture.identity.operation_id)?
                .is_none(),
            "retry resolution must not fabricate a saved outcome before the caller persists it"
        );
        Ok(())
    }

    #[test]
    fn admitted_retry_inconsistent_capture_receipt_stays_fenced() -> Result<()> {
        let fixture = AdmittedCaptureFixture::new()?;
        let directory = fixture.store.directory(&fixture.identity.operation_id)?;
        let receipt_path = directory.join("run.json");
        let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path)?)?;
        receipt["stdout"]["sha256"] = json!(digest(b"different whole stream"));
        fs::write(&receipt_path, serde_json::to_vec(&receipt)?)?;

        let outcome = retry_admitted_dispatch_outcome(
            &fixture.store,
            &fixture.identity,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
            None,
        )?;
        assert_eq!(outcome.outcome, EffectOutcome::Unknown);
        assert_eq!(
            outcome.details["diagnostic_code"],
            "native_capture_proof_unavailable"
        );
        assert_eq!(
            outcome.details["native_capture_recovery_status"],
            "proof_unavailable"
        );
        assert_eq!(outcome.details["local_evidence_available"], false);
        assert_eq!(outcome.details["native_replay"], false);
        assert_eq!(outcome.details["task_acceptance_claimed"], false);
        assert!(
            fixture
                .store
                .read_outcome(&fixture.identity.operation_id)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn admitted_retry_missing_capture_stays_truthfully_unknown() -> Result<()> {
        let fixture = AdmittedCaptureFixture::new()?;
        let directory = fixture.store.directory(&fixture.identity.operation_id)?;
        for name in ["run.json", "stdout.ndjson", "stderr.txt"] {
            fs::remove_file(directory.join(name))?;
        }
        let retry_store = RunStore::new_for_profile(&fixture._state.0, crate::Profile::BatchV3)?;

        let outcome = retry_admitted_dispatch_outcome(
            &retry_store,
            &fixture.identity,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
            None,
        )?;
        assert_eq!(outcome.outcome, EffectOutcome::Unknown);
        assert_eq!(
            outcome.details["diagnostic_code"],
            "native_result_missing_after_admission"
        );
        assert_eq!(
            outcome.details["native_capture_recovery_status"],
            "not_retained"
        );
        assert_eq!(outcome.details["local_evidence_available"], false);
        assert_eq!(outcome.details["native_replay"], false);
        assert_eq!(outcome.details["task_acceptance_claimed"], false);
        assert!(
            retry_store
                .read_outcome(&fixture.identity.operation_id)?
                .is_none()
        );
        Ok(())
    }
}
