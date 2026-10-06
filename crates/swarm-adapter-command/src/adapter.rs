use crate::module_host::{self, ModuleHostIdentity};
use crate::{ARTIFACT_ID, CONTRACT_REVISION, EXECUTION_SHAPE, RUNTIME};
use crate::{
    journal::{DispatchIdentity, RunStore, digest},
    native,
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
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};
use tokio::time;

const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_CREDENTIAL_BYTES: u64 = 16_384;
const MAX_FIXED_ARGS: usize = 32;
const MAX_FIXED_ARG_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    module_artifact_id: String,
    command: PathBuf,
    #[serde(default)]
    command_args: Vec<String>,
    mod_path: PathBuf,
    run_timeout_ms: u64,
    #[serde(skip)]
    host_connection: Option<HostConnectionConfig>,
}

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

pub async fn run() -> Result<()> {
    let (host_config_path, config_path) = config_arguments()?;
    let config = load_config(&config_path)?;
    let owner = load_owner()?;
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
    let store = RunStore::new(&owner.state_dir)?;
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
        for (outcome, hash) in store.pending_outcomes()? {
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
    if prompt.as_bytes().len() != identity.prompt_bytes
        || digest(prompt.as_bytes()) != identity.prompt_sha256
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
                let mod_check = check_mod(&config.mod_path);
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
            let (prompt, identity) = native::prompt_for(command)?;
            let workspace = native::route_workspace(command)?;
            let dispatch_admission =
                normalized_dispatch_admission(owner, command, &identity, &prompt)?;
            let (_dir, existing) = store.admit(
                &command.operation_id,
                Some(&identity),
                &command.binding_id,
                command.generation,
                &command.route,
            )?;
            if existing {
                if module_host::normalized_dispatch_enabled(&owner.host.claim)
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
                let outcome = if let Some((saved, _)) = store.read_outcome(&command.operation_id)? {
                    if !native::result_identity_matches(&serde_json::to_value(&saved)?, &identity) {
                        return Err(Error::new(
                            "ADAPTER_EVIDENCE_CONFLICT",
                            "saved terminal receipt differs from the core-frozen Operation",
                        ));
                    }
                    saved
                } else {
                    native::core_bound_unknown(
                        &command.operation_id,
                        &identity,
                        "native_result_missing_after_admission",
                    )
                };
                validate_dispatch_outcome(&outcome, dispatch_admission.as_ref())?;
                return Ok((vec![outcome], false));
            }
            if let Some(admission) = dispatch_admission.as_ref() {
                store.save_dispatch_admission(&command.operation_id, admission)?;
            }
            let invocation = native::invoke(
                &native::InvocationConfig {
                    program: config.command.clone(),
                    fixed_args: config.command_args.clone(),
                    mod_path: config.mod_path.clone(),
                    run_timeout: Duration::from_millis(config.run_timeout_ms),
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
            if matches!(outcome.outcome, EffectOutcome::Applied) {
                if let Some(admission) = dispatch_admission.as_ref() {
                    outcome.details["dispatch_admission"] = serde_json::to_value(admission)?;
                }
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
                "module_artifact_id":ARTIFACT_ID,
                "native_root_id":Value::Null,
                "native_scope_key":Value::Null,
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
    if route["runtime"] != RUNTIME || route["module_artifact_id"] != ARTIFACT_ID {
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
            "module_artifact_id":ARTIFACT_ID,
            "contract_revision":CONTRACT_REVISION,
            "entrypoint":"native_headless_cli",
            "execution_shape":EXECUTION_SHAPE,
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
    if !matches!(
        command.method.as_str(),
        "agent.open" | "task.dispatch" | "agent.refresh" | "agent.reconcile" | "agent.result"
    ) {
        return Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "Command sessionless module received an unsupported method",
        ));
    }
    let result_kind = command.input["selector"]["kind"].as_str();
    if command.method == "agent.result"
        && (!matches!(result_kind, Some("command_status" | "command_output"))
            || command
                .target_input_sha256
                .as_deref()
                .is_none_or(|digest| !is_sha256(digest))
            || (result_kind == Some("command_status")
                && command.input["target_operation_status"]
                    .as_object()
                    .is_none())
            || (result_kind == Some("command_output")
                && command.input["target_command_output"].as_object().is_none()))
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

fn load_config(path: &Path) -> Result<Config> {
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
    if config.module_artifact_id != ARTIFACT_ID
        || !config.command.is_absolute()
        || !config.mod_path.is_absolute()
        || !(100..=86_400_000).contains(&config.run_timeout_ms)
        || config.command_args.len() > MAX_FIXED_ARGS
        || config.command_args.iter().any(|arg| {
            !Path::new(arg).is_absolute()
                || arg.len() > MAX_FIXED_ARG_BYTES
                || arg.chars().any(char::is_control)
                || reserved_argument(arg)
        })
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
    if owner_metadata.len() > MAX_CREDENTIAL_BYTES as u64 {
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
