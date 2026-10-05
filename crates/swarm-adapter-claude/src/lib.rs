mod config;
mod journal;
mod module_runtime;
mod native_state;
mod receipt;
mod sdk_harness;

pub use config::{ARTIFACT_ID, ARTIFACT_VERSION, AdapterConfig, NativeOptions, RUNTIME};
pub use module_runtime::OwnedBootstrap;

use journal::{OperationJournal, digest_bytes, digest_json};
use native_state::{NativeControl, safe_family_resource_links};
use sdk_harness::{HarnessFrame, NativeHarness};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use swarm_client::ModuleLink;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome, TaskDispatchAdmissionReceipt,
        TaskDispatchContext,
    },
};
use tokio::{sync::mpsc, time::sleep};

const IDLE_POLL_MS: u64 = 500;
const MAX_FRAMES: usize = 256;
const MAX_INPUT_TEXT_BYTES: usize = 64 * 1024;
const MAX_TASK_SNAPSHOT_BYTES: usize = 64 * 1024;
const MAX_REFRESH_EXECUTIONS: usize = 16;
const MAX_REFRESH_FAMILY_EVENTS: usize = 16;

pub async fn run_owned(bootstrap: OwnedBootstrap) -> Result<()> {
    let OwnedBootstrap {
        config: host_config,
        credential,
        worker,
        contract,
    } = bootstrap;
    host_config.validate()?;
    let boot_id = worker.boot_id.clone();
    let (mut link, native_options) = open_link(
        &host_config,
        None,
        &credential,
        &contract,
        &worker.owner_record,
        &boot_id,
        false,
        None,
    )
    .await?;
    let config = host_config.with_native_options(native_options);
    config.validate()?;
    let route_sha256 = digest_json(&serde_json::to_value(&config.native_options)?)?;
    let journal = OperationJournal::open(
        &config.state_dir,
        &config.binding_id,
        config.generation,
        &config.native_options.scope_key(),
        &route_sha256,
    )?;
    journal.recover_uncertain()?;
    let bridge_path = sdk_harness::materialize(&config.sdk_harness_dir)?;
    let (frame_tx, mut frame_rx) = mpsc::channel::<HarnessFrame>(MAX_FRAMES);
    let mut harness: Option<NativeHarness> = None;
    let mut harness_alive = false;
    let mut native_prepared = false;
    let mut latest_state = Value::Null;
    let mut session_root: Option<String> = None;
    let mut native_control = NativeControl::default();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut frames_closed = false;

    loop {
        flush_outbox(&mut link, &journal).await?;
        tokio::select! {
            signal = &mut ctrl_c => {
                signal.map_err(|_| Error::new("ADAPTER_SIGNAL", "shutdown signal could not be installed"))?;
                if let Some(native) = harness.as_mut() {
                    let _ = native.send(json!({"kind":"stop"})).await;
                }
                return Ok(());
            }
            frame = receive_frame(&mut frame_rx, frames_closed) => {
                match frame {
                    Some(frame) => {
                        if handle_frame(
                            frame,
                            &config,
                            &boot_id,
                            &journal,
                            &mut harness_alive,
                            &mut native_prepared,
                            &mut latest_state,
                            &mut session_root,
                            &mut native_control,
                        )? {
                            link = open_link(
                                &host_config,
                                Some(&config.native_options),
                                &credential,
                                &contract,
                                &worker.owner_record,
                                &boot_id,
                                harness_alive,
                                session_root.as_deref(),
                            ).await?.0;
                        }
                    }
                    None => frames_closed = true,
                }
            }
            response = link.next() => {
                let response = response?;
                let command_value = response.get("command").ok_or_else(|| {
                    Error::new("HOST_COMMAND_SCHEMA", "module.next response lacks command state")
                })?;
                if !command_value.is_null() {
                    let command: RuntimeCommand = serde_json::from_value(command_value.clone())
                        .map_err(|_| Error::new("HOST_COMMAND_SCHEMA", "module command does not match the shared contract"))?;
                    verify_command_scope(&config, &command, &contract)?;
                    if handle_command(
                        &config,
                        &contract,
                        &journal,
                        &mut harness,
                        &frame_tx,
                        &boot_id,
                        &mut harness_alive,
                        native_prepared,
                        &latest_state,
                        session_root.as_deref(),
                        &mut native_control,
                        &bridge_path,
                        &command,
                    ).await? {
                        link = open_link(
                            &host_config,
                            Some(&config.native_options),
                            &credential,
                            &contract,
                            &worker.owner_record,
                            &boot_id,
                            harness_alive,
                            session_root.as_deref(),
                        ).await?.0;
                    }
                } else {
                    sleep(Duration::from_millis(IDLE_POLL_MS)).await;
                }
            }
        }
    }
}

async fn receive_frame(
    rx: &mut mpsc::Receiver<HarnessFrame>,
    closed: bool,
) -> Option<HarnessFrame> {
    if closed {
        std::future::pending().await
    } else {
        rx.recv().await
    }
}

async fn open_link(
    host_config: &crate::config::HostBootstrapConfig,
    expected_options: Option<&NativeOptions>,
    credential: &Credential,
    claim: &ModuleContractClaim,
    owner: &Value,
    boot_id: &str,
    native_ready: bool,
    native_root: Option<&str>,
) -> Result<(ModuleLink, NativeOptions)> {
    let mut link =
        ModuleLink::connect(&host_config.host_data_dir, credential, &host_config.ipc).await?;
    let mut params = json!({
        "boot_id": boot_id,
        "module_artifact_id": ARTIFACT_ID,
        "native_ready": native_ready,
        "managed_owner": {"token": owner["token"], "process": owner["process"]}
    });
    if let Some(root) = native_root {
        let options = expected_options.ok_or_else(|| {
            Error::new(
                "ADAPTER_ROUTE_OPTIONS",
                "native root requires the retained route options",
            )
        })?;
        params["native_root_id"] = json!(root);
        params["native_scope_key"] = json!(options.scope_key());
    }
    let response = link.hello(params, Some(claim)).await?;
    let native_options = verify_hello(host_config, claim, &response, expected_options)?;
    Ok((link, native_options))
}

async fn handle_command(
    config: &AdapterConfig,
    claim: &ModuleContractClaim,
    journal: &OperationJournal,
    harness: &mut Option<NativeHarness>,
    frame_tx: &mpsc::Sender<HarnessFrame>,
    boot_id: &str,
    harness_alive: &mut bool,
    native_prepared: bool,
    latest_state: &Value,
    native_root_id: Option<&str>,
    native_control: &mut NativeControl,
    bridge_path: &Path,
    command: &RuntimeCommand,
) -> Result<bool> {
    let receipt = receipt::for_command(claim, command)?;
    if let Some(saved) = journal.get(&command.operation_id)? {
        if saved.receipt.as_ref() != Some(&receipt)
            || saved.method.as_deref() != Some(command.method.as_str())
        {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved operation receipt differs from the authenticated command",
            ));
        }
        if saved.outcome.is_none() {
            let outcome = unknown_outcome(
                command,
                &receipt,
                "EFFECT_MARKER_WITHOUT_SAVED_OUTCOME",
                native_root_id,
                &config.native_options.scope_key(),
            )?;
            journal.save_outcome(&command.operation_id, &outcome)?;
        }
        return Ok(false);
    }

    match command.method.as_str() {
        "agent.open" => {
            if let Err(error) = validate_route(config, command) {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    &error.code,
                    native_root_id,
                );
            }
            if command.native_root_id.is_some() || native_root_id.is_some() {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "OPEN_MUST_PRECEDE_NATIVE_IDENTITY",
                    native_root_id,
                );
            }
            if harness.is_some() {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "SESSION_ALREADY_OPEN",
                    native_root_id,
                );
            }
            let intent = intent_for(
                command,
                &receipt,
                None,
                boot_id,
                &config.native_options.scope_key(),
            )?;
            journal.write_intent(&command.operation_id, &receipt, &command.method, &intent)?;
            match NativeHarness::spawn(
                config.native_options.clone(),
                bridge_path.to_path_buf(),
                frame_tx.clone(),
            ) {
                Ok(native) => {
                    *harness = Some(native);
                    *harness_alive = true;
                    let send = harness.as_mut().expect("harness stored above").send(json!({
                        "kind":"prepare",
                        "operation_id":command.operation_id,
                        "workspace_root":&config.native_options.workspace_root,
                        "sdk_runtime_root":&config.native_options.sdk_runtime_root,
                        "native_executable":&config.native_options.native_executable,
                        "model_id":&config.native_options.model_id,
                        "permission_mode":&config.native_options.permission_mode,
                        "allow_dangerously_skip_permissions":config.native_options.allow_dangerously_skip_permissions,
                        "native_scope_key":config.native_options.scope_key(),
                        "bridge_boot_id":boot_id
                    })).await;
                    if send.is_err() {
                        *harness_alive = false;
                        let outcome = unknown_outcome(
                            command,
                            &receipt,
                            "SDK_HARNESS_PIPE_UNKNOWN",
                            native_root_id,
                            &config.native_options.scope_key(),
                        )?;
                        journal.save_outcome(&command.operation_id, &outcome)?;
                    }
                    Ok(true)
                }
                Err(_) => {
                    let saved = journal.get(&command.operation_id)?.ok_or_else(|| {
                        Error::new(
                            "ADAPTER_INTENT_MISSING",
                            "failed SDK start has no durable intent",
                        )
                    })?;
                    let outcome = rejected_outcome_from_saved(
                        &command.operation_id,
                        &saved,
                        "SDK_HARNESS_START_FAILED",
                        config,
                    )?;
                    journal.save_outcome(&command.operation_id, &outcome)?;
                    Ok(false)
                }
            }
        }
        "task.dispatch" | "agent.send" => {
            if let Err(error) = validate_route(config, command) {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    &error.code,
                    native_root_id,
                );
            }
            let Some(native) = harness.as_mut() else {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "NATIVE_SESSION_NOT_OPEN",
                    native_root_id,
                );
            };
            if !*harness_alive {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "SDK_HARNESS_NOT_ALIVE",
                    native_root_id,
                );
            }
            if !native_prepared {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "NATIVE_SESSION_NOT_PREPARED",
                    native_root_id,
                );
            }
            let first_dispatch = command.method == "task.dispatch" && native_root_id.is_none();
            if native_root_id.is_none() && !first_dispatch {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "INITIAL_TASK_DISPATCH_REQUIRED",
                    native_root_id,
                );
            }
            if first_dispatch && command.native_root_id.is_some() {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "INITIAL_DISPATCH_ROOT_MISMATCH",
                    native_root_id,
                );
            }
            if let Some(root) = native_root_id
                && command.native_root_id.as_deref() != Some(root)
            {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "NATIVE_IDENTITY_MISMATCH",
                    native_root_id,
                );
            }
            if first_dispatch && journal.has_task_dispatch_for_boot(boot_id)? {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "INITIAL_TASK_DISPATCH_ALREADY_CLAIMED",
                    native_root_id,
                );
            }
            if native_root_id.is_some() && command.method == "task.dispatch" {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "TASK_DISPATCH_ALREADY_CLAIMED",
                    native_root_id,
                );
            }
            if command.method == "agent.send" && command.input["delivery"] != "next_turn" {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    "UNSUPPORTED_DELIVERY",
                    native_root_id,
                );
            }
            let text = match input_text(command, first_dispatch) {
                Ok(text) => text,
                Err(error) => {
                    return queue_rejected(
                        config,
                        claim,
                        journal,
                        command,
                        &receipt,
                        &error.code,
                        native_root_id,
                    );
                }
            };
            let source_text = command.input["text"]
                .as_str()
                .ok_or_else(|| Error::invalid("input text must be nonempty"))?;
            let prompt_sha256 = digest_bytes(text.as_bytes());
            let task_snapshot_sha256 = if first_dispatch {
                Some(digest_bytes(
                    command.input["task_snapshot_canonical"]
                        .as_str()
                        .expect("validated canonical snapshot")
                        .as_bytes(),
                ))
            } else {
                None
            };
            let input_id = uuid::Uuid::new_v4().to_string();
            let native_payload = json!({
                "kind":"send",
                "operation_id":command.operation_id,
                "initial_dispatch":first_dispatch,
                "text":text,
                "native_root_id":native_root_id,
                "user_message_uuid":input_id
            });
            let dispatch_admission = if command.method == "task.dispatch" {
                normalized_dispatch_admission(
                    claim,
                    command,
                    &receipt,
                    boot_id,
                    &input_id,
                    source_text,
                    &native_payload,
                )?
            } else {
                None
            };
            let mut native_intent = json!({
                "user_message_uuid":input_id,
                "prompt_sha256":prompt_sha256,
                "prompt_bytes":text.len(),
                "task_snapshot_sha256":task_snapshot_sha256,
                "native_root_id":native_root_id,
                "native_scope_key":config.native_options.scope_key(),
            });
            if let Some(admission) = dispatch_admission.as_ref() {
                native_intent["dispatch_admission"] = serde_json::to_value(admission)?;
            }
            let intent = intent_for(
                command,
                &receipt,
                Some(native_intent),
                boot_id,
                &config.native_options.scope_key(),
            )?;
            // Durable intent is synced before the control pipe can make an SDK input visible.
            journal.write_intent(&command.operation_id, &receipt, &command.method, &intent)?;
            if native_control
                .register_input(&input_id, &command.operation_id)
                .is_err()
            {
                let saved = journal.get(&command.operation_id)?.ok_or_else(|| {
                    Error::new(
                        "ADAPTER_INTENT_MISSING",
                        "rejected native input has no durable intent",
                    )
                })?;
                let outcome = rejected_outcome_from_saved(
                    &command.operation_id,
                    &saved,
                    "NATIVE_INPUT_IDENTITY",
                    config,
                )?;
                journal.save_outcome(&command.operation_id, &outcome)?;
                return Ok(false);
            }
            let sent = native
                .send(native_payload)
                .await;
            if sent.is_err() {
                *harness_alive = false;
                native_control.forget_operation(&command.operation_id);
                let outcome = unknown_outcome(
                    command,
                    &receipt,
                    "SDK_HARNESS_PIPE_UNKNOWN",
                    native_root_id,
                    &config.native_options.scope_key(),
                )?;
                journal.save_outcome(&command.operation_id, &outcome)?;
                return Ok(true);
            }
            Ok(false)
        }
        "agent.reconcile" => {
            if let Err(error) = validate_route(config, command) {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    &error.code,
                    native_root_id,
                );
            }
            handle_reconcile(config, journal, command, &receipt, latest_state)
        }
        "agent.refresh" => {
            if let Err(error) = validate_route(config, command) {
                return queue_rejected(
                    config,
                    claim,
                    journal,
                    command,
                    &receipt,
                    &error.code,
                    command.native_root_id.as_deref(),
                );
            }
            handle_refresh(
                config,
                claim,
                journal,
                command,
                &receipt,
                boot_id,
                native_root_id,
                latest_state,
            )
        }
        _ => queue_rejected(
            config,
            claim,
            journal,
            command,
            &receipt,
            "UNSUPPORTED_CAPABILITY",
            native_root_id,
        ),
    }
}

fn handle_refresh(
    config: &AdapterConfig,
    claim: &ModuleContractClaim,
    journal: &OperationJournal,
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    boot_id: &str,
    native_root_id: Option<&str>,
    latest_state: &Value,
) -> Result<bool> {
    let Some(requested_session) = command.input["session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.len() <= 512)
    else {
        return queue_rejected(
            config,
            claim,
            journal,
            command,
            receipt,
            "HOST_COMMAND_IDENTITY",
            command.native_root_id.as_deref(),
        );
    };
    if command.input["binding_id"] != command.binding_id
        || command.input["generation"].as_i64() != Some(command.generation)
        || command.native_root_id.as_deref() != Some(requested_session)
    {
        return queue_rejected(
            config,
            claim,
            journal,
            command,
            receipt,
            "NATIVE_IDENTITY_MISMATCH",
            command.native_root_id.as_deref(),
        );
    }

    let intent = intent_for(
        command,
        receipt,
        Some(json!({
            "readback":"local_sdk_observation_cache",
            "native_root_id":requested_session,
            "native_scope_key":config.native_options.scope_key()
        })),
        boot_id,
        &config.native_options.scope_key(),
    )?;
    journal.write_intent(&command.operation_id, receipt, &command.method, &intent)?;

    let observation_matches = native_root_id == Some(requested_session)
        && latest_state["bridge_boot_id"] == boot_id
        && latest_state["root_id"] == requested_session
        && latest_state["state"]["init_session_id"] == requested_session
        && latest_state["native_scope_key"] == config.native_options.scope_key();
    let native_observation = if observation_matches {
        compact_refresh_observation(latest_state)
    } else {
        Value::Null
    };
    let mut details = json!({
        "completion_condition":"native_snapshot_recorded",
        "readback":"local_sdk_observation_cache",
        "native_observation_available":observation_matches,
        "native_observation":native_observation,
        "native_session_id":requested_session,
        "family_completeness":"partial",
        "enumeration_complete":false,
        "completeness_reason":"SDK task and subagent lifecycle events are a bounded observation stream; they do not enumerate every native process or durable family member",
        "native_process_family_status":"unknown",
        "family_departure_proven":false,
        "family_events_available":observation_matches,
        "execution_complete":false,
        "task_completion_claimed":false,
        "replay_permitted":false
    });
    receipt::insert(&mut details, receipt)?;
    let outcome = RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some(config.native_options.scope_key()),
        native_root_id: Some(requested_session.to_owned()),
        turn_id: None,
        native_input_id: None,
        details,
    };
    journal.save_outcome(&command.operation_id, &outcome)?;
    Ok(false)
}

fn compact_refresh_observation(latest_state: &Value) -> Value {
    let state = &latest_state["state"];
    let all = state["input_executions"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let truncated = all.len() > MAX_REFRESH_EXECUTIONS;
    let executions = all
        .iter()
        .rev()
        .take(MAX_REFRESH_EXECUTIONS)
        .rev()
        .map(|event| {
            let mut compact = serde_json::Map::new();
            for key in [
                "native_session_id",
                "native_input_id",
                "correlation",
                "result_frame_uuid",
                "effective_model",
                "result_sha256",
            ] {
                if let Some(value) = event.get(key).and_then(Value::as_str)
                    && !value.is_empty()
                    && value.len() <= 128
                    && !value.bytes().any(|byte| byte.is_ascii_control())
                {
                    compact.insert(key.to_owned(), json!(value));
                }
            }
            for key in ["result_index", "result_bytes"] {
                if let Some(value) = event.get(key).filter(|value| value.is_u64()) {
                    compact.insert(key.to_owned(), value.clone());
                }
            }
            for key in ["is_error"] {
                if let Some(value) = event.get(key).and_then(Value::as_bool) {
                    compact.insert(key.to_owned(), json!(value));
                }
            }
            Value::Object(compact)
        })
        .collect::<Vec<_>>();
    let family_all = state["family_events"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let family_events_truncated = family_all.len() > MAX_REFRESH_FAMILY_EVENTS
        || state["family_events_truncated"] == true;
    let family_events = family_all
        .iter()
        .rev()
        .take(MAX_REFRESH_FAMILY_EVENTS)
        .rev()
        .map(|event| {
            let mut compact = serde_json::Map::new();
            for key in [
                "source",
                "event_type",
                "native_session_id",
                "task_id",
                "task_identity",
                "agent_id",
                "member_identity",
                "tool_use_id",
                "hook_tool_use_id",
                "parent_tool_use_id",
                "agent_type",
                "task_type",
                "task_reason",
                "task_last_tool_name",
                "frame_uuid",
                "prompt_id",
                "user_message_uuid",
                "sdk_task_status",
                "task_patch_status",
            ] {
                if let Some(value) = event.get(key).and_then(Value::as_str)
                    && !value.is_empty()
                    && value.len() <= 512
                    && !value.bytes().any(|byte| byte.is_ascii_control())
                {
                    compact.insert(key.to_owned(), json!(value));
                }
            }
            if let Some(ids) = event.get("user_message_uuids").and_then(Value::as_array) {
                let ids = ids
                    .iter()
                    .take(16)
                    .filter_map(Value::as_str)
                    .filter(|id| {
                        !id.is_empty()
                            && id.len() <= 256
                            && !id.bytes().any(|byte| byte.is_ascii_control())
                    })
                    .collect::<Vec<_>>();
                if !ids.is_empty() {
                    compact.insert("user_message_uuids".to_owned(), json!(ids));
                }
            }
            for key in [
                "is_backgrounded",
                "task_patch_is_backgrounded",
                "ambient",
                "input_links_truncated",
            ] {
                if let Some(value) = event.get(key).and_then(Value::as_bool) {
                    compact.insert(key.to_owned(), json!(value));
                }
            }
            if let Some(value) = event.get("spawn_depth").filter(|value| value.as_u64().is_some_and(|depth| depth <= 128)) {
                compact.insert("spawn_depth".to_owned(), value.clone());
            }
            if let Some(links) = event.get("resource_links").and_then(safe_family_resource_links) {
                let truncated = links.len() > 16 || event["resource_links_truncated"] == true;
                compact.insert(
                    "resource_links".to_owned(),
                    json!(links.into_iter().take(16).collect::<Vec<_>>()),
                );
                if truncated {
                    compact.insert("resource_links_truncated".to_owned(), json!(true));
                }
            }
            Value::Object(compact)
        })
        .collect::<Vec<_>>();
    json!({
        "bridge_boot_id":latest_state["bridge_boot_id"],
        "native_scope_key":latest_state["native_scope_key"],
        "root_id":latest_state["root_id"],
        "state":{
            "init_session_id":state["init_session_id"],
            "effective_model":state["effective_model"],
            "native_events_seen":state["native_events_seen"],
            "input_executions":executions,
            "input_executions_truncated":truncated,
            "family_events":family_events,
            "family_event_count":state["family_event_count"],
            "family_events_truncated":family_events_truncated,
            "family_projection_incomplete":state["family_projection_incomplete"]
        }
    })
}

fn handle_frame(
    frame: HarnessFrame,
    config: &AdapterConfig,
    boot_id: &str,
    journal: &OperationJournal,
    harness_alive: &mut bool,
    native_prepared: &mut bool,
    latest_state: &mut Value,
    session_root: &mut Option<String>,
    native_control: &mut NativeControl,
) -> Result<bool> {
    match frame {
        HarnessFrame::Message(value) => {
            let kind = value["kind"].as_str().unwrap_or("");
            match kind {
                "state" | "refreshed" => Err(Error::new(
                    "SDK_HARNESS_SCHEMA",
                    "stateful SDK summaries are not accepted from the Node driver",
                )),
                "prepared" => {
                    let operation_id = required_message_text(&value, "operation_id")?;
                    let Some(saved) = journal.get(operation_id)? else {
                        return Err(Error::new(
                            "ADAPTER_INTENT_MISSING",
                            "SDK prepared event has no durable open intent",
                        ));
                    };
                    let saved_receipt = saved.receipt.as_ref().ok_or_else(|| {
                        Error::new("ADAPTER_INTENT_MISSING", "SDK open intent has no receipt")
                    })?;
                    if saved.method.as_deref() != Some("agent.open")
                        || value["bridge_boot_id"] != boot_id
                    {
                        return Err(Error::new(
                            "SDK_HARNESS_IDENTITY",
                            "prepared frame differs from the exact open operation",
                        ));
                    }
                    let details = json!({
                        "completion_condition":"native_executor_prepared",
                        "native_session_state":"prepared",
                        "pre_input_executor_ready":true,
                        "native_session_id":null,
                        "describe":{"session_id":null},
                        "bridge_boot_id":value["bridge_boot_id"],
                        "requested_model":value["requested_model"],
                        "entrypoint":"claude_agent_sdk_streaming_input",
                        "sdk_package":"@anthropic-ai/claude-agent-sdk",
                        "sdk_version":"0.3.287",
                        "process_owner":"verified_inherited_module_group",
                        "family_completeness":"partial",
                        "module_pre_input_open_contract":{"schema_version":1,"kind":"pre_input_executor_ready","native_identity":"rootless","executor_preparation":"sdk_warm_query"},
                        "module_receipt":saved_receipt
                    });
                    let outcome = RuntimeOutcome {
                        operation_id: operation_id.to_owned(),
                        outcome: EffectOutcome::Applied,
                        native_scope_key: None,
                        native_root_id: None,
                        turn_id: None,
                        native_input_id: None,
                        details,
                    };
                    journal.save_outcome(operation_id, &outcome)?;
                    *harness_alive = true;
                    *native_prepared = true;
                    Ok(false)
                }
                "sdk_frame" => {
                    let frame = value.get("frame").ok_or_else(|| {
                        Error::new("SDK_FRAME_SCHEMA", "SDK frame event has no metadata")
                    })?;
                    let previous_root = session_root.clone();
                    native_control.observe_frame(frame, config, boot_id, journal, session_root)?;
                    *latest_state =
                        native_control.snapshot(config, boot_id, session_root.as_deref());
                    Ok(previous_root.as_deref() != session_root.as_deref())
                }
                "operation_unknown" => {
                    let operation_id = required_message_text(&value, "operation_id")?;
                    let saved = journal.get(operation_id)?.ok_or_else(|| {
                        Error::new(
                            "ADAPTER_INTENT_MISSING",
                            "SDK unknown result has no durable intent",
                        )
                    })?;
                    let code = safe_diagnostic(&value["diagnostic_code"], "SDK_EFFECT_UNKNOWN");
                    let outcome = unknown_outcome_from_saved(operation_id, &saved, &code, config)?;
                    journal.save_outcome(operation_id, &outcome)?;
                    native_control.forget_operation(operation_id);
                    Ok(false)
                }
                "operation_rejected" => {
                    let operation_id = required_message_text(&value, "operation_id")?;
                    let saved = journal.get(operation_id)?.ok_or_else(|| {
                        Error::new(
                            "ADAPTER_INTENT_MISSING",
                            "SDK rejection has no durable intent",
                        )
                    })?;
                    let code = safe_diagnostic(&value["diagnostic_code"], "SDK_OPERATION_REJECTED");
                    let outcome = rejected_outcome_from_saved(operation_id, &saved, &code, config)?;
                    journal.save_outcome(operation_id, &outcome)?;
                    native_control.forget_operation(operation_id);
                    Ok(false)
                }
                "harness_ended" => {
                    *harness_alive = false;
                    *native_prepared = false;
                    let code = safe_diagnostic(&value["diagnostic_code"], "SDK_STREAM_ENDED");
                    journal.recover_uncertain_with_code(&code)?;
                    native_control.clear_pending();
                    Ok(true)
                }
                "diagnostic" | "input_queued" | "stopped" => Ok(false),
                _ => Err(Error::new(
                    "SDK_HARNESS_SCHEMA",
                    "SDK harness sent an unsupported frame",
                )),
            }
        }
        HarnessFrame::Exited { .. } => {
            *harness_alive = false;
            *native_prepared = false;
            journal.recover_uncertain_with_code("SDK_HARNESS_PROCESS_EXITED_BEFORE_RECEIPT")?;
            native_control.clear_pending();
            Ok(true)
        }
        HarnessFrame::ReadFailure => {
            *harness_alive = false;
            *native_prepared = false;
            journal.recover_uncertain_with_code("SDK_HARNESS_FRAME_READ_FAILURE")?;
            native_control.clear_pending();
            Ok(true)
        }
    }
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
    receipt::for_command(claim, command)?;
    Ok(())
}

fn validate_route(config: &AdapterConfig, command: &RuntimeCommand) -> Result<()> {
    if command.route["runtime"] != RUNTIME
        || command.route["module_artifact_id"] != ARTIFACT_ID
        || command.route["workspace_option"] != "workspaceRoot"
        || command.route["native_options"] != serde_json::to_value(&config.native_options)?
    {
        return Err(Error::new(
            "ARTIFACT_MISMATCH",
            "module route differs from the exact prepared-open descriptor",
        ));
    }
    Ok(())
}

fn verify_hello(
    host_config: &crate::config::HostBootstrapConfig,
    claim: &ModuleContractClaim,
    response: &Value,
    expected_options: Option<&NativeOptions>,
) -> Result<NativeOptions> {
    if response["binding_id"] != host_config.binding_id
        || response["generation"] != host_config.generation
        || response["route"]["runtime"] != RUNTIME
        || response["route"]["module_artifact_id"] != ARTIFACT_ID
        || response["route"]["workspace_option"] != "workspaceRoot"
    {
        return Err(Error::new(
            "HOST_SCOPE_MISMATCH",
            "Store binding differs from the selected module descriptor",
        ));
    }
    let native_options: NativeOptions =
        serde_json::from_value(response["route"]["native_options"].clone()).map_err(|_| {
            Error::new(
                "ADAPTER_ROUTE_OPTIONS",
                "selected route options are malformed",
            )
        })?;
    native_options.validate()?;
    if expected_options.is_some_and(|expected| expected != &native_options) {
        return Err(Error::new(
            "HOST_SCOPE_MISMATCH",
            "the retained binding route options changed during module reconnect",
        ));
    }
    let negotiation = &response["module_contract_negotiation"];
    if negotiation["status"] != "negotiated"
        || negotiation["module_id"] != claim.module_id.as_str()
        || negotiation["artifact"] != serde_json::to_value(&claim.artifact)?
        || negotiation["protocol"] != serde_json::to_value(claim.protocol)?
        || negotiation["capabilities"] != serde_json::to_value(&claim.capabilities)?
        || negotiation["config_schema"] != serde_json::to_value(&claim.config_schema)?
        || negotiation["pre_input_open"] != serde_json::to_value(&claim.pre_input_open)?
        || negotiation["command_schemas"] != serde_json::to_value(&claim.command_schemas)?
        || negotiation["event_schemas"] != serde_json::to_value(&claim.event_schemas)?
        || negotiation["effects_authorized_by_descriptor"] != false
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "Store negotiation differs from the exact launch descriptor",
        ));
    }
    Ok(native_options)
}

fn intent_for(
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    native: Option<Value>,
    boot_id: &str,
    native_scope_key: &str,
) -> Result<Value> {
    let mut intent = json!({
        "receipt":receipt,
        "method":command.method,
        "binding_id":command.binding_id,
        "generation":command.generation,
        "input_sha256":receipt.input_sha256,
        "native":native,
        "bridge_boot_id":boot_id,
        "native_scope_key":native_scope_key,
        "replay_permitted":false
    });
    if let Some(root) = command.native_root_id.as_deref()
        && let Some(object) = intent.as_object_mut()
    {
        object.insert("native_root_id".to_owned(), json!(root));
    }
    Ok(intent)
}

fn normalized_dispatch_enabled(claim: &ModuleContractClaim) -> bool {
    claim.command_schemas.iter().any(|schema| {
        schema.schema_id == "swarm.task_dispatch_context"
            && schema.version == "1"
            && schema.sha256.is_none()
    }) && claim.event_schemas.iter().any(|schema| {
        schema.schema_id == "swarm.task_dispatch_admission"
            && schema.version == "1"
            && schema.sha256.is_none()
    })
}

fn normalized_dispatch_admission(
    claim: &ModuleContractClaim,
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    boot_id: &str,
    input_id: &str,
    source_text: &str,
    native_payload: &Value,
) -> Result<Option<TaskDispatchAdmissionReceipt>> {
    if !normalized_dispatch_enabled(claim) {
        return Ok(None);
    }
    if command.method != "task.dispatch" {
        return Ok(None);
    }
    let value = command.input.get("task_dispatch_context").ok_or_else(|| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_MISSING",
            "normalized task.dispatch command has no Store-supplied context",
        )
    })?;
    let context: TaskDispatchContext = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "Store-supplied task.dispatch context is malformed",
        )
    })?;
    context.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "Store-supplied task.dispatch context is invalid",
        )
    })?;
    let source_text_bytes = u64::try_from(source_text.len())
        .map_err(|_| Error::invalid("task dispatch text length is out of range"))?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != boot_id
        || context.source_text_sha256 != digest_bytes(source_text.as_bytes())
        || context.source_text_bytes != source_text_bytes
    {
        return Err(Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "Store-supplied task.dispatch context differs from the authenticated command",
        ));
    }
    let payload_bytes = serde_json::to_vec(native_payload)?;
    let native_payload_bytes = u64::try_from(payload_bytes.len())
        .map_err(|_| Error::invalid("native dispatch payload length is out of range"))?;
    let admission = TaskDispatchAdmissionReceipt {
        schema_version: context.schema_version,
        module_receipt: receipt.clone(),
        operation_id: context.operation_id.clone(),
        binding_id: context.binding_id.clone(),
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id.clone(),
        attempt_id: context.attempt_id.clone(),
        task_id: context.task_id.clone(),
        task_revision: context.task_revision,
        task_snapshot_sha256: context.task_snapshot_sha256.clone(),
        source_text_sha256: context.source_text_sha256.clone(),
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: digest_bytes(&payload_bytes),
        native_payload_bytes,
        native_input_id: Some(input_id.to_owned()),
    };
    admission.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_ADMISSION_INVALID",
            "generated normalized task dispatch receipt is invalid",
        )
    })?;
    Ok(Some(admission))
}

fn input_text(command: &RuntimeCommand, first_dispatch: bool) -> Result<String> {
    let text = command.input["text"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("input text must be nonempty"))?;
    if text.len() > MAX_INPUT_TEXT_BYTES {
        return Err(Error::new(
            "INPUT_BOUNDARY",
            "native input exceeds the adapter's text boundary",
        ));
    }
    if !first_dispatch {
        return Ok(text.to_owned());
    }
    if !command.input["task_snapshot"].is_object() {
        return Err(Error::invalid("immutable task snapshot is required"));
    }
    let canonical = command.input["task_snapshot_canonical"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::invalid("canonical task snapshot is required"))?;
    if canonical != canonical_json(&command.input["task_snapshot"])? {
        return Err(Error::new(
            "TASK_SNAPSHOT_MISMATCH",
            "canonical task snapshot differs from the retained value",
        ));
    }
    if canonical.len() > MAX_TASK_SNAPSHOT_BYTES {
        return Err(Error::new(
            "INPUT_BOUNDARY",
            "task snapshot exceeds the adapter's size boundary",
        ));
    }
    Ok(format!("Task specification: {canonical}\n\n{text}"))
}

fn canonical_json(value: &Value) -> Result<String> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                let mut sorted = serde_json::Map::new();
                for (key, value) in entries {
                    sorted.insert(key.clone(), ordered(value));
                }
                Value::Object(sorted)
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&ordered(value)).map_err(Into::into)
}

fn unknown_outcome(
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    code: &str,
    native_root: Option<&str>,
    scope: &str,
) -> Result<RuntimeOutcome> {
    let mut details =
        json!({"diagnostic_code":code,"completion_condition":"unknown","replay_permitted":false});
    receipt::insert(&mut details, receipt)?;
    Ok(RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: Some(scope.to_owned()),
        native_root_id: native_root.map(ToOwned::to_owned),
        turn_id: None,
        native_input_id: None,
        details,
    })
}

fn unknown_outcome_from_saved(
    operation_id: &str,
    saved: &journal::OperationState,
    code: &str,
    config: &AdapterConfig,
) -> Result<RuntimeOutcome> {
    let receipt = saved.receipt.as_ref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTENT_MISSING",
            "saved operation receipt is missing",
        )
    })?;
    let intent = saved.intent.as_ref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTENT_MISSING",
            "saved operation marker is missing",
        )
    })?;
    let native = &intent["native"];
    let mut details =
        json!({"diagnostic_code":code,"completion_condition":"unknown","replay_permitted":false});
    receipt::insert(&mut details, receipt)?;
    Ok(RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: native["native_scope_key"]
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| Some(config.native_options.scope_key())),
        native_root_id: native["native_root_id"].as_str().map(ToOwned::to_owned),
        turn_id: None,
        native_input_id: native["user_message_uuid"].as_str().map(ToOwned::to_owned),
        details,
    })
}

fn rejected_outcome_from_saved(
    operation_id: &str,
    saved: &journal::OperationState,
    code: &str,
    config: &AdapterConfig,
) -> Result<RuntimeOutcome> {
    let mut outcome = unknown_outcome_from_saved(operation_id, saved, code, config)?;
    outcome.outcome = EffectOutcome::Rejected;
    outcome.details["completion_condition"] = json!("rejected_before_native_admission");
    Ok(outcome)
}

fn queue_rejected(
    config: &AdapterConfig,
    _claim: &ModuleContractClaim,
    journal: &OperationJournal,
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    code: &str,
    native_root: Option<&str>,
) -> Result<bool> {
    let intent = intent_for(
        command,
        receipt,
        None,
        "preflight",
        &config.native_options.scope_key(),
    )?;
    journal.write_intent(&command.operation_id, receipt, &command.method, &intent)?;
    let mut details = json!({"diagnostic_code":code,"completion_condition":"rejected_before_native_admission","replay_permitted":false});
    receipt::insert(&mut details, receipt)?;
    let outcome = RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: Some(config.native_options.scope_key()),
        native_root_id: native_root.map(ToOwned::to_owned),
        turn_id: None,
        native_input_id: None,
        details,
    };
    journal.save_outcome(&command.operation_id, &outcome)?;
    Ok(false)
}

fn handle_reconcile(
    config: &AdapterConfig,
    journal: &OperationJournal,
    command: &RuntimeCommand,
    receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    latest_state: &Value,
) -> Result<bool> {
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("reconcile target operation ID is missing"))?;
    let target = journal.get(target_id)?;
    let target_receipt = target.as_ref().and_then(|state| state.receipt.as_ref());
    if let Some(target_receipt) = target_receipt {
        if command.target_input_sha256.as_deref() != Some(target_receipt.input_sha256.as_str())
            || target_receipt.binding_id != command.binding_id
            || target_receipt.binding_generation != command.generation
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "reconcile target receipt differs from the exact original Operation",
            ));
        }
    } else if !command
        .target_input_sha256
        .as_deref()
        .is_some_and(receipt::is_sha256)
    {
        return Err(Error::new(
            "HOST_COMMAND_IDENTITY",
            "reconcile target lacks its canonical Operation digest",
        ));
    }
    let intent = intent_for(
        command,
        receipt,
        Some(
            json!({"reconcile_target_operation_id":target_id,"native_scope_key":config.native_options.scope_key()}),
        ),
        latest_state["bridge_boot_id"].as_str().unwrap_or("unknown"),
        &config.native_options.scope_key(),
    )?;
    journal.write_intent(&command.operation_id, receipt, &command.method, &intent)?;

    let mut native_result = None;
    if let Some(target) = target.as_ref()
        && let (Some(target_intent), Some(state)) =
            (target.intent.as_ref(), latest_state["state"].as_object())
    {
        let native = &target_intent["native"];
        let expected_input = native["user_message_uuid"].as_str();
        let expected_boot = target_intent["bridge_boot_id"].as_str();
        let expected_root = target
            .outcome
            .as_ref()
            .and_then(|outcome| outcome["native_root_id"].as_str());
        let scope_ok = latest_state["native_scope_key"] == config.native_options.scope_key();
        if scope_ok && expected_boot == latest_state["bridge_boot_id"].as_str() {
            if let (Some(input_id), Some(root), Some(executions)) = (
                expected_input,
                expected_root,
                state.get("input_executions").and_then(Value::as_array),
            ) {
                native_result = executions
                    .iter()
                    .find(|event| {
                        event["native_input_id"] == input_id
                            && event["user_message_uuid"] == input_id
                            && event["correlation"] == "unique"
                            && event["native_session_id"] == root
                            && event["result_frame_uuid"]
                                .as_str()
                                .is_some_and(|value| !value.is_empty())
                    })
                    .cloned();
            }
        }
    }
    let target_outcome = target.as_ref().and_then(|state| state.outcome.as_ref());
    let rejected_before_admission =
        target_outcome.is_some_and(|outcome| outcome["outcome"] == "rejected");
    let resolved = native_result.is_some() || rejected_before_admission;
    let mut details = json!({
        "completion_condition":"adapter_readback_recorded",
        "target_operation_id":target_id,
        "target_module_receipt":target_receipt,
        "target_outcome":target_outcome,
        "native_result_evidence":native_result,
        "resolved":resolved,
        "native_readback":"current_sdk_observation_only",
        "native_replay":false
    });
    receipt::insert(&mut details, receipt)?;
    let outcome = RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some(config.native_options.scope_key()),
        native_root_id: command.native_root_id.clone(),
        turn_id: None,
        native_input_id: None,
        details,
    };
    journal.save_outcome(&command.operation_id, &outcome)?;
    Ok(false)
}

async fn flush_outbox(link: &mut ModuleLink, journal: &OperationJournal) -> Result<()> {
    for item in journal.pending_outcomes()? {
        let outcome = item
            .outcome
            .as_ref()
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "pending outcome is missing"))?;
        let response = link.outcome(outcome.clone()).await?;
        if response["recorded"] != true {
            return Err(Error::new(
                "HOST_ACK_SCHEMA",
                "Store did not record the exact adapter outcome",
            ));
        }
        journal.acknowledge(&item.operation_id, outcome)?;
    }
    Ok(())
}

fn required_message_text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "SDK_HARNESS_SCHEMA",
                "SDK harness message is missing an identity field",
            )
        })
}

fn safe_diagnostic(value: &Value, fallback: &'static str) -> String {
    value
        .as_str()
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 64
                && code.as_bytes()[0].is_ascii_uppercase()
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| fallback.to_owned())
}
