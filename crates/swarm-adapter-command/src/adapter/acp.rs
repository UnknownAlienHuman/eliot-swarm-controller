//! Native ACP session path for the separately registered Command ACP artifact.
//!
//! Only Store's versioned TaskPrompt envelope is admitted for task dispatch.
//! The journal is immutable at the native-effect boundary: an uncertain prompt
//! is read back as Unknown and is never sent a second time.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::{
    AcpAgent, AcpAgentConfig, Agent, Client, ConnectionTo, LineDirection,
    schema::{ProtocolVersion, v1::*},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_client::{HostConnectionConfig, ModuleLink};
use swarm_contracts::{
    EffectOutcome, RuntimeCommand, RuntimeOutcome,
    error::{Error, Result},
    runtime::{NormalizedResultOriginContext, TaskDispatchAdmissionReceipt},
};
use swarm_process::{private_permissions, replace_private_durable, write_private_new};
use tokio::{sync::{oneshot, Notify}, time};

use super::{Config, Link, Owner};
use crate::{
    ACP_ARTIFACT_ID, ACP_EXECUTION_SHAPE, Profile,
    acp_prompt::{self, PreparedAcpDispatch},
    adapter,
    journal::digest,
    module_host,
    native,
};

const MAX_RECORD_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CAPTURE_BYTES: usize = 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_PERMISSION_BYTES: usize = 64 * 1024;
const PERMISSION_REPLY_TIMEOUT: Duration = Duration::from_secs(90);
const OUTPUT_ROOT: &str = "command-acp-runs-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    schema_version: u8,
    artifact_id: String,
    operation_id: String,
    input_sha256: String,
    binding_id: String,
    generation: i64,
    route_sha256: String,
    task_prompt_sha256: String,
    task_prompt_bytes: u64,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    task_snapshot_sha256: String,
    dispatch_admission: Option<TaskDispatchAdmissionReceipt>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedOutcome {
    schema_version: u8,
    operation_id: String,
    outcome_sha256: String,
    outcome: RuntimeOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeAck {
    operation_id: String,
    outcome_sha256: String,
    ack: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedResultPage {
    schema_version: u8,
    operation_id: String,
    page_sha256: String,
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultPageAck {
    schema_version: u8,
    operation_id: String,
    page_sha256: String,
    ack: Value,
}

#[derive(Clone)]
struct AcpJournal {
    root: PathBuf,
}

#[derive(Default)]
struct Capture {
    output: Vec<u8>,
    output_truncated: bool,
    stderr: Vec<u8>,
    stderr_truncated: bool,
    updates: u64,
    pending_requests: Vec<Value>,
}

struct PendingPermission {
    session_id: String,
    fingerprint: String,
    options: Vec<(String, PermissionOptionKind)>,
    reply: oneshot::Sender<String>,
    acknowledgement: oneshot::Receiver<bool>,
}

type PermissionBroker = Arc<Mutex<HashMap<String, PendingPermission>>>;

struct ActiveSession {
    id: SessionId,
    owner_operation_id: String,
    workspace: PathBuf,
    model_id: String,
    close_supported: bool,
    closed: bool,
}

impl AcpJournal {
    fn new(state_dir: &Path) -> Result<Self> {
        let state = fs::canonicalize(state_dir)?;
        let root = state.join(OUTPUT_ROOT);
        match fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(invalid("ACP evidence root is not a regular directory"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&root)?;
                private_permissions(&root, true)?;
            }
            Err(error) => return Err(error.into()),
        }
        let root = fs::canonicalize(root)?;
        if root.parent() != Some(state.as_path()) {
            return Err(invalid("ACP evidence root escaped module state"));
        }
        Ok(Self { root })
    }

    fn operation_dir(&self, operation_id: &str) -> Result<PathBuf> {
        if operation_id.trim().is_empty() || operation_id.len() > 512 {
            return Err(invalid("ACP operation identity is invalid"));
        }
        let path = self.root.join(format!("op-{}", digest(operation_id.as_bytes())));
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(invalid("ACP operation evidence path is not a directory"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&path)?;
                private_permissions(&path, true)?;
            }
            Err(error) => return Err(error.into()),
        }
        let canonical = fs::canonicalize(path)?;
        if canonical.parent() != Some(self.root.as_path()) {
            return Err(invalid("ACP operation evidence escaped its root"));
        }
        Ok(canonical)
    }

    fn admit(
        &self,
        command: &RuntimeCommand,
        dispatch: &PreparedAcpDispatch,
        receipt: Option<TaskDispatchAdmissionReceipt>,
    ) -> Result<(PathBuf, bool)> {
        let directory = self.operation_dir(&command.operation_id)?;
        let path = directory.join("admission.json");
        let candidate = Admission {
            schema_version: 1,
            artifact_id: ACP_ARTIFACT_ID.to_owned(),
            operation_id: command.operation_id.clone(),
            input_sha256: command.input_sha256.clone().unwrap_or_default(),
            binding_id: command.binding_id.clone(),
            generation: command.generation,
            route_sha256: digest(serde_json::to_vec(&command.route)?.as_slice()),
            task_prompt_sha256: dispatch.identity.prompt_sha256.clone(),
            task_prompt_bytes: dispatch.identity.prompt_bytes,
            task_id: dispatch.identity.task_id.clone(),
            task_revision: dispatch.identity.task_revision,
            attempt_id: dispatch.identity.attempt_id.clone(),
            task_snapshot_sha256: dispatch.identity.task_snapshot_sha256.clone(),
            dispatch_admission: receipt,
        };
        if path.exists() {
            let saved: Admission = read_json(&path)?;
            if saved.artifact_id != candidate.artifact_id
                || saved.operation_id != candidate.operation_id
                || saved.input_sha256 != candidate.input_sha256
                || saved.binding_id != candidate.binding_id
                || saved.generation != candidate.generation
                || saved.route_sha256 != candidate.route_sha256
                || saved.task_prompt_sha256 != candidate.task_prompt_sha256
                || saved.task_prompt_bytes != candidate.task_prompt_bytes
                || saved.task_id != candidate.task_id
                || saved.task_revision != candidate.task_revision
                || saved.attempt_id != candidate.attempt_id
                || saved.task_snapshot_sha256 != candidate.task_snapshot_sha256
                || saved.dispatch_admission != candidate.dispatch_admission
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "saved ACP admission differs from the exact Store TaskPrompt",
                ));
            }
            return Ok((directory, true));
        }
        if fs::read_dir(&directory)?.next().transpose()?.is_some() {
            return Err(invalid("ACP evidence exists without its admission marker"));
        }
        write_json_new(&path, &candidate)?;
        Ok((directory, false))
    }

    fn save_session(&self, session: &Value) -> Result<()> {
        let path = self.root.join("session.json");
        let bytes = serde_json::to_vec(session)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("ACP session receipt exceeds its size bound"));
        }
        if path.exists() {
            replace_private_durable(&path, &bytes)?;
        } else {
            write_private_new(&path, &bytes)?;
        }
        Ok(())
    }

    fn read_session(&self) -> Result<Option<Value>> {
        let path = self.root.join("session.json");
        if !path.exists() {
            return Ok(None);
        }
        read_json(&path).map(Some)
    }

    fn mark_session_recovery(&self, reason: &str, capture: &Value) -> Result<()> {
        let Some(mut session) = self.read_session()? else {
            return Ok(());
        };
        if session["native_session_id"].as_str().is_some() {
            session["native_session_state"] = json!("recovery_required");
            session["recovery_status"] = json!(reason);
            session["last_capture"] = capture.clone();
            session["family_departure_claimed"] = json!(false);
            self.save_session(&session)?;
        }
        Ok(())
    }

    fn save_output(&self, operation_id: &str, bytes: &[u8]) -> Result<String> {
        if bytes.len() > MAX_CAPTURE_BYTES {
            return Err(invalid("ACP output capture exceeded its bound"));
        }
        let path = self.operation_dir(operation_id)?.join("output.txt");
        if path.exists() {
            let existing = read_bytes(&path, MAX_CAPTURE_BYTES)?;
            if existing != bytes {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "ACP output changed after its immutable capture was saved",
                ));
            }
        } else {
            write_private_new(&path, bytes)?;
        }
        Ok(digest(bytes))
    }

    fn read_output(&self, operation_id: &str) -> Result<Vec<u8>> {
        let path = self.operation_dir(operation_id)?.join("output.txt");
        if !path.exists() {
            return Err(Error::new(
                "RESULT_EVIDENCE_UNAVAILABLE",
                "ACP assistant-message capture is not retained for this Operation",
            ));
        }
        read_bytes(&path, MAX_CAPTURE_BYTES)
    }

    fn save_result_page(&self, operation_id: &str, params: &Value) -> Result<String> {
        if params["operation_id"].as_str() != Some(operation_id)
            || params["page"].as_object().is_none()
        {
            return Err(invalid("ACP result page differs from its exact Operation"));
        }
        let directory = self.operation_dir(operation_id)?;
        let hash = digest(serde_json::to_vec(params)?.as_slice());
        let path = directory.join("result-page.json");
        if path.exists() {
            let (saved, saved_hash) = self
                .read_result_page(operation_id)?
                .ok_or_else(|| invalid("saved ACP result page disappeared"))?;
            if saved_hash != hash || saved != *params {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "a different ACP result page is already saved for this Operation",
                ));
            }
            return Ok(hash);
        }
        write_json_new(
            &path,
            &SavedResultPage {
                schema_version: 1,
                operation_id: operation_id.to_owned(),
                page_sha256: hash.clone(),
                params: params.clone(),
            },
        )?;
        Ok(hash)
    }

    fn read_result_page(&self, operation_id: &str) -> Result<Option<(Value, String)>> {
        let path = self.operation_dir(operation_id)?.join("result-page.json");
        if !path.exists() {
            return Ok(None);
        }
        let saved: SavedResultPage = read_json(&path)?;
        let hash = digest(serde_json::to_vec(&saved.params)?.as_slice());
        if saved.schema_version != 1
            || saved.operation_id != operation_id
            || saved.params["operation_id"].as_str() != Some(operation_id)
            || saved.page_sha256 != hash
        {
            return Err(invalid("saved ACP result page failed its identity or digest check"));
        }
        Ok(Some((saved.params, hash)))
    }

    fn acknowledge_result_page(&self, operation_id: &str, hash: &str, ack: Value) -> Result<()> {
        if ack["recorded"] != true || ack["artifact_ref"].as_str().is_none_or(str::is_empty) {
            return Err(Error::new(
                "MODULE_RESULT_ACK_INVALID",
                "module.result did not confirm an immutable ACP artifact reference",
            ));
        }
        let (_, saved_hash) = self
            .read_result_page(operation_id)?
            .ok_or_else(|| invalid("saved ACP result page is missing"))?;
        if saved_hash != hash {
            return Err(invalid("ACP result acknowledgement differs from saved page"));
        }
        let path = self.operation_dir(operation_id)?.join("result-page-ack.json");
        let record = ResultPageAck {
            schema_version: 1,
            operation_id: operation_id.to_owned(),
            page_sha256: hash.to_owned(),
            ack,
        };
        if path.exists() {
            let prior: ResultPageAck = read_json(&path)?;
            if prior.schema_version != 1
                || prior.operation_id != operation_id
                || prior.page_sha256 != hash
            {
                return Err(invalid("saved ACP result acknowledgement conflicts with page"));
            }
            return Ok(());
        }
        write_json_new(&path, &record)
    }

    fn result_page_pending(&self, operation_id: &str, hash: &str) -> Result<bool> {
        let path = self.operation_dir(operation_id)?.join("result-page-ack.json");
        if !path.exists() {
            return Ok(true);
        }
        let ack: ResultPageAck = read_json(&path)?;
        if ack.schema_version != 1
            || ack.operation_id != operation_id
            || ack.page_sha256 != hash
            || ack.ack["recorded"] != true
            || ack.ack["artifact_ref"].as_str().is_none_or(str::is_empty)
        {
            return Err(invalid("saved ACP result acknowledgement is invalid"));
        }
        Ok(false)
    }

    fn pending_result_pages(&self) -> Result<Vec<(Value, String)>> {
        let mut paths = fs::read_dir(&self.root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.sort();
        let mut pending = Vec::new();
        for path in paths {
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(invalid("ACP evidence root contains a symbolic link"));
            }
            if path.file_name().and_then(|name| name.to_str()) == Some("session.json")
                && metadata.is_file()
            {
                continue;
            }
            if !metadata.is_dir() {
                return Err(invalid("unexpected non-directory entry in ACP evidence root"));
            }
            let saved_path = path.join("result-page.json");
            if !saved_path.exists() {
                continue;
            }
            let saved: SavedResultPage = read_json(&saved_path)?;
            let Some((params, hash)) = self.read_result_page(&saved.operation_id)? else {
                continue;
            };
            if path.file_name().and_then(|name| name.to_str())
                != Some(format!("op-{}", digest(saved.operation_id.as_bytes())).as_str())
            {
                return Err(invalid("ACP result page is stored under another Operation"));
            }
            if self.result_page_pending(&saved.operation_id, &hash)? {
                pending.push((params, hash));
            }
        }
        Ok(pending)
    }

    fn save_outcome(&self, outcome: &RuntimeOutcome) -> Result<String> {
        let directory = self.operation_dir(&outcome.operation_id)?;
        let path = directory.join("outcome.json");
        let canonical = serde_json::to_vec(outcome)?;
        let hash = digest(&canonical);
        let saved = SavedOutcome {
            schema_version: 1,
            operation_id: outcome.operation_id.clone(),
            outcome_sha256: hash.clone(),
            outcome: outcome.clone(),
        };
        if path.exists() {
            let prior: SavedOutcome = read_json(&path)?;
            if prior.operation_id != outcome.operation_id
                || prior.outcome_sha256 != hash
                || prior.outcome != *outcome
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "ACP terminal receipt changed after durable admission",
                ));
            }
        } else {
            write_json_new(&path, &saved)?;
        }
        Ok(hash)
    }

    fn read_outcome(&self, operation_id: &str) -> Result<Option<(RuntimeOutcome, String)>> {
        let path = self.operation_dir(operation_id)?.join("outcome.json");
        if !path.exists() {
            return Ok(None);
        }
        let saved: SavedOutcome = read_json(&path)?;
        let actual = digest(&serde_json::to_vec(&saved.outcome)?);
        if saved.schema_version != 1
            || saved.operation_id != operation_id
            || saved.outcome.operation_id != operation_id
            || saved.outcome_sha256 != actual
        {
            return Err(invalid("saved ACP outcome failed exact identity validation"));
        }
        Ok(Some((saved.outcome, actual)))
    }

    fn acknowledge(&self, operation_id: &str, hash: &str, ack: Value) -> Result<()> {
        let path = self.operation_dir(operation_id)?.join("outcome-ack.json");
        let record = OutcomeAck {
            operation_id: operation_id.to_owned(),
            outcome_sha256: hash.to_owned(),
            ack,
        };
        if path.exists() {
            let prior: OutcomeAck = read_json(&path)?;
            if prior.operation_id != record.operation_id || prior.outcome_sha256 != hash {
                return Err(invalid("ACP outcome acknowledgement conflicts with receipt"));
            }
        } else {
            write_json_new(&path, &record)?;
        }
        Ok(())
    }

    fn pending_outcomes(&self) -> Result<Vec<(RuntimeOutcome, String)>> {
        let mut pending = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                return Err(invalid("unexpected entry in ACP evidence root"));
            }
            if entry.file_name() == "session.json" && metadata.is_file() {
                continue;
            }
            if !metadata.is_dir() {
                return Err(invalid("unexpected entry in ACP evidence root"));
            }
            let outcome_path = entry.path().join("outcome.json");
            if !outcome_path.exists() || entry.path().join("outcome-ack.json").exists() {
                continue;
            }
            let saved: SavedOutcome = read_json(&outcome_path)?;
            let hash = digest(&serde_json::to_vec(&saved.outcome)?);
            if saved.schema_version != 1
                || saved.outcome.operation_id != saved.operation_id
                || saved.outcome_sha256 != hash
            {
                return Err(invalid("pending ACP outcome is malformed"));
            }
            pending.push((saved.outcome, hash));
        }
        Ok(pending)
    }
}

pub(super) async fn run(config: Config, owner: Owner, credential: swarm_contracts::Credential) -> Result<()> {
    if owner.host.profile != Profile::AcpV1 || !module_host::task_prompt_v1_enabled(&owner.host.claim) {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "ACP requires the registered TaskPrompt v1 command and admission schemas",
        ));
    }
    let journal = AcpJournal::new(&owner.state_dir)?;
    let mut expected_route: Option<Value> = None;
    let last_operation = Arc::new(Mutex::new(Value::Null));
    let observation_sequence = Arc::new(Mutex::new(1_u64));

    loop {
        let retained_session = journal.read_session()?;
        let retained_session_id = retained_session
            .as_ref()
            .and_then(|session| session["native_session_id"].as_str());
        let mut link = match super::connect_module_with_native_state(
            &config,
            &credential,
            &owner,
            retained_session_id,
            retained_session_id,
        )
        .await
        {
            Ok(link) => link,
            Err(error) if super::is_connect_retryable(&error) => {
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        if expected_route.as_ref().is_some_and(|route| route != &link.route) {
            return Err(Error::new("ROUTE_CHANGED", "ACP route changed while owner was live"));
        }
        expected_route = Some(link.route.clone());

        let mut reconnect_required = false;
        for (outcome, hash) in journal.pending_outcomes()? {
            module_host::validate_saved_receipt(
                &outcome,
                &owner.host.claim,
                &link.binding_id,
                link.generation,
            )?;
            match link.client.outcome(serde_json::to_value(&outcome)?).await {
                Ok(ack) => journal.acknowledge(&outcome.operation_id, &hash, ack)?,
                Err(error) if super::is_transport_uncertain(&error) => {
                    reconnect_required = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if !reconnect_required {
            for (params, hash) in journal.pending_result_pages()? {
                result_page::validate_saved(
                    &params,
                    &owner.host.claim,
                    &link.binding_id,
                    link.generation,
                )?;
                let operation_id = params["operation_id"]
                    .as_str()
                    .ok_or_else(|| invalid("saved ACP result page has no Operation ID"))?;
                if !journal.result_page_pending(operation_id, &hash)? {
                    continue;
                }
                match link.client.result(params).await {
                    Ok(ack) => journal.acknowledge_result_page(operation_id, &hash, ack)?,
                    Err(error) if super::is_transport_uncertain(&error) => {
                        reconnect_required = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if reconnect_required {
            drop(link);
            time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        let last = last_operation
            .lock()
            .map_err(|_| invalid("ACP last-operation lock is poisoned"))?
            .clone();
        let initial = acp_state(&link, &owner, &last, retained_session.as_ref(), &[]);
        let sequence = take_observation_sequence(&observation_sequence)?;
        super::send_observation(&mut link.client, &owner, sequence, initial).await?;

        loop {
            let next = match link.client.next().await {
                Ok(value) => value,
                Err(error) if super::is_transport_uncertain(&error) => break,
                Err(error) => return Err(error),
            };
            if next.get("rejected_operation_id").is_some() {
                let last = json!({"operation_id":next["rejected_operation_id"],"state":"rejected_before_native_effect"});
                *last_operation
                    .lock()
                    .map_err(|_| invalid("ACP last-operation lock is poisoned"))? = last.clone();
                let session = journal.read_session()?;
                let state = acp_state(&link, &owner, &last, session.as_ref(), &[]);
                let sequence = take_observation_sequence(&observation_sequence)?;
                super::send_observation(&mut link.client, &owner, sequence, state).await?;
                continue;
            }
            if next["command"].is_null() {
                continue;
            }
            let command: RuntimeCommand = serde_json::from_value(next["command"].clone()).map_err(|_| {
                Error::new("MODULE_COMMAND_INVALID", "ACP module command envelope is invalid")
            })?;
            super::validate_command(&command, &link)?;
            let first = command.clone();
            let mut current_session: Option<ActiveSession> = None;
            let workspace = native::route_workspace(&first).ok();
            let model = command_model(&link.route).unwrap_or_default().to_owned();
            let capture = Arc::new(Mutex::new(Capture::default()));
            let notification_capture = Arc::clone(&capture);
            let stderr_capture = Arc::clone(&capture);
            let permission_capture = Arc::clone(&capture);
            let permission_broker: PermissionBroker = Arc::new(Mutex::new(HashMap::new()));
            let callback_permissions = Arc::clone(&permission_broker);
            let permission_wakeup = Arc::new(Notify::new());
            let callback_wakeup = Arc::clone(&permission_wakeup);
            let active_operation: Arc<Mutex<Option<RuntimeCommand>>> =
                Arc::new(Mutex::new(None));
            let callback_observation_sequence = Arc::clone(&observation_sequence);
            let callback_last_operation = Arc::clone(&last_operation);
            let agent = AcpAgent::new(
                AcpAgentConfig::new(config.command.clone()).args(config.command_args.clone()),
            )
            .with_debug(move |line, direction| {
                let Ok(mut capture) = stderr_capture.lock() else { return };
                match direction {
                    LineDirection::Stderr => append_capture(&mut capture.stderr, &mut capture.stderr_truncated, line.as_bytes(), MAX_STDERR_BYTES),
                    LineDirection::Stdout | LineDirection::Stdin => {}
                }
            });
            let run = Client
                .builder()
                .name("eliot-command-acp")
                .on_receive_notification(
                    async move |notification: SessionNotification, _connection| {
                        if let Ok(bytes) = serde_json::to_vec(&notification)
                            && let Ok(mut capture) = notification_capture.lock()
                        {
                            capture.updates = capture.updates.saturating_add(1);
                            if let Some(text) = update_text(&notification.update) {
                                append_capture(&mut capture.output, &mut capture.output_truncated, text.as_bytes(), MAX_CAPTURE_BYTES);
                            }
                            if bytes.len() > MAX_CAPTURE_BYTES { capture.output_truncated = true; }
                        }
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |request: RequestPermissionRequest, responder, _connection| {
                        handle_permission_request(
                            request,
                            responder,
                            &permission_capture,
                            &callback_permissions,
                            &callback_wakeup,
                        ).await
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(agent, move |connection| {
                    let mut link = link;
                    let owner = owner.clone();
                    let config = config.clone();
                    let credential = credential.clone();
                    let journal = journal.clone();
                    let capture = capture;
                    let permission_broker = permission_broker;
                    let permission_wakeup = permission_wakeup;
                    let active_operation = active_operation;
                    let observation_sequence = observation_sequence;
                    let last_operation = last_operation;
                    let first = first;
                    let workspace = workspace;
                    let model = model;
                    async move {
                        let close_supported = initialize(&connection).await.map_err(to_acp_error)?;
                        let mut queued = Some(first);
                        loop {
                            let command = if let Some(command) = queued.take() {
                                command
                            } else {
                                let next = match link.client.next().await {
                                    Ok(value) => value,
                                    Err(_) => return Ok(()),
                                };
                                if next["command"].is_null() { continue; }
                                let command: RuntimeCommand = match serde_json::from_value(next["command"].clone()) {
                                    Ok(command) => command,
                                    Err(_) => continue,
                                };
                                command
                            };
                            *active_operation
                                .lock()
                                .map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))? =
                                Some(command.clone());
                            let outcomes = if let Err(error) = super::validate_command(&command, &link) {
                                vec![rejected(&command, &error.code)]
                            } else {
                                match process_command(
                                    &connection,
                                    &mut link,
                                    &owner,
                                    &config,
                                    &journal,
                                    &mut current_session,
                                    workspace.as_deref(),
                                    &model,
                                    close_supported,
                                    &capture,
                                    &permission_broker,
                                    &permission_wakeup,
                                    &observation_sequence,
                                    &last_operation,
                                    &command,
                                ).await {
                                    Ok(outcomes) => outcomes,
                                    Err(error) if uncertain_acp_error(&error) => {
                                        vec![unknown(&command, &error.code)]
                                    }
                                    Err(error) => vec![rejected(&command, &error.code)],
                                }
                            };
                            if link.recovery_required {
                                *active_operation.lock().map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))? = None;
                                return Ok(());
                            }
                            for mut outcome in outcomes {
                                module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim).map_err(to_acp_error)?;
                                let hash = journal.save_outcome(&outcome).map_err(to_acp_error)?;
                                let payload = serde_json::to_value(&outcome).map_err(|_| agent_client_protocol::Error::internal_error())?;
                                match link.client.outcome(payload).await {
                                    Ok(ack) => journal.acknowledge(&outcome.operation_id, &hash, ack).map_err(to_acp_error)?,
                                    Err(_) => return Ok(()),
                                }
                            }
                            let last = json!({"operation_id":command.operation_id,"method":command.method,"state":"recorded"});
                            *last_operation.lock().map_err(|_| to_acp_error(invalid("ACP last-operation lock is poisoned")))? = last.clone();
                            let session = journal.read_session().map_err(to_acp_error)?;
                            let pending = capture.lock().map(|capture| capture.pending_requests.clone()).unwrap_or_default();
                            let state = acp_state(&link, &owner, &last, session.as_ref(), &pending);
                            let sequence = take_observation_sequence(&observation_sequence).map_err(to_acp_error)?;
                            if super::send_observation(&mut link.client, &owner, sequence, state).await.is_err() {
                                return Ok(());
                            }
                            *active_operation.lock().map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))? = None;
                        }
                    }
                })
                .await;

            let snapshot = capture.lock().map(|capture| json!({
                "output_bytes":capture.output.len(),
                "output_sha256":digest(&capture.output),
                "output_truncated":capture.output_truncated,
                "stderr_bytes":capture.stderr.len(),
                "stderr_sha256":digest(&capture.stderr),
                "stderr_truncated":capture.stderr_truncated,
                "session_updates":capture.updates,
                "family_departure_claimed":false
            })).unwrap_or(Value::Null);
            if let Some(command) = active_operation
                .lock()
                .map_err(|_| invalid("ACP active-operation lock is poisoned"))?
                .take()
            {
                let output = capture
                    .lock()
                    .map_err(|_| invalid("ACP capture lock is poisoned"))?
                    .output
                    .clone();
                journal.save_output(&command.operation_id, &output)?;
                if journal.read_outcome(&command.operation_id)?.is_none() {
                    let mut outcome = unknown(&command, "acp_connection_ended_native_completion_unknown");
                    module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim)?;
                    journal.save_outcome(&outcome)?;
                }
            }
            journal.mark_session_recovery(
                if run.is_err() { "native_process_connection_lost_no_replay" } else { "module_transport_ended_no_replay" },
                &snapshot,
            )?;
            time::sleep(Duration::from_millis(250)).await;
            break;
        }
    }
}

async fn initialize(connection: &ConnectionTo<Agent>) -> std::result::Result<bool, agent_client_protocol::Error> {
    let response = connection
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await?;
    if response.protocol_version != ProtocolVersion::V1 {
        return Err(agent_client_protocol::Error::invalid_params().data("ACP v1 negotiation was not returned"));
    }
    Ok(response.agent_capabilities.session_capabilities.close.is_some())
}

#[allow(clippy::too_many_arguments)]
async fn process_command(
    connection: &ConnectionTo<Agent>,
    link: &mut Link,
    owner: &Owner,
    _config: &Config,
    journal: &AcpJournal,
    session: &mut Option<ActiveSession>,
    workspace: Option<&Path>,
    model: &str,
    capture: &Arc<Mutex<Capture>>,
    command: &RuntimeCommand,
) -> Result<Vec<RuntimeOutcome>> {
    match command.method.as_str() {
        "agent.open" | "task.dispatch" | "agent.send" => {
            if session.is_none() {
                let workspace = workspace.ok_or_else(|| Error::new("WORKSPACE_INVALID", "ACP requires the admitted absolute workspace"))?;
                *session = Some(open_session(connection, journal, command, workspace, model, owner).await?);
            }
            if command.method == "agent.open" {
                let session = session.as_ref().expect("opened above");
                return Ok(vec![RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Applied,
                    native_scope_key: Some(session.id.to_string()),
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "execution_shape":ACP_EXECUTION_SHAPE,
                        "native_session_id":session.id,
                        "requested_model":model,
                        "effective_model":session.model_id,
                        "effective_model_status":"verified_by_session_config_readback",
                        "workspace":session.workspace,
                        "family_departure_claimed":false,
                        "task_completion":"unknown"
                    }),
                }]);
            }
            let session = session.as_ref().expect("opened above");
            let (prompt, prompt_identity, admission) = if command.method == "task.dispatch" {
                let dispatch = acp_prompt::prepare(command, &owner.host.boot_id)?;
                let receipt = adapter::acp_dispatch_admission(owner, command, &dispatch)?;
                let (dir, existing) = journal.admit(command, &dispatch, Some(receipt.clone()))?;
                if existing {
                    if let Some((saved, _)) = journal.read_outcome(&command.operation_id)? {
                        return Ok(vec![saved]);
                    }
                    return Ok(vec![unknown(command, "native_prompt_may_have_started_no_replay")]);
                }
                let marker = json!({"state":"native_prompt_started","session_id":session.id,"prompt_sha256":dispatch.identity.prompt_sha256,"prompt_bytes":dispatch.identity.prompt_bytes,"turn_id":command.operation_id});
                write_json_new(&dir.join("prompt-state.json"), &marker)?;
                (dispatch.envelope.prompt.clone(), Some(dispatch), Some(receipt))
            } else {
                if command.input["delivery"] != "next_turn" {
                    return Err(Error::new("CAPABILITY_UNAVAILABLE", "ACP exposes agent.send/next_turn only; native steer is not provided"));
                }
                let text = command.input["text"].as_str().filter(|text| !text.trim().is_empty()).ok_or_else(|| Error::invalid("ACP next_turn text is missing"))?;
                (text.to_owned(), None, None)
            };
            let prompt_request = PromptRequest::new(
                session.id.clone(),
                vec![ContentBlock::Text(TextContent::new(prompt.clone()))],
            );
            let prompt_result = prompt_with_cancel(
                connection,
                link,
                owner,
                journal,
                session,
                command,
                prompt_request,
            ).await;
            let capture_snapshot = capture.lock().map_err(|_| invalid("ACP capture lock is poisoned"))?;
            let output = capture_snapshot.output.clone();
            let output_hash = journal.save_output(&command.operation_id, &output)?;
            let outcome = match prompt_result {
                Ok(response) => RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Applied,
                    native_scope_key: Some(session.id.to_string()),
                    native_root_id: None,
                    turn_id: Some(command.operation_id.clone()),
                    native_input_id: None,
                    details: json!({
                        "execution_shape":ACP_EXECUTION_SHAPE,
                        "completion_condition":"native_turn_completed",
                        "task_completion":"unknown",
                        "native_session_id":session.id,
                        "native_stop_reason":format!("{:?}",response.stop_reason),
                        "native_payload_sha256":prompt_identity.as_ref().map(|d|d.identity.prompt_sha256.as_str()),
                        "native_payload_bytes":prompt_identity.as_ref().map(|d|d.identity.prompt_bytes),
                        "output_sha256":output_hash,
                        "output_bytes":output.len(),
                        "output_truncated":capture_snapshot.output_truncated,
                        "native_stderr_bytes":capture_snapshot.stderr.len(),
                        "native_stderr_sha256":digest(&capture_snapshot.stderr),
                        "native_stderr_truncated":capture_snapshot.stderr_truncated,
                        "task_dispatch_admission":admission,
                        "family_departure_claimed":false
                    }),
                },
                Err(error) => unknown(command, &error.code),
            };
            drop(capture_snapshot);
            if let Some(dispatch) = prompt_identity {
                if outcome.outcome == EffectOutcome::Applied {
                    if let Some(saved) = journal.read_outcome(&command.operation_id)? {
                        if saved.0 != outcome {
                            return Err(Error::new("ADAPTER_EVIDENCE_CONFLICT", "ACP outcome changed after TaskPrompt admission"));
                        }
                    }
                }
                let _ = dispatch;
            }
            Ok(vec![outcome])
        }
        "agent.configure" => {
            let session = session.as_ref().ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            let settings = &command.input["settings"];
            if settings["modelId"].as_str() != Some(session.model_id.as_str()) {
                return Err(Error::new("CONFIGURATION_UNSUPPORTED", "ACP model changes require a new route and verified session readback"));
            }
            Ok(vec![applied(command, json!({"configuration":"unchanged_route_model_verified","native_session_id":session.id,"task_completion":"unknown"}))])
        }
        "agent.refresh" => Ok(vec![applied(command, session_state(session.as_ref()))]),
        "agent.reconcile" => {
            let target = command.input["operation_id"].as_str().filter(|id| !id.trim().is_empty()).ok_or_else(|| Error::invalid("reconcile target Operation is missing"))?;
            if let Some((saved, _)) = journal.read_outcome(target)? { return Ok(vec![saved]); }
            Ok(vec![unknown_target(command, target, "native_result_not_durably_known_no_replay")])
        }
        "agent.reply" => Ok(vec![rejected(command, "NO_PENDING_ACP_PERMISSION_REQUEST")]),
        "native.command.cancel_turn" => {
            let session = session.as_ref().ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            let target = command.input["target_operation_id"].as_str().unwrap_or_default();
            let turn = command.input["turn_id"].as_str().unwrap_or_default();
            let session_id = command.input["session_id"].as_str().unwrap_or_default();
            if command.input["binding_id"] != link.binding_id
                || command.input["binding_generation"].as_i64() != Some(link.generation)
                || command.input["request_id"] != command.operation_id
                || session_id != session.id.to_string()
                || target != turn
                || target.trim().is_empty()
            {
                return Err(Error::new("NATIVE_IDENTITY_MISMATCH", "ACP cancel target does not identify one exact session turn"));
            }
            connection.send_notification(CancelNotification::new(session.id.clone())).await.map_err(|_| Error::new("ACP_CANCEL_UNKNOWN", "ACP cancel notification delivery is uncertain"))?;
            Ok(vec![applied(command, json!({"native_session_id":session.id,"target_operation_id":target,"turn_id":turn,"cancel":"requested_not_completion_proof","family_departure_claimed":false}))])
        }
        "native.command.close_session" => {
            let session = session.as_ref().ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            if !session.close_supported || !owner.host.claim.capabilities.iter().any(|cap| cap.as_str()=="native.command.close_session") {
                return Err(Error::new("CAPABILITY_UNAVAILABLE", "ACP agent did not advertise session/close"));
            }
            if command.input["binding_id"] != link.binding_id
                || command.input["binding_generation"].as_i64() != Some(link.generation)
                || command.input["request_id"] != command.operation_id
                || command.input["session_id"] != session.id.to_string()
                || command.input["target_operation_id"].as_str().is_none_or(str::is_empty)
            {
                return Err(Error::new("NATIVE_IDENTITY_MISMATCH", "ACP close target differs from the active session"));
            }
            connection.send_request(CloseSessionRequest::new(session.id.clone())).block_task().await.map_err(|_| Error::new("ACP_CLOSE_UNKNOWN", "ACP session/close response was not confirmed"))?;
            *session = ActiveSession { id: session.id.clone(), workspace: session.workspace.clone(), model_id: session.model_id.clone(), close_supported: false };
            Ok(vec![applied(command, json!({"native_session_id":session.id,"native_close":"response_confirmed","family_departure_claimed":false}))])
        }
        "agent.result" => Err(Error::new("CAPABILITY_UNAVAILABLE", "ACP result page projection is not yet wired")),
        _ => Err(Error::new("CAPABILITY_UNAVAILABLE", "ACP received an unsupported runtime command")),
    }
}

async fn open_session(
    connection: &ConnectionTo<Agent>,
    journal: &AcpJournal,
    command: &RuntimeCommand,
    workspace: &Path,
    requested_model: &str,
    owner: &Owner,
) -> Result<ActiveSession> {
    if !workspace.is_absolute() || !workspace.is_dir() {
        return Err(Error::new("WORKSPACE_INVALID", "ACP workspace must be an admitted absolute directory"));
    }
    if let Some(saved) = journal.read_session()? {
        if saved["native_session_id"].as_str().is_some() {
            return Err(Error::new("RECOVERY_REQUIRED", "prior ACP session is retained for readback; a second native session will not be started"));
        }
    }
    let opened = connection
        .send_request(NewSessionRequest::new(workspace))
        .block_task()
        .await
        .map_err(|_| Error::new("ACP_SESSION_OPEN_UNKNOWN", "ACP session/new response is uncertain"))?;
    let session_id = opened.session_id;
    let mut options = opened.config_options.clone().unwrap_or_default();
    let advertised = serde_json::to_value(&options)?;
    if !model_value_available(&advertised, requested_model) {
        let _ = if owner.host.claim.capabilities.iter().any(|cap| cap.as_str()=="native.command.close_session") {
            connection.send_request(CloseSessionRequest::new(session_id.clone())).block_task().await
        } else { Ok(()) };
        return Err(Error::new("COMMAND_MODEL_UNVERIFIED", "ACP session did not advertise the exact routed model value"));
    }
    if !model_current_value_matches(&advertised, requested_model) {
        let configured = connection
            .send_request(SetSessionConfigOptionRequest::new(
                session_id.clone(),
                "model",
                SessionConfigOptionValue::value_id(requested_model),
            ))
            .block_task()
            .await
            .map_err(|_| Error::new("COMMAND_MODEL_UNVERIFIED", "ACP model configuration response is uncertain"))?;
        options = configured.config_options;
    }
    let readback = serde_json::to_value(&options)?;
    if !model_current_value_matches(&readback, requested_model) {
        return Err(Error::new("COMMAND_MODEL_UNVERIFIED", "ACP effective model did not match the requested model on readback"));
    }
    let session = ActiveSession {
        id: session_id,
        workspace: workspace.to_path_buf(),
        model_id: requested_model.to_owned(),
        close_supported: true,
    };
    journal.save_session(&json!({
        "schema_version":1,
        "artifact_id":ACP_ARTIFACT_ID,
        "binding_id":command.binding_id,
        "binding_generation":command.generation,
        "operation_id":command.operation_id,
        "native_session_id":session.id,
        "workspace":session.workspace,
        "requested_model":requested_model,
        "effective_model":requested_model,
        "model_readback":readback,
        "native_close_capability":"session/close" ,
        "family_departure_claimed":false
    }))?;
    Ok(session)
}

async fn prompt_with_cancel(
    connection: &ConnectionTo<Agent>,
    link: &mut Link,
    owner: &Owner,
    journal: &AcpJournal,
    session: &ActiveSession,
    dispatch_command: &RuntimeCommand,
    prompt: PromptRequest,
) -> Result<PromptResponse> {
    let request = connection.send_request(prompt).block_task();
    tokio::pin!(request);
    loop {
        tokio::select! {
            response = &mut request => {
                return response.map_err(|_| Error::new("ACP_PROMPT_UNKNOWN", "ACP prompt response is uncertain after native admission"));
            }
            next = link.client.next() => {
                let next = match next {
                    Ok(next) => next,
                    Err(_) => return Err(Error::new("ACP_PROMPT_UNKNOWN", "module transport failed while native prompt was active")),
                };
                if next["command"].is_null() { continue; }
                let control: RuntimeCommand = serde_json::from_value(next["command"].clone()).map_err(|_| Error::new("MODULE_COMMAND_INVALID", "ACP control envelope is malformed"))?;
                super::validate_command(&control, link)?;
                if control.method != "native.command.cancel_turn" {
                    // Preserve no command by treating an out-of-band mutation as an uncertain boundary.
                    return Err(Error::new("ACP_PROMPT_UNKNOWN", "a non-cancel command arrived during the active ACP turn"));
                }
                let target = control.input["target_operation_id"].as_str().unwrap_or_default();
                if control.input["binding_id"] != link.binding_id
                    || control.input["binding_generation"].as_i64() != Some(link.generation)
                    || control.input["request_id"] != control.operation_id
                    || control.input["session_id"] != session.id.to_string()
                    || control.input["turn_id"] != dispatch_command.operation_id
                    || target != dispatch_command.operation_id
                {
                    return Err(Error::new("NATIVE_IDENTITY_MISMATCH", "ACP cancel does not match the exact active session turn"));
                }
                connection.send_notification(CancelNotification::new(session.id.clone())).await.map_err(|_| Error::new("ACP_CANCEL_UNKNOWN", "ACP cancel delivery is uncertain"))?;
                let outcome = applied(&control, json!({
                    "native_session_id":session.id,
                    "target_operation_id":target,
                    "turn_id":control.input["turn_id"],
                    "cancel":"requested_not_completion_proof",
                    "family_departure_claimed":false
                }));
                deliver_outcome(link, owner, journal, control, outcome).await?;
            }
        }
    }
}

async fn deliver_outcome(
    link: &mut Link,
    owner: &Owner,
    journal: &AcpJournal,
    command: RuntimeCommand,
    mut outcome: RuntimeOutcome,
) -> Result<()> {
    module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim)?;
    let hash = journal.save_outcome(&outcome)?;
    let ack = link.client.outcome(serde_json::to_value(&outcome)?).await?;
    journal.acknowledge(&outcome.operation_id, &hash, ack)
}

fn model_value_available(options: &Value, requested: &str) -> bool {
    options.as_array().is_some_and(|options| options.iter().any(|option| {
        option["id"] == "model"
            && option.get("options").and_then(Value::as_array).is_some_and(|values| values.iter().any(|value| value["value"] == requested || value["value"]["id"] == requested || value["id"] == requested))
    }))
}

fn model_current_value_matches(options: &Value, requested: &str) -> bool {
    options.as_array().is_some_and(|options| options.iter().any(|option| {
        option["id"] == "model"
            && (option["currentValue"] == requested
                || option["currentValue"]["value"] == requested
                || option["currentValue"]["id"] == requested)
    }))
}

fn command_model(route: &Value) -> Option<&str> {
    route["native_options"]["modelId"].as_str().filter(|model| {
        !model.trim().is_empty() && model.trim() == *model && model.len() <= 256
    })
}

fn update_text(update: &SessionUpdate) -> Option<&str> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) | SessionUpdate::AgentThoughtChunk(chunk) => {
            match &chunk.content {
                ContentBlock::Text(text) => Some(text.text.as_str()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn append_capture(target: &mut Vec<u8>, truncated: &mut bool, bytes: &[u8], limit: usize) {
    let remaining = limit.saturating_sub(target.len());
    let copied = remaining.min(bytes.len());
    target.extend_from_slice(&bytes[..copied]);
    if copied != bytes.len() { *truncated = true; }
}

fn acp_state(link: &Link, owner: &Owner, last_operation: &Value, session: Option<&Value>) -> Value {
    json!({
        "phase":if session.is_some(){"ready"}else{"sessionless"},
        "native_root_id":Value::Null,
        "native_scope_key":session.and_then(|s|s["native_session_id"].as_str()),
        "native_session_state":if session.is_some(){"session_open"}else{"not_started"},
        "boot_id":owner.host.boot_id,
        "describe":{
            "runtime":"command",
            "module_artifact_id":owner.host.profile.artifact_id(),
            "module_artifact_version":owner.host.profile.artifact_version(),
            "contract_revision":owner.host.profile.contract_revision(),
            "execution_shape":ACP_EXECUTION_SHAPE,
            "requested_model":command_model(&link.route),
            "effective_model":session.and_then(|s|s["effective_model"].as_str()),
            "effective_model_status":if session.is_some(){"verified_by_session_config_readback"}else{"unknown"},
            "capabilities":{
                "open":"native_acp_session_new_with_model_readback",
                "task_dispatch":"exact_store_task_prompt_v1_acp_prompt",
                "reconcile":"durable_receipt_readback_no_prompt_replay",
                "refresh":"local_session_and_capture_readback",
                "send":"next_turn_only",
                "steer":"unavailable",
                "configure":"route_model_immutable_after_verified_open",
                "reply":"explicit_permission_reply_required; implicit_permission_cancelled",
                "close":"native_capability_and_close_response_required",
                "result_pages":"pending_bounded_acp_capture_projection"
            }
        },
        "binding_id":link.binding_id,
        "generation":link.generation,
        "recovery_required":link.recovery_required,
        "last_operation":last_operation,
        "family_departure_claimed":false,
        "capture_retained":true
    })
}

fn session_state(session: Option<&ActiveSession>) -> Value {
    match session {
        Some(session) => json!({"native_session_id":session.id,"workspace":session.workspace,"requested_model":session.model_id,"native_close_capability":session.close_supported,"family_departure_claimed":false}),
        None => json!({"native_session_state":"not_started","family_departure_claimed":false}),
    }
}

fn applied(command: &RuntimeCommand, details: Value) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn rejected(command: &RuntimeCommand, code: &str) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({"execution_shape":ACP_EXECUTION_SHAPE,"error_code":code,"effect":"not_applied","family_departure_claimed":false}),
    }
}

fn unknown(command: &RuntimeCommand, reason: &str) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({"execution_shape":ACP_EXECUTION_SHAPE,"completion_condition":"unknown","task_completion":"unknown","recovery_reason":reason,"native_replay":false,"family_departure_claimed":false,"capture_retained":true}),
    }
}

fn unknown_target(command: &RuntimeCommand, target: &str, reason: &str) -> RuntimeOutcome {
    let mut outcome = unknown(command, reason);
    outcome.operation_id = target.to_owned();
    outcome
}

fn next_sequence(current: u64) -> Result<u64> {
    current.checked_add(1).ok_or_else(|| Error::new("MODULE_OBSERVATION_INVALID", "observation sequence overflowed"))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES {
        return Err(invalid("ACP journal record is not a bounded regular file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(MAX_RECORD_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES { return Err(invalid("ACP journal record exceeded its read bound")); }
    serde_json::from_slice(&bytes).map_err(|_| invalid("ACP journal record is malformed"))
}

fn read_bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(invalid("ACP capture is not a bounded regular file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum { return Err(invalid("ACP capture exceeded its read bound")); }
    Ok(bytes)
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES { return Err(invalid("ACP journal record exceeds its write bound")); }
    write_private_new(path, &bytes)
}

fn to_acp_error(error: Error) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(error.code)
}

fn invalid(message: &'static str) -> Error {
    Error::new("ADAPTER_EVIDENCE_INVALID", message)
}
