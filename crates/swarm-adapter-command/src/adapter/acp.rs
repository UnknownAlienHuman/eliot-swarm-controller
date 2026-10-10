//! Native ACP session path for the separately registered Command ACP artifact.
//!
//! Only Store's versioned TaskPrompt envelope is admitted for task dispatch.
//! The journal is immutable at the native-effect boundary: an uncertain prompt
//! is read back as Unknown and is never sent a second time.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::{
    Agent, Client, ConnectionTo, Lines,
    schema::{ProtocolVersion, v1::*},
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_contracts::{
    EffectOutcome, RuntimeCommand, RuntimeOutcome,
    error::{Error, Result},
    runtime::TaskDispatchAdmissionReceipt,
};
use swarm_process::{private_permissions, replace_private_durable, write_private_new};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{Notify, oneshot},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use super::{Config, Link, Owner, is_sha256};
use crate::{
    ACP_ARTIFACT_ID, ACP_EXECUTION_SHAPE, Profile,
    acp_prompt::{self, PreparedAcpDispatch},
    adapter,
    journal::digest,
    module_host, native, result_page,
};

const MAX_RECORD_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CAPTURE_BYTES: usize = 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_PERMISSION_BYTES: usize = 64 * 1024;
const MAX_ACP_FRAME_BYTES: usize = 1024 * 1024;
const CHILD_WAIT_GRACE: Duration = Duration::from_secs(2);
const CAPTURE_DRAIN_GRACE: Duration = Duration::from_secs(2);
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
    output_total_bytes: u64,
    output_truncated: bool,
    output_complete: bool,
    output_receipt_saved: bool,
    stop_reason: Option<String>,
    stderr: Vec<u8>,
    stderr_total_bytes: u64,
    stderr_truncated: bool,
    stderr_error: Option<String>,
    protocol_error: Option<String>,
    updates: u64,
    history_updates: u64,
    history_replaying: bool,
    tool_events: Vec<Value>,
    agent_children: Vec<Value>,
    pending_requests: Vec<Value>,
    native_session_id: Option<String>,
    native_child_identity: Option<Value>,
    native_child_exit_code: Option<i32>,
}

impl Capture {
    fn begin_turn(&mut self) {
        self.output.clear();
        self.output_total_bytes = 0;
        self.output_truncated = false;
        self.output_complete = false;
        self.output_receipt_saved = false;
        self.stop_reason = None;
        self.protocol_error = None;
        self.updates = 0;
        self.tool_events.clear();
        self.agent_children.clear();
    }
}

struct PendingPermission {
    session_id: String,
    request_id: Value,
    fingerprint: String,
    options: Vec<(String, PermissionOptionKind, String)>,
    reply: oneshot::Sender<RequestPermissionOutcome>,
    acknowledgement: oneshot::Receiver<bool>,
}

type PermissionBroker = Arc<Mutex<HashMap<String, PendingPermission>>>;

#[derive(Debug, Clone)]
struct ActiveSession {
    id: SessionId,
    owner_operation_id: String,
    workspace: PathBuf,
    model_id: String,
    close_supported: bool,
    closed: bool,
}

struct CommandContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    link: &'a mut Link,
    owner: &'a Owner,
    journal: &'a AcpJournal,
    features: &'a AgentFeatures,
    session: &'a mut Option<ActiveSession>,
    workspace: Option<&'a Path>,
    model: &'a str,
    capture: &'a Arc<Mutex<Capture>>,
    permissions: &'a PermissionBroker,
    permission_wakeup: &'a Notify,
    observation_sequence: &'a Arc<Mutex<u64>>,
}

struct SessionRestoreContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    journal: &'a AcpJournal,
    link: &'a Link,
    owner: &'a Owner,
    features: &'a AgentFeatures,
    capture: &'a Arc<Mutex<Capture>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedOutputCapture {
    schema_version: u8,
    operation_id: String,
    session_id: Option<String>,
    total_bytes: u64,
    stored_bytes: u64,
    stored_sha256: String,
    truncated: bool,
    complete: bool,
    stop_reason: Option<String>,
    protocol_error: Option<String>,
}

#[derive(Debug, Clone)]
struct AgentFeatures {
    close_supported: bool,
    load_session_supported: bool,
    agent_info: Option<Value>,
    capabilities: Value,
}

#[derive(Clone)]
struct PermissionHandlerState {
    capture: Arc<Mutex<Capture>>,
    permissions: PermissionBroker,
    wakeup: Arc<Notify>,
    active_operation: Arc<Mutex<Option<RuntimeCommand>>>,
    journal: AcpJournal,
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
        let path = self
            .root
            .join(format!("op-{}", digest(operation_id.as_bytes())));
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

    fn record_command(&self, command: &RuntimeCommand) -> Result<()> {
        let directory = self.operation_dir(&command.operation_id)?;
        let path = directory.join("command.json");
        let bytes = serde_json::to_vec(command)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("ACP command receipt exceeds its size bound"));
        }
        if path.exists() {
            if read_bytes(&path, MAX_RECORD_BYTES as usize)? != bytes {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "a different module command was already retained for this Operation",
                ));
            }
        } else {
            write_private_new(&path, &bytes)?;
        }
        Ok(())
    }

    fn record_effect(
        &self,
        command: &RuntimeCommand,
        effect_id: &str,
        phase: &str,
        evidence: Value,
    ) -> Result<()> {
        if effect_id.trim().is_empty() || effect_id.len() > 256 {
            return Err(invalid("ACP effect identity is outside its bound"));
        }
        if !matches!(phase, "intent" | "receipt") {
            return Err(invalid("ACP effect phase is unsupported"));
        }
        let directory = self.operation_dir(&command.operation_id)?;
        let path = directory.join(format!(
            "effect-{phase}-{}.json",
            digest(effect_id.as_bytes())
        ));
        let record = json!({
            "schema_version":1,
            "operation_id":command.operation_id,
            "effect_id":effect_id,
            "phase":phase,
            "evidence":evidence
        });
        let bytes = serde_json::to_vec(&record)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("ACP effect evidence exceeds its size bound"));
        }
        if path.exists() {
            if read_bytes(&path, MAX_RECORD_BYTES as usize)? != bytes {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "ACP effect evidence changed after it was retained",
                ));
            }
        } else {
            write_private_new(&path, &bytes)?;
        }
        Ok(())
    }

    fn pending_commands(&self) -> Result<Vec<RuntimeCommand>> {
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
            if !metadata.is_dir() {
                if path.file_name().and_then(|name| name.to_str()) == Some("session.json") {
                    continue;
                }
                return Err(invalid(
                    "unexpected non-directory entry in ACP evidence root",
                ));
            }
            let command_path = path.join("command.json");
            if !command_path.exists() || path.join("outcome.json").exists() {
                continue;
            }
            let command: RuntimeCommand = read_json(&command_path)?;
            if path.file_name().and_then(|name| name.to_str())
                != Some(format!("op-{}", digest(command.operation_id.as_bytes())).as_str())
            {
                return Err(invalid(
                    "retained ACP command is stored under another Operation",
                ));
            }
            pending.push(command);
        }
        Ok(pending)
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
            if saved.schema_version != candidate.schema_version
                || saved.artifact_id != candidate.artifact_id
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
        // The consumed module command is durable before dispatch admission.
        // It authorizes no native prompt by itself; all other evidence without
        // the admission marker remains an uncertain native boundary.
        let command_path = directory.join("command.json");
        if read_bytes(&command_path, MAX_RECORD_BYTES as usize)? != serde_json::to_vec(command)? {
            return Err(invalid(
                "ACP admission has no exact retained module command",
            ));
        }
        for entry in fs::read_dir(&directory)? {
            if entry?.path() != command_path {
                return Err(invalid(
                    "ACP effect evidence exists without its admission marker",
                ));
            }
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

    fn save_output_capture(&self, receipt: &SavedOutputCapture) -> Result<()> {
        if receipt.schema_version != 1
            || receipt.operation_id.trim().is_empty()
            || receipt
                .session_id
                .as_deref()
                .is_some_and(|session_id| session_id.trim().is_empty())
            || receipt.stored_bytes > MAX_CAPTURE_BYTES as u64
            || !is_sha256(&receipt.stored_sha256)
            || (receipt.complete
                && (receipt.session_id.is_none()
                    || receipt.truncated
                    || receipt.total_bytes != receipt.stored_bytes
                    || receipt.stop_reason.is_none()
                    || receipt.protocol_error.is_some()))
        {
            return Err(invalid("ACP output capture receipt is inconsistent"));
        }
        let path = self
            .operation_dir(&receipt.operation_id)?
            .join("output-capture.json");
        if path.exists() {
            let prior: SavedOutputCapture = read_json(&path)?;
            if prior != *receipt {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "ACP output completeness evidence changed after it was retained",
                ));
            }
        } else {
            write_json_new(&path, receipt)?;
        }
        Ok(())
    }

    fn output_capture_exists(&self, operation_id: &str) -> Result<bool> {
        let path = self
            .operation_dir(operation_id)?
            .join("output-capture.json");
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                Err(invalid("ACP output capture receipt is not a regular file"))
            }
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn read_output_capture(&self, operation_id: &str) -> Result<(Vec<u8>, SavedOutputCapture)> {
        let directory = self.operation_dir(operation_id)?;
        let bytes = read_bytes(&directory.join("output.txt"), MAX_CAPTURE_BYTES)?;
        let receipt: SavedOutputCapture = read_json(&directory.join("output-capture.json"))?;
        if receipt.schema_version != 1
            || receipt.operation_id != operation_id
            || receipt.stored_bytes != bytes.len() as u64
            || receipt.stored_sha256 != digest(&bytes)
            || (receipt.complete
                && (receipt.session_id.is_none()
                    || receipt.truncated
                    || receipt.total_bytes != receipt.stored_bytes
                    || receipt.stop_reason.is_none()
                    || receipt.protocol_error.is_some()))
        {
            return Err(invalid(
                "ACP output bytes differ from their completeness receipt",
            ));
        }
        Ok((bytes, receipt))
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
            return Err(invalid(
                "saved ACP result page failed its identity or digest check",
            ));
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
            return Err(invalid(
                "ACP result acknowledgement differs from saved page",
            ));
        }
        let path = self
            .operation_dir(operation_id)?
            .join("result-page-ack.json");
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
                return Err(invalid(
                    "saved ACP result acknowledgement conflicts with page",
                ));
            }
            return Ok(());
        }
        write_json_new(&path, &record)
    }

    fn result_page_pending(&self, operation_id: &str, hash: &str) -> Result<bool> {
        let path = self
            .operation_dir(operation_id)?
            .join("result-page-ack.json");
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
                return Err(invalid(
                    "unexpected non-directory entry in ACP evidence root",
                ));
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
        let saved = json!({
            "schema_version":1,
            "operation_id":outcome.operation_id,
            "outcome_sha256":hash,
            "outcome":outcome
        });
        if path.exists() {
            let prior: SavedOutcome = read_json(&path)?;
            if prior.operation_id != outcome.operation_id
                || prior.outcome_sha256 != hash
                || serde_json::to_vec(&prior.outcome)? != canonical
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
            return Err(invalid(
                "saved ACP outcome failed exact identity validation",
            ));
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
                return Err(invalid(
                    "ACP outcome acknowledgement conflicts with receipt",
                ));
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

pub(super) async fn run(
    config: Config,
    owner: Owner,
    credential: swarm_contracts::Credential,
) -> Result<()> {
    if owner.host.profile != Profile::AcpV1
        || !module_host::task_prompt_v1_enabled(&owner.host.claim)
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "ACP requires the registered TaskPrompt v1 command and admission schemas",
        ));
    }
    let journal = AcpJournal::new(&owner.state_dir)?;
    let saved_session = journal.read_session()?;
    for command in journal.pending_commands()? {
        let mut outcome = unknown(
            &command,
            "adapter_restarted_after_module_command_consumed_without_terminal_receipt",
        );
        attach_saved_session_identity(&mut outcome, &command, saved_session.as_ref());
        module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim)?;
        journal.save_outcome(&outcome)?;
    }
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
        if expected_route
            .as_ref()
            .is_some_and(|route| route != &link.route)
        {
            return Err(Error::new(
                "ROUTE_CHANGED",
                "ACP route changed while owner was live",
            ));
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
                    .ok_or_else(|| invalid("saved ACP result page has no Operation ID"))?
                    .to_owned();
                if !journal.result_page_pending(&operation_id, &hash)? {
                    continue;
                }
                match link.client.result(params).await {
                    Ok(ack) => journal.acknowledge_result_page(&operation_id, &hash, ack)?,
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
        let initial = acp_state(&link, &owner, &last, retained_session.as_ref(), &[], None);
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
                let state = acp_state(&link, &owner, &last, session.as_ref(), &[], None);
                let sequence = take_observation_sequence(&observation_sequence)?;
                super::send_observation(&mut link.client, &owner, sequence, state).await?;
                continue;
            }
            if next["command"].is_null() {
                continue;
            }
            let command: RuntimeCommand =
                serde_json::from_value(next["command"].clone()).map_err(|_| {
                    Error::new(
                        "MODULE_COMMAND_INVALID",
                        "ACP module command envelope is invalid",
                    )
                })?;
            journal.record_command(&command)?;
            if let Err(error) = super::validate_command(&command, &link) {
                let mut outcome = rejected(&command, &error.code);
                module_host::attach_receipt_identity(&mut outcome, &command, &owner.host.claim)?;
                let hash = journal.save_outcome(&outcome)?;
                let ack = link.client.outcome(serde_json::to_value(&outcome)?).await?;
                journal.acknowledge(&outcome.operation_id, &hash, ack)?;
                continue;
            }
            let first = command.clone();
            let mut current_session: Option<ActiveSession> = None;
            let workspace = native::route_workspace(&first).ok();
            let model = command_model(&link.route).unwrap_or_default().to_owned();
            let capture = Arc::new(Mutex::new(Capture::default()));
            let notification_capture = Arc::clone(&capture);
            let permission_broker: PermissionBroker = Arc::new(Mutex::new(HashMap::new()));
            let permission_wakeup = Arc::new(Notify::new());
            let active_operation: Arc<Mutex<Option<RuntimeCommand>>> = Arc::new(Mutex::new(None));
            *active_operation
                .lock()
                .map_err(|_| invalid("ACP active-operation lock is poisoned"))? =
                Some(first.clone());
            let permission_handler = PermissionHandlerState {
                capture: Arc::clone(&capture),
                permissions: Arc::clone(&permission_broker),
                wakeup: Arc::clone(&permission_wakeup),
                active_operation: Arc::clone(&active_operation),
                journal: journal.clone(),
            };
            let mut native_command = Command::new(&config.command);
            native_command
                .args(&config.command_args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            let mut native_child = native_command
                .spawn()
                .map_err(|error| Error::new("ACP_PROCESS_START_FAILED", error.to_string()))?;
            let native_pid = native_child.id().ok_or_else(|| {
                Error::new("ACP_PROCESS_ID_MISSING", "spawned ACP child has no PID")
            })?;
            let native_identity = swarm_process::process_image_identity(native_pid)?;
            if !swarm_process::module_child_belongs_to_owner(&owner.record, &native_identity)? {
                let _ = stop_direct_child(&mut native_child).await;
                return Err(Error::new(
                    "ACP_PROCESS_OWNER_MISMATCH",
                    "spawned ACP child is not a current member of this exact module owner",
                ));
            }
            if let Ok(mut capture) = capture.lock() {
                capture.native_child_identity = Some(native_identity);
            }
            let child_stdin = native_child.stdin.take().ok_or_else(|| {
                Error::new("ACP_PROCESS_PIPE_MISSING", "ACP stdin pipe is unavailable")
            })?;
            let child_stdout = native_child.stdout.take().ok_or_else(|| {
                Error::new("ACP_PROCESS_PIPE_MISSING", "ACP stdout pipe is unavailable")
            })?;
            let child_stderr = native_child.stderr.take().ok_or_else(|| {
                Error::new("ACP_PROCESS_PIPE_MISSING", "ACP stderr pipe is unavailable")
            })?;
            let outgoing = SinkExt::<String>::sink_map_err(
                FramedWrite::new(
                    child_stdin,
                    LinesCodec::new_with_max_length(MAX_ACP_FRAME_BYTES),
                ),
                |error| std::io::Error::other(error.to_string()),
            );
            let incoming = FramedRead::new(
                child_stdout,
                LinesCodec::new_with_max_length(MAX_ACP_FRAME_BYTES),
            )
            .map(|result| result.map_err(|error| std::io::Error::other(error.to_string())));
            let transport = Lines::new(outgoing, incoming);
            let stderr_task = tokio::spawn(drain_native_stderr(child_stderr, Arc::clone(&capture)));
            let connection_capture = Arc::clone(&capture);
            let connection_active_operation = Arc::clone(&active_operation);
            let connection_journal = journal.clone();
            let connection_owner = owner.clone();
            let run = Client
                .builder()
                .name("eliot-command-acp")
                .on_receive_notification(
                    async move |notification: SessionNotification, _connection| {
                        if let Ok(bytes) = serde_json::to_vec(&notification)
                            && let Ok(mut capture) = notification_capture.lock()
                        {
                            let capture = &mut *capture;
                            if capture.native_session_id.as_deref()
                                != Some(notification.session_id.to_string().as_str())
                            {
                                capture.protocol_error = Some(
                                    "ACP update named a different session".to_owned(),
                                );
                                return Ok(());
                            }
                            capture.updates = capture.updates.saturating_add(1);
                            if capture.history_replaying {
                                capture.history_updates = capture.history_updates.saturating_add(1);
                            } else if let Some(text) = update_text(&notification.update) {
                                append_capture(
                                    &mut capture.output,
                                    &mut capture.output_total_bytes,
                                    &mut capture.output_truncated,
                                    text.as_bytes(),
                                    MAX_CAPTURE_BYTES,
                                );
                            }
                            if let Some(event) = compact_tool_event(&notification.update) {
                                retain_bounded_event(&mut capture.tool_events, event);
                                if let Some(child) = native_agent_child(&notification.update) {
                                    retain_bounded_event(&mut capture.agent_children, child);
                                }
                            }
                            if bytes.len() > MAX_CAPTURE_BYTES {
                                capture.protocol_error = Some("ACP notification exceeded capture bound".to_owned());
                            }
                        }
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |request: RequestPermissionRequest, responder, _connection| {
                        permission_handler.handle(request, responder).await
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(transport, move |connection| {
                    let mut link = link;
                    let owner = connection_owner;
                    let journal = connection_journal;
                    let capture = connection_capture;
                    let permission_broker = permission_broker;
                    let permission_wakeup = permission_wakeup;
                    let active_operation = connection_active_operation;
                    let observation_sequence = observation_sequence;
                    let last_operation = last_operation;
                    let first = first;
                    let workspace = workspace;
                    let model = model;
                    async move {
                        let features = initialize(&connection).await?;
                        if let Some(saved) = journal.read_session().map_err(to_acp_error)? {
                            current_session = Some(
                                restore_session(
                                    SessionRestoreContext {
                                        connection: &connection,
                                        journal: &journal,
                                        link: &link,
                                        owner: &owner,
                                        features: &features,
                                        capture: &capture,
                                    },
                                    &first,
                                    saved,
                                )
                                .await
                                .map_err(to_acp_error)?,
                            );
                        }
                        let mut queued = Some(first);
                        loop {
                            let command = if let Some(command) = queued.take() {
                                command
                            } else {
                                let next = match link.client.next().await {
                                    Ok(value) => value,
                                    Err(error) => {
                                        return Err(to_acp_error(Error::new(
                                            "ACP_MODULE_LINK_LOST",
                                            error.to_string(),
                                        )));
                                    }
                                };
                                if next["command"].is_null() { continue; }
                                let command: RuntimeCommand = match serde_json::from_value(next["command"].clone()) {
                                    Ok(command) => command,
                                    Err(_) => return Err(agent_client_protocol::Error::invalid_params().data("module.next returned an invalid ACP command")),
                                };
                                journal.record_command(&command).map_err(to_acp_error)?;
                                command
                            };
                            *active_operation
                                .lock()
                                .map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))? =
                                Some(command.clone());
                            let (outcomes, uncertain) = {
                                let mut context = CommandContext {
                                    connection: &connection,
                                    link: &mut link,
                                    owner: &owner,
                                    journal: &journal,
                                    features: &features,
                                    session: &mut current_session,
                                    workspace: workspace.as_deref(),
                                    model: &model,
                                    capture: &capture,
                                    permissions: &permission_broker,
                                    permission_wakeup: &permission_wakeup,
                                    observation_sequence: &observation_sequence,
                                };
                                if let Err(error) = super::validate_command(&command, context.link) {
                                    let outcome = context.session.as_ref().map_or_else(
                                        || rejected(&command, &error.code),
                                        |session| rejected_in_session(
                                            &command,
                                            session,
                                            &error.code,
                                            Value::Null,
                                        ),
                                    );
                                    (vec![outcome], false)
                                } else {
                                    match process_command(&mut context, &command).await {
                                        Ok(outcomes) => (outcomes, false),
                                        Err(error) if uncertain_acp_error(&error) => {
                                            let outcome = context.session.as_ref().map_or_else(
                                                || unknown(&command, &error.code),
                                                |session| unknown_in_session(
                                                    &command,
                                                    session,
                                                    &error.code,
                                                ),
                                            );
                                            (vec![outcome], true)
                                        }
                                        Err(error) => {
                                            let outcome = context.session.as_ref().map_or_else(
                                                || rejected(&command, &error.code),
                                                |session| rejected_in_session(
                                                    &command,
                                                    session,
                                                    &error.code,
                                                    Value::Null,
                                                ),
                                            );
                                            (vec![outcome], false)
                                        }
                                    }
                                }
                            };
                            if uncertain {
                                link.recovery_required = true;
                            }
                            let saved_session = journal.read_session().map_err(to_acp_error)?;
                            for mut outcome in outcomes {
                                attach_saved_session_identity(
                                    &mut outcome,
                                    &command,
                                    saved_session.as_ref(),
                                );
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
                            let state = {
                                let capture_state = capture
                                    .lock()
                                    .map_err(|_| to_acp_error(invalid("ACP capture lock is poisoned")))?;
                                acp_state(
                                    &link,
                                    &owner,
                                    &last,
                                    session.as_ref(),
                                    &capture_state.pending_requests,
                                    Some(&capture_state),
                                )
                            };
                            let sequence = take_observation_sequence(&observation_sequence).map_err(to_acp_error)?;
                            if super::send_observation(&mut link.client, &owner, sequence, state).await.is_err() {
                                return Ok(());
                            }
                            *active_operation.lock().map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))? = None;
                            if link.recovery_required {
                                return Ok(());
                            }
                        }
                    }
                })
                .await;

            let child_exit = stop_direct_child(&mut native_child).await;
            if let Ok(mut capture) = capture.lock() {
                capture.native_child_exit_code = child_exit.exit_code;
            }
            let mut stderr_task = stderr_task;
            match time::timeout(CAPTURE_DRAIN_GRACE, &mut stderr_task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    if let Ok(mut capture) = capture.lock() {
                        capture.stderr_error = Some("native stderr drain task failed".to_owned());
                    }
                }
                Err(_) => {
                    stderr_task.abort();
                    if let Ok(mut capture) = capture.lock() {
                        capture.stderr_error =
                            Some("native stderr drain exceeded its bounded deadline".to_owned());
                    }
                }
            }
            let snapshot = capture
                .lock()
                .map(|capture| {
                    json!({
                            "output_bytes":capture.output.len(),
                            "output_total_bytes":capture.output_total_bytes,
                            "output_sha256":digest(&capture.output),
                            "output_truncated":capture.output_truncated,
                            "output_complete":capture.output_complete,
                    "output_receipt_saved":capture.output_receipt_saved,
                            "output_stop_reason":capture.stop_reason,
                            "stderr_bytes":capture.stderr.len(),
                            "stderr_total_bytes":capture.stderr_total_bytes,
                            "stderr_sha256":digest(&capture.stderr),
                            "stderr_truncated":capture.stderr_truncated,
                            "stderr_error":capture.stderr_error,
                            "protocol_error":capture.protocol_error,
                            "session_updates":capture.updates,
                            "native_session_id":capture.native_session_id,
                            "native_child_identity":capture.native_child_identity,
                            "direct_child_exit_observed":child_exit.observed,
                            "direct_child_exit_code":child_exit.exit_code,
                            "direct_child_kill_requested":child_exit.kill_requested,
                            "cleanup_pending":child_exit.cleanup_pending,
                            "family_departure_claimed":false,
                            "owner_family_departure_required":true
                        })
                })
                .unwrap_or(Value::Null);
            if let Some(command) = active_operation
                .lock()
                .map_err(|_| invalid("ACP active-operation lock is poisoned"))?
                .take()
            {
                if matches!(command.method.as_str(), "task.dispatch" | "agent.send") {
                    let (output, total_bytes, truncated, session_id, stop_reason, protocol_error) = {
                        let capture = capture
                            .lock()
                            .map_err(|_| invalid("ACP capture lock is poisoned"))?;
                        (
                            capture.output.clone(),
                            capture.output_total_bytes,
                            capture.output_truncated,
                            capture.native_session_id.clone(),
                            capture.stop_reason.clone(),
                            capture.protocol_error.clone(),
                        )
                    };
                    journal.save_output(&command.operation_id, &output)?;
                    if !journal.output_capture_exists(&command.operation_id)? {
                        let session_id = match session_id {
                            Some(session_id) => Some(session_id),
                            None => journal.read_session()?.and_then(|saved| {
                                saved["native_session_id"].as_str().map(str::to_owned)
                            }),
                        };
                        journal.save_output_capture(&SavedOutputCapture {
                            schema_version: 1,
                            operation_id: command.operation_id.clone(),
                            session_id,
                            total_bytes,
                            stored_bytes: output.len() as u64,
                            stored_sha256: digest(&output),
                            truncated,
                            complete: false,
                            stop_reason,
                            protocol_error,
                        })?;
                    }
                    journal.read_output_capture(&command.operation_id)?;
                    capture
                        .lock()
                        .map_err(|_| invalid("ACP capture lock is poisoned"))?
                        .output_receipt_saved = true;
                }
                if journal.read_outcome(&command.operation_id)?.is_none() {
                    let mut outcome =
                        unknown(&command, "acp_connection_ended_native_completion_unknown");
                    let saved_session = journal.read_session()?;
                    attach_saved_session_identity(&mut outcome, &command, saved_session.as_ref());
                    module_host::attach_receipt_identity(
                        &mut outcome,
                        &command,
                        &owner.host.claim,
                    )?;
                    journal.save_outcome(&outcome)?;
                }
            }
            journal.mark_session_recovery(
                if run.is_err() {
                    "native_process_connection_lost_no_replay"
                } else {
                    "module_transport_ended_no_replay"
                },
                &snapshot,
            )?;
            return Err(Error::new(
                "ACP_OWNER_CLEANUP_PENDING",
                if child_exit.cleanup_pending {
                    "ACP transport ended; exact child stop is unconfirmed and owner family departure remains required"
                } else {
                    "ACP transport ended; exact child was stopped, but only the owner can prove family departure"
                },
            ));
        }
    }
}

async fn initialize(
    connection: &ConnectionTo<Agent>,
) -> std::result::Result<AgentFeatures, agent_client_protocol::Error> {
    let response = connection
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await?;
    if response.protocol_version != ProtocolVersion::V1 {
        return Err(agent_client_protocol::Error::invalid_params()
            .data("ACP v1 negotiation was not returned"));
    }
    let capabilities = serde_json::to_value(&response.agent_capabilities)
        .map_err(|_| agent_client_protocol::Error::internal_error())?;
    let agent_info = response
        .agent_info
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| agent_client_protocol::Error::internal_error())?;
    Ok(AgentFeatures {
        close_supported: response
            .agent_capabilities
            .session_capabilities
            .close
            .is_some(),
        load_session_supported: response.agent_capabilities.load_session,
        agent_info,
        capabilities,
    })
}

struct DirectChildExit {
    observed: bool,
    exit_code: Option<i32>,
    kill_requested: bool,
    cleanup_pending: bool,
}

async fn stop_direct_child(child: &mut Child) -> DirectChildExit {
    let mut kill_requested = false;
    let status = match child.try_wait() {
        Ok(Some(status)) => Some(status),
        Ok(None) | Err(_) => {
            kill_requested = true;
            let _ = child.start_kill();
            match time::timeout(CHILD_WAIT_GRACE, child.wait()).await {
                Ok(Ok(status)) => Some(status),
                Ok(Err(_)) | Err(_) => None,
            }
        }
    };
    DirectChildExit {
        observed: status.is_some(),
        exit_code: status.and_then(|status| status.code()),
        kill_requested,
        cleanup_pending: status.is_none(),
    }
}

async fn drain_native_stderr<R>(mut reader: R, capture: Arc<Mutex<Capture>>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => return,
            Ok(read) => {
                if let Ok(mut capture) = capture.lock() {
                    let capture = &mut *capture;
                    append_capture(
                        &mut capture.stderr,
                        &mut capture.stderr_total_bytes,
                        &mut capture.stderr_truncated,
                        &buffer[..read],
                        MAX_STDERR_BYTES,
                    );
                }
            }
            Err(error) => {
                if let Ok(mut capture) = capture.lock() {
                    capture.stderr_error = Some(error.to_string().chars().take(256).collect());
                }
                return;
            }
        }
    }
}

async fn process_command(
    context: &mut CommandContext<'_>,
    command: &RuntimeCommand,
) -> Result<Vec<RuntimeOutcome>> {
    match command.method.as_str() {
        "agent.open" | "task.dispatch" | "agent.send" => {
            // Validate and admit the Store TaskPrompt before session creation
            // or any other native effect. A retained admission never replays.
            let prepared_dispatch = if command.method == "task.dispatch" {
                let dispatch = acp_prompt::prepare(command)?;
                let receipt = adapter::acp_dispatch_admission(context.owner, command, &dispatch)?;
                let (directory, existing) =
                    context
                        .journal
                        .admit(command, &dispatch, Some(receipt.clone()))?;
                if existing {
                    if let Some((saved, _)) = context.journal.read_outcome(&command.operation_id)? {
                        return Ok(vec![saved]);
                    }
                    let outcome = context.session.as_ref().map_or_else(
                        || unknown(command, "native_prompt_may_have_started_no_replay"),
                        |session| {
                            unknown_in_session(
                                command,
                                session,
                                "native_prompt_may_have_started_no_replay",
                            )
                        },
                    );
                    return Ok(vec![outcome]);
                }
                Some((dispatch, receipt, directory))
            } else {
                None
            };
            if context.session.is_none() {
                let workspace = context.workspace.ok_or_else(|| {
                    Error::new(
                        "WORKSPACE_INVALID",
                        "ACP requires the admitted absolute workspace",
                    )
                })?;
                *context.session = Some(
                    open_session(
                        context.connection,
                        context.journal,
                        command,
                        workspace,
                        context.model,
                        context.owner,
                        context.features,
                    )
                    .await?,
                );
                if let (Some(session), Ok(mut capture)) =
                    (context.session.as_ref(), context.capture.lock())
                {
                    capture.native_session_id = Some(session.id.to_string());
                }
            }
            let session = context.session.as_ref().ok_or_else(|| {
                Error::new("NATIVE_SESSION_MISSING", "ACP session was not opened")
            })?;
            if session.closed {
                return Err(Error::new(
                    "NATIVE_SESSION_CLOSED",
                    "the exact ACP session was already closed",
                ));
            }
            if command.method == "agent.open" {
                return Ok(vec![applied_in_session(
                    command,
                    session,
                    json!({
                        "execution_shape":ACP_EXECUTION_SHAPE,
                        "native_session_id":session.id,
                        "requested_model":context.model,
                        "effective_model":session.model_id,
                        "effective_model_status":"verified_by_session_config_readback",
                        "workspace":session.workspace,
                        "family_departure_claimed":false,
                        "task_completion":"unknown"
                    }),
                )]);
            }

            let (prompt, dispatch, admission) =
                if let Some((dispatch, receipt, directory)) = prepared_dispatch {
                    context.journal.record_effect(
                        command,
                        "session/prompt",
                        "intent",
                        json!({
                            "native_session_id":session.id,
                            "prompt_sha256":dispatch.identity.prompt_sha256,
                            "prompt_bytes":dispatch.identity.prompt_bytes,
                            "turn_id":command.operation_id
                        }),
                    )?;
                    let marker = json!({
                        "state":"native_prompt_started",
                        "session_id":session.id,
                        "prompt_sha256":dispatch.identity.prompt_sha256,
                        "prompt_bytes":dispatch.identity.prompt_bytes,
                        "turn_id":command.operation_id
                    });
                    write_json_new(&directory.join("prompt-state.json"), &marker)?;
                    (
                        dispatch.envelope.prompt.clone(),
                        Some(dispatch),
                        Some(receipt),
                    )
                } else {
                    if command.input["delivery"] != "next_turn" {
                        return Err(Error::new(
                            "CAPABILITY_UNAVAILABLE",
                            "ACP exposes agent.send/next_turn only; native steer is not provided",
                        ));
                    }
                    let text = command.input["text"]
                        .as_str()
                        .filter(|text| !text.trim().is_empty())
                        .ok_or_else(|| Error::invalid("ACP next_turn text is missing"))?;
                    context.journal.record_effect(
                        command,
                        "session/prompt",
                        "intent",
                        json!({
                            "native_session_id":session.id,
                            "prompt_sha256":digest(text.as_bytes()),
                            "prompt_bytes":text.len(),
                            "turn_id":command.operation_id,
                            "delivery":"next_turn"
                        }),
                    )?;
                    (text.to_owned(), None, None)
                };

            context
                .capture
                .lock()
                .map_err(|_| invalid("ACP capture lock is poisoned"))?
                .begin_turn();
            let session_id = session.id.clone();
            let prompt_request = PromptRequest::new(
                session_id,
                vec![ContentBlock::Text(TextContent::new(prompt.clone()))],
            );
            let prompt_result = prompt_with_cancel(context, command, prompt_request).await;
            let observed_stop_reason = prompt_result
                .as_ref()
                .ok()
                .map(|response| format!("{:?}", response.stop_reason));
            let (
                output,
                output_total_bytes,
                output_truncated,
                output_complete,
                stderr_len,
                stderr_total_bytes,
                stderr_hash,
                stderr_truncated,
                stderr_error,
                protocol_error,
                updates,
            ) = {
                let capture = context.capture.lock().map_err(|_| {
                    Error::new(
                        "ACP_PROMPT_UNKNOWN",
                        "ACP capture state became unavailable after the prompt request",
                    )
                })?;
                (
                    capture.output.clone(),
                    capture.output_total_bytes,
                    capture.output_truncated,
                    observed_stop_reason.is_some()
                        && !capture.output_truncated
                        && capture.protocol_error.is_none(),
                    capture.stderr.len(),
                    capture.stderr_total_bytes,
                    digest(&capture.stderr),
                    capture.stderr_truncated,
                    capture.stderr_error.clone(),
                    capture.protocol_error.clone(),
                    capture.updates,
                )
            };
            let mut capture = context.capture.lock().map_err(|_| {
                Error::new(
                    "ACP_PROMPT_UNKNOWN",
                    "ACP capture state became unavailable after the prompt response",
                )
            })?;
            capture.stop_reason.clone_from(&observed_stop_reason);
            capture.output_complete = output_complete;
            if let Err(error) = &prompt_result
                && capture.protocol_error.is_none()
            {
                capture.protocol_error = Some(error.code.clone());
            }
            drop(capture);
            let output_hash = context
                .journal
                .save_output(&command.operation_id, &output)
                .map_err(|error| {
                    Error::new(
                        "ACP_PROMPT_UNKNOWN",
                        format!("native prompt completed or became uncertain but output retention failed: {}", error.code),
                    )
                })?;
            let session = context.session.as_ref().ok_or_else(|| {
                Error::new(
                    "ACP_PROMPT_UNKNOWN",
                    "ACP session disappeared after the prompt request",
                )
            })?;
            context.journal.save_output_capture(&SavedOutputCapture {
                schema_version: 1,
                operation_id: command.operation_id.clone(),
                session_id: Some(session.id.to_string()),
                total_bytes: output_total_bytes,
                stored_bytes: output.len() as u64,
                stored_sha256: digest(&output),
                truncated: output_truncated,
                complete: output_complete,
                stop_reason: observed_stop_reason.clone(),
                protocol_error: protocol_error.clone().or_else(|| {
                    prompt_result.as_ref().err().map(|error| error.code.clone())
                }),
            }).map_err(|error| {
                Error::new(
                    "ACP_PROMPT_UNKNOWN",
                    format!("native prompt completed or became uncertain but capture receipt retention failed: {}", error.code),
                )
            })?;
            context
                .capture
                .lock()
                .map_err(|_| {
                    Error::new(
                        "ACP_PROMPT_UNKNOWN",
                        "ACP capture state became unavailable after output receipt persistence",
                    )
                })?
                .output_receipt_saved = true;
            let outcome = match prompt_result {
                Ok(response) => {
                    context
                        .journal
                        .record_effect(
                            command,
                            "session/prompt",
                            "receipt",
                            json!({
                                "native_session_id":session.id,
                                "stop_reason":format!("{:?}",response.stop_reason),
                                "output_sha256":output_hash,
                                "output_bytes":output.len(),
                                "output_truncated":output_truncated,
                                "stderr_error":stderr_error,
                                "protocol_error":protocol_error
                            }),
                        )
                        .map_err(|error| {
                            Error::new(
                                "ACP_PROMPT_UNKNOWN",
                                format!(
                                    "native prompt completed but its durable receipt failed: {}",
                                    error.code
                                ),
                            )
                        })?;
                    applied_in_session(
                        command,
                        session,
                        json!({
                            "execution_shape":ACP_EXECUTION_SHAPE,
                            "completion_condition":"native_turn_completed",
                            "task_completion":"unknown",
                            "native_session_id":session.id,
                            "native_stop_reason":format!("{:?}",response.stop_reason),
                            "native_payload_sha256":dispatch.as_ref().map(|value| value.identity.prompt_sha256.clone()).unwrap_or_else(|| digest(prompt.as_bytes())),
                            "native_payload_bytes":prompt.len(),
                            "output_sha256":output_hash,
                            "output_bytes":output.len(),
                            "output_total_bytes":output_total_bytes,
                            "output_truncated":output_truncated,
                            "output_complete":output_complete,
                            "native_stderr_bytes":stderr_len,
                            "native_stderr_total_bytes":stderr_total_bytes,
                            "native_stderr_sha256":stderr_hash,
                            "native_stderr_truncated":stderr_truncated,
                            "native_stderr_error":stderr_error,
                            "protocol_capture_error":protocol_error,
                            "session_updates":updates,
                            "task_dispatch_admission":admission,
                            "family_departure_claimed":false
                        }),
                    )
                }
                Err(error) => unknown_in_session(command, session, &error.code),
            };
            Ok(vec![outcome])
        }
        "agent.configure" => {
            let session = context
                .session
                .as_ref()
                .ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            if session.closed {
                return Err(Error::new("NATIVE_SESSION_CLOSED", "ACP session is closed"));
            }
            let settings = &command.input["settings"];
            if settings["modelId"].as_str() != Some(session.model_id.as_str()) {
                return Err(Error::new(
                    "CONFIGURATION_UNSUPPORTED",
                    "ACP model changes require a new route and verified session readback",
                ));
            }
            Ok(vec![applied_in_session(
                command,
                session,
                json!({
                    "configuration":"unchanged_route_model_verified",
                    "native_session_id":session.id,
                    "task_completion":"unknown"
                }),
            )])
        }
        "agent.refresh" => {
            if let Some(session) = context.session.as_ref() {
                Ok(vec![applied_in_session(
                    command,
                    session,
                    session_state(Some(session)),
                )])
            } else {
                Ok(vec![applied(command, session_state(None))])
            }
        }
        "agent.reconcile" => {
            let target = command.input["operation_id"]
                .as_str()
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| Error::invalid("reconcile target Operation is missing"))?;
            let details = if let Some((saved, hash)) = context.journal.read_outcome(target)? {
                json!({
                    "target_operation_id":target,
                    "target_outcome_sha256":hash,
                    "target_outcome":saved.outcome,
                    "native_replay":false
                })
            } else {
                json!({
                    "target_operation_id":target,
                    "completion_condition":"native_result_not_durably_known",
                    "task_completion":"unknown",
                    "native_replay":false
                })
            };
            if let Some(session) = context.session.as_ref() {
                Ok(vec![applied_in_session(command, session, details)])
            } else {
                Ok(vec![applied(command, details)])
            }
        }
        "agent.reply" => Ok(vec![reply_to_permission(context, command).await?]),
        "native.command.cancel_turn" => {
            let session = context
                .session
                .as_ref()
                .ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            validate_session_control(command, context.link, session)?;
            Ok(vec![rejected_in_session(
                command,
                session,
                "ACP_TURN_NOT_ACTIVE",
                json!({}),
            )])
        }
        "native.command.close_session" => {
            let session = context
                .session
                .as_mut()
                .ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            validate_session_control(command, context.link, session)?;
            if !session.close_supported
                || !context
                    .owner
                    .host
                    .claim
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "native.command.close_session")
            {
                return Err(Error::new(
                    "CAPABILITY_UNAVAILABLE",
                    "ACP agent did not advertise session/close",
                ));
            }
            let target = command.input["target_operation_id"]
                .as_str()
                .ok_or_else(|| Error::invalid("ACP close target Operation is missing"))?;
            let (target_outcome, _) = context.journal.read_outcome(target)?.ok_or_else(|| {
                Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "ACP close target has no retained outcome",
                )
            })?;
            if target_outcome.native_root_id.as_deref() != Some(session.id.to_string().as_str()) {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "ACP close target does not name this exact session",
                ));
            }
            context.journal.record_effect(
                command,
                "session/close",
                "intent",
                json!({"native_session_id":session.id,"target_operation_id":target}),
            )?;
            context
                .connection
                .send_request(CloseSessionRequest::new(session.id.clone()))
                .block_task()
                .await
                .map_err(|_| {
                    Error::new(
                        "ACP_CLOSE_UNKNOWN",
                        "ACP session/close response was not confirmed",
                    )
                })?;
            context
                .journal
                .record_effect(
                    command,
                    "session/close",
                    "receipt",
                    json!({"native_session_id":session.id,"response":"confirmed"}),
                )
                .map_err(|error| {
                    Error::new(
                        "ACP_CLOSE_UNKNOWN",
                        format!(
                            "native session close was confirmed but its receipt failed: {}",
                            error.code
                        ),
                    )
                })?;
            session.closed = true;
            context.journal.save_session(&json!({
                "schema_version":1,
                "artifact_id":ACP_ARTIFACT_ID,
                "binding_id":command.binding_id,
                "binding_generation":command.generation,
                "operation_id":session.owner_operation_id,
                "native_session_id":session.id,
                "native_root_id":session.id,
                "native_scope_key":session.id,
                "workspace":session.workspace,
                "requested_model":session.model_id,
                "effective_model":session.model_id,
                "native_session_state":"closed",
                "native_close_capability":"session/close",
                "family_departure_claimed":false
            }))
            .map_err(|error| {
                Error::new(
                    "ACP_CLOSE_UNKNOWN",
                    format!("native session close was confirmed but closed-state retention failed: {}", error.code),
                )
            })?;
            Ok(vec![applied_in_session(
                command,
                session,
                json!({
                    "native_session_id":session.id,
                    "native_close":"response_confirmed",
                    "family_departure_claimed":false
                }),
            )])
        }
        "agent.result" => {
            let target = command.input["selector"]["input_operation_id"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Error::invalid("ACP result target Operation is missing"))?;
            let session = context.session.as_ref().ok_or_else(|| {
                Error::new(
                    "NATIVE_SESSION_MISSING",
                    "ACP result requires the retained session",
                )
            })?;
            let (target_outcome, _) = context.journal.read_outcome(target)?.ok_or_else(|| {
                Error::new(
                    "RESULT_EVIDENCE_UNAVAILABLE",
                    "exact ACP target Operation has no retained terminal outcome",
                )
            })?;
            if target_outcome.native_root_id.as_deref() != Some(session.id.to_string().as_str())
                || target_outcome.native_scope_key.as_deref()
                    != Some(session.id.to_string().as_str())
            {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "ACP result target does not belong to the exact retained session",
                ));
            }
            let (bytes, receipt) = context.journal.read_output_capture(target)?;
            let session_id = session.id.to_string();
            if receipt.session_id.as_deref() != Some(session_id.as_str()) {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "ACP result capture does not name the exact retained session",
                ));
            }
            if !receipt.complete {
                return Ok(vec![rejected_in_session(
                    command,
                    session,
                    "ACP_RESULT_CAPTURE_INCOMPLETE",
                    json!({
                        "target_operation_id":target,
                        "capture_complete":false,
                        "capture_truncated":receipt.truncated,
                        "capture_total_bytes":receipt.total_bytes,
                        "capture_stored_bytes":receipt.stored_bytes,
                        "capture_stored_sha256":receipt.stored_sha256,
                        "capture_protocol_error":receipt.protocol_error,
                        "task_completion":"unknown",
                        "native_replay":false
                    }),
                )]);
            }
            let params = result_page::build_acp_output(command, &context.owner.host.claim, &bytes)?;
            let hash = context
                .journal
                .save_result_page(&command.operation_id, &params)?;
            if context
                .journal
                .result_page_pending(&command.operation_id, &hash)?
            {
                let ack = context.link.client.result(params).await?;
                context
                    .journal
                    .acknowledge_result_page(&command.operation_id, &hash, ack)?;
            }
            let details = json!({
                "result_page_sha256":hash,
                "target_operation_id":target,
                "payload_bytes":bytes.len(),
                "payload_sha256":digest(&bytes),
                "capture_complete":receipt.complete,
                "capture_truncated":receipt.truncated,
                "capture_total_bytes":receipt.total_bytes,
                "capture_stored_bytes":receipt.stored_bytes,
                "native_stop_reason":receipt.stop_reason,
                "task_completion":"unknown",
                "native_replay":false
            });
            Ok(vec![applied_in_session(command, session, details)])
        }
        _ => Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "ACP received an unsupported runtime command",
        )),
    }
}
async fn open_session(
    connection: &ConnectionTo<Agent>,
    journal: &AcpJournal,
    command: &RuntimeCommand,
    workspace: &Path,
    requested_model: &str,
    owner: &Owner,
    features: &AgentFeatures,
) -> Result<ActiveSession> {
    if !workspace.is_absolute() || !workspace.is_dir() {
        return Err(Error::new(
            "WORKSPACE_INVALID",
            "ACP workspace must be an admitted absolute directory",
        ));
    }
    if let Some(saved) = journal.read_session()?
        && saved["native_session_id"].as_str().is_some()
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "prior ACP session is retained for exact load; a second native session will not be started",
        ));
    }
    journal.record_effect(
        command,
        "session/new",
        "intent",
        json!({
            "workspace":workspace,
            "requested_model":requested_model,
            "load_session_fallback":false
        }),
    )?;
    let opened = connection
        .send_request(NewSessionRequest::new(workspace))
        .block_task()
        .await
        .map_err(|_| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP session/new response is uncertain",
            )
        })?;
    let session_id = opened.session_id;
    let close_supported = features.close_supported
        && owner
            .host
            .claim
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "native.command.close_session");
    let session = ActiveSession {
        id: session_id.clone(),
        owner_operation_id: command.operation_id.clone(),
        workspace: workspace.to_path_buf(),
        model_id: requested_model.to_owned(),
        close_supported,
        closed: false,
    };
    journal
        .save_session(&json!({
            "schema_version":1,
            "artifact_id":ACP_ARTIFACT_ID,
            "binding_id":command.binding_id,
            "binding_generation":command.generation,
            "operation_id":command.operation_id,
            "native_session_id":session.id,
            "native_root_id":session.id,
            "native_scope_key":session.id,
            "workspace":session.workspace,
            "requested_model":requested_model,
            "effective_model":Value::Null,
            "native_session_state":"configuring",
            "native_close_capability":close_supported,
            "agent_info":features.agent_info,
            "agent_capabilities":features.capabilities,
            "family_departure_claimed":false
        }))
        .map_err(|error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "native ACP session was created but its identity receipt failed: {}",
                    error.code
                ),
            )
        })?;
    journal
        .record_effect(
            command,
            "session/new",
            "receipt",
            json!({
                "native_session_id":session.id,
                "native_root_id":session.id,
                "native_scope_key":session.id,
                "workspace":session.workspace
            }),
        )
        .map_err(|error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "native ACP session was created but its durable receipt failed: {}",
                    error.code
                ),
            )
        })?;
    let mut options = opened.config_options.clone().unwrap_or_default();
    let advertised = serde_json::to_value(&options)?;
    if !model_value_available(&advertised, requested_model) {
        return Err(Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP created a session but did not advertise the exact routed model; the retained session requires readback",
        ));
    }
    if !model_current_value_matches(&advertised, requested_model) {
        let configured = connection
            .send_request(SetSessionConfigOptionRequest::new(
                session_id.clone(),
                "model",
                SessionConfigOptionValue::value_id(requested_model.to_owned()),
            ))
            .block_task()
            .await
            .map_err(|_| {
                Error::new(
                    "ACP_SESSION_OPEN_UNKNOWN",
                    "ACP model configuration response is uncertain after session creation",
                )
            })?;
        options = configured.config_options;
    }
    let readback = serde_json::to_value(&options)?;
    if !model_current_value_matches(&readback, requested_model) {
        return Err(Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP effective model did not match the requested model; the retained session requires readback",
        ));
    }
    journal
        .save_session(&json!({
            "schema_version":1,
            "artifact_id":ACP_ARTIFACT_ID,
            "binding_id":command.binding_id,
            "binding_generation":command.generation,
            "operation_id":session.owner_operation_id,
            "native_session_id":session.id,
            "native_root_id":session.id,
            "native_scope_key":session.id,
            "workspace":session.workspace,
            "requested_model":requested_model,
            "effective_model":requested_model,
            "model_readback":readback,
            "native_session_state":"open",
            "native_close_capability":session.close_supported,
            "agent_info":features.agent_info,
            "agent_capabilities":features.capabilities,
            "family_departure_claimed":false
        }))
        .map_err(|error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "native ACP session model was read back but open-state retention failed: {}",
                    error.code
                ),
            )
        })?;
    Ok(session)
}

async fn restore_session(
    context: SessionRestoreContext<'_>,
    command: &RuntimeCommand,
    saved: Value,
) -> Result<ActiveSession> {
    let SessionRestoreContext {
        connection,
        journal,
        link,
        owner,
        features,
        capture,
    } = context;
    let session_text = saved["native_session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("retained ACP session has no native ID"))?;
    if saved["schema_version"] != 1
        || saved["artifact_id"] != ACP_ARTIFACT_ID
        || saved["binding_id"] != link.binding_id
        || saved["binding_generation"].as_i64() != Some(link.generation)
        || saved["native_root_id"] != session_text
        || saved["native_scope_key"] != session_text
        || saved["native_session_state"] == "closed"
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "retained ACP session identity is closed or differs from this exact binding",
        ));
    }
    if !features.load_session_supported {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "ACP agent does not advertise session/load; no replacement session or prompt replay is allowed",
        ));
    }
    let session_id: SessionId = serde_json::from_value(json!(session_text))
        .map_err(|_| invalid("retained ACP session ID is malformed"))?;
    let workspace = PathBuf::from(
        saved["workspace"]
            .as_str()
            .ok_or_else(|| invalid("retained ACP session omitted its workspace"))?,
    );
    let route_workspace = link.route["native_options"]["workspaceRoot"]
        .as_str()
        .filter(|value| Path::new(value).is_absolute())
        .ok_or_else(|| Error::new("WORKSPACE_INVALID", "route workspaceRoot is invalid"))?;
    if !workspace.is_absolute()
        || fs::canonicalize(&workspace)? != fs::canonicalize(route_workspace)?
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "retained ACP session workspace differs from the immutable binding route",
        ));
    }
    let model_id = command_model(&link.route)
        .filter(|model| saved["requested_model"] == *model)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "retained ACP session model differs from the immutable binding route",
            )
        })?
        .to_owned();
    journal.record_effect(
        command,
        "session/load",
        "intent",
        json!({
            "native_session_id":session_id,
            "native_root_id":session_id,
            "native_scope_key":session_id,
            "workspace":workspace,
            "requested_model":model_id,
            "native_replay":false
        }),
    )?;
    if let Ok(mut capture) = capture.lock() {
        capture.native_session_id = Some(session_text.to_owned());
        capture.history_replaying = true;
    }
    let loaded = connection
        .send_request(LoadSessionRequest::new(
            session_id.clone(),
            workspace.clone(),
        ))
        .block_task()
        .await;
    if let Ok(mut capture) = capture.lock() {
        capture.history_replaying = false;
    }
    let loaded = loaded.map_err(|_| {
        Error::new(
            "RECOVERY_REQUIRED",
            "ACP session/load failed; original TaskPrompt will not be replayed",
        )
    })?;
    let readback = serde_json::to_value(loaded.config_options.unwrap_or_default())?;
    if !model_current_value_matches(&readback, &model_id) {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "ACP session/load did not read back the retained exact model",
        ));
    }
    journal.record_effect(
        command,
        "session/load",
        "receipt",
        json!({
            "native_session_id":session_id,
            "native_root_id":session_id,
            "native_scope_key":session_id,
            "model_readback":readback,
            "history_updates":capture.lock().map(|capture| capture.history_updates).unwrap_or(0),
            "native_replay":false
        }),
    )?;
    let close_supported = features.close_supported
        && owner
            .host
            .claim
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "native.command.close_session");
    let owner_operation_id = saved["operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("retained ACP session omitted its owner Operation"))?
        .to_owned();
    let session = ActiveSession {
        id: session_id,
        owner_operation_id,
        workspace,
        model_id,
        close_supported,
        closed: false,
    };
    let mut updated = saved;
    updated["native_session_state"] = json!("loaded");
    updated["native_close_capability"] = json!(close_supported);
    updated["agent_info"] = features.agent_info.clone().unwrap_or(Value::Null);
    updated["agent_capabilities"] = features.capabilities.clone();
    updated["model_readback"] = readback;
    updated["family_departure_claimed"] = json!(false);
    journal.save_session(&updated)?;
    Ok(session)
}

impl PermissionHandlerState {
    async fn handle(
        &self,
        request: RequestPermissionRequest,
        responder: agent_client_protocol::Responder<RequestPermissionResponse>,
    ) -> std::result::Result<(), agent_client_protocol::Error> {
        let request_id = serde_json::to_value(responder.id())
            .map_err(|_| agent_client_protocol::Error::internal_error())?;
        if request_id.is_null() || request.options.is_empty() || request.options.len() > 32 {
            return responder
                .respond_with_internal_error("ACP permission request is outside its bounds");
        }
        let request_value = serde_json::to_value(&request)
            .map_err(|_| agent_client_protocol::Error::internal_error())?;
        let canonical = json!({"request_id":request_id,"request":request_value});
        let canonical_bytes = serde_json::to_vec(&canonical)
            .map_err(|_| agent_client_protocol::Error::internal_error())?;
        if canonical_bytes.len() > MAX_PERMISSION_BYTES {
            return responder
                .respond_with_internal_error("ACP permission request exceeds its bound");
        }
        let request_id = canonical["request_id"].clone();
        let key = serde_json::to_string(&request_id)
            .map_err(|_| agent_client_protocol::Error::internal_error())?;
        let session_id = request.session_id.to_string();
        let operation = self
            .active_operation
            .lock()
            .map_err(|_| to_acp_error(invalid("ACP active-operation lock is poisoned")))?
            .clone();
        let Some(operation) = operation.filter(|operation| {
            matches!(operation.method.as_str(), "task.dispatch" | "agent.send")
        }) else {
            return responder.respond_with_internal_error(
                "ACP permission request has no exact active input turn",
            );
        };
        let expected_session = self
            .capture
            .lock()
            .map_err(|_| to_acp_error(invalid("ACP capture lock is poisoned")))?
            .native_session_id
            .clone();
        if expected_session.as_deref() != Some(session_id.as_str()) {
            return responder
                .respond_with_internal_error("ACP permission request belongs to another session");
        }
        let fingerprint = digest(&canonical_bytes);
        let options: Vec<_> = request
            .options
            .iter()
            .map(|option| {
                (
                    option.option_id.to_string(),
                    option.kind,
                    option.name.clone(),
                )
            })
            .collect();
        if options
            .iter()
            .enumerate()
            .any(|(index, option)| options[..index].iter().any(|prior| prior.0 == option.0))
        {
            return responder.respond_with_internal_error(
                "ACP permission options contain duplicate native IDs",
            );
        }
        let tool_call = request_value["toolCall"].clone();
        let pending_record = json!({
            "kind":"permission",
            "session_id":session_id,
            "request_id":request_id,
            "fingerprint":fingerprint,
            "tool_call":tool_call,
            "options":request.options.iter().map(|option| json!({
                "option_id":option.option_id,
                "kind":format!("{:?}",option.kind),
                "name":option.name
            })).collect::<Vec<_>>()
        });
        if serde_json::to_vec(&pending_record)
            .map_err(|_| agent_client_protocol::Error::internal_error())?
            .len()
            > MAX_PERMISSION_BYTES
        {
            return responder
                .respond_with_internal_error("ACP permission projection exceeds its bound");
        }
        self.journal
            .record_effect(
                &operation,
                &permission_effect_id(&key),
                "intent",
                json!({
                    "session_id":session_id,
                    "request_id":request_id,
                    "fingerprint":fingerprint,
                    "option_ids":options.iter().map(|option| &option.0).collect::<Vec<_>>()
                }),
            )
            .map_err(to_acp_error)?;
        let (reply, response) = oneshot::channel();
        let (acknowledgement, acknowledged) = oneshot::channel();
        {
            let mut permissions = self
                .permissions
                .lock()
                .map_err(|_| to_acp_error(invalid("ACP permission broker lock is poisoned")))?;
            if permissions.contains_key(&key) || permissions.len() >= 64 {
                return responder.respond_with_internal_error(
                    "ACP permission request is duplicate or exceeds the pending bound",
                );
            }
            permissions.insert(
                key.clone(),
                PendingPermission {
                    session_id: session_id.clone(),
                    request_id: request_id.clone(),
                    fingerprint: fingerprint.clone(),
                    options,
                    reply,
                    acknowledgement: acknowledged,
                },
            );
        }
        {
            let mut capture = self
                .capture
                .lock()
                .map_err(|_| to_acp_error(invalid("ACP capture lock is poisoned")))?;
            capture.pending_requests.push(pending_record);
        }
        self.wakeup.notify_one();

        let outcome = match response.await {
            Ok(outcome) => outcome,
            Err(_) => {
                if let Ok(mut permissions) = self.permissions.lock() {
                    permissions.remove(&key);
                }
                if let Ok(mut capture) = self.capture.lock() {
                    capture
                        .pending_requests
                        .retain(|item| item["request_id"] != request_id);
                }
                return Err(to_acp_error(Error::new(
                    "ACP_PERMISSION_REPLY_UNKNOWN",
                    "ACP permission reply channel ended before a native choice was delivered",
                )));
            }
        };
        let response_result = responder.respond(RequestPermissionResponse::new(outcome));
        let response_sent = response_result.is_ok();
        self.journal
            .record_effect(
                &operation,
                &permission_effect_id(&key),
                "receipt",
                json!({
                    "session_id":session_id,
                    "request_id":request_id,
                    "fingerprint":fingerprint,
                    "response_sent":response_sent
                }),
            )
            .map_err(to_acp_error)?;
        if let Ok(mut permissions) = self.permissions.lock() {
            permissions.remove(&key);
        }
        if let Ok(mut capture) = self.capture.lock() {
            capture
                .pending_requests
                .retain(|item| item["request_id"] != request_id);
        }
        let _ = acknowledgement.send(response_sent);
        self.wakeup.notify_one();
        response_result.map_err(|_| {
            to_acp_error(Error::new(
                "ACP_PERMISSION_REPLY_UNKNOWN",
                "ACP permission response could not be written to the native transport",
            ))
        })
    }
}

async fn prompt_with_cancel(
    context: &mut CommandContext<'_>,
    dispatch_command: &RuntimeCommand,
    prompt: PromptRequest,
) -> Result<PromptResponse> {
    let connection = context.connection.clone();
    let request = connection.send_request(prompt).block_task();
    tokio::pin!(request);
    loop {
        tokio::select! {
            response = &mut request => {
                return response.map_err(|_| Error::new(
                    "ACP_PROMPT_UNKNOWN",
                    "ACP prompt response is uncertain after native admission",
                ));
            }
            next = context.link.client.next() => {
                let next = next.map_err(|_| Error::new(
                    "ACP_PROMPT_UNKNOWN",
                    "module transport failed while an ACP prompt was active",
                ))?;
                if next["command"].is_null() {
                    continue;
                }
                let control: RuntimeCommand = serde_json::from_value(next["command"].clone())
                    .map_err(|_| Error::new(
                        "MODULE_COMMAND_INVALID",
                        "module.next returned an invalid ACP control envelope",
                    ))?;
                process_control_during_turn(context, dispatch_command, control).await?;
            }
            _ = context.permission_wakeup.notified() => {
                let last = json!({
                    "operation_id":dispatch_command.operation_id,
                    "method":dispatch_command.method,
                    "state":"running"
                });
                let session = context.journal.read_session()?;
                let observation = {
                    let capture = context
                        .capture
                        .lock()
                        .map_err(|_| invalid("ACP capture lock is poisoned"))?;
                    acp_state(
                        context.link,
                        context.owner,
                        &last,
                        session.as_ref(),
                        &capture.pending_requests,
                        Some(&capture),
                    )
                };
                let sequence = take_observation_sequence(context.observation_sequence)?;
                super::send_observation(&mut context.link.client, context.owner, sequence, observation)
                    .await
                    .map_err(|_| Error::new(
                        "ACP_PROMPT_UNKNOWN",
                        "permission attention could not be durably published during the active turn",
                    ))?;
            }
        }
    }
}

async fn process_control_during_turn(
    context: &mut CommandContext<'_>,
    dispatch_command: &RuntimeCommand,
    control: RuntimeCommand,
) -> Result<()> {
    context.journal.record_command(&control)?;
    let outcome = if let Err(error) = super::validate_command(&control, context.link) {
        rejected(&control, &error.code)
    } else {
        match control.method.as_str() {
            "native.command.cancel_turn" => {
                match cancel_active_turn(context, dispatch_command, &control).await {
                    Ok(outcome) => outcome,
                    Err(error) if is_uncertain_control_error(&error) => {
                        context.link.recovery_required = true;
                        unknown_in_session(
                            &control,
                            context.session.as_ref().ok_or_else(|| {
                                invalid("active ACP session disappeared during cancellation")
                            })?,
                            &error.code,
                        )
                    }
                    Err(error) => rejected(&control, &error.code),
                }
            }
            "agent.reply" => match reply_to_permission(context, &control).await {
                Ok(outcome) => outcome,
                Err(error) if is_uncertain_control_error(&error) => {
                    context.link.recovery_required = true;
                    unknown_in_session(
                        &control,
                        context.session.as_ref().ok_or_else(|| {
                            invalid("active ACP session disappeared during permission reply")
                        })?,
                        &error.code,
                    )
                }
                Err(error) => rejected(&control, &error.code),
            },
            _ => {
                let session = context.session.as_ref().ok_or_else(|| {
                    invalid("active ACP session disappeared while a turn was running")
                })?;
                rejected_in_session(
                    &control,
                    session,
                    "ACP_TURN_ACTIVE",
                    json!({
                        "active_turn_operation_id":dispatch_command.operation_id,
                        "retry_after_turn":true,
                        "native_replay":false
                    }),
                )
            }
        }
    };
    let uncertain = outcome.outcome == EffectOutcome::Unknown;
    deliver_outcome(
        context.link,
        context.owner,
        context.journal,
        control,
        outcome,
    )
    .await?;
    if uncertain {
        return Err(Error::new(
            "ACP_CONTROL_EFFECT_UNKNOWN",
            "ACP control effect is uncertain; the exact native connection must be retired",
        ));
    }
    Ok(())
}

async fn cancel_active_turn(
    context: &mut CommandContext<'_>,
    dispatch_command: &RuntimeCommand,
    control: &RuntimeCommand,
) -> Result<RuntimeOutcome> {
    let session = context.session.as_ref().ok_or_else(|| {
        Error::new(
            "NATIVE_SESSION_MISSING",
            "ACP cancel requires an open session",
        )
    })?;
    validate_session_control(control, context.link, session)?;
    let target = control.input["target_operation_id"]
        .as_str()
        .ok_or_else(|| Error::invalid("ACP cancel target Operation is missing"))?;
    if target != dispatch_command.operation_id
        || control.input["turn_id"] != dispatch_command.operation_id
        || !matches!(
            dispatch_command.method.as_str(),
            "task.dispatch" | "agent.send"
        )
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "ACP cancel does not identify the exact active session turn",
        ));
    }
    context.journal.record_effect(
        control,
        "session/cancel",
        "intent",
        json!({
            "native_session_id":session.id,
            "target_operation_id":target,
            "turn_id":dispatch_command.operation_id
        }),
    )?;
    cancel_pending_permissions(context, control, &session.id).await?;
    context
        .connection
        .send_notification(CancelNotification::new(session.id.clone()))
        .map_err(|_| {
            Error::new(
                "ACP_CANCEL_UNKNOWN",
                "ACP session/cancel notification delivery is uncertain",
            )
        })?;
    context
        .journal
        .record_effect(
            control,
            "session/cancel",
            "receipt",
            json!({
                "native_session_id":session.id,
                "target_operation_id":target,
                "notification":"sent",
                "completion_proved":false
            }),
        )
        .map_err(|error| {
            Error::new(
                "ACP_CANCEL_UNKNOWN",
                format!(
                    "session/cancel was sent but its receipt failed: {}",
                    error.code
                ),
            )
        })?;
    let mut outcome = applied_in_session(
        control,
        session,
        json!({
            "target_operation_id":target,
            "turn_id":dispatch_command.operation_id,
            "cancel":"notification_sent",
            "completion_proved":false,
            "family_departure_claimed":false
        }),
    );
    outcome.turn_id = Some(dispatch_command.operation_id.clone());
    Ok(outcome)
}

async fn cancel_pending_permissions(
    context: &CommandContext<'_>,
    cancel_command: &RuntimeCommand,
    session_id: &SessionId,
) -> Result<()> {
    let keys: Vec<String> = context
        .permissions
        .lock()
        .map_err(|_| invalid("ACP permission broker lock is poisoned"))?
        .iter()
        .filter(|(_, pending)| pending.session_id == session_id.to_string())
        .map(|(key, _)| key.clone())
        .collect();
    for key in keys {
        let (pending_session_id, request_id, fingerprint) = {
            let permissions = context
                .permissions
                .lock()
                .map_err(|_| invalid("ACP permission broker lock is poisoned"))?;
            let pending = permissions
                .get(&key)
                .ok_or_else(|| invalid("ACP pending permission disappeared during cancel"))?;
            (
                pending.session_id.clone(),
                pending.request_id.clone(),
                pending.fingerprint.clone(),
            )
        };
        let effect_id = permission_cancel_effect_id(&key);
        context.journal.record_effect(
            cancel_command,
            &effect_id,
            "intent",
            json!({
                "session_id":pending_session_id,
                "request_id":request_id,
                "fingerprint":fingerprint,
                "response":"cancelled"
            }),
        )?;
        let pending = context
            .permissions
            .lock()
            .map_err(|_| invalid("ACP permission broker lock is poisoned"))?
            .remove(&key)
            .ok_or_else(|| invalid("ACP pending permission disappeared during cancel"))?;
        pending
            .reply
            .send(RequestPermissionOutcome::Cancelled)
            .map_err(|_| {
                Error::new(
                    "ACP_PERMISSION_CANCEL_UNKNOWN",
                    "ACP pending permission handler ended before cancellation response",
                )
            })?;
        let response_sent = time::timeout(CAPTURE_DRAIN_GRACE, pending.acknowledgement)
            .await
            .map_err(|_| {
                Error::new(
                    "ACP_PERMISSION_CANCEL_UNKNOWN",
                    "ACP pending permission cancellation response exceeded its deadline",
                )
            })?
            .map_err(|_| {
                Error::new(
                    "ACP_PERMISSION_CANCEL_UNKNOWN",
                    "ACP pending permission handler did not confirm cancellation",
                )
            })?;
        if !response_sent {
            return Err(Error::new(
                "ACP_PERMISSION_CANCEL_UNKNOWN",
                "ACP pending permission cancellation was not written to the native transport",
            ));
        }
        context
            .journal
            .record_effect(
                cancel_command,
                &effect_id,
                "receipt",
                json!({
                    "session_id":pending_session_id,
                    "request_id":request_id,
                    "fingerprint":fingerprint,
                    "response":"cancelled",
                    "response_written":true
                }),
            )
            .map_err(|error| {
                Error::new(
                    "ACP_PERMISSION_CANCEL_UNKNOWN",
                    format!(
                        "native permission cancellation was written but its receipt failed: {}",
                        error.code
                    ),
                )
            })?;
    }
    Ok(())
}

async fn reply_to_permission(
    context: &mut CommandContext<'_>,
    command: &RuntimeCommand,
) -> Result<RuntimeOutcome> {
    validate_client_control_identity(command, context.link)?;
    let session = context.session.as_ref().ok_or_else(|| {
        Error::new(
            "NATIVE_SESSION_MISSING",
            "ACP permission reply requires an open session",
        )
    })?;
    let reply = &command.input["reply"];
    let session_id = reply["session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("ACP permission reply session_id is missing"))?;
    let request_id = reply["request_id"].clone();
    let fingerprint = reply["fingerprint"]
        .as_str()
        .filter(|value| is_sha256(value))
        .ok_or_else(|| Error::invalid("ACP permission reply fingerprint is invalid"))?;
    let option_id = reply["body"]["option_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.len() <= 256)
        .ok_or_else(|| Error::invalid("ACP permission reply must select a native option_id"))?;
    if reply["kind"] != "permission"
        || request_id.is_null()
        || session_id != session.id.to_string()
        || reply["body"]
            .as_object()
            .is_none_or(|body| body.len() != 1 || !body.contains_key("option_id"))
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "ACP reply must identify one exact live permission request and native option",
        ));
    }
    let key = serde_json::to_string(&request_id)?;
    let selected_kind = {
        let permissions = context
            .permissions
            .lock()
            .map_err(|_| invalid("ACP permission broker lock is poisoned"))?;
        let pending = permissions.get(&key).ok_or_else(|| {
            Error::new(
                "NATIVE_REQUEST_NOT_PENDING",
                "exact ACP permission request is no longer pending",
            )
        })?;
        if pending.session_id != session_id
            || pending.request_id != request_id
            || pending.fingerprint != fingerprint
        {
            return Err(Error::new(
                "NATIVE_REQUEST_CHANGED",
                "ACP permission identity or fingerprint differs from the live request",
            ));
        }
        pending
            .options
            .iter()
            .find(|(id, _, _)| id == option_id)
            .map(|(_, kind, _)| *kind)
            .ok_or_else(|| {
                Error::new(
                    "NATIVE_PERMISSION_OPTION_INVALID",
                    "selected ACP permission option was not advertised by the native request",
                )
            })?
    };
    if !matches!(
        selected_kind,
        PermissionOptionKind::AllowOnce | PermissionOptionKind::RejectOnce
    ) {
        return Err(Error::new(
            "NATIVE_PERMISSION_PERSISTENT_UNSUPPORTED",
            "ACP persistent permission choices require a separate policy mutation and readback",
        ));
    }
    let selected_option = option_id.to_owned();
    context.journal.record_effect(
        command,
        &format!("permission-reply:{}", digest(key.as_bytes())),
        "intent",
        json!({
            "session_id":session_id,
            "request_id":request_id,
            "fingerprint":fingerprint,
            "option_id":selected_option,
            "option_kind":format!("{:?}",selected_kind)
        }),
    )?;
    let pending = context
        .permissions
        .lock()
        .map_err(|_| invalid("ACP permission broker lock is poisoned"))?
        .remove(&key)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_REQUEST_NOT_PENDING",
                "exact ACP permission request was already answered",
            )
        })?;
    let PendingPermission {
        reply,
        acknowledgement,
        ..
    } = pending;
    reply
        .send(RequestPermissionOutcome::Selected(
            SelectedPermissionOutcome::new(selected_option.clone()),
        ))
        .map_err(|_| {
            Error::new(
                "ACP_PERMISSION_REPLY_UNKNOWN",
                "ACP permission request handler ended before the selected option was delivered",
            )
        })?;
    let response_sent = time::timeout(CAPTURE_DRAIN_GRACE, acknowledgement)
        .await
        .map_err(|_| {
            Error::new(
                "ACP_PERMISSION_REPLY_UNKNOWN",
                "ACP native permission response exceeded its bounded acknowledgement deadline",
            )
        })?
        .map_err(|_| {
            Error::new(
                "ACP_PERMISSION_REPLY_UNKNOWN",
                "ACP permission handler did not confirm the native response",
            )
        })?;
    if !response_sent {
        return Err(Error::new(
            "ACP_PERMISSION_REPLY_UNKNOWN",
            "ACP native permission response could not be written",
        ));
    }
    context
        .journal
        .record_effect(
            command,
            &format!("permission-reply:{}", digest(key.as_bytes())),
            "receipt",
            json!({
                "session_id":session_id,
                "request_id":request_id,
                "fingerprint":fingerprint,
                "option_id":selected_option,
                "response":"confirmed_written"
            }),
        )
        .map_err(|error| {
            Error::new(
                "ACP_PERMISSION_REPLY_UNKNOWN",
                format!(
                    "native permission reply was written but its receipt failed: {}",
                    error.code
                ),
            )
        })?;
    context.permission_wakeup.notify_one();
    Ok(applied_in_session(
        command,
        session,
        json!({
            "permission_request_id":request_id,
            "permission_fingerprint":fingerprint,
            "selected_option_id":selected_option,
            "selected_option_kind":format!("{:?}",selected_kind),
            "persistent_choice":false
        }),
    ))
}

fn validate_client_control_identity(command: &RuntimeCommand, link: &Link) -> Result<()> {
    let client_request_id = command.input["client_request_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.len() <= 128)
        .ok_or_else(|| Error::invalid("client_request_id is missing or invalid"))?;
    if client_request_id.trim() != client_request_id
        || command.input["binding_id"] != link.binding_id
        || command.input["generation"].as_i64() != Some(link.generation)
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "ACP control differs from its exact client request and binding generation",
        ));
    }
    Ok(())
}

fn validate_session_control(
    command: &RuntimeCommand,
    link: &Link,
    session: &ActiveSession,
) -> Result<()> {
    validate_client_control_identity(command, link)?;
    let target = command.input["target_operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid("ACP control target_operation_id is missing"))?;
    if command.input["session_id"] != session.id.to_string()
        || (command.method == "native.command.cancel_turn" && command.input["turn_id"] != target)
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "ACP control session or turn differs from its exact target",
        ));
    }
    Ok(())
}

fn permission_effect_id(key: &str) -> String {
    format!("permission:{}", digest(key.as_bytes()))
}

fn permission_cancel_effect_id(key: &str) -> String {
    format!("permission-cancel:{}", digest(key.as_bytes()))
}

fn uncertain_acp_error(error: &Error) -> bool {
    super::is_transport_uncertain(error)
        || matches!(
            error.code.as_str(),
            "ACP_PROMPT_UNKNOWN"
                | "ACP_SESSION_OPEN_UNKNOWN"
                | "ACP_CLOSE_UNKNOWN"
                | "ACP_CANCEL_UNKNOWN"
                | "ACP_PERMISSION_CANCEL_UNKNOWN"
                | "ACP_PERMISSION_REPLY_UNKNOWN"
                | "ACP_CONTROL_EFFECT_UNKNOWN"
                | "ACP_MODULE_LINK_LOST"
        )
}

fn is_uncertain_control_error(error: &Error) -> bool {
    uncertain_acp_error(error)
}

fn attach_saved_session_identity(
    outcome: &mut RuntimeOutcome,
    command: &RuntimeCommand,
    saved: Option<&Value>,
) {
    let Some(saved) = saved else {
        return;
    };
    let Some(session_id) = saved["native_session_id"].as_str() else {
        return;
    };
    if session_id.trim().is_empty()
        || saved["artifact_id"] != ACP_ARTIFACT_ID
        || saved["binding_id"] != command.binding_id
        || saved["binding_generation"].as_i64() != Some(command.generation)
        || saved["native_root_id"] != session_id
        || saved["native_scope_key"] != session_id
        || (command.method == "agent.open" && saved["operation_id"] != command.operation_id)
        || outcome
            .native_root_id
            .as_deref()
            .is_some_and(|root| root != session_id)
        || outcome
            .native_scope_key
            .as_deref()
            .is_some_and(|scope| scope != session_id)
    {
        return;
    }
    if let Some(details) = outcome.details.as_object()
        && ["native_session_id", "native_root_id", "native_scope_key"]
            .iter()
            .any(|key| {
                details
                    .get(*key)
                    .is_some_and(|value| value.as_str() != Some(session_id))
            })
    {
        return;
    }

    outcome.native_root_id = Some(session_id.to_owned());
    outcome.native_scope_key = Some(session_id.to_owned());
    if let Some(details) = outcome.details.as_object_mut() {
        details
            .entry("native_session_id".to_owned())
            .or_insert_with(|| json!(session_id));
        details
            .entry("native_root_id".to_owned())
            .or_insert_with(|| json!(session_id));
        details
            .entry("native_scope_key".to_owned())
            .or_insert_with(|| json!(session_id));
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
    options.as_array().is_some_and(|options| {
        options.iter().any(|option| {
            option["id"] == "model"
                && option
                    .get("options")
                    .and_then(Value::as_array)
                    .is_some_and(|values| {
                        values.iter().any(|value| {
                            value["value"] == requested
                                || value["value"]["id"] == requested
                                || value["id"] == requested
                        })
                    })
        })
    })
}

fn model_current_value_matches(options: &Value, requested: &str) -> bool {
    options.as_array().is_some_and(|options| {
        options.iter().any(|option| {
            option["id"] == "model"
                && (option["currentValue"] == requested
                    || option["currentValue"]["value"] == requested
                    || option["currentValue"]["id"] == requested)
        })
    })
}

fn command_model(route: &Value) -> Option<&str> {
    route["native_options"]["modelId"]
        .as_str()
        .filter(|model| !model.trim().is_empty() && model.trim() == *model && model.len() <= 256)
}

fn update_text(update: &SessionUpdate) -> Option<&str> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
            ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn append_capture(
    target: &mut Vec<u8>,
    total_bytes: &mut u64,
    truncated: &mut bool,
    bytes: &[u8],
    limit: usize,
) {
    let remaining = limit.saturating_sub(target.len());
    let copied = remaining.min(bytes.len());
    target.extend_from_slice(&bytes[..copied]);
    *total_bytes = total_bytes.saturating_add(bytes.len() as u64);
    if copied != bytes.len() {
        *truncated = true;
    }
}

fn compact_tool_event(update: &SessionUpdate) -> Option<Value> {
    let kind = match update {
        SessionUpdate::ToolCall(_) => "tool_call",
        SessionUpdate::ToolCallUpdate(_) => "tool_call_update",
        SessionUpdate::Plan(_) => "plan",
        _ => return None,
    };
    let event = serde_json::to_value(update).ok()?;
    let bytes = serde_json::to_vec(&event).ok()?;
    if bytes.len() <= 8_192 {
        Some(json!({"type":kind,"event":event}))
    } else {
        Some(json!({
            "type":kind,
            "event_sha256":digest(&bytes),
            "event_bytes":bytes.len(),
            "truncated":true
        }))
    }
}

fn native_agent_child(update: &SessionUpdate) -> Option<Value> {
    let event = match update {
        SessionUpdate::ToolCall(call) => serde_json::to_value(call).ok()?,
        SessionUpdate::ToolCallUpdate(call) => serde_json::to_value(call).ok()?,
        _ => return None,
    };
    if event["name"] != "agent" {
        return None;
    }
    let raw_output = &event["rawOutput"];
    let output = raw_output
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or_else(|| raw_output.clone());
    let agent_id = output
        .get("agent_id")
        .or_else(|| output.get("agentId"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty() && id.len() <= 256)?;
    Some(json!({
        "tool_call_id":event["toolCallId"],
        "agent_id":agent_id,
        "status":event["status"],
        "family_control":"unavailable"
    }))
}

fn retain_bounded_event(events: &mut Vec<Value>, event: Value) {
    if events.len() == 64 {
        events.remove(0);
    }
    events.push(event);
}

fn acp_state(
    link: &Link,
    owner: &Owner,
    last_operation: &Value,
    session: Option<&Value>,
    pending: &[Value],
    capture: Option<&Capture>,
) -> Value {
    let root = session.and_then(|saved| saved["native_root_id"].as_str());
    let scope = session.and_then(|saved| saved["native_scope_key"].as_str());
    let paired_identity = root.is_some_and(|root| !root.trim().is_empty()) && root == scope;
    let state = session
        .and_then(|saved| saved["native_session_state"].as_str())
        .unwrap_or("not_started");
    let ready = paired_identity && matches!(state, "open" | "loaded");
    let close_supported = session.is_some_and(|saved| {
        saved["native_close_capability"] == true
            && owner
                .host
                .claim
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "native.command.close_session")
    });
    json!({
        "phase":if ready {"ready"} else if session.is_some() {"recovery_required"} else {"sessionless"},
        "native_root_id":if paired_identity {json!(root)} else {Value::Null},
        "native_scope_key":if paired_identity {json!(scope)} else {Value::Null},
        "native_session_state":state,
        "boot_id":owner.host.boot_id,
        "describe":{
            "runtime":"command",
            "module_artifact_id":owner.host.profile.artifact_id(),
            "module_artifact_version":owner.host.profile.artifact_version(),
            "contract_revision":owner.host.profile.contract_revision(),
            "execution_shape":ACP_EXECUTION_SHAPE,
            "requested_model":command_model(&link.route),
            "effective_model":session.and_then(|s|s["effective_model"].as_str()),
            "effective_model_status":if ready {"verified_by_session_config_readback"}else{"unknown"},
            "capabilities":{
                "open":"native_acp_session_new_with_model_readback",
                "task_dispatch":"exact_store_task_prompt_v1_acp_prompt",
                "reconcile":"durable_receipt_readback_no_prompt_replay",
                "refresh":"exact_session_and_capture_readback",
                "send":"next_turn_only",
                "steer":"unavailable",
                "configure":"route_model_immutable_after_verified_open",
                "reply":"exact_live_permission_fingerprint_and_one_time_native_option",
                "close":if close_supported {"advertised_session_close_with_response"} else {"unavailable"},
                "recover":"exact_session_load_if_advertised; no_prompt_replay",
                "result_pages":"bounded_exact_assistant_capture_with_completeness_gate",
                "native_children":"agent_tool_updates_only; direct_control_unavailable"
            }
        },
        "binding_id":link.binding_id,
        "generation":link.generation,
        "recovery_required":link.recovery_required,
        "last_operation":last_operation,
        "pending_permissions":pending,
        "history_updates":capture.map(|capture|capture.history_updates).unwrap_or(0),
        "native_tool_events":capture.map(|capture|capture.tool_events.clone()).unwrap_or_default(),
        "native_agent_children":capture.map(|capture|capture.agent_children.clone()).unwrap_or_default(),
        "native_family_coverage":"partial_when_native_ids_or_status_are_missing",
        "family_departure_claimed":false,
        "capture_retained":capture.is_some_and(|capture| capture.output_receipt_saved)
    })
}

fn session_state(session: Option<&ActiveSession>) -> Value {
    match session {
        Some(session) => json!({
            "native_session_id":session.id,
            "native_root_id":session.id,
            "native_scope_key":session.id,
            "native_session_state":if session.closed {"closed"} else {"open"},
            "workspace":session.workspace,
            "requested_model":session.model_id,
            "native_close_capability":session.close_supported,
            "family_departure_claimed":false
        }),
        None => json!({"native_session_state":"not_started","family_departure_claimed":false}),
    }
}

fn applied_in_session(
    command: &RuntimeCommand,
    session: &ActiveSession,
    mut details: Value,
) -> RuntimeOutcome {
    let identity = session.id.to_string();
    if let Some(fields) = details.as_object_mut() {
        fields.insert("native_session_id".to_owned(), json!(identity));
        fields.insert("native_root_id".to_owned(), json!(identity));
        fields.insert("native_scope_key".to_owned(), json!(identity));
        fields
            .entry("family_departure_claimed".to_owned())
            .or_insert(json!(false));
    }
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some(identity.clone()),
        native_root_id: Some(identity),
        turn_id: matches!(command.method.as_str(), "task.dispatch" | "agent.send")
            .then(|| command.operation_id.clone()),
        native_input_id: None,
        details,
    }
}

fn rejected_in_session(
    command: &RuntimeCommand,
    session: &ActiveSession,
    code: &str,
    extra: Value,
) -> RuntimeOutcome {
    let mut details = json!({
        "execution_shape":ACP_EXECUTION_SHAPE,
        "error_code":code,
        "effect":"not_applied",
        "family_departure_claimed":false
    });
    if let (Some(target), Some(source)) = (details.as_object_mut(), extra.as_object()) {
        target.extend(source.clone());
    }
    session_outcome(command, session, EffectOutcome::Rejected, details)
}

fn unknown_in_session(
    command: &RuntimeCommand,
    session: &ActiveSession,
    reason: &str,
) -> RuntimeOutcome {
    session_outcome(
        command,
        session,
        EffectOutcome::Unknown,
        json!({
            "execution_shape":ACP_EXECUTION_SHAPE,
            "completion_condition":"unknown",
            "task_completion":"unknown",
            "recovery_reason":reason,
            "native_replay":false,
            "family_departure_claimed":false
        }),
    )
}

fn session_outcome(
    command: &RuntimeCommand,
    session: &ActiveSession,
    outcome: EffectOutcome,
    mut details: Value,
) -> RuntimeOutcome {
    let identity = session.id.to_string();
    if let Some(fields) = details.as_object_mut() {
        fields.insert("native_session_id".to_owned(), json!(identity));
        fields.insert("native_root_id".to_owned(), json!(identity));
        fields.insert("native_scope_key".to_owned(), json!(identity));
    }
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: Some(identity.clone()),
        native_root_id: Some(identity),
        turn_id: None,
        native_input_id: None,
        details,
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
        details: json!({"execution_shape":ACP_EXECUTION_SHAPE,"completion_condition":"unknown","task_completion":"unknown","recovery_reason":reason,"native_replay":false,"family_departure_claimed":false}),
    }
}

fn next_sequence(current: u64) -> Result<u64> {
    current.checked_add(1).ok_or_else(|| {
        Error::new(
            "MODULE_OBSERVATION_INVALID",
            "observation sequence overflowed",
        )
    })
}

fn take_observation_sequence(sequence: &Arc<Mutex<u64>>) -> Result<u64> {
    let mut current = sequence
        .lock()
        .map_err(|_| invalid("ACP observation sequence lock is poisoned"))?;
    let value = *current;
    *current = next_sequence(value)?;
    Ok(value)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES
    {
        return Err(invalid("ACP journal record is not a bounded regular file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(invalid("ACP journal record exceeded its read bound"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("ACP journal record is malformed"))
}

fn read_bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(invalid("ACP capture is not a bounded regular file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(invalid("ACP capture exceeded its read bound"));
    }
    Ok(bytes)
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(invalid("ACP journal record exceeds its write bound"));
    }
    write_private_new(path, &bytes)
}

fn to_acp_error(error: Error) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(error.code)
}

fn invalid(message: &'static str) -> Error {
    Error::new("ADAPTER_EVIDENCE_INVALID", message)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::{Value, json};
    use swarm_contracts::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome,
        runtime::TaskDispatchContext,
        task_prompt::{TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION, TaskPromptEnvelopeV1},
    };
    use uuid::Uuid;

    use super::{AcpJournal, PreparedAcpDispatch, acp_prompt, digest};

    struct TestState {
        state_dir: PathBuf,
        journal: AcpJournal,
    }

    impl TestState {
        fn new() -> super::Result<Self> {
            let state_dir =
                std::env::temp_dir().join(format!("swarm-command-acp-journal-{}", Uuid::new_v4()));
            fs::create_dir(&state_dir)?;
            let journal = AcpJournal::new(&state_dir)?;
            Ok(Self { state_dir, journal })
        }
    }

    impl Drop for TestState {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.state_dir);
        }
    }

    fn task_prompt_fixture() -> super::Result<(RuntimeCommand, PreparedAcpDispatch)> {
        // Use the contracts' typed TaskPrompt/context, without manufacturing a
        // Store admission receipt or other authorization evidence.
        let operation_id = format!("op-{}", Uuid::new_v4());
        let binding_id = format!("binding-{}", Uuid::new_v4());
        let task_id = format!("task-{}", Uuid::new_v4());
        let attempt_id = format!("attempt-{}", Uuid::new_v4());
        let prompt = "Run the admitted task once.".to_owned();
        let task_snapshot_sha256 = digest(b"typed TaskPrompt test snapshot");
        let envelope = TaskPromptEnvelopeV1 {
            schema_id: TASK_PROMPT_SCHEMA_ID.to_owned(),
            schema_version: TASK_PROMPT_SCHEMA_VERSION,
            task_id: task_id.clone(),
            task_revision: 1,
            attempt_id: attempt_id.clone(),
            task_snapshot_sha256: task_snapshot_sha256.clone(),
            prompt_sha256: digest(prompt.as_bytes()),
            prompt_bytes: prompt.len() as u64,
            prompt,
        };
        let context = TaskDispatchContext {
            schema_version: 1,
            operation_id: operation_id.clone(),
            binding_id: binding_id.clone(),
            binding_generation: 1,
            worker_boot_id: Uuid::new_v4().to_string(),
            attempt_id,
            task_id,
            task_revision: 1,
            task_snapshot_sha256,
            source_text_sha256: digest(b"TaskPrompt source text"),
            source_text_bytes: 22,
        };
        let input = json!({
            "task_prompt": envelope,
            "task_dispatch_context": context
        });
        let input_sha256 = digest(serde_json::to_vec(&input)?.as_slice());
        let command = RuntimeCommand {
            operation_id,
            method: "task.dispatch".to_owned(),
            created_at_ms: 1,
            binding_id,
            generation: 1,
            native_root_id: None,
            route: json!({"native_options":{"modelId":"fixture-model"}}),
            input,
            input_sha256: Some(input_sha256),
            target_input_sha256: None,
        };
        let prepared = acp_prompt::prepare(&command)?;
        Ok((command, prepared))
    }

    fn assert_error_code<T>(result: super::Result<T>, expected: &str) {
        assert!(result.is_err(), "expected error code {expected}");
        if let Err(error) = result {
            assert_eq!(error.code, expected);
        }
    }

    fn outcome(command: &RuntimeCommand, reason: &str) -> RuntimeOutcome {
        RuntimeOutcome {
            operation_id: command.operation_id.clone(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({"recovery_reason":reason,"native_replay":false}),
        }
    }

    #[test]
    fn exact_command_admits_typed_task_prompt_once() -> super::Result<()> {
        let state = TestState::new()?;
        let (command, prepared) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;

        let (directory, existing) = state.journal.admit(&command, &prepared, None)?;
        assert!(!existing);
        assert_eq!(
            fs::read(directory.join("command.json"))?,
            serde_json::to_vec(&command)?
        );
        assert!(directory.join("admission.json").is_file());

        let (same_directory, existing) = state.journal.admit(&command, &prepared, None)?;
        assert!(existing);
        assert_eq!(same_directory, directory);
        assert!(!directory.join("prompt-state.json").exists());
        Ok(())
    }

    #[test]
    fn altered_retained_command_cannot_be_admitted() -> super::Result<()> {
        let state = TestState::new()?;
        let (command, prepared) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;
        let altered = RuntimeCommand {
            created_at_ms: command.created_at_ms + 1,
            ..command
        };

        assert_error_code(
            state.journal.admit(&altered, &prepared, None),
            "ADAPTER_EVIDENCE_INVALID",
        );
        Ok(())
    }

    #[test]
    fn preadmission_effect_without_admission_marker_is_rejected() -> super::Result<()> {
        let state = TestState::new()?;
        let (command, prepared) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;
        state.journal.record_effect(
            &command,
            "session/new",
            "intent",
            json!({"native_effect":"must_not_precede_admission"}),
        )?;

        assert_error_code(
            state.journal.admit(&command, &prepared, None),
            "ADAPTER_EVIDENCE_INVALID",
        );
        Ok(())
    }

    #[test]
    fn mismatched_saved_admission_schema_is_rejected() -> super::Result<()> {
        let state = TestState::new()?;
        let (command, prepared) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;
        let (directory, existing) = state.journal.admit(&command, &prepared, None)?;
        assert!(!existing);

        let path = directory.join("admission.json");
        let mut saved: Value = serde_json::from_slice(&fs::read(&path)?)?;
        saved["schema_version"] = json!(2);
        fs::write(path, serde_json::to_vec(&saved)?)?;

        assert_error_code(
            state.journal.admit(&command, &prepared, None),
            "ADAPTER_EVIDENCE_CONFLICT",
        );
        Ok(())
    }

    #[test]
    fn outcome_and_result_page_stay_pending_until_acknowledged() -> super::Result<()> {
        let state = TestState::new()?;
        let (command, _) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;

        let saved_outcome = outcome(&command, "native_result_unknown");
        let outcome_hash = state.journal.save_outcome(&saved_outcome)?;
        for _ in 0..2 {
            let pending = state.journal.pending_outcomes()?;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].0.operation_id, command.operation_id);
            assert_eq!(pending[0].1, outcome_hash);
        }
        assert_error_code(
            state
                .journal
                .save_outcome(&outcome(&command, "changed outcome")),
            "ADAPTER_EVIDENCE_CONFLICT",
        );
        state.journal.acknowledge(
            &command.operation_id,
            &outcome_hash,
            json!({"recorded":true}),
        )?;
        assert!(state.journal.pending_outcomes()?.is_empty());

        let page = json!({
            "operation_id":command.operation_id.clone(),
            "page":{"text":"retained result page","complete":false}
        });
        let page_hash = state
            .journal
            .save_result_page(&command.operation_id, &page)?;
        for _ in 0..2 {
            let pending = state.journal.pending_result_pages()?;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].0, page);
            assert_eq!(pending[0].1, page_hash);
        }
        let changed_page = json!({
            "operation_id":command.operation_id.clone(),
            "page":{"text":"changed result page","complete":false}
        });
        assert_error_code(
            state
                .journal
                .save_result_page(&command.operation_id, &changed_page),
            "ADAPTER_EVIDENCE_CONFLICT",
        );
        assert_error_code(
            state.journal.acknowledge_result_page(
                &command.operation_id,
                &page_hash,
                json!({"recorded":false}),
            ),
            "MODULE_RESULT_ACK_INVALID",
        );
        state.journal.acknowledge_result_page(
            &command.operation_id,
            &page_hash,
            json!({"recorded":true,"artifact_ref":"artifact:page-1"}),
        )?;
        assert!(state.journal.pending_result_pages()?.is_empty());
        Ok(())
    }
}
