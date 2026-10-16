//! Native ACP session path for the separately registered Command ACP artifact.
//!
//! Only Store's versioned TaskPrompt envelope is admitted for task dispatch.
//! The journal is immutable at the native-effect boundary: an uncertain prompt
//! is read back as Unknown and is never sent a second time.

use std::{
    collections::HashMap,
    fs::{self, File},
    future::Future,
    io::Read,
    path::{Path, PathBuf},
    pin::Pin,
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
use tokio_util::codec::{Decoder, FramedRead, FramedWrite, LinesCodec};

use super::{Config, Link, Owner};
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
const OUTPUT_ROOT_V1: &str = "command-acp-runs-v1";
const OUTPUT_ROOT_V2: &str = "command-acp-runs-v2";

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
    session_schema_version: u8,
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
    module_ipc_disconnected: bool,
    updates: u64,
    history_updates: u64,
    history_replaying: bool,
    current_mode_id: Option<String>,
    current_mode_updates: u64,
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
        self.module_ipc_disconnected = false;
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
    requested_model: Option<String>,
    effective_model: Option<String>,
    requested_effort: Option<String>,
    effective_effort: Option<String>,
    requested_mode: Option<String>,
    effective_mode: Option<String>,
    close_supported: bool,
    closed: bool,
}

struct SessionOpenContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    journal: &'a AcpJournal,
    command: &'a RuntimeCommand,
    workspace: &'a Path,
    owner: &'a Owner,
    features: &'a AgentFeatures,
    capture: &'a Arc<Mutex<Capture>>,
}

struct SessionRouteConfiguration {
    model_id: Option<String>,
    effort: Option<String>,
    mode: Option<String>,
}

struct SelectConfigSetting<'a> {
    field: &'a str,
    option: &'a SessionConfigOption,
    requested: &'a str,
}

struct SessionModeConfigContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    journal: &'a AcpJournal,
    command: &'a RuntimeCommand,
    session_id: &'a SessionId,
    requested_mode: Option<&'a str>,
    advertised_modes: Option<&'a SessionModeState>,
    options: &'a mut Vec<SessionConfigOption>,
    capture: &'a Arc<Mutex<Capture>>,
}

struct CommandContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    link: &'a mut Link,
    recovery: &'a ModuleLinkRecovery<'a>,
    owner: &'a Owner,
    journal: &'a AcpJournal,
    features: &'a AgentFeatures,
    session: &'a mut Option<ActiveSession>,
    workspace: Option<&'a Path>,
    model: Option<&'a str>,
    capture: &'a Arc<Mutex<Capture>>,
    permissions: &'a PermissionBroker,
    permission_wakeup: &'a Notify,
    observation_sequence: &'a Arc<Mutex<u64>>,
}

struct ModuleLinkRecovery<'a> {
    config: &'a Config,
    credential: &'a swarm_contracts::Credential,
    owner: &'a Owner,
    journal: &'a AcpJournal,
    capture: &'a Arc<Mutex<Capture>>,
}

struct SessionRestoreContext<'a> {
    connection: &'a ConnectionTo<Agent>,
    journal: &'a AcpJournal,
    binding_id: &'a str,
    generation: i64,
    route: &'a Value,
    owner: &'a Owner,
    features: &'a AgentFeatures,
    capture: &'a Arc<Mutex<Capture>>,
}

struct RestoreIdentity {
    session_text: String,
    session_id: SessionId,
    workspace: PathBuf,
    requested_model: Option<String>,
    requested_effort: Option<String>,
    requested_mode: Option<String>,
    owner_operation_id: String,
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

struct AcpLineCodec {
    lines: LinesCodec,
}

impl AcpLineCodec {
    fn new(max_frame_bytes: usize) -> Self {
        Self {
            lines: LinesCodec::new_with_max_length(max_frame_bytes),
        }
    }
}

impl Decoder for AcpLineCodec {
    type Item = String;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        source: &mut tokio_util::bytes::BytesMut,
    ) -> std::io::Result<Option<Self::Item>> {
        self.lines
            .decode(source)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }

    fn decode_eof(
        &mut self,
        source: &mut tokio_util::bytes::BytesMut,
    ) -> std::io::Result<Option<Self::Item>> {
        if !source.is_empty() && !source.contains(&b'\n') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unterminated ACP line at EOF",
            ));
        }
        self.lines
            .decode_eof(source)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
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
        Self::new_with_namespace(state_dir, OUTPUT_ROOT_V1, 1)
    }

    fn new_for_profile(state_dir: &Path, profile: Profile) -> Result<Self> {
        match profile {
            Profile::AcpV1 => Self::new(state_dir),
            Profile::AcpV2 => Self::new_with_namespace(state_dir, OUTPUT_ROOT_V2, 2),
            _ => Err(invalid("ACP journal requires an ACP artifact profile")),
        }
    }

    fn new_with_namespace(
        state_dir: &Path,
        namespace: &str,
        session_schema_version: u8,
    ) -> Result<Self> {
        let state = fs::canonicalize(state_dir)?;
        let root = state.join(namespace);
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
        Ok(Self {
            root,
            session_schema_version,
        })
    }

    fn session_schema_version(&self) -> u8 {
        self.session_schema_version
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

    fn read_effect(
        &self,
        command: &RuntimeCommand,
        effect_id: &str,
        phase: &str,
    ) -> Result<Option<Value>> {
        if effect_id.trim().is_empty()
            || effect_id.len() > 256
            || !matches!(phase, "intent" | "receipt")
        {
            return Err(invalid("ACP effect identity or phase is outside its bound"));
        }
        let path = self.operation_dir(&command.operation_id)?.join(format!(
            "effect-{phase}-{}.json",
            digest(effect_id.as_bytes())
        ));
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(invalid("saved ACP effect is not a regular file"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let bytes = read_bytes(&path, MAX_RECORD_BYTES as usize)?;
        let saved: Value = serde_json::from_slice(&bytes)?;
        if saved["schema_version"] != 1
            || saved["operation_id"] != command.operation_id
            || saved["effect_id"] != effect_id
            || saved["phase"] != phase
            || !saved["evidence"].is_object()
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "saved ACP effect identity or evidence is invalid",
            ));
        }
        Ok(Some(saved["evidence"].clone()))
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
            || !crate::is_lower_sha256_hex(&receipt.stored_sha256)
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
    if !matches!(owner.host.profile, Profile::AcpV1 | Profile::AcpV2)
        || !module_host::task_prompt_v1_enabled(&owner.host.claim)
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "ACP requires the registered TaskPrompt v1 command and admission schemas",
        ));
    }
    let journal = AcpJournal::new_for_profile(&owner.state_dir, owner.host.profile)?;
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
            false,
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
        validate_acp_route_for_profile(&link.route, owner.host.profile)?;

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
            if let Err(error) = super::validate_command(&command, &link, retained_session_id) {
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
            let model = command_model(&link.route).map(str::to_owned);
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
            let incoming = FramedRead::new(child_stdout, AcpLineCodec::new(MAX_ACP_FRAME_BYTES))
                .map(|result| result.map_err(|error| std::io::Error::other(error.to_string())));
            let transport = Lines::new(outgoing, incoming);
            let stderr_task = tokio::spawn(drain_native_stderr(child_stderr, Arc::clone(&capture)));
            let connection_capture = Arc::clone(&capture);
            let connection_active_operation = Arc::clone(&active_operation);
            let connection_journal = journal.clone();
            let connection_owner = owner.clone();
            let connection_config = config.clone();
            let connection_credential = credential.clone();
            let run = Client
                .builder()
                .name("eliot-command-acp")
                .on_receive_notification(
                    async move |notification: SessionNotification, _connection| {
                        if let Ok(mut capture) = notification_capture.lock() {
                            capture_session_notification(&mut capture, &notification);
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
                    let config = connection_config;
                    let credential = connection_credential;
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
                        let recovery = ModuleLinkRecovery {
                            config: &config,
                            credential: &credential,
                            owner: &owner,
                            journal: &journal,
                            capture: &capture,
                        };
                        let features = initialize(&connection).await?;
                        if let Some(saved) = journal.read_session().map_err(to_acp_error)? {
                            current_session = Some(
                                restore_session(
                                    SessionRestoreContext {
                                        connection: &connection,
                                        journal: &journal,
                                        binding_id: &link.binding_id,
                                        generation: link.generation,
                                        route: &link.route,
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
                                    Err(error) if super::is_transport_uncertain(&error) => {
                                        recovery.reconnect(&mut link, false).await.map_err(to_acp_error)?;
                                        continue;
                                    }
                                    Err(error) => return Err(to_acp_error(error)),
                                };
                                if next["command"].is_null() { continue; }
                                let command: RuntimeCommand = match serde_json::from_value(next["command"].clone()) {
                                    Ok(command) => command,
                                    Err(_) => return Err(agent_client_protocol::Error::invalid_params().data("module.next returned an invalid ACP command")),
                                };
                                journal.record_command(&command).map_err(to_acp_error)?;
                                if let Some((saved, hash)) = journal
                                    .read_outcome(&command.operation_id)
                                    .map_err(to_acp_error)?
                                {
                                    recovery
                                        .send_saved_outcome(&mut link, &saved, &hash)
                                        .await
                                        .map_err(to_acp_error)?;
                                    continue;
                                }
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
                                    recovery: &recovery,
                                    owner: &owner,
                                    journal: &journal,
                                    features: &features,
                                    session: &mut current_session,
                                    workspace: workspace.as_deref(),
                                    model: model.as_deref(),
                                    capture: &capture,
                                    permissions: &permission_broker,
                                    permission_wakeup: &permission_wakeup,
                                    observation_sequence: &observation_sequence,
                                };
                                let expected_native_root_id = context.session.as_ref().map(|session| session.id.to_string());
                                if let Err(error) = super::validate_command(&command, context.link, expected_native_root_id.as_deref()) {
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
                                recovery
                                    .send_saved_outcome(&mut link, &outcome, &hash)
                                    .await
                                    .map_err(to_acp_error)?;
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
                            recovery
                                .send_observation(&mut link, sequence, state)
                                .await
                                .map_err(to_acp_error)?;
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
            let mut snapshot = capture
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
                            "module_ipc_disconnected":capture.module_ipc_disconnected
                        })
                })
                .unwrap_or(Value::Null);
            add_family_custody_facts(&mut snapshot, &child_exit);
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
            let module_ipc_disconnected = snapshot["module_ipc_disconnected"] == true;
            journal.mark_session_recovery(
                session_recovery_reason(run.is_err(), module_ipc_disconnected),
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

fn begin_history_replay(capture: &mut Capture, session_id: &str) {
    capture.native_session_id = Some(session_id.to_owned());
    capture.history_replaying = true;
}

fn finish_history_replay(capture: &mut Capture) {
    capture.history_replaying = false;
}

fn capture_session_notification(capture: &mut Capture, notification: &SessionNotification) {
    let Ok(bytes) = serde_json::to_vec(notification) else {
        return;
    };
    let session_id = notification.session_id.to_string();
    if capture.native_session_id.as_deref() != Some(session_id.as_str()) {
        capture.protocol_error = Some("ACP update named a different session".to_owned());
        return;
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
    if let SessionUpdate::CurrentModeUpdate(update) = &notification.update {
        capture.current_mode_id = Some(update.current_mode_id.to_string());
        capture.current_mode_updates = capture.current_mode_updates.saturating_add(1);
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

fn add_family_custody_facts(snapshot: &mut Value, child_exit: &DirectChildExit) {
    let Some(fields) = snapshot.as_object_mut() else {
        return;
    };
    fields.insert(
        "direct_child_exit_observed".to_owned(),
        json!(child_exit.observed),
    );
    fields.insert(
        "direct_child_exit_code".to_owned(),
        json!(child_exit.exit_code),
    );
    fields.insert(
        "direct_child_kill_requested".to_owned(),
        json!(child_exit.kill_requested),
    );
    fields.insert(
        "cleanup_pending".to_owned(),
        json!(child_exit.cleanup_pending),
    );
    fields.insert("family_departure_claimed".to_owned(), json!(false));
    fields.insert("owner_family_departure_required".to_owned(), json!(true));
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
                let dispatch =
                    acp_prompt::prepare(command, context.owner.host.profile.model_optional())?;
                let receipt = adapter::acp_dispatch_admission(context.owner, command, &dispatch)?;
                let (directory, existing) =
                    context
                        .journal
                        .admit(command, &dispatch, Some(receipt.clone()))?;
                if existing {
                    return Ok(vec![retained_dispatch_outcome(
                        context.journal,
                        command,
                        context.session.as_ref(),
                    )?]);
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
                let route_configuration = SessionRouteConfiguration {
                    model_id: context.model.map(str::to_owned),
                    effort: command_effort(&context.link.route).map(str::to_owned),
                    mode: command_mode(&context.link.route).map(str::to_owned),
                };
                *context.session = Some(
                    open_session(
                        SessionOpenContext {
                            connection: context.connection,
                            journal: context.journal,
                            command,
                            workspace,
                            owner: context.owner,
                            features: context.features,
                            capture: context.capture,
                        },
                        &route_configuration,
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
                        "effective_model":session.effective_model,
                        "effective_model_status":session_model_status(
                            session.requested_model.as_deref(),
                            session.effective_model.as_deref(),
                        ),
                        "requested_effort":session.requested_effort,
                        "effective_effort":session.effective_effort,
                        "effort_readback_status":if session.requested_effort.is_none() || session.effective_effort.is_some() {"verified_or_unselected"}else{"unknown"},
                        "requested_mode":session.requested_mode,
                        "effective_mode":session.effective_mode,
                        "mode_readback_status":if session.requested_mode.is_none() || session.effective_mode.is_some() {"verified_or_unselected"}else{"unknown"},
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
                            "execution_complete":true,
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
                            "dispatch_admission":admission,
                            "family_departure_claimed":false
                        }),
                    )
                }
                Err(error) => unknown_in_session(command, session, &error.code),
            };
            Ok(vec![outcome])
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
            let details = reconciliation_details(context.journal, target)?;
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
            let (session_id, target) = {
                let session = context.session.as_ref().ok_or_else(|| {
                    Error::new("NATIVE_SESSION_MISSING", "ACP session is not open")
                })?;
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
                let (target_outcome, _) =
                    context.journal.read_outcome(target)?.ok_or_else(|| {
                        Error::new(
                            "NATIVE_IDENTITY_MISMATCH",
                            "ACP close target has no retained outcome",
                        )
                    })?;
                if target_outcome.native_root_id.as_deref() != Some(session.id.to_string().as_str())
                {
                    return Err(Error::new(
                        "NATIVE_IDENTITY_MISMATCH",
                        "ACP close target does not name this exact session",
                    ));
                }
                (session.id.clone(), target.to_owned())
            };
            cancel_pending_permissions(context.journal, context.permissions, command, &session_id)
                .await?;
            let expected_intent = json!({
                "native_session_id":session_id,
                "target_operation_id":target
            });
            let close_intent = context
                .journal
                .read_effect(command, "session/close", "intent")?;
            if close_intent
                .as_ref()
                .is_some_and(|evidence| evidence != &expected_intent)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "saved ACP close intent differs from its exact session and target",
                ));
            }
            let close_receipt = context
                .journal
                .read_effect(command, "session/close", "receipt")?;
            if close_receipt.as_ref().is_some_and(|evidence| {
                evidence["native_session_id"] != session_id.to_string()
                    || evidence["response"] != "confirmed"
            }) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "saved ACP close receipt differs from its exact session",
                ));
            }
            if close_receipt.is_none() {
                if close_intent.is_some() {
                    return Err(Error::new(
                        "ACP_CLOSE_UNKNOWN",
                        "ACP close intent exists without a confirmed receipt; native close will not be replayed",
                    ));
                }
                context.journal.record_effect(
                    command,
                    "session/close",
                    "intent",
                    expected_intent,
                )?;
                context
                    .connection
                    .send_request(CloseSessionRequest::new(session_id.clone()))
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
                        json!({"native_session_id":session_id,"response":"confirmed"}),
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
            } else if close_intent.is_none() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "ACP close receipt has no recorded intent",
                ));
            }
            let session = context
                .session
                .as_mut()
                .ok_or_else(|| Error::new("NATIVE_SESSION_MISSING", "ACP session is not open"))?;
            session.closed = true;
            context.journal.save_session(&json!({
                "schema_version":context.journal.session_schema_version(),
                "artifact_id":ACP_ARTIFACT_ID,
                "binding_id":command.binding_id,
                "binding_generation":command.generation,
                "operation_id":session.owner_operation_id,
                "native_session_id":session.id,
                "native_root_id":session.id,
                "native_scope_key":session.id,
                "workspace":session.workspace,
                "requested_model":session.requested_model,
                "requested_effort":session.requested_effort,
                "effective_effort":session.effective_effort,
                "requested_mode":session.requested_mode,
                "effective_mode":session.effective_mode,
                "effective_model":session.effective_model,
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
fn retained_dispatch_outcome(
    journal: &AcpJournal,
    command: &RuntimeCommand,
    session: Option<&ActiveSession>,
) -> Result<RuntimeOutcome> {
    if let Some((saved, _)) = journal.read_outcome(&command.operation_id)? {
        return Ok(saved);
    }
    Ok(session.map_or_else(
        || unknown(command, "native_prompt_may_have_started_no_replay"),
        |session| unknown_in_session(command, session, "native_prompt_may_have_started_no_replay"),
    ))
}

fn reconciliation_details(journal: &AcpJournal, target: &str) -> Result<Value> {
    if let Some((saved, hash)) = journal.read_outcome(target)? {
        Ok(json!({
            "target_operation_id":target,
            "target_outcome_sha256":hash,
            "target_outcome":saved.outcome,
            "native_replay":false
        }))
    } else {
        Ok(json!({
            "target_operation_id":target,
            "completion_condition":"native_result_not_durably_known",
            "task_completion":"unknown",
            "native_replay":false
        }))
    }
}

async fn open_session(
    context: SessionOpenContext<'_>,
    route: &SessionRouteConfiguration,
) -> Result<ActiveSession> {
    open_session_with_policy(context, route, false).await
}

async fn open_session_with_policy(
    context: SessionOpenContext<'_>,
    route: &SessionRouteConfiguration,
    require_initial_values: bool,
) -> Result<ActiveSession> {
    let SessionOpenContext {
        connection,
        journal,
        command,
        workspace,
        owner,
        features,
        capture,
    } = context;
    let requested_model = route.model_id.as_deref();
    let requested_effort = route.effort.as_deref();
    let requested_mode = route.mode.as_deref();
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
            "requested_effort":requested_effort,
            "requested_mode":requested_mode,
            "load_session_fallback":false
        }),
    )?;
    let opened = connection
        .send_request(NewSessionRequest::new(workspace))
        .block_task()
        .await
        .map_err(|native_error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "ACP session/new failed with native JSON-RPC code {}; response is uncertain",
                    i32::from(native_error.code)
                ),
            )
        })?;
    let session_id = opened.session_id;
    let mut options = opened.config_options.clone().unwrap_or_default();
    let mut session = ActiveSession {
        id: session_id.clone(),
        owner_operation_id: command.operation_id.clone(),
        workspace: workspace.to_path_buf(),
        requested_model: route.model_id.clone(),
        effective_model: None,
        requested_effort: requested_effort.map(str::to_owned),
        effective_effort: category_config_value(
            &options,
            &SessionConfigOptionCategory::ThoughtLevel,
        ),
        requested_mode: requested_mode.map(str::to_owned),
        effective_mode: mode_current_value(opened.modes.as_ref())
            .or_else(|| category_config_value(&options, &SessionConfigOptionCategory::Mode)),
        close_supported: false,
        closed: false,
    };
    {
        let mut capture = capture
            .lock()
            .map_err(|_| invalid("ACP capture lock is poisoned after session/new"))?;
        capture.native_session_id = Some(session_id.to_string());
        capture.current_mode_id = session.effective_mode.clone();
    }
    let close_supported = features.close_supported
        && owner
            .host
            .claim
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "native.command.close_session");
    session.close_supported = close_supported;
    journal
        .save_session(&json!({
            "schema_version":journal.session_schema_version(),
            "artifact_id":ACP_ARTIFACT_ID,
            "binding_id":command.binding_id,
            "binding_generation":command.generation,
            "operation_id":command.operation_id,
            "native_session_id":session.id,
            "native_root_id":session.id,
            "native_scope_key":session.id,
            "workspace":session.workspace,
            "requested_model":requested_model,
            "requested_effort":session.requested_effort,
            "requested_mode":session.requested_mode,
            "effective_model":Value::Null,
            "effective_effort":session.effective_effort,
            "effective_mode":session.effective_mode,
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
            "workspace":session.workspace,
            "requested_model":requested_model,
            "requested_effort":session.requested_effort,
            "requested_mode":session.requested_mode,
            "initial_config_options":session_config_options_snapshot(&options).map_err(|error| {
                Error::new(
                    "ACP_SESSION_OPEN_UNKNOWN",
                    format!("initial ACP configuration could not be retained: {}", error.code),
                )
            })?,
            "initial_mode_id":mode_current_value(opened.modes.as_ref()),
            "initial_mode_value_ids":opened.modes.as_ref().map(|modes| {
                modes.available_modes.iter().map(|mode| mode.id.to_string()).collect::<Vec<_>>()
            })
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
    if require_initial_values
        && (requested_model.is_some_and(|model| !model_current_value_matches(&options, model))
            || requested_effort.is_some_and(|effort| {
                !category_config_value_matches(
                    &options,
                    &SessionConfigOptionCategory::ThoughtLevel,
                    effort,
                )
            })
            || requested_mode.is_some_and(|mode| {
                if opened.modes.is_some() {
                    mode_current_value(opened.modes.as_ref()).as_deref() != Some(mode)
                } else {
                    !category_config_value_matches(
                        &options,
                        &SessionConfigOptionCategory::Mode,
                        mode,
                    )
                }
            }))
    {
        return Err(Error::new(
            "CONFIGURATION_UNSUPPORTED",
            "ACP live qualification requires the exact route values to match initial typed current values; no selector was changed",
        ));
    }
    if let Some(requested_model) = requested_model {
        if !model_value_available(&options, requested_model) {
            return Err(Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP did not advertise one unambiguous typed option with the exact routed model value; the retained session requires readback",
            ));
        }
        let option_id = model_config_option_id(&options, requested_model).ok_or_else(|| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP model option identity is ambiguous; the retained session requires readback",
            )
        })?;
        let option = config_option_by_id(&options, &option_id).ok_or_else(|| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP model option ID is missing or ambiguous in the advertised configuration",
            )
        })?;
        options = set_select_config_option(
            connection,
            journal,
            command,
            &session_id,
            SelectConfigSetting {
                field: "model",
                option,
                requested: requested_model,
            },
        )
        .await?;
        if !model_current_value_matches(&options, requested_model) {
            return Err(Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP effective model did not match the requested model; the retained session requires readback",
            ));
        }
    }
    session.effective_model = effective_model_value(&options, requested_model);
    if let Some(effort) = requested_effort {
        let option = category_config_option(&options, &SessionConfigOptionCategory::ThoughtLevel)
            .filter(|option| config_select_contains(option, effort))
            .ok_or_else(|| {
                Error::new(
                    "CONFIGURATION_UNSUPPORTED",
                    "ACP did not advertise one unambiguous thought_level option with the exact requested effort",
                )
            })?;
        options = set_select_config_option(
            connection,
            journal,
            command,
            &session_id,
            SelectConfigSetting {
                field: "effort",
                option,
                requested: effort,
            },
        )
        .await?;
        if !category_config_value_matches(
            &options,
            &SessionConfigOptionCategory::ThoughtLevel,
            effort,
        ) {
            return Err(Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                "ACP effort configuration did not read back the exact requested value",
            ));
        }
    }
    session.effective_effort =
        category_config_value(&options, &SessionConfigOptionCategory::ThoughtLevel);

    session.effective_mode = configure_session_mode(SessionModeConfigContext {
        connection,
        journal,
        command,
        session_id: &session_id,
        requested_mode,
        advertised_modes: opened.modes.as_ref(),
        options: &mut options,
        capture,
    })
    .await?;
    if session.effective_mode.is_none() {
        session.effective_mode = capture_mode_readback(capture)?
            .0
            .or_else(|| category_config_value(&options, &SessionConfigOptionCategory::Mode));
    }
    let config_snapshot = session_config_options_snapshot(&options).map_err(|error| {
        Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            format!(
                "native ACP configuration options could not be retained: {}",
                error.code
            ),
        )
    })?;
    journal
        .save_session(&json!({
            "schema_version":journal.session_schema_version(),
            "artifact_id":ACP_ARTIFACT_ID,
            "binding_id":command.binding_id,
            "binding_generation":command.generation,
            "operation_id":session.owner_operation_id,
            "native_session_id":session.id,
            "native_root_id":session.id,
            "native_scope_key":session.id,
            "workspace":session.workspace,
            "requested_model":requested_model,
            "requested_effort":session.requested_effort,
            "requested_mode":session.requested_mode,
            "effective_model":session.effective_model,
            "model_readback":session.effective_model,
            "effective_effort":session.effective_effort,
            "effective_mode":session.effective_mode,
            "config_options":config_snapshot,
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

async fn set_select_config_option(
    connection: &ConnectionTo<Agent>,
    journal: &AcpJournal,
    command: &RuntimeCommand,
    session_id: &SessionId,
    setting: SelectConfigSetting<'_>,
) -> Result<Vec<SessionConfigOption>> {
    let SelectConfigSetting {
        field,
        option,
        requested,
    } = setting;
    let config_id = option.id.to_string();
    if !config_select_contains(option, requested) {
        return Err(Error::new(
            "CONFIGURATION_UNSUPPORTED",
            "ACP configuration option did not advertise the exact requested value",
        ));
    }
    let effect_id = format!("session/config-option/{field}");
    journal.record_effect(
        command,
        &effect_id,
        "intent",
        json!({
            "native_session_id":session_id,
            "config_id":config_id,
            "requested_value_id":requested
        }),
    )?;
    let configured = connection
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            config_id.clone(),
            SessionConfigOptionValue::value_id(requested.to_owned()),
        ))
        .block_task()
        .await
        .map_err(|native_error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "ACP config-option setter failed with native JSON-RPC code {}; response is uncertain after session creation",
                    i32::from(native_error.code)
                ),
            )
        })?;
    let actual =
        config_option_by_id(&configured.config_options, &config_id).and_then(config_select_current);
    let verified = actual.as_deref() == Some(requested);
    journal.record_effect(
        command,
        &effect_id,
        "receipt",
        json!({
            "native_session_id":session_id,
            "config_id":config_id,
            "requested_value_id":requested,
            "effective_value_id":actual,
            "readback_verified":verified
        }),
    )?;
    if !verified {
        return Err(Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP config-option setter did not read back the exact requested value",
        ));
    }
    Ok(configured.config_options)
}

async fn set_session_mode(
    connection: &ConnectionTo<Agent>,
    journal: &AcpJournal,
    command: &RuntimeCommand,
    session_id: &SessionId,
    requested_mode: &str,
    advertised_modes: Option<&SessionModeState>,
    capture: &Arc<Mutex<Capture>>,
) -> Result<String> {
    if !mode_value_available(advertised_modes, requested_mode) {
        return Err(Error::new(
            "CONFIGURATION_UNSUPPORTED",
            "ACP did not advertise one unambiguous session mode with the exact requested value",
        ));
    }
    let initial_mode = mode_current_value(advertised_modes);
    let already_selected = initial_mode.as_deref() == Some(requested_mode);
    let (_, updates_before) = capture_mode_readback(capture)?;
    journal.record_effect(
        command,
        "session/set-mode",
        "intent",
        json!({
            "native_session_id":session_id,
            "requested_mode_id":requested_mode
        }),
    )?;
    connection
        .send_request(SetSessionModeRequest::new(
            session_id.clone(),
            requested_mode.to_owned(),
        ))
        .block_task()
        .await
        .map_err(|native_error| {
            Error::new(
                "ACP_SESSION_OPEN_UNKNOWN",
                format!(
                    "ACP mode setter failed with native JSON-RPC code {}; response is uncertain after session creation",
                    i32::from(native_error.code)
                ),
            )
        })?;
    let (immediate_mode, immediate_updates) = capture_mode_readback(capture)?;
    if immediate_mode.as_deref() == Some(requested_mode)
        && (already_selected || immediate_updates > updates_before)
    {
        journal.record_effect(
            command,
            "session/set-mode",
            "receipt",
            json!({
                "native_session_id":session_id,
                "requested_mode_id":requested_mode,
                "effective_mode_id":requested_mode,
                "readback_verified":true,
                "readback_source":if immediate_updates > updates_before {"current_mode_update"}else{"session_new_current_mode"}
            }),
        )?;
        return Ok(requested_mode.to_owned());
    }
    let mode_readback = time::timeout(Duration::from_secs(2), async {
        loop {
            let (current, updates) = capture_mode_readback(capture)?;
            if updates > updates_before && current.as_deref() == Some(requested_mode) {
                return Ok::<_, Error>(current);
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP mode setter did not produce an exact current-mode update",
        )
    })??;
    let effective_mode = mode_readback.ok_or_else(|| {
        Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP mode update omitted its current mode ID",
        )
    })?;
    journal.record_effect(
        command,
        "session/set-mode",
        "receipt",
        json!({
            "native_session_id":session_id,
            "requested_mode_id":requested_mode,
            "effective_mode_id":effective_mode,
            "readback_verified":true
        }),
    )?;
    Ok(effective_mode)
}

async fn configure_session_mode(context: SessionModeConfigContext<'_>) -> Result<Option<String>> {
    let SessionModeConfigContext {
        connection,
        journal,
        command,
        session_id,
        requested_mode,
        advertised_modes,
        options,
        capture,
    } = context;
    let Some(requested_mode) = requested_mode else {
        return Ok(mode_current_value(advertised_modes)
            .or_else(|| category_config_value(options, &SessionConfigOptionCategory::Mode)));
    };
    if let Some(advertised_modes) = advertised_modes {
        return set_session_mode(
            connection,
            journal,
            command,
            session_id,
            requested_mode,
            Some(advertised_modes),
            capture,
        )
        .await
        .map(Some);
    }
    let option = category_config_option(options, &SessionConfigOptionCategory::Mode)
        .filter(|option| config_select_contains(option, requested_mode))
        .ok_or_else(|| {
            Error::new(
                "CONFIGURATION_UNSUPPORTED",
                "ACP did not advertise one unambiguous typed mode selector with the exact requested mode",
            )
        })?;
    *options = set_select_config_option(
        connection,
        journal,
        command,
        session_id,
        SelectConfigSetting {
            field: "mode",
            option,
            requested: requested_mode,
        },
    )
    .await?;
    let effective_mode = category_config_value(options, &SessionConfigOptionCategory::Mode);
    if effective_mode.as_deref() != Some(requested_mode) {
        return Err(Error::new(
            "ACP_SESSION_OPEN_UNKNOWN",
            "ACP mode selector did not read back the exact requested value",
        ));
    }
    Ok(effective_mode)
}

fn config_option_by_id<'a>(
    options: &'a [SessionConfigOption],
    requested_id: &str,
) -> Option<&'a SessionConfigOption> {
    let mut candidates = options
        .iter()
        .filter(|option| option.id.to_string() == requested_id);
    let option = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    Some(option)
}

fn validate_restore_identity(
    saved: &Value,
    binding_id: &str,
    generation: i64,
    route: &Value,
    load_session_supported: bool,
) -> Result<RestoreIdentity> {
    validate_restore_identity_for_schema(
        saved,
        binding_id,
        generation,
        route,
        load_session_supported,
        1,
        false,
    )
}

fn validate_restore_identity_for_schema(
    saved: &Value,
    binding_id: &str,
    generation: i64,
    route: &Value,
    load_session_supported: bool,
    session_schema_version: u8,
    model_optional: bool,
) -> Result<RestoreIdentity> {
    let session_text = saved["native_session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("retained ACP session has no native ID"))?;
    if saved["schema_version"].as_u64() != Some(u64::from(session_schema_version))
        || saved["artifact_id"] != ACP_ARTIFACT_ID
        || saved["binding_id"] != binding_id
        || saved["binding_generation"].as_i64() != Some(generation)
        || saved["native_root_id"] != session_text
        || saved["native_scope_key"] != session_text
        || saved["native_session_state"] == "closed"
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "retained ACP session identity is closed or differs from this exact binding",
        ));
    }
    if !load_session_supported {
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
    let route_workspace = route["native_options"]["workspaceRoot"]
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
    let requested_model = command_model(route).map(str::to_owned);
    let model_field_present = route["native_options"]
        .get("modelId")
        .is_some_and(|value| !value.is_null());
    if (requested_model.is_none() && !model_optional)
        || (model_field_present && requested_model.is_none())
        || saved["requested_model"] != json!(requested_model)
        || (session_schema_version >= 2
            && saved.as_object().is_none_or(|fields| {
                !fields.contains_key("requested_model") || !fields.contains_key("effective_model")
            }))
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "retained ACP session model differs from the immutable binding route",
        ));
    }
    let requested_effort = command_effort(route).map(str::to_owned);
    if saved["requested_effort"].as_str() != requested_effort.as_deref() {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "retained ACP session effort differs from the immutable binding route",
        ));
    }
    let requested_mode = command_mode(route).map(str::to_owned);
    if saved["requested_mode"].as_str() != requested_mode.as_deref() {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "retained ACP session mode differs from the immutable binding route",
        ));
    }
    let owner_operation_id = saved["operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("retained ACP session omitted its owner Operation"))?
        .to_owned();
    Ok(RestoreIdentity {
        session_text: session_text.to_owned(),
        session_id,
        workspace,
        requested_model,
        requested_effort,
        requested_mode,
        owner_operation_id,
    })
}

async fn load_retained_session(
    connection: &ConnectionTo<Agent>,
    capture: &Arc<Mutex<Capture>>,
    session_text: &str,
    session_id: &SessionId,
    workspace: &Path,
) -> Result<LoadSessionResponse> {
    {
        let mut capture = capture.lock().map_err(|_| {
            Error::new(
                "RECOVERY_REQUIRED",
                "ACP capture state unavailable before session/load",
            )
        })?;
        begin_history_replay(&mut capture, session_text);
    }
    let loaded = connection
        .send_request(LoadSessionRequest::new(
            session_id.clone(),
            workspace.to_path_buf(),
        ))
        .block_task()
        .await;
    {
        let mut capture = capture.lock().map_err(|_| {
            Error::new(
                "RECOVERY_REQUIRED",
                "ACP capture state unavailable after session/load",
            )
        })?;
        finish_history_replay(&mut capture);
    }
    loaded.map_err(|native_error| {
        Error::new(
            "RECOVERY_REQUIRED",
            format!(
                "ACP session/load failed with native JSON-RPC code {}; original TaskPrompt will not be replayed",
                i32::from(native_error.code)
            ),
        )
    })
}

fn validate_restore_configuration(
    options: &[SessionConfigOption],
    modes: Option<&SessionModeState>,
    observed_mode_id: Option<&str>,
    requested_model: Option<&str>,
    requested_effort: Option<&str>,
    requested_mode: Option<&str>,
    saved: &Value,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let effective_model = effective_model_value(options, requested_model);
    let retained_observed_model = saved["effective_model"].as_str();
    if requested_model.is_some_and(|requested| effective_model.as_deref() != Some(requested))
        || (saved["schema_version"].as_u64() == Some(2)
            && requested_model.is_none()
            && retained_observed_model
                .is_some_and(|retained| effective_model.as_deref() != Some(retained)))
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "ACP session/load did not read back the retained model selection",
        ));
    }
    let effective_effort =
        category_config_value(options, &SessionConfigOptionCategory::ThoughtLevel);
    if requested_effort.is_some_and(|requested| {
        !category_config_value_matches(
            options,
            &SessionConfigOptionCategory::ThoughtLevel,
            requested,
        )
    }) || saved["effective_effort"]
        .as_str()
        .is_some_and(|retained| effective_effort.as_deref() != Some(retained))
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "ACP session/load did not read back the retained exact effort configuration",
        ));
    }
    let effective_mode = mode_readback_value(options, modes, observed_mode_id);
    if requested_mode.is_some_and(|requested| effective_mode.as_deref() != Some(requested))
        || saved["effective_mode"]
            .as_str()
            .is_some_and(|retained| effective_mode.as_deref() != Some(retained))
    {
        return Err(Error::new(
            "RECOVERY_REQUIRED",
            "ACP session/load did not read back the retained exact mode",
        ));
    }
    Ok((effective_model, effective_effort, effective_mode))
}

async fn restore_session(
    context: SessionRestoreContext<'_>,
    command: &RuntimeCommand,
    saved: Value,
) -> Result<ActiveSession> {
    let SessionRestoreContext {
        connection,
        journal,
        binding_id,
        generation,
        route,
        owner,
        features,
        capture,
    } = context;
    let RestoreIdentity {
        session_text,
        session_id,
        workspace,
        requested_model,
        requested_effort,
        requested_mode,
        owner_operation_id,
    } = if journal.session_schema_version() == 1 {
        validate_restore_identity(
            &saved,
            binding_id,
            generation,
            route,
            features.load_session_supported,
        )?
    } else {
        validate_restore_identity_for_schema(
            &saved,
            binding_id,
            generation,
            route,
            features.load_session_supported,
            journal.session_schema_version(),
            owner.host.profile.model_optional(),
        )?
    };
    journal.record_effect(
        command,
        "session/load",
        "intent",
        json!({
            "native_session_id":session_id,
            "native_root_id":session_id,
            "native_scope_key":session_id,
            "workspace":workspace,
            "requested_model":requested_model,
            "native_replay":false
        }),
    )?;
    let loaded =
        load_retained_session(connection, capture, &session_text, &session_id, &workspace).await?;
    let history_updates = capture
        .lock()
        .map_err(|_| Error::new("RECOVERY_REQUIRED", "ACP history capture state unavailable"))?
        .history_updates;
    let config_options = loaded.config_options.unwrap_or_default();
    let observed_mode_id = capture_mode_readback(capture)?.0;
    let (effective_model, effective_effort, effective_mode) = validate_restore_configuration(
        &config_options,
        loaded.modes.as_ref(),
        observed_mode_id.as_deref(),
        requested_model.as_deref(),
        requested_effort.as_deref(),
        requested_mode.as_deref(),
        &saved,
    )?;
    let config_snapshot = session_config_options_snapshot(&config_options).map_err(|error| {
        Error::new(
            "RECOVERY_REQUIRED",
            format!(
                "ACP session/load configuration options could not be retained: {}",
                error.code
            ),
        )
    })?;
    journal.record_effect(
        command,
        "session/load",
        "receipt",
        json!({
            "native_session_id":session_id,
            "native_root_id":session_id,
            "native_scope_key":session_id,
            "model_readback":effective_model,
            "effort_readback":effective_effort,
            "mode_readback":effective_mode,
            "config_options":config_snapshot.clone(),
            "history_updates":history_updates,
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
    let session = ActiveSession {
        id: session_id,
        owner_operation_id,
        workspace,
        requested_model,
        effective_model,
        requested_effort,
        effective_effort,
        requested_mode,
        effective_mode,
        close_supported,
        closed: false,
    };
    let mut updated = saved;
    updated["native_session_state"] = json!("loaded");
    updated["native_close_capability"] = json!(close_supported);
    updated["agent_info"] = features.agent_info.clone().unwrap_or(Value::Null);
    updated["agent_capabilities"] = features.capabilities.clone();
    updated["effective_model"] = json!(session.effective_model.clone());
    updated["model_readback"] = json!(session.effective_model.clone());
    updated["effective_effort"] = json!(session.effective_effort.clone());
    updated["effective_mode"] = json!(session.effective_mode.clone());
    updated["config_options"] = config_snapshot;
    updated["family_departure_claimed"] = json!(false);
    journal.save_session(&updated)?;
    if let Ok(mut capture) = capture.lock() {
        capture.current_mode_id = session.effective_mode.clone();
    }
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

trait HostCommandSource {
    fn next<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Value>> + 'a>>;
    fn reconnect<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>;
}

impl ModuleLinkRecovery<'_> {
    async fn reconnect(&self, link: &mut Link, native_ready: bool) -> Result<()> {
        note_module_ipc_disconnect(self.capture)?;
        let binding_id = link.binding_id.clone();
        let generation = link.generation;
        let route = link.route.clone();
        let recovery_required = link.recovery_required;
        let retained = self.journal.read_session()?;
        let session_id = retained
            .as_ref()
            .and_then(|session| session["native_session_id"].as_str())
            .map(str::to_owned);

        loop {
            match super::connect_module_with_native_state(
                self.config,
                self.credential,
                self.owner,
                session_id.as_deref(),
                session_id.as_deref(),
                native_ready,
            )
            .await
            {
                Ok(mut connected) => {
                    if connected.binding_id != binding_id || connected.generation != generation {
                        return Err(Error::new(
                            "NATIVE_IDENTITY_MISMATCH",
                            "reconnected module link changed its binding generation",
                        ));
                    }
                    if connected.route != route {
                        return Err(Error::new(
                            "ROUTE_CHANGED",
                            "ACP route changed while the native session remained owned",
                        ));
                    }
                    connected.recovery_required |= recovery_required;
                    *link = connected;
                    return Ok(());
                }
                Err(error) if super::is_connect_retryable(&error) => {
                    time::sleep(Duration::from_millis(250)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn send_observation(&self, link: &mut Link, sequence: u64, state: Value) -> Result<()> {
        loop {
            match super::send_observation(&mut link.client, self.owner, sequence, state.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if super::is_transport_uncertain(&error) => {
                    self.reconnect(link, false).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn send_saved_outcome(
        &self,
        link: &mut Link,
        outcome: &RuntimeOutcome,
        hash: &str,
    ) -> Result<()> {
        loop {
            module_host::validate_saved_receipt(
                outcome,
                &self.owner.host.claim,
                &link.binding_id,
                link.generation,
            )?;
            match link.client.outcome(serde_json::to_value(outcome)?).await {
                Ok(ack) => return self.journal.acknowledge(&outcome.operation_id, hash, ack),
                Err(error) if super::is_transport_uncertain(&error) => {
                    self.reconnect(link, false).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn deliver_outcome(
        &self,
        link: &mut Link,
        command: RuntimeCommand,
        mut outcome: RuntimeOutcome,
    ) -> Result<()> {
        module_host::attach_receipt_identity(&mut outcome, &command, &self.owner.host.claim)?;
        let hash = self.journal.save_outcome(&outcome)?;
        self.send_saved_outcome(link, &outcome, &hash).await
    }
}

struct OwnedModuleLinkSource<'link, 'recovery, 'context> {
    link: &'link mut Link,
    recovery: &'recovery ModuleLinkRecovery<'context>,
}

impl HostCommandSource for OwnedModuleLinkSource<'_, '_, '_> {
    fn next<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Value>> + 'a>> {
        Box::pin(self.link.client.next())
    }

    fn reconnect<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(self.recovery.reconnect(self.link, true))
    }
}

enum PromptEvent {
    Native(std::result::Result<PromptResponse, agent_client_protocol::Error>),
    Host(Value),
    HostLost(Error),
    Permission,
}

async fn next_prompt_event<R>(
    request: R,
    host: &mut impl HostCommandSource,
    permission_wakeup: &Notify,
) -> PromptEvent
where
    R: Future<Output = std::result::Result<PromptResponse, agent_client_protocol::Error>> + Unpin,
{
    let mut request = request;
    loop {
        tokio::select! {
            response = &mut request => return PromptEvent::Native(response),
            next = host.next() => match next {
                Ok(value) => return PromptEvent::Host(value),
                Err(error) if super::is_transport_uncertain(&error) => {
                    if let Err(reconnect_error) = host.reconnect().await {
                        return PromptEvent::HostLost(reconnect_error);
                    }
                }
                Err(error) => return PromptEvent::HostLost(error),
            },
            _ = permission_wakeup.notified() => return PromptEvent::Permission,
        }
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
        let event = {
            let mut host = OwnedModuleLinkSource {
                link: context.link,
                recovery: context.recovery,
            };
            next_prompt_event(request.as_mut(), &mut host, context.permission_wakeup).await
        };
        match event {
            PromptEvent::Native(response) => {
                return response.map_err(|_| {
                    Error::new(
                        "ACP_PROMPT_UNKNOWN",
                        "ACP prompt response is uncertain after native admission",
                    )
                });
            }
            PromptEvent::Host(next) => {
                if next["command"].is_null() {
                    continue;
                }
                let control: RuntimeCommand = serde_json::from_value(next["command"].clone())
                    .map_err(|_| {
                        Error::new(
                            "MODULE_COMMAND_INVALID",
                            "module.next returned an invalid ACP control envelope",
                        )
                    })?;
                process_control_during_turn(context, dispatch_command, control).await?;
            }
            PromptEvent::HostLost(error) => {
                return Err(Error::new(
                    "ACP_MODULE_LINK_LOST",
                    format!("module link could not be recovered: {}", error.code),
                ));
            }
            PromptEvent::Permission => {
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
                context
                    .recovery
                    .send_observation(context.link, sequence, observation)
                    .await
                    .map_err(|error| {
                        Error::new(
                            "ACP_MODULE_LINK_LOST",
                            format!(
                                "permission attention could not be published: {}",
                                error.code
                            ),
                        )
                    })?;
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
    let mut keep_prompt_after_permission_cancel_unknown = false;
    let expected_native_root_id = context
        .session
        .as_ref()
        .map(|session| session.id.to_string());
    let outcome = if let Err(error) =
        super::validate_command(&control, context.link, expected_native_root_id.as_deref())
    {
        rejected(&control, &error.code)
    } else {
        match control.method.as_str() {
            "native.command.cancel_turn" => {
                match cancel_active_turn(context, dispatch_command, &control).await {
                    Ok(outcome) => outcome,
                    Err(error) if is_uncertain_control_error(&error) => {
                        keep_prompt_after_permission_cancel_unknown =
                            is_pre_notification_permission_cancel_uncertain(&error);
                        if !keep_prompt_after_permission_cancel_unknown {
                            context.link.recovery_required = true;
                        }
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
    context
        .recovery
        .deliver_outcome(context.link, control, outcome)
        .await?;
    if uncertain && !keep_prompt_after_permission_cancel_unknown {
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
    cancel_active_turn_effect(
        context.connection,
        context.journal,
        context.permissions,
        dispatch_command,
        control,
        session,
    )
    .await
}

async fn cancel_active_turn_effect(
    connection: &ConnectionTo<Agent>,
    journal: &AcpJournal,
    permissions: &PermissionBroker,
    dispatch_command: &RuntimeCommand,
    control: &RuntimeCommand,
    session: &ActiveSession,
) -> Result<RuntimeOutcome> {
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
    let expected_effect = json!({
        "native_session_id":session.id,
        "target_operation_id":target,
        "turn_id":dispatch_command.operation_id
    });
    let intent = journal.read_effect(control, "session/cancel", "intent")?;
    if intent
        .as_ref()
        .is_some_and(|evidence| evidence != &expected_effect)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "saved ACP cancel intent differs from its exact session and turn",
        ));
    }
    let receipt = journal.read_effect(control, "session/cancel", "receipt")?;
    if receipt.as_ref().is_some_and(|evidence| {
        evidence["native_session_id"] != session.id.to_string()
            || evidence["target_operation_id"] != target
            || evidence["notification"] != "sent"
    }) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "saved ACP cancel receipt differs from its exact session and turn",
        ));
    }
    if receipt.is_some() && intent.is_none() {
        return Err(invalid("ACP cancel receipt has no recorded intent"));
    }
    if receipt.is_some() {
        cancel_pending_permissions(journal, permissions, control, &session.id).await?;
        let mut outcome = applied_in_session(
            control,
            session,
            json!({
                "target_operation_id":target,
                "turn_id":dispatch_command.operation_id,
                "cancel":"already_confirmed",
                "completion_proved":false,
                "family_departure_claimed":false
            }),
        );
        outcome.turn_id = Some(dispatch_command.operation_id.clone());
        return Ok(outcome);
    }
    if intent.is_some() {
        return Err(Error::new(
            "ACP_CANCEL_UNKNOWN",
            "ACP cancel intent exists without a confirmed receipt; notification will not be replayed",
        ));
    }
    // Permission settlement is a pre-notification prerequisite. Keep its
    // uncertainty separate from the session/cancel no-replay fence.
    cancel_pending_permissions(journal, permissions, control, &session.id).await?;
    journal.record_effect(control, "session/cancel", "intent", expected_effect)?;
    connection
        .send_notification(CancelNotification::new(session.id.clone()))
        .map_err(|_| {
            Error::new(
                "ACP_CANCEL_UNKNOWN",
                "ACP session/cancel notification delivery is uncertain",
            )
        })?;
    journal
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
    journal: &AcpJournal,
    permissions: &PermissionBroker,
    cancel_command: &RuntimeCommand,
    session_id: &SessionId,
) -> Result<()> {
    let session_id = session_id.to_string();
    let keys: Vec<String> = permissions
        .lock()
        .map_err(|_| invalid("ACP permission broker lock is poisoned"))?
        .iter()
        .filter(|(_, pending)| pending.session_id == session_id)
        .map(|(key, _)| key.clone())
        .collect();
    for key in keys {
        let effect_id = permission_cancel_effect_id(&key);
        let pending = claim_pending_permission(permissions, &key, &session_id, |pending| {
            journal.record_effect(
                cancel_command,
                &effect_id,
                "intent",
                json!({
                    "session_id":pending.session_id,
                    "request_id":pending.request_id,
                    "fingerprint":pending.fingerprint,
                    "response":"cancelled"
                }),
            )
        })?;
        let Some(pending) = pending else {
            // A reply may have won the exact-request claim. The turn cancel still
            // proceeds, but it cannot overwrite that already-claimed response.
            continue;
        };
        let pending_session_id = pending.session_id.clone();
        let request_id = pending.request_id.clone();
        let fingerprint = pending.fingerprint.clone();
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
        journal
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

fn claim_pending_permission(
    permissions: &PermissionBroker,
    key: &str,
    expected_session_id: &str,
    persist_intent: impl FnOnce(&PendingPermission) -> Result<()>,
) -> Result<Option<PendingPermission>> {
    let mut permissions = permissions
        .lock()
        .map_err(|_| invalid("ACP permission broker lock is poisoned"))?;
    let Some(pending) = permissions.get(key) else {
        return Ok(None);
    };
    if pending.session_id != expected_session_id {
        return Err(Error::new(
            "NATIVE_REQUEST_CHANGED",
            "ACP pending permission belongs to another session",
        ));
    }
    persist_intent(pending)?;
    Ok(permissions.remove(key))
}

// This private seam retains the exact native correlation tuple without
// introducing a second permission request or journal authority.
#[allow(clippy::too_many_arguments)]
fn record_permission_reply_intent(
    journal: &AcpJournal,
    command: &RuntimeCommand,
    key: &str,
    session_id: &str,
    request_id: &Value,
    fingerprint: &str,
    option_id: &str,
    selected_kind: PermissionOptionKind,
) -> Result<()> {
    if !matches!(
        selected_kind,
        PermissionOptionKind::AllowOnce | PermissionOptionKind::RejectOnce
    ) {
        return Err(Error::new(
            "NATIVE_PERMISSION_PERSISTENT_UNSUPPORTED",
            "ACP persistent permission choices require a separate policy mutation and readback",
        ));
    }
    journal.record_effect(
        command,
        &format!("permission-reply:{}", digest(key.as_bytes())),
        "intent",
        json!({
            "session_id":session_id,
            "request_id":request_id,
            "fingerprint":fingerprint,
            "option_id":option_id,
            "option_kind":format!("{:?}",selected_kind)
        }),
    )
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
        .filter(|value| crate::is_lower_sha256_hex(value))
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
    let selected_option = option_id.to_owned();
    let pending = claim_pending_permission(context.permissions, &key, session_id, |pending| {
        if pending.request_id != request_id
            || pending.fingerprint != fingerprint
            || !pending
                .options
                .iter()
                .any(|(id, kind, _)| id == option_id && *kind == selected_kind)
        {
            return Err(Error::new(
                "NATIVE_REQUEST_CHANGED",
                "ACP permission changed before its response was claimed",
            ));
        }
        record_permission_reply_intent(
            context.journal,
            command,
            &key,
            session_id,
            &request_id,
            fingerprint,
            &selected_option,
            selected_kind,
        )
    })?
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

fn is_pre_notification_permission_cancel_uncertain(error: &Error) -> bool {
    error.code == "ACP_PERMISSION_CANCEL_UNKNOWN"
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

fn note_module_ipc_disconnect(capture: &Arc<Mutex<Capture>>) -> Result<()> {
    capture
        .lock()
        .map_err(|_| invalid("ACP capture lock is poisoned after module IPC disconnect"))?
        .module_ipc_disconnected = true;
    Ok(())
}

fn session_recovery_reason(run_failed: bool, module_ipc_disconnected: bool) -> &'static str {
    if module_ipc_disconnected {
        "module_ipc_disconnected_no_replay"
    } else if run_failed {
        "acp_connection_ended_no_replay"
    } else {
        "module_transport_ended_no_replay"
    }
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

fn session_config_options_snapshot(options: &[SessionConfigOption]) -> Result<Value> {
    let mut snapshots = Vec::with_capacity(options.len());
    for option in options {
        let id = option.id.to_string();
        let category = match &option.category {
            Some(category) => serde_json::to_value(category)?,
            None => Value::Null,
        };
        let snapshot = match &option.kind {
            SessionConfigKind::Select(select) => {
                let allowed_value_ids = match &select.options {
                    SessionConfigSelectOptions::Ungrouped(values) => values
                        .iter()
                        .map(|value| value.value.to_string())
                        .collect::<Vec<_>>(),
                    SessionConfigSelectOptions::Grouped(groups) => groups
                        .iter()
                        .flat_map(|group| group.options.iter())
                        .map(|value| value.value.to_string())
                        .collect::<Vec<_>>(),
                    _ => return Err(invalid("ACP config option grouping is unsupported")),
                };
                json!({
                    "id":id,
                    "category":category,
                    "kind":"select",
                    "allowed_value_ids":allowed_value_ids,
                    "current_value_id":select.current_value.to_string()
                })
            }
            SessionConfigKind::Boolean(boolean) => json!({
                "id":id,
                "category":category,
                "kind":"boolean",
                "current_value":boolean.current_value
            }),
            _ => return Err(invalid("ACP config option kind is unsupported")),
        };
        snapshots.push(snapshot);
    }
    let snapshot = json!({"schema_version":1,"options":snapshots});
    if serde_json::to_vec(&snapshot)?.len() as u64 > MAX_RECORD_BYTES {
        return Err(invalid(
            "ACP config option snapshot exceeds its record bound",
        ));
    }
    Ok(snapshot)
}

fn config_select_contains(option: &SessionConfigOption, requested: &str) -> bool {
    let SessionConfigKind::Select(select) = &option.kind else {
        return false;
    };
    match &select.options {
        SessionConfigSelectOptions::Ungrouped(values) => values
            .iter()
            .any(|value| value.value.to_string() == requested),
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .any(|value| value.value.to_string() == requested),
        _ => false,
    }
}

fn config_select_current(option: &SessionConfigOption) -> Option<String> {
    match &option.kind {
        SessionConfigKind::Select(select) => Some(select.current_value.to_string()),
        _ => None,
    }
}

fn category_config_option<'a>(
    options: &'a [SessionConfigOption],
    category: &SessionConfigOptionCategory,
) -> Option<&'a SessionConfigOption> {
    let mut candidates = options.iter().filter(|option| {
        option.category.as_ref() == Some(category)
            && matches!(&option.kind, SessionConfigKind::Select(_))
    });
    let option = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    Some(option)
}

fn category_config_value(
    options: &[SessionConfigOption],
    category: &SessionConfigOptionCategory,
) -> Option<String> {
    category_config_option(options, category).and_then(config_select_current)
}

fn category_config_value_matches(
    options: &[SessionConfigOption],
    category: &SessionConfigOptionCategory,
    requested: &str,
) -> bool {
    category_config_option(options, category)
        .is_some_and(|option| config_select_current(option).as_deref() == Some(requested))
}

fn mode_value_available(modes: Option<&SessionModeState>, requested: &str) -> bool {
    modes.is_some_and(|modes| {
        let mut matches = modes
            .available_modes
            .iter()
            .filter(|mode| mode.id.to_string() == requested);
        matches.next().is_some() && matches.next().is_none()
    })
}

fn mode_current_value(modes: Option<&SessionModeState>) -> Option<String> {
    modes.map(|modes| modes.current_mode_id.to_string())
}

fn mode_readback_value(
    options: &[SessionConfigOption],
    modes: Option<&SessionModeState>,
    observed_mode_id: Option<&str>,
) -> Option<String> {
    if let Some(modes) = modes {
        return mode_current_value(Some(modes)).or_else(|| observed_mode_id.map(str::to_owned));
    }
    if options
        .iter()
        .any(|option| option.category.as_ref() == Some(&SessionConfigOptionCategory::Mode))
    {
        return category_config_value(options, &SessionConfigOptionCategory::Mode);
    }
    observed_mode_id.map(str::to_owned)
}

fn capture_mode_readback(capture: &Arc<Mutex<Capture>>) -> Result<(Option<String>, u64)> {
    let capture = capture
        .lock()
        .map_err(|_| invalid("ACP capture lock is poisoned during mode readback"))?;
    Ok((
        capture.current_mode_id.clone(),
        capture.current_mode_updates,
    ))
}

fn model_config_option<'a>(
    options: &'a [SessionConfigOption],
    requested: &str,
) -> Option<&'a SessionConfigOption> {
    let mut candidates = options.iter().filter(|option| {
        option.category.as_ref() == Some(&SessionConfigOptionCategory::Model)
            && config_select_contains(option, requested)
    });
    let option = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    Some(option)
}

fn model_config_option_id(options: &[SessionConfigOption], requested: &str) -> Option<String> {
    model_config_option(options, requested).map(|option| option.id.to_string())
}

fn model_value_available(options: &[SessionConfigOption], requested: &str) -> bool {
    model_config_option(options, requested).is_some()
}

fn model_current_value_matches(options: &[SessionConfigOption], requested: &str) -> bool {
    model_config_option(options, requested)
        .is_some_and(|option| config_select_current(option).as_deref() == Some(requested))
}

fn model_current_value(options: &[SessionConfigOption]) -> Option<String> {
    let mut candidates = options.iter().filter(|option| {
        option.category.as_ref() == Some(&SessionConfigOptionCategory::Model)
            && matches!(&option.kind, SessionConfigKind::Select(_))
    });
    let option = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    let current = config_select_current(option)?;
    (!current.is_empty()
        && current.len() <= 256
        && !current.chars().any(char::is_control)
        && config_select_contains(option, &current))
    .then_some(current)
}

fn effective_model_value(
    options: &[SessionConfigOption],
    requested: Option<&str>,
) -> Option<String> {
    match requested {
        Some(requested) => model_config_option(options, requested)
            .and_then(config_select_current)
            .filter(|current| current == requested),
        None => model_current_value(options),
    }
}

fn session_model_status(requested: Option<&str>, effective: Option<&str>) -> &'static str {
    match (requested, effective) {
        (Some(requested), Some(effective)) if requested == effective => {
            "verified_by_session_config_readback"
        }
        (Some(_), Some(_)) => "session_config_readback_mismatch",
        (None, Some(_)) => "observed_by_session_config_readback",
        _ => "unknown",
    }
}

fn command_model(route: &Value) -> Option<&str> {
    route["native_options"]["modelId"]
        .as_str()
        .filter(|model| !model.trim().is_empty() && model.trim() == *model && model.len() <= 256)
}

fn command_effort(route: &Value) -> Option<&str> {
    route["native_options"]["effort"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.trim() == *value && value.len() <= 64)
}

fn command_mode(route: &Value) -> Option<&str> {
    route["native_options"]["mode"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.trim() == *value && value.len() <= 64)
}

fn validate_acp_route(route: &Value) -> Result<()> {
    validate_acp_route_with_model_policy(route, false)
}

fn validate_acp_route_for_profile(route: &Value, profile: Profile) -> Result<()> {
    if profile.model_optional() {
        validate_acp_route_with_model_policy(route, true)
    } else {
        validate_acp_route(route)
    }
}

fn validate_acp_route_with_model_policy(route: &Value, model_optional: bool) -> Result<()> {
    let options = route["native_options"]
        .as_object()
        .ok_or_else(|| Error::new("CONFIGURATION_UNSUPPORTED", "ACP route options are missing"))?;
    if options.keys().any(|key| {
        !matches!(
            key.as_str(),
            "workspaceRoot" | "modelId" | "effort" | "mode"
        )
    }) {
        return Err(Error::new(
            "CONFIGURATION_UNSUPPORTED",
            "Command ACP route contains an unsupported native option",
        ));
    }
    if options["workspaceRoot"]
        .as_str()
        .is_none_or(|workspace| !Path::new(workspace).is_absolute())
    {
        return Err(Error::new(
            "WORKSPACE_INVALID",
            "Command ACP route requires an absolute workspaceRoot",
        ));
    }
    if (options.contains_key("modelId")
        && !options["modelId"].is_null()
        && command_model(route).is_none())
        || (!options.contains_key("modelId") && !model_optional)
        || (options.get("modelId").is_some_and(Value::is_null) && !model_optional)
    {
        return Err(Error::new(
            "COMMAND_MODEL_REQUIRED",
            "Command ACP route modelId must be an exact string when supplied",
        ));
    }
    for key in ["effort", "mode"] {
        if options.contains_key(key)
            && (options[key].as_str().is_none_or(|value| {
                value.trim().is_empty() || value.trim() != value || value.len() > 64
            }))
        {
            return Err(Error::new(
                "CONFIGURATION_UNSUPPORTED",
                "Command ACP effort and mode values must be exact non-empty strings",
            ));
        }
    }
    Ok(())
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
    let model_selection_capability = if owner.host.profile.model_optional() {
        "native_acp_session_new_with_optional_model_readback"
    } else {
        "native_acp_session_new_with_model_readback"
    };
    let model_configuration_capability = if owner.host.profile.model_optional() {
        "route_optional_model_effort_mode_at_open; runtime_agent.configure_unavailable"
    } else {
        "route_model_effort_mode_at_open; runtime_agent.configure_unavailable"
    };
    let effective_model_status = if ready {
        session_model_status(
            session.and_then(|saved| saved["requested_model"].as_str()),
            session.and_then(|saved| saved["effective_model"].as_str()),
        )
    } else {
        "unknown"
    };
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
            "effective_model_status":effective_model_status,
            "requested_effort":command_effort(&link.route),
            "effective_effort":session.and_then(|s|s["effective_effort"].as_str()),
            "effort_readback_status":if ready && session.and_then(|s|s["effective_effort"].as_str()).is_some() {"verified_by_session_config_readback"}else{"unknown_or_unadvertised"},
            "requested_mode":command_mode(&link.route),
            "effective_mode":session.and_then(|s|s["effective_mode"].as_str()),
            "mode_readback_status":if ready && session.and_then(|s|s["effective_mode"].as_str()).is_some() {"verified_by_session_new_or_mode_update"}else{"unknown_or_unadvertised"},
            "capabilities":{
                "open":model_selection_capability,
                "task_dispatch":"exact_store_task_prompt_v1_acp_prompt",
                "reconcile":"durable_receipt_readback_no_prompt_replay",
                "refresh":"exact_session_and_capture_readback",
                "send":"next_turn_only",
                "steer":"unavailable",
                "configure":model_configuration_capability,
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
            "requested_model":session.requested_model,
            "effective_model":session.effective_model,
            "requested_effort":session.requested_effort,
            "effective_effort":session.effective_effort,
            "requested_mode":session.requested_mode,
            "effective_mode":session.effective_mode,
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
#[path = "acp/runtime_ipc_fixture.rs"]
mod runtime_ipc_fixture;

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    mod installed_qualification;

    use std::{
        fs,
        future::Future,
        path::PathBuf,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use agent_client_protocol::{
        Channel, Client, RawJsonRpcMessage, RawJsonRpcParams, TransportBatch, TransportFrame,
        schema::{
            ProtocolVersion,
            v1::{
                AgentCapabilities, ContentBlock, ContentChunk, CurrentModeUpdate,
                InitializeResponse, LoadSessionResponse, PromptResponse, SessionConfigOption,
                SessionConfigOptionCategory, SessionConfigSelectOption, SessionId, SessionMode,
                SessionModeState, SessionNotification, SessionUpdate,
                SetSessionConfigOptionResponse, SetSessionModeResponse, StopReason, TextContent,
            },
        },
    };
    use futures::StreamExt as _;
    use serde_json::{Value, json};
    use swarm_contracts::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome,
        runtime::TaskDispatchContext,
        task_prompt::{TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION, TaskPromptEnvelopeV1},
    };
    use tokio::io::AsyncWriteExt as _;
    use tokio_util::codec::FramedRead;
    use uuid::Uuid;

    use super::{
        AcpJournal, AcpLineCodec, MAX_ACP_FRAME_BYTES, PreparedAcpDispatch, SelectConfigSetting,
        acp_prompt, add_family_custody_facts, capture_session_notification, digest,
        load_retained_session, set_select_config_option, stop_direct_child, validate_acp_route,
        validate_acp_route_with_model_policy, validate_restore_identity,
        validate_restore_identity_for_schema,
    };

    struct TestState {
        state_dir: PathBuf,
        journal: AcpJournal,
    }

    struct RecoveringHostFixture {
        drop_after_prompt: Option<tokio::sync::oneshot::Receiver<()>>,
        reconnected: Option<tokio::sync::oneshot::Sender<()>>,
        control_after_reconnect: Option<Value>,
        reconnect_count: usize,
    }

    impl super::HostCommandSource for RecoveringHostFixture {
        fn next<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = super::Result<Value>> + 'a>> {
            if let Some(drop_after_prompt) = self.drop_after_prompt.take() {
                Box::pin(async move {
                    drop_after_prompt.await.map_err(|_| {
                        super::invalid("host drop fixture lost its prompt admission signal")
                    })?;
                    Err(super::Error::new(
                        "DISCONNECTED",
                        "fixture host IPC disconnected",
                    ))
                })
            } else if let Some(control) = self.control_after_reconnect.take() {
                Box::pin(async move { Ok(control) })
            } else {
                Box::pin(futures::future::pending())
            }
        }

        fn reconnect<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = super::Result<()>> + 'a>> {
            self.reconnect_count += 1;
            let Some(reconnected) = self.reconnected.take() else {
                return Box::pin(async {
                    Err(super::invalid("host recovery was attempted more than once"))
                });
            };
            Box::pin(async move {
                reconnected
                    .send(())
                    .map_err(|_| super::invalid("host recovery fixture receiver was dropped"))?;
                Ok(())
            })
        }
    }

    struct PromptControlsFixture {
        controls: std::collections::VecDeque<Value>,
    }

    impl super::HostCommandSource for PromptControlsFixture {
        fn next<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = super::Result<Value>> + 'a>> {
            Box::pin(async move {
                self.controls
                    .pop_front()
                    .ok_or_else(|| super::invalid("prompt control fixture is exhausted"))
            })
        }

        fn reconnect<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = super::Result<()>> + 'a>> {
            Box::pin(async { Err(super::invalid("prompt control fixture cannot reconnect")) })
        }
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
        let prepared = acp_prompt::prepare(&command, false)?;
        Ok((command, prepared))
    }

    #[test]
    fn pr44_task_prompt_keeps_absent_and_null_model_as_unrequested() -> super::Result<()> {
        for use_null in [false, true] {
            let (mut command, _) = task_prompt_fixture()?;
            let native_options = command.route["native_options"]
                .as_object_mut()
                .expect("fixture route options are an object");
            if use_null {
                native_options.insert("modelId".to_owned(), Value::Null);
            } else {
                native_options.remove("modelId");
            }
            let prepared = acp_prompt::prepare(&command, true)?;
            assert_eq!(prepared.identity.requested_model, None);
            assert_error_code(
                acp_prompt::prepare(&command, false),
                "COMMAND_MODEL_REQUIRED",
            );
        }
        Ok(())
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

    fn permission_reply_command() -> RuntimeCommand {
        RuntimeCommand {
            operation_id: format!("op-{}", Uuid::new_v4()),
            method: "agent.reply".to_owned(),
            created_at_ms: 1,
            binding_id: "binding-fixture".to_owned(),
            generation: 1,
            native_root_id: None,
            route: json!({}),
            input: json!({}),
            input_sha256: None,
            target_input_sha256: None,
        }
    }

    fn journal_effect_count(state: &TestState, command: &RuntimeCommand) -> super::Result<usize> {
        let directory = state.journal.operation_dir(&command.operation_id)?;
        fs::read_dir(directory)?.try_fold(0usize, |count, entry| {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("effect-") {
                Ok(count + 1)
            } else {
                Ok(count)
            }
        })
    }

    fn restore_command() -> RuntimeCommand {
        let mut command = permission_reply_command();
        command.method = "agent.open".to_owned();
        command
    }

    fn retained_session_fixture(state: &TestState) -> Value {
        json!({
            "schema_version":1,
            "artifact_id":super::ACP_ARTIFACT_ID,
            "binding_id":"binding-fixture",
            "binding_generation":1,
            "operation_id":"op-retained-open",
            "native_session_id":"session-retained",
            "native_root_id":"session-retained",
            "native_scope_key":"session-retained",
            "workspace":state.state_dir,
            "requested_model":"fixture-model",
            "native_session_state":"open"
        })
    }

    fn restore_route(state: &TestState) -> Value {
        json!({
            "native_options":{
                "workspaceRoot":state.state_dir,
                "modelId":"fixture-model"
            }
        })
    }

    #[test]
    fn pr44_command_acp_route_accepts_optional_effort_mode_and_rejects_malformed_values() {
        let workspace = std::env::temp_dir().to_string_lossy().into_owned();
        let route = json!({"native_options":{"workspaceRoot":workspace,"modelId":"fixture-model"}});
        assert!(validate_acp_route(&route).is_ok());

        let mut selected = route.clone();
        selected["native_options"]["effort"] = json!("high");
        selected["native_options"]["mode"] = json!("plan");
        assert!(validate_acp_route(&selected).is_ok());

        let mut native_default = route.clone();
        native_default["native_options"]
            .as_object_mut()
            .expect("route options are an object")
            .remove("modelId");
        assert!(validate_acp_route_with_model_policy(&native_default, true).is_ok());
        assert_eq!(
            validate_acp_route_with_model_policy(&native_default, false)
                .unwrap_err()
                .code,
            "COMMAND_MODEL_REQUIRED"
        );
        native_default["native_options"]["modelId"] = Value::Null;
        assert!(validate_acp_route_with_model_policy(&native_default, true).is_ok());
        assert_eq!(
            validate_acp_route_with_model_policy(&native_default, false)
                .unwrap_err()
                .code,
            "COMMAND_MODEL_REQUIRED"
        );

        let mut effort = route.clone();
        effort["native_options"]["effort"] = json!(7);
        assert_eq!(
            validate_acp_route(&effort).unwrap_err().code,
            "CONFIGURATION_UNSUPPORTED"
        );

        let mut mode = route.clone();
        mode["native_options"]["mode"] = json!(" ");
        assert_eq!(
            validate_acp_route(&mode).unwrap_err().code,
            "CONFIGURATION_UNSUPPORTED"
        );

        let mut unknown = route;
        unknown["native_options"]["provider"] = json!("must-not-be-ignored");
        assert_eq!(
            validate_acp_route(&unknown).unwrap_err().code,
            "CONFIGURATION_UNSUPPORTED"
        );
    }

    #[tokio::test]
    async fn pr44_command_acp_line_framing_distinguishes_eof_and_rejects_bad_frames() {
        async fn read_first(bytes: &[u8]) -> Option<std::io::Result<String>> {
            let (mut writer, reader) = tokio::io::duplex(bytes.len().max(1));
            writer
                .write_all(bytes)
                .await
                .expect("fixture bytes fit the pipe");
            drop(writer);
            FramedRead::new(reader, AcpLineCodec::new(8)).next().await
        }

        assert!(read_first(b"").await.is_none());
        assert_eq!(
            read_first(b"{}\n")
                .await
                .expect("valid line has a frame")
                .unwrap(),
            "{}"
        );
        let unterminated = read_first(b"{}").await.expect("EOF reports partial frame");
        assert_eq!(
            unterminated.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert!(
            read_first(b"123456789\n")
                .await
                .expect("oversized terminated frame errors")
                .is_err()
        );
        assert!(
            read_first(b"123456789")
                .await
                .expect("oversized unterminated frame errors")
                .is_err()
        );
        assert_eq!(MAX_ACP_FRAME_BYTES, 1024 * 1024);
    }

    #[test]
    fn pr44_command_acp_restore_identity_requires_exact_binding_workspace_and_model_before_effects()
    -> super::Result<()> {
        let state = TestState::new()?;
        let saved = retained_session_fixture(&state);
        state.journal.save_session(&saved)?;
        let route = restore_route(&state);

        let identity = validate_restore_identity(&saved, "binding-fixture", 1, &route, true)?;
        assert_eq!(identity.session_text, "session-retained");
        assert_eq!(identity.session_id.to_string(), "session-retained");
        assert_eq!(identity.requested_model.as_deref(), Some("fixture-model"));
        assert_eq!(identity.requested_effort, None);
        assert_eq!(identity.requested_mode, None);

        assert_error_code(
            validate_restore_identity(&saved, "other-binding", 1, &route, true),
            "RECOVERY_REQUIRED",
        );
        assert_error_code(
            validate_restore_identity(&saved, "binding-fixture", 1, &route, false),
            "RECOVERY_REQUIRED",
        );
        let mut changed_model = route.clone();
        changed_model["native_options"]["modelId"] = json!("other-model");
        assert_error_code(
            validate_restore_identity(&saved, "binding-fixture", 1, &changed_model, true),
            "NATIVE_IDENTITY_MISMATCH",
        );
        assert_eq!(state.journal.read_session()?, Some(saved));
        assert_eq!(journal_effect_count(&state, &restore_command())?, 0);

        let mut optional_saved = retained_session_fixture(&state);
        optional_saved["schema_version"] = json!(2);
        optional_saved["requested_model"] = Value::Null;
        optional_saved["effective_model"] = json!("fixture-default");
        let mut optional_route = restore_route(&state);
        optional_route["native_options"]
            .as_object_mut()
            .expect("route options are an object")
            .remove("modelId");
        let optional_identity = validate_restore_identity_for_schema(
            &optional_saved,
            "binding-fixture",
            1,
            &optional_route,
            true,
            2,
            true,
        )?;
        assert_eq!(optional_identity.requested_model, None);

        let mut configured_saved = retained_session_fixture(&state);
        configured_saved["requested_effort"] = json!("high");
        configured_saved["requested_mode"] = json!("plan");
        let mut configured_route = restore_route(&state);
        configured_route["native_options"]["effort"] = json!("high");
        configured_route["native_options"]["mode"] = json!("plan");
        let configured = validate_restore_identity(
            &configured_saved,
            "binding-fixture",
            1,
            &configured_route,
            true,
        )?;
        assert_eq!(configured.requested_effort.as_deref(), Some("high"));
        assert_eq!(configured.requested_mode.as_deref(), Some("plan"));

        configured_route["native_options"]["effort"] = json!("default");
        assert_error_code(
            validate_restore_identity(
                &configured_saved,
                "binding-fixture",
                1,
                &configured_route,
                true,
            ),
            "NATIVE_IDENTITY_MISMATCH",
        );
        configured_route["native_options"]["effort"] = json!("high");
        configured_route["native_options"]["mode"] = json!("default");
        assert_error_code(
            validate_restore_identity(
                &configured_saved,
                "binding-fixture",
                1,
                &configured_route,
                true,
            ),
            "NATIVE_IDENTITY_MISMATCH",
        );
        Ok(())
    }

    #[test]
    fn pr44_command_acp_restore_readback_requires_selected_effort_and_mode() -> super::Result<()> {
        let options = vec![
            SessionConfigOption::select(
                "model",
                "Model",
                "model-a",
                vec![SessionConfigSelectOption::new("model-a", "Model A")],
            )
            .category(SessionConfigOptionCategory::Model),
            SessionConfigOption::select(
                "effort",
                "Effort",
                "high",
                vec![
                    SessionConfigSelectOption::new("default", "Default"),
                    SessionConfigSelectOption::new("high", "High"),
                ],
            )
            .category(SessionConfigOptionCategory::ThoughtLevel),
        ];
        let modes = super::SessionModeState::new(
            "plan",
            vec![
                super::SessionMode::new("default", "Default"),
                super::SessionMode::new("plan", "Plan"),
            ],
        );
        let saved = json!({"effective_effort":"high","effective_mode":"plan"});
        let readback = super::validate_restore_configuration(
            &options,
            Some(&modes),
            None,
            Some("model-a"),
            Some("high"),
            Some("plan"),
            &saved,
        )?;
        assert_eq!(readback.0.as_deref(), Some("model-a"));
        assert_eq!(readback.1.as_deref(), Some("high"));
        assert_eq!(readback.2.as_deref(), Some("plan"));

        assert_error_code(
            super::validate_restore_configuration(
                &options,
                Some(&modes),
                None,
                Some("model-a"),
                Some("default"),
                Some("plan"),
                &saved,
            ),
            "RECOVERY_REQUIRED",
        );
        assert_error_code(
            super::validate_restore_configuration(
                &options,
                Some(&modes),
                None,
                Some("model-a"),
                Some("high"),
                Some("default"),
                &saved,
            ),
            "RECOVERY_REQUIRED",
        );

        // Legacy receipts did not carry effort/mode fields; keep their model-only interpretation.
        let old_receipt = json!({});
        assert_eq!(
            super::validate_restore_configuration(
                &options,
                None,
                None,
                Some("model-a"),
                None,
                None,
                &old_receipt,
            )?,
            (Some("model-a".to_owned()), Some("high".to_owned()), None)
        );
        let mode_from_replay = super::validate_restore_configuration(
            &options,
            None,
            Some("plan"),
            Some("model-a"),
            None,
            Some("plan"),
            &saved,
        )?;
        assert_eq!(mode_from_replay.2.as_deref(), Some("plan"));

        let mut typed_mode_options = options.clone();
        typed_mode_options.push(
            SessionConfigOption::select(
                "mode",
                "Mode",
                "plan",
                vec![
                    SessionConfigSelectOption::new("default", "Default"),
                    SessionConfigSelectOption::new("plan", "Plan"),
                ],
            )
            .category(SessionConfigOptionCategory::Mode),
        );
        let typed_mode_readback = super::validate_restore_configuration(
            &typed_mode_options,
            None,
            None,
            Some("model-a"),
            Some("high"),
            Some("plan"),
            &saved,
        )?;
        assert_eq!(typed_mode_readback.2.as_deref(), Some("plan"));

        let optional_saved = json!({
            "schema_version":2,
            "effective_model":"model-a",
            "effective_effort":"high",
            "effective_mode":"plan"
        });
        let observed_default = super::validate_restore_configuration(
            &options,
            Some(&modes),
            None,
            None,
            Some("high"),
            Some("plan"),
            &optional_saved,
        )?;
        assert_eq!(observed_default.0.as_deref(), Some("model-a"));
        assert_eq!(observed_default.1.as_deref(), Some("high"));
        assert_eq!(observed_default.2.as_deref(), Some("plan"));

        let mut changed_default = optional_saved.clone();
        changed_default["effective_model"] = json!("different-model");
        assert_error_code(
            super::validate_restore_configuration(
                &options,
                Some(&modes),
                None,
                None,
                Some("high"),
                Some("plan"),
                &changed_default,
            ),
            "RECOVERY_REQUIRED",
        );

        typed_mode_options[2] = SessionConfigOption::select(
            "mode",
            "Mode",
            "default",
            vec![
                SessionConfigSelectOption::new("default", "Default"),
                SessionConfigSelectOption::new("plan", "Plan"),
            ],
        )
        .category(SessionConfigOptionCategory::Mode);
        assert_error_code(
            super::validate_restore_configuration(
                &typed_mode_options,
                None,
                None,
                Some("model-a"),
                Some("high"),
                Some("plan"),
                &saved,
            ),
            "RECOVERY_REQUIRED",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_config_setters_require_exact_typed_readback() -> super::Result<()> {
        let state = TestState::new()?;
        let command = restore_command();
        let session_id = SessionId::new("session-config-fixture");
        let initial = [
            SessionConfigOption::select(
                "model",
                "Model",
                "model-a",
                vec![
                    SessionConfigSelectOption::new("model-a", "Model A"),
                    SessionConfigSelectOption::new("model-b", "Model B"),
                ],
            )
            .category(SessionConfigOptionCategory::Model),
            SessionConfigOption::select(
                "effort",
                "Effort",
                "default",
                vec![
                    SessionConfigSelectOption::new("default", "Default"),
                    SessionConfigSelectOption::new("high", "High"),
                ],
            )
            .category(SessionConfigOptionCategory::ThoughtLevel),
            SessionConfigOption::select(
                "mode",
                "Mode",
                "default",
                vec![
                    SessionConfigSelectOption::new("default", "Default"),
                    SessionConfigSelectOption::new("plan", "Plan"),
                ],
            )
            .category(SessionConfigOptionCategory::Mode),
        ];
        let (transport, mut peer) = Channel::duplex();
        let client_journal = state.journal.clone();
        let client_command = command.clone();
        let client_session_id = session_id.clone();
        let mode_capture = Arc::new(Mutex::new(super::Capture::default()));
        let client_capture = Arc::clone(&mode_capture);
        let client = Client
            .builder()
            .connect_with(transport, async move |connection| {
                let selected_model = set_select_config_option(
                    &connection,
                    &client_journal,
                    &client_command,
                    &client_session_id,
                    SelectConfigSetting {
                        field: "model",
                        option: &initial[0],
                        requested: "model-b",
                    },
                )
                .await
                .map_err(super::to_acp_error)?;
                assert!(super::model_current_value_matches(
                    &selected_model,
                    "model-b"
                ));
                let effort = super::category_config_option(
                    &selected_model,
                    &SessionConfigOptionCategory::ThoughtLevel,
                )
                .expect("unique advertised effort option");
                let selected_effort = set_select_config_option(
                    &connection,
                    &client_journal,
                    &client_command,
                    &client_session_id,
                    SelectConfigSetting {
                        field: "effort",
                        option: effort,
                        requested: "high",
                    },
                )
                .await
                .map_err(super::to_acp_error)?;
                assert!(super::category_config_value_matches(
                    &selected_effort,
                    &SessionConfigOptionCategory::ThoughtLevel,
                    "high"
                ));
                let mut selected_options = selected_effort;
                let selected_mode =
                    super::configure_session_mode(super::SessionModeConfigContext {
                        connection: &connection,
                        journal: &client_journal,
                        command: &client_command,
                        session_id: &client_session_id,
                        requested_mode: Some("plan"),
                        advertised_modes: None,
                        options: &mut selected_options,
                        capture: &client_capture,
                    })
                    .await
                    .map_err(super::to_acp_error)?
                    .expect("requested mode returns an exact selector value");
                assert_eq!(selected_mode, "plan");
                let mut unsupported_options = Vec::new();
                let unsupported_mode =
                    super::configure_session_mode(super::SessionModeConfigContext {
                        connection: &connection,
                        journal: &client_journal,
                        command: &client_command,
                        session_id: &client_session_id,
                        requested_mode: Some("bypass"),
                        advertised_modes: None,
                        options: &mut unsupported_options,
                        capture: &client_capture,
                    })
                    .await;
                assert_error_code(unsupported_mode, "CONFIGURATION_UNSUPPORTED");
                Ok::<(), agent_client_protocol::Error>(())
            });
        let peer = async move {
            for (expected_id, expected_value, model_value, effort_value, mode_value) in [
                ("model", "model-b", "model-b", "default", "default"),
                ("effort", "high", "model-b", "high", "default"),
                ("mode", "plan", "model-b", "high", "plan"),
            ] {
                let Some(TransportFrame::Single(RawJsonRpcMessage::Request(request))) =
                    peer.rx.next().await
                else {
                    panic!("expected session/set_config_option request")
                };
                assert_eq!(request.method.as_ref(), "session/set_config_option");
                let Some(RawJsonRpcParams::Object(params)) = request.params else {
                    panic!("config-option request params are an object")
                };
                assert_eq!(params["sessionId"], "session-config-fixture");
                assert_eq!(params["configId"], expected_id);
                assert_eq!(params["value"], expected_value);
                let options = vec![
                    SessionConfigOption::select(
                        "model",
                        "Model",
                        model_value,
                        vec![
                            SessionConfigSelectOption::new("model-a", "Model A"),
                            SessionConfigSelectOption::new("model-b", "Model B"),
                        ],
                    )
                    .category(SessionConfigOptionCategory::Model),
                    SessionConfigOption::select(
                        "effort",
                        "Effort",
                        effort_value,
                        vec![
                            SessionConfigSelectOption::new("default", "Default"),
                            SessionConfigSelectOption::new("high", "High"),
                        ],
                    )
                    .category(SessionConfigOptionCategory::ThoughtLevel),
                    SessionConfigOption::select(
                        "mode",
                        "Mode",
                        mode_value,
                        vec![
                            SessionConfigSelectOption::new("default", "Default"),
                            SessionConfigSelectOption::new("plan", "Plan"),
                        ],
                    )
                    .category(SessionConfigOptionCategory::Mode),
                ];
                peer.tx
                    .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                        request.id,
                        Ok(
                            serde_json::to_value(SetSessionConfigOptionResponse::new(options))
                                .expect("config-option readback serializes"),
                        ),
                    )))
                    .expect("config-option readback reaches the adapter");
            }
            Ok::<(), agent_client_protocol::Error>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("config-option fixture timed out")
        .expect("config-option fixture failed");
        assert_eq!(journal_effect_count(&state, &command)?, 6);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_mode_setter_waits_for_current_mode_update() -> super::Result<()> {
        let state = TestState::new()?;
        let command = restore_command();
        let session_id = SessionId::new("session-mode-fixture");
        let modes = SessionModeState::new(
            "default",
            vec![
                SessionMode::new("default", "Default"),
                SessionMode::new("plan", "Plan"),
            ],
        );
        let capture = Arc::new(Mutex::new(super::Capture {
            native_session_id: Some(session_id.to_string()),
            current_mode_id: Some("default".to_owned()),
            ..super::Capture::default()
        }));
        let client_journal = state.journal.clone();
        let client_command = command.clone();
        let client_session_id = session_id.clone();
        let peer_session_id = session_id.clone();
        let notification_capture = Arc::clone(&capture);
        let client_capture = Arc::clone(&capture);
        let (transport, mut peer) = Channel::duplex();
        let client = Client
            .builder()
            .on_receive_notification(
                async move |notification: SessionNotification, _connection| {
                    if let Ok(mut capture) = notification_capture.lock() {
                        capture_session_notification(&mut capture, &notification);
                    }
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_with(transport, async move |connection| {
                let mut options = Vec::new();
                let mode = super::configure_session_mode(super::SessionModeConfigContext {
                    connection: &connection,
                    journal: &client_journal,
                    command: &client_command,
                    session_id: &client_session_id,
                    requested_mode: Some("plan"),
                    advertised_modes: Some(&modes),
                    options: &mut options,
                    capture: &client_capture,
                })
                .await
                .map_err(super::to_acp_error)?
                .expect("requested mode returns an exact mode ID");
                assert_eq!(mode, "plan");
                Ok::<(), agent_client_protocol::Error>(())
            });
        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(request))) =
                peer.rx.next().await
            else {
                panic!("expected session/set_mode request")
            };
            assert_eq!(request.method.as_ref(), "session/set_mode");
            let Some(RawJsonRpcParams::Object(params)) = request.params else {
                panic!("set-mode request params are an object")
            };
            assert_eq!(params["sessionId"], "session-mode-fixture");
            assert_eq!(params["modeId"], "plan");
            let notification = RawJsonRpcMessage::notification(
                "session/update".to_owned(),
                serde_json::to_value(SessionNotification::new(
                    peer_session_id,
                    SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("plan")),
                ))
                .expect("mode notification serializes"),
            )
            .expect("mode notification is valid JSON-RPC");
            let response = RawJsonRpcMessage::response(
                request.id,
                Ok(serde_json::to_value(SetSessionModeResponse::new())
                    .expect("mode response serializes")),
            );
            peer.tx
                .unbounded_send(TransportFrame::Batch(
                    TransportBatch::from_messages([notification, response])
                        .expect("mode update and response batch is non-empty"),
                ))
                .expect("mode update reaches adapter before response handling");
            Ok::<(), agent_client_protocol::Error>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("mode fixture timed out")
        .expect("mode fixture failed");
        let capture = capture.lock().expect("capture lock remains usable");
        assert_eq!(capture.current_mode_id.as_deref(), Some("plan"));
        assert_eq!(capture.current_mode_updates, 1);
        assert_eq!(journal_effect_count(&state, &command)?, 2);
        Ok(())
    }

    #[test]
    fn pr44_command_acp_restore_intent_coalesces_and_changed_target_conflicts() -> super::Result<()>
    {
        let state = TestState::new()?;
        let command = restore_command();
        let target = json!({
            "native_session_id":"session-retained",
            "native_root_id":"session-retained",
            "native_scope_key":"session-retained",
            "native_replay":false
        });
        state
            .journal
            .record_effect(&command, "session/load", "intent", target.clone())?;
        state
            .journal
            .record_effect(&command, "session/load", "intent", target)?;
        assert_eq!(journal_effect_count(&state, &command)?, 1);

        assert_error_code(
            state.journal.record_effect(
                &command,
                "session/load",
                "intent",
                json!({
                    "native_session_id":"different-session",
                    "native_root_id":"different-session",
                    "native_scope_key":"different-session",
                    "native_replay":false
                }),
            ),
            "ADAPTER_EVIDENCE_CONFLICT",
        );
        assert_eq!(journal_effect_count(&state, &command)?, 1);
        Ok(())
    }

    #[test]
    fn pr44_command_acp_unknown_dispatch_reconciles_same_operation_without_native_replay()
    -> super::Result<()> {
        let state = TestState::new()?;
        let (command, dispatch) = task_prompt_fixture()?;
        state.journal.record_command(&command)?;
        state.journal.admit(&command, &dispatch, None)?;

        let unknown = super::retained_dispatch_outcome(&state.journal, &command, None)?;
        assert_eq!(unknown.operation_id, command.operation_id);
        assert_eq!(unknown.outcome, EffectOutcome::Unknown);
        assert_eq!(unknown.details["native_replay"], false);
        assert_eq!(journal_effect_count(&state, &command)?, 0);

        let hash = state.journal.save_outcome(&unknown)?;
        let retained = super::retained_dispatch_outcome(&state.journal, &command, None)?;
        assert_eq!(
            serde_json::to_value(retained)?,
            serde_json::to_value(&unknown)?
        );

        let readback = super::reconciliation_details(&state.journal, &command.operation_id)?;
        assert_eq!(readback["target_operation_id"], command.operation_id);
        assert_eq!(readback["target_outcome_sha256"], hash);
        assert_eq!(readback["target_outcome"], "unknown");
        assert_eq!(readback["native_replay"], false);
        let (saved, saved_hash) = state
            .journal
            .read_outcome(&command.operation_id)?
            .expect("same operation's full Unknown receipt remains readable");
        assert_eq!(saved_hash, hash);
        assert_eq!(
            serde_json::to_value(saved)?,
            serde_json::to_value(&unknown)?
        );
        assert_eq!(journal_effect_count(&state, &command)?, 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_host_ipc_recovery_retains_prompt_and_client() -> super::Result<()> {
        let state = TestState::new().expect("ACP recovery journal opens");
        let (dispatch_command, _) = task_prompt_fixture().expect("typed TaskPrompt fixture");
        let session_id = SessionId::new("session-host-recovery-fixture");
        let session = super::ActiveSession {
            id: session_id.clone(),
            owner_operation_id: dispatch_command.operation_id.clone(),
            workspace: std::env::temp_dir(),
            requested_model: Some("fixture-model".to_owned()),
            effective_model: Some("fixture-model".to_owned()),
            requested_effort: None,
            effective_effort: None,
            requested_mode: None,
            effective_mode: None,
            close_supported: true,
            closed: false,
        };
        let mut cancel_command = permission_reply_command();
        cancel_command.method = "native.command.cancel_turn".to_owned();
        cancel_command.binding_id = dispatch_command.binding_id.clone();
        cancel_command.generation = dispatch_command.generation;
        cancel_command.input = json!({
            "session_id":session_id,
            "target_operation_id":dispatch_command.operation_id,
            "turn_id":dispatch_command.operation_id
        });
        let control_operation = cancel_command.operation_id.clone();
        let count_command = cancel_command.clone();
        let journal = state.journal.clone();
        let permissions: super::PermissionBroker = Arc::new(Mutex::new(Default::default()));
        let (transport, mut peer) = Channel::duplex();
        let (prompt_seen_sender, prompt_seen_receiver) = tokio::sync::oneshot::channel();
        let (reconnected_sender, reconnected_receiver) = tokio::sync::oneshot::channel();
        let client = Client
            .builder()
            .connect_with(transport, async move |connection| {
                let request = connection
                    .send_request(super::PromptRequest::new(
                        session_id,
                        vec![ContentBlock::Text(TextContent::new("fixture-only"))],
                    ))
                    .block_task();
                tokio::pin!(request);
                let permission_wakeup = tokio::sync::Notify::new();
                let mut source = RecoveringHostFixture {
                    drop_after_prompt: Some(prompt_seen_receiver),
                    reconnected: Some(reconnected_sender),
                    control_after_reconnect: Some(json!({"command":cancel_command})),
                    reconnect_count: 0,
                };
                let event =
                    super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup)
                        .await;
                let super::PromptEvent::Host(host_message) = event else {
                    panic!("the same prompt remains pending while its host reconnects")
                };
                assert_eq!(host_message["command"]["operation_id"], control_operation);
                let recovered_control: RuntimeCommand =
                    serde_json::from_value(host_message["command"].clone())
                        .expect("reconnected host delivers the exact cancel control");
                let first_cancel = super::cancel_active_turn_effect(
                    &connection,
                    &journal,
                    &permissions,
                    &dispatch_command,
                    &recovered_control,
                    &session,
                )
                .await
                .map_err(super::to_acp_error)?;
                assert_eq!(first_cancel.outcome, EffectOutcome::Applied);
                assert_eq!(first_cancel.details["cancel"], "notification_sent");
                let repeated_cancel = super::cancel_active_turn_effect(
                    &connection,
                    &journal,
                    &permissions,
                    &dispatch_command,
                    &recovered_control,
                    &session,
                )
                .await
                .map_err(super::to_acp_error)?;
                assert_eq!(repeated_cancel.details["cancel"], "already_confirmed");

                let event =
                    super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup)
                        .await;
                assert!(matches!(
                    event,
                    super::PromptEvent::Native(Ok(response))
                        if response.stop_reason == StopReason::Cancelled
                ));
                assert_eq!(source.reconnect_count, 1);
                Ok::<(), agent_client_protocol::Error>(())
            });
        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(request))) =
                peer.rx.next().await
            else {
                panic!("one active ACP prompt is admitted")
            };
            assert_eq!(request.method.as_ref(), "session/prompt");
            let prompt_id = request.id;
            prompt_seen_sender
                .send(())
                .expect("host disconnect follows the same prompt admission");
            reconnected_receiver
                .await
                .expect("source recovers the existing module link");
            let Some(TransportFrame::Single(RawJsonRpcMessage::Notification(cancel))) =
                peer.rx.next().await
            else {
                panic!("explicit cancel uses session/cancel on the retained client")
            };
            assert_eq!(cancel.method.as_ref(), "session/cancel");
            let response = serde_json::to_value(PromptResponse::new(StopReason::Cancelled))
                .expect("same prompt cancellation response serializes");
            peer.tx
                .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                    prompt_id,
                    Ok(response),
                )))
                .expect("same ACP connection returns the original prompt result");
            assert!(
                peer.rx.next().await.is_none(),
                "no cancellation request or prompt replay is emitted"
            );
            Ok::<(), agent_client_protocol::Error>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("host recovery fixture remains bounded")
        .expect("host recovery fixture succeeds");
        assert_eq!(journal_effect_count(&state, &count_command)?, 2);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_permission_cancel_unknown_keeps_exact_turn_cancel_path()
    -> super::Result<()> {
        let state = TestState::new()?;
        let (dispatch_command, _) = task_prompt_fixture()?;
        let session_id = SessionId::new("session-permission-cancel-uncertain-fixture");
        let session = super::ActiveSession {
            id: session_id.clone(),
            owner_operation_id: dispatch_command.operation_id.clone(),
            workspace: std::env::temp_dir(),
            requested_model: Some("fixture-model".to_owned()),
            effective_model: Some("fixture-model".to_owned()),
            requested_effort: None,
            effective_effort: None,
            requested_mode: None,
            effective_mode: None,
            close_supported: true,
            closed: false,
        };
        let exact_cancel = || {
            let mut command = permission_reply_command();
            command.method = "native.command.cancel_turn".to_owned();
            command.binding_id = dispatch_command.binding_id.clone();
            command.generation = dispatch_command.generation;
            command.input = json!({
                "session_id":session_id,
                "target_operation_id":dispatch_command.operation_id,
                "turn_id":dispatch_command.operation_id
            });
            command
        };
        let first_cancel = exact_cancel();
        let retry_cancel = exact_cancel();
        let maybe_written_cancel = exact_cancel();

        let request_id = json!("permission-request-uncertain");
        let key = serde_json::to_string(&request_id)?;
        let fingerprint = digest(b"permission response may have been written");
        let permissions: super::PermissionBroker =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (permission_reply, _permission_response) = tokio::sync::oneshot::channel();
        let (acknowledgement_sender, acknowledgement) = tokio::sync::oneshot::channel();
        acknowledgement_sender
            .send(false)
            .expect("fixture reports that permission response was not confirmed written");
        permissions.lock().expect("permission broker lock").insert(
            key.clone(),
            super::PendingPermission {
                session_id: session_id.to_string(),
                request_id,
                fingerprint,
                options: Vec::new(),
                reply: permission_reply,
                acknowledgement,
            },
        );

        let (transport, mut peer) = Channel::duplex();
        let journal = state.journal.clone();
        let dispatch_for_client = dispatch_command.clone();
        let session_for_client = session.clone();
        let permissions_for_client = Arc::clone(&permissions);
        let first_cancel_for_client = first_cancel.clone();
        let retry_cancel_for_client = retry_cancel.clone();
        let maybe_written_cancel_for_client = maybe_written_cancel.clone();
        let prompt_session_id = session_id.clone();
        let key_for_client = key.clone();
        let client = Client
            .builder()
            .connect_with(transport, async move |connection| {
                let request = connection
                    .send_request(super::PromptRequest::new(
                        prompt_session_id,
                        vec![ContentBlock::Text(TextContent::new("fixture-only"))],
                    ))
                    .block_task();
                tokio::pin!(request);
                let permission_wakeup = tokio::sync::Notify::new();
                let mut source = PromptControlsFixture {
                    controls: vec![
                        json!({"command":first_cancel_for_client}),
                        json!({"command":retry_cancel_for_client}),
                    ]
                    .into_iter()
                    .collect(),
                };

                let super::PromptEvent::Host(first_message) =
                    super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup)
                        .await
                else {
                    panic!("the active prompt receives the first exact-turn cancel")
                };
                let first_control: RuntimeCommand =
                    serde_json::from_value(first_message["command"].clone())
                        .expect("fixture control is a RuntimeCommand");
                let permission_error = match super::cancel_active_turn_effect(
                    &connection,
                    &journal,
                    &permissions_for_client,
                    &dispatch_for_client,
                    &first_control,
                    &session_for_client,
                )
                .await
                {
                    Err(error) => error,
                    Ok(_) => {
                        return Err(super::to_acp_error(super::invalid(
                            "permission uncertainty was marked applied",
                        )));
                    }
                };
                assert_eq!(permission_error.code, "ACP_PERMISSION_CANCEL_UNKNOWN");
                assert!(super::is_pre_notification_permission_cancel_uncertain(
                    &permission_error
                ));
                let first_unknown = super::unknown_in_session(
                    &first_control,
                    &session_for_client,
                    &permission_error.code,
                );
                assert_eq!(first_unknown.outcome, EffectOutcome::Unknown);
                assert!(
                    journal
                        .read_effect(&first_control, "session/cancel", "intent")
                        .map_err(super::to_acp_error)?
                        .is_none()
                );
                let permission_effect = super::permission_cancel_effect_id(&key_for_client);
                assert!(
                    journal
                        .read_effect(&first_control, &permission_effect, "intent")
                        .map_err(super::to_acp_error)?
                        .is_some()
                );
                assert!(
                    journal
                        .read_effect(&first_control, &permission_effect, "receipt")
                        .map_err(super::to_acp_error)?
                        .is_none()
                );

                let super::PromptEvent::Host(retry_message) =
                    super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup)
                        .await
                else {
                    panic!("the same prompt remains active for a subsequent exact cancel")
                };
                let retry_control: RuntimeCommand =
                    serde_json::from_value(retry_message["command"].clone())
                        .expect("fixture retry is a RuntimeCommand");
                assert_eq!(
                    retry_control.input["target_operation_id"],
                    dispatch_for_client.operation_id
                );
                assert_eq!(
                    retry_control.input["session_id"],
                    session_for_client.id.to_string()
                );
                let retry_outcome = super::cancel_active_turn_effect(
                    &connection,
                    &journal,
                    &permissions_for_client,
                    &dispatch_for_client,
                    &retry_control,
                    &session_for_client,
                )
                .await
                .map_err(super::to_acp_error)?;
                assert_eq!(retry_outcome.outcome, EffectOutcome::Applied);
                assert_eq!(retry_outcome.details["cancel"], "notification_sent");

                let expected_intent = json!({
                    "native_session_id":session_for_client.id,
                    "target_operation_id":dispatch_for_client.operation_id,
                    "turn_id":dispatch_for_client.operation_id
                });
                journal
                    .record_effect(
                        &maybe_written_cancel_for_client,
                        "session/cancel",
                        "intent",
                        expected_intent,
                    )
                    .map_err(super::to_acp_error)?;
                let fenced_error = match super::cancel_active_turn_effect(
                    &connection,
                    &journal,
                    &permissions_for_client,
                    &dispatch_for_client,
                    &maybe_written_cancel_for_client,
                    &session_for_client,
                )
                .await
                {
                    Err(error) => error,
                    Ok(_) => {
                        return Err(super::to_acp_error(super::invalid(
                            "possibly written cancel was replayed",
                        )));
                    }
                };
                assert_eq!(fenced_error.code, "ACP_CANCEL_UNKNOWN");
                let fenced_unknown = super::unknown_in_session(
                    &maybe_written_cancel_for_client,
                    &session_for_client,
                    &fenced_error.code,
                );
                assert_eq!(fenced_unknown.outcome, EffectOutcome::Unknown);
                Ok::<(), agent_client_protocol::Error>(())
            });
        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(prompt))) =
                peer.rx.next().await
            else {
                panic!("the fixture starts one native prompt")
            };
            assert_eq!(prompt.method.as_ref(), "session/prompt");
            let Some(TransportFrame::Single(RawJsonRpcMessage::Notification(cancel))) =
                peer.rx.next().await
            else {
                panic!("the later exact-turn cancel reaches the native session")
            };
            assert_eq!(cancel.method.as_ref(), "session/cancel");
            Ok::<(), agent_client_protocol::Error>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("permission uncertainty fixture remains bounded")
        .expect("permission uncertainty fixture succeeds");

        let permission_effect = super::permission_cancel_effect_id(&key);
        assert!(
            state
                .journal
                .read_effect(&first_cancel, "session/cancel", "intent")?
                .is_none()
        );
        assert!(
            state
                .journal
                .read_effect(&first_cancel, &permission_effect, "intent")?
                .is_some()
        );
        assert!(
            state
                .journal
                .read_effect(&first_cancel, &permission_effect, "receipt")?
                .is_none()
        );
        assert!(
            state
                .journal
                .read_effect(&retry_cancel, "session/cancel", "intent")?
                .is_some()
        );
        assert!(
            state
                .journal
                .read_effect(&retry_cancel, "session/cancel", "receipt")?
                .is_some()
        );
        assert!(
            state
                .journal
                .read_effect(&maybe_written_cancel, "session/cancel", "receipt")?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_production_control_keeps_prompt_on_permission_cancel_unknown()
    -> super::Result<()> {
        let state = TestState::new()?;
        let (mut dispatch_command, _) = task_prompt_fixture()?;
        dispatch_command.route["module_artifact_id"] = json!(crate::ACP_ARTIFACT_ID);
        let session_id = SessionId::new("session-production-cancel-unknown-fixture");
        let session = super::ActiveSession {
            id: session_id.clone(),
            owner_operation_id: dispatch_command.operation_id.clone(),
            workspace: std::env::temp_dir(),
            requested_model: Some("fixture-model".to_owned()),
            effective_model: Some("fixture-model".to_owned()),
            requested_effort: None,
            effective_effort: None,
            requested_mode: None,
            effective_mode: None,
            close_supported: true,
            closed: false,
        };
        let exact_cancel = || {
            let mut command = permission_reply_command();
            command.method = "native.command.cancel_turn".to_owned();
            command.binding_id = dispatch_command.binding_id.clone();
            command.generation = dispatch_command.generation;
            command.route = dispatch_command.route.clone();
            command.native_root_id = Some(session_id.to_string());
            command.input = json!({
                "client_request_id": command.operation_id,
                "binding_id": command.binding_id,
                "generation": command.generation,
                "session_id": session_id,
                "target_operation_id": dispatch_command.operation_id,
                "turn_id": dispatch_command.operation_id
            });
            let input_bytes = serde_json::to_vec(&command.input).expect("cancel input serializes");
            command.input_sha256 = Some(digest(input_bytes.as_slice()));
            command
        };
        let first_cancel = exact_cancel();
        let retry_cancel = exact_cancel();
        let maybe_written_cancel = exact_cancel();
        let request_id = json!("permission-request-production-unknown");
        let key = serde_json::to_string(&request_id)?;
        let fingerprint = digest(b"permission response may have been written");
        let permissions: super::PermissionBroker =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (permission_reply, _permission_response) = tokio::sync::oneshot::channel();
        let (acknowledgement_sender, acknowledgement) = tokio::sync::oneshot::channel();
        acknowledgement_sender
            .send(false)
            .expect("fixture reports permission response not written");
        permissions.lock().expect("permission broker lock").insert(
            key.clone(),
            super::PendingPermission {
                session_id: session_id.to_string(),
                request_id,
                fingerprint,
                options: Vec::new(),
                reply: permission_reply,
                acknowledgement,
            },
        );
        let fake_host_dir =
            std::env::temp_dir().join(format!("swarm-acp-fake-store-{}", Uuid::new_v4()));
        fs::create_dir_all(&fake_host_dir)?;
        let mut fake_listener = swarm_kernel_host::ipc::Listener::bind(&fake_host_dir)
            .map_err(|_| super::invalid("fake Store IPC bind failed"))?;
        let fake_server = tokio::spawn(async move {
            use futures::{SinkExt as _, StreamExt as _};
            let Ok(stream) = fake_listener.accept().await else {
                return;
            };
            let (read, write) = tokio::io::split(stream);
            let mut reader = tokio_util::codec::FramedRead::new(
                read,
                tokio_util::codec::LinesCodec::new_with_max_length(1024 * 1024),
            );
            let mut writer = tokio_util::codec::FramedWrite::new(
                write,
                tokio_util::codec::LinesCodec::new_with_max_length(1024 * 1024),
            );
            while let Some(line) = reader.next().await {
                let Ok(line) = line else {
                    break;
                };
                let request: Value = serde_json::from_str(&line).unwrap_or(json!({}));
                let id = request["id"].clone();
                let reply = json!({"jsonrpc": "2.0", "id": id, "result": {}});
                let Ok(encoded) = serde_json::to_string(&reply) else {
                    break;
                };
                if writer.send(encoded).await.is_err() {
                    break;
                }
            }
        });
        let fake_credential = swarm_contracts::Credential {
            client_id: "fake-store-client".to_owned(),
            token: "fake-store-token".to_owned(),
        };
        let fake_ipc_config = swarm_client::IpcConfig::default();
        let fake_module_link =
            swarm_client::ModuleLink::connect(&fake_host_dir, &fake_credential, &fake_ipc_config)
                .await?;
        let mut descriptor_value: Value =
            serde_json::from_str(include_str!("../../module-descriptor-acp.template.json"))
                .expect("maintained ACP descriptor parses");
        descriptor_value["enabled"] = json!(true);
        descriptor_value["launch"]["executable"] =
            json!(std::env::current_exe().expect("test executable path"));
        descriptor_value["launch"]["argv"] = json!([]);
        descriptor_value["launch"]["credential_ref"] =
            json!("qualification:pr44-production-cancel-unknown");
        let descriptor: swarm_contracts::module_catalog::ModuleDescriptor =
            serde_json::from_value(descriptor_value).expect("ACP descriptor is typed");
        let claim = swarm_contracts::module_contract::ModuleContractClaim::from_descriptor(
            &descriptor,
            swarm_contracts::module_contract::MODULE_PROTOCOL_V1,
        )
        .expect("descriptor yields claim");
        let owner = super::super::Owner {
            state_dir: state.state_dir.clone(),
            token: "production-cancel-unknown-token".to_owned(),
            record: json!({"version": 1, "token": "production-cancel-unknown-token"}),
            host: crate::module_host::ModuleHostIdentity {
                claim,
                profile: crate::Profile::AcpV2,
                boot_id: Uuid::new_v4().to_string(),
                binding_id: dispatch_command.binding_id.clone(),
                generation: dispatch_command.generation,
                module_client_id: fake_credential.client_id.clone(),
                credential_file: fake_host_dir.join("fake-credential.json"),
            },
        };
        let config = super::super::Config {
            module_artifact_id: crate::ACP_ARTIFACT_ID.to_owned(),
            command: std::env::temp_dir().join("unused-production-cancel"),
            command_args: Vec::new(),
            mod_path: None,
            run_timeout_ms: None,
            host_connection: None,
        };
        let mut link = super::super::Link {
            client: fake_module_link,
            binding_id: dispatch_command.binding_id.clone(),
            generation: dispatch_command.generation,
            route: dispatch_command.route.clone(),
            profile: crate::Profile::AcpV2,
            recovery_required: false,
        };
        let (transport, mut peer) = Channel::duplex();
        let dispatch_for_client = dispatch_command.clone();
        let permissions_for_client = Arc::clone(&permissions);
        let first_for_client = first_cancel.clone();
        let retry_for_client = retry_cancel.clone();
        let maybe_for_client = maybe_written_cancel.clone();
        let prompt_session_id = session_id.clone();
        let key_for_client = key.clone();
        let journal_for_client = state.journal.clone();
        let session_for_client = session.clone();
        let capture_for_client: Arc<Mutex<super::Capture>> =
            Arc::new(Mutex::new(super::Capture::default()));
        let features_for_client = super::AgentFeatures {
            close_supported: true,
            load_session_supported: false,
            agent_info: None,
            capabilities: json!({}),
        };
        let client = Client.builder().connect_with(transport, async move |connection| {
            let mut link = link;
            let owner = owner;
            let config = config;
            let capture = capture_for_client;
            let journal = journal_for_client;
            let recovery = super::ModuleLinkRecovery { config: &config, credential: &fake_credential, owner: &owner, journal: &journal, capture: &capture };
            let request = connection.send_request(super::PromptRequest::new(prompt_session_id, vec![ContentBlock::Text(TextContent::new("fixture-only"))])).block_task();
            tokio::pin!(request);
            let permission_wakeup = tokio::sync::Notify::new();
            let observation_sequence = Arc::new(Mutex::new(1_u64));
            let workspace_buf = std::env::temp_dir();
            let model_string = "fixture-model".to_owned();
            let mut session_state = Some(session_for_client);
            let mut source = PromptControlsFixture { controls: vec![json!({"command": first_for_client}), json!({"command": retry_for_client})].into_iter().collect() };
            let super::PromptEvent::Host(first_message) = super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup).await else { panic!("active prompt receives first cancel") };
            let first_control: RuntimeCommand = serde_json::from_value(first_message["command"].clone()).expect("control is RuntimeCommand");
            {
                let mut context = super::CommandContext { connection: &connection, link: &mut link, recovery: &recovery, owner: &owner, journal: &journal, features: &features_for_client, session: &mut session_state, workspace: Some(workspace_buf.as_path()), model: Some(model_string.as_str()), capture: &capture, permissions: &permissions_for_client, permission_wakeup: &permission_wakeup, observation_sequence: &observation_sequence };
                super::process_control_during_turn(&mut context, &dispatch_for_client, first_control.clone()).await.map_err(super::to_acp_error)?;
                assert!(!context.link.recovery_required, "pre-notification permission uncertainty keeps link");
                let (first_outcome, _) = context.journal.read_outcome(&first_control.operation_id).map_err(super::to_acp_error)?.expect("first Unknown published");
                assert_eq!(first_outcome.outcome, EffectOutcome::Unknown);
                let permission_effect = super::permission_cancel_effect_id(&key_for_client);
                assert!(context.journal.read_effect(&first_control, &permission_effect, "intent").map_err(super::to_acp_error)?.is_some());
                assert!(context.journal.read_effect(&first_control, &permission_effect, "receipt").map_err(super::to_acp_error)?.is_none());
                assert!(context.journal.read_effect(&first_control, "session/cancel", "intent").map_err(super::to_acp_error)?.is_none());
            }
            let super::PromptEvent::Host(retry_message) = super::next_prompt_event(request.as_mut(), &mut source, &permission_wakeup).await else { panic!("same prompt stays active for retry") };
            let retry_control: RuntimeCommand = serde_json::from_value(retry_message["command"].clone()).expect("retry is RuntimeCommand");
            assert_eq!(retry_control.input["target_operation_id"], dispatch_for_client.operation_id);
            {
                let mut context = super::CommandContext { connection: &connection, link: &mut link, recovery: &recovery, owner: &owner, journal: &journal, features: &features_for_client, session: &mut session_state, workspace: Some(workspace_buf.as_path()), model: Some(model_string.as_str()), capture: &capture, permissions: &permissions_for_client, permission_wakeup: &permission_wakeup, observation_sequence: &observation_sequence };
                super::process_control_during_turn(&mut context, &dispatch_for_client, retry_control.clone()).await.map_err(super::to_acp_error)?;
                assert!(!context.link.recovery_required);
                let (retry_outcome, _) = context.journal.read_outcome(&retry_control.operation_id).map_err(super::to_acp_error)?.expect("retry Applied published");
                assert_eq!(retry_outcome.outcome, EffectOutcome::Applied);
                assert_eq!(retry_outcome.details["cancel"], "notification_sent");
                assert!(context.journal.read_effect(&retry_control, "session/cancel", "intent").map_err(super::to_acp_error)?.is_some());
                assert!(context.journal.read_effect(&retry_control, "session/cancel", "receipt").map_err(super::to_acp_error)?.is_some());
            }
            {
                let expected_intent = json!({"native_session_id": session_state.as_ref().expect("session open").id, "target_operation_id": dispatch_for_client.operation_id, "turn_id": dispatch_for_client.operation_id});
                journal.record_effect(&maybe_for_client, "session/cancel", "intent", expected_intent).map_err(super::to_acp_error)?;
                let mut context = super::CommandContext { connection: &connection, link: &mut link, recovery: &recovery, owner: &owner, journal: &journal, features: &features_for_client, session: &mut session_state, workspace: Some(workspace_buf.as_path()), model: Some(model_string.as_str()), capture: &capture, permissions: &permissions_for_client, permission_wakeup: &permission_wakeup, observation_sequence: &observation_sequence };
                let fenced = match super::process_control_during_turn(&mut context, &dispatch_for_client, maybe_for_client.clone()).await { Err(error) => error, Ok(()) => return Err(super::to_acp_error(super::invalid("possibly written cancel replayed"))) };
                assert_eq!(fenced.code, "ACP_CONTROL_EFFECT_UNKNOWN");
                assert!(context.link.recovery_required, "other uncertainty retires link");
                let (fenced_outcome, _) = context.journal.read_outcome(&maybe_for_client.operation_id).map_err(super::to_acp_error)?.expect("fenced Unknown published");
                assert_eq!(fenced_outcome.outcome, EffectOutcome::Unknown);
            }
            Ok::<(), agent_client_protocol::Error>(())
        });
        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(prompt))) =
                peer.rx.next().await
            else {
                panic!("fixture starts one prompt")
            };
            assert_eq!(prompt.method.as_ref(), "session/prompt");
            let Some(TransportFrame::Single(RawJsonRpcMessage::Notification(cancel))) =
                peer.rx.next().await
            else {
                panic!("later exact cancel reaches native once")
            };
            assert_eq!(cancel.method.as_ref(), "session/cancel");
            Ok::<(), agent_client_protocol::Error>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("production fixture bounded")
        .expect("production fixture succeeds");
        fake_server.abort();
        let _ = fs::remove_dir_all(&fake_host_dir);
        let permission_effect = super::permission_cancel_effect_id(&key);
        assert!(
            state
                .journal
                .read_effect(&first_cancel, "session/cancel", "intent")?
                .is_none()
        );
        assert!(
            state
                .journal
                .read_effect(&first_cancel, &permission_effect, "intent")?
                .is_some()
        );
        assert!(
            state
                .journal
                .read_effect(&first_cancel, &permission_effect, "receipt")?
                .is_none()
        );
        let (first_saved, _) = state
            .journal
            .read_outcome(&first_cancel.operation_id)?
            .expect("first Unknown readable");
        assert_eq!(first_saved.outcome, EffectOutcome::Unknown);
        assert!(
            state
                .journal
                .read_effect(&retry_cancel, "session/cancel", "intent")?
                .is_some()
        );
        assert!(
            state
                .journal
                .read_effect(&retry_cancel, "session/cancel", "receipt")?
                .is_some()
        );
        let (retry_saved, _) = state
            .journal
            .read_outcome(&retry_cancel.operation_id)?
            .expect("retry Applied readable");
        assert_eq!(retry_saved.outcome, EffectOutcome::Applied);
        assert!(
            state
                .journal
                .read_effect(&maybe_written_cancel, "session/cancel", "receipt")?
                .is_none()
        );
        let (fenced_saved, _) = state
            .journal
            .read_outcome(&maybe_written_cancel.operation_id)?
            .expect("fenced Unknown readable");
        assert_eq!(fenced_saved.outcome, EffectOutcome::Unknown);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_family_stays_unknown_after_direct_child_exit() {
        let owner = tokio::process::Command::new(
            std::env::current_exe().expect("test executable path is available"),
        )
        .args([
            "--exact",
            "adapter::acp::tests::acp_family_fixture_owner",
            "--ignored",
            "--nocapture",
        ])
        .stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("isolated owner helper starts");
        let output = tokio::time::timeout(Duration::from_secs(8), owner.wait_with_output())
            .await
            .expect("isolated owner helper finishes within its bound")
            .expect("isolated owner helper exit is observable");
        assert!(output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("acp_family_fixture_owner ... ok"),
            "isolated helper ran the exact family fixture"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawned only by the bounded ACP process-family fixture"]
    async fn acp_family_fixture_owner() {
        let token = format!("pr44-acp-family-{}", Uuid::new_v4());
        let group = swarm_process::Group::enter_module(&token)
            .expect("isolated fixture obtains its own process family");
        let mut direct_child = tokio::process::Command::new(
            std::env::current_exe().expect("test executable path is available"),
        )
        .args([
            "--exact",
            "adapter::acp::tests::acp_family_fixture_direct_child",
            "--ignored",
            "--nocapture",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("fixture ACP child starts");
        let status = tokio::time::timeout(Duration::from_secs(3), direct_child.wait())
            .await
            .expect("fixture ACP child exits within its bound")
            .expect("fixture ACP child exit is observable");
        assert!(status.success());

        let direct_exit = stop_direct_child(&mut direct_child).await;
        assert!(direct_exit.observed);
        let mut receipt = json!({});
        add_family_custody_facts(&mut receipt, &direct_exit);
        assert_eq!(receipt["direct_child_exit_observed"], true);
        assert_eq!(receipt["family_departure_claimed"], false);
        assert_eq!(receipt["owner_family_departure_required"], true);
        assert!(
            !group
                .children_empty()
                .expect("exact fixture family is observable"),
            "the controlled grandchild outlives its direct ACP wrapper"
        );

        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert!(
            group
                .children_empty()
                .expect("exact fixture family remains observable")
        );
        group
            .disarm()
            .expect("fixture owner disarms only after family departure");
    }

    #[test]
    #[ignore = "spawned only by the bounded ACP process-family fixture"]
    fn acp_family_fixture_direct_child() {
        let grandchild = std::process::Command::new(
            std::env::current_exe().expect("test executable path is available"),
        )
        .args([
            "--exact",
            "adapter::acp::tests::acp_family_fixture_grandchild",
            "--ignored",
            "--nocapture",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("controlled fixture grandchild starts");
        drop(grandchild);
    }

    #[test]
    #[ignore = "spawned only by the bounded ACP process-family fixture"]
    fn acp_family_fixture_grandchild() {
        std::thread::sleep(Duration::from_millis(1200));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_restore_routes_pre_response_history_to_the_retained_session() {
        let session_id = SessionId::new("session-retained");
        let peer_session_id = session_id.clone();
        let session_text = session_id.to_string();
        let workspace = std::env::temp_dir().join("swarm-command-acp-restore-fixture");
        let client_workspace = workspace.clone();
        let peer_workspace = workspace.clone();
        let capture = Arc::new(Mutex::new(super::Capture::default()));
        let notification_capture = Arc::clone(&capture);
        let load_capture = Arc::clone(&capture);
        let (transport, mut peer) = Channel::duplex();

        let client = Client
            .builder()
            .on_receive_notification(
                async move |notification: SessionNotification, _connection| {
                    if let Ok(mut capture) = notification_capture.lock() {
                        capture_session_notification(&mut capture, &notification);
                    }
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_with(transport, async move |connection| {
                let features = super::initialize(&connection).await?;
                assert!(features.load_session_supported);
                let loaded = load_retained_session(
                    &connection,
                    &load_capture,
                    &session_text,
                    &session_id,
                    &client_workspace,
                )
                .await
                .map_err(super::to_acp_error)?;
                let options = loaded.config_options.unwrap_or_default();
                assert!(super::model_current_value_matches(
                    &options,
                    "fixture-model"
                ));
                Ok(())
            });

        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(initialize))) =
                peer.rx.next().await
            else {
                panic!("expected ACP initialize request")
            };
            assert_eq!(initialize.method.as_ref(), "initialize");
            let initialize_response = InitializeResponse::new(ProtocolVersion::V1)
                .agent_capabilities(AgentCapabilities::new().load_session(true));
            peer.tx
                .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                    initialize.id,
                    Ok(serde_json::to_value(initialize_response)
                        .expect("initialize response serializes")),
                )))
                .expect("client accepts initialize response");

            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(load))) =
                peer.rx.next().await
            else {
                panic!("expected exact session/load request")
            };
            assert_eq!(load.method.as_ref(), "session/load");
            let Some(RawJsonRpcParams::Object(params)) = load.params else {
                panic!("session/load parameters are an object")
            };
            assert_eq!(params["sessionId"], "session-retained");
            assert_eq!(params["cwd"], peer_workspace.to_string_lossy().as_ref());

            let notification = SessionNotification::new(
                peer_session_id,
                SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                    TextContent::new("replayed history"),
                ))),
            );
            let update = RawJsonRpcMessage::notification(
                "session/update".to_owned(),
                serde_json::to_value(notification).expect("history notification serializes"),
            )
            .expect("history notification is valid JSON-RPC");
            let response = RawJsonRpcMessage::response(
                load.id,
                Ok(
                    serde_json::to_value(LoadSessionResponse::new().config_options(vec![
                        SessionConfigOption::select(
                            "command-model-choice",
                            "Model",
                            "fixture-model",
                            vec![SessionConfigSelectOption::new(
                                "fixture-model",
                                "Fixture Model",
                            )],
                        )
                        .category(SessionConfigOptionCategory::Model),
                    ]))
                    .expect("load response serializes"),
                ),
            );
            let batch = TransportBatch::from_messages([update, response])
                .expect("pre-response replay batch is non-empty");
            peer.tx
                .unbounded_send(TransportFrame::Batch(batch))
                .expect("client accepts replay before load response");

            let mut later_requests = Vec::new();
            while let Some(frame) = peer.rx.next().await {
                if let TransportFrame::Single(RawJsonRpcMessage::Request(request)) = frame {
                    later_requests.push(request.method);
                }
            }
            assert!(
                later_requests.is_empty(),
                "restore sent another ACP request"
            );
            Ok::<(), agent_client_protocol::Error>(())
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("ACP restore fixture timed out")
        .expect("ACP restore fixture failed");

        let capture = capture.lock().expect("capture lock remains usable");
        assert_eq!(
            capture.native_session_id.as_deref(),
            Some("session-retained")
        );
        assert_eq!(capture.history_updates, 1);
        assert_eq!(capture.output_total_bytes, 0);
        assert!(capture.output.is_empty());
        assert!(!capture.history_replaying);
        assert!(capture.protocol_error.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_restore_failure_does_not_fallback_or_replay() {
        let session_id = SessionId::new("session-retained");
        let peer_session_id = session_id.clone();
        let session_text = session_id.to_string();
        let workspace = std::env::temp_dir().join("swarm-command-acp-failed-restore-fixture");
        let capture = Arc::new(Mutex::new(super::Capture::default()));
        let notification_capture = Arc::clone(&capture);
        let load_capture = Arc::clone(&capture);
        let (transport, mut peer) = Channel::duplex();

        let client = Client
            .builder()
            .on_receive_notification(
                async move |notification: SessionNotification, _connection| {
                    if let Ok(mut capture) = notification_capture.lock() {
                        capture_session_notification(&mut capture, &notification);
                    }
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_with(transport, async move |connection| {
                let features = super::initialize(&connection).await?;
                assert!(features.load_session_supported);
                let error = load_retained_session(
                    &connection,
                    &load_capture,
                    &session_text,
                    &session_id,
                    &workspace,
                )
                .await
                .expect_err("failed native load cannot create a replacement session");
                assert_eq!(error.code, "RECOVERY_REQUIRED");
                Ok(())
            });

        let peer = async move {
            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(initialize))) =
                peer.rx.next().await
            else {
                panic!("expected ACP initialize request")
            };
            let initialize_response = InitializeResponse::new(ProtocolVersion::V1)
                .agent_capabilities(AgentCapabilities::new().load_session(true));
            peer.tx
                .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                    initialize.id,
                    Ok(serde_json::to_value(initialize_response)
                        .expect("initialize response serializes")),
                )))
                .expect("client accepts initialize response");

            let Some(TransportFrame::Single(RawJsonRpcMessage::Request(load))) =
                peer.rx.next().await
            else {
                panic!("expected exact session/load request")
            };
            assert_eq!(load.method.as_ref(), "session/load");
            let Some(RawJsonRpcParams::Object(params)) = load.params else {
                panic!("session/load parameters are an object")
            };
            assert_eq!(params["sessionId"], peer_session_id.to_string());
            peer.tx
                .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                    load.id,
                    Err(agent_client_protocol::Error::invalid_params()),
                )))
                .expect("client accepts failed load response");

            let mut later_requests = Vec::new();
            while let Some(frame) = peer.rx.next().await {
                if let TransportFrame::Single(RawJsonRpcMessage::Request(request)) = frame {
                    later_requests.push(request.method);
                }
            }
            assert!(
                later_requests.is_empty(),
                "failed restore attempted a fallback request"
            );
            Ok::<(), agent_client_protocol::Error>(())
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            futures::try_join!(client, peer)
        })
        .await
        .expect("failed ACP restore fixture timed out")
        .expect("failed ACP restore fixture failed");

        let capture = capture.lock().expect("capture lock remains usable");
        assert_eq!(
            capture.native_session_id.as_deref(),
            Some("session-retained")
        );
        assert!(!capture.history_replaying);
        assert!(capture.output.is_empty());
    }

    #[test]
    fn persistent_permission_rejects_before_once_only_intent() -> super::Result<()> {
        let state = TestState::new()?;
        let request_id = json!("permission-request-1");
        let request_key = serde_json::to_string(&request_id)?;
        let fingerprint = digest(b"pending permission request fixture");

        let persistent_command = permission_reply_command();
        assert_error_code(
            super::record_permission_reply_intent(
                &state.journal,
                &persistent_command,
                &request_key,
                "session-fixture",
                &request_id,
                &fingerprint,
                "allow-always",
                super::PermissionOptionKind::AllowAlways,
            ),
            "NATIVE_PERMISSION_PERSISTENT_UNSUPPORTED",
        );
        assert_eq!(journal_effect_count(&state, &persistent_command)?, 0);

        let once_command = permission_reply_command();
        super::record_permission_reply_intent(
            &state.journal,
            &once_command,
            &request_key,
            "session-fixture",
            &request_id,
            &fingerprint,
            "allow-once",
            super::PermissionOptionKind::AllowOnce,
        )?;
        assert_eq!(journal_effect_count(&state, &once_command)?, 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr44_command_acp_close_settles_its_pending_permission_once() -> super::Result<()> {
        let state = TestState::new()?;
        let mut close_command = permission_reply_command();
        close_command.method = "native.command.close_session".to_owned();
        let session_id = SessionId::new("session-close-permission-fixture");
        let permissions: super::PermissionBroker =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let request_id = json!("permission-close-fixture");
        let key = serde_json::to_string(&request_id)?;
        let fingerprint = digest(b"close permission fixture");
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel();
        let (ack_sender, acknowledgement) = tokio::sync::oneshot::channel();
        permissions.lock().expect("permission broker lock").insert(
            key.clone(),
            super::PendingPermission {
                session_id: session_id.to_string(),
                request_id: request_id.clone(),
                fingerprint: fingerprint.clone(),
                options: vec![],
                reply: reply_sender,
                acknowledgement,
            },
        );

        let other_id = json!("permission-other-session");
        let other_key = serde_json::to_string(&other_id)?;
        let (other_reply_sender, _other_reply_receiver) = tokio::sync::oneshot::channel();
        let (_other_ack_sender, other_acknowledgement) = tokio::sync::oneshot::channel();
        permissions.lock().expect("permission broker lock").insert(
            other_key.clone(),
            super::PendingPermission {
                session_id: "session-other-fixture".to_owned(),
                request_id: other_id,
                fingerprint: digest(b"other session permission"),
                options: vec![],
                reply: other_reply_sender,
                acknowledgement: other_acknowledgement,
            },
        );

        let (cancel_result, response) = tokio::time::timeout(Duration::from_secs(3), async {
            let response = async move {
                let response = reply_receiver
                    .await
                    .expect("close sends the pending permission cancellation");
                ack_sender
                    .send(true)
                    .expect("native transport confirms the cancellation response");
                response
            };
            tokio::join!(
                super::cancel_pending_permissions(
                    &state.journal,
                    &permissions,
                    &close_command,
                    &session_id
                ),
                response
            )
        })
        .await
        .expect("close permission settlement remains bounded");
        cancel_result?;
        assert!(matches!(
            response,
            super::RequestPermissionOutcome::Cancelled
        ));
        super::cancel_pending_permissions(
            &state.journal,
            &permissions,
            &close_command,
            &session_id,
        )
        .await?;

        let pending = permissions.lock().expect("permission broker lock");
        assert!(!pending.contains_key(&key));
        assert!(pending.contains_key(&other_key));
        drop(pending);
        let effect_id = super::permission_cancel_effect_id(&key);
        let directory = state.journal.operation_dir(&close_command.operation_id)?;
        let effect_hash = digest(effect_id.as_bytes());
        let receipt: Value = serde_json::from_slice(&fs::read(
            directory.join(format!("effect-receipt-{effect_hash}.json")),
        )?)?;
        assert_eq!(receipt["evidence"]["response_written"], true);
        assert_eq!(journal_effect_count(&state, &close_command)?, 2);
        Ok(())
    }

    #[test]
    fn pr44_command_acp_cancel_and_reply_race_claims_one_response() -> super::Result<()> {
        let state = TestState::new()?;
        let request_id = json!("permission-race-request");
        let key = serde_json::to_string(&request_id)?;
        let fingerprint = digest(b"cancel-vs-reply fixture");
        let permissions: super::PermissionBroker =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (reply_sender, _reply_receiver) = tokio::sync::oneshot::channel();
        let (_ack_sender, acknowledgement) = tokio::sync::oneshot::channel();
        permissions.lock().expect("permission broker lock").insert(
            key.clone(),
            super::PendingPermission {
                session_id: "session-fixture".to_owned(),
                request_id: request_id.clone(),
                fingerprint: fingerprint.clone(),
                options: vec![(
                    "allow-once".to_owned(),
                    super::PermissionOptionKind::AllowOnce,
                    "allow once".to_owned(),
                )],
                reply: reply_sender,
                acknowledgement,
            },
        );

        let reply_command = permission_reply_command();
        let mut cancel_command = permission_reply_command();
        cancel_command.method = "native.command.cancel_turn".to_owned();
        let reply_command_for_thread = reply_command.clone();
        let cancel_command_for_thread = cancel_command.clone();
        let reply_journal = state.journal.clone();
        let cancel_journal = state.journal.clone();
        let reply_key = key.clone();
        let cancel_key = key.clone();
        let reply_request_id = request_id.clone();
        let cancel_request_id = request_id.clone();
        let reply_fingerprint = fingerprint.clone();
        let cancel_fingerprint = fingerprint.clone();
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let (reply_claim, cancel_claim) = std::thread::scope(|scope| {
            let reply_barrier = Arc::clone(&barrier);
            let cancel_barrier = Arc::clone(&barrier);
            let reply_permissions = Arc::clone(&permissions);
            let cancel_permissions = Arc::clone(&permissions);
            let reply = scope.spawn(move || {
                reply_barrier.wait();
                super::claim_pending_permission(
                    &reply_permissions,
                    &reply_key,
                    "session-fixture",
                    |pending| {
                        if pending.request_id != reply_request_id
                            || pending.fingerprint != reply_fingerprint
                        {
                            return Err(super::invalid("reply race identity changed"));
                        }
                        super::record_permission_reply_intent(
                            &reply_journal,
                            &reply_command_for_thread,
                            &reply_key,
                            "session-fixture",
                            &reply_request_id,
                            &reply_fingerprint,
                            "allow-once",
                            super::PermissionOptionKind::AllowOnce,
                        )
                    },
                )
            });
            let cancel = scope.spawn(move || {
                cancel_barrier.wait();
                super::claim_pending_permission(
                    &cancel_permissions,
                    &cancel_key,
                    "session-fixture",
                    |pending| {
                        cancel_journal.record_effect(
                            &cancel_command_for_thread,
                            "permission-cancel-race",
                            "intent",
                            json!({
                                "session_id":pending.session_id,
                                "request_id":cancel_request_id,
                                "fingerprint":cancel_fingerprint,
                                "response":"cancelled"
                            }),
                        )
                    },
                )
            });
            barrier.wait();
            (
                reply.join().expect("reply claim thread joins"),
                cancel.join().expect("cancel claim thread joins"),
            )
        });

        let reply_claim = reply_claim?;
        let cancel_claim = cancel_claim?;
        assert_ne!(reply_claim.is_some(), cancel_claim.is_some());
        assert!(
            permissions
                .lock()
                .expect("permission broker lock")
                .is_empty()
        );
        assert_eq!(
            journal_effect_count(&state, &reply_command)?
                + journal_effect_count(&state, &cancel_command)?,
            1
        );
        Ok(())
    }

    #[test]
    fn pr44_command_acp_config_option_snapshot_keeps_typed_values_and_resolves_model_id()
    -> super::Result<()> {
        let options: Vec<super::SessionConfigOption> = serde_json::from_value(json!([
            {
                "id":"command-model-choice",
                "name":"Model",
                "category":"model",
                "description":"display-only model label",
                "type":"select",
                "currentValue":"model-a",
                "options":[
                    {"value":"model-a","name":"Model A","description":"label A","_meta":{"vendor":"drop"}},
                    {"value":"model-b","name":"Model B","_meta":{"vendor":"drop"}}
                ],
                "_meta":{"vendor":"drop"}
            },
            {
                "id":"thought",
                "name":"Thought level",
                "category":"thought_level",
                "type":"select",
                "currentValue":"low",
                "options":[
                    {"group":"levels","name":"Levels","options":[
                        {"value":"low","name":"Low","_meta":{"vendor":"drop"}},
                        {"value":"high","name":"High"}
                    ]}
                ],
                "_meta":{"vendor":"drop"}
            },
            {
                "id":"fast-mode",
                "name":"Fast mode",
                "type":"boolean",
                "currentValue":true,
                "_meta":{"vendor":"drop"}
            }
        ]))?;
        let snapshot = super::session_config_options_snapshot(&options)?;
        assert_eq!(
            super::model_config_option_id(&options, "model-a").as_deref(),
            Some("command-model-choice")
        );
        assert!(super::model_value_available(&options, "model-a"));
        assert!(super::model_current_value_matches(&options, "model-a"));
        assert!(!super::model_current_value_matches(&options, "model-b"));
        assert!(super::category_config_value_matches(
            &options,
            &SessionConfigOptionCategory::ThoughtLevel,
            "low"
        ));
        assert!(super::config_select_contains(&options[1], "high"));
        assert!(!super::config_select_contains(&options[1], "max"));
        let untagged = vec![SessionConfigOption::select(
            "provider-specific-model-option",
            "Model",
            "model-a",
            vec![SessionConfigSelectOption::new("model-a", "Model A")],
        )];
        assert!(super::model_config_option_id(&untagged, "model-a").is_none());
        assert!(!super::model_value_available(&untagged, "model-a"));
        let custom = SessionConfigOption::select(
            "custom-option",
            "Custom",
            "model-a",
            vec![SessionConfigSelectOption::new("model-a", "Same value")],
        )
        .category(SessionConfigOptionCategory::Other("custom".to_owned()));
        assert!(super::model_config_option_id(std::slice::from_ref(&custom), "model-a").is_none());
        let typed_and_custom = vec![options[0].clone(), custom];
        assert_eq!(
            super::model_config_option_id(&typed_and_custom, "model-a").as_deref(),
            Some("command-model-choice")
        );
        let model_config = vec![
            SessionConfigOption::select(
                "model-tuning",
                "Model tuning",
                "model-a",
                vec![SessionConfigSelectOption::new("model-a", "Model A")],
            )
            .category(SessionConfigOptionCategory::ModelConfig),
        ];
        assert!(super::model_config_option_id(&model_config, "model-a").is_none());
        assert_eq!(
            snapshot,
            json!({
                "schema_version":1,
                "options":[
                    {
                        "id":"command-model-choice",
                        "category":"model",
                        "kind":"select",
                        "allowed_value_ids":["model-a","model-b"],
                        "current_value_id":"model-a"
                    },
                    {
                        "id":"thought",
                        "category":"thought_level",
                        "kind":"select",
                        "allowed_value_ids":["low","high"],
                        "current_value_id":"low"
                    },
                    {
                        "id":"fast-mode",
                        "category":null,
                        "kind":"boolean",
                        "current_value":true
                    }
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn config_option_snapshot_respects_record_byte_bound() -> super::Result<()> {
        let oversized_id = "x".repeat(super::MAX_RECORD_BYTES as usize);
        let options = [super::SessionConfigOption::boolean(
            oversized_id,
            "toggle",
            true,
        )];
        assert_error_code(
            super::session_config_options_snapshot(&options),
            "ADAPTER_EVIDENCE_INVALID",
        );
        Ok(())
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
