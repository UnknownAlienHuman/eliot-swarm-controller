//! Bounded descriptor-bound native MCP command input.
//!
//! The controller sends this value inside the existing authenticated
//! [`crate::runtime::RuntimeCommand`]. It carries the retained operation and
//! assignment identity plus opaque, Store-authorized private artifact
//! references. It never carries credentials, profile contents, native command
//! arguments, challenge nonce bytes, or provider/model payloads.

use crate::{
    module_catalog::{ProtectedRef, Sha256Digest},
    runtime::RuntimeCommand,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub const NATIVE_MCP_COMMAND_SCHEMA_ID: &str = "swarm.native_mcp_command";
pub const NATIVE_MCP_COMMAND_SCHEMA_VERSION: &str = "2";

pub const NATIVE_MCP_INSTALL_METHOD: &str = "native.mcp.install";
pub const NATIVE_MCP_OBSERVE_METHOD: &str = "native.mcp.observe";
pub const NATIVE_MCP_ARM_METHOD: &str = "native.mcp.arm";
pub const NATIVE_MCP_READ_METHOD: &str = "native.mcp.read";

/// Sorted in the same order as the descriptor's capability set.
pub const NATIVE_MCP_METHODS: [&str; 4] = [
    NATIVE_MCP_ARM_METHOD,
    NATIVE_MCP_INSTALL_METHOD,
    NATIVE_MCP_OBSERVE_METHOD,
    NATIVE_MCP_READ_METHOD,
];

const MAX_OPERATION_ID_BYTES: usize = 256;
const MAX_BINDING_ID_BYTES: usize = 256;
const MAX_SESSION_ID_BYTES: usize = 256;
const MAX_TASK_ID_BYTES: usize = 512;
const MAX_SERVICE_ID_BYTES: usize = 128;
const MAX_SERVICE_VERSION_BYTES: usize = 128;
const MAX_NATIVE_TEXT_BYTES: usize = 256;
const MAX_PARTICIPANT_ID_BYTES: usize = 256;
const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_NATIVE_MCP_SERVERS: usize = 256;
#[cfg(test)]
const PINNED_OPENCODE_VERSION: &str = "2.0.7";

pub const NATIVE_MCP_ASSIGNMENT_READBACK_KIND: &str = "native_mcp_assignment_readback";

pub const CODEX_NATIVE_MCP_COMMAND_SCHEMA_ID: &str = "swarm.codex_native_mcp_command";
pub const CODEX_NATIVE_MCP_COMMAND_SCHEMA_VERSION: &str = "3";
pub const CODEX_NATIVE_MCP_LAUNCH_PROFILE_SCHEMA_ID: &str = "swarm.codex_native_mcp_launch_profile";
pub const CODEX_NATIVE_MCP_LAUNCH_PROFILE_SCHEMA_VERSION: &str = "1";
pub const CODEX_NATIVE_MCP_READBACK_KIND: &str = "codex_native_mcp_assignment_readback";
pub const CODEX_NATIVE_MCP_SERVER_INFO_NAME: &str = "eliot-swarm-controller";
pub const CODEX_NATIVE_MCP_EXECUTABLE_NAME: &str = if cfg!(windows) {
    "swarm-mcp.exe"
} else {
    "swarm-mcp"
};

const MAX_PRIVATE_PATH_BYTES: usize = 4096;

/// Stable, launch-specific Codex MCP configuration key. Its name is separate
/// from the MCP server's peer-reported `serverInfo.name` and avoids replacing
/// any operator-configured server entry.
pub fn codex_native_mcp_server_name(launch_operation_id: &str) -> Result<String, &'static str> {
    if !bounded_text(launch_operation_id, MAX_OPERATION_ID_BYTES) {
        return Err("Codex launch operation identity is invalid");
    }
    let digest = format!("{:x}", Sha256::digest(launch_operation_id.as_bytes()));
    Ok(format!("eliot_swarm_{digest}"))
}

/// Transient input used only on the exact `agent.open` RuntimeCommand. Store
/// resolves the opaque references to these two existing private files in
/// memory; it must not persist either path in an Operation or receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexNativeMcpLaunchProfile {
    pub schema_version: u16,
    pub kind: String,
    pub launch_operation_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub participant_id: String,
    pub grant_revision: i64,
    /// The Codex thread does not exist yet. A caller must never fabricate a
    /// prepared root or pre-bind this Participant to an unknown session ID.
    pub participant_native_session_id: Option<String>,
    pub credential_ref: String,
    pub profile_config_ref: String,
    pub profile_name: String,
    pub surface: String,
    pub server_name: String,
    pub executable: String,
    pub credential_path: String,
    pub profile_config_path: String,
}

impl CodexNativeMcpLaunchProfile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        launch_operation_id: String,
        task_id: String,
        task_revision: i64,
        attempt_id: String,
        binding_id: String,
        binding_generation: i64,
        participant_id: String,
        grant_revision: i64,
        credential_ref: String,
        profile_config_ref: String,
        profile_name: String,
        surface: String,
        executable: String,
        credential_path: String,
        profile_config_path: String,
    ) -> Result<Self, &'static str> {
        let value = Self {
            schema_version: 1,
            kind: "codex_native_mcp_launch_profile".to_owned(),
            server_name: codex_native_mcp_server_name(&launch_operation_id)?,
            launch_operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            participant_id,
            grant_revision,
            participant_native_session_id: None,
            credential_ref,
            profile_config_ref,
            profile_name,
            surface,
            executable,
            credential_path,
            profile_config_path,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        let expected_executable = Path::new(&self.executable)
            .file_name()
            .and_then(|name| name.to_str());
        if self.schema_version != 1
            || self.kind != "codex_native_mcp_launch_profile"
            || !bounded_text(&self.launch_operation_id, MAX_OPERATION_ID_BYTES)
            || !bounded_text(&self.task_id, MAX_TASK_ID_BYTES)
            || self.task_revision <= 0
            || !bounded_text(&self.attempt_id, MAX_SESSION_ID_BYTES)
            || !bounded_text(&self.binding_id, MAX_BINDING_ID_BYTES)
            || self.binding_generation <= 0
            || !bounded_text(&self.participant_id, MAX_PARTICIPANT_ID_BYTES)
            || self.grant_revision <= 0
            || self.participant_native_session_id.is_some()
            || !opaque_assignment_ref(&self.credential_ref, "participant-credential")
            || !opaque_assignment_ref(&self.profile_config_ref, "participant-mcp-config")
            || !bounded_text(&self.profile_name, MAX_PROFILE_NAME_BYTES)
            || self.surface != "participant-core"
            || self.server_name != codex_native_mcp_server_name(&self.launch_operation_id)?
            || !private_absolute_path(&self.executable)
            || expected_executable != Some(CODEX_NATIVE_MCP_EXECUTABLE_NAME)
            || !private_absolute_path(&self.credential_path)
            || !private_absolute_path(&self.profile_config_path)
            || self.credential_path == self.profile_config_path
        {
            return Err("Codex native MCP launch profile identity is invalid");
        }
        Ok(())
    }
}

/// Build only the dotted thread config overrides supported by Codex's
/// `thread/start` config map. No native user config is read or replaced.
pub fn codex_native_mcp_thread_config(
    profile: &CodexNativeMcpLaunchProfile,
) -> Result<Value, &'static str> {
    profile.validate()?;
    let prefix = format!("mcp_servers.{}", profile.server_name);
    let args = json!([
        "--config",
        profile.profile_config_path,
        "--credential",
        profile.credential_path,
        "--profile",
        profile.profile_name,
    ]);
    let mut values = serde_json::Map::new();
    values.insert(format!("{prefix}.command"), json!(profile.executable));
    values.insert(format!("{prefix}.args"), args);
    values.insert(format!("{prefix}.enabled"), json!(true));
    values.insert(format!("{prefix}.required"), json!(true));
    Ok(Value::Object(values))
}

fn private_absolute_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PRIVATE_PATH_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_control())
        && Path::new(value).is_absolute()
}

/// The single canonical Codex C8 comparison set. The digest is over sorted
/// native tool names and JavaScript-compatible input schemas, matching the
/// representation returned by the app-server MCP status API.
pub fn codex_participant_toolset() -> Result<(Vec<Value>, Sha256Digest), &'static str> {
    let contracts = crate::mcp_catalog::participant_core_tool_contracts()
        .map_err(|_| "Codex Participant core catalog is unavailable")?;
    let mut tools = Vec::with_capacity(contracts.len());
    let mut names = BTreeSet::new();
    for contract in contracts {
        let method = contract["method"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("Codex Participant method is invalid")?;
        let name = contract["name"]
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= MAX_NATIVE_TEXT_BYTES)
            .ok_or("Codex Participant tool name is invalid")?;
        let schema = contract["input_schema"]
            .as_object()
            .map(|_| javascript_schema(&contract["input_schema"]))
            .transpose()?
            .ok_or("Codex Participant input schema is invalid")?;
        if crate::mcp_catalog::tool_name(method) != name || !names.insert(name.to_owned()) {
            return Err("Codex Participant tool names are inconsistent");
        }
        tools.push(json!({"name":name,"input_schema":schema}));
    }
    if tools.is_empty() {
        return Err("Codex Participant core catalog is empty");
    }
    tools.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["name"].as_str().unwrap_or_default())
    });
    let ordered = ordered_json(&Value::Array(tools.clone()));
    let bytes = serde_json::to_vec(&ordered).map_err(|_| "Codex toolset serialization failed")?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    let digest = Sha256Digest::new(digest).map_err(|_| "Codex toolset digest is invalid")?;
    Ok((tools, digest))
}

fn javascript_schema(value: &Value) -> Result<Value, &'static str> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or("Codex MCP schema number is not representable"),
        Value::Array(values) => values
            .iter()
            .map(javascript_schema)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| Ok((key.clone(), javascript_schema(value)?)))
            .collect::<Result<serde_json::Map<String, Value>, &'static str>>()
            .map(Value::Object),
        other => Ok(other.clone()),
    }
}

fn ordered_json(value: &Value) -> Value {
    match value {
        Value::Object(fields) => {
            let fields: BTreeMap<_, _> = fields
                .iter()
                .map(|(key, value)| (key.clone(), ordered_json(value)))
                .collect();
            Value::Object(fields.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.iter().map(ordered_json).collect()),
        other => other.clone(),
    }
}

/// Store-admitted, read-only Codex MCP observation for the exact current
/// Participant assignment. It carries current assignment identity and expected
/// authorization/toolset digests, never profile refs, paths, tokens, or provider
/// settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexNativeMcpCommand {
    pub schema_version: u16,
    pub codex_schema_version: u16,
    pub kind: String,
    pub operation_id: String,
    pub launch_operation_id: String,
    pub phase: String,
    pub observation_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<Sha256Digest>,
    pub assignment_sha256: Sha256Digest,
    pub binding_id: String,
    pub binding_generation: i64,
    pub open_operation_id: String,
    pub native_root_id: String,
    pub native_scope_key: String,
    pub expected_adapter_boot_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub participant_id: String,
    pub grant_revision: i64,
    pub participant_native_session_id: Option<String>,
    pub surface: String,
    pub server_name: String,
    pub expected_authorization_revision: Sha256Digest,
    pub expected_toolset_sha256: Sha256Digest,
}

impl CodexNativeMcpCommand {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 3
            || self.codex_schema_version != 3
            || self.kind != "codex_native_mcp_command"
            || !bounded_text(&self.operation_id, MAX_OPERATION_ID_BYTES)
            || !bounded_text(&self.launch_operation_id, MAX_OPERATION_ID_BYTES)
            || self.phase != "observe"
            || self.observation_kind != "assigned_session"
            || self
                .input_sha256
                .as_ref()
                .is_some_and(|digest| !is_lower_sha256(digest))
            || !is_lower_sha256(&self.assignment_sha256)
            || !bounded_text(&self.binding_id, MAX_BINDING_ID_BYTES)
            || self.binding_generation <= 0
            || !bounded_text(&self.open_operation_id, MAX_OPERATION_ID_BYTES)
            || !bounded_native_text(&self.native_root_id)
            || !bounded_native_text(&self.native_scope_key)
            || !bounded_text(&self.expected_adapter_boot_id, MAX_OPERATION_ID_BYTES)
            || !bounded_text(&self.task_id, MAX_TASK_ID_BYTES)
            || self.task_revision <= 0
            || !bounded_text(&self.attempt_id, MAX_SESSION_ID_BYTES)
            || !bounded_text(&self.participant_id, MAX_PARTICIPANT_ID_BYTES)
            || self.grant_revision <= 0
            || self
                .participant_native_session_id
                .as_deref()
                .is_some_and(|value| !bounded_native_text(value))
            || self.surface != "participant-core"
            || self.server_name != codex_native_mcp_server_name(&self.launch_operation_id)?
            || !is_lower_sha256(&self.expected_authorization_revision)
            || !is_lower_sha256(&self.expected_toolset_sha256)
            || self.computed_assignment_sha256()?.as_str() != self.assignment_sha256.as_str()
        {
            return Err("Codex native MCP command identity is invalid");
        }
        Ok(())
    }

    pub fn validate_against(&self, command: &RuntimeCommand) -> Result<(), &'static str> {
        self.validate()?;
        if command.method != "native.mcp.observe"
            || self.operation_id != command.operation_id
            || self.binding_id != command.binding_id
            || self.binding_generation != command.generation
            || self.input_sha256.as_ref().map(Sha256Digest::as_str)
                != command.input_sha256.as_deref()
            || self.input_sha256.is_none()
        {
            return Err("Codex native MCP command differs from its authenticated RuntimeCommand");
        }
        Ok(())
    }

    /// Private RuntimeCommand effect envelope. The token, paths, and profile
    /// body are intentionally absent; the launch resolver consumes the exact
    /// issued references separately when it constructs `thread/start.config`.
    pub fn effect_identity(&self) -> Value {
        json!({
            "schema_version":3,
            "kind":"codex_native_mcp_effect",
            "observation_kind":"assigned_session",
            "operation_id":self.operation_id,
            "launch_operation_id":self.launch_operation_id,
            "assignment_sha256":self.assignment_sha256,
            "binding_id":self.binding_id,
            "binding_generation":self.binding_generation,
            "open_operation_id":self.open_operation_id,
            "native_root_id":self.native_root_id,
            "native_scope_key":self.native_scope_key,
            "expected_adapter_boot_id":self.expected_adapter_boot_id,
            "task_id":self.task_id,
            "task_revision":self.task_revision,
            "attempt_id":self.attempt_id,
            "participant_id":self.participant_id,
            "grant_revision":self.grant_revision,
            "participant_native_session_id":self.participant_native_session_id,
            "surface":self.surface,
            "server_name":self.server_name,
            "expected_authorization_revision":self.expected_authorization_revision,
            "expected_toolset_sha256":self.expected_toolset_sha256,
        })
    }

    pub fn computed_assignment_sha256(&self) -> Result<Sha256Digest, &'static str> {
        codex_value_sha256(&json!({
            "operation_id":self.operation_id,
            "launch_operation_id":self.launch_operation_id,
            "binding_id":self.binding_id,
            "binding_generation":self.binding_generation,
            "open_operation_id":self.open_operation_id,
            "native_root_id":self.native_root_id,
            "native_scope_key":self.native_scope_key,
            "expected_adapter_boot_id":self.expected_adapter_boot_id,
            "task_id":self.task_id,
            "task_revision":self.task_revision,
            "attempt_id":self.attempt_id,
            "participant_id":self.participant_id,
            "grant_revision":self.grant_revision,
            "participant_native_session_id":self.participant_native_session_id,
            "surface":self.surface,
            "expected_authorization_revision":self.expected_authorization_revision,
            "expected_toolset_sha256":self.expected_toolset_sha256,
        }))
    }
}

/// Sanitized output from the Codex app-server's per-thread MCP status and
/// actual read-only Participant-core calls. No tool schema bodies, credentials,
/// filesystem paths, or provider settings are retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexNativeMcpReadback {
    pub schema_version: u16,
    pub kind: String,
    pub operation_id: String,
    pub launch_operation_id: String,
    pub assignment_sha256: Sha256Digest,
    pub binding_id: String,
    pub binding_generation: i64,
    pub open_operation_id: String,
    pub native_root_id: String,
    pub native_scope_key: String,
    pub adapter_boot_id: String,
    pub reported_product: String,
    pub reported_version: String,
    pub effective_model_provider: String,
    pub effective_model: String,
    pub server_info_name: String,
    pub server_info_version: String,
    pub server_id_sha256: Sha256Digest,
    pub runtime_status: String,
    pub participant_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub grant_revision: i64,
    pub participant_native_session_id: Option<String>,
    pub surface: String,
    pub authorization_revision: Sha256Digest,
    pub toolset_sha256: Sha256Digest,
    pub tool_count: u32,
    pub observed_at_ms: i64,
    pub evidence_sha256: Sha256Digest,
}

impl CodexNativeMcpReadback {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 2
            || self.kind != CODEX_NATIVE_MCP_READBACK_KIND
            || !bounded_text(&self.operation_id, MAX_OPERATION_ID_BYTES)
            || !bounded_text(&self.launch_operation_id, MAX_OPERATION_ID_BYTES)
            || !is_lower_sha256(&self.assignment_sha256)
            || !bounded_text(&self.binding_id, MAX_BINDING_ID_BYTES)
            || self.binding_generation <= 0
            || !bounded_native_text(&self.native_root_id)
            || !bounded_native_text(&self.native_scope_key)
            || !bounded_text(&self.adapter_boot_id, MAX_OPERATION_ID_BYTES)
            || !bounded_native_text(&self.reported_product)
            || !bounded_native_text(&self.reported_version)
            || !bounded_native_text(&self.effective_model_provider)
            || !bounded_native_text(&self.effective_model)
            || self.server_info_name != CODEX_NATIVE_MCP_SERVER_INFO_NAME
            || !bounded_native_text(&self.server_info_version)
            || !is_lower_sha256(&self.server_id_sha256)
            || self.runtime_status != "connected"
            || !bounded_text(&self.participant_id, MAX_PARTICIPANT_ID_BYTES)
            || !bounded_text(&self.task_id, MAX_TASK_ID_BYTES)
            || self.task_revision <= 0
            || !bounded_text(&self.attempt_id, MAX_SESSION_ID_BYTES)
            || self.grant_revision <= 0
            || self
                .participant_native_session_id
                .as_deref()
                .is_some_and(|value| !bounded_native_text(value))
            || !bounded_text(&self.open_operation_id, MAX_OPERATION_ID_BYTES)
            || self.surface != "participant-core"
            || !is_lower_sha256(&self.authorization_revision)
            || !is_lower_sha256(&self.toolset_sha256)
            || self.tool_count == 0
            || self.observed_at_ms <= 0
            || !is_lower_sha256(&self.evidence_sha256)
            || self.computed_evidence_sha256()?.as_str() != self.evidence_sha256.as_str()
        {
            return Err("Codex native MCP readback identity is invalid");
        }
        Ok(())
    }

    pub fn computed_evidence_sha256(&self) -> Result<Sha256Digest, &'static str> {
        let mut value = serde_json::to_value(self)
            .map_err(|_| "Codex native MCP evidence serialization failed")?;
        value
            .as_object_mut()
            .ok_or("Codex native MCP evidence is not an object")?
            .remove("evidence_sha256");
        codex_value_sha256(&value)
    }
}

fn codex_value_sha256(value: &Value) -> Result<Sha256Digest, &'static str> {
    let ordered = ordered_json(value);
    let bytes =
        serde_json::to_vec(&ordered).map_err(|_| "Codex native MCP digest serialization failed")?;
    Sha256Digest::new(format!("{:x}", Sha256::digest(bytes)))
        .map_err(|_| "Codex native MCP digest is invalid")
}

fn opaque_assignment_ref(value: &str, prefix: &str) -> bool {
    let Some((actual, id)) = value.split_once(':') else {
        return false;
    };
    actual == prefix
        && id.len() == 36
        && id.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMcpPhase {
    Install,
    Observe,
    Arm,
    Read,
}

impl NativeMcpPhase {
    pub const fn method(self) -> &'static str {
        match self {
            Self::Install => NATIVE_MCP_INSTALL_METHOD,
            Self::Observe => NATIVE_MCP_OBSERVE_METHOD,
            Self::Arm => NATIVE_MCP_ARM_METHOD,
            Self::Read => NATIVE_MCP_READ_METHOD,
        }
    }

    const fn requires_prepared_command(self) -> bool {
        matches!(self, Self::Install | Self::Observe)
    }

    const fn requires_challenge(self) -> bool {
        matches!(self, Self::Arm | Self::Read)
    }
}

/// Selects one of the two read-only observation purposes. Installed-server
/// observation remains the C8 install readback; assigned-session observation
/// is the separate C7 assignment proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMcpObservationKind {
    InstalledServer,
    AssignedSession,
}

/// A reference to a private, already-authorized artifact. The path or secret
/// bytes are resolved only by the trusted worker boundary; the shared command
/// carries its reference and immutable content digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectedArtifactRef {
    pub protected_ref: ProtectedRef,
    pub sha256: Sha256Digest,
}

impl ProtectedArtifactRef {
    pub fn validate(&self) -> Result<(), &'static str> {
        if ProtectedRef::new(self.protected_ref.as_str().to_owned()).is_err()
            || !is_lower_sha256(&self.sha256)
        {
            return Err("native MCP protected artifact reference is invalid");
        }
        Ok(())
    }
}

/// Store-admitted native MCP work. `input_sha256` is the retained original
/// Operation digest, not a hash of this enriched command object; the adapter
/// binds it to the outer RuntimeCommand before reading either private artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMcpCommand {
    pub schema_version: u16,
    pub operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub input_sha256: Sha256Digest,
    pub assignment_sha256: Sha256Digest,
    pub native_session_id: String,
    pub service_id: String,
    pub service_version: String,
    pub service_pid: u32,
    pub location_sha256: Sha256Digest,
    pub phase: NativeMcpPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_kind: Option<NativeMcpObservationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "prepared")]
    pub prepared_command: Option<ProtectedArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub challenge: Option<ProtectedArtifactRef>,
}

impl NativeMcpCommand {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 2
            || !bounded_text(&self.operation_id, MAX_OPERATION_ID_BYTES)
            || !bounded_text(&self.binding_id, MAX_BINDING_ID_BYTES)
            || self.binding_generation <= 0
            || !is_lower_sha256(&self.input_sha256)
            || !is_lower_sha256(&self.assignment_sha256)
            || !bounded_text(&self.native_session_id, MAX_SESSION_ID_BYTES)
            || !self.native_session_id.starts_with("ses_")
            || !service_id(&self.service_id)
            || !service_version(&self.service_version)
            || self.service_pid == 0
            || !is_lower_sha256(&self.location_sha256)
            || matches!(self.phase, NativeMcpPhase::Observe) != self.observation_kind.is_some()
        {
            return Err("native MCP command identity is invalid");
        }

        if self.phase.requires_prepared_command() != self.prepared_command.is_some()
            || self.phase.requires_challenge() != self.challenge.is_some()
            || (self.prepared_command.is_some() && self.challenge.is_some())
        {
            return Err("native MCP command artifact references do not match its phase");
        }
        if let Some(reference) = &self.prepared_command {
            reference.validate()?;
        }
        if let Some(reference) = &self.challenge {
            reference.validate()?;
        }
        Ok(())
    }

    /// Bind the enriched DTO to the authenticated generic command before any
    /// private artifact is opened or native effect is attempted.
    pub fn validate_against(&self, command: &RuntimeCommand) -> Result<(), &'static str> {
        self.validate()?;
        if self.phase.method() != command.method.as_str()
            || self.operation_id.as_str() != command.operation_id.as_str()
            || self.binding_id.as_str() != command.binding_id.as_str()
            || self.binding_generation != command.generation
            || command.input_sha256.as_deref() != Some(self.input_sha256.as_str())
        {
            return Err("native MCP command differs from its authenticated RuntimeCommand");
        }
        Ok(())
    }
}

/// Sanitized, assignment-bound OpenCode MCP observation returned by the
/// standalone adapter. Raw server errors, integration IDs, and API payloads
/// are intentionally absent from this shared receipt contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMcpAssignmentReadback {
    pub schema_version: u16,
    pub kind: String,
    pub assignment_sha256: Sha256Digest,
    pub service_id: String,
    pub service_pid: u32,
    pub service_version: String,
    pub location_sha256: Sha256Digest,
    pub native_session: NativeMcpSessionObservation,
    pub mcp_servers: Vec<NativeMcpServerObservation>,
    pub observed_at_ms: i64,
}

impl NativeMcpAssignmentReadback {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.kind != NATIVE_MCP_ASSIGNMENT_READBACK_KIND
            || !is_lower_sha256(&self.assignment_sha256)
            || !service_id(&self.service_id)
            || self.service_pid == 0
            || !service_version(&self.service_version)
            || !is_lower_sha256(&self.location_sha256)
            || self.observed_at_ms <= 0
            || self.mcp_servers.len() > MAX_NATIVE_MCP_SERVERS
        {
            return Err("native MCP assignment readback identity is invalid");
        }

        self.native_session.validate()?;
        let mut names = BTreeSet::new();
        for server in &self.mcp_servers {
            server.validate()?;
            if !names.insert(server.name.as_str()) {
                return Err("native MCP assignment readback has duplicate server names");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMcpSessionObservation {
    pub id: String,
    pub project_id: String,
    pub parent_id: Option<String>,
    pub agent: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl NativeMcpSessionObservation {
    fn validate(&self) -> Result<(), &'static str> {
        if !native_identifier(&self.id, "ses_")
            || !bounded_text(&self.project_id, MAX_NATIVE_TEXT_BYTES)
            || self
                .project_id
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
            || self
                .parent_id
                .as_deref()
                .is_some_and(|value| !native_identifier(value, "ses_"))
            || self
                .agent
                .as_deref()
                .is_some_and(|value| !bounded_native_text(value))
            || self.created_at_ms == 0
            || self.updated_at_ms < self.created_at_ms
        {
            return Err("native MCP session observation is invalid");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMcpServerStatus {
    Connected,
    Pending,
    Disabled,
    Failed,
    NeedsAuth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMcpServerObservation {
    pub name: String,
    pub status: NativeMcpServerStatus,
    pub integration_id_sha256: Option<Sha256Digest>,
    pub error_present: bool,
}

impl NativeMcpServerObservation {
    fn validate(&self) -> Result<(), &'static str> {
        let status_requires_error = matches!(
            self.status,
            NativeMcpServerStatus::Failed | NativeMcpServerStatus::NeedsAuth
        );
        if !bounded_native_text(&self.name)
            || self
                .integration_id_sha256
                .as_ref()
                .is_some_and(|digest| !is_lower_sha256(digest))
            || self.error_present != status_requires_error
        {
            return Err("native MCP server observation is invalid");
        }
        Ok(())
    }
}

fn bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn native_identifier(value: &str, prefix: &str) -> bool {
    bounded_native_text(value)
        && value.len() > prefix.len()
        && !value.bytes().any(|byte| byte.is_ascii_whitespace())
        && value.starts_with(prefix)
        && value.len() > prefix.len()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn bounded_native_text(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_NATIVE_TEXT_BYTES
        && !value.chars().any(char::is_control)
}

fn service_id(value: &str) -> bool {
    bounded_text(value, MAX_SERVICE_ID_BYTES)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn service_version(value: &str) -> bool {
    bounded_text(value, MAX_SERVICE_VERSION_BYTES)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+_-".contains(&byte))
}

fn is_lower_sha256(value: &Sha256Digest) -> bool {
    Sha256Digest::new(value.as_str().to_owned())
        .is_ok_and(|normalized| normalized.as_str() == value.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn digest() -> Sha256Digest {
        Sha256Digest::new("a".repeat(64)).unwrap()
    }

    fn protected_ref() -> ProtectedArtifactRef {
        ProtectedArtifactRef {
            protected_ref: ProtectedRef::new("store://native-mcp/test/prepared").unwrap(),
            sha256: digest(),
        }
    }

    fn command(phase: NativeMcpPhase) -> NativeMcpCommand {
        NativeMcpCommand {
            schema_version: 2,
            operation_id: "op_123".into(),
            binding_id: "binding_123".into(),
            binding_generation: 1,
            input_sha256: digest(),
            assignment_sha256: digest(),
            native_session_id: "ses_123".into(),
            service_id: "opencode-main".into(),
            service_version: "2.0.7".into(),
            service_pid: 42,
            location_sha256: digest(),
            phase,
            observation_kind: (phase == NativeMcpPhase::Observe)
                .then_some(NativeMcpObservationKind::AssignedSession),
            prepared_command: phase.requires_prepared_command().then(protected_ref),
            challenge: phase.requires_challenge().then(protected_ref),
        }
    }

    fn valid_readback() -> NativeMcpAssignmentReadback {
        NativeMcpAssignmentReadback {
            schema_version: 1,
            kind: NATIVE_MCP_ASSIGNMENT_READBACK_KIND.into(),
            assignment_sha256: digest(),
            service_id: "opencode-main".into(),
            service_pid: 42,
            service_version: PINNED_OPENCODE_VERSION.into(),
            location_sha256: digest(),
            native_session: NativeMcpSessionObservation {
                id: "ses_123".into(),
                project_id: "prj_123".into(),
                parent_id: Some("ses_parent".into()),
                agent: Some("build".into()),
                created_at_ms: 10,
                updated_at_ms: 11,
            },
            mcp_servers: vec![NativeMcpServerObservation {
                name: "docs".into(),
                status: NativeMcpServerStatus::Connected,
                integration_id_sha256: Some(digest()),
                error_present: false,
            }],
            observed_at_ms: 12,
        }
    }

    #[test]
    fn command_v2_requires_an_observation_kind_only_for_observe() {
        assert_eq!(NATIVE_MCP_COMMAND_SCHEMA_VERSION, "2");
        assert!(command(NativeMcpPhase::Observe).validate().is_ok());
        assert!(command(NativeMcpPhase::Install).validate().is_ok());

        let mut missing = command(NativeMcpPhase::Observe);
        missing.observation_kind = None;
        assert!(missing.validate().is_err());

        let mut unexpected = command(NativeMcpPhase::Install);
        unexpected.observation_kind = Some(NativeMcpObservationKind::InstalledServer);
        assert!(unexpected.validate().is_err());

        let mut v1 = command(NativeMcpPhase::Observe);
        v1.schema_version = 1;
        assert!(v1.validate().is_err());
    }

    #[test]
    fn command_observation_kind_uses_closed_snake_case_values() {
        let value = serde_json::to_value(command(NativeMcpPhase::Observe)).unwrap();
        assert_eq!(value["observation_kind"], "assigned_session");
        let mut unknown = value;
        unknown["observation_kind"] = json!("arbitrary");
        assert!(serde_json::from_value::<NativeMcpCommand>(unknown).is_err());
    }

    #[test]
    fn assignment_readback_accepts_only_bounded_exact_sanitized_facts() {
        let readback = valid_readback();
        assert!(readback.validate().is_ok());

        // This DTO validates sanitized facts. The adapter/Store bind the
        // observed version to the exact live native instance, not one release.
        let mut other_version = readback.clone();
        other_version.service_version = "2.0.8".into();
        assert!(other_version.validate().is_ok());
        for invalid_version in ["", "2.0.8\n", "version with spaces"] {
            other_version.service_version = invalid_version.into();
            assert!(other_version.validate().is_err());
        }

        let mut wrong_session = readback.clone();
        wrong_session.native_session.id = "ses_../other".into();
        assert!(wrong_session.validate().is_err());

        let mut opaque_project = readback.clone();
        opaque_project.native_session.project_id = "native-project-without-prefix".into();
        assert!(opaque_project.validate().is_ok());

        let mut inconsistent_error = readback;
        inconsistent_error.mcp_servers[0].status = NativeMcpServerStatus::Failed;
        assert!(inconsistent_error.validate().is_err());
    }

    #[test]
    fn assignment_readback_rejects_duplicate_servers_and_unknown_payload_fields() {
        let mut duplicate = valid_readback();
        duplicate.mcp_servers.push(duplicate.mcp_servers[0].clone());
        assert!(duplicate.validate().is_err());

        let mut invalid_time = valid_readback();
        invalid_time.observed_at_ms = 0;
        assert!(invalid_time.validate().is_err());

        let mut invalid_pid = valid_readback();
        invalid_pid.service_pid = 0;
        assert!(invalid_pid.validate().is_err());

        let mut invalid_digest = valid_readback();
        invalid_digest.mcp_servers[0].integration_id_sha256 =
            Some(serde_json::from_value(json!("A".repeat(64))).unwrap());
        assert!(invalid_digest.validate().is_err());

        let mut value = serde_json::to_value(valid_readback()).unwrap();
        value["raw_error"] = json!("must not pass");
        assert!(serde_json::from_value::<NativeMcpAssignmentReadback>(value).is_err());
    }

    #[test]
    fn codex_launch_config_is_thread_scoped_and_leaves_native_readiness_defaulted() {
        let root = std::env::temp_dir();
        let profile = CodexNativeMcpLaunchProfile::new(
            "launch-config-test".into(),
            "task-config-test".into(),
            1,
            "attempt-config-test".into(),
            "binding-config-test".into(),
            1,
            "participant-config-test".into(),
            1,
            "participant-credential:00000000-0000-4000-8000-000000000001".into(),
            "participant-mcp-config:00000000-0000-4000-8000-000000000002".into(),
            "work-participant".into(),
            "participant-core".into(),
            root.join(CODEX_NATIVE_MCP_EXECUTABLE_NAME)
                .to_string_lossy()
                .into_owned(),
            root.join("codex-c8-credential.json")
                .to_string_lossy()
                .into_owned(),
            root.join("codex-c8-profile.toml")
                .to_string_lossy()
                .into_owned(),
        )
        .expect("valid launch-scoped Participant profile");
        let config = codex_native_mcp_thread_config(&profile)
            .expect("native thread config overrides validate");
        let prefix = format!("mcp_servers.{}.", profile.server_name);
        let command_key = format!("{prefix}command");
        let enabled_key = format!("{prefix}enabled");
        let required_key = format!("{prefix}required");
        let args_key = format!("{prefix}args");
        let startup_readiness_key = format!("{prefix}startup_readiness");

        assert_eq!(
            config.get(command_key.as_str()).and_then(Value::as_str),
            Some(profile.executable.as_str())
        );
        assert_eq!(
            config.get(enabled_key.as_str()).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            config.get(required_key.as_str()).and_then(Value::as_bool),
            Some(true)
        );
        let args = config
            .get(args_key.as_str())
            .and_then(Value::as_array)
            .unwrap();
        assert_eq!(args.first().and_then(Value::as_str), Some("--config"));
        assert_eq!(args.get(2).and_then(Value::as_str), Some("--credential"));
        assert_eq!(args.get(4).and_then(Value::as_str), Some("--profile"));
        assert!(config.get(startup_readiness_key.as_str()).is_none());
        assert!(config.get("model").is_none());
        assert!(config.get("model_provider").is_none());
    }
}
