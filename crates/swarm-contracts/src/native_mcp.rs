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
use std::collections::BTreeSet;

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
const MAX_SERVICE_ID_BYTES: usize = 128;
const MAX_SERVICE_VERSION_BYTES: usize = 128;
const MAX_NATIVE_TEXT_BYTES: usize = 256;
const MAX_NATIVE_MCP_SERVERS: usize = 256;
const PINNED_OPENCODE_VERSION: &str = "2.0.7";

pub const NATIVE_MCP_ASSIGNMENT_READBACK_KIND: &str = "native_mcp_assignment_readback";

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
            || self.service_version != PINNED_OPENCODE_VERSION
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

        let mut wrong_version = readback.clone();
        wrong_version.service_version = "2.0.8".into();
        assert!(wrong_version.validate().is_err());

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
}
