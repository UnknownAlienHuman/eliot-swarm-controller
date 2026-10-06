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

pub const NATIVE_MCP_COMMAND_SCHEMA_ID: &str = "swarm.native_mcp_command";
pub const NATIVE_MCP_COMMAND_SCHEMA_VERSION: &str = "1";

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
    pub prepared_command: Option<ProtectedArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub challenge: Option<ProtectedArtifactRef>,
}

impl NativeMcpCommand {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
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

fn bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
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
