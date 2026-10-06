//! Binding-scoped native MCP observations.
//!
//! A controller assignment is not evidence that OpenCode loaded the assigned
//! tool surface. This module keeps Store assignment facts separate from facts
//! the selected native API can actually return.

use crate::{
    config::McpToolProfile,
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const READBACK_SCHEMA_VERSION: u32 = 1;
const OPENCODE_VERSION: &str = "2.0.7";
const OPENCODE_MCP_API_CONTRACT: &str = "@opencode/protocol 2.0.7 mcp.list";
const MAX_SERVER_FACTS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParticipationBasis {
    AttemptOwner,
    ProducerRef { assignment_id: String },
    SponsoredReviewer { review_assignment_id: String },
}

/// Exact assignment context supplied by the trusted Store read path. The
/// runtime adapter must not infer these values from OpenCode configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssignmentContext {
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
    native_session_id: String,
    participant_id: String,
    profile: McpToolProfile,
    grant_revision: i64,
    basis: ParticipationBasis,
}

/// Trusted Store facts used to mint an adapter scope. This type is not a
/// request schema and is only assembled after Store authorization.
pub(crate) struct AssignmentSeed {
    pub(crate) task_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: String,
    pub(crate) binding_id: String,
    pub(crate) binding_generation: i64,
    pub(crate) native_session_id: String,
    pub(crate) participant_id: String,
    pub(crate) profile: McpToolProfile,
    pub(crate) grant_revision: i64,
    pub(crate) basis_kind: String,
    pub(crate) assignment_id: Option<String>,
    pub(crate) review_assignment_id: Option<String>,
}

impl AssignmentContext {
    pub(crate) fn new(seed: AssignmentSeed) -> Result<Self> {
        let AssignmentSeed {
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            native_session_id,
            participant_id,
            profile,
            grant_revision,
            basis_kind,
            assignment_id,
            review_assignment_id,
        } = seed;
        for (field, value, limit) in [
            ("task_id", task_id.as_str(), 256),
            ("attempt_id", attempt_id.as_str(), 256),
            ("binding_id", binding_id.as_str(), 256),
            ("native_session_id", native_session_id.as_str(), 256),
            ("participant_id", participant_id.as_str(), 256),
        ] {
            validate_identifier(value, field, limit)?;
        }
        if task_revision <= 0 || binding_generation <= 0 || grant_revision <= 0 {
            return Err(Error::invalid(
                "native MCP assignment revisions and generations must be positive",
            ));
        }
        if !native_session_id.starts_with("ses_") {
            return Err(Error::invalid(
                "native MCP assignment requires one exact OpenCode session ID",
            ));
        }

        let basis = match basis_kind.as_str() {
            "attempt_owner"
                if profile == McpToolProfile::Participant
                    && assignment_id.is_none()
                    && review_assignment_id.is_none() =>
            {
                ParticipationBasis::AttemptOwner
            }
            "producer_ref"
                if profile == McpToolProfile::Participant && review_assignment_id.is_none() =>
            {
                let assignment_id = assignment_id.ok_or_else(|| {
                    Error::invalid("producer_ref scope requires its exact assignment ID")
                })?;
                validate_identifier(&assignment_id, "assignment_id", 128)?;
                ParticipationBasis::ProducerRef { assignment_id }
            }
            "sponsored_reviewer"
                if profile == McpToolProfile::AssignedReviewer && assignment_id.is_none() =>
            {
                let review_assignment_id = review_assignment_id.ok_or_else(|| {
                    Error::invalid("reviewer scope requires its exact review assignment ID")
                })?;
                validate_identifier(&review_assignment_id, "review_assignment_id", 128)?;
                ParticipationBasis::SponsoredReviewer {
                    review_assignment_id,
                }
            }
            _ => {
                return Err(Error::invalid(
                    "native MCP scope must match an attempt-owner, producer, or assigned-reviewer profile",
                ));
            }
        };

        Ok(Self {
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            native_session_id,
            participant_id,
            profile,
            grant_revision,
            basis,
        })
    }

    pub(crate) fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub(crate) fn binding_generation(&self) -> i64 {
        self.binding_generation
    }

    pub(crate) fn native_session_id(&self) -> &str {
        &self.native_session_id
    }

    pub(crate) fn participant_id(&self) -> &str {
        &self.participant_id
    }

    pub(crate) fn basis_kind(&self) -> &'static str {
        match self.basis {
            ParticipationBasis::AttemptOwner => "attempt_owner",
            ParticipationBasis::ProducerRef { .. } => "producer_ref",
            ParticipationBasis::SponsoredReviewer { .. } => "sponsored_reviewer",
        }
    }

    pub(crate) fn assignment_id(&self) -> Option<&str> {
        match &self.basis {
            ParticipationBasis::ProducerRef { assignment_id } => Some(assignment_id),
            _ => None,
        }
    }

    pub(crate) fn review_assignment_id(&self) -> Option<&str> {
        match &self.basis {
            ParticipationBasis::SponsoredReviewer {
                review_assignment_id,
            } => Some(review_assignment_id),
            _ => None,
        }
    }

    pub(crate) fn as_value(&self) -> Value {
        json!({
            "task_id":self.task_id,
            "task_revision":self.task_revision,
            "attempt_id":self.attempt_id,
            "binding_id":self.binding_id,
            "binding_generation":self.binding_generation,
            "native_session_id":self.native_session_id,
            "participant_id":self.participant_id,
            "mcp_profile":profile_name(self.profile),
            "grant_revision":self.grant_revision,
            "participation_basis":self.basis_kind(),
            "assignment_id":self.assignment_id(),
            "review_assignment_id":self.review_assignment_id(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpServerStatus {
    Connected,
    Pending,
    Disabled,
    Failed,
    NeedsAuth,
}

impl McpServerStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Pending => "pending",
            Self::Disabled => "disabled",
            Self::Failed => "failed",
            Self::NeedsAuth => "needs_auth",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct McpServerFact {
    name: String,
    status: McpServerStatus,
    integration_id_sha256: Option<String>,
    error_present: bool,
}

impl McpServerFact {
    pub(crate) fn new(
        name: String,
        status: McpServerStatus,
        integration_id: Option<String>,
        error_present: bool,
    ) -> Result<Self> {
        validate_text(&name, "MCP server name", 256)?;
        let integration_id_sha256 = integration_id
            .map(|value| -> Result<String> {
                validate_text(&value, "MCP integration ID", 256)?;
                Ok(format!("sha256:{}", model::digest(value.as_bytes())))
            })
            .transpose()?;
        let status_requires_error =
            matches!(status, McpServerStatus::Failed | McpServerStatus::NeedsAuth);
        if error_present != status_requires_error {
            return Err(Error::invalid(
                "MCP server error presence must match the native status schema",
            ));
        }
        Ok(Self {
            name,
            status,
            integration_id_sha256,
            error_present,
        })
    }

    fn as_value(&self) -> Value {
        json!({
            "name":self.name,
            "status":self.status.as_str(),
            "integration_id_sha256":self.integration_id_sha256,
            "error_present":self.error_present,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeSessionFact {
    session_id: String,
    project_id: String,
    parent_session_id: Option<String>,
    agent: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl NativeSessionFact {
    pub(crate) fn new(
        session_id: String,
        project_id: String,
        parent_session_id: Option<String>,
        agent: Option<String>,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<Self> {
        validate_identifier(&session_id, "native_session_id", 256)?;
        if !session_id.starts_with("ses_") {
            return Err(Error::invalid("native session ID must use the ses_ prefix"));
        }
        validate_identifier(&project_id, "native project ID", 256)?;
        if let Some(parent) = parent_session_id.as_deref() {
            validate_identifier(parent, "native parent session ID", 256)?;
            if !parent.starts_with("ses_") {
                return Err(Error::invalid("native parent session ID must use ses_"));
            }
        }
        if let Some(agent) = agent.as_deref() {
            validate_text(agent, "native session agent", 256)?;
        }
        if created_at_ms == 0 || updated_at_ms < created_at_ms {
            return Err(Error::invalid("native session timestamps are invalid"));
        }
        Ok(Self {
            session_id,
            project_id,
            parent_session_id,
            agent,
            created_at_ms,
            updated_at_ms,
        })
    }

    fn as_value(&self) -> Value {
        json!({
            "id":self.session_id,
            "project_id":self.project_id,
            "parent_session_id":self.parent_session_id,
            "agent":self.agent,
            "created_at_ms":self.created_at_ms,
            "updated_at_ms":self.updated_at_ms,
            "evidence":"exact_session_readback",
        })
    }
}

/// Immutable, typed result of the OpenCode HTTP adapter read. It is not
/// deserializable from a tool request and always marks tool loading unknown.
#[derive(Debug, Clone)]
pub(crate) struct NativeMcpReadback {
    scope: AssignmentContext,
    payload: Value,
    evidence_digest: String,
}

/// Bounded facts captured by the trusted OpenCode V2 adapter before the Store
/// validates and records the assignment-scoped readback.
pub(crate) struct OpenCodeV2ReadbackInput {
    pub(crate) service_id: String,
    pub(crate) service_pid: u32,
    pub(crate) service_version: String,
    pub(crate) directory_sha256: String,
    pub(crate) session: NativeSessionFact,
    pub(crate) servers: Vec<McpServerFact>,
    pub(crate) observed_at_ms: i64,
}

impl NativeMcpReadback {
    pub(crate) fn from_opencode_v2(
        scope: AssignmentContext,
        input: OpenCodeV2ReadbackInput,
    ) -> Result<Self> {
        let OpenCodeV2ReadbackInput {
            service_id,
            service_pid,
            service_version,
            directory_sha256,
            session,
            mut servers,
            observed_at_ms,
        } = input;
        validate_identifier(&service_id, "OpenCode service ID", 128)?;
        validate_text(&service_version, "OpenCode service version", 256)?;
        if service_version != OPENCODE_VERSION {
            return Err(Error::new(
                "NATIVE_MCP_VERSION_UNSUPPORTED",
                "native MCP readback is pinned to OpenCode 2.0.7",
            ));
        }
        if service_pid == 0 || observed_at_ms <= 0 {
            return Err(Error::invalid(
                "OpenCode service identity and observation time are required",
            ));
        }
        if !directory_sha256
            .strip_prefix("sha256:")
            .is_some_and(is_lowercase_sha256)
        {
            return Err(Error::invalid(
                "OpenCode directory identity must be a SHA-256 digest",
            ));
        }
        if session.session_id != scope.native_session_id {
            return Err(Error::new(
                "NATIVE_MCP_SCOPE_MISMATCH",
                "native session readback differs from the exact assigned session",
            ));
        }
        if servers.len() > MAX_SERVER_FACTS {
            return Err(Error::new(
                "NATIVE_MCP_RESPONSE_LIMIT",
                "native MCP server inventory exceeds the supported page boundary",
            ));
        }
        servers.sort_by(|left, right| left.name.cmp(&right.name));
        let mut names = BTreeSet::new();
        if servers
            .iter()
            .any(|server| !names.insert(server.name.as_str()))
        {
            return Err(Error::new(
                "NATIVE_MCP_SCHEMA",
                "native MCP server inventory contains duplicate names",
            ));
        }

        let payload = json!({
            "schema_version":READBACK_SCHEMA_VERSION,
            "source":{
                "runtime":"opencode_v2",
                "api_contract":OPENCODE_MCP_API_CONTRACT,
                "api_method":"GET /api/mcp",
                "api_scope":"configured_server_connection_status_only",
                "service_id":service_id,
                "service_identity_basis":"explicit_route_id_and_verified_connection_pid_version",
                "service_pid":service_pid,
                "service_version":service_version,
                "directory_sha256":directory_sha256,
            },
            "assignment":scope.as_value(),
            "native_session":session.as_value(),
            "mcp_servers":servers.iter().map(McpServerFact::as_value).collect::<Vec<_>>(),
            "binding_session_mapping":{
                "status":"unknown",
                "reason":"the OpenCode HTTP API does not expose a Swarm binding-to-session mapping",
            },
            "loaded_tool_set":{
                "status":"unknown",
                "items":Value::Null,
                "digest":Value::Null,
                "reason":"the pinned OpenCode HTTP MCP API exposes server status, not tools/list inventory or model-context load",
            },
            "model_context_loaded":"unknown",
            "readiness":"incomplete",
            "dispatch_permitted":false,
            "gaps":[
                "native_binding_to_session_mapping_unavailable",
                "native_loaded_tool_set_unavailable",
                "native_model_context_load_unavailable",
            ],
            "observed_at_ms":observed_at_ms,
        });
        let evidence_digest = digest_value(&payload)?;
        Ok(Self {
            scope,
            payload,
            evidence_digest,
        })
    }

    pub(crate) fn scope(&self) -> &AssignmentContext {
        &self.scope
    }

    pub(crate) fn payload(&self) -> Value {
        let mut payload = self.payload.clone();
        payload["evidence_digest"] = json!(format!("sha256:{}", self.evidence_digest));
        payload
    }

    pub(crate) fn verify_digest(&self) -> Result<()> {
        if digest_value(&self.payload)? != self.evidence_digest {
            return Err(Error::new(
                "NATIVE_MCP_DIGEST_MISMATCH",
                "native MCP readback digest does not match its exact observed facts",
            ));
        }
        Ok(())
    }
}

fn profile_name(profile: McpToolProfile) -> &'static str {
    match profile {
        McpToolProfile::Participant => "participant",
        McpToolProfile::AssignedReviewer => "assigned_reviewer",
        _ => "unsupported",
    }
}

fn validate_identifier(value: &str, field: &str, max: usize) -> Result<()> {
    validate_text(value, field, max)?;
    if value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(Error::invalid(format!("{field} cannot contain whitespace")));
    }
    Ok(())
}

fn validate_text(value: &str, field: &str, max: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > max
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(format!(
            "{field} must be bounded nonempty text"
        )));
    }
    Ok(())
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_value(value: &Value) -> Result<String> {
    Ok(model::digest(model::canonical(value)?.as_bytes()))
}
