//! Closed DTOs shared by the hook setup CLI, Store ingress and local wrapper.

use crate::{
    error::{Error, Result},
    model::{self, Credential},
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const EVENT_NAME: &str = "git.post_commit";
pub const EVENT_PHASE: &str = "post_commit";
pub const SOURCE_SCHEMA_VERSION: u32 = 1;
pub const FACT_SCHEMA_VERSION: u32 = 1;
pub const MAX_SOURCE_EVENTS_PAGE: usize = 64;
pub const MAX_EVENT_KEY_BYTES: usize = 512;
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 48 * 1024;

/// One supported event is intentionally a closed contract, not a caller DSL.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookSetupRequest {
    pub client_request_id: String,
    pub project_id: String,
    pub source_id: String,
    pub credential: Credential,
}

impl HookSetupRequest {
    pub fn parse(value: &serde_json::Value) -> Result<Self> {
        model::fields(
            value,
            &["client_request_id", "project_id", "source_id", "credential"],
        )?;
        let client_request_id = model::text(value, "client_request_id")?.to_owned();
        let project_id = model::text(value, "project_id")?.to_owned();
        let source_id = model::text(value, "source_id")?.to_owned();
        let credential_value = value
            .get("credential")
            .ok_or_else(|| Error::invalid("credential is required"))?;
        model::fields(credential_value, &["client_id", "token"])?;
        let credential: Credential = serde_json::from_value(credential_value.clone())
            .map_err(|_| Error::invalid("hook source credential is invalid"))?;
        if client_request_id.len() > 128
            || project_id.len() > 128
            || client_request_id.chars().any(char::is_control)
            || project_id.chars().any(char::is_control)
            || !is_canonical_v4_uuid(&source_id)
            || credential.client_id != format!("hook-source:{source_id}")
            || !valid_hook_token(&credential.token)
        {
            return Err(Error::invalid(
                "invalid hook setup request identity or credential",
            ));
        }
        Ok(Self {
            client_request_id,
            project_id,
            source_id,
            credential,
        })
    }
}

/// Setup returns public metadata only. The caller generated and persisted the
/// transport credential before IPC, so a lost RPC reply cannot lose the only
/// copy of a committed credential.
pub struct HookSetupResponse {
    pub source: HookSourceRecord,
}

impl HookSetupResponse {
    pub fn source_value(&self) -> serde_json::Value {
        serde_json::json!({
            "source_id":self.source.source_id,
            "project_id":self.source.project_id,
            "event":EVENT_NAME,
            "phase":EVENT_PHASE,
            "veto":"none_after_commit",
            "canonical_repository":self.source.canonical_repository,
            "registration_id":self.source.registration_id,
            "registration_generation":self.source.registration_generation,
            "revision":self.source.revision,
            "enabled":self.source.revoked_at_ms.is_none()
        })
    }

    /// Compatibility name for Store wiring that projects public metadata.
    pub fn public_value(&self) -> serde_json::Value {
        self.source_value()
    }
}

/// The immutable, bounded fact emitted by the repository-local post-commit
/// wrapper. Its shape is deliberately closed so event producers cannot choose
/// project, repository, or automation policy fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookCommitFact {
    pub schema_version: u32,
    pub event: String,
    pub source_id: String,
    pub project_id: String,
    pub canonical_repository: String,
    pub registration_id: String,
    pub registration_generation: i64,
    pub commit_oid: String,
    pub readback_verified: bool,
}

impl HookCommitFact {
    pub fn parse(value: &serde_json::Value) -> Result<Self> {
        let fact: Self = serde_json::from_value(value.clone()).map_err(|_| {
            Error::new(
                "HOOK_EVENT_RECORD_INVALID",
                "retained post-commit fact fields are invalid",
            )
        })?;
        if fact.schema_version != FACT_SCHEMA_VERSION
            || fact.event != EVENT_NAME
            || !is_canonical_v4_uuid(&fact.source_id)
            || fact.project_id.trim().is_empty()
            || fact.project_id.len() > 128
            || fact.project_id.chars().any(char::is_control)
            || !is_canonical_repository(&fact.canonical_repository)
            || fact.registration_id.trim().is_empty()
            || fact.registration_id.len() > 128
            || fact.registration_id.chars().any(char::is_control)
            || fact.registration_generation <= 0
            || !crate::forge::valid_object_id(&fact.commit_oid)
            || !fact.readback_verified
        {
            return Err(Error::new(
                "HOOK_EVENT_RECORD_INVALID",
                "retained post-commit fact is outside the verified contract",
            ));
        }
        Ok(fact)
    }
}

fn is_canonical_repository(value: &str) -> bool {
    crate::forge::canonical_repository(value).is_ok_and(|canonical| canonical == value)
}

/// Hook source identities and credentials use canonical lower-case UUIDv4
/// components so source IDs, client IDs and private token files have one
/// stable spelling.
pub(crate) fn is_canonical_v4_uuid(value: &str) -> bool {
    let Ok(uuid) = uuid::Uuid::parse_str(value) else {
        return false;
    };
    uuid.hyphenated().to_string() == value
        && uuid.as_bytes()[6] >> 4 == 4
        && uuid.as_bytes()[8] & 0xc0 == 0x80
}

pub(crate) fn valid_hook_token(value: &str) -> bool {
    value.is_ascii()
        && value.len() == 72
        && is_canonical_v4_uuid(&value[..36])
        && is_canonical_v4_uuid(&value[36..])
}

pub(crate) fn validate_hook_credential(source_id: &str, credential: &Credential) -> Result<()> {
    if !is_canonical_v4_uuid(source_id)
        || credential.client_id != format!("hook-source:{source_id}")
        || !valid_hook_token(&credential.token)
    {
        return Err(Error::new(
            "HOOK_CREDENTIAL_SCOPE_INVALID",
            "hook source credential does not match its canonical source identity",
        ));
    }
    Ok(())
}

/// The retained Store identity. Paths and credentials are deliberately absent:
/// repository paths are re-derived from the active workspace registration, and
/// only the normal `client:*` record retains a hash of the HookSource secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookSourceRecord {
    pub schema_version: u32,
    pub source_id: String,
    pub client_id: String,
    pub project_id: String,
    pub event: String,
    pub registration_id: String,
    pub registration_generation: i64,
    pub registration_digest: String,
    pub canonical_repository: String,
    /// Digest of the caller's stable setup request ID. This supports safe
    /// collision detection without retaining a transport secret or raw request.
    pub setup_request_digest: String,
    pub created_by: String,
    pub created_at_ms: i64,
    pub revision: i64,
    pub revoked_at_ms: Option<i64>,
    pub last_observation_id: Option<i64>,
}

impl HookSourceRecord {
    /// Safe source metadata for RPC and readback. This projection contains no
    /// HookSource token or credential hash.
    pub fn public_value(&self) -> serde_json::Value {
        serde_json::json!({
            "source_id":self.source_id,
            "client_id":self.client_id,
            "project_id":self.project_id,
            "event":self.event,
            "phase":EVENT_PHASE,
            "veto":"none_after_commit",
            "canonical_repository":self.canonical_repository,
            "registration_id":self.registration_id,
            "registration_generation":self.registration_generation,
            "revision":self.revision,
            "enabled":self.revoked_at_ms.is_none(),
            "created_by":self.created_by,
            "created_at_ms":self.created_at_ms,
            "revoked_at_ms":self.revoked_at_ms,
            "last_observation_id":self.last_observation_id
        })
    }
}

/// Server-derived scope passed from the Store to the bounded Git readback
/// phase. No path or project field is accepted from the HookSource request.
#[derive(Debug, Clone)]
pub struct EmitScope {
    pub source: HookSourceRecord,
    pub repository_path: PathBuf,
    pub git_executable: PathBuf,
}

/// Exact OID captured by the wrapper at post-commit time. The Store verifies
/// that this object is a commit in the setup-bound local repository before it
/// commits the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitSnapshot {
    pub commit_oid: String,
}
