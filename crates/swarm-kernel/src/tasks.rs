//! Provider-neutral Task policy and transition validation.
//!
//! The Store remains the authority for SQLite reads and writes, operation
//! receipts, authorization and dependency/acceptance queries.  This module
//! owns only the deterministic rules that can be evaluated from a Task value
//! and bounded transition facts.  It deliberately has no controller, adapter,
//! database or process dependency.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// A bounded validation failure that the Store maps to its existing error
/// envelope.  Keeping the code and message here preserves the public method
/// contract while allowing the policy to be consumed by another Store facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    code: &'static str,
    message: String,
}

impl ValidationError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_PARAMS", message)
    }

    /// The existing root error code for this policy decision.
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// The existing bounded human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

pub type Result<T> = std::result::Result<T, ValidationError>;

/// Validate the provider-neutral portion of a serialized TaskSpec.
///
/// Serde still owns the closed DTO shape and the root acceptance module still
/// validates its nested acceptance policy.  These are the Task invariants
/// which do not require either of those root concerns: nonempty objective and
/// phase, unique requirements/dependencies, source references, source-index
/// pinning and the baseline reference bound.
pub fn validate_spec(spec: &Value) -> Result<()> {
    let object = spec
        .as_object()
        .ok_or_else(|| ValidationError::invalid("spec must be an object"))?;

    if let Some(policy_id) = object.get("owner_policy_id")
        && !policy_id.is_null()
        && policy_id
            .as_str()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ValidationError::invalid("owner_policy_id cannot be empty"));
    }

    if let Some(reference) = object.get("baseline_candidate_ref")
        && !reference.is_null()
        && reference.as_str().is_none_or(|value| {
            value.trim().is_empty() || value.len() > 512 || value.contains('\0')
        })
    {
        return Err(ValidationError::invalid(
            "baseline_candidate_ref must be a nonempty artifact reference of at most 512 bytes",
        ));
    }

    let objective = object.get("objective").and_then(Value::as_str);
    let phase = object.get("phase").and_then(Value::as_str);
    let requirements = object.get("requirements").and_then(Value::as_array);
    if objective.is_none_or(|value| value.trim().is_empty())
        || phase.is_none_or(|value| value.trim().is_empty())
        || requirements.is_none_or(|items| items.is_empty())
    {
        return Err(ValidationError::invalid(
            "objective, phase and at least one requirement are required",
        ));
    }

    let requirements = requirements.expect("checked above");
    let mut requirement_ids = BTreeSet::new();
    for requirement in requirements {
        let Some(requirement) = requirement.as_object() else {
            return Err(ValidationError::invalid(
                "requirement IDs must be nonempty and unique; statements cannot be empty",
            ));
        };
        let id = requirement.get("id").and_then(Value::as_str);
        let statement = requirement.get("statement").and_then(Value::as_str);
        if id.is_none_or(|value| value.trim().is_empty())
            || statement.is_none_or(|value| value.trim().is_empty())
            || !requirement_ids.insert(id.expect("checked above"))
        {
            return Err(ValidationError::invalid(
                "requirement IDs must be nonempty and unique; statements cannot be empty",
            ));
        }
    }

    let dependencies = object
        .get("dependencies")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut dependency_ids = BTreeSet::new();
    for dependency in dependencies {
        let Some(dependency) = dependency.as_object() else {
            return Err(ValidationError::invalid(
                "dependencies require unique task IDs, revision and phase",
            ));
        };
        let task_id = dependency.get("task_id").and_then(Value::as_str);
        let required_revision = dependency.get("required_revision").and_then(Value::as_i64);
        let required_phase = dependency.get("required_phase").and_then(Value::as_str);
        if task_id.is_none_or(|value| value.trim().is_empty())
            || required_revision.is_none_or(|revision| revision < 1)
            || required_phase.is_none_or(|value| value.trim().is_empty())
            || !dependency_ids.insert(task_id.expect("checked above"))
        {
            return Err(ValidationError::invalid(
                "dependencies require unique task IDs, revision and phase",
            ));
        }
    }

    let source_refs = object
        .get("source_refs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if source_refs.iter().any(|source_ref| {
        source_ref
            .as_str()
            .is_none_or(|value| value.trim().is_empty())
    }) {
        return Err(ValidationError::invalid(
            "source_refs cannot contain empty references",
        ));
    }

    let source_index = object
        .get("source_index")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut indexed_refs = BTreeSet::new();
    for source in source_index {
        validate_source_index_entry(source)?;
        let source_ref = source
            .get("source_ref")
            .and_then(Value::as_str)
            .expect("source_index validation checks source_ref");
        if !indexed_refs.insert(source_ref) {
            return Err(ValidationError::invalid(
                "source_index source_ref values must be unique",
            ));
        }
    }
    Ok(())
}

/// Validate one source-index entry, including the exact UTF-8 digest binding.
pub fn validate_source_index_entry(entry: &Value) -> Result<()> {
    let Some(entry) = entry.as_object() else {
        return Err(ValidationError::invalid(
            "source_index source_ref must be nonempty",
        ));
    };
    let source_ref = entry.get("source_ref").and_then(Value::as_str);
    if source_ref.is_none_or(|value| value.trim().is_empty()) {
        return Err(ValidationError::invalid(
            "source_index source_ref must be nonempty",
        ));
    }
    if entry
        .get("revision")
        .and_then(Value::as_str)
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ValidationError::invalid(
            "source_index revision cannot be empty",
        ));
    }
    if let Some(digest) = entry.get("content_sha256").and_then(Value::as_str) {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ValidationError::invalid(
                "source_index content_sha256 must be 64 lowercase hexadecimal characters",
            ));
        }
        if let Some(text) = entry.get("text").and_then(Value::as_str) {
            let expected = sha256(text.as_bytes());
            if digest != expected {
                return Err(ValidationError::invalid(
                    "source_index content_sha256 does not match the exact UTF-8 text",
                ));
            }
        }
    }

    match entry.get("status").and_then(Value::as_str) {
        Some("selected") => {
            if entry.get("revision").and_then(Value::as_str).is_none()
                || entry
                    .get("text")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                || entry
                    .get("content_sha256")
                    .and_then(Value::as_str)
                    .is_none()
            {
                return Err(ValidationError::invalid(
                    "selected source_index entries require revision, exact text and content_sha256",
                ));
            }
            if entry
                .get("gap_reason")
                .is_some_and(|value| !value.is_null())
            {
                return Err(ValidationError::invalid(
                    "selected source_index entries cannot have a gap_reason",
                ));
            }
        }
        Some("gap") => {
            if entry
                .get("gap_reason")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(ValidationError::invalid(
                    "gap source_index entries require a nonempty gap_reason",
                ));
            }
        }
        _ => {
            return Err(ValidationError::invalid(
                "source_index status must be selected or gap",
            ));
        }
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Validate the pure part of a Task revision transition and return its next
/// revision.  SQL CAS, live Attempt ownership and current-GM authorization
/// remain in the Store immediately around this check.
pub fn validate_revision_transition(
    expected_revision: i64,
    current_revision: i64,
    current_state: &str,
    depends_on_self: bool,
) -> Result<i64> {
    validate_revision_state(expected_revision, current_revision, current_state)?;
    validate_revision_update(expected_revision, depends_on_self)
}

/// Validate the stale/CAS portion before Store checks live ownership.  The
/// call ordering is part of the existing authorization contract.
pub fn validate_revision_state(
    expected_revision: i64,
    current_revision: i64,
    current_state: &str,
) -> Result<()> {
    if current_revision != expected_revision || current_state == "archived" {
        return Err(ValidationError::new(
            "STALE_REVISION",
            "Task changed or is archived",
        ));
    }
    Ok(())
}

/// Validate the revision payload after live ownership authorization.
pub fn validate_revision_update(expected_revision: i64, depends_on_self: bool) -> Result<i64> {
    if depends_on_self {
        return Err(ValidationError::invalid("Task cannot depend on itself"));
    }
    expected_revision
        .checked_add(1)
        .ok_or_else(|| ValidationError::invalid("revision overflow"))
}

/// Validate claim facts that are independent of the database and adapter.
pub fn validate_claim_start(start_owner: &str, launch_claim: bool) -> Result<()> {
    if launch_claim && start_owner != "controller" {
        return Err(ValidationError::new(
            "FORBIDDEN",
            "launch claims require controller-owned Attempt start",
        ));
    }
    Ok(())
}

/// `binding_id` and `binding_generation` are an all-or-nothing pair.  Explicit
/// nulls remain present and are left to the Store's existing text/positive
/// checks, preserving the current request errors.
pub fn validate_claim_binding_pair(
    binding_id: Option<&Value>,
    binding_generation: Option<&Value>,
) -> Result<()> {
    if binding_id.is_some() != binding_generation.is_some() {
        return Err(ValidationError::invalid(
            "binding_id and binding_generation must be supplied together",
        ));
    }
    Ok(())
}

/// Validate the persisted Task state used by a claim before Store SQL creates
/// an Attempt.  Existing Attempt deduplication and native binding readiness
/// remain SQL/Store concerns.
pub fn validate_claim_task_state(
    current_revision: i64,
    expected_revision: i64,
    state: &str,
) -> Result<()> {
    if current_revision != expected_revision || state != "open" {
        return Err(ValidationError::new(
            "STALE_REVISION",
            "Task is not open at the expected revision",
        ));
    }
    Ok(())
}

/// Validate the caller's release attestation before any Store readback.
pub fn validate_release_request(outcome: &str, assignment_closed: bool) -> Result<()> {
    if !matches!(outcome, "accepted" | "failed" | "cancelled" | "superseded") {
        return Err(ValidationError::invalid(
            "release outcome must be accepted/failed/cancelled/superseded; accepted requires an existing decision",
        ));
    }
    if !assignment_closed {
        return Err(ValidationError::invalid(
            "explicit assignment_closed=true attestation required; no process is stopped by release",
        ));
    }
    Ok(())
}

/// Validate release against bounded Store readback facts after authorization.
pub fn validate_release_transition(
    outcome: &str,
    accepted: bool,
    check_resource_held: bool,
    unresolved_operations: i64,
    unresolved_producers: bool,
) -> Result<()> {
    if (outcome == "accepted") != accepted {
        return Err(ValidationError::new(
            "ACCEPTANCE_STATE_MISMATCH",
            "accepted ownership must be released as accepted; revoke the decision before changing its outcome",
        ));
    }
    if check_resource_held {
        return Err(ValidationError::new(
            "CHECK_RESOURCE_HELD",
            "check process disposition is unresolved",
        ));
    }
    if unresolved_operations > 0 {
        return Err(ValidationError::new(
            "OUTCOME_UNKNOWN",
            "resolve already-sent effects before releasing ownership",
        ));
    }
    if unresolved_producers {
        return Err(ValidationError::new(
            "NATIVE_WORK_UNRESOLVED",
            "the exact assigned native runs have not ended; attestation cannot override a known producer",
        ));
    }
    Ok(())
}
