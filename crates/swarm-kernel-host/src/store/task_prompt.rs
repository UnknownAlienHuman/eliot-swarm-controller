//! Store-only authority for a descriptor-selected immutable TaskPrompt.
//! No adapter is permitted to rebuild the prompt from the Task snapshot.

use super::module_handshake;
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use swarm_contracts::{
    module_contract,
    task_prompt::{
        TASK_PROMPT_CONTRACT_REVISION, TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION,
        TaskPromptEnvelopeV1,
    },
};

/// Selection is pinned by the binding's registered immutable descriptor,
/// never a caller field or module-supplied feature string.
pub(super) fn selected(db: &Connection, binding: &Value) -> Result<bool> {
    let Some(selector) = binding["observation"].get("module_contract_selector") else {
        return Ok(false);
    };
    let identity = module_handshake::retained_contract_identity(
        db,
        model::text(binding, "module_artifact_id")?,
        Some(selector),
    )?
    .ok_or_else(|| {
        Error::new(
            "MODULE_DESCRIPTOR_MISSING",
            "selected binding has no retained module descriptor",
        )
    })?;
    module_contract::task_prompt_selected(
        identity.command_schemas.iter(),
        identity.event_schemas.iter(),
        identity.capabilities.iter(),
    )
    .map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "selected TaskPrompt schema is unknown or lacks its dispatch admission contract",
        )
    })
}

/// One deterministic producer for source text, retained frozen Task brief,
/// and an optional trusted launch packet. The entire snapshot stays in Store.
pub(super) fn build(
    attempt: &Value,
    source_text: &str,
    launch_packet: Option<&Value>,
) -> Result<TaskPromptEnvelopeV1> {
    let snapshot = attempt.get("task_snapshot").filter(|value| value.is_object()).ok_or_else(|| {
        Error::new("TASK_PROMPT_INVALID", "retained Attempt has no frozen Task snapshot")
    })?;
    let brief = snapshot.get("brief").filter(|value| value.is_object()).ok_or_else(|| {
        Error::new("TASK_PROMPT_INVALID", "retained frozen Task brief is missing")
    })?;
    if source_text.trim().is_empty() {
        return Err(Error::new("TASK_PROMPT_INVALID", "source task text is empty"));
    }
    let task_id = model::text(attempt, "task_id")?.to_owned();
    let attempt_id = model::text(attempt, "attempt_id")?.to_owned();
    let task_revision = model::positive(attempt, "task_revision")?;
    let task_snapshot_sha256 = model::digest(model::canonical(snapshot)?.as_bytes());
    let identity = json!({
        "attempt_id":attempt_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "task_snapshot_sha256":task_snapshot_sha256,
    });
    let mut prompt = format!(
        "{source_text}\n\nELIOT Task identity v1:\n{}\n\nELIOT Task brief v1:\n{}",
        model::canonical(&identity)?,
        model::canonical(brief)?,
    );
    if let Some(packet) = launch_packet {
        if !packet.is_object() {
            return Err(Error::new("TASK_PROMPT_INVALID", "launch packet must be an object"));
        }
        prompt.push_str("\n\nELIOT Launch dispatch packet v1:\n");
        prompt.push_str(&model::canonical(packet)?);
    }
    let envelope = TaskPromptEnvelopeV1 {
        schema_id: TASK_PROMPT_SCHEMA_ID.to_owned(),
        schema_version: TASK_PROMPT_SCHEMA_VERSION,
        task_id,
        task_revision,
        attempt_id,
        task_snapshot_sha256,
        prompt_sha256: model::digest(prompt.as_bytes()),
        prompt_bytes: u64::try_from(prompt.len())
            .map_err(|_| Error::new("TASK_PROMPT_INVALID", "prompt byte length overflows"))?,
        prompt,
    };
    envelope.validate_shape().map_err(|_| {
        Error::new("TASK_PROMPT_INVALID", "constructed TaskPrompt envelope is invalid")
    })?;
    Ok(envelope)
}

/// Validate saved effective bytes against the current immutable Attempt and
/// exact source text. Never repair/re-render a missing v1 envelope on read.
pub(super) fn load(
    effective: &Value,
    attempt: &Value,
    source_text: &str,
) -> Result<TaskPromptEnvelopeV1> {
    if effective["operation_contract"]["task_prompt"]["contract_revision"]
        != TASK_PROMPT_CONTRACT_REVISION
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "selected TaskPrompt operation contract is absent or changed",
        ));
    }
    let saved: TaskPromptEnvelopeV1 =
        serde_json::from_value(effective["task_prompt"].clone()).map_err(|_| {
            Error::new("TASK_PROMPT_INVALID", "selected TaskPrompt envelope is missing or malformed")
        })?;
    saved.validate_shape().map_err(|_| {
        Error::new("TASK_PROMPT_INVALID", "selected TaskPrompt envelope fails shape validation")
    })?;
    let expected = build(
        attempt,
        source_text,
        effective.get("launch_dispatch_packet").filter(|value| !value.is_null()),
    )?;
    if saved != expected {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "selected TaskPrompt differs from the frozen Task, source text or launch packet",
        ));
    }
    Ok(saved)
}
