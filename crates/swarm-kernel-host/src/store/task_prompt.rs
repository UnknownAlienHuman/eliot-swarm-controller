//! Store-owned deterministic prompt construction from a frozen Attempt.

use super::launcher_dispatch::LaunchDispatchAdmission;
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use swarm_contracts::task_prompt::{
    TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION, TaskPromptEnvelopeV1,
};

pub(super) fn selected_task_prompt_v1(db: &Connection, binding: &Value) -> Result<bool> {
    if binding["route"]["runtime"] == "zed"
        && binding["module_artifact_id"] == "eliot-zed.eval-cli.2"
    {
        return Ok(true);
    }
    let Some(selector) = binding["observation"].get("module_contract_selector") else {
        return Ok(false);
    };
    let identity = super::module_handshake::retained_contract_identity(
        db,
        model::text(binding, "module_artifact_id")?,
        Some(selector),
    )?
    .ok_or_else(|| {
        Error::new(
            "MODULE_DESCRIPTOR_MISSING",
            "TaskPrompt selector has no retained descriptor",
        )
    })?;
    let prompt = swarm_contracts::module_contract::task_prompt_schema();
    if identity
        .command_schemas
        .iter()
        .any(|schema| schema.schema_id == prompt.schema_id && schema != &prompt)
    {
        return Err(Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "unsupported TaskPrompt schema",
        ));
    }
    if !identity.command_schemas.contains(&prompt) {
        return Ok(false);
    }
    if !identity
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == "task.dispatch")
        || !identity
            .command_schemas
            .contains(&swarm_contracts::module_contract::task_dispatch_context_schema())
        || !identity
            .event_schemas
            .contains(&swarm_contracts::module_contract::task_dispatch_admission_schema())
    {
        return Err(Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "TaskPrompt requires exact dispatch capability and admission schemas",
        ));
    }
    Ok(true)
}

/// Old executors remain readable for retained bindings. New roots use the
/// current prompt contract, pinned by a trusted descriptor where applicable.
pub(super) fn require_new_binding(
    route: &crate::config::Route,
    selector: Option<&Value>,
) -> Result<()> {
    if matches!(
        route.module_artifact_id.as_str(),
        "eliot-zed.eval-cli.1"
            | "command-mod-0.1.0-glue.2"
            | "command-mod-0.1.0-glue.3"
            | "command-mod-0.1.0-glue.4"
            | "codex-sdk-18194bf-bridge.3"
            | "claude-agent-sdk-0.3.287-bridge.3"
            | "antigravity-cli-warm-bridge.2"
            | "muse-sdk-1.3.0-bridge.8"
    ) {
        return Err(Error::new(
            "ARTIFACT_RETIRED",
            "this retained artifact is unavailable for new bindings",
        ));
    }
    if selector.is_none()
        && matches!(
            route.module_artifact_id.as_str(),
            "codex-rust-controller.1"
                | "eliot-command.rust-headless.1"
                | "eliot-command.acp-rust.1"
                | "eliot-opencode-v2.rust-http.1"
                | "eliot-antigravity.rust-headless.1"
                | "claude-agent-sdk-rust-controller.5"
                | "muse-sdk-1.3.0-bridge.9"
                | "command-mod-0.1.0-glue.5"
        )
    {
        return Err(Error::new(
            "MODULE_CONTRACT_REQUIRED",
            "current artifact requires its exact trusted descriptor for a new binding",
        ));
    }
    Ok(())
}

pub(super) fn retained_prompt(effective: &Value, attempt: &Value) -> Result<TaskPromptEnvelopeV1> {
    let envelope: TaskPromptEnvelopeV1 =
        serde_json::from_value(effective["task_prompt"].clone())
            .map_err(|_| invalid_prompt("retained TaskPrompt is missing or malformed"))?;
    envelope
        .validate_shape()
        .map_err(|_| invalid_prompt("retained TaskPrompt shape is invalid"))?;
    if envelope.task_id != model::text(attempt, "task_id")?
        || envelope.attempt_id != model::text(attempt, "attempt_id")?
        || envelope.task_revision != model::positive(attempt, "task_revision")?
        || envelope.task_snapshot_sha256
            != model::digest(model::canonical(&attempt["task_snapshot"])?.as_bytes())
        || envelope.prompt_sha256 != model::digest(envelope.prompt.as_bytes())
    {
        return Err(invalid_prompt(
            "retained TaskPrompt identity or digest differs",
        ));
    }
    Ok(envelope)
}

/// Build the one immutable prompt envelope for a newly admitted Task dispatch.
/// `launch_dispatch` must be the exact admission produced in the same Store
/// transaction; no request-supplied packet or Task snapshot is accepted here.
pub(super) fn build_task_prompt_v1(
    attempt: &Value,
    source_text: &str,
    launch_dispatch: Option<&LaunchDispatchAdmission>,
) -> Result<TaskPromptEnvelopeV1> {
    let task_id = model::text(attempt, "task_id")?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let task_revision = model::positive(attempt, "task_revision")?;
    if source_text.trim().is_empty() {
        return Err(invalid_prompt("source text is empty"));
    }

    let snapshot = attempt
        .get("task_snapshot")
        .filter(|snapshot| snapshot.is_object())
        .ok_or_else(|| invalid_prompt("frozen Attempt snapshot is missing"))?;
    if snapshot["revision"].as_i64() != Some(task_revision) {
        return Err(invalid_prompt(
            "frozen Attempt revision differs from its snapshot",
        ));
    }
    let brief = snapshot
        .get("brief")
        .filter(|brief| brief.is_object())
        .ok_or_else(|| invalid_prompt("frozen Task brief is missing"))?;

    let snapshot_sha256 = model::digest(model::canonical(snapshot)?.as_bytes());
    let canonical_brief = model::canonical(brief)?;
    let identity = model::canonical(&json!({
        "attempt_id":attempt_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "task_snapshot_sha256":snapshot_sha256,
    }))?;

    let mut prompt = String::with_capacity(source_text.len() + canonical_brief.len() + 256);
    prompt.push_str(source_text);
    prompt.push_str("\n\nELIOT Task identity v1:\n");
    prompt.push_str(&identity);
    prompt.push_str("\n\nELIOT Task brief v1:\n");
    prompt.push_str(&canonical_brief);

    if let Some(admission) = launch_dispatch {
        validate_launch_dispatch_identity(
            admission,
            task_id,
            task_revision,
            attempt_id,
            &snapshot_sha256,
        )?;
        prompt.push_str("\n\nELIOT Launch dispatch packet v1:\n");
        prompt.push_str(&model::canonical(&admission.packet)?);
    }

    let prompt_bytes = u64::try_from(prompt.len())
        .map_err(|_| invalid_prompt("prompt byte count is outside the supported range"))?;
    let envelope = TaskPromptEnvelopeV1 {
        schema_id: TASK_PROMPT_SCHEMA_ID.to_owned(),
        schema_version: TASK_PROMPT_SCHEMA_VERSION,
        task_id: task_id.to_owned(),
        task_revision,
        attempt_id: attempt_id.to_owned(),
        task_snapshot_sha256: snapshot_sha256,
        prompt_sha256: model::digest(prompt.as_bytes()),
        prompt_bytes,
        prompt,
    };
    envelope
        .validate_shape()
        .map_err(|_| invalid_prompt("constructed TaskPrompt envelope failed validation"))?;
    Ok(envelope)
}

fn validate_launch_dispatch_identity(
    admission: &LaunchDispatchAdmission,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    snapshot_sha256: &str,
) -> Result<()> {
    let packet = &admission.packet;
    let packet_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(packet)?.as_bytes())
    );
    let expected_snapshot_digest = format!("sha256:{snapshot_sha256}");
    if packet["schema_version"] != 1
        || packet["launch_operation_id"].as_str() != Some(admission.launch_operation_id.as_str())
        || packet["task"]["task_id"].as_str() != Some(task_id)
        || packet["task"]["revision"].as_i64() != Some(task_revision)
        || packet["task"]["attempt_id"].as_str() != Some(attempt_id)
        || packet["task"]["snapshot_digest"].as_str() != Some(expected_snapshot_digest.as_str())
        || admission.packet_digest != packet_digest
    {
        return Err(invalid_prompt(
            "launch dispatch packet differs from the frozen Attempt",
        ));
    }
    Ok(())
}

fn invalid_prompt(message: &str) -> Error {
    Error::new("TASK_PROMPT_INVALID", message)
}
