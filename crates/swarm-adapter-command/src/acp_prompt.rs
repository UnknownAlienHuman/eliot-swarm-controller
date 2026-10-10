//! Exact Store-produced TaskPrompt admission for the separately selected ACP artifact.

use swarm_contracts::{
    error::{Error, Result},
    runtime::{RuntimeCommand, TaskDispatchContext},
    task_prompt::{TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION, TaskPromptEnvelopeV1},
};

use crate::journal::digest;

const MAX_TASK_PROMPT_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpDispatchIdentity {
    pub operation_id: String,
    pub input_sha256: String,
    pub requested_model: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub task_snapshot_sha256: String,
    pub prompt_sha256: String,
    pub prompt_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedAcpDispatch {
    pub envelope: TaskPromptEnvelopeV1,
    pub context: TaskDispatchContext,
    pub identity: AcpDispatchIdentity,
}

/// Prepare the exact Store projection for ACP. This function deliberately
/// never reads `text`, `task_snapshot`, or the batch prompt renderer.
pub(crate) fn prepare(command: &RuntimeCommand) -> Result<PreparedAcpDispatch> {
    if command.method != "task.dispatch" {
        return Err(Error::invalid("ACP TaskPrompt requires task.dispatch"));
    }

    let envelope: TaskPromptEnvelopeV1 =
        serde_json::from_value(command.input["task_prompt"].clone()).map_err(|_| {
            Error::new(
                "TASK_PROMPT_INVALID",
                "Store did not provide the exact TaskPrompt v1 envelope",
            )
        })?;
    envelope.validate_shape().map_err(|_| {
        Error::new(
            "TASK_PROMPT_INVALID",
            "Store TaskPrompt v1 envelope is invalid",
        )
    })?;

    let prompt = envelope.prompt.as_bytes();
    if envelope.schema_id != TASK_PROMPT_SCHEMA_ID
        || envelope.schema_version != TASK_PROMPT_SCHEMA_VERSION
        || prompt.is_empty()
        || prompt.len() > MAX_TASK_PROMPT_BYTES
        || u64::try_from(prompt.len()).ok() != Some(envelope.prompt_bytes)
        || digest(prompt) != envelope.prompt_sha256
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "TaskPrompt bytes, schema, or digest differ from the retained envelope",
        ));
    }

    let context: TaskDispatchContext =
        serde_json::from_value(command.input["task_dispatch_context"].clone()).map_err(|_| {
            Error::new(
                "TASK_DISPATCH_CONTEXT_INVALID",
                "ACP dispatch context is malformed",
            )
        })?;
    context.validate().map_err(|_| {
        Error::new(
            "TASK_DISPATCH_CONTEXT_INVALID",
            "ACP dispatch context is invalid",
        )
    })?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.task_id != envelope.task_id
        || context.task_revision != envelope.task_revision
        || context.attempt_id != envelope.attempt_id
        || context.task_snapshot_sha256 != envelope.task_snapshot_sha256
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "TaskPrompt identity differs from the Store dispatch context",
        ));
    }

    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|value| is_sha256(value))
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "Store Operation digest is missing or invalid",
            )
        })?;
    let requested_model = command.route["native_options"]["modelId"]
        .as_str()
        .filter(|value| !value.trim().is_empty() && value.trim() == *value && value.len() <= 256)
        .ok_or_else(|| {
            Error::new(
                "COMMAND_MODEL_REQUIRED",
                "explicit ACP route modelId is invalid",
            )
        })?;

    Ok(PreparedAcpDispatch {
        identity: AcpDispatchIdentity {
            operation_id: command.operation_id.clone(),
            input_sha256: input_sha256.to_owned(),
            requested_model: requested_model.to_owned(),
            task_id: envelope.task_id.clone(),
            task_revision: envelope.task_revision,
            attempt_id: envelope.attempt_id.clone(),
            task_snapshot_sha256: envelope.task_snapshot_sha256.clone(),
            prompt_sha256: digest(prompt),
            prompt_bytes: envelope.prompt_bytes,
        },
        envelope,
        context,
    })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
