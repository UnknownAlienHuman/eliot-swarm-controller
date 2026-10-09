use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use swarm_contracts::runtime::{
    ModuleReceiptIdentity, RuntimeCommand, TaskDispatchAdmissionReceipt, TaskDispatchContext,
};
use swarm_contracts::task_prompt::TaskPromptEnvelopeV1;

pub const ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";
pub const ARTIFACT_VERSION: &str = "5";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationIdentity {
    pub operation_id: String,
    pub method: String,
    pub binding_id: String,
    pub generation: i64,
    pub native_root_id: Option<String>,
    pub input_sha256: String,
    pub module_receipt: ModuleReceiptIdentity,
    pub target_input_sha256: Option<String>,
    pub target_operation_id: Option<String>,
}

impl TryFrom<&RuntimeCommand> for OperationIdentity {
    type Error = &'static str;

    fn try_from(command: &RuntimeCommand) -> Result<Self, Self::Error> {
        if command.operation_id.trim().is_empty()
            || command.binding_id.trim().is_empty()
            || command.generation <= 0
        {
            return Err("INVALID_OPERATION_IDENTITY");
        }
        if command.route.get("runtime").and_then(Value::as_str) != Some("antigravity")
            || command
                .route
                .get("module_artifact_id")
                .and_then(Value::as_str)
                != Some(ARTIFACT_ID)
        {
            return Err("MODULE_ARTIFACT_MISMATCH");
        }
        if !matches!(
            command.method.as_str(),
            "agent.open"
                | "task.dispatch"
                | "agent.send"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.background"
                | "agent.result"
                | "agent.recover"
        ) {
            return Err("UNSUPPORTED_METHOD");
        }
        let input_sha256 = command
            .input_sha256
            .as_deref()
            .filter(|digest| crate::module_receipt::is_lower_sha256(digest))
            .ok_or("MISSING_OPERATION_DIGEST")?
            .to_owned();
        let module_receipt = crate::module_receipt::for_command(command)
            .map_err(|_| "MODULE_RECEIPT_IDENTITY_INVALID")?;
        let (target_operation_id, target_input_sha256) = if command.method == "agent.reconcile" {
            let target_operation_id = command
                .input
                .get("operation_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or("RECONCILE_OPERATION_ID_REQUIRED")?
                .to_owned();
            let target_input_sha256 = command
                .target_input_sha256
                .as_deref()
                .filter(|digest| crate::module_receipt::is_lower_sha256(digest))
                .ok_or("MISSING_RECONCILE_TARGET_DIGEST")?
                .to_owned();
            (Some(target_operation_id), Some(target_input_sha256))
        } else if command.method == "agent.result" {
            if command.input["selector"]["kind"] != "antigravity_status"
                && !command.input["normalized_result_origin"].is_object()
            {
                return Err("UNSUPPORTED_RESULT_SELECTOR");
            }
            let target_operation_id = command
                .input
                .get("selector")
                .and_then(|selector| selector.get("input_operation_id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or("RESULT_TARGET_OPERATION_ID_REQUIRED")?
                .to_owned();
            let target_input_sha256 = command
                .target_input_sha256
                .as_deref()
                .filter(|digest| crate::module_receipt::is_lower_sha256(digest))
                .ok_or("MISSING_RESULT_TARGET_DIGEST")?
                .to_owned();
            (Some(target_operation_id), Some(target_input_sha256))
        } else {
            (None, None)
        };
        Ok(Self {
            operation_id: command.operation_id.clone(),
            method: command.method.clone(),
            binding_id: command.binding_id.clone(),
            generation: command.generation,
            native_root_id: command.native_root_id.clone(),
            input_sha256,
            module_receipt,
            target_input_sha256,
            target_operation_id,
        })
    }
}

pub fn command_params(command: &RuntimeCommand) -> Result<Value, serde_json::Error> {
    serde_json::to_value(command)
}

#[derive(Serialize)]
struct UserMessage<'a> {
    event: &'static str,
    message: UserContent<'a>,
}

#[derive(Serialize)]
struct UserContent<'a> {
    content: &'a str,
}

pub fn encode_user_line(text: &str) -> Result<Vec<u8>, &'static str> {
    const MAX_PROMPT_BYTES: usize = 256 * 1024;
    if text.trim().is_empty() {
        return Err("PROMPT_TEXT_REQUIRED");
    }
    if text.len() > MAX_PROMPT_BYTES {
        return Err("PROMPT_TOO_LARGE");
    }
    let mut line = serde_json::to_vec(&UserMessage {
        event: "user",
        message: UserContent { content: text },
    })
    .map_err(|_| "PROMPT_ENCODING_FAILED")?;
    line.push(b'\n');
    if line.len() > 1_048_576 {
        return Err("PROMPT_TOO_LARGE");
    }
    Ok(line)
}

pub fn normalized_dispatch_admission(
    command: &RuntimeCommand,
    identity: &OperationIdentity,
    boot_id: &str,
    prompt: &str,
) -> Result<TaskDispatchAdmissionReceipt, &'static str> {
    if command.method != "task.dispatch" {
        return Err("EXPECTED_TASK_DISPATCH");
    }
    let (envelope, context) = validated_task_prompt(command)?;
    if identity.method != "task.dispatch"
        || context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != boot_id
        || identity.operation_id != command.operation_id
        || identity.binding_id != command.binding_id
        || identity.generation != command.generation
        || prompt != envelope.prompt.as_str()
    {
        return Err("TASK_DISPATCH_CONTEXT_INVALID");
    }
    let prompt_bytes = u64::try_from(prompt.len()).map_err(|_| "TASK_PROMPT_INVALID")?;
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt: identity.module_receipt.clone(),
        operation_id: context.operation_id,
        binding_id: context.binding_id,
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id,
        attempt_id: context.attempt_id,
        task_id: context.task_id,
        task_revision: context.task_revision,
        task_snapshot_sha256: context.task_snapshot_sha256,
        source_text_sha256: context.source_text_sha256,
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: envelope.prompt_sha256,
        native_payload_bytes: prompt_bytes,
        native_input_id: None,
    };
    receipt
        .validate()
        .map_err(|_| "TASK_DISPATCH_CONTEXT_INVALID")?;
    Ok(receipt)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validated_task_prompt(
    command: &RuntimeCommand,
) -> Result<(TaskPromptEnvelopeV1, TaskDispatchContext), &'static str> {
    if command.input.get("task_snapshot").is_some()
        || command.input.get("task_snapshot_canonical").is_some()
    {
        return Err("TASK_PROMPT_SNAPSHOT_FALLBACK_FORBIDDEN");
    }
    let envelope: TaskPromptEnvelopeV1 = serde_json::from_value(
        command
            .input
            .get("task_prompt")
            .cloned()
            .ok_or("TASK_PROMPT_REQUIRED")?,
    )
    .map_err(|_| "TASK_PROMPT_INVALID")?;
    envelope
        .validate_shape()
        .map_err(|_| "TASK_PROMPT_INVALID")?;
    if envelope.prompt_sha256 != sha256(envelope.prompt.as_bytes()) {
        return Err("TASK_PROMPT_DIGEST_MISMATCH");
    }

    let context: TaskDispatchContext = serde_json::from_value(
        command
            .input
            .get("task_dispatch_context")
            .cloned()
            .ok_or("TASK_DISPATCH_CONTEXT_INVALID")?,
    )
    .map_err(|_| "TASK_DISPATCH_CONTEXT_INVALID")?;
    context
        .validate()
        .map_err(|_| "TASK_DISPATCH_CONTEXT_INVALID")?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || envelope.task_id != context.task_id
        || envelope.task_revision != context.task_revision
        || envelope.attempt_id != context.attempt_id
        || envelope.task_snapshot_sha256 != context.task_snapshot_sha256
    {
        return Err("TASK_PROMPT_IDENTITY_MISMATCH");
    }
    let source_text = command
        .input
        .get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or("TASK_DISPATCH_CONTEXT_INVALID")?;
    let source_text_bytes =
        u64::try_from(source_text.len()).map_err(|_| "TASK_DISPATCH_CONTEXT_INVALID")?;
    if context.source_text_sha256 != sha256(source_text.as_bytes())
        || context.source_text_bytes != source_text_bytes
    {
        return Err("TASK_DISPATCH_CONTEXT_INVALID");
    }
    Ok((envelope, context))
}

pub fn prompt_for(command: &RuntimeCommand) -> Result<String, &'static str> {
    match command.method.as_str() {
        "task.dispatch" => validated_task_prompt(command).map(|(envelope, _)| envelope.prompt),
        "agent.send" => {
            if command.input.get("delivery").and_then(Value::as_str) == Some("steer") {
                return Err("UNSUPPORTED_DELIVERY");
            }
            if command
                .input
                .get("delivery")
                .and_then(Value::as_str)
                .is_some_and(|delivery| delivery != "next_turn")
            {
                return Err("UNSUPPORTED_DELIVERY");
            }
            command
                .input
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned)
                .ok_or("PROMPT_TEXT_REQUIRED")
        }
        _ => Err("METHOD_HAS_NO_PROMPT"),
    }
}
