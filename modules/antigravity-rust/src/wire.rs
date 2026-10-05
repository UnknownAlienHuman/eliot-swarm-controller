use serde::Serialize;
use serde_json::Value;
use swarm_contracts::runtime::{ModuleReceiptIdentity, RuntimeCommand};

pub const ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";
pub const ARTIFACT_VERSION: &str = "2";
pub const REQUIRED_MODEL_ID: &str = "gemini-3.8-flash-high";

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

pub fn prompt_for(command: &RuntimeCommand) -> Result<String, &'static str> {
    match command.method.as_str() {
        "task.dispatch" => {
            let snapshot = command.input.get("task_snapshot");
            let body = command.input.get("text").and_then(Value::as_str);
            let specification = snapshot
                .filter(|value| !value.is_null())
                .map(|value| format!("Task specification: {value}"));
            let body = body.filter(|value| !value.trim().is_empty());
            match (specification, body) {
                (Some(specification), Some(body)) => Ok(format!("{specification}\n\n{body}")),
                (Some(specification), None) => Ok(specification),
                (None, Some(body)) => Ok(body.to_owned()),
                (None, None) => Err("DISPATCH_TEXT_REQUIRED"),
            }
        }
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
