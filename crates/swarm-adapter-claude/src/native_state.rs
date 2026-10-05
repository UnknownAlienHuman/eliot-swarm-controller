//! Rust-owned control receipts and bounded SDK readback state.
//!
//! The Node process is deliberately only the pinned SDK driver: it sanitizes
//! native frames, but it does not match input UUIDs to Operations, adopt a
//! session identity, or decide whether a native effect was admitted.

use crate::{config::AdapterConfig, journal::OperationJournal, receipt};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{EffectOutcome, RuntimeOutcome, TaskDispatchAdmissionReceipt},
};

const MAX_SEEN_FRAMES: usize = 8_192;
const MAX_INPUT_EXECUTIONS: usize = 128;
const MAX_PENDING_INPUTS: usize = 64;
const MAX_FAMILY_EVENTS: usize = 128;

#[derive(Default)]
pub struct NativeControl {
    pending_inputs: HashMap<String, String>,
    init_session_id: Option<String>,
    init_model: Option<String>,
    input_executions: Vec<Value>,
    family_events: Vec<Value>,
    family_event_count: u64,
    family_events_truncated: bool,
    family_projection_incomplete: bool,
    seen_frames: HashSet<String>,
    frame_order: VecDeque<String>,
    native_events_seen: u64,
}

impl NativeControl {
    pub fn register_input(&mut self, input_id: &str, operation_id: &str) -> Result<()> {
        if input_id.trim().is_empty()
            || input_id.len() > 128
            || operation_id.trim().is_empty()
            || self.pending_inputs.len() >= MAX_PENDING_INPUTS
            || self.pending_inputs.contains_key(input_id)
        {
            return Err(Error::new(
                "NATIVE_INPUT_IDENTITY",
                "input identity is invalid or already pending",
            ));
        }
        self.pending_inputs
            .insert(input_id.to_owned(), operation_id.to_owned());
        Ok(())
    }

    pub fn forget_operation(&mut self, operation_id: &str) {
        self.pending_inputs
            .retain(|_, pending_operation| pending_operation != operation_id);
    }

    pub fn clear_pending(&mut self) {
        self.pending_inputs.clear();
    }

    pub fn snapshot(
        &self,
        config: &AdapterConfig,
        boot_id: &str,
        session_root: Option<&str>,
    ) -> Value {
        json!({
            "bridge_boot_id":boot_id,
            "native_scope_key":config.native_options.scope_key(),
            "root_id":session_root,
            "state":{
                "init_session_id":self.init_session_id,
                "effective_model":self.init_model,
                "input_executions":&self.input_executions,
                "family_events":&self.family_events,
                "family_event_count":self.family_event_count,
                "family_events_truncated":self.family_events_truncated,
                "family_projection_incomplete":self.family_projection_incomplete,
                "native_events_seen":self.native_events_seen
            }
        })
    }

    pub fn observe_frame(
        &mut self,
        frame: &Value,
        config: &AdapterConfig,
        boot_id: &str,
        journal: &OperationJournal,
        session_root: &mut Option<String>,
    ) -> Result<()> {
        let frame_type = frame["type"]
            .as_str()
            .ok_or_else(|| Error::new("SDK_FRAME_SCHEMA", "SDK frame is missing its type"))?;
        if frame_type.len() > 32 {
            return Err(Error::new(
                "SDK_FRAME_SCHEMA",
                "SDK frame type exceeds its boundary",
            ));
        }
        self.native_events_seen = self.native_events_seen.saturating_add(1);
        let frame_key = safe_identity(&frame["uuid"]).or_else(|| {
            (frame_type == "hook").then(|| {
                format!(
                    "hook:{}:{}:{}",
                    frame["subtype"].as_str().unwrap_or("unknown"),
                    frame["session_id"].as_str().unwrap_or("unknown"),
                    frame["agent_id"].as_str().unwrap_or("unknown")
                )
            })
        });
        let duplicate_frame = frame_key.is_some_and(|key| !self.remember_frame(key));

        if !duplicate_frame && frame_type == "system" && frame["subtype"] == "init" {
            self.init_session_id = safe_identity(&frame["session_id"]);
            self.init_model = safe_identity(&frame["model"]);
        }

        let frame_session = safe_identity(&frame["session_id"]);
        let mut input_ids = Vec::new();
        if let Some(id) = safe_identity(&frame["user_message_uuid"]) {
            input_ids.push(id);
        }
        if let Some(ids) = frame["user_message_uuids"].as_array() {
            for value in ids.iter().take(64) {
                if let Some(id) = safe_identity(value)
                    && !input_ids.contains(&id)
                {
                    input_ids.push(id);
                }
            }
        }

        for input_id in input_ids {
            let Some(operation_id) = self.pending_inputs.get(&input_id).cloned() else {
                continue;
            };
            let Some(saved) = journal.get(&operation_id)? else {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISSING",
                    "SDK input echo has no durable operation intent",
                ));
            };
            let Some(native) = saved.intent.as_ref().map(|intent| &intent["native"]) else {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISSING",
                    "SDK input echo has no durable native identity",
                ));
            };
            let expected_initial = saved.method.as_deref() == Some("task.dispatch")
                && native["native_root_id"].is_null();
            let exact_boot = saved
                .intent
                .as_ref()
                .and_then(|intent| intent["bridge_boot_id"].as_str())
                == Some(boot_id);
            let exact_scope = native["native_scope_key"] == config.native_options.scope_key();
            let exact_session = frame_session.as_deref().is_some_and(|session| {
                if expected_initial {
                    session_root.is_none() && self.init_session_id.as_deref() == Some(session)
                } else {
                    session_root.as_deref() == Some(session)
                        && native["native_root_id"].as_str() == Some(session)
                }
            });
            if native["user_message_uuid"].as_str() != Some(input_id.as_str())
                || !exact_boot
                || !exact_scope
                || !exact_session
            {
                continue;
            }
            let session_id = frame_session.as_deref().expect("validated above");
            save_input_admission(
                config,
                journal,
                &operation_id,
                &saved,
                &input_id,
                session_id,
                expected_initial,
            )?;
            if expected_initial {
                *session_root = Some(session_id.to_owned());
            }
            self.pending_inputs.remove(&input_id);
        }

        if !duplicate_frame && frame_type == "result" {
            self.record_result(frame, session_root.as_deref());
        }
        if !duplicate_frame {
            self.record_family_event(frame, frame_type, frame_session.as_deref(), session_root.as_deref());
        }
        Ok(())
    }

    fn remember_frame(&mut self, frame_id: String) -> bool {
        if !self.seen_frames.insert(frame_id.clone()) {
            return false;
        }
        self.frame_order.push_back(frame_id);
        if self.frame_order.len() > MAX_SEEN_FRAMES
            && let Some(oldest) = self.frame_order.pop_front()
        {
            self.seen_frames.remove(&oldest);
        }
        true
    }

    fn record_family_event(
        &mut self,
        frame: &Value,
        frame_type: &str,
        frame_session: Option<&str>,
        session_root: Option<&str>,
    ) {
        let subtype = frame["subtype"].as_str().unwrap_or("");
        let event_type = if frame_type == "system"
            && matches!(
                subtype,
                "task_started" | "task_progress" | "task_notification" | "task_updated"
            )
        {
            Some(subtype)
        } else if frame_type == "hook" && matches!(subtype, "subagent_started" | "subagent_stopped") {
            Some(subtype)
        } else if matches!(frame_type, "assistant" | "user")
            && frame["parent_tool_use_id"].is_string()
        {
            Some("subagent_message")
        } else if frame_type == "system"
            && subtype == "permission_denied"
            && frame["agent_id"].is_string()
        {
            Some("subagent_permission_denied")
        } else {
            None
        };
        let Some(event_type) = event_type else {
            return;
        };

        let Some(session_id) = frame_session.filter(|session| {
            self.init_session_id.as_deref() == Some(*session)
                && session_root.is_none_or(|root| root == *session)
        }) else {
            self.family_projection_incomplete = true;
            return;
        };

        let task_id = if matches!(event_type, "task_started" | "task_progress" | "task_notification" | "task_updated") {
            let Some(task_id) = safe_family_link(&frame["task_id"]) else {
                self.family_projection_incomplete = true;
                return;
            };
            Some(task_id)
        } else {
            None
        };
        let task_identity = if let Some(task_id) = task_id.as_deref() {
            Some(format!("claude-sdk:task:v1:{task_id}"))
        } else {
            None
        };
        let agent_id = safe_family_link(&frame["agent_id"]);
        let member_identity = if matches!(event_type, "subagent_started" | "subagent_stopped" | "subagent_permission_denied") {
            let Some(agent_id) = safe_family_link(&frame["agent_id"]) else {
                self.family_projection_incomplete = true;
                return;
            };
            Some(format!("claude-sdk:agent:v1:{agent_id}"))
        } else if event_type == "subagent_message" {
            agent_id
                .as_deref()
                .map(|agent_id| format!("claude-sdk:agent:v1:{agent_id}"))
        } else {
            None
        };
        let parent_tool_use_id = if event_type == "subagent_message" {
            safe_family_link(&frame["parent_tool_use_id"])
        } else {
            None
        };
        if event_type == "subagent_message" && parent_tool_use_id.is_none() {
            self.family_projection_incomplete = true;
            return;
        }

        let tool_use_id = safe_family_link(&frame["tool_use_id"]);
        let hook_tool_use_id = safe_family_link(&frame["hook_tool_use_id"]);
        let agent_type = safe_family_label(&frame["agent_type"])
            .or_else(|| safe_family_label(&frame["subagent_type"]));
        let task_type = safe_family_label(&frame["task_type"]);
        let task_reason = safe_family_label(&frame["task_reason"]);
        let task_last_tool_name = safe_family_label(&frame["task_last_tool_name"]);
        let resource_links = safe_family_resource_links(&frame["resource_links"]);
        let mut event = json!({
            "source":"claude_agent_sdk",
            "event_type":event_type,
            "native_session_id":session_id,
            "task_id":task_id,
            "task_identity":task_identity,
            "agent_id":agent_id,
            "member_identity":member_identity,
            "tool_use_id":tool_use_id,
            "hook_tool_use_id":hook_tool_use_id,
            "parent_tool_use_id":parent_tool_use_id,
            "agent_type":agent_type,
            "task_type":task_type,
            "task_reason":task_reason,
            "task_last_tool_name":task_last_tool_name,
            "resource_links":resource_links,
            "frame_uuid":safe_identity(&frame["uuid"]),
            "prompt_id":safe_family_link(&frame["prompt_id"]),
            "user_message_uuid":safe_family_link(&frame["user_message_uuid"]),
            "user_message_uuids":safe_family_input_links(&frame["user_message_uuids"]),
            "sdk_task_status":safe_sdk_task_status(&frame["task_status"]),
            "task_patch_status":safe_sdk_task_status(&frame["task_patch_status"]),
            "is_backgrounded":frame["is_backgrounded"].as_bool(),
            "task_patch_is_backgrounded":frame["task_patch_is_backgrounded"].as_bool(),
            "spawn_depth":frame["spawn_depth"].as_u64().filter(|depth| *depth <= 128),
            "ambient":frame["ambient"].as_bool(),
            "resource_links_truncated":frame["resource_links_overflow"] == true,
            "input_links_truncated":frame["user_message_uuids_overflow"] == true
        });
        if !matches!(
            event_type,
            "task_started" | "task_progress" | "task_notification" | "task_updated"
        ) {
            event.as_object_mut().expect("family event is an object").remove("sdk_task_status");
            event.as_object_mut().expect("family event is an object").remove("task_patch_status");
            event.as_object_mut().expect("family event is an object").remove("is_backgrounded");
            event.as_object_mut().expect("family event is an object").remove("task_patch_is_backgrounded");
            event.as_object_mut().expect("family event is an object").remove("spawn_depth");
            event.as_object_mut().expect("family event is an object").remove("ambient");
        }
        if task_id.is_none() {
            event.as_object_mut().expect("family event is an object").remove("task_id");
            event.as_object_mut().expect("family event is an object").remove("task_type");
        }
        if agent_id.is_none() {
            event.as_object_mut().expect("family event is an object").remove("agent_id");
        }
        if member_identity.is_none() {
            event.as_object_mut().expect("family event is an object").remove("member_identity");
        }
        if agent_type.is_none() {
            event.as_object_mut().expect("family event is an object").remove("agent_type");
        }
        if tool_use_id.is_none() {
            event.as_object_mut().expect("family event is an object").remove("tool_use_id");
        }
        if hook_tool_use_id.is_none() {
            event.as_object_mut().expect("family event is an object").remove("hook_tool_use_id");
        }
        if event_type != "subagent_message" {
            event.as_object_mut().expect("family event is an object").remove("parent_tool_use_id");
        }
        if frame["uuid"].is_null() {
            event.as_object_mut().expect("family event is an object").remove("frame_uuid");
        }
        if frame["prompt_id"].is_null() {
            event.as_object_mut().expect("family event is an object").remove("prompt_id");
        }
        if frame["user_message_uuid"].is_null() {
            event.as_object_mut().expect("family event is an object").remove("user_message_uuid");
        }
        if event["user_message_uuids"].as_array().is_none_or(Vec::is_empty) {
            event.as_object_mut().expect("family event is an object").remove("user_message_uuids");
        }
        if frame["user_message_uuids_overflow"] != true {
            event.as_object_mut().expect("family event is an object").remove("input_links_truncated");
        }
        if frame["is_backgrounded"].as_bool().is_none() {
            event.as_object_mut().expect("family event is an object").remove("is_backgrounded");
        }
        if frame["task_patch_is_backgrounded"].as_bool().is_none() {
            event.as_object_mut().expect("family event is an object").remove("task_patch_is_backgrounded");
        }
        if frame["spawn_depth"].as_u64().filter(|depth| *depth <= 128).is_none() {
            event.as_object_mut().expect("family event is an object").remove("spawn_depth");
        }
        if frame["ambient"].as_bool().is_none() {
            event.as_object_mut().expect("family event is an object").remove("ambient");
        }
        if task_reason.is_none() {
            event.as_object_mut().expect("family event is an object").remove("task_reason");
        }
        if task_last_tool_name.is_none() {
            event.as_object_mut().expect("family event is an object").remove("task_last_tool_name");
        }
        if event["resource_links"].as_array().is_none_or(Vec::is_empty) {
            event.as_object_mut().expect("family event is an object").remove("resource_links");
        }
        if frame["resource_links_overflow"] != true {
            event.as_object_mut().expect("family event is an object").remove("resource_links_truncated");
        }

        self.family_event_count = self.family_event_count.saturating_add(1);
        if self.family_events.len() == MAX_FAMILY_EVENTS {
            self.family_events.remove(0);
            self.family_events_truncated = true;
        }
        self.family_events.push(event);
    }

    fn record_result(&mut self, frame: &Value, session_root: Option<&str>) {
        let Some(session) = safe_identity(&frame["session_id"]) else {
            return;
        };
        if session_root != Some(session.as_str())
            || self.init_session_id.as_deref() != Some(session.as_str())
        {
            return;
        }
        let Some(frame_id) = safe_identity(&frame["uuid"]) else {
            return;
        };
        let primary_id = safe_identity(&frame["user_message_uuid"]);
        let listed = frame["user_message_uuids"].as_array();
        let mut input_ids = Vec::new();
        if let Some(listed) = listed {
            for value in listed.iter().take(64) {
                if let Some(id) = safe_identity(value)
                    && !input_ids.contains(&id)
                {
                    input_ids.push(id);
                }
            }
        } else if let Some(id) = primary_id.as_ref() {
            input_ids.push(id.clone());
        }
        if input_ids.is_empty() {
            return;
        }
        let list_overflow = frame["user_message_uuids_overflow"] == true;
        let unique_link = if let Some(listed) = listed {
            !list_overflow
                && frame["user_message_uuids_count"].as_u64() == Some(1)
                && listed.len() == 1
                && listed.first().and_then(Value::as_str) == primary_id.as_deref()
        } else {
            primary_id.is_some()
        };
        let subtype = frame["subtype"].as_str();
        let is_error = frame["is_error"].as_bool();
        let terminal_status = match (subtype, is_error) {
            (Some("success"), Some(true)) => Some("failed"),
            (Some("success"), Some(false)) => Some("completed"),
            (Some("error_during_execution"), Some(_))
            | (Some("error_max_turns"), Some(_))
            | (Some("error_max_budget_usd"), Some(_))
            | (Some("error_max_structured_output_retries"), Some(_)) => Some("failed"),
            _ => None,
        };
        let metadata = json!({
            "native_session_id":session,
            "native_input_id":Value::Null,
            "user_message_uuid":primary_id,
            "user_message_uuids":input_ids,
            "correlation":if unique_link {"unique"} else {"ambiguous_multi_input"},
            "result_frame_uuid":frame_id,
            "result_index":frame["result_index"],
            "result_subtype":subtype,
            "terminal_status":terminal_status,
            "is_error":is_error,
            "stop_reason":frame["stop_reason"],
            "effective_model":self.init_model,
            "result_sha256":frame["result_sha256"],
            "result_bytes":frame["result_bytes"]
        });
        if let Some(ids) = metadata["user_message_uuids"].as_array() {
            for id in ids {
                let Some(id) = id.as_str() else { continue };
                let mut execution = metadata.clone();
                execution["native_input_id"] = json!(id);
                self.input_executions.push(execution);
            }
        }
        if self.input_executions.len() > MAX_INPUT_EXECUTIONS {
            let excess = self.input_executions.len() - MAX_INPUT_EXECUTIONS;
            self.input_executions.drain(0..excess);
        }
    }
}

fn safe_family_link(value: &Value) -> Option<String> {
    safe_identity(value).filter(|identity| identity.len() <= 256)
}

fn safe_family_uri(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|uri| {
            !uri.is_empty()
                && uri.len() <= 2_048
                && !uri.bytes().any(|byte| byte.is_ascii_control())
        })
        .map(ToOwned::to_owned)
}

fn safe_family_label(value: &Value) -> Option<String> {
    safe_identity(value).filter(|label| label.len() <= 128)
}

fn safe_family_text(value: &Value, maximum: usize) -> Option<String> {
    safe_identity(value).filter(|text| text.len() <= maximum)
}

fn safe_sdk_task_status(value: &Value) -> Option<&'static str> {
    match value.as_str()? {
        "completed" => Some("completed"),
        "failed" => Some("failed"),
        "stopped" => Some("stopped"),
        "pending" => Some("pending"),
        "running" => Some("running"),
        "killed" => Some("killed"),
        "paused" => Some("paused"),
        _ => None,
    }
}

fn safe_family_input_links(value: &Value) -> Option<Vec<String>> {
    let values = value.as_array()?;
    let links = values
        .iter()
        .take(16)
        .filter_map(safe_family_link)
        .collect::<Vec<_>>();
    (!links.is_empty()).then_some(links)
}

pub(crate) fn safe_family_resource_links(value: &Value) -> Option<Vec<Value>> {
    let values = value.as_array()?;
    let links = values
        .iter()
        .take(50)
        .filter_map(|value| {
            let object = value.as_object()?;
            let uri = safe_family_uri(object.get("uri")?)?;
            let name = safe_family_text(object.get("name")?, 256)?;
            let mut link = json!({"uri":uri,"name":name});
            if let Some(title) = object.get("title").and_then(|value| safe_family_text(value, 256)) {
                link["title"] = json!(title);
            }
            if let Some(mime_type) = object.get("mimeType").and_then(|value| safe_family_text(value, 128)) {
                link["mimeType"] = json!(mime_type);
            }
            if let Some(size) = object.get("size").and_then(Value::as_u64) {
                link["size"] = json!(size);
            }
            Some(link)
        })
        .collect::<Vec<_>>();
    (!links.is_empty()).then_some(links)
}

fn save_input_admission(
    config: &AdapterConfig,
    journal: &OperationJournal,
    operation_id: &str,
    saved: &crate::journal::OperationState,
    input_id: &str,
    session_id: &str,
    initial_dispatch: bool,
) -> Result<()> {
    let receipt = saved
        .receipt
        .as_ref()
        .ok_or_else(|| Error::new("ADAPTER_INTENT_MISSING", "SDK input intent has no receipt"))?;
    let intent = saved
        .intent
        .as_ref()
        .ok_or_else(|| Error::new("ADAPTER_INTENT_MISSING", "SDK input intent is missing"))?;
    if let Some(outcome) = saved.outcome.as_ref() {
        if outcome["details"]["module_receipt"] != serde_json::to_value(receipt)? {
            return Err(Error::new(
                "ADAPTER_INTENT_MISMATCH",
                "saved outcome receipt differs from the input intent",
            ));
        }
        if outcome["outcome"] != "applied"
            || outcome["native_root_id"] != session_id
            || outcome["native_input_id"] != input_id
        {
            return Err(Error::new(
                "ADAPTER_OUTCOME_CONFLICT",
                "saved operation outcome conflicts with the native input echo",
            ));
        }
        return Ok(());
    }
    let native = &intent["native"];
    let mut details = json!({
        "completion_condition":"native_input_admitted",
        "evidence":"native_frame_echo",
        "execution_complete":false,
        "initial_task_dispatch":initial_dispatch,
        "bridge_boot_id":intent["bridge_boot_id"],
        "native_frame_session_id":session_id,
        "system_init_session_id":if initial_dispatch { json!(session_id) } else { Value::Null },
        "native_scope_key":config.native_options.scope_key(),
        "user_message_uuid":input_id,
        "prompt_sha256":native["prompt_sha256"],
        "prompt_bytes":native["prompt_bytes"],
        "task_snapshot_sha256":native["task_snapshot_sha256"],
        "module_receipt":receipt
    });
    if let Some(value) = native.get("dispatch_admission") {
        let admission: TaskDispatchAdmissionReceipt =
            serde_json::from_value(value.clone()).map_err(|_| {
                Error::new(
                    "TASK_DISPATCH_ADMISSION_INVALID",
                    "saved normalized dispatch admission is malformed",
                )
            })?;
        admission.validate().map_err(|_| {
            Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "saved normalized dispatch admission is invalid",
            )
        })?;
        if admission.module_receipt != *receipt
            || admission.operation_id != operation_id
            || admission.native_input_id.as_deref() != Some(input_id)
        {
            return Err(Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "saved normalized dispatch admission does not match the echoed input",
            ));
        }
        details["dispatch_admission"] = serde_json::to_value(admission)?;
    }
    receipt::insert(&mut details, receipt)?;
    let outcome = RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some(config.native_options.scope_key()),
        native_root_id: Some(session_id.to_owned()),
        turn_id: None,
        native_input_id: Some(input_id.to_owned()),
        details,
    };
    journal.save_outcome(operation_id, &outcome)
}

fn safe_identity(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 512
                && !value.bytes().any(|byte| byte.is_ascii_control())
        })
        .map(ToOwned::to_owned)
}
