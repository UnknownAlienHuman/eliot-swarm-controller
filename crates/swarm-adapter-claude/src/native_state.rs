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
    runtime::{EffectOutcome, RuntimeOutcome},
};

const MAX_SEEN_FRAMES: usize = 8_192;
const MAX_INPUT_EXECUTIONS: usize = 128;
const MAX_PENDING_INPUTS: usize = 64;

#[derive(Default)]
pub struct NativeControl {
    pending_inputs: HashMap<String, String>,
    init_session_id: Option<String>,
    init_model: Option<String>,
    input_executions: Vec<Value>,
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
        let duplicate_frame = if let Some(frame_id) = safe_identity(&frame["uuid"]) {
            if self.seen_frames.contains(&frame_id) {
                true
            } else {
                self.seen_frames.insert(frame_id.clone());
                self.frame_order.push_back(frame_id);
                if self.frame_order.len() > MAX_SEEN_FRAMES
                    && let Some(oldest) = self.frame_order.pop_front()
                {
                    self.seen_frames.remove(&oldest);
                }
                false
            }
        } else {
            false
        };

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
        Ok(())
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
