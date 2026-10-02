//! Durable inbox -> serialized native busy-period evidence. Projected messages
//! and the volatile service feed never enter this state machine.
use super::{Options, Service, effects::delivered_matches, input_id, valid_id};
use crate::{
    error::{Error, Result},
    model,
    runtime::RuntimeCommand,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub(crate) const READER_REVISION: &str = "opencode-execution-log-v1";

fn enqueued_matches(item: &Value, command: &RuntimeCommand) -> Result<bool> {
    if item["type"] != "user" || item["delivery"] != "queue" || !item["payload"].is_object() {
        return Ok(false);
    }
    // Reuse the exact original prompt/metadata/attachment contract. The log
    // envelope supplies identity; the native inbox payload supplies content.
    let mut message = item["payload"].clone();
    message["id"] = json!(input_id(&command.operation_id));
    message["type"] = json!("user");
    message["sessionID"] = json!(command.native_root_id);
    delivered_matches(&message, command)
}

fn gap(code: &str) -> Error {
    Error::new(
        code,
        "native execution log does not establish the requested input's disposition",
    )
}
fn sequence(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or_else(|| gap("NATIVE_LOG_SEQUENCE"))
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventRef {
    id: String,
    seq: u64,
    sha256: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Terminal {
    event: EventRef,
    outcome: String,
    reason: Option<String>,
}

/// This is a bounded projection, not copied history. It is stored atomically
/// with derived producer evidence, and is scoped to the immutable request.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionScan {
    revision: String,
    request_sha256: String,
    session_id: String,
    /// Exclusive native cursor preceding `anchor`; never invent seq - 1.
    after: Option<u64>,
    anchor: Option<EventRef>,
    watermark: Option<u64>,
    creation: Option<EventRef>,
    active: Option<EventRef>,
    admission: Option<EventRef>,
    delivery: Option<EventRef>,
    run: Option<EventRef>,
    terminal: Option<Terminal>,
    uncertainty: Option<String>,
}

pub(crate) struct ExecutionRead {
    pub scan: ExecutionScan,
    pub synced: bool,
    pub gap: Option<&'static str>,
}

impl ExecutionScan {
    pub(crate) fn restore(command: &RuntimeCommand, saved: Option<&Value>) -> Result<Self> {
        let session = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| gap("NATIVE_ROOT_MISSING"))?;
        valid_id(session, "ses")?;
        if !matches!(command.method.as_str(), "task.dispatch" | "agent.send") {
            return Err(Error::invalid(
                "execution read requires an already-sent input",
            ));
        }
        let fingerprint = model::digest(model::canonical(&json!(command))?.as_bytes());
        if let Some(saved) = saved {
            let scan: Self = serde_json::from_value(saved.clone())
                .map_err(|_| gap("NATIVE_LOG_CHECKPOINT_SCHEMA"))?;
            if scan.revision != READER_REVISION
                || scan.request_sha256 != fingerprint
                || scan.session_id != session
                || (scan.after.is_some() && scan.anchor.is_none())
                || scan
                    .after
                    .zip(scan.anchor.as_ref().map(|a| a.seq))
                    .is_some_and(|(a, b)| a >= b)
                || scan.anchor.is_some() != scan.creation.is_some()
            {
                return Err(gap("NATIVE_LOG_CHECKPOINT_SCOPE"));
            }
            return Ok(scan);
        }
        Ok(Self {
            revision: READER_REVISION.into(),
            request_sha256: fingerprint,
            session_id: session.into(),
            after: None,
            anchor: None,
            watermark: None,
            creation: None,
            active: None,
            admission: None,
            delivery: None,
            run: None,
            terminal: None,
            uncertainty: None,
        })
    }
    pub(crate) fn after(&self) -> Option<u64> {
        self.after
    }
    pub(crate) fn anchor(&self) -> Option<&EventRef> {
        self.anchor.as_ref()
    }
    fn mark_uncertain(&mut self, code: &str) {
        if self.uncertainty.is_none() {
            self.uncertainty = Some(code.into());
        }
    }
    pub(crate) fn verify_anchor(&self, value: &Value) -> Result<()> {
        if self.anchor.as_ref() != Some(&self.envelope(value)?) {
            return Err(gap("NATIVE_LOG_ANCHOR_CHANGED"));
        }
        Ok(())
    }
    fn envelope(&self, value: &Value) -> Result<EventRef> {
        let id = model::text(value, "id").map_err(|_| gap("NATIVE_LOG_SCHEMA"))?;
        valid_id(id, "evt_").map_err(|_| gap("NATIVE_LOG_SCHEMA"))?;
        if value["durable"]["aggregateID"] != self.session_id
            || value["data"]["sessionID"] != self.session_id
            || value["created"]
                .as_f64()
                .is_none_or(|n| !n.is_finite() || n < 0.0)
            || value["durable"]["version"].as_u64().is_none_or(|n| n == 0)
        {
            return Err(gap("NATIVE_LOG_SCOPE"));
        }
        Ok(EventRef {
            id: id.into(),
            seq: sequence(&value["durable"]["seq"])?,
            sha256: model::digest(model::canonical(value)?.as_bytes()),
        })
    }
    pub(crate) fn consume(&mut self, value: &Value, command: &RuntimeCommand) -> Result<()> {
        let event = self.envelope(value)?;
        if self
            .anchor
            .as_ref()
            .is_some_and(|a| event.seq <= a.seq || event.id == a.id)
        {
            return Err(gap("NATIVE_LOG_ORDER"));
        }
        if self
            .watermark
            .is_some_and(|watermark| event.seq <= watermark)
        {
            return Err(gap("NATIVE_LOG_REPLAY_CHANGED"));
        }
        let kind = model::text(value, "type").map_err(|_| gap("NATIVE_LOG_SCHEMA"))?;
        let data = &value["data"];
        let version = value["durable"]["version"].as_u64().unwrap_or(0);
        if self.creation.is_none() {
            if kind != "session.created"
                || version != 1
                || !data["parentID"].is_null()
                || data["metadata"]["eliot"]["binding"] != command.binding_id
                || data["metadata"]["eliot"]["generation"] != command.generation
                || data["model"] != command.route["native_options"]["model"]
            {
                return Err(gap("NATIVE_LOG_ORIGIN"));
            }
            self.creation = Some(event.clone());
        } else if kind == "session.created" {
            return Err(gap("NATIVE_LOG_ORIGIN_CHANGED"));
        }
        if (kind.starts_with("session.execution.") || kind.starts_with("session.inbox."))
            && version != 1
        {
            return Err(gap("NATIVE_LOG_LIFECYCLE_VERSION"));
        }
        let input = input_id(&command.operation_id);
        match kind {
            "session.created" => {}
            "session.execution.started" => {
                if let Some(active) = &self.active
                    && self.run.as_ref() == Some(active)
                    && self.terminal.is_none()
                {
                    self.mark_uncertain("NATIVE_EXECUTION_RESTART_GAP");
                }
                self.active = Some(event.clone());
            }
            "session.inbox.enqueued" if data["inboxID"] == input => {
                if self.admission.is_some() {
                    self.mark_uncertain("NATIVE_INPUT_ID_REUSED");
                } else if enqueued_matches(&data["item"], command)? {
                    self.admission = Some(event.clone());
                } else {
                    return Err(gap("NATIVE_INPUT_MISMATCH"));
                }
            }
            "session.inbox.delivered" if data["inboxID"] == input => {
                if self.admission.is_none() {
                    return Err(gap("NATIVE_INPUT_ADMISSION_MISSING"));
                }
                if self.delivery.is_some() || self.terminal.is_some() {
                    self.mark_uncertain("NATIVE_INPUT_ID_REUSED");
                } else {
                    self.delivery = Some(event.clone());
                    self.run = self.active.clone();
                    if self.run.is_none() {
                        self.mark_uncertain("NATIVE_EXECUTION_START_MISSING");
                    }
                }
            }
            "session.inbox.cancelled" if data["inboxID"] == input => {
                if self.admission.is_none() {
                    return Err(gap("NATIVE_INPUT_ADMISSION_MISSING"));
                }
                if self.delivery.is_some() || self.terminal.is_some() {
                    self.mark_uncertain("NATIVE_INPUT_ID_REUSED");
                } else {
                    self.terminal = Some(Terminal {
                        event: event.clone(),
                        outcome: "cancelled".into(),
                        reason: Some("inbox_cancelled_before_delivery".into()),
                    });
                }
            }
            "session.execution.succeeded"
            | "session.execution.failed"
            | "session.execution.interrupted" => {
                let reason = if kind == "session.execution.interrupted" {
                    let reason = model::text(data, "reason")
                        .map_err(|_| gap("NATIVE_LOG_TERMINAL_SCHEMA"))?;
                    if !matches!(reason, "user" | "inactivity" | "shutdown" | "superseded") {
                        return Err(gap("NATIVE_LOG_TERMINAL_SCHEMA"));
                    }
                    Some(reason.to_owned())
                } else {
                    None
                };
                if kind == "session.execution.failed" && !data["error"].is_object() {
                    return Err(gap("NATIVE_LOG_TERMINAL_SCHEMA"));
                }
                if let Some(active) = &self.active
                    && self.run.as_ref() == Some(active)
                    && self.delivery.is_some()
                    && self.terminal.is_none()
                {
                    self.terminal = Some(Terminal {
                        event: event.clone(),
                        outcome: match kind {
                            "session.execution.succeeded" => "completed",
                            "session.execution.failed" => "failed",
                            _ => "cancelled",
                        }
                        .into(),
                        reason,
                    });
                }
                self.active = None;
            }
            "session.inbox.enqueued"
            | "session.inbox.delivered"
            | "session.inbox.cancelled"
            | "session.inbox.delivery.changed" => {}
            "session.forked"
            | "session.deleted"
            | "session.revert.staged"
            | "session.revert.cleared"
            | "session.revert.committed" => {
                if self.admission.is_some() {
                    self.mark_uncertain("NATIVE_INPUT_HISTORY_CHANGED");
                }
            }
            // These events neither start/settle a native busy period nor consume
            // inbox identity. Unknown lifecycle types affect this reader only.
            "session.agent.selected"
            | "session.model.selected"
            | "session.moved"
            | "session.renamed"
            | "session.metadata.updated"
            | "session.permissions"
            | "session.viewed"
            | "session.message.content.updated"
            | "session.usage.recorded"
            | "session.instructions.updated"
            | "session.synthetic"
            | "session.skill.activated"
            | "session.shell.started"
            | "session.shell.ended"
            | "session.step.started"
            | "session.step.streamed"
            | "session.step.ended"
            | "session.step.failed"
            | "session.text.started"
            | "session.text.ended"
            | "session.reasoning.started"
            | "session.reasoning.ended"
            | "session.tool.input.started"
            | "session.tool.input.ended"
            | "session.tool.called"
            | "session.tool.success"
            | "session.tool.failed"
            | "session.retry.scheduled"
            | "session.compaction.started"
            | "session.compaction.ended"
            | "session.compaction.failed" => {}
            _ => return Err(gap("NATIVE_LOG_EVENT_UNSUPPORTED")),
        }
        self.after = self.anchor.as_ref().map(|a| a.seq);
        self.anchor = Some(event);
        Ok(())
    }
    pub(crate) fn synchronize(&mut self, value: &Value) -> Result<()> {
        if value["type"] != "log.synced" || value["aggregateID"] != self.session_id {
            return Err(gap("NATIVE_LOG_WATERMARK_SCOPE"));
        }
        let watermark = value.get("seq").map(sequence).transpose()?;
        if self
            .anchor
            .as_ref()
            .is_some_and(|a| watermark.is_none_or(|n| n < a.seq))
            || self
                .watermark
                .is_some_and(|old| watermark.is_none_or(|n| n < old))
        {
            return Err(gap("NATIVE_LOG_WATERMARK_REGRESSED"));
        }
        self.watermark = watermark;
        Ok(())
    }
    pub(crate) fn proof(&self, command: &RuntimeCommand) -> Option<Value> {
        let admission = self.admission.as_ref()?;
        let disposition = if self.uncertainty.is_some() {
            "unknown"
        } else if let Some(terminal) = &self.terminal {
            if matches!(terminal.reason.as_deref(), Some("shutdown" | "superseded")) {
                "recovery_pending"
            } else {
                &terminal.outcome
            }
        } else if self.run.is_some() {
            "running"
        } else {
            "queued"
        };
        Some(
            json!({"reader_revision":READER_REVISION,"operation_id":command.operation_id,
            "native_session_id":self.session_id,"native_input_id":input_id(&command.operation_id),
            "native_run_id":self.run.as_ref().map(|r| &r.id),
            "native_run_id_kind":if self.run.is_some(){Some("execution_started_event")}else{None},
            "admission":admission,"delivery":self.delivery,"execution_started":self.run,
            "terminal":self.terminal,"disposition":disposition,"uncertainty":self.uncertainty,
            "log_watermark":self.watermark,"correlation":"durable_serialized_execution",
            "family_complete":false}),
        )
    }
}

impl Service {
    pub(crate) async fn read_execution(
        &self,
        command: &RuntimeCommand,
        options: &Options,
        saved: Option<&Value>,
    ) -> Result<ExecutionRead> {
        let scan = ExecutionScan::restore(command, saved)?;
        let root = &scan.session_id;
        self.verify().await?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let session = self.session(root).await?;
        if !session["fork"].is_null() || !session["revert"].is_null() {
            return Err(gap("NATIVE_INPUT_HISTORY_CHANGED"));
        }
        let read = self.execution_log(command, scan).await?;
        self.verify_binding(
            &read.scan.session_id,
            options,
            &command.binding_id,
            command.generation,
        )
        .await?;
        let after = self.session(&read.scan.session_id).await?;
        if after["projectID"] != session["projectID"]
            || after["metadata"]["eliot"] != session["metadata"]["eliot"]
            || !after["revert"].is_null()
        {
            return Err(gap("NATIVE_LOG_SCOPE_CHANGED"));
        }
        self.verify().await?;
        Ok(read)
    }
}
