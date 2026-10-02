//! Durable inbox -> serialized native busy-period evidence. Projected messages
//! and the volatile service feed never enter this state machine.
//!
//! Correlation is always through one exact input, named by a
//! [`NativeInputDescriptor`]: the exact native input ID, the SHA-256 digest of
//! the exact prompt text and the exact `eliot` marker the input carried. There
//! is deliberately no text or time-window matching anywhere in this reader.
//! Execution start correlates through the input's own `inbox.delivered` event:
//! delivery binds the `session.execution.started` event that is active at
//! delivery time, and the terminal of that serialized busy period belongs to
//! the same run. A busy period started by an earlier input may therefore be
//! shared by several inputs delivered while it is active; sharing is an
//! expected property of the serialized native model, not an ambiguity.
use super::{
    Options, Service,
    effects::{marker, no_attachments, prompt},
    input_id, valid_id,
};
use crate::{
    error::{Error, Result},
    model,
    runtime::RuntimeCommand,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub(crate) const READER_REVISION: &str = "opencode-execution-log-v1";

/// Typed identity of the one native input an [`ExecutionScan`] correlates.
///
/// Every fact here is controller-known before the input is sent: the exact
/// input ID derived from the Operation, the digest of the exact prompt text,
/// the exact marker object placed in the input metadata, and the session
/// origin facts (binding, generation, creation model) the log's
/// `session.created` event must match. Two different Operations on the same
/// session produce different descriptors; no descriptor can match another
/// input's lifecycle events.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInputDescriptor {
    session_id: String,
    input_id: String,
    prompt_sha256: String,
    marker: Value,
    binding_id: String,
    generation: i64,
    model: Value,
}

impl NativeInputDescriptor {
    /// Descriptor for a `task.dispatch` / `agent.send` input: the prompt text
    /// and marker are exactly the ones `effects` put on the wire.
    pub(crate) fn from_command(command: &RuntimeCommand) -> Result<Self> {
        let session_id = command
            .native_root_id
            .clone()
            .ok_or_else(|| gap("NATIVE_ROOT_MISSING"))?;
        valid_id(&session_id, "ses")?;
        Ok(Self {
            session_id,
            input_id: input_id(&command.operation_id),
            prompt_sha256: model::digest(prompt(command)?.as_bytes()),
            marker: marker(command),
            binding_id: command.binding_id.clone(),
            generation: command.generation,
            model: command.route["native_options"]["model"].clone(),
        })
    }

    /// Descriptor for a goal activation input. The goal's prompt text and
    /// marker are record-derived rather than command-derived, so the caller
    /// supplies exactly the text and marker it placed (or would have placed)
    /// on the activation input for this goal Operation.
    pub(crate) fn for_goal_activation(
        command: &RuntimeCommand,
        prompt_text: &str,
        marker: Value,
    ) -> Result<Self> {
        let session_id = command
            .native_root_id
            .clone()
            .ok_or_else(|| gap("NATIVE_ROOT_MISSING"))?;
        valid_id(&session_id, "ses")?;
        Ok(Self {
            session_id,
            input_id: input_id(&command.operation_id),
            prompt_sha256: model::digest(prompt_text.as_bytes()),
            marker,
            binding_id: command.binding_id.clone(),
            generation: command.generation,
            model: command.route["native_options"]["model"].clone(),
        })
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn input_id(&self) -> &str {
        &self.input_id
    }

    fn fingerprint(&self) -> Result<String> {
        Ok(model::digest(
            model::canonical(&serde_json::to_value(self)?)?.as_bytes(),
        ))
    }
}

fn enqueued_matches(item: &Value, descriptor: &NativeInputDescriptor) -> bool {
    if item["type"] != "user" || item["delivery"] != "queue" || !item["payload"].is_object() {
        return false;
    }
    // The exact original prompt/marker/attachment contract, checked against
    // the descriptor: the payload text must hash to the descriptor's prompt
    // digest and the payload marker must equal the descriptor's marker. The
    // log envelope supplies identity; the native inbox payload supplies content.
    let payload = &item["payload"];
    payload["text"]
        .as_str()
        .is_some_and(|text| model::digest(text.as_bytes()) == descriptor.prompt_sha256)
        && payload["metadata"]["eliot"] == descriptor.marker
        && no_attachments(payload)
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
fn envelope_for(value: &Value, session_id: &str) -> Result<EventRef> {
    let id = model::text(value, "id").map_err(|_| gap("NATIVE_LOG_SCHEMA"))?;
    valid_id(id, "evt_").map_err(|_| gap("NATIVE_LOG_SCHEMA"))?;
    if value["durable"]["aggregateID"] != session_id
        || value["data"]["sessionID"] != session_id
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
fn watermark_for(
    value: &Value,
    session_id: &str,
    anchor: &Option<EventRef>,
    watermark: &Option<u64>,
) -> Result<Option<u64>> {
    if value["type"] != "log.synced" || value["aggregateID"] != session_id {
        return Err(gap("NATIVE_LOG_WATERMARK_SCOPE"));
    }
    let next = value.get("seq").map(sequence).transpose()?;
    if anchor
        .as_ref()
        .is_some_and(|a| next.is_none_or(|n| n < a.seq))
        || watermark.is_some_and(|old| next.is_none_or(|n| n < old))
    {
        return Err(gap("NATIVE_LOG_WATERMARK_REGRESSED"));
    }
    Ok(next)
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
        if !matches!(command.method.as_str(), "task.dispatch" | "agent.send") {
            return Err(Error::invalid(
                "execution read requires an already-sent input",
            ));
        }
        let session = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| gap("NATIVE_ROOT_MISSING"))?;
        valid_id(session, "ses")?;
        let fingerprint = model::digest(model::canonical(&json!(command))?.as_bytes());
        Self::restore_for(session, fingerprint, saved)
    }

    /// A fresh, uncheckpointed scan for a goal activation input. Goal evidence
    /// is derived at reconciliation time only; nothing here is persisted, so
    /// there is no saved checkpoint to validate. The scan is fingerprinted by
    /// its descriptor so a scan can never be replayed against another input.
    pub(crate) fn for_goal(descriptor: &NativeInputDescriptor) -> Result<Self> {
        Self::restore_for(descriptor.session_id(), descriptor.fingerprint()?, None)
    }

    fn restore_for(session: &str, fingerprint: String, saved: Option<&Value>) -> Result<Self> {
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
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
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
        envelope_for(value, &self.session_id)
    }
    pub(crate) fn consume(
        &mut self,
        value: &Value,
        descriptor: &NativeInputDescriptor,
    ) -> Result<()> {
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
                || data["metadata"]["eliot"]["binding"] != descriptor.binding_id
                || data["metadata"]["eliot"]["generation"] != descriptor.generation
                || data["model"] != descriptor.model
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
        let input = descriptor.input_id.as_str();
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
                } else if enqueued_matches(&data["item"], descriptor) {
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
        self.watermark = watermark_for(value, &self.session_id, &self.anchor, &self.watermark)?;
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

    /// The exact input was admitted: its `session.inbox.enqueued` event
    /// matched the descriptor. Admission proves the input exists natively;
    /// it says nothing about delivery or model execution.
    pub(crate) fn input_admitted(&self) -> bool {
        self.admission.is_some()
    }

    /// The `session.execution.started` event this input's delivery correlated
    /// to, as a serialized event ref — the only execution-start evidence this
    /// reader ever reports. `None` both when no start is proven and when the
    /// scan is uncertain (for example delivery with no active start,
    /// `NATIVE_EXECUTION_START_MISSING`): uncertainty is never reported as a
    /// start. A start proven for the busy period may be shared with other
    /// inputs delivered during the same period.
    pub(crate) fn execution_started(&self) -> Option<Value> {
        if self.uncertainty.is_some() {
            return None;
        }
        self.run
            .as_ref()
            .and_then(|run| serde_json::to_value(run).ok())
    }
}

pub(crate) const SESSION_READER_REVISION: &str = "opencode-session-log-v1";
/// Busy periods retained per child session. The open period is never dropped;
/// older terminal periods fall off first so the checkpoint stays bounded.
const MAX_SESSION_PERIODS: usize = 16;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SessionPeriod {
    started: EventRef,
    terminal: Option<Terminal>,
    /// An interrupted period whose native fate after a service shutdown is
    /// unproven. It is not a terminal and never discharges a producer.
    recovery_reason: Option<String>,
}
impl SessionPeriod {
    fn disposition(&self) -> &str {
        if let Some(terminal) = &self.terminal {
            &terminal.outcome
        } else if self.recovery_reason.is_some() {
            "recovery_pending"
        } else {
            "running"
        }
    }
    fn turn(&self, session_id: &str) -> Value {
        let (event, cursor) = match &self.terminal {
            Some(terminal) => (json!(terminal.event), terminal.event.seq),
            None => (json!(self.started), self.started.seq),
        };
        json!({"sessionId":session_id,"turnId":self.started.id,"event":event,
        "terminal":self.terminal.as_ref().map(|t| t.outcome.clone()),
        "viewCursor":cursor,"disposition":self.disposition(),
        "native_run_id_kind":"execution_started_event",
        "reader_revision":SESSION_READER_REVISION})
    }
}

/// Per-session busy-period evidence from the same durable log contract the
/// root input reader uses. There is no controller input in a natively spawned
/// child session, so nothing here matches inbox payloads: the addressed facts
/// are the session's own execution periods, keyed by their started event.
/// A period closes only from its own terminal event; absence, idle and
/// enumeration state never enter this state machine.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionScan {
    revision: String,
    session_id: String,
    parent_id: String,
    after: Option<u64>,
    anchor: Option<EventRef>,
    watermark: Option<u64>,
    creation: Option<EventRef>,
    periods: Vec<SessionPeriod>,
    uncertainty: Option<String>,
}

pub(crate) struct SessionRead {
    pub scan: SessionScan,
    pub synced: bool,
    pub gap: Option<&'static str>,
}

impl SessionScan {
    pub(crate) fn restore(
        session_id: &str,
        parent_id: &str,
        saved: Option<&Value>,
    ) -> Result<Self> {
        valid_id(session_id, "ses")?;
        valid_id(parent_id, "ses")?;
        if let Some(saved) = saved {
            let scan: Self = serde_json::from_value(saved.clone())
                .map_err(|_| gap("NATIVE_LOG_CHECKPOINT_SCHEMA"))?;
            if scan.revision != SESSION_READER_REVISION
                || scan.session_id != session_id
                || scan.parent_id != parent_id
                || (scan.after.is_some() && scan.anchor.is_none())
                || scan
                    .after
                    .zip(scan.anchor.as_ref().map(|a| a.seq))
                    .is_some_and(|(a, b)| a >= b)
                || scan.anchor.is_some() != scan.creation.is_some()
                || scan.periods.len() > MAX_SESSION_PERIODS
            {
                return Err(gap("NATIVE_LOG_CHECKPOINT_SCOPE"));
            }
            return Ok(scan);
        }
        Ok(Self {
            revision: SESSION_READER_REVISION.into(),
            session_id: session_id.into(),
            parent_id: parent_id.into(),
            after: None,
            anchor: None,
            watermark: None,
            creation: None,
            periods: Vec::new(),
            uncertainty: None,
        })
    }
    pub(crate) fn after(&self) -> Option<u64> {
        self.after
    }
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
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
        if self.anchor.as_ref() != Some(&envelope_for(value, &self.session_id)?) {
            return Err(gap("NATIVE_LOG_ANCHOR_CHANGED"));
        }
        Ok(())
    }
    pub(crate) fn consume(&mut self, value: &Value) -> Result<()> {
        let event = envelope_for(value, &self.session_id)?;
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
            // Family membership was established by native parent enumeration;
            // the log must still open with this exact child's own creation.
            if kind != "session.created" || version != 1 || data["parentID"] != self.parent_id {
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
        match kind {
            "session.created" => {}
            "session.execution.started" => {
                if self.periods.last().is_some_and(|p| p.terminal.is_none()) {
                    self.mark_uncertain("NATIVE_EXECUTION_RESTART_GAP");
                }
                self.periods.push(SessionPeriod {
                    started: event.clone(),
                    terminal: None,
                    recovery_reason: None,
                });
                while self.periods.len() > MAX_SESSION_PERIODS {
                    let Some(oldest_terminal) =
                        self.periods.iter().position(|p| p.terminal.is_some())
                    else {
                        break;
                    };
                    self.periods.remove(oldest_terminal);
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
                if let Some(period) = self
                    .periods
                    .last_mut()
                    .filter(|p| p.terminal.is_none() && p.recovery_reason.is_none())
                {
                    if matches!(reason.as_deref(), Some("shutdown" | "superseded")) {
                        period.recovery_reason = reason;
                    } else {
                        period.terminal = Some(Terminal {
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
                }
            }
            // Native inputs inside a child are the parent agent's own work;
            // they neither start nor settle this session's busy periods here.
            "session.inbox.enqueued"
            | "session.inbox.delivered"
            | "session.inbox.cancelled"
            | "session.inbox.delivery.changed" => {}
            "session.forked"
            | "session.deleted"
            | "session.revert.staged"
            | "session.revert.cleared"
            | "session.revert.committed" => {
                self.mark_uncertain("NATIVE_INPUT_HISTORY_CHANGED");
            }
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
        self.watermark = watermark_for(value, &self.session_id, &self.anchor, &self.watermark)?;
        Ok(())
    }
    /// Session-level disposition. Evidence about periods, never about the
    /// family: `family_complete` stays false at every consumer.
    pub(crate) fn disposition(&self) -> &str {
        if self.uncertainty.is_some() {
            return "unknown";
        }
        match self.periods.last() {
            None => "not_observed",
            Some(period) => period.disposition(),
        }
    }
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.disposition(), "completed" | "failed" | "cancelled")
    }
    pub(crate) fn turns(&self) -> Vec<Value> {
        if self.uncertainty.is_some() {
            return Vec::new();
        }
        self.periods
            .iter()
            .map(|p| p.turn(&self.session_id))
            .collect()
    }
    pub(crate) fn last_turn(&self) -> Option<Value> {
        if self.uncertainty.is_some() {
            return None;
        }
        self.periods.last().map(|p| p.turn(&self.session_id))
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
        let descriptor = NativeInputDescriptor::from_command(command)?;
        let read = self.execution_log(&descriptor, scan).await?;
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

    /// Reads one enumerated child's durable log. Membership is the caller's
    /// parent enumeration; this read only binds the log to the exact child
    /// session, its recorded parent and the root's project.
    pub(crate) async fn read_session_execution(
        &self,
        session_id: &str,
        parent_id: &str,
        project_id: &str,
        saved: Option<&Value>,
    ) -> Result<SessionRead> {
        let scan = SessionScan::restore(session_id, parent_id, saved)?;
        let session = self.session(session_id).await?;
        if session["parentID"] != parent_id
            || session["projectID"] != project_id
            || !session["fork"].is_null()
            || !session["revert"].is_null()
        {
            return Err(gap("NATIVE_CHILD_SCOPE"));
        }
        let read = self.session_log(scan).await?;
        let after = self.session(&read.scan.session_id).await?;
        if after["parentID"] != parent_id
            || after["projectID"] != project_id
            || !after["revert"].is_null()
        {
            return Err(gap("NATIVE_LOG_SCOPE_CHANGED"));
        }
        Ok(read)
    }
}
