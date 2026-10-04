//! Controller-recorded goal for OpenCode V2.
//!
//! OpenCode has no native goal API (reviewed at `4c0d0ff4`; the local runtime
//! matrix records `coverage.goal` as "No durable goal API established"). The
//! goal therefore lives in the native durable instruction-entry backend under
//! the controller-owned `eliot.goal` key, and activation reuses the existing
//! single prompt admission. Explicit manager-enabled Goal progression may
//! issue one continuation after an authenticated completed terminal EventRef;
//! it does not create a hidden model loop, a native Goal, or Task acceptance. The goal
//! result keeps admission and execution start as separate facts: an admitted
//! activation input never by itself proves the model started; start is proven
//! only by the durable execution log (see `execution.rs`). A lost
//! mutation response reconciles by GET/readback only; no PUT, DELETE or
//! prompt is ever replayed.
use super::{
    ExecutionScan, NativeInputDescriptor, Options, Service,
    configuration::{digest_json, owned_projection, projection_revision},
    effects::{failed, marker, no_attachments, outcome},
    http::{Data, decode},
    input_id,
};
use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use serde_json::{Value, json};

pub(crate) const GOAL_CONTRACT_REVISION: &str = "opencode-goal-v1";
pub(crate) const GOAL_SETTINGS_REVISION_KIND: &str = "opencode_session_goal_v1";
const GOAL_ENTRY_KEY: &str = "eliot.goal";
/// Objective bound chosen well below the native 256 KiB entry limit; the
/// Muse path's only objective bound is the IPC frame, so this adapter owns
/// an explicit one.
const MAX_OBJECTIVE_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum GoalAction {
    Set,
    Edit,
    Pause,
    Resume,
    Clear,
    Continue,
}

impl GoalAction {
    fn name(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Edit => "edit",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Clear => "clear",
            Self::Continue => "continue",
        }
    }
}

struct GoalRequest {
    action: GoalAction,
    objective: Option<String>,
    expected_revision: Option<u64>,
}

impl GoalRequest {
    /// Mirror Store validation as defense in depth. Continue is an explicit
    /// one-input activation of the exact currently active controller Goal.
    fn parse(input: &Value) -> Result<Self> {
        let action = match model::text(input, "action")? {
            "set" => GoalAction::Set,
            "edit" => GoalAction::Edit,
            "pause" => GoalAction::Pause,
            "resume" => GoalAction::Resume,
            "clear" => GoalAction::Clear,
            "continue" => GoalAction::Continue,
            _ => {
                return Err(Error::invalid(
                    "goal action must be set/edit/pause/resume/clear/continue",
                ));
            }
        };
        let expected_revision = if action == GoalAction::Continue {
            Some(input.get("expected_revision").and_then(Value::as_u64).ok_or_else(|| {
                Error::invalid("continue requires a nonnegative expected_revision (0 means no native Goal exists)")
            })?)
        } else {
            if input.get("expected_revision").is_some() {
                return Err(Error::invalid(
                    "expected_revision is only valid for continue",
                ));
            }
            None
        };
        let objective = match action {
            GoalAction::Set | GoalAction::Edit | GoalAction::Continue => {
                let objective = model::text(input, "objective")?;
                if objective.len() > MAX_OBJECTIVE_BYTES {
                    return Err(Error::invalid(
                        "goal objective exceeds the 32 KiB controller boundary",
                    ));
                }
                Some(objective.to_owned())
            }
            _ => {
                if input.get("objective").is_some() {
                    return Err(Error::invalid(
                        "objective is only valid for set/edit/continue",
                    ));
                }
                None
            }
        };
        Ok(Self {
            action,
            objective,
            expected_revision,
        })
    }
}
#[derive(Clone, PartialEq, Eq)]
struct GoalRecord {
    objective: String,
    status: String,
    revision: u64,
    operation_id: String,
}

impl GoalRecord {
    fn value(&self) -> Value {
        json!({
            "objective":self.objective,
            "status":self.status,
            "revision":self.revision,
            "updated_by_operation_id":self.operation_id
        })
    }

    /// Strictly parse a native entry value. A foreign or damaged record is a
    /// schema gap, never a guessed goal.
    fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &["objective", "status", "revision", "updated_by_operation_id"],
        )
        .map_err(|_| Error::new("NATIVE_GOAL_SCHEMA", "native goal record is invalid"))?;
        let invalid = || Error::new("NATIVE_GOAL_SCHEMA", "native goal record is invalid");
        let objective = model::text(value, "objective").map_err(|_| invalid())?;
        if objective.len() > MAX_OBJECTIVE_BYTES {
            return Err(invalid());
        }
        let status = model::text(value, "status").map_err(|_| invalid())?;
        if !matches!(status, "active" | "paused") {
            return Err(invalid());
        }
        let revision = value["revision"]
            .as_u64()
            .filter(|n| *n >= 1)
            .ok_or(invalid())?;
        let operation_id = model::text(value, "updated_by_operation_id").map_err(|_| invalid())?;
        if operation_id.len() > 128 {
            return Err(invalid());
        }
        Ok(Self {
            objective: objective.to_owned(),
            status: status.to_owned(),
            revision,
            operation_id: operation_id.to_owned(),
        })
    }

    fn objective_digest(&self) -> Result<String> {
        digest_json(&json!(self.objective))
    }
}

struct GoalReadback {
    record: Option<GoalRecord>,
    entries_revision: String,
    settings_revision: String,
}

fn absent() -> Error {
    Error::new(
        "NATIVE_GOAL_ABSENT",
        "no controller-recorded goal exists on this native session",
    )
}

fn unresolved(message: &str) -> Error {
    Error::new("NATIVE_GOAL_UNRESOLVED", message.to_owned())
}

/// The activation prompt is honest about what it is: a controller record
/// delivered to the model, not a native goal admission.
fn goal_prompt_text(record: &GoalRecord) -> String {
    format!(
        "ELIOT controller-recorded goal (revision {}): {}\n\nThis objective is recorded by the controller in this session's durable \"eliot.goal\" instruction entry. OpenCode has no native goal API: this prompt is the goal's activation, not a native goal admission and not Task acceptance.",
        record.revision, record.objective
    )
}

fn goal_marker(command: &RuntimeCommand, record: &GoalRecord) -> Value {
    let mut value = marker(command);
    value["goal_revision"] = json!(record.revision);
    value["goal_operation"] = json!(command.operation_id);
    value
}

fn goal_inbox_matches(item: &Value, root: &str, id: &str, text: &str, marker: &Value) -> bool {
    item["id"] == id
        && item["sessionID"] == root
        && item["type"] == "user"
        && item["delivery"] == "queue"
        && item["payload"]["text"] == text
        && item["payload"]["metadata"]["eliot"] == *marker
        && no_attachments(&item["payload"])
}

/// The split activation facts of one goal Operation. Admission and execution
/// start are different native facts and are never conflated: an admitted
/// activation input proves the input exists (inbox/message readback or the
/// durable log's exact `session.inbox.enqueued` event), while execution start
/// is proven only by the durable execution log correlating the input's
/// `inbox.delivered` to an active `session.execution.started` event. Neither
/// fact is Task acceptance or Goal completion. For `continue`, exact input
/// admission is the Operation's completion boundary; ordinary record actions
/// retain their `native_goal_recorded` boundary.
#[derive(Default)]
struct GoalActivation {
    input_id: Option<String>,
    admitted: bool,
    execution_started: bool,
    execution_ref: Option<Value>,
}

fn goal_details(
    action: GoalAction,
    record: Option<&GoalRecord>,
    readback: &GoalReadback,
    evidence: &str,
    mutation_sent: bool,
    activation: &GoalActivation,
) -> Result<Value> {
    let objective_digest = record.map(GoalRecord::objective_digest).transpose()?;
    Ok(json!({
        "completion_condition":if action == GoalAction::Continue { "native_input_admitted" } else { "native_goal_recorded" },
        "goal":{
            "action":action.name(),
            "present":record.is_some(),
            "status":record.map(|record| record.status.as_str()),
            "revision":record.map(|record| record.revision),
            "objective_digest":objective_digest,
            "entries_revision":readback.entries_revision,
            "settings_revision":readback.settings_revision,
            "settings_revision_kind":GOAL_SETTINGS_REVISION_KIND,
            "application_scope":"session",
            "continuation_owner":"controller_record",
            "native_goal_api":false,
            "record_applied":action != GoalAction::Continue || mutation_sent,
            "activation_input_id":activation.input_id,
            "activation_admitted":activation.admitted,
            "activation_execution_started":activation.execution_started,
            "activation_execution_ref":activation.execution_ref,
            "mutation_sent":mutation_sent,
            "evidence":evidence,
            "contract_revision":GOAL_CONTRACT_REVISION,
            "replay_policy":"readback_only_no_mutation_replay"
        }
    }))
}

enum GoalDecision {
    /// The exact desired state is already recorded; no mutation, no prompt.
    Noop,
    /// Write this record; activation admits one prompt afterwards.
    Write(GoalRecord, bool),
    /// Admit one prompt without mutating an exact active record.
    Activate(GoalRecord),
    /// Delete the entry; clear never activates.
    Remove,
}

fn decide(
    request: &GoalRequest,
    before: Option<&GoalRecord>,
    operation_id: &str,
) -> Result<GoalDecision> {
    let next_revision = before.map_or(1, |record| record.revision.saturating_add(1));
    let write = |objective: String, status: &str, activate: bool| {
        GoalDecision::Write(
            GoalRecord {
                objective,
                status: status.to_owned(),
                revision: next_revision,
                operation_id: operation_id.to_owned(),
            },
            activate,
        )
    };
    match request.action {
        GoalAction::Set => {
            let objective = request.objective.clone().unwrap_or_default();
            if before
                .is_some_and(|record| record.objective == objective && record.status == "active")
            {
                return Ok(GoalDecision::Noop);
            }
            Ok(write(objective, "active", true))
        }
        GoalAction::Edit => {
            let before = before.ok_or_else(absent)?;
            let objective = request.objective.clone().unwrap_or_default();
            if before.objective == objective {
                return Ok(GoalDecision::Noop);
            }
            let activate = before.status == "active";
            Ok(write(objective, &before.status, activate))
        }
        GoalAction::Pause => {
            let before = before.ok_or_else(absent)?;
            if before.status == "paused" {
                return Ok(GoalDecision::Noop);
            }
            Ok(write(before.objective.clone(), "paused", false))
        }
        GoalAction::Resume => {
            let before = before.ok_or_else(absent)?;
            if before.status == "active" {
                return Ok(GoalDecision::Noop);
            }
            Ok(write(before.objective.clone(), "active", true))
        }
        GoalAction::Clear => Ok(if before.is_none() {
            GoalDecision::Noop
        } else {
            GoalDecision::Remove
        }),
        GoalAction::Continue => {
            let objective = request
                .objective
                .as_deref()
                .ok_or_else(|| Error::invalid("continue objective is required"))?;
            match (request.expected_revision, before) {
                (Some(0), None) => Ok(write(objective.to_owned(), "active", true)),
                (Some(expected), Some(record))
                    if expected == record.revision
                        && record.status == "active"
                        && record.objective == objective =>
                {
                    Ok(GoalDecision::Activate(record.clone()))
                }
                _ => Err(Error::new(
                    "NATIVE_GOAL_CONFLICT",
                    "continue expected an absent Goal or the exact active revision and objective",
                )),
            }
        }
    }
}
/// Reconstruct the exact deterministic activation input for the persistent
/// terminal reader. The original request commits both objective and native
/// revision, so no mutable native readback is needed to derive its descriptor.
pub(super) fn continuation_execution_descriptor(
    command: &RuntimeCommand,
) -> Result<NativeInputDescriptor> {
    let request = GoalRequest::parse(&command.input)?;
    if request.action != GoalAction::Continue {
        return Err(Error::invalid(
            "terminal execution scan requires agent.goal continue",
        ));
    }
    let revision = match request.expected_revision {
        Some(0) => 1,
        Some(revision) => revision,
        None => return Err(Error::invalid("continue expected_revision is missing")),
    };
    let record = GoalRecord {
        objective: request
            .objective
            .ok_or_else(|| Error::invalid("continue objective is missing"))?,
        status: "active".to_owned(),
        revision,
        operation_id: command.operation_id.clone(),
    };
    NativeInputDescriptor::for_goal_activation(
        command,
        &goal_prompt_text(&record),
        goal_marker(command, &record),
    )
}
impl Service {
    async fn goal_readback(
        &self,
        root: &str,
        options: &Options,
        command: &RuntimeCommand,
    ) -> Result<GoalReadback> {
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let entries = self.instruction_entries(root).await?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let record = entries
            .iter()
            .find(|entry| entry["key"] == GOAL_ENTRY_KEY)
            .map(|entry| GoalRecord::parse(&entry["value"]))
            .transpose()?;
        let canonical = model::canonical(&Value::Array(entries.clone()))?;
        Ok(GoalReadback {
            record,
            entries_revision: format!("sha256:{}", model::digest(canonical.as_bytes())),
            settings_revision: projection_revision(&owned_projection(&entries)?)?,
        })
    }

    async fn admit_goal_activation(
        &self,
        root: &str,
        command: &RuntimeCommand,
        record: &GoalRecord,
    ) -> Result<String> {
        let id = input_id(&command.operation_id);
        let text = goal_prompt_text(record);
        let marker = goal_marker(command, record);
        let body = json!({"id":id,"text":text,"metadata":{"eliot":marker},"delivery":"queue","resume":true});
        let reply = self
            .post(&format!("/api/session/{root}/prompt"), body)
            .await;
        match reply.and_then(decode::<Data<Value>>) {
            Ok(response) if goal_inbox_matches(&response.data, root, &id, &text, &marker) => Ok(id),
            Ok(_) => Err(Error::new(
                "NATIVE_SCHEMA_ERROR",
                "native goal activation receipt failed identity/content verification",
            )),
            Err(error) => Err(error),
        }
    }

    /// Best-effort activation lookup for reconciliation. For ordinary Goal
    /// edits, the record itself remains the completion condition. For an
    /// explicit `continue`, the admitted activation input is the effect and
    /// must be observed before the Operation can settle.
    async fn goal_activation_observed(
        &self,
        root: &str,
        command: &RuntimeCommand,
        record: &GoalRecord,
    ) -> Option<String> {
        let id = input_id(&command.operation_id);
        let text = goal_prompt_text(record);
        let marker = goal_marker(command, record);
        if let Ok(inbox) = self
            .get(&format!("/api/session/{root}/inbox"), &[])
            .await
            .and_then(decode::<Data<Vec<Value>>>)
            && inbox
                .data
                .iter()
                .any(|item| goal_inbox_matches(item, root, &id, &text, &marker))
        {
            return Some(id);
        }
        let message: Data<Value> = self
            .get(&format!("/api/session/{root}/message/{id}"), &[])
            .await
            .and_then(decode)
            .ok()?;
        let delivered = message.data["id"] == id
            && message.data["type"] == "user"
            && message
                .data
                .get("sessionID")
                .is_none_or(|session| session.as_str() == Some(root))
            && message.data["text"] == text
            && message.data["metadata"]["eliot"] == marker
            && no_attachments(&message.data);
        delivered.then_some(id)
    }

    /// Activation evidence for reconciliation: the exact input's admission
    /// from inbox/message readback or from the durable log, and — from the
    /// log only — whether that input's delivery correlated to an active
    /// `session.execution.started`. Strictly best-effort and additive: the
    /// record readback has already settled ordinary record actions at
    /// `native_goal_recorded`. A `continue` Operation is settled only after
    /// its exact input admission is observed. Any later log failure, gap or
    /// uncertainty leaves execution-start facts unproven; this path never
    /// replays a mutation or prompt.
    async fn goal_activation_evidence(
        &self,
        root: &str,
        command: &RuntimeCommand,
        record: &GoalRecord,
    ) -> GoalActivation {
        let mut activation = GoalActivation::default();
        if let Some(id) = self.goal_activation_observed(root, command, record).await {
            activation.input_id = Some(id);
            activation.admitted = true;
        }
        let descriptor = NativeInputDescriptor::for_goal_activation(
            command,
            &goal_prompt_text(record),
            goal_marker(command, record),
        );
        if let Ok(descriptor) = descriptor
            && let Ok(session) = self.session(root).await
            && session["fork"].is_null()
            && session["revert"].is_null()
            && let Ok(scan) = ExecutionScan::for_goal(&descriptor)
            && let Ok(read) = self.execution_log(&descriptor, scan).await
            && read.synced
        {
            if read.scan.input_admitted() {
                activation.admitted = true;
                activation.input_id = Some(descriptor.input_id().to_owned());
            }
            if let Some(started) = read.scan.execution_started() {
                activation.execution_started = true;
                activation.execution_ref = Some(started);
            }
        }
        activation
    }

    pub(super) async fn execute_goal(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let root = match command.native_root_id.as_deref() {
            Some(root) => root.to_owned(),
            None => {
                return failed(
                    command,
                    options,
                    &Error::invalid("native root is missing"),
                    false,
                );
            }
        };
        let request = match GoalRequest::parse(&command.input) {
            Ok(request) => request,
            Err(error) => return failed(command, options, &error, false),
        };
        let before = match self.goal_readback(&root, options, command).await {
            Ok(readback) => readback,
            Err(error) => return failed(command, options, &error, false),
        };
        let decision = match decide(&request, before.record.as_ref(), &command.operation_id) {
            Ok(decision) => decision,
            Err(error) => return failed(command, options, &error, false),
        };
        let desired: Option<GoalRecord> = match &decision {
            GoalDecision::Noop => before.record.clone(),
            GoalDecision::Write(record, _) => Some(record.clone()),
            GoalDecision::Activate(record) => Some(record.clone()),
            GoalDecision::Remove => None,
        };
        if matches!(decision, GoalDecision::Noop) {
            return match goal_details(
                request.action,
                desired.as_ref(),
                &before,
                "preexisting_exact_readback",
                false,
                &GoalActivation::default(),
            ) {
                Ok(details) => outcome(command, EffectOutcome::Applied, options, details),
                Err(error) => failed(command, options, &error, false),
            };
        }
        let path =
            format!("/api/experimental/session/{root}/instructions/entries/{GOAL_ENTRY_KEY}");
        let mutation_sent = matches!(decision, GoalDecision::Write(_, _) | GoalDecision::Remove);
        let written = match &decision {
            GoalDecision::Write(record, _) => {
                self.put(&path, json!({"value":record.value()})).await
            }
            GoalDecision::Activate(_) => Ok(Value::Null),
            GoalDecision::Remove => self.delete(&path).await,
            GoalDecision::Noop => unreachable!(),
        };
        match written {
            Ok(Value::Null) => {}
            Ok(_) => {
                return failed(
                    command,
                    options,
                    &Error::new(
                        "NATIVE_GOAL_SCHEMA",
                        "native goal mutation returned an unexpected body",
                    ),
                    true,
                );
            }
            Err(error) => return failed(command, options, &error, true),
        }
        let after = match self.goal_readback(&root, options, command).await {
            Ok(readback) => readback,
            Err(error) => return failed(command, options, &error, mutation_sent),
        };
        if after.record != desired {
            return failed(
                command,
                options,
                &unresolved("native goal record did not match after mutation acknowledgement"),
                mutation_sent,
            );
        }
        let activate = matches!(
            decision,
            GoalDecision::Write(_, true) | GoalDecision::Activate(_)
        );
        let mut activation_input_id = None;
        if activate {
            if let Err(error) = self
                .require_durable_root_creation(
                    &root,
                    &command.binding_id,
                    command.generation,
                    options,
                )
                .await
            {
                // The goal record is already durable, but this activation
                // input has not crossed its POST boundary. Keep the Operation
                // unresolved so readback cannot imply that provider work ran.
                return failed(command, options, &error, mutation_sent);
            }
            let record = desired.as_ref().expect("activation requires a record");
            match self.admit_goal_activation(&root, command, record).await {
                Ok(id) => activation_input_id = Some(id),
                Err(error) => return failed(command, options, &error, true),
            }
        }
        let activation = GoalActivation {
            input_id: activation_input_id.clone(),
            admitted: activation_input_id.is_some(),
            // Ordinary Goal record actions complete at `native_goal_recorded`;
            // continue completes at `native_input_admitted`. This immediate
            // receipt proves only that the input was admitted. Execution start
            // is a later fact proven solely by the durable log.
            execution_started: false,
            execution_ref: None,
        };
        let details = match goal_details(
            request.action,
            desired.as_ref(),
            &after,
            if mutation_sent {
                "post_mutation_exact_readback"
            } else {
                "exact_active_record_readback_before_activation"
            },
            mutation_sent,
            &activation,
        ) {
            Ok(details) => details,
            Err(error) => return failed(command, options, &error, true),
        };
        let mut result = outcome(command, EffectOutcome::Applied, options, details);
        result.native_input_id = activation_input_id;
        result
    }

    /// Readback-only reconciliation: the exact entry this Operation wrote
    /// (its `updated_by_operation_id` is this Operation) plus, when the
    /// action activated the goal, the deterministic activation input. A
    /// record owned by any other operation is not this Operation's evidence:
    /// the runtime layer cannot distinguish a later goal Operation from an
    /// external write, so it stays unknown rather than claiming the effect.
    pub(super) async fn reconcile_goal(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let readback = async {
            self.verify().await?;
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let request = GoalRequest::parse(&command.input)?;
            let current = self.goal_readback(root, options, command).await?;
            if request.action == GoalAction::Clear {
                if current.record.is_some() {
                    return Err(unresolved("the goal record is still present after clear"));
                }
                let details = goal_details(
                    request.action,
                    None,
                    &current,
                    "exact_state_reconciliation",
                    false,
                    &GoalActivation::default(),
                )?;
                return Ok((details, None));
            }
            let record = current
                .record
                .as_ref()
                .ok_or_else(|| unresolved("the goal record is absent"))?;
            if request.action != GoalAction::Continue && record.operation_id != command.operation_id
            {
                return Err(unresolved(
                    "the goal record was not written by this operation",
                ));
            }
            // A record carrying this Operation's ID is exactly the record this
            // Operation wrote, so its status/objective fix the recorded part
            // of the effect. The activation input is attached when observed.
            let recorded = match request.action {
                GoalAction::Set | GoalAction::Resume => {
                    record.status == "active"
                        && request
                            .objective
                            .as_deref()
                            .is_none_or(|objective| objective == record.objective)
                }
                GoalAction::Edit => request
                    .objective
                    .as_deref()
                    .is_none_or(|objective| objective == record.objective),
                GoalAction::Pause => record.status == "paused",
                GoalAction::Continue => {
                    let expected = request.expected_revision.unwrap_or(0);
                    let revision = if expected == 0 { 1 } else { expected };
                    record.status == "active"
                        && record.revision == revision
                        && request.objective.as_deref() == Some(record.objective.as_str())
                        && (expected != 0 || record.operation_id == command.operation_id)
                }
                GoalAction::Clear => unreachable!(),
            };
            if !recorded {
                return Err(unresolved("the goal record does not match this operation"));
            }
            let activation = self.goal_activation_evidence(root, command, record).await;
            if request.action == GoalAction::Continue && !activation.admitted {
                return Err(unresolved(
                    "exact Goal continuation input admission is not observed",
                ));
            }
            let activation_input_id = activation.input_id.clone();
            let details = goal_details(
                request.action,
                Some(record),
                &current,
                "exact_state_reconciliation",
                false,
                &activation,
            )?;
            Ok((details, activation_input_id))
        }
        .await;
        match readback {
            Ok((details, activation_input_id)) => {
                let mut result = outcome(command, EffectOutcome::Applied, options, details);
                result.native_input_id = activation_input_id;
                result
            }
            Err(error) => outcome(
                command,
                EffectOutcome::Unknown,
                options,
                super::diagnostic(&error),
            ),
        }
    }

    pub(super) fn goal_observation_from_entries(entries: &[Value]) -> Result<Value> {
        let record = entries
            .iter()
            .find(|entry| entry["key"] == GOAL_ENTRY_KEY)
            .map(|entry| GoalRecord::parse(&entry["value"]))
            .transpose()?;
        let settings_revision = projection_revision(&owned_projection(entries)?)?;
        let objective_digest = record
            .as_ref()
            .map(GoalRecord::objective_digest)
            .transpose()?;
        Ok(json!({
            "complete":true,
            "present":record.is_some(),
            "status":record.as_ref().map(|record| record.status.as_str()),
            "revision":record.as_ref().map(|record| record.revision),
            "objective_digest":objective_digest,
            "settings_revision":settings_revision,
            "source":"experimental.session.instructions.entry.list",
            "objective_content_persisted":false,
            "continuation_owner":"controller_record",
            "native_goal_api":false
        }))
    }
}
