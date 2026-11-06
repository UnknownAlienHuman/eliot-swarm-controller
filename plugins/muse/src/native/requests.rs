use crate::native::types::{
    NativeApprovalDecideParams, NativeApprovalRequestParams, NativeUserInputAnswer,
    NativeUserInputAnswerParams, NativeUserInputCancelParams, NativeUserInputRequestParams,
};
use eliot_core::{Error, PendingRequest, Result};
use eliot_plugin::{MAX_EVENT_BYTES, protocol::MAX_COMMAND_ID_BYTES};
use serde::Deserialize;
use serde_json::{Map, Value};

const APPROVAL_REQUEST: &str = "approval/request";
const USER_INPUT_REQUEST: &str = "userInput/request";

/// Native request metadata returned to the EventProcessor for durable opening.
/// The public pending id binds the exact session, active turn and native
/// request id; `details` carries the offered choices/questions the owner
/// decides from (raw native JSON, validated structurally first so future
/// additive-optional members survive without DTO churn).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CorePending {
    pub id: String,
    pub turn_id: String,
    pub kind: String,
    pub details: Value,
}

impl From<CorePending> for PendingRequest {
    fn from(request: CorePending) -> Self {
        Self {
            id: request.id,
            turn_id: request.turn_id,
            kind: request.kind,
            details: request.details,
        }
    }
}

/// One validated native reply: the exact command-lane method plus its params.
/// The decision/answer never travels as the transport response — the receipt
/// (`{}`) only acknowledges presentation (SS5.3.3).
#[derive(Debug)]
pub(crate) struct NativeReply {
    pub method: &'static str,
    pub params: Value,
}

/// Describe only the two server-request families Eliot can answer.
/// Rejects anything outside the exact owned session and active turn; child
/// (subagent-origin) approvals are described like parent-own ones — the
/// origin is provenance in `details`, the decision still targets the exact
/// approval id on this session.
pub(crate) fn describe_request(
    method: &str,
    params: &Value,
    expected_session: &str,
    active_turn: &str,
) -> Result<CorePending> {
    validate_request_scope(expected_session, active_turn)?;
    match method {
        APPROVAL_REQUEST => describe_approval(params, expected_session, active_turn),
        USER_INPUT_REQUEST => describe_user_input(params, expected_session, active_turn),
        _ => Err(Error::new(
            "unsupported",
            "Muse server request method is unsupported",
        )),
    }
}

fn describe_approval(
    params: &Value,
    expected_session: &str,
    active_turn: &str,
) -> Result<CorePending> {
    let request: NativeApprovalRequestParams =
        decode_params(params, "approval request params are invalid")?;
    validate_session_turn(
        &request.session_id,
        &request.turn_id,
        expected_session,
        active_turn,
    )?;
    require_nonempty(&request.approval_id, "approval id")?;
    require_nonempty(&request.view_cursor, "approval view cursor")?;
    if request.available_choices.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse approval offered no choices",
        ));
    }
    {
        let mut seen = std::collections::HashSet::new();
        for choice in &request.available_choices {
            require_nonempty(&choice.choice_id, "approval choice id")?;
            require_nonempty(&choice.decision, "approval choice decision")?;
            require_nonempty(&choice.label, "approval choice label")?;
            require_nonempty(&choice.scope, "approval choice scope")?;
            if !seen.insert(choice.choice_id.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    "Muse approval offered a duplicate choice id",
                ));
            }
        }
    }
    if request.current_requirement_id.approval_id != request.approval_id {
        return Err(Error::new(
            "source_integrity",
            "Muse approval requirement does not belong to its approval",
        ));
    }
    if !request.subject.is_object() {
        return Err(Error::new(
            "native_protocol",
            "Muse approval subject is not an object",
        ));
    }
    let raw_args_bytes = params
        .get("rawArgs")
        .and_then(Value::as_str)
        .map(str::len)
        .unwrap_or(0);
    let mut details = Map::new();
    details.insert(
        "session_id".to_owned(),
        Value::String(request.session_id.clone()),
    );
    details.insert("turn_id".to_owned(), Value::String(request.turn_id.clone()));
    details.insert(
        "approval_id".to_owned(),
        Value::String(request.approval_id.clone()),
    );
    details.insert("item_id".to_owned(), Value::String(request.item_id.clone()));
    details.insert(
        "tool_name".to_owned(),
        Value::String(request.tool_name.clone()),
    );
    details.insert(
        "tool_call_id".to_owned(),
        Value::String(request.tool_call_id.clone()),
    );
    if let Some(task_id) = request.task_id.as_deref() {
        details.insert("task_id".to_owned(), Value::String(task_id.to_owned()));
    }
    if let Some(judge_escalated) = request.judge_escalated {
        details.insert("judge_escalated".to_owned(), Value::Bool(judge_escalated));
    }
    if let Some(protected_write) = request.protected_write {
        details.insert("protected_write".to_owned(), Value::Bool(protected_write));
    }
    details.insert("subject".to_owned(), request.subject.clone());
    details.insert(
        "raw_args_bytes".to_owned(),
        Value::from(raw_args_bytes as u64),
    );
    // Raw validated JSON: future additive-optional choice members (rule
    // previews) survive without DTO churn; the reply path parses defensively.
    details.insert(
        "available_choices".to_owned(),
        params
            .get("availableChoices")
            .cloned()
            .unwrap_or(Value::Null),
    );
    details.insert(
        "current_requirement".to_owned(),
        params
            .get("currentRequirementId")
            .cloned()
            .unwrap_or(Value::Null),
    );
    details.insert(
        "offer_seq".to_owned(),
        Value::from(request.current_requirement_id.source_index),
    );
    if let Some(origin) = request.subagent_origin.clone() {
        details.insert("subagent_origin".to_owned(), origin);
    }
    details.insert(
        "view_cursor".to_owned(),
        Value::String(request.view_cursor.clone()),
    );
    make_pending(
        expected_session,
        active_turn,
        &request.approval_id,
        "approval",
        details,
    )
}

fn describe_user_input(
    params: &Value,
    expected_session: &str,
    active_turn: &str,
) -> Result<CorePending> {
    let request: NativeUserInputRequestParams =
        decode_params(params, "user-input request params are invalid")?;
    validate_session_turn(
        &request.session_id,
        &request.turn_id,
        expected_session,
        active_turn,
    )?;
    require_nonempty(&request.user_input_id, "user-input id")?;
    require_nonempty(&request.view_cursor, "user-input view cursor")?;
    if request.questions.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse user-input request offered no questions",
        ));
    }
    {
        let mut seen = std::collections::HashSet::new();
        for question in &request.questions {
            require_nonempty(&question.id, "user-input question id")?;
            if !seen.insert(question.id.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    "Muse user-input request has a duplicate question id",
                ));
            }
            if !matches!(question.selection.mode.as_str(), "single" | "multiple") {
                return Err(Error::new(
                    "native_protocol",
                    "Muse user-input selection mode is not single or multiple",
                ));
            }
            for option in &question.options {
                require_nonempty(&option.label, "user-input option label")?;
            }
        }
    }
    let mut details = Map::new();
    details.insert(
        "session_id".to_owned(),
        Value::String(request.session_id.clone()),
    );
    details.insert("turn_id".to_owned(), Value::String(request.turn_id.clone()));
    details.insert(
        "user_input_id".to_owned(),
        Value::String(request.user_input_id.clone()),
    );
    details.insert("item_id".to_owned(), Value::String(request.item_id.clone()));
    details.insert(
        "tool_name".to_owned(),
        Value::String(request.tool_name.clone()),
    );
    details.insert(
        "tool_call_id".to_owned(),
        Value::String(request.tool_call_id.clone()),
    );
    if let Some(auto_ms) = request.auto_resolution_ms {
        details.insert("auto_resolution_ms".to_owned(), Value::from(auto_ms));
    }
    details.insert(
        "questions".to_owned(),
        params.get("questions").cloned().unwrap_or(Value::Null),
    );
    details.insert(
        "view_cursor".to_owned(),
        Value::String(request.view_cursor.clone()),
    );
    make_pending(
        expected_session,
        active_turn,
        &request.user_input_id,
        "user_input",
        details,
    )
}

/// Build the one-shot native command for a previously described request.
/// Every error here becomes a rejected (never applied, never unknown) reply:
/// the caller's choice/answer did not match the currently offered set.
pub(crate) fn reply_for(
    request: &CorePending,
    verb: &str,
    args: &Value,
    command_id: &str,
    session_id: &str,
) -> Result<NativeReply> {
    match (request.kind.as_str(), verb) {
        ("approval", "approve") => reply_to_approval(request, true, args, command_id, session_id),
        ("approval", "deny") => reply_to_approval(request, false, args, command_id, session_id),
        ("user_input", "answer") => reply_to_user_input(request, args, command_id, session_id),
        ("user_input", "deny") => reply_to_user_input_cancel(request, args, command_id, session_id),
        _ => Err(Error::new(
            "unsupported",
            "reply verb is unsupported for this native request",
        )),
    }
}

fn reply_to_approval(
    request: &CorePending,
    approve: bool,
    args: &Value,
    command_id: &str,
    session_id: &str,
) -> Result<NativeReply> {
    let args = eliot_plugin::session::approval_reply_args(args)?;
    let choices: Vec<OfferedChoice> = request
        .details
        .get("available_choices")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted its offered choices",
            )
        })?;
    let requirement: NativeApprovalRequirementGuard = serde_json::from_value(
        request
            .details
            .get("current_requirement")
            .cloned()
            .unwrap_or(Value::Null),
    )
    .map_err(|_| {
        Error::new(
            "source_integrity",
            "pending request omitted its current requirement",
        )
    })?;
    let wanted = args
        .get("choice_id")
        .map(|value| {
            value
                .as_str()
                .filter(|text| !text.is_empty())
                .ok_or_else(|| {
                    Error::new("invalid", "approval choice_id must be a non-empty string")
                })
        })
        .transpose()?;
    let choice = resolve_choice(&choices, approve, wanted)?;
    let feedback = match args.get("feedback") {
        None => None,
        Some(Value::String(text)) => {
            if !choice.accepts_feedback {
                return Err(Error::new(
                    "invalid",
                    "approval feedback is only valid on a choice that accepts it",
                ));
            }
            Some(text.clone())
        }
        Some(_) => {
            return Err(Error::new("invalid", "approval feedback must be a string"));
        }
    };
    let approval_id = request
        .details
        .get("approval_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted its approval id",
            )
        })?;
    let params = NativeApprovalDecideParams {
        approval_id: approval_id.to_owned(),
        choice_id: choice.choice_id.clone(),
        command_id: command_id.to_owned(),
        requirement_id: crate::native::types::NativeApprovalRequirementRef {
            approval_id: requirement.approval_id,
            source_index: requirement.source_index,
        },
        session_id: session_id.to_owned(),
        feedback,
    };
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("internal", "approval decide params could not be encoded"))?;
    Ok(NativeReply {
        method: "approval/decide",
        params,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OfferedChoice {
    choice_id: String,
    decision: String,
    #[serde(default)]
    accepts_feedback: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeApprovalRequirementGuard {
    approval_id: String,
    source_index: i64,
}

fn resolve_choice<'a>(
    choices: &'a [OfferedChoice],
    approve: bool,
    wanted: Option<&str>,
) -> Result<&'a OfferedChoice> {
    let verb = if approve { "approve" } else { "deny" };
    let compatible = |decision: &str| {
        if approve {
            matches!(
                decision,
                "approved" | "approvedForSession" | "approvedPolicyAmendment"
            )
        } else {
            matches!(decision, "denied" | "deniedPolicyAmendment")
        }
    };
    if let Some(wanted) = wanted {
        let choice = choices
            .iter()
            .find(|choice| choice.choice_id == wanted)
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "approval choice was not among the offered choices",
                )
            })?;
        if !compatible(choice.decision.as_str()) {
            return Err(Error::new(
                "invalid",
                format!("approval choice {wanted} cannot be decided by {verb}"),
            ));
        }
        return Ok(choice);
    }
    let mut candidates = choices.iter().filter(|choice| compatible(&choice.decision));
    match (candidates.next(), candidates.next()) {
        (Some(only), None) => Ok(only),
        (None, _) => Err(Error::new(
            "invalid",
            format!("no approval choice is currently offered for {verb}"),
        )),
        (Some(_), Some(_)) => Err(Error::new(
            "invalid",
            format!("more than one approval choice is offered for {verb}; name choice_id"),
        )),
    }
}

fn reply_to_user_input(
    request: &CorePending,
    args: &Value,
    command_id: &str,
    session_id: &str,
) -> Result<NativeReply> {
    let args = args
        .as_object()
        .filter(|args| args.len() == 1)
        .ok_or_else(|| Error::new("invalid", "answer args must contain only answers"))?;
    let answers = args
        .get("answers")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("invalid", "answer args.answers must be an object"))?;
    let questions: Vec<OfferedQuestion> = request
        .details
        .get("questions")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted question offers",
            )
        })?;
    let offered = questions
        .iter()
        .map(|question| (question.id.clone(), question))
        .collect::<std::collections::BTreeMap<_, _>>();
    if answers.len() != offered.len() {
        return Err(Error::new(
            "invalid",
            "answer must answer every offered question exactly once",
        ));
    }
    let mut wire = Vec::with_capacity(answers.len());
    for (question_id, answer) in answers {
        let question = offered
            .get(question_id)
            .ok_or_else(|| Error::new("invalid", "answer names a question that was not offered"))?;
        wire.push(answer_for(question, answer)?);
    }
    // Deterministic wire order: the offered question order, not the JSON map
    // order, so value-identical replays stay byte-identical.
    wire.sort_by_key(|answer: &NativeUserInputAnswer| {
        offered
            .get(&answer.question_id)
            .and_then(|question| {
                questions
                    .iter()
                    .position(|offered| offered.id == question.id)
            })
            .unwrap_or(usize::MAX)
    });
    let user_input_id = request
        .details
        .get("user_input_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted its user-input id",
            )
        })?;
    let params = NativeUserInputAnswerParams {
        answers: wire,
        command_id: command_id.to_owned(),
        session_id: session_id.to_owned(),
        user_input_id: user_input_id.to_owned(),
    };
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("internal", "user-input answer params could not be encoded"))?;
    Ok(NativeReply {
        method: "userInput/answer",
        params,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OfferedQuestion {
    id: String,
    #[serde(default)]
    options: Vec<OfferedOption>,
    selection: OfferedSelection,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OfferedOption {
    label: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OfferedSelection {
    mode: String,
    #[serde(default)]
    min_selections: Option<i64>,
    #[serde(default)]
    max_selections: Option<i64>,
}

fn answer_for(question: &OfferedQuestion, answer: &Value) -> Result<NativeUserInputAnswer> {
    let answer = answer.as_object().ok_or_else(|| {
        Error::new(
            "invalid",
            "each answer must be an object with one answer shape and an optional note",
        )
    })?;
    if answer.keys().any(|key| {
        !matches!(
            key.as_str(),
            "selected_label" | "selected_labels" | "free_text" | "note"
        )
    }) {
        return Err(Error::new(
            "invalid",
            "each answer must be an object with one answer shape and an optional note",
        ));
    }
    let note = match answer.get("note") {
        None => None,
        Some(Value::String(note)) if note.is_empty() => None,
        Some(Value::String(note)) => {
            if note.chars().count() > 500 {
                return Err(Error::new("invalid", "answer note exceeds 500 chars"));
            }
            Some(note.clone())
        }
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "answer note must be a string of at most 500 chars",
            ));
        }
    };
    let shapes = ["selected_label", "selected_labels", "free_text"]
        .into_iter()
        .filter(|shape| answer.contains_key(*shape))
        .collect::<Vec<_>>();
    if shapes.len() != 1 {
        return Err(Error::new(
            "invalid",
            "each answer carries exactly one of selected_label, selected_labels, or free_text",
        ));
    }
    let labels = question
        .options
        .iter()
        .map(|option| option.label.as_str())
        .collect::<Vec<_>>();
    let mut wire = NativeUserInputAnswer {
        question_id: question.id.clone(),
        selected_label: None,
        selected_labels: None,
        free_text: None,
        note,
    };
    match shapes[0] {
        "selected_label" => {
            if question.selection.mode != "single" || labels.is_empty() {
                return Err(Error::new(
                    "invalid",
                    "selected_label answers only a single-mode question with options",
                ));
            }
            let label = answer
                .get("selected_label")
                .and_then(Value::as_str)
                .filter(|label| !label.is_empty())
                .ok_or_else(|| {
                    Error::new("invalid", "selected_label must be a non-empty string")
                })?;
            if !labels.contains(&label) {
                return Err(Error::new(
                    "invalid",
                    "selected_label is not among the offered option labels",
                ));
            }
            wire.selected_label = Some(label.to_owned());
        }
        "selected_labels" => {
            if question.selection.mode != "multiple" || labels.is_empty() {
                return Err(Error::new(
                    "invalid",
                    "selected_labels answers only a multiple-mode question with options",
                ));
            }
            let selected = answer
                .get("selected_labels")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::new("invalid", "selected_labels must be an array"))?;
            if selected.is_empty() {
                return Err(Error::new(
                    "invalid",
                    "selected_labels must name at least one option",
                ));
            }
            let mut picked: Vec<String> = Vec::with_capacity(selected.len());
            for label in selected {
                let label = label
                    .as_str()
                    .filter(|label| !label.is_empty())
                    .ok_or_else(|| {
                        Error::new("invalid", "selected_labels must be non-empty strings")
                    })?;
                if !labels.contains(&label) {
                    return Err(Error::new(
                        "invalid",
                        "selected_labels names a label that was not offered",
                    ));
                }
                if picked.iter().any(|picked| picked == label) {
                    return Err(Error::new(
                        "invalid",
                        "selected_labels must not repeat a label",
                    ));
                }
                picked.push(label.to_owned());
            }
            if let Some(min) = question.selection.min_selections
                && (picked.len() as i64) < min
            {
                return Err(Error::new(
                    "invalid",
                    "selected_labels names fewer options than the offered minimum",
                ));
            }
            if let Some(max) = question.selection.max_selections
                && (picked.len() as i64) > max
            {
                return Err(Error::new(
                    "invalid",
                    "selected_labels names more options than the offered maximum",
                ));
            }
            wire.selected_labels = Some(picked);
        }
        "free_text" => {
            if !labels.is_empty() {
                return Err(Error::new(
                    "invalid",
                    "free_text answers only a question without offered options",
                ));
            }
            let text = answer
                .get("free_text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .ok_or_else(|| Error::new("invalid", "free_text must be a non-empty string"))?;
            if text.chars().count() > 500 {
                return Err(Error::new("invalid", "free_text exceeds 500 chars"));
            }
            wire.free_text = Some(text.to_owned());
        }
        _ => unreachable!("answer shape filter"),
    }
    Ok(wire)
}

fn reply_to_user_input_cancel(
    request: &CorePending,
    args: &Value,
    command_id: &str,
    session_id: &str,
) -> Result<NativeReply> {
    let args = args.as_object().ok_or_else(|| {
        Error::new(
            "invalid",
            "cancel reply args must be an object with only reason",
        )
    })?;
    if args.keys().any(|key| key != "reason") {
        return Err(Error::new(
            "invalid",
            "cancel reply args must be an object with only reason",
        ));
    }
    let reason = match args.get("reason") {
        None => None,
        Some(Value::String(reason)) if !reason.is_empty() => Some(reason.clone()),
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "cancel reason must be a non-empty string",
            ));
        }
    };
    let user_input_id = request
        .details
        .get("user_input_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted its user-input id",
            )
        })?;
    let params = NativeUserInputCancelParams {
        command_id: command_id.to_owned(),
        session_id: session_id.to_owned(),
        user_input_id: user_input_id.to_owned(),
        reason,
    };
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("internal", "user-input cancel params could not be encoded"))?;
    Ok(NativeReply {
        method: "userInput/cancel",
        params,
    })
}

/// Refresh the in-memory offer after `approval/updated`: the available
/// choices, current requirement and view cursor are replaced wholesale from
/// the validated raw params, plus the subject when the raw offer carries it
/// (stage resolutions advance with the requirement). The caller persists the
/// same refreshed offer to the Store under the exact pending id so the owner
/// projection exposes the current requirement; the reply path always decides
/// from this refreshed offer.
pub(crate) fn refresh_offer_from_raw(details: &mut Value, raw: &Value) -> Result<()> {
    let Some(fields) = details.as_object_mut() else {
        return Err(Error::new(
            "source_integrity",
            "pending request details are not an object",
        ));
    };
    for key in ["availableChoices", "currentRequirementId", "viewCursor"] {
        let Some(value) = raw.get(key) else {
            return Err(Error::new(
                "native_protocol",
                "Muse approval update omitted its refreshed offer",
            ));
        };
        let camel = match key {
            "availableChoices" => "available_choices",
            "currentRequirementId" => "current_requirement",
            "viewCursor" => "view_cursor",
            _ => unreachable!("refresh keys"),
        };
        fields.insert(camel.to_owned(), value.clone());
    }
    if let Some(subject) = raw.get("subject") {
        fields.insert("subject".to_owned(), subject.clone());
    }
    if let Some(index) = raw
        .get("currentRequirementId")
        .and_then(|value| value.get("sourceIndex"))
        .and_then(Value::as_i64)
    {
        fields.insert("offer_seq".to_owned(), Value::from(index));
    }
    Ok(())
}

/// Whether `raw` proves a strictly newer multi-stage requirement than the
/// retained offer: same approval id, strictly greater source index. Anything
/// else (same or older index, another approval, malformed) is not newer.
pub(crate) fn is_newer_approval_requirement(existing_details: &Value, raw: &Value) -> bool {
    let current = existing_details
        .get("current_requirement")
        .and_then(|value| {
            Some((
                value.get("approvalId")?.as_str()?,
                value.get("sourceIndex")?.as_i64()?,
            ))
        });
    let incoming = raw.get("currentRequirementId").and_then(|value| {
        Some((
            value.get("approvalId")?.as_str()?,
            value.get("sourceIndex")?.as_i64()?,
        ))
    });
    match (current, incoming) {
        (Some((old_approval, old_index)), Some((new_approval, new_index))) => {
            !old_approval.is_empty() && old_approval == new_approval && new_index > old_index
        }
        _ => false,
    }
}

/// Whether a re-issued `approval/request` carries the same offer the entry
/// already holds (same choices and requirement): an idempotent re-presentation
/// refreshes nothing, a different offer replaces the in-memory snapshot.
pub(crate) fn same_approval_offer(existing_details: &Value, raw: &Value) -> bool {
    let existing = (
        existing_details.get("available_choices"),
        existing_details.get("current_requirement"),
    );
    let incoming = (raw.get("availableChoices"), raw.get("currentRequirementId"));
    match (existing, incoming) {
        ((Some(a), Some(b)), (Some(c), Some(d))) => a == c && b == d,
        _ => false,
    }
}

fn make_pending(
    expected_session: &str,
    active_turn: &str,
    native_id: &str,
    kind: &str,
    details: Map<String, Value>,
) -> Result<CorePending> {
    let id = format!("muse-request:{expected_session}:{active_turn}:{native_id}");
    if id.len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::new(
            "source_integrity",
            "native request identity exceeds the public id limit",
        ));
    }
    let details = Value::Object(details);
    let encoded_details = serde_json::to_vec(&details)
        .map_err(|_| Error::new("internal", "native request details could not be encoded"))?;
    if encoded_details.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "native request details exceed the persisted-event limit",
        ));
    }
    Ok(CorePending {
        id,
        turn_id: active_turn.to_owned(),
        kind: kind.to_owned(),
        details,
    })
}

fn validate_request_scope(expected_session: &str, active_turn: &str) -> Result<()> {
    if expected_session.trim().is_empty() || active_turn.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "native request has no exact owned session and active turn",
        ));
    }
    Ok(())
}

fn validate_session_turn(
    actual_session: &str,
    actual_turn: &str,
    expected_session: &str,
    active_turn: &str,
) -> Result<()> {
    if actual_session != expected_session || actual_turn != active_turn {
        return Err(Error::new(
            "source_integrity",
            "native request does not match the owned session and active turn",
        ));
    }
    Ok(())
}

fn require_nonempty(value: &str, what: &str) -> Result<()> {
    eliot_plugin::session::require_nonempty(value, "Muse", what)
}

fn decode_params<T: for<'de> Deserialize<'de>>(params: &Value, message: &str) -> Result<T> {
    serde_json::from_value(params.clone()).map_err(|_| Error::new("native_protocol", message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn approval_params() -> Value {
        json!({
            "approvalId": "approval-1",
            "availableChoices": [
                {"choiceId": "allow", "decision": "approved", "label": "Allow",
                 "scope": "once", "acceptsFeedback": false},
                {"choiceId": "deny", "decision": "denied", "label": "Deny",
                 "scope": "once", "acceptsFeedback": true},
            ],
            "currentRequirementId": {"approvalId": "approval-1", "sourceIndex": 1},
            "itemId": "item-1",
            "sessionId": "session-1",
            "subject": {"kind": "shell", "command": "ls"},
            "toolCallId": "call-1",
            "toolName": "shell",
            "turnId": "turn-1",
            "viewCursor": "cursor-3",
        })
    }

    fn user_input_params() -> Value {
        json!({
            "itemId": "item-2",
            "questions": [{
                "header": "Deploy",
                "id": "q1",
                "options": [{"label": "staging"}, {"label": "production"}],
                "question": "Where?",
                "selection": {"mode": "single"},
            }],
            "sessionId": "session-1",
            "toolCallId": "call-2",
            "toolName": "ask",
            "turnId": "turn-1",
            "userInputId": "prompt-1",
            "viewCursor": "cursor-4",
        })
    }

    #[test]
    fn approval_describe_binds_exact_session_turn_and_offer() {
        let pending = describe_request(
            "approval/request",
            &approval_params(),
            "session-1",
            "turn-1",
        )
        .expect("approval describes");
        assert_eq!(pending.kind, "approval");
        assert_eq!(pending.turn_id, "turn-1");
        assert!(
            pending
                .id
                .starts_with("muse-request:session-1:turn-1:approval-1")
        );
        assert_eq!(
            pending.details.get("approval_id").and_then(Value::as_str),
            Some("approval-1")
        );
        assert_eq!(
            pending
                .details
                .get("available_choices")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            pending
                .details
                .get("raw_args_bytes")
                .and_then(Value::as_u64),
            Some(0)
        );
        let pending_request = PendingRequest::from(pending.clone());
        assert_eq!(pending_request.id, pending.id);
    }

    #[test]
    fn approval_describe_rejects_foreign_session_or_turn() {
        for (session, turn) in [("other", "turn-1"), ("session-1", "other")] {
            let error = describe_request("approval/request", &approval_params(), session, turn)
                .expect_err("foreign binding is rejected");
            assert_eq!(error.code, "source_integrity");
        }
    }

    #[test]
    fn approval_describe_rejects_empty_or_duplicate_choices() {
        let mut params = approval_params();
        params["availableChoices"] = Value::Array(vec![]);
        assert!(describe_request("approval/request", &params, "session-1", "turn-1").is_err());
        let mut params = approval_params();
        params["availableChoices"] = json!([
            {"choiceId": "same", "decision": "approved", "label": "A", "scope": "once"},
            {"choiceId": "same", "decision": "denied", "label": "B", "scope": "once"},
        ]);
        assert!(describe_request("approval/request", &params, "session-1", "turn-1").is_err());
    }

    #[test]
    fn approve_resolves_the_unique_approving_choice_without_args() {
        let pending = describe_request(
            "approval/request",
            &approval_params(),
            "session-1",
            "turn-1",
        )
        .expect("approval describes");
        let reply = reply_for(&pending, "approve", &json!({}), "cmd-1", "session-1")
            .expect("approve resolves");
        assert_eq!(reply.method, "approval/decide");
        assert_eq!(
            reply.params.get("choiceId").and_then(Value::as_str),
            Some("allow")
        );
        assert_eq!(
            reply.params.get("requirementId"),
            Some(&json!({"approvalId": "approval-1", "sourceIndex": 1}))
        );
        assert!(reply.params.get("feedback").is_none());
    }

    #[test]
    fn deny_needs_an_explicit_choice_when_ambiguous() {
        let mut params = approval_params();
        params["availableChoices"] = json!([
            {"choiceId": "deny", "decision": "denied", "label": "Deny", "scope": "once",
             "acceptsFeedback": true},
            {"choiceId": "deny-always", "decision": "deniedPolicyAmendment",
             "label": "Always deny", "scope": "localPersistent", "acceptsFeedback": true},
        ]);
        let pending = describe_request("approval/request", &params, "session-1", "turn-1")
            .expect("approval describes");
        let error = reply_for(&pending, "deny", &json!({}), "cmd-1", "session-1")
            .expect_err("ambiguous deny is rejected");
        assert!(error.message.contains("choice_id"));
        let reply = reply_for(
            &pending,
            "deny",
            &json!({"choice_id": "deny-always", "feedback": "never touch prod"}),
            "cmd-1",
            "session-1",
        )
        .expect("explicit deny resolves");
        assert_eq!(reply.method, "approval/decide");
        assert_eq!(
            reply.params.get("choiceId").and_then(Value::as_str),
            Some("deny-always")
        );
        assert_eq!(
            reply.params.get("feedback").and_then(Value::as_str),
            Some("never touch prod")
        );
    }

    #[test]
    fn approve_rejects_unoffered_and_mismatched_choices() {
        let pending = describe_request(
            "approval/request",
            &approval_params(),
            "session-1",
            "turn-1",
        )
        .expect("approval describes");
        assert!(
            reply_for(
                &pending,
                "approve",
                &json!({"choice_id": "nope"}),
                "cmd-1",
                "session-1"
            )
            .is_err()
        );
        assert!(
            reply_for(
                &pending,
                "approve",
                &json!({"choice_id": "deny"}),
                "cmd-1",
                "session-1"
            )
            .is_err()
        );
        assert!(
            reply_for(
                &pending,
                "approve",
                &json!({"choice_id": "allow", "feedback": "x"}),
                "cmd-1",
                "session-1"
            )
            .is_err()
        );
        assert!(reply_for(&pending, "answer", &json!({}), "cmd-1", "session-1").is_err());
    }

    #[test]
    fn answer_validates_every_question_against_its_offer() {
        let pending = describe_request(
            "userInput/request",
            &user_input_params(),
            "session-1",
            "turn-1",
        )
        .expect("prompt describes");
        let reply = reply_for(
            &pending,
            "answer",
            &json!({"answers": {"q1": {"selected_label": "staging"}}}),
            "cmd-1",
            "session-1",
        )
        .expect("answer resolves");
        assert_eq!(reply.method, "userInput/answer");
        assert_eq!(
            reply.params.get("answers"),
            Some(&json!([{"questionId": "q1", "selectedLabel": "staging"}]))
        );
        // Unoffered label, missing question, and extra question are all rejected.
        assert!(
            reply_for(
                &pending,
                "answer",
                &json!({"answers": {"q1": {"selected_label": "nope"}}}),
                "cmd-1",
                "session-1"
            )
            .is_err()
        );
        assert!(
            reply_for(
                &pending,
                "answer",
                &json!({"answers": {}}),
                "cmd-1",
                "session-1"
            )
            .is_err()
        );
        assert!(reply_for(
            &pending,
            "answer",
            &json!({"answers": {"q1": {"selected_label": "staging"}, "q2": {"free_text": "x"}}}),
            "cmd-1",
            "session-1"
        )
        .is_err());
        assert!(reply_for(&pending, "approve", &json!({}), "cmd-1", "session-1").is_err());
    }

    #[test]
    fn deny_cancels_a_prompt_with_an_optional_reason() {
        let pending = describe_request(
            "userInput/request",
            &user_input_params(),
            "session-1",
            "turn-1",
        )
        .expect("prompt describes");
        let reply =
            reply_for(&pending, "deny", &json!({}), "cmd-1", "session-1").expect("deny cancels");
        assert_eq!(reply.method, "userInput/cancel");
        assert!(reply.params.get("reason").is_none());
        let reply = reply_for(
            &pending,
            "deny",
            &json!({"reason": "not now"}),
            "cmd-1",
            "session-1",
        )
        .expect("deny with reason cancels");
        assert_eq!(
            reply.params.get("reason").and_then(Value::as_str),
            Some("not now")
        );
    }
}
