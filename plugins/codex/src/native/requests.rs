use crate::native::types::{
    NativeApprovalDecision, NativeCommandExecutionRequestApprovalParams,
    NativeCommandExecutionRequestApprovalResponse, NativeFileChangeRequestApprovalParams,
    NativeFileChangeRequestApprovalResponse, NativeRequestId, NativeToolRequestUserInputAnswer,
    NativeToolRequestUserInputParams, NativeToolRequestUserInputResponse,
};
use eliot_core::requests::{
    AnswerArgs, ApprovalChoice, ApprovalDecision, ApprovalRequest, Question, QuestionKind,
    QuestionOption, QuestionRequest,
};
use eliot_core::{Error, PendingRequest, Result};
use eliot_plugin::{MAX_EVENT_BYTES, protocol::MAX_COMMAND_ID_BYTES};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashSet};

const COMMAND_APPROVAL: &str = "item/commandExecution/requestApproval";
const FILE_CHANGE_APPROVAL: &str = "item/fileChange/requestApproval";
const USER_INPUT: &str = "item/tool/requestUserInput";

/// Native request metadata returned to the EventProcessor for durable opening.
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

/// Describe only the three 0.161 server-request families Eliot can answer.
/// `transport_id` is retained in details for the later native response write;
/// the public pending id binds it to the exact thread and active turn.
pub(crate) fn describe_request(
    method: &str,
    params: &Value,
    transport_id: &Value,
    expected_thread: &str,
    active_turn: &str,
) -> Result<CorePending> {
    validate_request_scope(expected_thread, active_turn)?;
    serde_json::from_value::<NativeRequestId>(transport_id.clone()).map_err(|_| {
        Error::new(
            "native_protocol",
            "native request id must be a string or int64",
        )
    })?;

    match method {
        COMMAND_APPROVAL => {
            let params: NativeCommandExecutionRequestApprovalParams =
                decode_params(params, "command approval params are invalid")?;
            validate_thread_turn(
                &params.thread_id,
                &params.turn_id,
                expected_thread,
                active_turn,
            )?;
            let approval_kind = params.kind.as_deref().unwrap_or("command");
            if approval_kind != "command" {
                return Err(Error::new(
                    "unsupported",
                    "Codex command approval kind is unsupported",
                ));
            }
            let mut details = Map::new();
            details.insert("item_id".to_owned(), Value::String(params.item_id));
            details.insert("started_at_ms".to_owned(), json!(params.started_at_ms));
            details.insert(
                "approval_kind".to_owned(),
                Value::String(approval_kind.to_owned()),
            );
            if let Some(command) = params.command {
                details.insert("command".to_owned(), Value::String(command));
            }
            if let Some(cwd) = params.cwd {
                details.insert("cwd".to_owned(), Value::String(cwd));
            }
            if let Some(reason) = params.reason {
                details.insert("reason".to_owned(), Value::String(reason));
            }
            if let Some(approval_id) = params.approval_id {
                details.insert("approval_id".to_owned(), Value::String(approval_id));
            }
            if let Some(context) = params.network_approval_context {
                details.insert("network_approval_context".to_owned(), context);
            }
            if let Some(amendment) = params.proposed_execpolicy_amendment {
                details.insert(
                    "proposed_execpolicy_amendment".to_owned(),
                    serde_json::to_value(amendment).map_err(|_| {
                        Error::new("internal", "Codex execpolicy proposal could not be encoded")
                    })?,
                );
            }
            if let Some(amendments) = params.proposed_network_policy_amendments {
                details.insert(
                    "proposed_network_policy_amendments".to_owned(),
                    serde_json::to_value(amendments).map_err(|_| {
                        Error::new("internal", "Codex network proposal could not be encoded")
                    })?,
                );
            }
            make_pending(
                method,
                transport_id,
                expected_thread,
                active_turn,
                "command_execution_approval",
                details,
            )
        }
        FILE_CHANGE_APPROVAL => {
            let params: NativeFileChangeRequestApprovalParams =
                decode_params(params, "file-change approval params are invalid")?;
            validate_thread_turn(
                &params.thread_id,
                &params.turn_id,
                expected_thread,
                active_turn,
            )?;
            if params.grant_root.is_some() {
                return Err(Error::new(
                    "unsupported",
                    "Codex file-change approval with a session permission grant is unsupported",
                ));
            }
            let mut details = Map::new();
            details.insert("item_id".to_owned(), Value::String(params.item_id));
            details.insert("started_at_ms".to_owned(), json!(params.started_at_ms));
            if let Some(reason) = params.reason {
                details.insert("reason".to_owned(), Value::String(reason));
            }
            make_pending(
                method,
                transport_id,
                expected_thread,
                active_turn,
                "file_change_approval",
                details,
            )
        }
        USER_INPUT => {
            let params: NativeToolRequestUserInputParams =
                decode_params(params, "user-input request params are invalid")?;
            validate_thread_turn(
                &params.thread_id,
                &params.turn_id,
                expected_thread,
                active_turn,
            )?;
            let mut seen = HashSet::new();
            let mut questions = Vec::with_capacity(params.questions.len());
            for question in params.questions {
                if question.id.trim().is_empty() || !seen.insert(question.id.clone()) {
                    return Err(Error::new(
                        "source_integrity",
                        "Codex user-input request has an empty or duplicate question id",
                    ));
                }
                let mut offered = Map::new();
                offered.insert("id".to_owned(), Value::String(question.id));
                offered.insert("is_secret".to_owned(), Value::Bool(question.is_secret));
                if !question.is_secret {
                    offered.insert("header".to_owned(), Value::String(question.header));
                    offered.insert("question".to_owned(), Value::String(question.question));
                    offered.insert("is_other".to_owned(), Value::Bool(question.is_other));
                    if let Some(options) = question.options {
                        let options = serde_json::to_value(options).map_err(|_| {
                            Error::new("internal", "Codex question options could not be encoded")
                        })?;
                        offered.insert("options".to_owned(), options);
                    }
                }
                questions.push(Value::Object(offered));
            }
            if questions.is_empty() {
                return Err(Error::new(
                    "source_integrity",
                    "Codex user-input request offered no questions",
                ));
            }
            let mut details = Map::new();
            details.insert("item_id".to_owned(), Value::String(params.item_id));
            details.insert("is_blocking".to_owned(), Value::Bool(params.is_blocking));
            details.insert("questions".to_owned(), Value::Array(questions));
            make_pending(
                method,
                transport_id,
                expected_thread,
                active_turn,
                "user_input",
                details,
            )
        }
        _ => Err(Error::new(
            "unsupported",
            "Codex server request method is unsupported",
        )),
    }
}

/// Build the one-shot native response body for a previously described request.
pub(crate) fn reply_for(request: &CorePending, verb: &str, args: &Value) -> Result<Value> {
    match (request.kind.as_str(), verb) {
        ("command_execution_approval", "approve") => {
            require_approval_args(args, true)?;
            encode_response(NativeCommandExecutionRequestApprovalResponse {
                decision: NativeApprovalDecision::Accept,
            })
        }
        ("command_execution_approval", "deny") => {
            require_approval_args(args, false)?;
            encode_response(NativeCommandExecutionRequestApprovalResponse {
                decision: NativeApprovalDecision::Decline,
            })
        }
        ("file_change_approval", "approve") => {
            require_approval_args(args, true)?;
            encode_response(NativeFileChangeRequestApprovalResponse {
                decision: NativeApprovalDecision::Accept,
            })
        }
        ("file_change_approval", "deny") => {
            require_approval_args(args, false)?;
            encode_response(NativeFileChangeRequestApprovalResponse {
                decision: NativeApprovalDecision::Decline,
            })
        }
        ("user_input", "answer") => reply_to_user_input(request, args),
        _ => Err(Error::new(
            "unsupported",
            "reply verb is unsupported for this native request",
        )),
    }
}

fn reply_to_user_input(request: &CorePending, args: &Value) -> Result<Value> {
    let args = AnswerArgs::parse(args)?;
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
        .collect::<BTreeMap<_, _>>();

    let mut response_answers = BTreeMap::new();
    for (question_id, answer) in &args.answers {
        let question = offered
            .get(question_id)
            .ok_or_else(|| Error::new("invalid", "answer names a question that was not offered"))?;
        if question.is_secret {
            return Err(Error::new(
                "secret_in_payload",
                "answers to secret Codex questions are unsupported",
            ));
        }
        let mut strings = Vec::with_capacity(answer.answers.len());
        for value in &answer.answers {
            if !question.allows(value) {
                return Err(Error::new(
                    "invalid",
                    "answer is not among the offered options or allowed freeform choice",
                ));
            }
            strings.push(value.clone());
        }
        response_answers.insert(
            question_id.to_owned(),
            NativeToolRequestUserInputAnswer { answers: strings },
        );
    }

    encode_response(NativeToolRequestUserInputResponse {
        answers: response_answers,
    })
}

#[derive(Deserialize, Clone)]
struct OfferedQuestion {
    id: String,
    #[serde(default)]
    header: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    is_secret: bool,
    #[serde(default)]
    is_other: bool,
    #[serde(default)]
    options: Option<Vec<crate::native::types::NativeToolRequestUserInputOption>>,
}

impl OfferedQuestion {
    fn allows(&self, answer: &str) -> bool {
        match self.options.as_deref() {
            Some(options) if !options.is_empty() => {
                self.is_other || options.iter().any(|option| option.label == answer)
            }
            _ => true,
        }
    }
}

fn make_pending(
    method: &str,
    transport_id: &Value,
    expected_thread: &str,
    active_turn: &str,
    kind: &str,
    mut details: Map<String, Value>,
) -> Result<CorePending> {
    let encoded_identity = serde_json::to_string(&(expected_thread, active_turn, transport_id))
        .map_err(|_| Error::new("internal", "native request identity could not be encoded"))?;
    let id = format!("codex-request:{encoded_identity}");
    if id.len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::new(
            "source_integrity",
            "native request identity exceeds the public id limit",
        ));
    }
    details.insert("native_method".to_owned(), Value::String(method.to_owned()));
    details.insert("transport_id".to_owned(), transport_id.clone());
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

fn validate_request_scope(expected_thread: &str, active_turn: &str) -> Result<()> {
    if expected_thread.trim().is_empty() || active_turn.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "native request has no exact owned thread and active turn",
        ));
    }
    Ok(())
}

fn validate_thread_turn(
    actual_thread: &str,
    actual_turn: &str,
    expected_thread: &str,
    active_turn: &str,
) -> Result<()> {
    if actual_thread != expected_thread || actual_turn != active_turn {
        return Err(Error::new(
            "source_integrity",
            "native request does not match the owned thread and active turn",
        ));
    }
    Ok(())
}

fn decode_params<T: for<'de> Deserialize<'de>>(params: &Value, message: &str) -> Result<T> {
    serde_json::from_value(params.clone()).map_err(|_| Error::new("native_protocol", message))
}

fn require_approval_args(args: &Value, approve: bool) -> Result<()> {
    let args = eliot_plugin::session::approval_reply_args(args)?;
    if let Some(choice_id) = args.get("choice_id").and_then(Value::as_str) {
        let wanted = if approve { "approve" } else { "deny" };
        if choice_id != wanted {
            return Err(Error::new(
                "invalid",
                "approval choice_id does not match the reply verb",
            ));
        }
    }
    if args.contains_key("feedback") {
        Err(Error::new(
            "unsupported",
            "Codex approvals do not accept feedback",
        ))
    } else {
        Ok(())
    }
}

fn encode_response<T: serde::Serialize>(response: T) -> Result<Value> {
    serde_json::to_value(response)
        .map_err(|_| Error::new("internal", "native request response could not be encoded"))
}
/// Project the retained native offer to its public `request.opened` kind and details.
pub(crate) fn public_request_view(request: &CorePending) -> Result<(String, Value)> {
    let details = request.details.as_object().ok_or_else(|| {
        Error::new(
            "source_integrity",
            "pending request details are not an object",
        )
    })?;
    match request.kind.as_str() {
        "user_input" => {
            let offered: Vec<OfferedQuestion> = details
                .get("questions")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "pending request omitted question offers",
                    )
                })?;
            let questions = offered
                .into_iter()
                .map(|question| {
                    let options = question
                        .options
                        .unwrap_or_default()
                        .into_iter()
                        .map(|option| QuestionOption {
                            id: option.label.clone(),
                            label: option.label,
                            description: if option.description.trim().is_empty() {
                                None
                            } else {
                                Some(option.description)
                            },
                        })
                        .collect::<Vec<_>>();
                    let kind = if options.is_empty() || question.is_other {
                        QuestionKind::Text
                    } else {
                        QuestionKind::Single
                    };
                    Question {
                        id: question.id.clone(),
                        kind,
                        title: question.header,
                        prompt: question.question,
                        options,
                        free_entry: question.is_other || question.is_secret,
                        is_secret: question.is_secret,
                        min_selections: None,
                        max_selections: None,
                    }
                })
                .collect::<Vec<_>>();
            let details = serde_json::to_value(QuestionRequest { questions }).map_err(|_| {
                Error::new("internal", "question public details could not be encoded")
            })?;
            Ok(("question".to_owned(), details))
        }
        "command_execution_approval" | "file_change_approval" => {
            let summary = approval_summary(request.kind.as_str(), details);
            let details = serde_json::to_value(ApprovalRequest {
                summary,
                choices: vec![
                    ApprovalChoice {
                        id: "approve".to_owned(),
                        label: "Approve".to_owned(),
                        decision: ApprovalDecision::Approve,
                    },
                    ApprovalChoice {
                        id: "deny".to_owned(),
                        label: "Deny".to_owned(),
                        decision: ApprovalDecision::Deny,
                    },
                ],
                feedback_allowed: false,
            })
            .map_err(|_| Error::new("internal", "approval public details could not be encoded"))?;
            Ok(("approval".to_owned(), details))
        }
        _ => Err(Error::new(
            "source_integrity",
            "pending request kind is neither approval nor user input",
        )),
    }
}

fn approval_summary(kind: &str, details: &Map<String, Value>) -> String {
    let mut summary = match kind {
        "command_execution_approval" => "Command execution approval",
        _ => "File change approval",
    }
    .to_owned();
    if let Some(command) = details.get("command").and_then(Value::as_str) {
        summary.push_str(": ");
        summary.push_str(command);
    }
    if let Some(cwd) = details.get("cwd").and_then(Value::as_str) {
        summary.push_str(" in ");
        summary.push_str(cwd);
    }
    if let Some(reason) = details.get("reason").and_then(Value::as_str) {
        summary.push_str(" — ");
        summary.push_str(reason);
    }
    summary
}
