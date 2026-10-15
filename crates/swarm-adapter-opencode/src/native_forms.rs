//! Actual SDK 2.0.7 question-form helpers (native question tool backing).
//!
//! The native question tool (opencode.tool.question, SDK 2.0.7) does not
//! expose /question routes. It creates a native form via forms.ask with:
//! title "Questions", metadata {kind:"question", tool:{messageID,id}},
//! fields q0..qN where each field is {key,title=header,description=question,
//! type=string|multiselect,options=[{value=label,label,description}],custom:true}.
//! Single-select maps to type:"string" with a scalar native answer;
//! multi-select maps to type:"multiselect" with an array native answer.
//! Option value always equals label for coding questions; custom is
//! always true (free-form answers allowed). Anything else (hidden/when/
//! required/number/boolean/external/global-sentinel/auth forms) is not a
//! coding question and must be rejected without guessed coercions.
//!
//! This module keeps the documented ELIOT coding-question contract stable:
//! external agent.reply bodies stay {answers:[[..]]} / {decision:"reject"}
//! and observations stay pending_questions with {question,header,options,
//! multiple,custom}. Only the native wire mapping uses frm_ form routes.
//! Raw answers/keys are never persisted here; callers store digests only.

use serde_json::{Value, json};
use swarm_contracts::error::{Error, Result};

pub const MAX_QUESTIONS: usize = 32;
pub const MAX_OPTIONS: usize = 32;
pub const MAX_ANSWERS: usize = 32;
pub const MAX_NATIVE_TEXT_BYTES: usize = 4096;
pub const MAX_INTERACTION_BYTES: usize = 32 * 1024;

/// Validate a native Form.Info as an exact coding-question form.
/// Returns the ordered field count (q0..qN). Strict: any deviation
/// (non-question metadata, global sentinel, hidden/when/external/numeric,
/// value!=label, custom!=true, key order break) is a schema error so the
/// caller treats the id as not-pending-as-question instead of coercing it.
pub fn validate_question_form(item: &Value) -> Result<usize> {
    exact_fields(
        item,
        &["id", "sessionID", "title", "metadata", "fields"],
        &[],
    )?;
    let id = text(item, "id", 256)?;
    if !is_frm(id) {
        return Err(schema_error("native form ID is not a current frm_ form"));
    }
    let session = text(item, "sessionID", 256)?;
    if session == "global" || !is_ses(session) {
        return Err(schema_error("native form belongs to another session"));
    }
    if text(item, "title", MAX_NATIVE_TEXT_BYTES)? != "Questions" {
        return Err(schema_error("native form title is not a coding question"));
    }
    let metadata = item
        .get("metadata")
        .filter(|v| v.is_object())
        .ok_or_else(|| schema_error("native question metadata is invalid"))?;
    exact_fields(metadata, &["kind", "tool"], &[])?;
    if metadata.get("kind").and_then(Value::as_str) != Some("question") {
        return Err(schema_error("native form is not a coding question"));
    }
    let tool = metadata
        .get("tool")
        .filter(|v| v.is_object())
        .ok_or_else(|| schema_error("native question tool is missing"))?;
    exact_fields(tool, &["messageID", "id"], &[])?;
    let mid = bounded_text(tool.get("messageID"), 256)?;
    let tid = bounded_text(tool.get("id"), 256)?;
    if mid == "global" || tid == "global" {
        return Err(schema_error("native question tool names a global sentinel"));
    }
    let fields = item
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| schema_error("native question list is empty or too large"))?;
    if fields.is_empty() || fields.len() > MAX_QUESTIONS {
        return Err(schema_error("native question list is empty or too large"));
    }
    for (index, field) in fields.iter().enumerate() {
        let expected = format!("q{index}");
        exact_fields(
            field,
            &["key", "title", "description", "type", "options", "custom"],
            &[],
        )?;
        if field.get("key").and_then(Value::as_str) != Some(expected.as_str()) {
            return Err(schema_error(
                "native question field key is not the generated qN sequence",
            ));
        }
        let _ = text(field, "title", MAX_NATIVE_TEXT_BYTES)?;
        let _ = text(field, "description", MAX_NATIVE_TEXT_BYTES)?;
        let ftype = text(field, "type", 16)?;
        if !matches!(ftype, "string" | "multiselect") {
            return Err(schema_error(
                "native question field type is not a coding answer",
            ));
        }
        if field.get("custom").and_then(Value::as_bool) != Some(true) {
            return Err(schema_error(
                "native question field does not allow custom answers",
            ));
        }
        let options = field
            .get("options")
            .and_then(Value::as_array)
            .ok_or_else(|| schema_error("question options have an invalid shape"))?;
        if options.is_empty() || options.len() > MAX_OPTIONS {
            return Err(schema_error("question options have an invalid shape"));
        }
        for option in options {
            exact_fields(option, &["value", "label", "description"], &[])?;
            let value = text(option, "value", MAX_NATIVE_TEXT_BYTES)?;
            let label = text(option, "label", MAX_NATIVE_TEXT_BYTES)?;
            if value != label {
                return Err(schema_error(
                    "native question option value differs from its label",
                ));
            }
            let _ = text(option, "description", MAX_NATIVE_TEXT_BYTES)?;
        }
    }
    if serde_json::to_vec(item)
        .map_err(|_| schema_error("pending interaction cannot be represented as JSON"))?
        .len()
        > MAX_INTERACTION_BYTES
    {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "pending interaction exceeds its bound",
        ));
    }
    Ok(fields.len())
}

/// Project an exact question form to the stable ELIOT pending_questions
/// observation. Tool id is surfaced as call_id for backwards compat
/// with swarm.opencode_interaction_observation@1 (root owns metadata);
/// native validation already enforces id (not callID).
pub fn project_question_form(item: &Value, session_id: &str, fingerprint: &str) -> Result<Value> {
    let _ = validate_question_form(item)?;
    let fields = item["fields"]
        .as_array()
        .ok_or_else(|| schema_error("native question list is invalid"))?;
    let mut questions = Vec::with_capacity(fields.len());
    for field in fields {
        let options = field["options"]
            .as_array()
            .ok_or_else(|| schema_error("question options are missing"))?;
        let mut projected = Vec::with_capacity(options.len());
        for option in options {
            projected.push(json!({
                "label": text(option, "label", MAX_NATIVE_TEXT_BYTES)?,
                "description": text(option, "description", MAX_NATIVE_TEXT_BYTES)?,
            }));
        }
        let ftype = text(field, "type", 16)?;
        questions.push(json!({
            "question": text(field, "description", MAX_NATIVE_TEXT_BYTES)?,
            "header": text(field, "title", MAX_NATIVE_TEXT_BYTES)?,
            "options": projected,
            "multiple": ftype == "multiselect",
            "custom": true,
        }));
    }
    let tool = &item["metadata"]["tool"];
    let mut result = json!({
        "session_id": session_id,
        "request_id": item["id"],
        "fingerprint": fingerprint,
        "questions": questions,
    });
    result["tool"] = json!({
        "message_id": text(tool, "messageID", 256)?,
        "call_id": text(tool, "id", 256)?,
    });
    if serde_json::to_vec(&result)
        .map_err(|_| schema_error("native interaction cannot be represented as JSON"))?
        .len()
        > MAX_INTERACTION_BYTES
    {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "native interaction exceeds its bound",
        ));
    }
    Ok(result)
}

/// Validate external {answers:[[..]]} against the exact native question
/// form and build the native {answer:Record} payload (qN keys).
/// String fields take a scalar string; multiselect fields take an array.
/// Empty groups are omitted (unanswered optional fields). Custom is
/// always true for coding questions so any bounded non-empty text
/// is accepted; no guessed coercions to numbers/booleans/externals.
pub fn question_answers_to_native(
    body: &Value,
    form: &Value,
    question_count: usize,
) -> Result<(Vec<Vec<String>>, Value)> {
    let obj = body
        .as_object()
        .ok_or_else(|| invalid("native reply body must be an object"))?;
    if obj.len() != 1 || !obj.contains_key("answers") {
        return Err(invalid("native reply body contains unsupported fields"));
    }
    let answers = body["answers"]
        .as_array()
        .filter(|a| a.len() == question_count && a.len() <= MAX_QUESTIONS)
        .ok_or_else(|| invalid("answer groups must match the ordered native questions"))?;
    let fields = form["fields"]
        .as_array()
        .ok_or_else(|| schema_error("native question list is invalid"))?;
    let mut eliot: Vec<Vec<String>> = Vec::with_capacity(answers.len());
    let mut native = serde_json::Map::new();
    for (group_value, field) in answers.iter().zip(fields.iter()) {
        let selected = group_value
            .as_array()
            .filter(|s| s.len() <= MAX_ANSWERS)
            .ok_or_else(|| invalid("each native question requires a bounded answer list"))?;
        let ftype = text(field, "type", 16)
            .map_err(|_| schema_error("native question options are invalid"))?;
        let multiple = ftype == "multiselect";
        if (!multiple && selected.len() > 1) || (multiple && selected.len() > MAX_OPTIONS) {
            return Err(invalid(
                "answer count conflicts with native multiple-selection rules",
            ));
        }
        let mut group: Vec<String> = Vec::with_capacity(selected.len());
        for answer in selected {
            let answer = answer
                .as_str()
                .filter(|a| !a.trim().is_empty() && a.len() <= MAX_NATIVE_TEXT_BYTES)
                .ok_or_else(|| invalid("native answer is empty or too large"))?;
            if group.iter().any(|p| p == answer) {
                return Err(invalid("duplicate answer is not allowed"));
            }
            group.push(answer.to_owned());
        }
        let key = text(field, "key", 256)
            .map_err(|_| schema_error("native question options are invalid"))?;
        if !group.is_empty() {
            if multiple {
                native.insert(
                    key.to_owned(),
                    Value::Array(group.iter().map(|s| Value::String(s.clone())).collect()),
                );
            } else {
                native.insert(key.to_owned(), Value::String(group[0].clone()));
            }
        }
        eliot.push(group);
    }
    Ok((eliot, Value::Object(native)))
}

/// True only for exact {decision:"reject"} (question cancel).
pub fn is_question_reject(body: &Value) -> Result<bool> {
    if body.get("decision").and_then(Value::as_str) != Some("reject") {
        return Ok(false);
    }
    let obj = body
        .as_object()
        .ok_or_else(|| invalid("native reply body must be an object"))?;
    if obj.len() != 1 || !obj.contains_key("decision") {
        return Err(invalid("native reply body contains unsupported fields"));
    }
    Ok(true)
}

fn exact_fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| schema_error("native interaction object has an invalid shape"))?;
    if required.iter().any(|f| !object.contains_key(*f))
        || object
            .keys()
            .any(|f| !required.contains(&f.as_str()) && !optional.contains(&f.as_str()))
    {
        return Err(schema_error(
            "native interaction has missing or unknown fields",
        ));
    }
    Ok(())
}

fn text<'a>(value: &'a Value, field: &str, maximum: usize) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|t| !t.trim().is_empty() && t.len() <= maximum)
        .ok_or_else(|| schema_error("native interaction field is missing or invalid"))
}

fn bounded_text(value: Option<&Value>, maximum: usize) -> Result<String> {
    value
        .and_then(Value::as_str)
        .filter(|t| !t.trim().is_empty() && t.len() <= maximum && !t.chars().any(char::is_control))
        .map(ToOwned::to_owned)
        .ok_or_else(|| schema_error("native interaction field is missing or invalid"))
}

fn is_frm(value: &str) -> bool {
    value.starts_with("frm_")
        && value.len() >= 4
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

fn is_ses(value: &str) -> bool {
    value.starts_with("ses_")
        && value.len() >= 4
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

fn invalid(message: &'static str) -> Error {
    Error::new("INVALID_NATIVE_INTERACTION", message)
}

fn schema_error(message: &'static str) -> Error {
    Error::new("NATIVE_SCHEMA_ERROR", message)
}
