//! Native request describe/reply for the OpenCode permission and form families.
//!
//! `permission.asked` opens an Eliot `approval` request and `form.created`
//! opens a `question` request, both bound to the exact session, the active
//! turn and the native request id (the public request id IS the native
//! `per_...`/`frm_...` id). Replies decide from the currently retained
//! offer; every staging error becomes a rejected (never applied, never
//! unknown) reply. Installed shapes come from `GET /openapi.json` of
//! `opencode serve` v2.0.32 (`Permission.Request`, `Permission.Reply`,
//! `Form.Info`) plus the `permission.ts` / `form.ts` event definitions at
//! tag `v2.0.32`, verified against a locally started server with no model
//! call. No I/O: the event processor owns the stream and the HTTP writes.

use eliot_core::{Error, Result, SecretPolicy};
use serde_json::{Map, Value};

use crate::native::types::{FormInfo, PermissionRequest};

/// Native request metadata returned to the EventProcessor for durable
/// opening. `details` carries the offered decision data the owner decides
/// from (raw native JSON, validated structurally first).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CorePending {
    pub id: String,
    pub turn_id: String,
    pub kind: String,
    pub details: Value,
}

/// One validated native reply: which route to write and its exact payload.
/// The Eliot command id is retained host-side; HTTP has no idempotency
/// handle, so a lost reply settles unknown and is never written again.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StagedReply {
    pub route: ReplyRoute,
    pub native_id: String,
}

/// The exact native write a staged reply performs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReplyRoute {
    Permission {
        decision: String,
        message: Option<String>,
    },
    Form {
        answer: Map<String, Value>,
    },
    FormCancel,
}

/// Describe a `permission.asked` payload as an `approval` request bound to
/// the exact owned session and the active turn. The event carries no turn;
/// the caller binds its current active turn (an ask with no active turn is
/// foreign or pre-readiness and never reaches describe).
pub(crate) fn describe_permission(
    asked: &PermissionRequest,
    expected_session: &str,
    active_turn: &str,
) -> Result<CorePending> {
    if asked.session_id != expected_session {
        return Err(Error::new(
            "source_integrity",
            "OpenCode permission request named another native session",
        ));
    }
    require_nonempty(&asked.id, "permission id")?;
    require_nonempty(&asked.action, "permission action")?;
    for resource in &asked.resources {
        require_nonempty(resource, "permission resource")?;
    }
    Ok(CorePending {
        id: asked.id.clone(),
        turn_id: active_turn.to_owned(),
        kind: "approval".to_owned(),
        details: serde_json::json!({
            "permission_id": asked.id,
            "action": asked.action,
            "resources": asked.resources,
            "save": asked.save,
            "message": asked.message,
        }),
    })
}

/// Describe a `form.created` payload as a `question` request bound to the
/// exact owned session and the active turn. The wire `sessionID` may carry
/// the upstream `"global"` sentinel for MCP elicitation; anything but the
/// exact owned session never binds.
pub(crate) fn describe_form(
    form: &FormInfo,
    expected_session: &str,
    active_turn: &str,
) -> Result<CorePending> {
    if form.session_id != expected_session {
        return Err(Error::new(
            "source_integrity",
            "OpenCode form named another native session",
        ));
    }
    require_nonempty(&form.id, "form id")?;
    require_nonempty(&form.title, "form title")?;
    if form.fields.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "OpenCode form offered no fields",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for field in &form.fields {
        require_nonempty(&field.key, "form field key")?;
        require_nonempty(&field.field_type, "form field type")?;
        if !seen.insert(field.key.as_str()) {
            return Err(Error::new(
                "source_integrity",
                "OpenCode form offered a duplicate field key",
            ));
        }
        for option in &field.options {
            require_nonempty(&option.value, "form option value")?;
            require_nonempty(&option.label, "form option label")?;
        }
    }
    let details = serde_json::to_value(form)
        .map_err(|_| Error::new("internal", "OpenCode form details could not be encoded"))?;
    Ok(CorePending {
        id: form.id.clone(),
        turn_id: active_turn.to_owned(),
        kind: "question".to_owned(),
        details,
    })
}

/// Stage the one-shot native write for a previously described request.
/// Every error here becomes a rejected reply: the caller's choice/answer
/// did not match the currently offered set.
pub(crate) fn reply_for(request: &CorePending, verb: &str, args: &Value) -> Result<StagedReply> {
    match (request.kind.as_str(), verb) {
        ("approval", "approve") => reply_to_approval(request, true, args),
        ("approval", "deny") => reply_to_approval(request, false, args),
        ("question", "answer") => reply_to_form(request, args),
        ("question", "deny") => reply_to_form_cancel(request, args),
        _ => Err(Error::new(
            "unsupported",
            "reply verb is unsupported for this native request",
        )),
    }
}

/// Approval replies: the installed offer is always the three decisions
/// (`once`, `always`, `reject`). Approve offers two decisions, so it must
/// name one with `choice_id` (no default choice); deny has only `reject`.
/// An explicit `choice_id` must name an offered decision the verb can
/// decide. `feedback` travels as the reply `message`.
fn reply_to_approval(request: &CorePending, approve: bool, args: &Value) -> Result<StagedReply> {
    let args = eliot_plugin::session::approval_reply_args(args)?;
    let offered = ["once", "always", "reject"];
    let wanted = match args.get("choice_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(choice)) if !choice.is_empty() => Some(choice.as_str()),
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "approval choice_id must be a non-empty string",
            ));
        }
    };
    if let Some(wanted) = wanted
        && !offered.contains(&wanted)
    {
        return Err(Error::new(
            "invalid",
            "approval choice was not among the offered choices",
        ));
    }
    let verb = if approve { "approve" } else { "deny" };
    let decision = match (approve, wanted) {
        (true, None) => {
            return Err(Error::new(
                "invalid",
                "approve must name its choice_id: the offer has two approving decisions (once, always)",
            ));
        }
        (false, None) => "reject",
        (true, Some("once" | "always")) => wanted.expect("checked offered decision"),
        (false, Some("reject")) => "reject",
        _ => {
            return Err(Error::new(
                "invalid",
                format!(
                    "approval choice {} cannot be decided by {verb}",
                    wanted.unwrap_or_default()
                ),
            ));
        }
    };
    let message = match args.get("feedback") {
        None | Some(Value::Null) => None,
        Some(Value::String(feedback)) => Some(feedback.clone()),
        Some(_) => {
            return Err(Error::new("invalid", "approval feedback must be a string"));
        }
    };
    let native_id = request
        .details
        .get("permission_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "pending request omitted its permission id",
            )
        })?;
    Ok(StagedReply {
        route: ReplyRoute::Permission {
            decision: decision.to_owned(),
            message,
        },
        native_id: native_id.to_owned(),
    })
}

/// Form answers: `args.answers` maps field keys to values. Keys must be
/// offered and option-bearing non-custom fields only accept offered values;
/// the native endpoint enforces the rest (required, `when`, value types)
/// and answers 400 when the answer violates them. Partial answers are
/// allowed: optional fields, defaults and conditions are the server's call.
fn reply_to_form(request: &CorePending, args: &Value) -> Result<StagedReply> {
    let args = args
        .as_object()
        .filter(|args| args.len() == 1)
        .ok_or_else(|| Error::new("invalid", "answer args must contain only answers"))?;
    let answers = args
        .get("answers")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("invalid", "answer args.answers must be an object"))?;
    let form: FormInfo = serde_json::from_value(request.details.clone()).map_err(|_| {
        Error::new(
            "source_integrity",
            "pending request omitted its offered form",
        )
    })?;
    let offered = form
        .fields
        .iter()
        .map(|field| (field.key.as_str(), field))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (key, value) in answers {
        let field = offered
            .get(key.as_str())
            .ok_or_else(|| Error::new("invalid", "answer names a field that was not offered"))?;
        check_offered_option(field, value)?;
    }
    let native_id = request
        .details
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "pending request omitted its form id"))?;
    Ok(StagedReply {
        route: ReplyRoute::Form {
            answer: answers.clone(),
        },
        native_id: native_id.to_owned(),
    })
}

/// A non-custom field with offered options only accepts offered values (a
/// single string, or every member of a string array for multiselect).
fn check_offered_option(field: &crate::native::types::FormField, value: &Value) -> Result<()> {
    if field.options.is_empty() || field.custom {
        return Ok(());
    }
    let values = match value {
        Value::String(one) => vec![one.as_str()],
        Value::Array(many) => {
            let mut values = Vec::with_capacity(many.len());
            for entry in many {
                values.push(entry.as_str().ok_or_else(|| {
                    Error::new("invalid", "answer option values must be strings")
                })?);
            }
            values
        }
        _ => {
            return Err(Error::new(
                "invalid",
                "answer option values must be strings",
            ));
        }
    };
    for value in values {
        if !field.options.iter().any(|option| option.value == value) {
            return Err(Error::new(
                "invalid",
                "answer was not among the offered choices",
            ));
        }
    }
    Ok(())
}

/// Deny on a question cancels the native form. No answers travel.
fn reply_to_form_cancel(request: &CorePending, args: &Value) -> Result<StagedReply> {
    if args.as_object().is_none_or(|args| !args.is_empty()) {
        return Err(Error::new(
            "invalid",
            "form cancel args must be an empty object",
        ));
    }
    let native_id = request
        .details
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "pending request omitted its form id"))?;
    Ok(StagedReply {
        route: ReplyRoute::FormCancel,
        native_id: native_id.to_owned(),
    })
}

fn require_nonempty(value: &str, what: &str) -> Result<()> {
    eliot_plugin::session::require_nonempty(value, "OpenCode", what)
}

/// Redact display text in committed request details with the session
/// secret policy. Structure (native ids, field keys/types) stays so the
/// offer keeps its shape; every human-readable string is redacted. Only
/// known secrets mask — ordinary offers pass through byte-identical — and
/// the in-memory offer stays raw so choice matching keeps working.
pub(crate) fn redact_details_for_commit(details: &Value, policy: &SecretPolicy) -> Value {
    let mut redacted = details.clone();
    if let Some(action) = redacted.get("action").and_then(Value::as_str) {
        redacted["action"] = Value::String(policy.redact_text(action));
    }
    if let Some(resources) = redacted.get("resources").and_then(Value::as_array) {
        redacted["resources"] = Value::Array(
            resources
                .iter()
                .map(|resource| match resource.as_str() {
                    Some(text) => Value::String(policy.redact_text(text)),
                    None => resource.clone(),
                })
                .collect(),
        );
    }
    if let Some(message) = redacted.get("message").and_then(Value::as_str) {
        redacted["message"] = Value::String(policy.redact_text(message));
    }
    if let Some(title) = redacted.get("title").and_then(Value::as_str) {
        redacted["title"] = Value::String(policy.redact_text(title));
    }
    if let Some(fields) = redacted.get_mut("fields").and_then(Value::as_array_mut) {
        for field in fields {
            redact_field_for_commit(field, policy);
        }
    }
    redacted
}

/// Redact one committed form field: titles, descriptions and option
/// labels/values mask known secrets; keys, types and flags stay.
fn redact_field_for_commit(field: &mut Value, policy: &SecretPolicy) {
    for key in ["title", "description", "placeholder", "pattern", "default"] {
        if let Some(text) = field.get(key).and_then(Value::as_str) {
            field[key] = Value::String(policy.redact_text(text));
        }
    }
    if let Some(options) = field.get_mut("options").and_then(Value::as_array_mut) {
        for option in options {
            for key in ["value", "label", "description"] {
                if let Some(text) = option.get(key).and_then(Value::as_str) {
                    option[key] = Value::String(policy.redact_text(text));
                }
            }
        }
    }
    if let Some(defaults) = field.get("default").and_then(Value::as_array) {
        field["default"] = Value::Array(
            defaults
                .iter()
                .map(|entry| match entry.as_str() {
                    Some(text) => Value::String(policy.redact_text(text)),
                    None => entry.clone(),
                })
                .collect(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn asked() -> PermissionRequest {
        PermissionRequest {
            id: "per_test1".to_owned(),
            session_id: "ses_test".to_owned(),
            action: "read".to_owned(),
            resources: vec!["C:/work/.env".to_owned()],
            save: Vec::new(),
            message: None,
        }
    }

    fn form() -> FormInfo {
        serde_json::from_value(json!({
            "id": "frm_test1", "sessionID": "ses_test", "title": "Pick one",
            "fields": [
                {"key": "choice", "title": "Pick", "type": "string",
                 "options": [{"value": "a", "label": "Alpha"},
                             {"value": "b", "label": "Beta"}]},
                {"key": "note", "type": "string"},
                {"key": "tags", "type": "multiselect",
                 "options": [{"value": "x", "label": "X"},
                             {"value": "y", "label": "Y"}]},
            ],
        }))
        .expect("test form")
    }

    #[test]
    fn permission_describe_binds_the_exact_session_and_turn() {
        let pending = describe_permission(&asked(), "ses_test", "turn-1").expect("described");
        assert_eq!(pending.id, "per_test1");
        assert_eq!(pending.turn_id, "turn-1");
        assert_eq!(pending.kind, "approval");
        assert_eq!(
            pending.details.get("permission_id").and_then(Value::as_str),
            Some("per_test1")
        );
        let error = describe_permission(&asked(), "ses_other", "turn-1").expect_err("foreign");
        assert_eq!(error.code, "source_integrity");
    }

    #[test]
    fn form_describe_rejects_foreign_and_empty_offers() {
        let pending = describe_form(&form(), "ses_test", "turn-1").expect("described");
        assert_eq!(pending.kind, "question");
        assert_eq!(
            pending.details.get("id").and_then(Value::as_str),
            Some("frm_test1")
        );
        let error = describe_form(&form(), "global", "turn-1").expect_err("sentinel");
        assert_eq!(error.code, "source_integrity");
        let mut empty = form();
        empty.fields.clear();
        let error = describe_form(&empty, "ses_test", "turn-1").expect_err("no fields");
        assert_eq!(error.code, "source_integrity");
        let mut dupe = form();
        dupe.fields.push(dupe.fields[0].clone());
        let error = describe_form(&dupe, "ses_test", "turn-1").expect_err("dupe key");
        assert_eq!(error.code, "source_integrity");
    }

    #[test]
    fn approval_replies_decide_from_the_offered_decisions() {
        let pending = describe_permission(&asked(), "ses_test", "turn-1").expect("described");
        // Approve has no default choice; deny has only `reject`.
        let error = reply_for(&pending, "approve", &json!({})).expect_err("no default");
        assert_eq!(error.code, "invalid");
        let staged =
            reply_for(&pending, "approve", &json!({"choice_id": "once"})).expect("approve once");
        assert_eq!(
            staged.route,
            ReplyRoute::Permission {
                decision: "once".to_owned(),
                message: None,
            }
        );
        assert_eq!(staged.native_id, "per_test1");
        let staged = reply_for(&pending, "deny", &json!({})).expect("deny");
        assert!(matches!(
            staged.route,
            ReplyRoute::Permission { ref decision, .. } if decision == "reject"
        ));
        // Explicit compatible choices with feedback.
        let staged = reply_for(
            &pending,
            "approve",
            &json!({"choice_id": "always", "feedback": "trust it"}),
        )
        .expect("always");
        assert_eq!(
            staged.route,
            ReplyRoute::Permission {
                decision: "always".to_owned(),
                message: Some("trust it".to_owned()),
            }
        );
        // Verb-incompatible and unknown choices are invalid, never unknown.
        for (verb, args) in [
            ("approve", json!({"choice_id": "reject"})),
            ("deny", json!({"choice_id": "always"})),
            ("approve", json!({"choice_id": "sometimes"})),
            ("approve", json!({"choice_id": 7})),
            ("approve", json!({"bogus": true})),
            ("answer", json!({})),
        ] {
            let error = reply_for(&pending, verb, &args).expect_err("rejected");
            assert!(
                matches!(error.code.as_str(), "invalid" | "unsupported"),
                "{verb} {args}: {}",
                error.code
            );
        }
    }

    #[test]
    fn form_answers_check_keys_and_offered_options() {
        let pending = describe_form(&form(), "ses_test", "turn-1").expect("described");
        let staged = reply_for(&pending, "answer", &json!({"answers": {"choice": "a"}}))
            .expect("offered option");
        assert!(matches!(staged.route, ReplyRoute::Form { .. }));
        assert_eq!(staged.native_id, "frm_test1");
        // Free-text fields and multiselect arrays pass their keys through.
        reply_for(
            &pending,
            "answer",
            &json!({"answers": {"note": "anything", "tags": ["x", "y"]}}),
        )
        .expect("free keys");
        // An offered value in either scalar or array shape stages; the
        // server owns the exact shape rule and answers 400 otherwise.
        reply_for(&pending, "answer", &json!({"answers": {"tags": "x"}})).expect("scalar");
        // Unknown keys and non-offered options are invalid.
        for answers in [
            json!({"answers": {"missing": "a"}}),
            json!({"answers": {"choice": "zzz"}}),
            json!({"answers": {"tags": ["x", "zzz"]}}),
            json!({"answers": {"tags": [7]}}),
        ] {
            let error = reply_for(&pending, "answer", &answers).expect_err("rejected");
            assert_eq!(error.code, "invalid", "{answers}");
        }
        // Deny cancels with no answers.
        let staged = reply_for(&pending, "deny", &json!({})).expect("cancel");
        assert_eq!(staged.route, ReplyRoute::FormCancel);
        let error = reply_for(&pending, "deny", &json!({"answers": {}})).expect_err("args");
        assert_eq!(error.code, "invalid");
        let error = reply_for(&pending, "approve", &json!({})).expect_err("verb");
        assert_eq!(error.code, "unsupported");
    }

    #[test]
    fn commit_redaction_masks_secrets_and_keeps_structure() {
        let policy = SecretPolicy::new(["hunter2".to_owned()]);
        let mut tainted = asked();
        tainted.message = Some("token hunter2 inside".to_owned());
        let pending = describe_permission(&tainted, "ses_test", "turn-1").expect("described");
        let committed = redact_details_for_commit(&pending.details, &policy);
        let encoded = serde_json::to_string(&committed).expect("encode");
        assert!(!encoded.contains("hunter2"), "{encoded}");
        assert_eq!(
            committed.get("permission_id").and_then(Value::as_str),
            Some("per_test1"),
            "ids stay"
        );
        // The in-memory offer stays raw so matching keeps working.
        assert!(
            serde_json::to_string(&pending.details)
                .expect("encode")
                .contains("hunter2")
        );
        let mut tainted_form = form();
        tainted_form.title = "rotate hunter2 now".to_owned();
        let pending = describe_form(&tainted_form, "ses_test", "turn-1").expect("described");
        let committed = redact_details_for_commit(&pending.details, &policy);
        let encoded = serde_json::to_string(&committed).expect("encode");
        assert!(!encoded.contains("hunter2"), "{encoded}");
        assert_eq!(
            committed
                .get("fields")
                .and_then(|fields| fields.get(0))
                .and_then(|field| field.get("key"))
                .and_then(Value::as_str),
            Some("choice"),
            "keys stay"
        );
    }
}
