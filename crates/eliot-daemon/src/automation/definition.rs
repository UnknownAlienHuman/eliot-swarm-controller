use eliot_core::{Error, Event, canonical_json_sha256, parse_agent_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    io::Read,
};

const MAX_RULE_FILE_BYTES: usize = 128 * 1024;
// Canonical Agent IDs are `a` plus at most 19 positive-i64 decimal digits.
const MAX_AGENT_ID_BYTES: usize = 20;
const SUPPORTED_EVENT: &str = "turn.completed";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDefinition {
    pub id: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    #[serde(rename = "on")]
    trigger: EventTrigger,
    #[serde(rename = "do")]
    actions: Vec<StopActionDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventTrigger {
    event: String,
    #[serde(default, rename = "match")]
    matches: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StopActionDefinition {
    action: String,
    target: Value,
    expected_generation: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopAction {
    pub target: String,
    pub expected_generation: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleFile {
    rule: Vec<toml::Value>,
}

fn enabled_by_default() -> bool {
    true
}

/// Reads and validates one bounded global rule file.
///
/// Directory loading, cross-file duplicate detection, startup high-water capture,
/// rate accounting and cursor settlement belong to the automation lifecycle owner.
pub fn load(reader: impl Read) -> Result<Vec<RuleDefinition>, Error> {
    let mut bytes = Vec::with_capacity(MAX_RULE_FILE_BYTES + 1);
    reader
        .take((MAX_RULE_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("unavailable", "cannot read rule file"))?;
    if bytes.len() > MAX_RULE_FILE_BYTES {
        return Err(Error::new("invalid", "rule file exceeds the 128 KiB bound"));
    }
    let source = String::from_utf8(bytes)
        .map_err(|_| Error::new("invalid", "rule file is not valid UTF-8"))?;
    let file: RuleFile = toml::from_str(&source)
        .map_err(|_| Error::new("invalid", "rule file is malformed or has unknown keys"))?;
    if file.rule.is_empty() {
        return Err(Error::new(
            "invalid",
            "rule file must contain at least one rule",
        ));
    }

    let mut rules = Vec::with_capacity(file.rule.len());
    let mut ids = HashSet::with_capacity(file.rule.len());
    for raw in file.rule {
        reject_unimplemented_options(&raw)?;
        let rule: RuleDefinition = raw
            .try_into()
            .map_err(|_| Error::new("invalid", "rule definition is malformed"))?;
        validate_rule(&rule)?;
        if !ids.insert(rule.id.clone()) {
            return Err(Error::new("invalid", "rule file contains duplicate ids"));
        }
        rules.push(rule);
    }
    Ok(rules)
}

impl RuleDefinition {
    /// Canonical semantic identity; comments and TOML key order do not change it.
    pub fn semantic_digest(&self) -> Result<[u8; 32], Error> {
        let value = serde_json::to_value(self)
            .map_err(|_| Error::new("invalid", "rule definition cannot be encoded"))?;
        canonical_json_sha256(&value)
            .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))
    }
}

/// Matches `Event.subject`, the captured event-time subject, and renders the stop action.
/// `None` means disabled, a different event, or no exact match. The caller advances
/// disabled/no-match cursors and settles rate-limit skips with the event transaction.
pub fn evaluate(rule: &RuleDefinition, event: &Event) -> Result<Option<StopAction>, Error> {
    if !rule.enabled || event.kind != rule.trigger.event {
        return Ok(None);
    }

    let event_value = serde_json::to_value(event)
        .map_err(|_| Error::new("invalid", "rule event cannot be encoded"))?;
    let subject_value = event
        .subject
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| Error::new("invalid", "rule subject cannot be encoded"))?;
    let subject = subject_value.as_ref();
    for (path, expected) in &rule.trigger.matches {
        let Some(actual) = lookup(path, &event_value, subject, &event.data) else {
            return Ok(None);
        };
        if actual != expected {
            return Ok(None);
        }
    }

    let action = rule
        .actions
        .first()
        .ok_or_else(|| Error::new("invalid", "rule has no stop action"))?;
    let roots = TemplateRoots {
        event: &event_value,
        subject,
        data: &event.data,
    };
    let target = render_agent_target(&action.target, &roots)?;
    parse_agent_id(&target)
        .map_err(|_| Error::new("invalid", "stop target is not a canonical Agent id"))?;
    let generation = render_generation(&action.expected_generation, &roots)?;
    let generation = generation
        .as_i64()
        .filter(|generation| *generation >= 0)
        .ok_or_else(|| Error::new("invalid", "stop generation did not render to an integer"))?;

    Ok(Some(StopAction {
        target,
        expected_generation: generation,
    }))
}

/// Inspect only known later-slice knobs before typed deserialization so they get an
/// explicit `unsupported` result. Required fields and their types are decoded once.
fn reject_unimplemented_options(raw: &toml::Value) -> Result<(), Error> {
    let Some(rule) = raw.as_table() else {
        return Ok(());
    };
    let has_unsupported_rule_option = ["limit", "catch_up", "allow_self"]
        .iter()
        .any(|key| rule.contains_key(*key));
    let has_unsupported_trigger =
        rule.get("on")
            .and_then(toml::Value::as_table)
            .is_some_and(|on| {
                ["every", "cron", "tz", "anchor", "at", "manual"]
                    .iter()
                    .any(|key| on.contains_key(*key))
            });
    let has_unsupported_selector =
        rule.get("do")
            .and_then(toml::Value::as_array)
            .is_some_and(|actions| {
                actions.iter().any(|action| {
                    action
                        .as_table()
                        .is_some_and(|action| action.contains_key("selector"))
                })
            });
    if has_unsupported_rule_option || has_unsupported_trigger || has_unsupported_selector {
        return Err(Error::new(
            "unsupported",
            "rule uses a trigger or option not supported in this increment",
        ));
    }
    Ok(())
}

fn validate_rule(rule: &RuleDefinition) -> Result<(), Error> {
    if rule.id.trim().is_empty() || rule.id.contains("${") {
        return Err(Error::new("invalid", "rule id must be a nonempty literal"));
    }
    if rule.trigger.event != SUPPORTED_EVENT {
        return Err(Error::new(
            "unsupported",
            "only turn.completed rules are supported",
        ));
    }
    for (path, expected) in &rule.trigger.matches {
        validate_path(path)?;
        match expected {
            Value::String(value) if value.starts_with("glob:") || value.starts_with("re:") => {
                return Err(Error::new(
                    "unsupported",
                    "only exact rule matches are supported",
                ));
            }
            Value::String(_) | Value::Number(_) | Value::Bool(_) => {}
            Value::Null | Value::Array(_) | Value::Object(_) => {
                return Err(Error::new(
                    "unsupported",
                    "only exact scalar rule matches are supported",
                ));
            }
        }
    }

    if rule.actions.len() != 1 {
        let code = if rule.actions.is_empty() {
            "invalid"
        } else {
            "unsupported"
        };
        return Err(Error::new(
            code,
            "rules currently support exactly one action",
        ));
    }
    let action = &rule.actions[0];
    if action.action != "stop" {
        return Err(Error::new(
            "unsupported",
            "only the stop action is supported",
        ));
    }
    let target = action
        .target
        .as_str()
        .ok_or_else(|| Error::new("invalid", "stop action requires a string target"))?;
    validate_template(target, true)?;
    match &action.expected_generation {
        Value::String(value) => {
            validate_template(value, false)?;
            if !is_whole_template(value) {
                return Err(Error::new(
                    "invalid",
                    "stop generation must be a nonnegative integer or whole-value template",
                ));
            }
        }
        Value::Number(value) if value.as_i64().is_some_and(|generation| generation >= 0) => {}
        _ => {
            return Err(Error::new(
                "invalid",
                "stop generation must be nonnegative or a whole-value template",
            ));
        }
    }
    Ok(())
}

fn validate_template(value: &str, embedded: bool) -> Result<(), Error> {
    if value.matches("${").count() != value.matches('}').count() {
        return Err(Error::new(
            "invalid",
            "rule template has an unmatched delimiter",
        ));
    }
    let mut rest = value;
    let mut found = false;
    while let Some(start) = rest.find("${") {
        found = true;
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(Error::new("invalid", "rule template is unterminated"));
        };
        let path = &after[..end];
        validate_path(path)?;
        rest = &after[end + 1..];
    }
    if value.contains('}') && !found {
        return Err(Error::new(
            "invalid",
            "rule template has an unmatched delimiter",
        ));
    }
    if found && !embedded && !is_whole_template(value) {
        return Err(Error::new(
            "invalid",
            "stop generation templates must occupy the whole value",
        ));
    }
    Ok(())
}

fn is_whole_template(value: &str) -> bool {
    value
        .strip_prefix("${")
        .and_then(|inner| inner.strip_suffix('}'))
        .is_some_and(|path| !path.is_empty() && !path.contains('{') && !path.contains('}'))
}

fn validate_path(path: &str) -> Result<(), Error> {
    let mut parts = path.split('.');
    let root = parts.next().unwrap_or_default();
    let Some(field) = parts.next() else {
        return Err(Error::new(
            "invalid",
            "rule paths must use event, subject or data followed by object keys",
        ));
    };
    let remaining: Vec<_> = parts.collect();
    if field.is_empty() || remaining.iter().any(|part| part.is_empty()) {
        return Err(Error::new("invalid", "rule path contains an empty key"));
    }

    let supported = match root {
        // Event data is a semantic JSON payload whose fields vary by event kind.
        "data" => true,
        "subject" => remaining.is_empty() && is_subject_field(field),
        "event" => match field {
            "data" => true,
            "subject" => remaining.len() == 1 && is_subject_field(remaining[0]),
            "seq" | "ts" | "kind" | "agent" | "generation" | "source" | "native_id" => {
                remaining.is_empty()
            }
            _ => false,
        },
        _ => {
            return Err(Error::new(
                "invalid",
                "rule paths must use event, subject or data roots",
            ));
        }
    };
    if !supported {
        return Err(Error::new(
            "unsupported",
            "rule path is not present in Core Event or EventSubject",
        ));
    }
    Ok(())
}

fn is_subject_field(field: &str) -> bool {
    matches!(
        field,
        "project" | "harness" | "profile" | "state" | "workspace"
    )
}

struct TemplateRoots<'a> {
    event: &'a Value,
    subject: Option<&'a Value>,
    data: &'a Value,
}

fn lookup<'a>(
    path: &str,
    event: &'a Value,
    subject: Option<&'a Value>,
    data: &'a Value,
) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut value = match parts.next()? {
        "event" => event,
        "subject" => subject?,
        "data" => data,
        _ => return None,
    };
    for part in parts {
        value = value.as_object()?.get(part)?;
    }
    Some(value)
}

fn render_agent_target(value: &Value, roots: &TemplateRoots<'_>) -> Result<String, Error> {
    let source = value
        .as_str()
        .ok_or_else(|| Error::new("invalid", "stop action requires a string target"))?;
    if is_whole_template(source) {
        let path = &source[2..source.len() - 1];
        let target = lookup(path, roots.event, roots.subject, roots.data)
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "stop target template is unavailable"))?;
        ensure_agent_id_capacity(target.len())?;
        return Ok(target.to_owned());
    }
    if !source.contains("${") {
        ensure_agent_id_capacity(source.len())?;
        return Ok(source.to_owned());
    }

    let mut rendered = String::new();
    let mut rest = source;
    while let Some(start) = rest.find("${") {
        push_agent_id_fragment(&mut rendered, &rest[..start])?;
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| Error::new("invalid", "rule template is unterminated"))?;
        let value = lookup(&after[..end], roots.event, roots.subject, roots.data)
            .ok_or_else(|| Error::new("invalid", "rule template value is unavailable"))?;
        let text = scalar_text(value)?;
        push_agent_id_fragment(&mut rendered, text.as_ref())?;
        rest = &after[end + 1..];
    }
    push_agent_id_fragment(&mut rendered, rest)?;
    Ok(rendered)
}

fn ensure_agent_id_capacity(length: usize) -> Result<(), Error> {
    if length > MAX_AGENT_ID_BYTES {
        return Err(Error::new(
            "invalid",
            "rendered stop target exceeds Agent id bounds",
        ));
    }
    Ok(())
}

fn push_agent_id_fragment(target: &mut String, fragment: &str) -> Result<(), Error> {
    if fragment.len() > MAX_AGENT_ID_BYTES.saturating_sub(target.len()) {
        return Err(Error::new(
            "invalid",
            "rendered stop target exceeds Agent id bounds",
        ));
    }
    target.push_str(fragment);
    Ok(())
}

fn render_generation(value: &Value, roots: &TemplateRoots<'_>) -> Result<Value, Error> {
    let Some(source) = value.as_str() else {
        return Ok(value.clone());
    };
    let path = source
        .strip_prefix("${")
        .and_then(|inner| inner.strip_suffix('}'))
        .ok_or_else(|| Error::new("invalid", "stop generation template is malformed"))?;
    lookup(path, roots.event, roots.subject, roots.data)
        .cloned()
        .ok_or_else(|| Error::new("invalid", "stop generation template is unavailable"))
}

fn scalar_text(value: &Value) -> Result<Cow<'_, str>, Error> {
    match value {
        Value::String(value) => Ok(Cow::Borrowed(value)),
        Value::Number(value) => Ok(Cow::Owned(value.to_string())),
        Value::Bool(true) => Ok(Cow::Borrowed("true")),
        Value::Bool(false) => Ok(Cow::Borrowed("false")),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(Error::new(
            "invalid",
            "embedded rule template values must be scalar",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    const RULE: &str = r#"
[[rule]]
id = 'stop-on-result'
on = { event = 'turn.completed', match = { 'subject.harness' = 'fake' } }
do = [{ action = 'stop', target = '${event.agent}', expected_generation = '${event.generation}' }]
"#;

    fn event() -> Event {
        Event {
            seq: 7,
            ts: 100,
            kind: "turn.completed".to_owned(),
            agent: Some("a7".to_owned()),
            subject: Some(eliot_core::EventSubject {
                project: "project-test".to_owned(),
                harness: "fake".to_owned(),
                profile: "fake".to_owned(),
                state: eliot_core::AgentState::Busy,
                workspace: Some("workspace-test".to_owned()),
            }),
            generation: Some(3),
            source: "stream".to_owned(),
            native_id: Some("fake:turn.completed".to_owned()),
            data: json!({"command_id":"send-id", "turn_id":"turn-id", "status":"completed"}),
        }
    }

    #[test]
    fn load_rejects_invalid_and_unimplemented_rule_fields() {
        let unsupported = [
            RULE.replace("do =", "limit = { max = 2 }\ndo ="),
            RULE.replace("event = 'turn.completed'", "every = '30m', event = 'turn.completed'"),
            RULE.replace("action = 'stop'", "action = 'send'"),
            RULE.replace("'subject.harness' = 'fake'", "'subject.harness' = ['fake']"),
            RULE.replace("'subject.harness' = 'fake'", "'subject.labels.role' = 'writer'"),
            RULE.replace("'subject.harness' = 'fake'", "'event.parent' = 'command-id'"),
            RULE.replace(
                "expected_generation = '${event.generation}' }]",
                "expected_generation = '${event.generation}' }, { action = 'stop', target = '${event.agent}', expected_generation = '${event.generation}' }]",
            ),
        ];
        for source in unsupported {
            let error = load(Cursor::new(source)).expect_err("unsupported rule shape");
            assert_eq!(error.code, "unsupported");
        }

        let unknown = RULE.replace(
            "id = 'stop-on-result'",
            "id = 'stop-on-result'\nextra = true",
        );
        assert_eq!(load(Cursor::new(unknown)).unwrap_err().code, "invalid");
        let duplicate = format!("{RULE}\n{RULE}");
        assert_eq!(load(Cursor::new(duplicate)).unwrap_err().code, "invalid");
    }

    #[test]
    fn missing_event_time_subject_does_not_match() {
        let rule = load(Cursor::new(RULE)).unwrap().remove(0);
        let mut event = event();
        event.subject = None;
        assert_eq!(evaluate(&rule, &event).unwrap(), None);
    }

    #[test]
    fn whole_generation_template_keeps_integer_type() {
        let rule = load(Cursor::new(RULE)).unwrap().remove(0);
        let action = evaluate(&rule, &event())
            .unwrap()
            .expect("matching stop action");
        assert_eq!(action.target, "a7");
        assert_eq!(action.expected_generation, 3);
    }

    #[test]
    fn repeated_large_target_expansion_stops_at_agent_id_bound() {
        let mut target = String::new();
        for _ in 0..4_000 {
            target.push_str("${data.expansion}");
        }
        let source = RULE.replace("target = '${event.agent}'", &format!("target = '{target}'"));
        assert!(source.len() <= MAX_RULE_FILE_BYTES);
        let rule = load(Cursor::new(source)).unwrap().remove(0);
        let mut event = event();
        event.data["expansion"] = Value::String("x".repeat(64 * 1024));

        let error = evaluate(&rule, &event).unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(
            error.message,
            "rendered stop target exceeds Agent id bounds"
        );
    }

    #[test]
    fn semantic_digest_ignores_toml_key_order_and_default_enabled() {
        let first = load(Cursor::new(RULE)).unwrap().remove(0);
        let second = load(Cursor::new(RULE.replace(
            "id = 'stop-on-result'",
            "enabled = true\nid = 'stop-on-result'",
        )))
        .unwrap()
        .remove(0);
        assert_eq!(
            first.semantic_digest().unwrap(),
            second.semantic_digest().unwrap()
        );
    }
}
