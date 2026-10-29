use eliot_core::{
    Error, Event,
    api::{CatchUp, TimerSchedule},
    canonical_json_sha256, parse_agent_id,
};
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
    trigger: TriggerDefinition,
    #[serde(rename = "do")]
    actions: Vec<StopActionDefinition>,
    #[serde(
        default = "default_catch_up",
        skip_serializing_if = "is_default_catch_up"
    )]
    catch_up: CatchUp,
    #[serde(skip)]
    schedule: Option<TimerSchedule>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event: Option<String>,
    #[serde(default, rename = "match")]
    matches: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    every: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cron: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tz: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    at: Option<String>,
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

fn default_catch_up() -> CatchUp {
    CatchUp::None
}

fn is_default_catch_up(catch_up: &CatchUp) -> bool {
    matches!(catch_up, CatchUp::None)
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
        let mut rule: RuleDefinition = raw
            .try_into()
            .map_err(|_| Error::new("invalid", "rule definition is malformed"))?;
        validate_rule(&mut rule)?;
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

    pub fn schedule(&self) -> Option<&TimerSchedule> {
        self.schedule.as_ref()
    }

    pub fn catch_up(&self) -> &CatchUp {
        &self.catch_up
    }

    /// Render the frozen literal stop action. Lifecycle and admission gates are
    /// enforced by the caller before invoking this projection.
    pub fn evaluate_timer(&self) -> Result<StopAction, Error> {
        if self.schedule.is_none() {
            return Err(Error::new("unsupported", "rule is not a timer"));
        }
        timer_stop_action(self)
    }
}

impl TriggerDefinition {
    fn has_timer_fields(&self) -> bool {
        self.every.is_some()
            || self.anchor.is_some()
            || self.cron.is_some()
            || self.tz.is_some()
            || self.at.is_some()
    }
}

/// Matches `Event.subject`, the captured event-time subject, and renders the stop action.
/// `None` means disabled, a different event, or no exact match. The caller advances
/// disabled/no-match cursors and settles rate-limit skips with the event transaction.
pub fn evaluate(rule: &RuleDefinition, event: &Event) -> Result<Option<StopAction>, Error> {
    if !rule.enabled
        || event.kind != SUPPORTED_EVENT
        || rule.trigger.event.as_deref() != Some(event.kind.as_str())
    {
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
    let has_unsupported_rule_option = ["limit", "allow_self"]
        .iter()
        .any(|key| rule.contains_key(*key));
    let has_unsupported_trigger = rule
        .get("on")
        .and_then(toml::Value::as_table)
        .is_some_and(|on| ["manual"].iter().any(|key| on.contains_key(*key)));
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

fn validate_rule(rule: &mut RuleDefinition) -> Result<(), Error> {
    if rule.id.trim().is_empty() || rule.id.contains("${") {
        return Err(Error::new("invalid", "rule id must be a nonempty literal"));
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

    if let Some(event) = rule.trigger.event.as_deref() {
        if rule.trigger.has_timer_fields() {
            return Err(Error::new(
                "invalid",
                "rule on cannot mix event and timer triggers",
            ));
        }
        if event != SUPPORTED_EVENT {
            return Err(Error::new(
                "unsupported",
                "only turn.completed event rules are supported",
            ));
        }
        if !is_default_catch_up(&rule.catch_up) {
            return Err(Error::new("invalid", "catch_up requires a timer trigger"));
        }
        validate_event_matches(&rule.trigger.matches)?;
        validate_event_action(action)?;
        rule.schedule = None;
        return Ok(());
    }

    if !rule.trigger.matches.is_empty() {
        return Err(Error::new("invalid", "match requires an event trigger"));
    }
    let schedule = match (
        rule.trigger.every.as_deref(),
        rule.trigger.cron.as_deref(),
        rule.trigger.at.as_deref(),
    ) {
        (Some(every), None, None) if rule.trigger.tz.is_none() => {
            super::schedule::parse_interval(every, rule.trigger.anchor)?
        }
        (None, Some(expression), None) if rule.trigger.anchor.is_none() => {
            super::schedule::parse_cron(expression, rule.trigger.tz.as_deref().unwrap_or("UTC"))?
        }
        (None, None, Some(at)) if rule.trigger.anchor.is_none() && rule.trigger.tz.is_none() => {
            super::schedule::parse_once(at)?
        }
        _ => {
            return Err(Error::new(
                "invalid",
                "rule on must select exactly one interval, cron or once trigger",
            ));
        }
    };
    schedule.validate()?;
    rule.schedule = Some(schedule);
    timer_stop_action(rule)?;
    Ok(())
}

fn validate_event_matches(matches: &BTreeMap<String, Value>) -> Result<(), Error> {
    for (path, expected) in matches {
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
    Ok(())
}

fn validate_event_action(action: &StopActionDefinition) -> Result<(), Error> {
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

fn timer_stop_action(rule: &RuleDefinition) -> Result<StopAction, Error> {
    let action = rule
        .actions
        .first()
        .ok_or_else(|| Error::new("invalid", "rule has no stop action"))?;
    let target = action
        .target
        .as_str()
        .ok_or_else(|| Error::new("invalid", "timer stop target must be a string"))?;
    if target.contains("${") || target.contains('}') {
        return Err(Error::new(
            "invalid",
            "timer stop target cannot use templates without event data",
        ));
    }
    ensure_agent_id_capacity(target.len())?;
    parse_agent_id(target)
        .map_err(|_| Error::new("invalid", "timer stop target is not a canonical Agent id"))?;
    let expected_generation = action
        .expected_generation
        .as_i64()
        .filter(|generation| *generation >= 0)
        .ok_or_else(|| {
            Error::new(
                "invalid",
                "timer stop generation must be a nonnegative integer without templates",
            )
        })?;
    Ok(StopAction {
        target: target.to_owned(),
        expected_generation,
    })
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

    const INTERVAL_TIMER: &str = r#"
[[rule]]
id = 'interval-stop'
on = { every = '30m' }
do = [{ action = 'stop', target = 'a7', expected_generation = 3 }]
"#;

    const CRON_TIMER: &str = r#"
[[rule]]
id = 'cron-stop'
on = { cron = '0 12 * * *' }
do = [{ action = 'stop', target = 'a7', expected_generation = 3 }]
"#;

    const ONCE_TIMER: &str = r#"
[[rule]]
id = 'once-stop'
on = { at = '1970-01-01T00:00:01Z' }
do = [{ action = 'stop', target = 'a7', expected_generation = 3 }]
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
        let mixed = RULE.replace(
            "event = 'turn.completed'",
            "event = 'turn.completed', every = '30m'",
        );
        assert_eq!(load(Cursor::new(mixed)).unwrap_err().code, "invalid");
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

    #[test]
    fn timer_on_forms_parse_to_core_schedules_and_literal_stop() {
        let interval = load(Cursor::new(INTERVAL_TIMER)).unwrap().remove(0);
        assert_eq!(
            interval.schedule(),
            Some(&TimerSchedule::Interval {
                period_ms: 30 * 60 * 1_000,
                anchor: None,
            })
        );
        assert_eq!(interval.catch_up(), &CatchUp::None);
        assert_eq!(
            interval.evaluate_timer().unwrap(),
            StopAction {
                target: "a7".to_owned(),
                expected_generation: 3,
            }
        );

        let cron = load(Cursor::new(CRON_TIMER)).unwrap().remove(0);
        assert!(matches!(
            cron.schedule(),
            Some(TimerSchedule::Cron { expression, tz })
                if expression.as_str() == "0 12 * * *" && tz.as_str() == "UTC"
        ));

        let once = load(Cursor::new(ONCE_TIMER)).unwrap().remove(0);
        assert_eq!(once.schedule(), Some(&TimerSchedule::Once { at: 1_000 }));
    }

    #[test]
    fn timer_explicit_latest_catch_up_is_typed() {
        let latest_source = INTERVAL_TIMER.replace(
            "on = { every = '30m' }",
            "on = { every = '30m' }\ncatch_up = 'latest'",
        );
        let latest = load(Cursor::new(latest_source)).unwrap().remove(0);
        assert_eq!(latest.catch_up(), &CatchUp::Latest);
    }

    #[test]
    fn timer_rejects_unresolved_templates_and_mixed_trigger_forms() {
        let templated = INTERVAL_TIMER.replace("target = 'a7'", "target = '${event.agent}'");
        assert_eq!(load(Cursor::new(templated)).unwrap_err().code, "invalid");

        let mixed = INTERVAL_TIMER.replace("every = '30m'", "every = '30m', cron = '0 12 * * *'");
        assert_eq!(load(Cursor::new(mixed)).unwrap_err().code, "invalid");
    }
}
