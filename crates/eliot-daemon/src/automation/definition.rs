use eliot_core::{
    Error, Event, SubmitCommand,
    api::{
        CatchUp, CommandSubmitRequest, RuleLimit, RuleLimitScope, RunDefinitionArgs, TimerSchedule,
    },
    canonical_json_sha256,
    limits::{MAX_EVENT_BYTES, MAX_TEXT_BYTES},
    parse_agent_id,
};
use globset::{Glob, GlobMatcher};
use regex::Regex;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    io::Read,
};

const MAX_RULE_FILE_BYTES: usize = 128 * 1024;
// Canonical Agent IDs are `a` plus at most 19 positive-i64 decimal digits.
const MAX_AGENT_ID_BYTES: usize = 20;
const MAX_RULE_CAUSE_DEPTH: u8 = 5;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDefinition {
    pub id: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    #[serde(rename = "on")]
    trigger: TriggerDefinition,
    #[serde(rename = "do")]
    actions: Vec<ActionDefinition>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_rule_limit"
    )]
    limit: Option<RuleLimit>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_self: bool,
    #[serde(
        default = "default_catch_up",
        skip_serializing_if = "is_default_catch_up"
    )]
    catch_up: CatchUp,
    #[serde(skip)]
    schedule: Option<TimerSchedule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(skip)]
    compiled_matches: BTreeMap<String, CompiledMatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum AgentActionDefinition {
    Stop {
        target: Value,
        expected_generation: Value,
    },
    Send {
        target: Value,
        expected_generation: Value,
        text: Value,
    },
    Interrupt {
        target: Value,
        expected_generation: Value,
        expected_turn_id: Value,
    },
    Steer {
        target: Value,
        expected_generation: Value,
        expected_turn_id: Value,
        text: Value,
    },
    Approve {
        target: Value,
        expected_generation: Value,
        expected_turn_id: Value,
        request_id: Value,
    },
    Deny {
        target: Value,
        expected_generation: Value,
        expected_turn_id: Value,
        request_id: Value,
    },
    Answer {
        target: Value,
        expected_generation: Value,
        expected_turn_id: Value,
        request_id: Value,
        answers: BTreeMap<String, Value>,
    },
    Resume {
        target: Value,
        expected_generation: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<Value>,
    },
    Launch {
        profile: String,
        workspace: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<Value>,
    },
}

#[derive(Clone, Debug)]
enum ActionDefinition {
    Agent(AgentActionDefinition),
    Run { definition: RunDefinitionArgs },
}

impl<'de> Deserialize<'de> for ActionDefinition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("rule action must be an object"))?;
        let action = object
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("rule action kind is missing"))?
            .to_owned();
        if action == "run" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                serde_json::from_value(Value::Object(fields)).map_err(D::Error::custom)?;
            return Ok(Self::Run { definition });
        }
        serde_json::from_value::<AgentActionDefinition>(value)
            .map(Self::Agent)
            .map_err(D::Error::custom)
    }
}

impl Serialize for ActionDefinition {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Agent(action) => action.serialize(serializer),
            Self::Run { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "run definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("run".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleLimitInput {
    max: u64,
    per: RuleLimitScope,
    #[serde(default)]
    window: Option<String>,
    #[serde(default)]
    window_ms: Option<i64>,
}

#[derive(Clone, Debug)]
enum CompiledMatch {
    Exact(Value),
    Glob(GlobMatcher),
    Regex(Regex),
    OneOf(Vec<Value>),
    Exists(bool),
}

impl CompiledMatch {
    fn matches(&self, actual: Option<&Value>) -> bool {
        match self {
            Self::Exact(expected) => actual == Some(expected),
            Self::Glob(pattern) => actual
                .and_then(Value::as_str)
                .is_some_and(|value| pattern.is_match(value)),
            Self::Regex(pattern) => actual
                .and_then(Value::as_str)
                .is_some_and(|value| pattern.is_match(value)),
            Self::OneOf(choices) => actual.is_some_and(|value| choices.contains(value)),
            Self::Exists(expected) => actual.is_some() == *expected,
        }
    }
}

impl AgentActionDefinition {
    fn target(&self) -> Option<&Value> {
        match self {
            Self::Stop { target, .. }
            | Self::Send { target, .. }
            | Self::Interrupt { target, .. }
            | Self::Steer { target, .. }
            | Self::Approve { target, .. }
            | Self::Deny { target, .. }
            | Self::Answer { target, .. }
            | Self::Resume { target, .. } => Some(target),
            Self::Launch { .. } => None,
        }
    }

    fn expected_generation(&self) -> Option<&Value> {
        match self {
            Self::Stop {
                expected_generation,
                ..
            }
            | Self::Send {
                expected_generation,
                ..
            }
            | Self::Interrupt {
                expected_generation,
                ..
            }
            | Self::Steer {
                expected_generation,
                ..
            }
            | Self::Approve {
                expected_generation,
                ..
            }
            | Self::Deny {
                expected_generation,
                ..
            }
            | Self::Answer {
                expected_generation,
                ..
            }
            | Self::Resume {
                expected_generation,
                ..
            } => Some(expected_generation),
            Self::Launch { .. } => None,
        }
    }

    fn text(&self) -> Option<(&Value, usize, &'static str)> {
        match self {
            Self::Send { text, .. } => Some((text, MAX_EVENT_BYTES, "64 KiB persisted-text bound")),
            Self::Steer { text, .. } => Some((text, MAX_TEXT_BYTES, "512 KiB text bound")),
            Self::Launch {
                text: Some(text), ..
            }
            | Self::Resume {
                text: Some(text), ..
            } => Some((text, MAX_EVENT_BYTES, "64 KiB persisted-text bound")),
            _ => None,
        }
    }

    fn turn_binding(&self) -> Option<(&Value, &'static str)> {
        match self {
            Self::Interrupt {
                expected_turn_id, ..
            } => Some((expected_turn_id, "interrupt")),
            Self::Steer {
                expected_turn_id, ..
            } => Some((expected_turn_id, "steer")),
            Self::Approve {
                expected_turn_id, ..
            } => Some((expected_turn_id, "approve")),
            Self::Deny {
                expected_turn_id, ..
            } => Some((expected_turn_id, "deny")),
            Self::Answer {
                expected_turn_id, ..
            } => Some((expected_turn_id, "answer")),
            _ => None,
        }
    }

    fn request_binding(&self) -> Option<(&Value, &'static str)> {
        match self {
            Self::Approve { request_id, .. } => Some((request_id, "approve")),
            Self::Deny { request_id, .. } => Some((request_id, "deny")),
            Self::Answer { request_id, .. } => Some((request_id, "answer")),
            _ => None,
        }
    }

    fn answers(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Self::Answer { answers, .. } => Some(answers),
            _ => None,
        }
    }

    fn verb(&self) -> &'static str {
        match self {
            Self::Stop { .. } => "stop",
            Self::Send { .. } => "send",
            Self::Interrupt { .. } => "interrupt",
            Self::Steer { .. } => "steer",
            Self::Approve { .. } => "approve",
            Self::Deny { .. } => "deny",
            Self::Answer { .. } => "answer",
            Self::Resume { .. } => "resume",
            Self::Launch { .. } => "launch",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleFile {
    rule: Vec<toml::Value>,
}

fn enabled_by_default() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !value
}

fn default_catch_up() -> CatchUp {
    CatchUp::None
}

fn is_default_catch_up(catch_up: &CatchUp) -> bool {
    matches!(catch_up, CatchUp::None)
}

fn deserialize_rule_limit<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<RuleLimit>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(input) = Option::<RuleLimitInput>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let window_ms = match (input.window, input.window_ms) {
        (Some(window), None) => super::schedule::parse_positive_duration_ms(&window)
            .map_err(|_| D::Error::custom("limit window must be positive whole milliseconds"))?,
        (None, Some(window_ms)) => window_ms,
        _ => {
            return Err(D::Error::custom(
                "limit requires exactly one window or normalized window_ms",
            ));
        }
    };
    let limit = RuleLimit {
        max: input.max,
        per: input.per,
        window_ms,
    };
    limit.validate().map_err(D::Error::custom)?;
    Ok(Some(limit))
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

    /// Digest the normalized schedule and catch-up policy, excluding action-only edits.
    pub fn schedule_digest(&self) -> Result<Option<[u8; 32]>, Error> {
        let Some(schedule) = &self.schedule else {
            return Ok(None);
        };
        let value = serde_json::to_value((schedule, &self.catch_up))
            .map_err(|_| Error::new("invalid", "timer schedule cannot be encoded"))?;
        canonical_json_sha256(&value)
            .map(Some)
            .map_err(|_| Error::new("invalid", "timer schedule cannot be canonicalized"))
    }

    /// Restore a previously validated canonical definition and rebuild skipped caches.
    pub fn deserialize_saved(source: &str) -> Result<Self, Error> {
        if source.len() > MAX_RULE_FILE_BYTES {
            return Err(Error::new(
                "invalid",
                "saved rule definition exceeds the 128 KiB bound",
            ));
        }
        let mut definition: Self = serde_json::from_str(source)
            .map_err(|_| Error::new("invalid", "saved rule definition is malformed"))?;
        validate_rule(&mut definition)?;
        Ok(definition)
    }

    pub fn schedule(&self) -> Option<&TimerSchedule> {
        self.schedule.as_ref()
    }

    pub fn catch_up(&self) -> &CatchUp {
        &self.catch_up
    }

    /// Render the frozen literal Agent command. Lifecycle and admission gates are
    /// enforced by the caller before invoking this projection.
    pub fn evaluate_timer(&self) -> Result<SubmitCommand, Error> {
        if self.schedule.is_none() {
            return Err(Error::new("unsupported", "rule is not a timer"));
        }
        timer_action(self)
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

/// Matches a committed event and renders its one bound Agent command.
/// `None` means disabled, a different event, or no exact match. The caller advances
/// disabled/no-match cursors and settles rate-limit skips with the event transaction.
pub fn evaluate(rule: &RuleDefinition, event: &Event) -> Result<Option<SubmitCommand>, Error> {
    if !rule.enabled || rule.trigger.event.as_deref() != Some(event.kind.as_str()) {
        return Ok(None);
    }
    if event
        .cause
        .as_ref()
        .is_some_and(|cause| cause.depth >= MAX_RULE_CAUSE_DEPTH)
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
    for (path, matcher) in &rule.trigger.compiled_matches {
        if !matcher.matches(lookup(path, &event_value, subject, &event.data)) {
            return Ok(None);
        }
    }

    let action = rule
        .actions
        .first()
        .ok_or_else(|| Error::new("invalid", "rule has no action"))?;
    let action = match action {
        ActionDefinition::Run { definition } => return submit_run_command(definition).map(Some),
        ActionDefinition::Agent(action) => action,
    };
    let roots = TemplateRoots {
        event: &event_value,
        subject,
        data: &event.data,
    };
    if let AgentActionDefinition::Launch {
        profile, workspace, ..
    } = action
    {
        let text = action
            .text()
            .map(|(text, max_bytes, limit_label)| {
                render_text(text, &roots, max_bytes, action.verb(), limit_label)
            })
            .transpose()?;
        return submit_launch_command(profile, workspace, text).map(Some);
    }
    let target_source = action
        .target()
        .ok_or_else(|| Error::new("invalid", "Agent action target is missing"))?;
    let target = render_agent_target(target_source, &roots)?;
    parse_agent_id(&target)
        .map_err(|_| Error::new("invalid", "action target is not a canonical Agent id"))?;
    let generation_source = action
        .expected_generation()
        .ok_or_else(|| Error::new("invalid", "Agent action generation is missing"))?;
    let generation = render_generation(generation_source, &roots)?;
    let generation = generation
        .as_i64()
        .filter(|generation| *generation >= 0)
        .ok_or_else(|| Error::new("invalid", "action generation did not render to an integer"))?;
    let event_target = event
        .agent
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "rule action requires an event-time Agent"))?;
    let event_generation = event
        .generation
        .filter(|generation| *generation >= 0)
        .ok_or_else(|| Error::new("invalid", "rule action requires an event-time generation"))?;
    if target != event_target || generation != event_generation {
        return Err(Error::new(
            "invalid",
            "rule action must match the event-time Agent and generation",
        ));
    }
    let verb = action.verb();
    let args = action
        .text()
        .map(|(text, max_bytes, limit_label)| {
            render_text(text, &roots, max_bytes, verb, limit_label)
        })
        .transpose()?
        .map(|text| json!({"text": text}))
        .or_else(|| action.answers().map(|answers| json!({"answers": answers})))
        .unwrap_or_else(|| json!({}));
    let expected_turn_id = action
        .turn_binding()
        .map(|(source, verb)| {
            render_event_binding(source, &roots, event, verb, "turn_id", "expected_turn_id")
        })
        .transpose()?;
    let request_id = action
        .request_binding()
        .map(|(source, verb)| {
            render_event_binding(source, &roots, event, verb, "request_id", "request_id")
        })
        .transpose()?;

    Ok(Some(submit_command(
        action.verb(),
        target,
        generation,
        args,
        expected_turn_id,
        request_id,
    )))
}

fn render_event_binding(
    source: &Value,
    roots: &TemplateRoots<'_>,
    event: &Event,
    verb: &str,
    event_field: &str,
    action_field: &str,
) -> Result<String, Error> {
    let rendered_event_field = render_exact_string(source, roots, verb, action_field)?;
    let event_turn_id = event
        .data
        .get(event_field)
        .and_then(Value::as_str)
        .filter(|turn_id| !turn_id.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{verb} requires event.data.{event_field} as a string"),
            )
        })?;
    if rendered_event_field != event_turn_id {
        return Err(Error::new(
            "invalid",
            format!("{verb} {action_field} must match event.data.{event_field}"),
        ));
    }
    Ok(event_turn_id.to_owned())
}

fn submit_run_command(definition: &RunDefinitionArgs) -> Result<SubmitCommand, Error> {
    definition.validate()?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "run definition cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "run");
    command.args = args;
    command.validate()?;
    Ok(command)
}

fn submit_launch_command(
    profile: &str,
    workspace: &str,
    text: Option<String>,
) -> Result<SubmitCommand, Error> {
    let mut command = CommandSubmitRequest::new(String::new(), "launch");
    command.expected_generation = Some(0);
    command.args = json!({"profile": profile, "workspace": workspace});
    if let Some(text) = text {
        command.args["text"] = Value::String(text);
    }
    command.validate()?;
    Ok(command)
}

/// Inspect only known later-slice knobs before typed deserialization so they get an
/// explicit `unsupported` result. Required fields and their types are decoded once.
fn reject_unimplemented_options(raw: &toml::Value) -> Result<(), Error> {
    let Some(rule) = raw.as_table() else {
        return Ok(());
    };
    if rule
        .get("limit")
        .and_then(toml::Value::as_table)
        .is_some_and(|limit| limit.contains_key("window_ms"))
    {
        return Err(Error::new(
            "invalid",
            "rule files express limit windows as durations",
        ));
    }
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
    let has_unsupported_launch_option =
        rule.get("do")
            .and_then(toml::Value::as_array)
            .is_some_and(|actions| {
                actions.iter().any(|action| {
                    let Some(action_fields) = action.as_table() else {
                        return false;
                    };
                    action_fields.get("action").and_then(toml::Value::as_str) == Some("launch")
                        && [
                            "worktree",
                            "base",
                            "setup",
                            "model",
                            "effort",
                            "permissions",
                            "executable",
                        ]
                        .iter()
                        .any(|field| action_fields.contains_key(*field))
                })
            });
    let has_unsupported_action =
        rule.get("do")
            .and_then(toml::Value::as_array)
            .is_some_and(|actions| {
                actions.iter().any(|action| {
                    action
                        .as_table()
                        .and_then(|action| action.get("action"))
                        .and_then(toml::Value::as_str)
                        .is_some_and(|action| {
                            !matches!(
                                action,
                                "stop"
                                    | "send"
                                    | "interrupt"
                                    | "steer"
                                    | "approve"
                                    | "deny"
                                    | "answer"
                                    | "launch"
                                    | "resume"
                                    | "run"
                            )
                        })
                })
            });
    if has_unsupported_launch_option {
        return Err(Error::new(
            "unsupported",
            "launch accepts only profile, workspace, and optional text; worktree and per-action overrides are unsupported",
        ));
    }
    if has_unsupported_trigger || has_unsupported_selector || has_unsupported_action {
        return Err(Error::new(
            "unsupported",
            "rule uses a trigger, action or option not supported in this increment",
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
    if let Some(event) = rule.trigger.event.clone() {
        if rule.trigger.has_timer_fields() {
            return Err(Error::new(
                "invalid",
                "rule on cannot mix event and timer triggers",
            ));
        }
        if event.trim().is_empty() || event.contains("${") {
            return Err(Error::new(
                "invalid",
                "event trigger must be a nonempty literal event kind",
            ));
        }
        if !is_default_catch_up(&rule.catch_up) {
            return Err(Error::new("invalid", "catch_up requires a timer trigger"));
        }
        if matches!(action, ActionDefinition::Agent(agent) if agent.request_binding().is_some())
            && event != "request.opened"
        {
            return Err(Error::new(
                "invalid",
                "request reply actions require a request.opened trigger",
            ));
        }
        validate_event_matches(&mut rule.trigger)?;
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
    timer_action(rule)?;
    Ok(())
}

fn validate_event_matches(trigger: &mut TriggerDefinition) -> Result<(), Error> {
    let mut compiled = BTreeMap::new();
    for (path, expected) in &trigger.matches {
        validate_path(path)?;
        compiled.insert(path.clone(), compile_match(expected)?);
    }
    trigger.compiled_matches = compiled;
    Ok(())
}

fn compile_match(value: &Value) -> Result<CompiledMatch, Error> {
    match value {
        Value::String(value) if let Some(pattern) = value.strip_prefix("glob:") => {
            let glob = Glob::new(pattern)
                .map_err(|_| Error::new("invalid", "rule glob pattern is invalid"))?;
            Ok(CompiledMatch::Glob(glob.compile_matcher()))
        }
        Value::String(value) if let Some(pattern) = value.strip_prefix("re:") => {
            Regex::new(pattern)
                .map(CompiledMatch::Regex)
                .map_err(|_| Error::new("invalid", "rule regex pattern is invalid"))
        }
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
            Ok(CompiledMatch::Exact(value.clone()))
        }
        Value::Object(operators) if operators.len() == 1 => {
            let Some((operator, operand)) = operators.iter().next() else {
                return Err(Error::new("invalid", "rule match operator is missing"));
            };
            match (operator.as_str(), operand) {
                ("one_of", Value::Array(choices))
                    if !choices.is_empty() && choices.iter().all(is_match_scalar) =>
                {
                    Ok(CompiledMatch::OneOf(choices.clone()))
                }
                ("one_of", _) => Err(Error::new(
                    "invalid",
                    "one_of must contain a nonempty array of scalar values",
                )),
                ("exists", Value::Bool(expected)) => Ok(CompiledMatch::Exists(*expected)),
                ("exists", _) => Err(Error::new("invalid", "exists must be a boolean")),
                _ => Err(Error::new("unsupported", "unknown rule match operator")),
            }
        }
        Value::Object(_) => Err(Error::new(
            "invalid",
            "match values must contain exactly one operator",
        )),
        Value::Array(_) => Err(Error::new(
            "invalid",
            "one-of matches must use an inline one_of table",
        )),
    }
}

fn is_match_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null
    )
}

fn validate_event_action(action: &ActionDefinition) -> Result<(), Error> {
    let action = match action {
        ActionDefinition::Run { definition } => return definition.validate(),
        ActionDefinition::Agent(action) => action,
    };
    if let AgentActionDefinition::Launch {
        profile,
        workspace,
        text,
    } = action
    {
        return validate_launch_action(profile, workspace, text.as_ref(), true);
    }
    let target = action
        .target()
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "Agent action requires a string target"))?;
    validate_template(target, true)?;
    let expected_generation = action
        .expected_generation()
        .ok_or_else(|| Error::new("invalid", "Agent action generation is missing"))?;
    match expected_generation {
        Value::String(value) => {
            let parts = parse_template(value)?;
            if !is_whole_template(&parts) {
                return Err(Error::new(
                    "invalid",
                    "action generation must be a nonnegative integer or whole-value template",
                ));
            }
        }
        Value::Number(value) if value.as_i64().is_some_and(|generation| generation >= 0) => {}
        _ => {
            return Err(Error::new(
                "invalid",
                "action generation must be nonnegative or a whole-value template",
            ));
        }
    }
    if let Some((text, max_bytes, limit_label)) = action.text() {
        validate_text_template(text, true, max_bytes, action.verb(), limit_label)?;
    }
    if let Some((expected_turn_id, verb)) = action.turn_binding() {
        validate_event_binding_template(expected_turn_id, verb, "expected_turn_id")?;
    }
    if let Some((request_id, verb)) = action.request_binding() {
        validate_event_binding_template(request_id, verb, "request_id")?;
    }
    Ok(())
}

fn validate_launch_action(
    profile: &str,
    workspace: &str,
    text: Option<&Value>,
    allow_text_templates: bool,
) -> Result<(), Error> {
    validate_launch_literal(profile, "profile")?;
    validate_launch_literal(workspace, "workspace")?;
    if let Some(text) = text {
        validate_text_template(
            text,
            allow_text_templates,
            MAX_EVENT_BYTES,
            "launch",
            "64 KiB persisted-text bound",
        )?;
    }
    Ok(())
}

fn validate_launch_literal(value: &str, field: &str) -> Result<(), Error> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            format!("launch {field} must be a nonempty literal string"),
        ));
    }
    if has_templates(&parse_template(value)?) {
        return Err(Error::new(
            "invalid",
            format!("launch {field} cannot use templates"),
        ));
    }
    Ok(())
}

fn validate_event_binding_template(
    value: &Value,
    verb: &str,
    action_field: &str,
) -> Result<(), Error> {
    let source = value
        .as_str()
        .ok_or_else(|| Error::new("invalid", format!("{verb} {action_field} must be a string")))?;
    let parts = parse_template(source)?;
    if source.trim().is_empty() || (has_templates(&parts) && !is_whole_template(&parts)) {
        return Err(Error::new(
            "invalid",
            format!("{verb} {action_field} must be a literal or whole-value template"),
        ));
    }
    Ok(())
}

fn timer_action(rule: &RuleDefinition) -> Result<SubmitCommand, Error> {
    let action = rule
        .actions
        .first()
        .ok_or_else(|| Error::new("invalid", "rule has no action"))?;
    let action = match action {
        ActionDefinition::Run { definition } => return submit_run_command(definition),
        ActionDefinition::Agent(action) => action,
    };
    let (target_value, generation_value, args) = match action {
        AgentActionDefinition::Stop {
            target,
            expected_generation,
        } => (target, expected_generation, json!({})),
        AgentActionDefinition::Send {
            target,
            expected_generation,
            text,
        } => {
            let (_, max_bytes, limit_label) = action.text().expect("Send action has text");
            validate_text_template(text, false, max_bytes, action.verb(), limit_label)?;
            (
                target,
                expected_generation,
                json!({"text": text.as_str().expect("validated send text")}),
            )
        }
        AgentActionDefinition::Interrupt { .. } => {
            return Err(Error::new(
                "unsupported",
                "interrupt actions require an event trigger",
            ));
        }
        AgentActionDefinition::Resume {
            target,
            expected_generation,
            ..
        } => {
            let args = match action.text() {
                Some((text, max_bytes, limit_label)) => {
                    validate_text_template(text, false, max_bytes, action.verb(), limit_label)?;
                    let text = text.as_str().expect("validated resume text");
                    if text.trim().is_empty() {
                        return Err(Error::new(
                            "invalid",
                            "rule resume text must be a nonempty string when supplied",
                        ));
                    }
                    json!({"text": text})
                }
                None => json!({}),
            };
            (target, expected_generation, args)
        }
        AgentActionDefinition::Steer { .. } => {
            return Err(Error::new(
                "unsupported",
                "steer actions require an event trigger",
            ));
        }
        AgentActionDefinition::Approve { .. }
        | AgentActionDefinition::Deny { .. }
        | AgentActionDefinition::Answer { .. } => {
            return Err(Error::new(
                "unsupported",
                "request reply actions require an event trigger",
            ));
        }
        AgentActionDefinition::Launch {
            profile,
            workspace,
            text,
        } => {
            validate_launch_action(profile, workspace, text.as_ref(), false)?;
            let text = text
                .as_ref()
                .map(|text| text.as_str().expect("validated launch text").to_owned());
            return submit_launch_command(profile, workspace, text);
        }
    };
    let target = target_value
        .as_str()
        .ok_or_else(|| Error::new("invalid", "timer action target must be a string"))?;
    if has_templates(&parse_template(target)?) {
        return Err(Error::new(
            "invalid",
            "timer action target cannot use templates without event data",
        ));
    }
    ensure_agent_id_capacity(target.len())?;
    parse_agent_id(target)
        .map_err(|_| Error::new("invalid", "timer action target is not a canonical Agent id"))?;
    let is_resume = matches!(action, AgentActionDefinition::Resume { .. });
    let expected_generation = generation_value
        .as_i64()
        .filter(|generation| {
            if is_resume {
                *generation > 0
            } else {
                *generation >= 0
            }
        })
        .ok_or_else(|| {
            Error::new(
                "invalid",
                if is_resume {
                    "timer resume generation must be a positive integer without templates"
                } else {
                    "timer action generation must be a nonnegative integer without templates"
                },
            )
        })?;
    Ok(submit_command(
        action.verb(),
        target.to_owned(),
        expected_generation,
        args,
        None,
        None,
    ))
}

fn submit_command(
    verb: &str,
    target: String,
    generation: i64,
    args: Value,
    expected_turn_id: Option<String>,
    request_id: Option<String>,
) -> SubmitCommand {
    SubmitCommand {
        id: String::new(),
        target: Some(target),
        verb: verb.to_owned(),
        args,
        expected_generation: Some(generation),
        expected_turn_id,
        request_id,
        probe: false,
    }
}

fn validate_text_template(
    value: &Value,
    allow_templates: bool,
    max_bytes: usize,
    verb: &str,
    limit_label: &str,
) -> Result<(), Error> {
    let text = value
        .as_str()
        .ok_or_else(|| Error::new("invalid", format!("{verb} action text must be a string")))?;
    if text.len() > max_bytes {
        return Err(Error::new(
            "invalid",
            format!("{verb} action text exceeds the {limit_label}"),
        ));
    }
    let parts = parse_template(text)?;
    if !allow_templates && has_templates(&parts) {
        Err(Error::new(
            "invalid",
            format!("timer {verb} text cannot use templates without event data"),
        ))
    } else {
        Ok(())
    }
}

fn validate_template(value: &str, embedded: bool) -> Result<(), Error> {
    let parts = parse_template(value)?;
    if !embedded && has_templates(&parts) && !is_whole_template(&parts) {
        return Err(Error::new(
            "invalid",
            "generation templates must occupy the whole value",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum TemplatePart<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

fn parse_template(value: &str) -> Result<Vec<TemplatePart<'_>>, Error> {
    let mut parts = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        if start > 0 {
            parts.push(TemplatePart::Literal(&rest[..start]));
        }
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| Error::new("invalid", "rule template is unterminated"))?;
        let path = &after[..end];
        if path.contains("${") {
            return Err(Error::new(
                "invalid",
                "rule template cannot nest placeholders",
            ));
        }
        validate_path(path)?;
        parts.push(TemplatePart::Placeholder(path));
        rest = &after[end + 1..];
    }
    if !rest.is_empty() {
        parts.push(TemplatePart::Literal(rest));
    }
    Ok(parts)
}

fn has_templates(parts: &[TemplatePart<'_>]) -> bool {
    parts
        .iter()
        .any(|part| matches!(part, TemplatePart::Placeholder(_)))
}

fn is_whole_template(parts: &[TemplatePart<'_>]) -> bool {
    matches!(parts, [TemplatePart::Placeholder(_)])
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
            "cause" => {
                remaining.len() == 1 && matches!(remaining[0], "command_id" | "rule" | "depth")
            }
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
        .ok_or_else(|| Error::new("invalid", "Agent action requires a string target"))?;
    let parts = parse_template(source)?;
    if let [TemplatePart::Placeholder(path)] = parts.as_slice() {
        let target = lookup(path, roots.event, roots.subject, roots.data)
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "action target template is unavailable"))?;
        ensure_agent_id_capacity(target.len())?;
        return Ok(target.to_owned());
    }
    if !has_templates(&parts) {
        ensure_agent_id_capacity(source.len())?;
        return Ok(source.to_owned());
    }

    let mut rendered = String::new();
    for part in parts {
        match part {
            TemplatePart::Literal(text) => push_agent_id_fragment(&mut rendered, text)?,
            TemplatePart::Placeholder(path) => {
                let value = lookup(path, roots.event, roots.subject, roots.data)
                    .ok_or_else(|| Error::new("invalid", "rule template value is unavailable"))?;
                let text = scalar_text(value)?;
                push_agent_id_fragment(&mut rendered, text.as_ref())?;
            }
        }
    }
    Ok(rendered)
}

fn ensure_agent_id_capacity(length: usize) -> Result<(), Error> {
    if length > MAX_AGENT_ID_BYTES {
        return Err(Error::new(
            "invalid",
            "rendered action target exceeds Agent id bounds",
        ));
    }
    Ok(())
}

fn push_agent_id_fragment(target: &mut String, fragment: &str) -> Result<(), Error> {
    if fragment.len() > MAX_AGENT_ID_BYTES.saturating_sub(target.len()) {
        return Err(Error::new(
            "invalid",
            "rendered action target exceeds Agent id bounds",
        ));
    }
    target.push_str(fragment);
    Ok(())
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

fn render_generation(value: &Value, roots: &TemplateRoots<'_>) -> Result<Value, Error> {
    let Some(source) = value.as_str() else {
        return Ok(value.clone());
    };
    let parts = parse_template(source)?;
    let [TemplatePart::Placeholder(path)] = parts.as_slice() else {
        return Err(Error::new(
            "invalid",
            "action generation template must occupy the whole value",
        ));
    };
    lookup(path, roots.event, roots.subject, roots.data)
        .cloned()
        .ok_or_else(|| Error::new("invalid", "action generation template is unavailable"))
}

fn render_exact_string(
    value: &Value,
    roots: &TemplateRoots<'_>,
    verb: &str,
    field: &str,
) -> Result<String, Error> {
    let source = value
        .as_str()
        .ok_or_else(|| Error::new("invalid", format!("{verb} {field} must be a string")))?;
    let parts = parse_template(source)?;
    let rendered = match parts.as_slice() {
        [TemplatePart::Placeholder(path)] => lookup(path, roots.event, roots.subject, roots.data)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    format!("{verb} {field} template is unavailable or not a string"),
                )
            })?,
        [TemplatePart::Literal(_)] => source,
        _ => {
            return Err(Error::new(
                "invalid",
                format!("{verb} {field} must occupy the whole value"),
            ));
        }
    };
    if rendered.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            format!("{verb} {field} cannot be empty"),
        ));
    }
    Ok(rendered.to_owned())
}

fn render_text(
    value: &Value,
    roots: &TemplateRoots<'_>,
    max_bytes: usize,
    verb: &str,
    limit_label: &str,
) -> Result<String, Error> {
    let source = value
        .as_str()
        .ok_or_else(|| Error::new("invalid", format!("{verb} action text must be a string")))?;
    let parts = parse_template(source)?;
    if let [TemplatePart::Placeholder(path)] = parts.as_slice() {
        let text = lookup(path, roots.event, roots.subject, roots.data)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    format!("{verb} text template is unavailable or not a string"),
                )
            })?;
        if text.len() > max_bytes {
            return Err(Error::new(
                "invalid",
                format!("rendered {verb} text exceeds the {limit_label}"),
            ));
        }
        return Ok(text.to_owned());
    }

    let mut rendered = String::new();
    for part in parts {
        let fragment = match part {
            TemplatePart::Literal(text) => text,
            TemplatePart::Placeholder(path) => lookup(path, roots.event, roots.subject, roots.data)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new(
                        "invalid",
                        format!("{verb} text template is unavailable or not a string"),
                    )
                })?,
        };
        push_bounded_text(&mut rendered, fragment, max_bytes, verb, limit_label)?;
    }
    Ok(rendered)
}

fn push_bounded_text(
    target: &mut String,
    fragment: &str,
    max_bytes: usize,
    verb: &str,
    limit_label: &str,
) -> Result<(), Error> {
    if fragment.len() > max_bytes.saturating_sub(target.len()) {
        return Err(Error::new(
            "invalid",
            format!("rendered {verb} text exceeds the {limit_label}"),
        ));
    }
    target.push_str(fragment);
    Ok(())
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

    const SEND_RULE: &str = r#"
[[rule]]
id = 'send-on-result'
on = { event = 'turn.completed' }
do = [{ action = 'send', target = '${event.agent}', expected_generation = '${event.generation}', text = 'Result: ${data.status}' }]
"#;

    const INTERRUPT_RULE: &str = r#"
[[rule]]
id = 'interrupt-on-turn-start'
on = { event = 'turn.started' }
do = [{ action = 'interrupt', target = '${event.agent}', expected_generation = '${event.generation}', expected_turn_id = '${event.data.turn_id}' }]
"#;

    const STEER_RULE: &str = r#"
[[rule]]
id = 'steer-on-turn-start'
on = { event = 'turn.started' }
do = [{ action = 'steer', target = '${event.agent}', expected_generation = '${event.generation}', expected_turn_id = '${event.data.turn_id}', text = 'Steer: ${data.status}' }]
"#;

    const APPROVE_REQUEST_RULE: &str = r#"
[[rule]]
id = 'approve-on-request'
on = { event = 'request.opened' }
do = [{ action = 'approve', target = '${event.agent}', expected_generation = '${event.generation}', expected_turn_id = '${event.data.turn_id}', request_id = '${event.data.request_id}' }]
"#;

    const PROJECT_RUN_RULE: &str = r#"
[[rule]]
id = 'run-on-result'
on = { event = 'turn.completed' }
do = [{ action = 'run', cmd = ['pwsh', '.eliot/scripts/check.ps1', '-Changed'], cwd = '.', timeout = '10m', env = { CI = 'true' } }]
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

    const LIMITED_RULE: &str = r#"
[[rule]]
id = 'limited-stop'
on = { event = 'command.applied' }
limit = { max = 3, per = 'agent', window = '1m' }
do = [{ action = 'stop', target = 'a7', expected_generation = 3 }]
"#;

    const OPERATOR_RULE: &str = r#"
[[rule]]
id = 'command-launch'
on = { event = 'command.applied', match = { 'event.kind' = 'glob:command.*', 'event.data.verb' = 're:^launch$', 'event.agent' = { one_of = ['a1', 'a2'] }, 'event.generation' = { exists = true } } }
do = [{ action = 'stop', target = '${event.agent}', expected_generation = '${event.generation}' }]
"#;

    const EVENT_LAUNCH_RULE: &str = r#"
[[rule]]
id = 'launch-on-result'
on = { event = 'turn.completed' }
do = [{ action = 'launch', profile = 'fake', workspace = '/work/eliot', text = 'Review ${data.status}' }]
"#;

    const EVENT_RESUME_RULE: &str = r#"
[[rule]]
id = 'resume-after-known-stop'
on = { event = 'command.applied', match = { 'event.data.command_id' = '01ARZ3NDEKTSV4RRFFQ69G5FAV' } }
do = [{ action = 'resume', target = '${event.agent}', expected_generation = '${event.generation}', text = 'Recover ${event.kind}' }]
"#;

    fn event() -> Event {
        Event {
            seq: 7,
            ts: 100,
            kind: "turn.completed".to_owned(),
            agent: Some("a7".to_owned()),
            cause: None,
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

    fn request_opened_event() -> Event {
        let mut event = event();
        event.kind = "request.opened".to_owned();
        event.data = json!({
            "request_id": "request-id",
            "turn_id": "turn-id",
            "kind": "question"
        });
        event
    }

    fn known_stop_applied_event() -> Event {
        let mut event = event();
        event.kind = "command.applied".to_owned();
        event.source = "daemon".to_owned();
        event.native_id = None;
        event.data = json!({
            "command_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "state":"applied",
            "outcome":null
        });
        if let Some(subject) = event.subject.as_mut() {
            subject.harness = "codex".to_owned();
            subject.profile = "codex".to_owned();
        }
        event
    }

    #[test]
    fn load_rejects_invalid_and_unimplemented_rule_fields() {
        let incomplete_limit = RULE.replace("do =", "limit = { max = 2 }\ndo =");
        assert_eq!(
            load(Cursor::new(incomplete_limit)).unwrap_err().code,
            "invalid"
        );

        let unsupported = [
            RULE.replace("action = 'stop'", "action = 'notify'"),
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

        let missing_send_text = RULE.replace("action = 'stop'", "action = 'send'");
        assert_eq!(
            load(Cursor::new(missing_send_text)).unwrap_err().code,
            "invalid"
        );

        let unknown = RULE.replace(
            "id = 'stop-on-result'",
            "id = 'stop-on-result'\nextra = true",
        );
        assert_eq!(load(Cursor::new(unknown)).unwrap_err().code, "invalid");
        let direct_array =
            RULE.replace("'subject.harness' = 'fake'", "'subject.harness' = ['fake']");
        assert_eq!(load(Cursor::new(direct_array)).unwrap_err().code, "invalid");
        let duplicate = format!("{RULE}\n{RULE}");
        assert_eq!(load(Cursor::new(duplicate)).unwrap_err().code, "invalid");
        let mixed = RULE.replace(
            "event = 'turn.completed'",
            "event = 'turn.completed', every = '30m'",
        );
        assert_eq!(load(Cursor::new(mixed)).unwrap_err().code, "invalid");
    }

    #[test]
    fn legacy_stop_send_interrupt_wire_shapes_and_digests_are_stable() {
        let fixtures = [
            (
                RULE,
                json!({
                    "action":"stop",
                    "target":"${event.agent}",
                    "expected_generation":"${event.generation}"
                }),
            ),
            (
                SEND_RULE,
                json!({
                    "action":"send",
                    "target":"${event.agent}",
                    "expected_generation":"${event.generation}",
                    "text":"Result: ${data.status}"
                }),
            ),
            (
                INTERRUPT_RULE,
                json!({
                    "action":"interrupt",
                    "target":"${event.agent}",
                    "expected_generation":"${event.generation}",
                    "expected_turn_id":"${event.data.turn_id}"
                }),
            ),
        ];
        for (source, expected_action) in fixtures {
            let rule = load(Cursor::new(source)).unwrap().remove(0);
            let wire = serde_json::to_value(&rule).unwrap();
            assert_eq!(wire["do"][0], expected_action);
            assert!(wire["do"][0].get("definition").is_none());

            let saved = serde_json::to_string(&rule).unwrap();
            let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
            assert_eq!(
                restored.semantic_digest().unwrap(),
                rule.semantic_digest().unwrap()
            );
        }
    }

    #[test]
    fn event_and_timer_runs_emit_raw_targetless_commands() {
        let rule = load(Cursor::new(PROJECT_RUN_RULE)).unwrap().remove(0);
        let command = evaluate(&rule, &event()).unwrap().unwrap();
        assert_eq!(command.id, "");
        assert_eq!(command.verb, "run");
        assert_eq!(command.target, None);
        assert_eq!(command.expected_generation, None);
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert!(!command.probe);
        assert_eq!(
            command.args,
            json!({
                "cmd":["pwsh",".eliot/scripts/check.ps1","-Changed"],
                "cwd":".",
                "timeout":"10m",
                "env":{"CI":"true"}
            })
        );
        command.validate().unwrap();

        assert_eq!(
            serde_json::to_value(&rule).unwrap()["do"][0],
            json!({
                "action": "run",
                "cmd": ["pwsh", ".eliot/scripts/check.ps1", "-Changed"],
                "cwd": ".",
                "timeout": "10m",
                "env": {"CI": "true"}
            })
        );
        let saved = serde_json::to_string(&rule).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            rule.semantic_digest().unwrap()
        );

        let timer = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'run', cmd = ['pwsh', '.eliot/scripts/check.ps1'], timeout = '10m'",
        );
        let timer_rule = load(Cursor::new(timer)).unwrap().remove(0);
        let timer_command = timer_rule.evaluate_timer().expect("timer Run command");
        assert_eq!(timer_command.verb, "run");
        assert_eq!(timer_command.target, None);
        assert_eq!(timer_command.expected_generation, None);
        assert_eq!(
            timer_command.args,
            json!({
                "cmd": ["pwsh", ".eliot/scripts/check.ps1"],
                "timeout": "10m",
                "env": {}
            })
        );
        timer_command.validate().unwrap();

        let unknown =
            PROJECT_RUN_RULE.replace("timeout = '10m'", "timeout = '10m', principal = 'owner'");
        assert_eq!(load(Cursor::new(unknown)).unwrap_err().code, "invalid");
    }

    #[test]
    fn event_launch_is_flat_targetless_and_renders_only_its_text() {
        let rule = load(Cursor::new(EVENT_LAUNCH_RULE)).unwrap().remove(0);
        let command = evaluate(&rule, &event()).unwrap().unwrap();

        assert_eq!(command.id, "");
        assert_eq!(command.verb, "launch");
        assert_eq!(command.target, None);
        assert_eq!(command.expected_generation, Some(0));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert!(!command.probe);
        assert_eq!(
            command.args,
            json!({
                "profile": "fake",
                "workspace": "/work/eliot",
                "text": "Review completed"
            })
        );
        command.validate().unwrap();

        assert_eq!(
            serde_json::to_value(&rule).unwrap()["do"][0],
            json!({
                "action": "launch",
                "profile": "fake",
                "workspace": "/work/eliot",
                "text": "Review ${data.status}"
            })
        );
        let saved = serde_json::to_string(&rule).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            rule.semantic_digest().unwrap()
        );

        let without_text = EVENT_LAUNCH_RULE.replace(", text = 'Review ${data.status}'", "");
        let without_text = load(Cursor::new(without_text)).unwrap().remove(0);
        assert_eq!(
            evaluate(&without_text, &event()).unwrap().unwrap().args,
            json!({"profile":"fake", "workspace":"/work/eliot"})
        );
    }

    #[test]
    fn event_resume_projects_exact_event_binding_and_optional_text() {
        let event = known_stop_applied_event();
        assert_eq!(
            event.subject.as_ref().unwrap().state,
            eliot_core::AgentState::Busy
        );
        assert_eq!(event.data.get("verb"), None);
        let rule = load(Cursor::new(EVENT_RESUME_RULE)).unwrap().remove(0);
        let mut unrelated_stop = event.clone();
        unrelated_stop.data["command_id"] = json!("another-stop-id");
        assert_eq!(evaluate(&rule, &unrelated_stop).unwrap(), None);
        let command = evaluate(&rule, &event).unwrap().unwrap();

        assert_eq!(command.id, "");
        assert_eq!(command.verb, "resume");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert!(!command.probe);
        assert_eq!(command.args, json!({"text":"Recover command.applied"}));
        command.validate().unwrap();

        assert_eq!(
            serde_json::to_value(&rule).unwrap()["do"][0],
            json!({
                "action":"resume",
                "target":"${event.agent}",
                "expected_generation":"${event.generation}",
                "text":"Recover ${event.kind}"
            })
        );
        let saved = serde_json::to_string(&rule).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            rule.semantic_digest().unwrap()
        );

        let without_text = EVENT_RESUME_RULE.replace(", text = 'Recover ${event.kind}'", "");
        let without_text = load(Cursor::new(without_text)).unwrap().remove(0);
        assert_eq!(
            evaluate(&without_text, &event).unwrap().unwrap().args,
            json!({})
        );
    }

    #[test]
    fn event_resume_rejects_binding_mismatches_and_overrides() {
        let event = known_stop_applied_event();
        for (old, replacement) in [
            ("target = '${event.agent}'", "target = 'a8'"),
            (
                "expected_generation = '${event.generation}'",
                "expected_generation = 4",
            ),
        ] {
            let source = EVENT_RESUME_RULE.replace(old, replacement);
            let rule = load(Cursor::new(source)).unwrap().remove(0);
            assert_eq!(evaluate(&rule, &event).unwrap_err().code, "invalid");
        }

        for field in [
            "profile = 'other'",
            "profile_snapshot = {}",
            "harness = 'fake'",
            "workspace = '/other'",
            "project = 'other'",
            "worktree = 'branch'",
            "base = 'HEAD'",
            "setup = true",
            "model = 'other'",
            "effort = 'high'",
            "permissions = 'other'",
            "expected_turn_id = 'turn-id'",
            "request_id = 'request-id'",
        ] {
            let source = EVENT_RESUME_RULE.replace(
                "expected_generation = '${event.generation}'",
                &format!("expected_generation = '${{event.generation}}', {field}"),
            );
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "invalid");
        }

        let partial_generation = EVENT_RESUME_RULE.replace(
            "expected_generation = '${event.generation}'",
            "expected_generation = '3${event.generation}'",
        );
        assert_eq!(
            load(Cursor::new(partial_generation)).unwrap_err().code,
            "invalid"
        );

        let unresolved_text =
            EVENT_RESUME_RULE.replace("Recover ${event.kind}", "Recover ${data.missing}");
        let unresolved_text = load(Cursor::new(unresolved_text)).unwrap().remove(0);
        assert_eq!(
            evaluate(&unresolved_text, &event).unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn timer_resume_projects_literal_target_generation_and_optional_text() {
        let source = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'resume', target = 'a7', expected_generation = 3, text = 'new input'",
        );
        let rule = load(Cursor::new(source)).unwrap().remove(0);
        let command = rule.evaluate_timer().unwrap();

        assert_eq!(command.id, "");
        assert_eq!(command.verb, "resume");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert!(!command.probe);
        assert_eq!(command.args, json!({"text":"new input"}));
        command.validate().unwrap();

        let without_text = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'resume', target = 'a7', expected_generation = 3",
        );
        let without_text = load(Cursor::new(without_text)).unwrap().remove(0);
        assert_eq!(without_text.evaluate_timer().unwrap().args, json!({}));
    }

    #[test]
    fn timer_resume_rejects_empty_and_whitespace_supplied_text() {
        for text in ["", " \t\r\n "] {
            let action = format!(
                "action = 'resume', target = 'a7', expected_generation = 3, text = {text:?}"
            );
            let source = INTERVAL_TIMER.replace(
                "action = 'stop', target = 'a7', expected_generation = 3",
                &action,
            );
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn timer_resume_rejects_templates_invalid_generation_and_overrides() {
        let timer = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'resume', target = 'a7', expected_generation = 3, text = 'new input'",
        );

        for (old, replacement) in [
            ("target = 'a7'", "target = '${event.agent}'"),
            (
                "expected_generation = 3",
                "expected_generation = '${event.generation}'",
            ),
            (
                "expected_generation = 3",
                "expected_generation = '3${event.generation}'",
            ),
            ("expected_generation = 3", "expected_generation = 0"),
            ("expected_generation = 3", "expected_generation = -1"),
            ("expected_generation = 3", "expected_generation = '3'"),
            ("text = 'new input'", "text = '${data.status}'"),
            ("text = 'new input'", "text = '${subject.profile}'"),
        ] {
            let source = timer.replace(old, replacement);
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "invalid");
        }

        let missing_generation = timer.replace("expected_generation = 3, ", "");
        assert_eq!(
            load(Cursor::new(missing_generation)).unwrap_err().code,
            "invalid"
        );

        for field in [
            "profile = 'other'",
            "workspace = '/other'",
            "worktree = 'branch'",
            "base = 'HEAD'",
            "setup = true",
            "model = 'other'",
            "effort = 'high'",
            "permissions = 'other'",
            "expected_turn_id = 'turn-id'",
            "request_id = 'request-id'",
        ] {
            let source = timer.replace(
                "expected_generation = 3",
                &format!("expected_generation = 3, {field}"),
            );
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn timer_launch_keeps_literal_text_and_rejects_event_templates() {
        let timer = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'launch', profile = 'fake', workspace = '/work/eliot', text = 'timer input'",
        );
        let rule = load(Cursor::new(timer)).unwrap().remove(0);
        let command = rule.evaluate_timer().unwrap();
        assert_eq!(command.verb, "launch");
        assert_eq!(command.target, None);
        assert_eq!(command.expected_generation, Some(0));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert_eq!(
            command.args,
            json!({
                "profile": "fake",
                "workspace": "/work/eliot",
                "text": "timer input"
            })
        );

        let templated = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'launch', profile = 'fake', workspace = '/work/eliot', text = '${data.status}'",
        );
        assert_eq!(load(Cursor::new(templated)).unwrap_err().code, "invalid");
    }

    #[test]
    fn launch_rejects_templates_managed_worktrees_and_profile_overrides() {
        for (old, replacement) in [
            ("profile = 'fake'", "profile = '${data.profile}'"),
            (
                "workspace = '/work/eliot'",
                "workspace = '${data.workspace}'",
            ),
        ] {
            let source = EVENT_LAUNCH_RULE.replace(old, replacement);
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "invalid");
        }

        for option in [
            "worktree = 'branch'",
            "base = 'HEAD'",
            "setup = true",
            "model = 'other'",
            "effort = 'high'",
            "permissions = true",
            "executable = 'other'",
        ] {
            let source = EVENT_LAUNCH_RULE.replace(
                "workspace = '/work/eliot'",
                &format!("workspace = '/work/eliot', {option}"),
            );
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "unsupported");
        }
    }

    #[test]
    fn launch_text_uses_the_normal_persisted_launch_bound() {
        let accepted_text = "x".repeat(MAX_EVENT_BYTES);
        let accepted = EVENT_LAUNCH_RULE.replace(
            "text = 'Review ${data.status}'",
            &format!("text = '{accepted_text}'"),
        );
        let rule = load(Cursor::new(accepted)).unwrap().remove(0);
        let command = evaluate(&rule, &event()).unwrap().unwrap();
        assert_eq!(
            command.args["text"].as_str().unwrap().len(),
            MAX_EVENT_BYTES
        );

        let oversized_text = "x".repeat(MAX_EVENT_BYTES + 1);
        let oversized = EVENT_LAUNCH_RULE.replace(
            "text = 'Review ${data.status}'",
            &format!("text = '{oversized_text}'"),
        );
        assert_eq!(load(Cursor::new(oversized)).unwrap_err().code, "invalid");
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
        let command = evaluate(&rule, &event())
            .unwrap()
            .expect("matching stop action");
        assert_eq!(command.id, "");
        assert_eq!(command.verb, "stop");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.args, json!({}));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);
        assert!(!command.probe);
    }

    #[test]
    fn send_action_renders_bounded_text_with_exact_event_binding() {
        let rule = load(Cursor::new(SEND_RULE)).unwrap().remove(0);
        let command = evaluate(&rule, &event())
            .unwrap()
            .expect("matching send action");
        assert_eq!(command.id, "");
        assert_eq!(command.verb, "send");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.args, json!({"text":"Result: completed"}));
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(command.request_id, None);

        let exact = SEND_RULE.replace("text = 'Result: ${data.status}'", "text = '${data.large}'");
        let exact = load(Cursor::new(exact)).unwrap().remove(0);
        let mut event = event();
        event.data["large"] = Value::String("é".repeat(MAX_EVENT_BYTES / 2));
        let command = evaluate(&exact, &event).unwrap().unwrap();
        assert_eq!(
            command.args["text"].as_str().unwrap().len(),
            MAX_EVENT_BYTES
        );

        let oversized =
            SEND_RULE.replace("text = 'Result: ${data.status}'", "text = 'x${data.large}'");
        let oversized = load(Cursor::new(oversized)).unwrap().remove(0);
        let error = evaluate(&oversized, &event).unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(
            error.message,
            "rendered send text exceeds the 64 KiB persisted-text bound"
        );
    }

    #[test]
    fn event_and_timer_literal_text_use_the_utf8_persisted_text_bound() {
        let at_limit = "é".repeat(MAX_EVENT_BYTES / 2);
        let over_limit = format!("{at_limit}é");

        let event_at_limit = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            &format!("text = '{at_limit}'"),
        );
        assert!(load(Cursor::new(event_at_limit)).is_ok());
        let event_over_limit = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            &format!("text = '{over_limit}'"),
        );
        assert_eq!(
            load(Cursor::new(event_over_limit)).unwrap_err().code,
            "invalid"
        );

        let timer_at_limit = INTERVAL_TIMER.replace(
            "action = 'stop'",
            &format!("action = 'send', text = '{at_limit}'"),
        );
        assert!(load(Cursor::new(timer_at_limit)).is_ok());
        let timer_over_limit = INTERVAL_TIMER.replace(
            "action = 'stop'",
            &format!("action = 'send', text = '{over_limit}'"),
        );
        assert_eq!(
            load(Cursor::new(timer_over_limit)).unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn plain_json_braces_are_literal_for_event_timer_and_saved_rules() {
        let event_source = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            "text = 'Return JSON: {}'",
        );
        let event_rule = load(Cursor::new(event_source)).unwrap().remove(0);
        let event_saved = serde_json::to_string(&event_rule).unwrap();
        let restored_event = RuleDefinition::deserialize_saved(&event_saved).unwrap();
        assert_eq!(
            restored_event.semantic_digest().unwrap(),
            event_rule.semantic_digest().unwrap()
        );
        assert_eq!(
            evaluate(&restored_event, &event()).unwrap().unwrap().args,
            json!({"text":"Return JSON: {}"})
        );

        let timer_source = INTERVAL_TIMER.replace(
            "action = 'stop'",
            "action = 'send', text = 'Return JSON: {}'",
        );
        let timer_rule = load(Cursor::new(timer_source)).unwrap().remove(0);
        let timer_saved = serde_json::to_string(&timer_rule).unwrap();
        let restored_timer = RuleDefinition::deserialize_saved(&timer_saved).unwrap();
        assert_eq!(
            restored_timer.semantic_digest().unwrap(),
            timer_rule.semantic_digest().unwrap()
        );
        assert_eq!(
            restored_timer.evaluate_timer().unwrap().args,
            json!({"text":"Return JSON: {}"})
        );
    }

    #[test]
    fn malformed_or_unknown_placeholders_remain_invalid() {
        let unterminated = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            "text = 'Result: ${data.status'",
        );
        assert_eq!(load(Cursor::new(unterminated)).unwrap_err().code, "invalid");

        let unknown_root = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            "text = '${agent.turn_id}'",
        );
        assert_eq!(load(Cursor::new(unknown_root)).unwrap_err().code, "invalid");
    }

    #[test]
    fn interrupt_action_uses_exact_persisted_event_turn_id() {
        let rule = load(Cursor::new(INTERRUPT_RULE)).unwrap().remove(0);
        let mut event = event();
        event.kind = "turn.started".to_owned();
        let command = evaluate(&rule, &event).unwrap().unwrap();
        assert_eq!(command.id, "");
        assert_eq!(command.verb, "interrupt");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.expected_turn_id.as_deref(), Some("turn-id"));
        assert_eq!(command.request_id, None);
        assert_eq!(command.args, json!({}));
        assert_eq!(rule.evaluate_timer().unwrap_err().code, "unsupported");
    }

    #[test]
    fn interrupt_action_rejects_missing_mismatched_and_nonstring_turn_ids() {
        let rule = load(Cursor::new(INTERRUPT_RULE)).unwrap().remove(0);

        let mut missing = event();
        missing.kind = "turn.started".to_owned();
        missing.data.as_object_mut().unwrap().remove("turn_id");
        assert_eq!(evaluate(&rule, &missing).unwrap_err().code, "invalid");

        let mismatched_rule = INTERRUPT_RULE.replace(
            "expected_turn_id = '${event.data.turn_id}'",
            "expected_turn_id = '${event.data.other_turn}'",
        );
        let mismatched_rule = load(Cursor::new(mismatched_rule)).unwrap().remove(0);
        let mut mismatched = event();
        mismatched.kind = "turn.started".to_owned();
        mismatched.data["other_turn"] = json!("other-turn");
        assert_eq!(
            evaluate(&mismatched_rule, &mismatched).unwrap_err().code,
            "invalid"
        );

        let mut nonstring = event();
        nonstring.kind = "turn.started".to_owned();
        nonstring.data["turn_id"] = json!(1);
        assert_eq!(evaluate(&rule, &nonstring).unwrap_err().code, "invalid");
    }

    #[test]
    fn steer_binds_event_turn_and_renders_frozen_text() {
        let rule = load(Cursor::new(STEER_RULE)).unwrap().remove(0);
        let mut event = event();
        event.kind = "turn.started".to_owned();
        let command = evaluate(&rule, &event).unwrap().unwrap();

        assert_eq!(command.verb, "steer");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.expected_turn_id.as_deref(), Some("turn-id"));
        assert_eq!(command.args, json!({"text":"Steer: completed"}));

        assert_eq!(
            serde_json::to_value(&rule).unwrap()["do"][0],
            json!({
                "action": "steer",
                "target": "${event.agent}",
                "expected_generation": "${event.generation}",
                "expected_turn_id": "${event.data.turn_id}",
                "text": "Steer: ${data.status}"
            })
        );
        let saved = serde_json::to_string(&rule).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            rule.semantic_digest().unwrap()
        );
    }

    #[test]
    fn steer_rejects_a_turn_id_different_from_the_captured_event() {
        let source = STEER_RULE.replace(
            "expected_turn_id = '${event.data.turn_id}'",
            "expected_turn_id = '${event.data.other_turn}'",
        );
        let rule = load(Cursor::new(source)).unwrap().remove(0);
        let mut event = event();
        event.kind = "turn.started".to_owned();
        event.data["other_turn"] = json!("stale-turn");

        let error = evaluate(&rule, &event).unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(
            error.message,
            "steer expected_turn_id must match event.data.turn_id"
        );
    }

    #[test]
    fn steer_uses_core_text_bound_for_literal_and_rendered_text() {
        let literal = "x".repeat(MAX_EVENT_BYTES + 1);
        let literal_action = format!("text = '{literal}'");
        let literal_source = STEER_RULE.replace("text = 'Steer: ${data.status}'", &literal_action);
        assert!(literal_source.len() < MAX_RULE_FILE_BYTES);
        let literal_rule = load(Cursor::new(literal_source)).unwrap().remove(0);
        let mut literal_event = event();
        literal_event.kind = "turn.started".to_owned();
        let literal_command = evaluate(&literal_rule, &literal_event).unwrap().unwrap();
        assert_eq!(
            literal_command.args["text"].as_str().unwrap(),
            literal.as_str()
        );

        let event_text = "t".repeat(60_000);
        let mut event = event();
        event.kind = "turn.started".to_owned();
        event.data["status"] = Value::String(event_text.clone());
        assert!(serde_json::to_vec(&event.data).unwrap().len() < MAX_EVENT_BYTES);

        let accepted_source =
            STEER_RULE.replace("Steer: ${data.status}", "${data.status}${data.status}");
        let accepted = load(Cursor::new(accepted_source)).unwrap().remove(0);
        let accepted_text = evaluate(&accepted, &event).unwrap().unwrap().args["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(accepted_text.len() > MAX_EVENT_BYTES);
        assert!(accepted_text.len() <= MAX_TEXT_BYTES);
        assert_eq!(accepted_text, event_text.repeat(2));

        let oversized_template = "${data.status}".repeat(9);
        let oversized_source = STEER_RULE.replace("Steer: ${data.status}", &oversized_template);
        let oversized = load(Cursor::new(oversized_source)).unwrap().remove(0);
        let error = evaluate(&oversized, &event).unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(
            error.message,
            "rendered steer text exceeds the 512 KiB text bound"
        );
    }

    #[test]
    fn request_reply_actions_bind_the_opened_request_and_keep_answers_literal() {
        let deny_source = APPROVE_REQUEST_RULE.replace("action = 'approve'", "action = 'deny'");
        let answer_source = APPROVE_REQUEST_RULE
            .replace("action = 'approve'", "action = 'answer'")
            .replace(
                "request_id = '${event.data.request_id}' }]",
                "request_id = '${event.data.request_id}', answers = { 'question-id' = { answers = ['${data.status}'] } } }]",
            );
        let cases = [
            (APPROVE_REQUEST_RULE.to_owned(), "approve", json!({})),
            (deny_source, "deny", json!({})),
            (
                answer_source,
                "answer",
                json!({
                    "answers": {"question-id": {"answers": ["${data.status}"]}}
                }),
            ),
        ];
        let event = request_opened_event();

        for (source, verb, expected_args) in cases {
            let rule = load(Cursor::new(source)).unwrap().remove(0);
            let command = evaluate(&rule, &event)
                .unwrap()
                .expect("request reply command");
            assert_eq!(command.verb, verb);
            assert_eq!(command.target.as_deref(), Some("a7"));
            assert_eq!(command.expected_generation, Some(3));
            assert_eq!(command.expected_turn_id.as_deref(), Some("turn-id"));
            assert_eq!(command.request_id.as_deref(), Some("request-id"));
            assert_eq!(command.args, expected_args);

            let wire_action = serde_json::to_value(&rule).unwrap()["do"][0].clone();
            assert_eq!(wire_action["action"], verb);
            assert_eq!(wire_action["target"], "${event.agent}");
            assert_eq!(wire_action["expected_generation"], "${event.generation}");
            assert_eq!(wire_action["expected_turn_id"], "${event.data.turn_id}");
            assert_eq!(wire_action["request_id"], "${event.data.request_id}");
            assert!(wire_action.get("definition").is_none());
            if verb == "answer" {
                assert_eq!(wire_action["answers"], expected_args["answers"]);
            }
            let saved = serde_json::to_string(&rule).unwrap();
            let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
            assert_eq!(
                restored.semantic_digest().unwrap(),
                rule.semantic_digest().unwrap()
            );
        }
    }

    #[test]
    fn request_reply_actions_reject_wrong_trigger_and_captured_bindings() {
        let wrong_trigger =
            APPROVE_REQUEST_RULE.replace("event = 'request.opened'", "event = 'turn.completed'");
        assert_eq!(
            load(Cursor::new(wrong_trigger)).unwrap_err().code,
            "invalid"
        );

        let event = request_opened_event();
        let mismatches = [
            ("target = '${event.agent}'", "target = 'a8'"),
            (
                "expected_generation = '${event.generation}'",
                "expected_generation = 2",
            ),
            (
                "expected_turn_id = '${event.data.turn_id}'",
                "expected_turn_id = 'stale-turn'",
            ),
            (
                "request_id = '${event.data.request_id}'",
                "request_id = 'stale-request'",
            ),
        ];
        for (old, replacement) in mismatches {
            let source = APPROVE_REQUEST_RULE.replace(old, replacement);
            let rule = load(Cursor::new(source)).unwrap().remove(0);
            assert_eq!(evaluate(&rule, &event).unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn request_reply_actions_remain_unsupported_for_timers() {
        let timer_actions = [
            "action = 'approve', target = 'a7', expected_generation = 3, expected_turn_id = 'turn-id', request_id = 'request-id'",
            "action = 'deny', target = 'a7', expected_generation = 3, expected_turn_id = 'turn-id', request_id = 'request-id'",
            "action = 'answer', target = 'a7', expected_generation = 3, expected_turn_id = 'turn-id', request_id = 'request-id', answers = { 'question-id' = { answers = ['Continue'] } }",
        ];
        for action in timer_actions {
            let source = INTERVAL_TIMER.replace(
                "action = 'stop', target = 'a7', expected_generation = 3",
                action,
            );
            assert_eq!(load(Cursor::new(source)).unwrap_err().code, "unsupported");
        }
    }

    #[test]
    fn interrupt_action_is_unsupported_for_timers() {
        let timer = INTERVAL_TIMER.replace(
            "action = 'stop'",
            "action = 'interrupt', expected_turn_id = '${event.data.turn_id}'",
        );
        assert_eq!(load(Cursor::new(timer)).unwrap_err().code, "unsupported");
    }

    #[test]
    fn steer_action_is_unsupported_for_timers() {
        let timer_steer = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'steer', target = 'a7', expected_generation = 3, expected_turn_id = 'turn-id', text = 'continue'",
        );
        assert_eq!(
            load(Cursor::new(timer_steer)).unwrap_err().code,
            "unsupported"
        );
    }

    #[test]
    fn action_templates_keep_json_types_and_require_event_time_binding() {
        let numeric_text = SEND_RULE.replace(
            "text = 'Result: ${data.status}'",
            "text = '${event.generation}'",
        );
        let numeric_text = load(Cursor::new(numeric_text)).unwrap().remove(0);
        assert_eq!(
            evaluate(&numeric_text, &event()).unwrap_err().code,
            "invalid"
        );

        let numeric_target = RULE.replace(
            "target = '${event.agent}'",
            "target = 'a${event.generation}'",
        );
        let numeric_target = load(Cursor::new(numeric_target)).unwrap().remove(0);
        let mut target_event = event();
        target_event.agent = Some("a1".to_owned());
        target_event.generation = Some(1);
        let command = evaluate(&numeric_target, &target_event).unwrap().unwrap();
        assert_eq!(command.target.as_deref(), Some("a1"));
        assert_eq!(command.expected_generation, Some(1));

        let string_generation = RULE.replace(
            "expected_generation = '${event.generation}'",
            "expected_generation = '${data.generation}'",
        );
        let string_generation = load(Cursor::new(string_generation)).unwrap().remove(0);
        let mut generation_event = event();
        generation_event.data["generation"] = Value::String("3".to_owned());
        assert_eq!(
            evaluate(&string_generation, &generation_event)
                .unwrap_err()
                .code,
            "invalid"
        );

        let mismatched_target = RULE.replace("target = '${event.agent}'", "target = 'a8'");
        let mismatched_target = load(Cursor::new(mismatched_target)).unwrap().remove(0);
        assert_eq!(
            evaluate(&mismatched_target, &event()).unwrap_err().code,
            "invalid"
        );

        let mismatched_generation = RULE.replace(
            "expected_generation = '${event.generation}'",
            "expected_generation = 4",
        );
        let mismatched_generation = load(Cursor::new(mismatched_generation)).unwrap().remove(0);
        assert_eq!(
            evaluate(&mismatched_generation, &event()).unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn committed_event_match_operators_compile_and_survive_saved_decode() {
        let rule = load(Cursor::new(OPERATOR_RULE)).unwrap().remove(0);
        let saved_json = serde_json::to_string(&rule).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved_json).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            rule.semantic_digest().unwrap()
        );

        let mut command_event = event();
        command_event.kind = "command.applied".to_owned();
        command_event.agent = Some("a2".to_owned());
        command_event.data = json!({"verb":"launch"});
        let command = evaluate(&restored, &command_event)
            .unwrap()
            .expect("all exact/glob/regex/one_of/exists conditions match");
        assert_eq!(command.target.as_deref(), Some("a2"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.verb, "stop");

        let bad_regex = OPERATOR_RULE.replace("re:^launch$", "re:[");
        assert_eq!(load(Cursor::new(bad_regex)).unwrap_err().code, "invalid");
    }

    #[test]
    fn exists_false_matches_only_a_missing_path() {
        let source = OPERATOR_RULE.replace(
            "'event.kind' = 'glob:command.*', 'event.data.verb' = 're:^launch$', 'event.agent' = { one_of = ['a1', 'a2'] }, 'event.generation' = { exists = true }",
            "'data.absent' = { exists = false }",
        );
        let rule = load(Cursor::new(source)).unwrap().remove(0);
        let mut command_event = event();
        command_event.kind = "command.applied".to_owned();
        command_event.data = json!({});
        assert!(evaluate(&rule, &command_event).unwrap().is_some());

        command_event.data["absent"] = Value::Null;
        assert_eq!(evaluate(&rule, &command_event).unwrap(), None);
    }

    #[test]
    fn depth_five_suppression_precedes_placeholder_validation() {
        let missing_template =
            RULE.replace("target = '${event.agent}'", "target = '${data.missing}'");
        let rule = load(Cursor::new(missing_template)).unwrap().remove(0);
        let mut exhausted_event = event();
        exhausted_event.cause = Some(eliot_core::EventCause {
            command_id: "stop-child".to_owned(),
            rule: Some(rule.id.clone()),
            depth: 5,
        });
        assert_eq!(evaluate(&rule, &exhausted_event).unwrap(), None);

        let rule = load(Cursor::new(RULE)).unwrap().remove(0);
        let mut event = event();
        event.cause = Some(eliot_core::EventCause {
            command_id: "prior-command".to_owned(),
            rule: Some("prior-rule".to_owned()),
            depth: 4,
        });
        assert_eq!(
            evaluate(&rule, &event).unwrap().unwrap().target.as_deref(),
            Some("a7")
        );
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
            "rendered action target exceeds Agent id bounds"
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
    fn saved_stop_action_json_and_semantic_digest_stay_compatible() {
        let definition = load(Cursor::new(RULE)).unwrap().remove(0);
        let value = serde_json::to_value(&definition).unwrap();
        assert_eq!(
            value["do"][0],
            json!({
                "action":"stop",
                "target":"${event.agent}",
                "expected_generation":"${event.generation}"
            })
        );
        let digest = definition.semantic_digest().unwrap();
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            digest,
            "b68d1cf0d2b630f1cf20eb5e4df911c58412d4265c4723bba494978f2a121d5f"
        );

        let saved = serde_json::to_string(&definition).unwrap();
        let restored = RuleDefinition::deserialize_saved(&saved).unwrap();
        assert_eq!(
            restored.semantic_digest().unwrap(),
            definition.semantic_digest().unwrap()
        );
    }

    #[test]
    fn timer_on_forms_parse_to_core_schedules_and_literal_commands() {
        let interval = load(Cursor::new(INTERVAL_TIMER)).unwrap().remove(0);
        assert_eq!(
            interval.schedule(),
            Some(&TimerSchedule::Interval {
                period_ms: 30 * 60 * 1_000,
                anchor: None,
            })
        );
        assert_eq!(interval.catch_up(), &CatchUp::None);
        let command = interval.evaluate_timer().unwrap();
        assert_eq!(command.verb, "stop");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.args, json!({}));

        let timer_send =
            INTERVAL_TIMER.replace("action = 'stop'", "action = 'send', text = 'timer is due'");
        let timer_send = load(Cursor::new(timer_send)).unwrap().remove(0);
        let command = timer_send.evaluate_timer().unwrap();
        assert_eq!(command.verb, "send");
        assert_eq!(command.target.as_deref(), Some("a7"));
        assert_eq!(command.expected_generation, Some(3));
        assert_eq!(command.args, json!({"text":"timer is due"}));

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
    fn timer_run_renders_raw_targetless_command_without_event_templates() {
        let source = INTERVAL_TIMER.replace(
            "action = 'stop', target = 'a7', expected_generation = 3",
            "action = 'run', cmd = ['pwsh', 'check.ps1', '${event.data.status}'], cwd = 'work', timeout = '10m', env = { CI = 'true' }",
        );
        let rule = load(Cursor::new(source)).unwrap().remove(0);
        let command = rule.evaluate_timer().expect("timer Run command");

        assert_eq!(command.verb, "run");
        assert_eq!(command.target, None);
        assert_eq!(command.expected_generation, None);
        assert_eq!(command.expected_turn_id, None);
        assert_eq!(
            command.args,
            json!({
                "cmd": ["pwsh", "check.ps1", "${event.data.status}"],
                "cwd": "work",
                "timeout": "10m",
                "env": {"CI": "true"}
            })
        );
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
    fn custom_rule_limit_normalizes_and_survives_saved_decode() {
        let definition = load(Cursor::new(LIMITED_RULE)).unwrap().remove(0);
        let canonical_value = serde_json::to_value(&definition).unwrap();
        let limit: RuleLimit = serde_json::from_value(canonical_value["limit"].clone()).unwrap();
        assert_eq!(limit.max, 3);
        assert_eq!(limit.per, RuleLimitScope::Agent);
        assert_eq!(limit.window_ms, 60_000);

        let per_rule = LIMITED_RULE.replace("per = 'agent'", "per = 'rule'");
        let per_rule = load(Cursor::new(per_rule)).unwrap().remove(0);
        let per_rule_value = serde_json::to_value(&per_rule).unwrap();
        let per_rule_limit: RuleLimit =
            serde_json::from_value(per_rule_value["limit"].clone()).unwrap();
        assert_eq!(per_rule_limit.per, RuleLimitScope::Rule);

        let canonical = serde_json::to_string(&definition).unwrap();
        assert!(canonical.contains("\"window_ms\":60000"));
        let restored = RuleDefinition::deserialize_saved(&canonical).unwrap();
        let restored_value = serde_json::to_value(&restored).unwrap();
        let restored_limit: RuleLimit =
            serde_json::from_value(restored_value["limit"].clone()).unwrap();
        assert_eq!(restored_limit.window_ms, 60_000);
        assert_eq!(
            restored.semantic_digest().unwrap(),
            definition.semantic_digest().unwrap()
        );
    }

    #[test]
    fn custom_rule_limit_requires_positive_whole_milliseconds() {
        let zero_max = LIMITED_RULE.replace("max = 3", "max = 0");
        assert_eq!(load(Cursor::new(zero_max)).unwrap_err().code, "invalid");
        let submillisecond = LIMITED_RULE.replace("window = '1m'", "window = '500us'");
        assert_eq!(
            load(Cursor::new(submillisecond)).unwrap_err().code,
            "invalid"
        );
        let internal_unit = LIMITED_RULE.replace("window = '1m'", "window_ms = 60000");
        assert_eq!(
            load(Cursor::new(internal_unit)).unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn timer_rejects_unresolved_templates_and_mixed_trigger_forms() {
        let templated = INTERVAL_TIMER.replace("target = 'a7'", "target = '${event.agent}'");
        assert_eq!(load(Cursor::new(templated)).unwrap_err().code, "invalid");

        let templated_text = INTERVAL_TIMER.replace(
            "action = 'stop'",
            "action = 'send', text = '${data.message}'",
        );
        assert_eq!(
            load(Cursor::new(templated_text)).unwrap_err().code,
            "invalid"
        );

        let mixed = INTERVAL_TIMER.replace("every = '30m'", "every = '30m', cron = '0 12 * * *'");
        assert_eq!(load(Cursor::new(mixed)).unwrap_err().code, "invalid");
    }
}
