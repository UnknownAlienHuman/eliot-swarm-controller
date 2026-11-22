use eliot_core::{
    Error, Event, SubmitCommand,
    api::{
        CatchUp, CommandSubmitRequest, EmitDefinitionArgs, GitFetchDefinitionArgs,
        GitIntegrateDefinitionArgs, GitPublishDefinitionArgs, GitPublishTrigger,
        MAX_RULE_FILE_BYTES, MailDefinitionArgs, NotifyDefinitionArgs, ProjectCheckPassed,
        RuleLimit, RuleLimitScope, RunDefinitionArgs, TimerSchedule, VarDefinitionArgs,
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

// Canonical Agent IDs are `a` plus at most 19 positive-i64 decimal digits.
const MAX_AGENT_ID_BYTES: usize = 20;
const MAX_RULE_CAUSE_DEPTH: u8 = 5;
const MAX_MATCH_ALTERNATIVES: usize = 32;

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
    compiled_matches: MatchGroup,
    #[serde(skip)]
    compiled_any_of: Vec<MatchGroup>,
}

type MatchGroup = BTreeMap<String, CompiledMatch>;

impl RuleDefinition {
    pub(super) fn guards_launch_worktree(&self) -> bool {
        matches!(
            self.actions.first(),
            Some(ActionDefinition::Agent(AgentActionDefinition::Launch(launch)))
                if launch.require_worktree
        )
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchActionDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile: Option<String>,
    workspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assignment: Option<AssignmentActionDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal: Option<BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    no_policy: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    require_worktree: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<Value>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentActionDefinition {
    key: String,
    folder: String,
    max_busy: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal: Option<eliot_core::api::AssignmentGoal>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchResultMail {
    pub(super) from_launch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) to: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnswerActionDefinition {
    target: Value,
    expected_generation: Value,
    expected_turn_id: Value,
    request_id: Value,
    answers: BTreeMap<String, Value>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StopActionDefinition {
    target: Value,
    expected_generation: Value,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendActionDefinition {
    target: Value,
    expected_generation: Value,
    text: Value,
    #[serde(default, skip_serializing_if = "is_false")]
    probe: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactActionDefinition {
    target: Value,
    expected_generation: Value,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwitchActionDefinition {
    target: Value,
    expected_generation: Value,
    expected_turn_id: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = eliot_core::api::MAX_HANDOFF_SENDS), extend("uniqueItems" = true))]
    commands: Vec<String>,
    launch: Box<eliot_core::api::SwitchLaunch>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InterruptActionDefinition {
    target: Value,
    expected_generation: Value,
    expected_turn_id: Value,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SteerActionDefinition {
    target: Value,
    expected_generation: Value,
    expected_turn_id: Value,
    text: Value,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChoiceActionDefinition {
    target: Value,
    expected_generation: Value,
    expected_turn_id: Value,
    request_id: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    choice: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    feedback: Option<Value>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeActionDefinition {
    target: Value,
    expected_generation: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<Value>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InjectActionDefinition {
    target: Value,
    at: String,
    text: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum AgentActionDefinition {
    Stop(StopActionDefinition),
    Send(SendActionDefinition),
    Interrupt(InterruptActionDefinition),
    Steer(SteerActionDefinition),
    Approve(ChoiceActionDefinition),
    Deny(ChoiceActionDefinition),
    Answer(AnswerActionDefinition),
    Resume(ResumeActionDefinition),
    Launch(LaunchActionDefinition),
    Compact(CompactActionDefinition),
    Switch(SwitchActionDefinition),
}

#[derive(Clone, Debug)]
enum ActionDefinition {
    Agent(AgentActionDefinition),
    Run {
        definition: RunDefinitionArgs,
    },
    GitFetch {
        definition: GitFetchDefinitionArgs,
    },
    GitIntegrate {
        definition: GitIntegrateDefinitionArgs,
    },
    GitPublish {
        definition: GitPublishDefinitionArgs,
    },
    Notify {
        definition: NotifyDefinitionArgs,
    },
    Emit {
        definition: EmitDefinitionArgs,
    },
    Mail {
        definition: MailDefinitionArgs,
    },
    LaunchResultMail(LaunchResultMail),
    Var {
        definition: VarDefinitionArgs,
    },
    Inject {
        definition: InjectActionDefinition,
    },
}

impl<'de> Deserialize<'de> for ActionDefinition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::decode_at(Value::deserialize(deserializer)?, "action").map_err(D::Error::custom)
    }
}

impl ActionDefinition {
    fn decode_at(value: Value, root: &str) -> Result<Self, Error> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::invalid_field(root, "rule action object"))?;
        let action = object
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid_field(format!("{root}.action"), "action name string"))?
            .to_owned();
        if action == "run" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Run { definition });
        }
        if action == "git.fetch" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::GitFetch { definition });
        }
        if action == "git.integrate" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::GitIntegrate { definition });
        }
        if action == "git.publish" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::GitPublish { definition });
        }
        if action == "notify" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Notify { definition });
        }
        if action == "emit" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Emit { definition });
        }
        if action == "mail.send" || action == "msg.send" {
            let mut fields = object.clone();
            fields.remove("action");
            if fields.contains_key("from_launch") {
                let definition =
                    eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
                return Ok(Self::LaunchResultMail(definition));
            }
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Mail { definition });
        }
        if action == "var.set" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Var { definition });
        }
        if action == "inject" {
            let mut fields = object.clone();
            fields.remove("action");
            let definition: InjectActionDefinition =
                eliot_core::api::discovery::decode_params_at(Value::Object(fields), root)?;
            return Ok(Self::Inject { definition });
        }
        let mut fields = object.clone();
        fields.remove("action");
        let fields = Value::Object(fields);
        let action = match action.as_str() {
            "stop" => AgentActionDefinition::Stop(eliot_core::api::discovery::decode_params_at(
                fields, root,
            )?),
            "send" => AgentActionDefinition::Send(eliot_core::api::discovery::decode_params_at(
                fields, root,
            )?),
            "interrupt" => AgentActionDefinition::Interrupt(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "steer" => AgentActionDefinition::Steer(eliot_core::api::discovery::decode_params_at(
                fields, root,
            )?),
            "approve" => AgentActionDefinition::Approve(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "deny" => AgentActionDefinition::Deny(eliot_core::api::discovery::decode_params_at(
                fields, root,
            )?),
            "answer" => AgentActionDefinition::Answer(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "resume" => AgentActionDefinition::Resume(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "launch" => AgentActionDefinition::Launch(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "compact" => AgentActionDefinition::Compact(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            "switch" => AgentActionDefinition::Switch(
                eliot_core::api::discovery::decode_params_at(fields, root)?,
            ),
            _ => {
                return Err(Error::invalid_field(
                    format!("{root}.action"),
                    "supported Agent action name",
                ));
            }
        };
        Ok(Self::Agent(action))
    }
}

impl Serialize for ActionDefinition {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Agent(action) => action.serialize(serializer),
            Self::GitFetch { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "git.fetch definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("git.fetch".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
            Self::GitIntegrate { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "git.integrate definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert(
                    "action".to_owned(),
                    Value::String("git.integrate".to_owned()),
                );
                Value::Object(fields).serialize(serializer)
            }
            Self::GitPublish { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "git.publish definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("git.publish".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
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
            Self::Notify { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "notify definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("notify".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
            Self::Emit { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "emit definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("emit".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
            Self::Mail { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "mail.send definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("mail.send".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
            Self::LaunchResultMail(definition) => {
                #[derive(Serialize)]
                struct Action<'a> {
                    action: &'static str,
                    #[serde(flatten)]
                    definition: &'a LaunchResultMail,
                }
                Action {
                    action: "mail.send",
                    definition,
                }
                .serialize(serializer)
            }
            Self::Var { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "var.set definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("var.set".to_owned()));
                Value::Object(fields).serialize(serializer)
            }
            Self::Inject { definition } => {
                let mut fields =
                    match serde_json::to_value(definition).map_err(serde::ser::Error::custom)? {
                        Value::Object(fields) => fields,
                        _ => {
                            return Err(serde::ser::Error::custom(
                                "inject definition must serialize as an object",
                            ));
                        }
                    };
                fields.insert("action".to_owned(), Value::String("inject".to_owned()));
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
    AtMost(u64),
    Not(Box<Self>),
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
            Self::AtMost(max) => actual
                .and_then(Value::as_u64)
                .is_some_and(|value| value <= *max),
            Self::Not(condition) => !condition.matches(actual),
        }
    }
}

impl AgentActionDefinition {
    fn target(&self) -> Option<&Value> {
        match self {
            Self::Stop(StopActionDefinition { target, .. })
            | Self::Send(SendActionDefinition { target, .. })
            | Self::Interrupt(InterruptActionDefinition { target, .. })
            | Self::Steer(SteerActionDefinition { target, .. })
            | Self::Approve(ChoiceActionDefinition { target, .. })
            | Self::Deny(ChoiceActionDefinition { target, .. })
            | Self::Answer(AnswerActionDefinition { target, .. })
            | Self::Resume(ResumeActionDefinition { target, .. })
            | Self::Compact(CompactActionDefinition { target, .. })
            | Self::Switch(SwitchActionDefinition { target, .. }) => Some(target),
            Self::Launch(_) => None,
        }
    }

    fn expected_generation(&self) -> Option<&Value> {
        match self {
            Self::Stop(StopActionDefinition {
                expected_generation,
                ..
            })
            | Self::Send(SendActionDefinition {
                expected_generation,
                ..
            })
            | Self::Interrupt(InterruptActionDefinition {
                expected_generation,
                ..
            })
            | Self::Steer(SteerActionDefinition {
                expected_generation,
                ..
            })
            | Self::Approve(ChoiceActionDefinition {
                expected_generation,
                ..
            })
            | Self::Deny(ChoiceActionDefinition {
                expected_generation,
                ..
            })
            | Self::Answer(AnswerActionDefinition {
                expected_generation,
                ..
            })
            | Self::Resume(ResumeActionDefinition {
                expected_generation,
                ..
            })
            | Self::Compact(CompactActionDefinition {
                expected_generation,
                ..
            })
            | Self::Switch(SwitchActionDefinition {
                expected_generation,
                ..
            }) => Some(expected_generation),
            Self::Launch(_) => None,
        }
    }

    fn text(&self) -> Option<(&Value, usize, &'static str)> {
        match self {
            Self::Send(SendActionDefinition { text, .. }) => {
                Some((text, MAX_EVENT_BYTES, "64 KiB persisted-text bound"))
            }
            Self::Steer(SteerActionDefinition { text, .. }) => {
                Some((text, MAX_TEXT_BYTES, "512 KiB text bound"))
            }
            Self::Launch(LaunchActionDefinition {
                text: Some(text), ..
            })
            | Self::Resume(ResumeActionDefinition {
                text: Some(text), ..
            }) => Some((text, MAX_EVENT_BYTES, "64 KiB persisted-text bound")),
            _ => None,
        }
    }

    fn turn_binding(&self) -> Option<(&Value, &'static str)> {
        match self {
            Self::Interrupt(InterruptActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "interrupt")),
            Self::Steer(SteerActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "steer")),
            Self::Approve(ChoiceActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "approve")),
            Self::Deny(ChoiceActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "deny")),
            Self::Answer(AnswerActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "answer")),
            Self::Switch(SwitchActionDefinition {
                expected_turn_id, ..
            }) => Some((expected_turn_id, "switch")),
            _ => None,
        }
    }

    fn request_binding(&self) -> Option<(&Value, &'static str)> {
        match self {
            Self::Approve(ChoiceActionDefinition { request_id, .. }) => {
                Some((request_id, "approve"))
            }
            Self::Deny(ChoiceActionDefinition { request_id, .. }) => Some((request_id, "deny")),
            Self::Answer(AnswerActionDefinition { request_id, .. }) => Some((request_id, "answer")),
            _ => None,
        }
    }

    fn approval_choice(&self) -> Option<&Value> {
        match self {
            Self::Approve(ChoiceActionDefinition { choice, .. })
            | Self::Deny(ChoiceActionDefinition { choice, .. }) => choice.as_ref(),
            _ => None,
        }
    }

    fn approval_feedback(&self) -> Option<&Value> {
        match self {
            Self::Approve(ChoiceActionDefinition { feedback, .. })
            | Self::Deny(ChoiceActionDefinition { feedback, .. }) => feedback.as_ref(),
            _ => None,
        }
    }

    fn answers(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Self::Answer(AnswerActionDefinition { answers, .. }) => Some(answers),
            _ => None,
        }
    }

    fn verb(&self) -> &'static str {
        match self {
            Self::Stop(_) => "stop",
            Self::Send(_) => "send",
            Self::Interrupt(_) => "interrupt",
            Self::Steer(_) => "steer",
            Self::Approve(_) => "approve",
            Self::Deny(_) => "deny",
            Self::Answer(_) => "answer",
            Self::Resume(_) => "resume",
            Self::Launch(_) => "launch",
            Self::Compact(_) => "compact",
            Self::Switch(_) => "switch",
        }
    }

    fn probe(&self) -> bool {
        matches!(self, Self::Send(SendActionDefinition { probe: true, .. }))
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
    for (index, raw) in file.rule.into_iter().enumerate() {
        reject_unimplemented_options(&raw)?;
        let mut rule = decode_rule_definition(raw, &format!("rule[{index}]"))?;
        validate_rule(&mut rule)?;
        if !ids.insert(rule.id.clone()) {
            return Err(Error::new("invalid", "rule file contains duplicate ids"));
        }
        rules.push(rule);
    }
    Ok(rules)
}

fn decode_rule_definition(mut raw: toml::Value, root: &str) -> Result<RuleDefinition, Error> {
    let path = format!("{root}.do");
    let actions = raw
        .get_mut("do")
        .ok_or_else(|| Error::invalid_field(&path, "array of action objects"))?;
    // Decode the existing rule DTO envelope without routing its actions through
    // serde's string-only Error::custom boundary. Restore the actual actions
    // before validation; their conversion uses the same Value boundary as
    // ActionDefinition::deserialize and retains Core Error details directly.
    let actions = std::mem::replace(actions, toml::Value::Array(Vec::new()));
    let mut rule: RuleDefinition = raw
        .try_into()
        .map_err(|_| Error::new("invalid", "rule definition is malformed"))?;
    let actions: Vec<Value> = actions
        .try_into()
        .map_err(|_| Error::invalid_field(&path, "array of action objects"))?;
    rule.actions = actions
        .into_iter()
        .enumerate()
        .map(|(index, value)| ActionDefinition::decode_at(value, &format!("{path}[{index}]")))
        .collect::<Result<_, _>>()?;
    Ok(rule)
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

pub(super) enum RuleEvaluation {
    NoMatch,
    CauseDepth,
    Command(SubmitCommand),
    LaunchResultMail(LaunchResultMail),
}

/// Matches a committed event and projects its one ordinary command action.
pub(super) fn evaluate(rule: &RuleDefinition, event: &Event) -> Result<RuleEvaluation, Error> {
    if !rule.enabled || rule.trigger.event.as_deref() != Some(event.kind.as_str()) {
        return Ok(RuleEvaluation::NoMatch);
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
    let matches_group = |group: &MatchGroup| {
        group.iter().all(|(path, matcher)| {
            matcher.matches(lookup(path, &event_value, subject, &event.data))
        })
    };
    let alternatives = &rule.trigger.compiled_any_of;
    if !matches_group(&rule.trigger.compiled_matches)
        || (!alternatives.is_empty() && !alternatives.iter().any(matches_group))
    {
        return Ok(RuleEvaluation::NoMatch);
    }
    if event
        .cause
        .as_ref()
        .is_some_and(|cause| cause.depth >= MAX_RULE_CAUSE_DEPTH)
    {
        return Ok(RuleEvaluation::CauseDepth);
    }

    let action = rule
        .actions
        .first()
        .ok_or_else(|| Error::new("invalid", "rule has no action"))?;
    let action = match action {
        ActionDefinition::Run { definition } => {
            return submit_run_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::GitFetch { definition } => {
            return submit_git_fetch_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::GitIntegrate { definition } => {
            return submit_git_integrate_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::GitPublish { definition } => {
            return submit_git_publish_command(definition, event).map(RuleEvaluation::Command);
        }
        ActionDefinition::Notify { definition } => {
            return submit_notify_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::Emit { definition } => {
            return submit_emit_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::Mail { definition } => {
            return submit_mail_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::LaunchResultMail(definition) => {
            return Ok(RuleEvaluation::LaunchResultMail(definition.clone()));
        }
        ActionDefinition::Var { definition } => {
            return submit_var_command(definition).map(RuleEvaluation::Command);
        }
        ActionDefinition::Inject { definition } => {
            return submit_inject_event_command(
                definition,
                event,
                &event_value,
                subject,
                &event.data,
            )
            .map(RuleEvaluation::Command);
        }
        ActionDefinition::Agent(action) => action,
    };
    let roots = TemplateRoots {
        event: &event_value,
        subject,
        data: &event.data,
    };
    if let AgentActionDefinition::Launch(LaunchActionDefinition {
        profile,
        workspace,
        role,
        assignment,
        goal,
        no_policy,
        ..
    }) = action
    {
        let text = action
            .text()
            .map(|(text, max_bytes, limit_label)| {
                render_text(text, &roots, max_bytes, action.verb(), limit_label)
            })
            .transpose()?;
        return submit_launch_command(
            profile.as_deref(),
            workspace,
            role.as_deref(),
            assignment.as_ref(),
            goal.as_ref(),
            no_policy.as_deref(),
            text,
        )
        .map(RuleEvaluation::Command);
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
    let args = if let AgentActionDefinition::Switch(switch) = action {
        json!({"commands": &switch.commands, "launch": &switch.launch})
    } else {
        action
            .text()
            .map(|(text, max_bytes, limit_label)| {
                render_text(text, &roots, max_bytes, verb, limit_label)
            })
            .transpose()?
            .map(|text| json!({"text": text}))
            .or_else(|| action.answers().map(|answers| json!({"answers": answers})))
            .map(Ok)
            .or_else(|| approval_reply_args(action, &roots, verb).transpose())
            .transpose()?
            .unwrap_or_else(|| json!({}))
    };
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

    let mut command = submit_command(
        action.verb(),
        target,
        generation,
        args,
        expected_turn_id,
        request_id,
    );
    command.probe = action.probe();
    Ok(RuleEvaluation::Command(command))
}

fn approval_reply_args(
    action: &AgentActionDefinition,
    roots: &TemplateRoots<'_>,
    verb: &str,
) -> Result<Option<Value>, Error> {
    let choice = action.approval_choice();
    let feedback = action.approval_feedback();
    if choice.is_none() && feedback.is_none() {
        return Ok(None);
    }
    let mut args = serde_json::Map::new();
    if let Some(source) = choice {
        args.insert(
            "choice_id".to_owned(),
            Value::String(render_exact_string(source, roots, verb, "choice")?),
        );
    }
    if let Some(source) = feedback {
        args.insert(
            "feedback".to_owned(),
            Value::String(render_text(
                source,
                roots,
                MAX_EVENT_BYTES,
                verb,
                "64 KiB persisted-text bound",
            )?),
        );
    }
    Ok(Some(Value::Object(args)))
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

fn validate_git_fetch_definition(definition: &GitFetchDefinitionArgs) -> Result<(), Error> {
    definition.validate()?;
    if let Some(timeout) = &definition.timeout {
        super::schedule::parse_positive_duration_ms(timeout).map_err(|_| {
            Error::new(
                "invalid",
                "git.fetch timeout must be a positive whole-millisecond duration",
            )
        })?;
    }
    Ok(())
}

fn validate_git_integrate_definition(definition: &GitIntegrateDefinitionArgs) -> Result<(), Error> {
    definition.validate()?;
    if let Some(timeout) = &definition.timeout {
        super::schedule::parse_positive_duration_ms(timeout).map_err(|_| {
            Error::new(
                "invalid",
                "git.integrate timeout must be a positive whole-millisecond duration",
            )
        })?;
        super::git_integrate_timeout_ms(Some(timeout))?;
    }
    Ok(())
}
fn validate_git_publish_definition(definition: &GitPublishDefinitionArgs) -> Result<(), Error> {
    definition.validate()?;
    if let Some(timeout) = &definition.timeout {
        super::schedule::parse_positive_duration_ms(timeout).map_err(|_| {
            Error::new(
                "invalid",
                "git.publish timeout must be a positive whole-millisecond duration",
            )
        })?;
    }
    Ok(())
}

fn submit_notify_command(definition: &NotifyDefinitionArgs) -> Result<SubmitCommand, Error> {
    definition.validate()?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "notify definition cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "notify");
    command.args = args;
    command.validate()?;
    Ok(command)
}

fn submit_emit_command(definition: &EmitDefinitionArgs) -> Result<SubmitCommand, Error> {
    definition.validate()?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "emit definition cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "emit");
    command.args = args;
    command.validate()?;
    Ok(command)
}

pub(super) fn submit_mail_command(definition: &MailDefinitionArgs) -> Result<SubmitCommand, Error> {
    definition.validate().map_err(|error| {
        Error::new(
            error.code.clone(),
            format!("mail.send definition invalid: {}", error.message),
        )
    })?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "mail.send definition cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "mail.send");
    command.args = args;
    command.validate()?;
    Ok(command)
}

fn submit_var_command(definition: &VarDefinitionArgs) -> Result<SubmitCommand, Error> {
    definition.validate().map_err(|error| {
        Error::new(
            error.code.clone(),
            format!("var.set definition invalid: {}", error.message),
        )
    })?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "var.set definition cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "var.set");
    command.args = args;
    command.validate()?;
    Ok(command)
}

fn validate_inject_action(definition: &InjectActionDefinition) -> Result<(), Error> {
    let target = definition
        .target
        .as_str()
        .ok_or_else(|| Error::new("invalid", "inject action requires a string target"))?;
    validate_template(target, true)?;
    if definition.at.trim().is_empty()
        || has_templates(&parse_template(&definition.at)?)
        || !matches!(
            definition.at.as_str(),
            "soonest" | "turn_safe_point" | "active_turn" | "next_input"
        )
    {
        return Err(Error::new(
            "invalid",
            "inject at must be soonest, turn_safe_point, active_turn or next_input without templates",
        ));
    }
    validate_text_template(
        &definition.text,
        true,
        MAX_TEXT_BYTES,
        "inject",
        "512 KiB text bound",
    )?;
    Ok(())
}

fn submit_inject_event_command(
    definition: &InjectActionDefinition,
    event: &Event,
    event_value: &Value,
    subject: Option<&Value>,
    data: &Value,
) -> Result<SubmitCommand, Error> {
    if !matches!(
        definition.at.as_str(),
        "soonest" | "turn_safe_point" | "active_turn" | "next_input"
    ) {
        return Err(Error::new(
            "invalid",
            "inject at must be soonest, turn_safe_point, active_turn or next_input",
        ));
    }
    if has_templates(&parse_template(&definition.at)?) {
        return Err(Error::new("invalid", "inject at cannot use templates"));
    }
    let roots = TemplateRoots {
        event: event_value,
        subject,
        data,
    };
    let target_source = definition
        .target
        .as_str()
        .ok_or_else(|| Error::new("invalid", "inject action requires a string target"))?;
    let target = render_agent_target(&Value::String(target_source.to_owned()), &roots)?;
    parse_agent_id(&target)
        .map_err(|_| Error::new("invalid", "inject target is not a canonical Agent id"))?;
    let event_target = event
        .agent
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "inject rule requires an event-time Agent"))?;
    if target != event_target {
        return Err(Error::new(
            "invalid",
            "inject target must match the event-time Agent",
        ));
    }
    let generation = event
        .generation
        .ok_or_else(|| Error::new("invalid", "inject rule requires an event-time generation"))?;
    let text = render_text(
        &definition.text,
        &roots,
        MAX_TEXT_BYTES,
        "inject",
        "512 KiB text bound",
    )?;
    if text.trim().is_empty() {
        return Err(Error::new("invalid", "inject text cannot be empty"));
    }
    let expected_turn_id = if definition.at == "soonest" {
        event
            .data
            .get("turn_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    } else if definition.at != "next_input" {
        let turn_id = event
            .data
            .get("turn_id")
            .and_then(Value::as_str)
            .filter(|turn_id| !turn_id.trim().is_empty())
            .ok_or_else(|| {
                Error::new("invalid", "turn-bound inject requires event.data.turn_id")
            })?;
        Some(turn_id.to_owned())
    } else {
        None
    };
    let mut command = CommandSubmitRequest::new(String::new(), "inject");
    command.target = Some(target);
    command.expected_generation = Some(generation);
    command.expected_turn_id = expected_turn_id;
    command.args = json!({"text": text, "point": definition.at});
    command.validate()?;
    Ok(command)
}

fn submit_git_fetch_command(definition: &GitFetchDefinitionArgs) -> Result<SubmitCommand, Error> {
    validate_git_fetch_definition(definition)?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "git.fetch arguments cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "git.fetch");
    command.args = args;
    command.validate()?;
    Ok(command)
}

fn submit_git_integrate_command(
    definition: &GitIntegrateDefinitionArgs,
) -> Result<SubmitCommand, Error> {
    validate_git_integrate_definition(definition)?;
    let args = serde_json::to_value(definition)
        .map_err(|_| Error::new("invalid", "git.integrate arguments cannot be encoded"))?;
    let mut command = CommandSubmitRequest::new(String::new(), "git.integrate");
    command.args = args;
    command.validate()?;
    Ok(command)
}
fn submit_git_publish_command(
    definition: &GitPublishDefinitionArgs,
    event: &Event,
) -> Result<SubmitCommand, Error> {
    validate_git_publish_definition(definition)?;
    if event.kind != "project.check.passed" {
        return Err(Error::new(
            "not_allowed",
            "git.publish requires a successful final project check",
        ));
    }
    let check = serde_json::from_value::<ProjectCheckPassed>(event.data.clone())
        .map_err(|_| Error::new("invalid", "project.check.passed data is invalid"))?;
    check.validate()?;
    let (Some(agent), Some(generation), Some(workspace)) = (
        event.agent.as_deref(),
        event.generation,
        event
            .subject
            .as_ref()
            .and_then(|subject| subject.workspace.as_deref()),
    ) else {
        return Err(Error::new(
            "not_allowed",
            "project.check.passed is missing its Agent workspace binding",
        ));
    };
    let trigger = GitPublishTrigger {
        repository: definition.repository.clone(),
        remote: definition.remote.clone(),
        create_pr: definition.create_pr,
        timeout_ms: super::git_publish_timeout_ms(definition.timeout.as_deref())?,
        source_event_seq: event.seq,
        check,
        agent: agent.to_owned(),
        generation,
        workspace: workspace.to_owned(),
    };
    trigger.validate()?;
    let mut command = CommandSubmitRequest::new(String::new(), "git.publish");
    command.args = serde_json::to_value(trigger)
        .map_err(|_| Error::new("invalid", "git.publish trigger cannot be encoded"))?;
    command.validate()?;
    Ok(command)
}

fn submit_launch_command(
    profile: Option<&str>,
    workspace: &str,
    role: Option<&str>,
    assignment: Option<&AssignmentActionDefinition>,
    goal: Option<&BTreeMap<String, Value>>,
    no_policy: Option<&str>,
    text: Option<String>,
) -> Result<SubmitCommand, Error> {
    let mut command = CommandSubmitRequest::new(String::new(), "launch");
    command.expected_generation = Some(0);
    command.args = json!({"workspace": workspace});
    if let Some(profile) = profile {
        command.args["profile"] = Value::String(profile.to_owned());
    }
    if let Some(role) = role {
        command.args["role"] = Value::String(role.to_owned());
    }
    if let Some(assignment) = assignment {
        command.args["assignment"] = json!({
            "key": assignment.key,
            "folder": assignment.folder,
            "max_busy": assignment.max_busy,
        });
        if let Some(goal) = &assignment.goal {
            goal.validate()?;
            command.args["assignment"]["goal"] = serde_json::to_value(goal)
                .map_err(|_| Error::new("invalid", "assignment goal cannot be encoded"))?;
        }
    }
    if let Some(goal) = goal {
        command.args["goal"] = serde_json::to_value(goal)
            .map_err(|_| Error::new("invalid", "launch goal cannot be encoded"))?;
    }
    if let Some(reason) = no_policy {
        command.args["no_policy"] = Value::String(reason.to_owned());
    }
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
                                    | "git.fetch"
                                    | "git.integrate"
                                    | "git.publish"
                                    | "notify"
                                    | "emit"
                                    | "mail.send"
                                    | "msg.send"
                                    | "var.set"
                                    | "inject"
                                    | "compact"
                                    | "switch"
                            )
                        })
                })
            });
    if has_unsupported_launch_option {
        return Err(Error::new(
            "unsupported",
            "launch accepts profile or role, workspace, optional text, assignment, optional goal and no_policy; worktree, base, setup, model, effort, permissions and executable are unsupported",
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
        if rule.guards_launch_worktree()
            && !matches!(
                event.as_str(),
                "turn.completed" | "project.worktree.check.passed"
            )
        {
            return Err(Error::invalid_field(
                "on.event",
                "worktree-bound terminal event",
            ));
        }
        if matches!(action, ActionDefinition::GitPublish { .. }) && event != "project.check.passed"
        {
            return Err(Error::new(
                "invalid",
                "git.publish requires the project.check.passed event trigger",
            ));
        }
        if !is_default_catch_up(&rule.catch_up) {
            return Err(Error::new("invalid", "catch_up requires a timer trigger"));
        }
        if matches!(action, ActionDefinition::LaunchResultMail(_)) && event != "turn.completed" {
            return Err(Error::invalid_field(
                "on.event",
                "turn.completed for mail.send from_launch",
            ));
        }
        if matches!(action, ActionDefinition::Agent(agent) if agent.request_binding().is_some())
            && event != "request.opened"
        {
            return Err(Error::new(
                "invalid",
                "request reply actions require a request.opened trigger",
            ));
        }
        if matches!(
            action,
            ActionDefinition::Agent(AgentActionDefinition::Launch(LaunchActionDefinition {
                assignment: Some(_),
                ..
            }))
        ) {
            return Err(Error::new(
                "unsupported",
                "folder assignment launches require a timer trigger",
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
    let mut alternatives = Vec::new();
    for (path, expected) in &trigger.matches {
        if path == "any_of" {
            let branches = expected
                .as_array()
                .filter(|branches| (1..=MAX_MATCH_ALTERNATIVES).contains(&branches.len()))
                .ok_or_else(|| Error::new("invalid", "any_of needs 1 to 32 match tables"))?;
            for branch in branches {
                let fields = branch
                    .as_object()
                    .filter(|fields| !fields.is_empty())
                    .ok_or_else(|| Error::new("invalid", "any_of needs nonempty match tables"))?;
                alternatives.push(compile_match_group(fields.iter())?);
            }
            continue;
        }
        compiled.insert(path.clone(), compile_field_match(path, expected)?);
    }
    trigger.compiled_matches = compiled;
    trigger.compiled_any_of = alternatives;
    Ok(())
}

fn compile_match_group<'a>(
    fields: impl Iterator<Item = (&'a String, &'a Value)>,
) -> Result<MatchGroup, Error> {
    fields
        .map(|(path, value)| Ok((path.clone(), compile_field_match(path, value)?)))
        .collect()
}

fn compile_field_match(path: &str, value: &Value) -> Result<CompiledMatch, Error> {
    validate_path(path)?;
    if let Some(operand) = value
        .as_object()
        .filter(|operators| operators.len() == 1)
        .and_then(|operators| operators.get("not"))
    {
        return Ok(CompiledMatch::Not(Box::new(compile_match(operand)?)));
    }
    compile_match(value)
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
                ("at_most", n) => n
                    .as_u64()
                    .map(CompiledMatch::AtMost)
                    .ok_or_else(|| Error::invalid_field("at_most", "nonnegative integer")),
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
        ActionDefinition::GitFetch { definition } => {
            return validate_git_fetch_definition(definition);
        }
        ActionDefinition::GitIntegrate { definition } => {
            return validate_git_integrate_definition(definition);
        }
        ActionDefinition::GitPublish { definition } => {
            return validate_git_publish_definition(definition);
        }
        ActionDefinition::Notify { definition } => return definition.validate(),
        ActionDefinition::Emit { definition } => return definition.validate(),
        ActionDefinition::Mail { definition } => {
            return definition.validate().map_err(|error| {
                Error::new(
                    error.code.clone(),
                    format!("mail.send definition invalid: {}", error.message),
                )
            });
        }
        ActionDefinition::LaunchResultMail(definition) => {
            if definition.from_launch.trim().is_empty() || definition.from_launch.contains("${") {
                return Err(Error::invalid_field(
                    "from_launch",
                    "nonempty literal rule id",
                ));
            }
            if let Some(to) = &definition.to {
                parse_agent_id(to)?;
            }
            return Ok(());
        }
        ActionDefinition::Var { definition } => {
            return definition.validate().map_err(|error| {
                Error::new(
                    error.code.clone(),
                    format!("var.set definition invalid: {}", error.message),
                )
            });
        }
        ActionDefinition::Inject { definition } => {
            return validate_inject_action(definition);
        }
        ActionDefinition::Agent(action) => action,
    };
    if let AgentActionDefinition::Launch(LaunchActionDefinition {
        profile,
        workspace,
        role,
        assignment,
        goal,
        text,
        ..
    }) = action
    {
        return validate_launch_action(
            profile.as_deref(),
            workspace,
            role.as_deref(),
            assignment.as_ref(),
            goal.as_ref(),
            text.as_ref(),
            true,
        );
    }
    if let AgentActionDefinition::Switch(switch) = action {
        switch.launch.validate()?;
        if switch.commands.len() > eliot_core::api::MAX_HANDOFF_SENDS {
            return Err(Error::new(
                "invalid",
                "switch commands exceed the handoff limit",
            ));
        }
        let mut seen = HashSet::new();
        if switch
            .commands
            .iter()
            .any(|id| eliot_core::validate_command_id(id).is_err() || !seen.insert(id))
        {
            return Err(Error::new(
                "invalid",
                "switch commands must be distinct valid command IDs",
            ));
        }
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
    if let Some(choice) = action.approval_choice() {
        validate_event_binding_template(choice, action.verb(), "choice")?;
    }
    if let Some(feedback) = action.approval_feedback() {
        validate_text_template(
            feedback,
            true,
            MAX_EVENT_BYTES,
            action.verb(),
            "64 KiB persisted-text bound",
        )?;
        let text = feedback.as_str().ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{} feedback must be a string", action.verb()),
            )
        })?;
        if text.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                format!("{} feedback cannot be empty", action.verb()),
            ));
        }
    }
    Ok(())
}

fn validate_launch_action(
    profile: Option<&str>,
    workspace: &str,
    role: Option<&str>,
    assignment: Option<&AssignmentActionDefinition>,
    goal: Option<&BTreeMap<String, Value>>,
    text: Option<&Value>,
    allow_text_templates: bool,
) -> Result<(), Error> {
    if goal.is_some() && assignment.is_some_and(|assignment| assignment.goal.is_some()) {
        return Err(Error::new(
            "invalid",
            "launch cannot set both a folder assignment goal and a goal override",
        ));
    }
    if let Some(profile) = profile {
        validate_launch_literal(profile, "profile")?;
    }
    validate_launch_literal(workspace, "workspace")?;
    if let Some(role) = role {
        validate_launch_literal(role, "role")?;
        crate::config::validate_routing_role(role)?;
    }
    if profile.is_some() && role.is_some() {
        return Err(Error::new(
            "invalid",
            "launch profile and role are mutually exclusive",
        ));
    }
    if let Some(assignment) = assignment {
        if assignment.key.trim().is_empty()
            || assignment.key.len() > 128
            || !assignment.key.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
            || assignment.max_busy == 0
            || assignment.max_busy > 1_000
        {
            return Err(Error::new(
                "invalid",
                "launch assignment requires a valid key and max_busy from 1 through 1000",
            ));
        }
        validate_launch_literal(&assignment.key, "assignment key")?;
        validate_launch_literal(&assignment.folder, "assignment folder")?;
        if role.is_none() || profile.is_some() || text.is_some() {
            return Err(Error::new(
                "invalid",
                "assignment launch requires a role and cannot set profile or text",
            ));
        }
    }
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
        ActionDefinition::GitFetch { definition } => return submit_git_fetch_command(definition),
        ActionDefinition::GitIntegrate { definition } => {
            return submit_git_integrate_command(definition);
        }
        ActionDefinition::GitPublish { .. } => {
            return Err(Error::new(
                "unsupported",
                "git.publish requires a project.check.passed event",
            ));
        }
        ActionDefinition::Notify { definition } => return submit_notify_command(definition),
        ActionDefinition::Emit { definition } => return submit_emit_command(definition),
        ActionDefinition::Mail { definition } => return submit_mail_command(definition),
        ActionDefinition::LaunchResultMail(_) => {
            return Err(Error::invalid_field(
                "on",
                "event trigger for mail.send from_launch",
            ));
        }
        ActionDefinition::Var { definition } => return submit_var_command(definition),
        ActionDefinition::Inject { .. } => {
            return Err(Error::new(
                "unsupported",
                "inject actions require an event trigger",
            ));
        }
        ActionDefinition::Agent(action) => action,
    };
    let (target_value, generation_value, args) = match action {
        AgentActionDefinition::Stop(StopActionDefinition {
            target,
            expected_generation,
        }) => (target, expected_generation, json!({})),
        AgentActionDefinition::Send(SendActionDefinition {
            target,
            expected_generation,
            text,
            ..
        }) => {
            let (_, max_bytes, limit_label) = action.text().expect("Send action has text");
            validate_text_template(text, false, max_bytes, action.verb(), limit_label)?;
            (
                target,
                expected_generation,
                json!({"text": text.as_str().expect("validated send text")}),
            )
        }
        AgentActionDefinition::Compact(CompactActionDefinition {
            target,
            expected_generation,
        }) => (target, expected_generation, json!({})),
        AgentActionDefinition::Switch(_) => {
            return Err(Error::new(
                "unsupported",
                "switch actions require an event trigger",
            ));
        }
        AgentActionDefinition::Interrupt(_) => {
            return Err(Error::new(
                "unsupported",
                "interrupt actions require an event trigger",
            ));
        }
        AgentActionDefinition::Resume(ResumeActionDefinition {
            target,
            expected_generation,
            ..
        }) => {
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
        AgentActionDefinition::Steer(_) => {
            return Err(Error::new(
                "unsupported",
                "steer actions require an event trigger",
            ));
        }
        AgentActionDefinition::Approve(_)
        | AgentActionDefinition::Deny(_)
        | AgentActionDefinition::Answer(_) => {
            return Err(Error::new(
                "unsupported",
                "request reply actions require an event trigger",
            ));
        }
        AgentActionDefinition::Launch(LaunchActionDefinition {
            profile,
            workspace,
            role,
            assignment,
            goal,
            no_policy,
            require_worktree,
            text,
        }) => {
            if *require_worktree {
                return Err(Error::invalid_field(
                    "require_worktree",
                    "event trigger for a worktree-bound launch",
                ));
            }
            validate_launch_action(
                profile.as_deref(),
                workspace,
                role.as_deref(),
                assignment.as_ref(),
                goal.as_ref(),
                text.as_ref(),
                false,
            )?;
            let text = text
                .as_ref()
                .map(|text| text.as_str().expect("validated launch text").to_owned());
            return submit_launch_command(
                profile.as_deref(),
                workspace,
                role.as_deref(),
                assignment.as_ref(),
                goal.as_ref(),
                no_policy.as_deref(),
                text,
            );
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
    let is_resume = matches!(action, AgentActionDefinition::Resume(_));
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
    let mut command = submit_command(
        action.verb(),
        target.to_owned(),
        expected_generation,
        args,
        None,
        None,
    );
    command.probe = action.probe();
    Ok(command)
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
        selector: None,
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
    let mut rendered = String::new();
    for part in parts {
        match part {
            TemplatePart::Literal(text) => {
                push_bounded_text(&mut rendered, text, max_bytes, verb, limit_label)?;
            }
            TemplatePart::Placeholder(path) => {
                let text = lookup(path, roots.event, roots.subject, roots.data)
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Error::new(
                            "invalid",
                            format!("{verb} text template is unavailable or not a string"),
                        )
                    })?;
                let fragment = if text.trim().is_empty() {
                    Cow::Borrowed(text)
                } else {
                    Cow::Owned(format!("\n{}\n", eliot_core::fence_untrusted_text(text)))
                };
                push_bounded_text(&mut rendered, &fragment, max_bytes, verb, limit_label)?;
            }
        }
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
