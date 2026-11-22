//! Typed Core inputs for rule limits and explicit retained-history catch-up.

use crate::{Command, CommandState, Error, Result, StderrSnapshot};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const MAX_RULE_REJECTION_MESSAGE_BYTES: usize = 512;
pub const MAX_RULE_SHOW_STDERR_LINES: usize = 8;
pub const MAX_RULE_SHOW_STDERR_BYTES: usize = 1_024;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleShowView {
    #[serde(flatten)]
    pub state: super::reads::RuleStateView,
    #[serde(default)]
    #[schemars(
        description = "Cumulative count of automatic event firings rejected by the rate limit."
    )]
    pub rate_limited_firings: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<RuleLastOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refusal: Option<RuleLastRefusal>,
}

impl RuleShowView {
    pub fn validate(&self) -> Result<()> {
        self.state.validate()?;
        if self
            .last_outcome
            .as_ref()
            .is_some_and(|outcome| outcome.validate().is_err())
            || self
                .last_refusal
                .as_ref()
                .is_some_and(|refusal| refusal.validate().is_err())
        {
            return Err(Error::new("invalid", "rule status view is invalid"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleLastOutcome {
    pub settled_at: i64,
    pub state: CommandState,
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<StderrSnapshot>,
}

impl RuleLastOutcome {
    pub fn validate(&self) -> Result<()> {
        if self.settled_at < 0
            || self
                .stderr
                .as_ref()
                .is_some_and(|stderr| stderr.text.len() > MAX_RULE_SHOW_STDERR_BYTES)
        {
            return Err(Error::new("invalid", "rule last outcome is invalid"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleLastRefusal {
    /// Sequence of the event whose action was refused.
    pub event_seq: i64,
    /// Sequence of the retained rejection event.
    pub recorded_event_seq: i64,
    pub at: i64,
    pub code: String,
    /// Bounded message, falling back to code for older retained events.
    pub reason: String,
}

impl RuleLastRefusal {
    pub fn validate(&self) -> Result<()> {
        if self.event_seq <= 0
            || self.recorded_event_seq <= 0
            || self.at < 0
            || self.code.trim().is_empty()
            || self.code.len() > 128
            || self.reason.trim().is_empty()
            || self.reason.len() > MAX_RULE_REJECTION_MESSAGE_BYTES
        {
            return Err(Error::new("invalid", "rule last refusal is invalid"));
        }
        Ok(())
    }
}

impl super::reads::RuleGetRequest {
    pub fn validate_show_response(&self, response: &RuleShowView) -> Result<()> {
        if self.validate().is_err()
            || response.state.id != self.id
            || response.state.project != self.project
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "rule.show response does not match the requested rule",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleLimitScope {
    Rule,
    Agent,
    Key,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleLimit {
    pub max: u64,
    pub per: RuleLimitScope,
    pub window_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_interval_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl RuleLimit {
    pub fn validate(&self) -> Result<()> {
        if self.max == 0 {
            return Err(Error::new("invalid", "rule limit max must be positive"));
        }
        if self.window_ms <= 0 {
            return Err(Error::new(
                "invalid",
                "rule limit window_ms must be positive",
            ));
        }
        if self.min_interval_ms.is_some_and(|interval| interval <= 0) {
            return Err(Error::new(
                "invalid",
                "rule limit min_interval_ms must be positive",
            ));
        }
        if (self.per != RuleLimitScope::Key && self.key.is_some())
            || self
                .key
                .as_deref()
                .is_some_and(|key| key.trim().is_empty() || key.len() > 4096)
        {
            return Err(Error::new("invalid", "rule limit key is invalid"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleCatchUpRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub rule: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_project"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
    #[serde(deserialize_with = "nonnegative_sequence")]
    #[schemars(range(min = 0))]
    pub after_seq: i64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_sequence"
    )]
    #[schemars(range(min = 0))]
    pub through_seq: Option<i64>,
}

impl RuleCatchUpRequest {
    pub fn new(
        id: impl Into<String>,
        rule: impl Into<String>,
        after_seq: i64,
        through_seq: Option<i64>,
    ) -> Self {
        Self {
            id: id.into(),
            rule: rule.into(),
            project: None,
            after_seq,
            through_seq,
        }
    }

    pub fn in_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn validate(&self) -> Result<()> {
        validate_text(&self.id, "id")?;
        validate_text(&self.rule, "rule")?;
        if let Some(project) = &self.project {
            validate_text(project, "project")?;
        }
        if self.after_seq < 0 {
            return Err(Error::new(
                "invalid",
                "rule catch-up after_seq must be nonnegative",
            ));
        }
        if self
            .through_seq
            .is_some_and(|through| through < self.after_seq)
        {
            return Err(Error::new(
                "invalid",
                "rule catch-up through_seq must not precede after_seq",
            ));
        }
        Ok(())
    }

    /// Binds the retained Command parent to the requested rule and finite range.
    /// When through_seq was omitted, the reply must expose the captured boundary.
    pub fn validate_response(&self, response: &Command) -> Result<()> {
        let args = &response.args;
        let through = args.get("through_seq").and_then(Value::as_i64);
        if self.validate().is_err()
            || response.id != self.id
            || response.verb != "rule.catch_up"
            || args.get("rule").and_then(Value::as_str) != Some(self.rule.as_str())
            || !optional_text_matches(args, "project", self.project.as_deref())
            || args.get("after_seq").and_then(Value::as_i64) != Some(self.after_seq)
            || through.is_none_or(|through| {
                through < self.after_seq
                    || self.through_seq.is_some_and(|expected| expected != through)
            })
            || args
                .get("revision")
                .and_then(Value::as_i64)
                .is_none_or(|revision| revision < 0)
        {
            return Err(Error::new(
                "unknown",
                "rule.catch_up response does not match the requested rule and range",
            ));
        }
        Ok(())
    }
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            format!("rule catch-up {field} must be nonempty"),
        ));
    }
    Ok(())
}

fn optional_text_matches(args: &Value, field: &str, expected: Option<&str>) -> bool {
    match (args.get(field), expected) {
        (None, None) | (Some(Value::Null), None) => true,
        (Some(Value::String(actual)), Some(expected)) => actual == expected,
        _ => false,
    }
}

fn required_text<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    validate_text(&value, "field").map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn optional_project<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(project) = &value {
        validate_text(project, "project").map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn nonnegative_sequence<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let value = i64::deserialize(deserializer)?;
    if value < 0 {
        return Err(serde::de::Error::custom(
            "rule catch-up sequence must be nonnegative",
        ));
    }
    Ok(value)
}

fn optional_sequence<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    let value = Option::<i64>::deserialize(deserializer)?;
    if value.is_some_and(|sequence| sequence < 0) {
        return Err(serde::de::Error::custom(
            "rule catch-up sequence must be nonnegative",
        ));
    }
    Ok(value)
}
