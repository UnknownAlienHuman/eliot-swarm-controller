//! Typed timer schedule, read and manual-run API values.

use crate::{Command, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TimerSchedule {
    Interval {
        period_ms: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        anchor: Option<i64>,
    },
    Cron {
        expression: String,
        tz: String,
    },
    Once {
        at: i64,
    },
}

impl TimerSchedule {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Interval { period_ms, .. } if *period_ms <= 0 => Err(Error::new(
                "invalid",
                "timer interval period_ms must be positive",
            )),
            Self::Cron { expression, tz }
                if expression.trim().is_empty()
                    || expression.split_whitespace().count() != 5
                    || tz.trim().is_empty() =>
            {
                Err(Error::new(
                    "invalid",
                    "timer cron requires five fields and a timezone",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUp {
    Latest,
    #[default]
    None,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerStateView {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub schedule_revision: i64,
    pub schedule: TimerSchedule,
    /// Persisted interval anchor or cron/once registration boundary in signed
    /// Unix epoch milliseconds.
    pub anchor: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_key: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_due_at: Option<i64>,
    pub enabled: bool,
    /// Latest scheduled check result. Assignment readbacks also expose a
    /// current arbitration wait here while its scheduled slot remains due.
    /// A manual run changes neither this nor `last_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<String>,
}

impl TimerStateView {
    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "timer id")?;
        if let Some(project) = &self.project {
            validate_nonempty(project, "project id")?;
        }
        if let Some(code) = &self.last_outcome {
            validate_nonempty(code, "timer last_outcome")?;
        }
        if self.schedule_revision < 0 {
            return Err(Error::new(
                "invalid",
                "timer schedule_revision must be nonnegative",
            ));
        }
        self.schedule.validate()?;
        let invalid_anchor = match &self.schedule {
            TimerSchedule::Interval {
                anchor: Some(anchor),
                ..
            } => self.anchor != *anchor,
            TimerSchedule::Interval { anchor: None, .. } => false,
            TimerSchedule::Cron { .. } | TimerSchedule::Once { .. } => false,
        };
        if invalid_anchor {
            return Err(Error::new(
                "invalid",
                "timer state anchor does not match its schedule",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerGetRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_project"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
}

impl TimerGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            project: None,
        }
    }

    pub fn in_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "timer id")?;
        if let Some(project) = &self.project {
            validate_nonempty(project, "project id")?;
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &TimerStateView) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.project != self.project
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "timer.get response does not match the requested timer",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerRunRequest {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<String>,
}

impl TimerRunRequest {
    pub fn new(id: impl Into<String>, rule: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            rule: rule.into(),
            project: None,
            include: None,
        }
    }

    pub fn in_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "mutation id")?;
        validate_nonempty(&self.rule, "timer id")?;
        if let Some(project) = &self.project {
            validate_nonempty(project, "project id")?;
        }
        if let Some(name) = &self.include
            && (self.project.is_some()
                || name.len() > 255
                || !name.to_ascii_lowercase().ends_with(".md")
                || name.contains(['/', '\\'])
                || name.chars().any(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "include requires one brief filename on an Owner timer",
            ));
        }
        Ok(())
    }

    /// Manual timer runs return their existing durable parent command.
    pub fn validate_response(&self, response: &Command) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.verb != "timer.run"
            || response
                .args
                .get("rule")
                .and_then(serde_json::Value::as_str)
                != Some(self.rule.as_str())
            || !project_matches_args(&response.args, self.project.as_deref())
            || response.args.get("include").and_then(Value::as_str) != self.include.as_deref()
        {
            return Err(Error::new(
                "unknown",
                "timer.run response does not match the requested mutation",
            ));
        }
        Ok(())
    }
}

fn project_matches_args(args: &Value, expected: Option<&str>) -> bool {
    match (args.get("project"), expected) {
        (None, None) | (Some(Value::Null), None) => true,
        (Some(Value::String(actual)), Some(expected)) => actual == expected,
        _ => false,
    }
}

fn validate_nonempty(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", format!("{name} must be nonempty")));
    }
    Ok(())
}

fn required_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    validate_nonempty(&value, "timer request field").map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn optional_project<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let project = Option::<String>::deserialize(deserializer)?;
    if let Some(project) = &project {
        validate_nonempty(project, "project id").map_err(serde::de::Error::custom)?;
    }
    Ok(project)
}
