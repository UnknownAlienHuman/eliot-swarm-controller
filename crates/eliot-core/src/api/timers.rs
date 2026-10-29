//! Typed timer schedule, read and manual-run API values.

use crate::{Command, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerStateView {
    pub id: String,
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
}

impl TimerStateView {
    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "timer id")?;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerGetRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
}

impl TimerGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "timer id")
    }

    pub fn validate_response(&self, response: &TimerStateView) -> Result<()> {
        if self.validate().is_err() || response.id != self.id || response.validate().is_err() {
            return Err(Error::new(
                "unknown",
                "timer.get response does not match the requested timer",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerRunRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(deserialize_with = "required_text")]
    pub rule: String,
}

impl TimerRunRequest {
    pub fn new(id: impl Into<String>, rule: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            rule: rule.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_nonempty(&self.id, "mutation id")?;
        validate_nonempty(&self.rule, "timer id")
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
        {
            return Err(Error::new(
                "unknown",
                "timer.run response does not match the requested mutation",
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn timer_state() -> TimerStateView {
        TimerStateView {
            id: "daily-stop".to_owned(),
            schedule_revision: 2,
            schedule: TimerSchedule::Interval {
                period_ms: 30_000,
                anchor: None,
            },
            anchor: -1,
            last_key: None,
            next_due_at: Some(29_999),
            enabled: true,
        }
    }

    fn command(id: &str, rule: &str) -> Command {
        Command {
            id: id.to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "timer.run".to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args: json!({"rule":rule}),
            state: crate::CommandState::Applied,
            turn_id: None,
            request_id: None,
            outcome: Some(json!({"children":[]})),
            created_at: 0,
            updated_at: 0,
            probe: false,
        }
    }

    #[test]
    fn timer_schedules_have_canonical_tags_and_signed_millisecond_fields() {
        let cases = [
            (
                TimerSchedule::Interval {
                    period_ms: 30_000,
                    anchor: None,
                },
                json!({"kind":"interval", "period_ms":30_000}),
            ),
            (
                TimerSchedule::Cron {
                    expression: "0 9 * * 1-5".to_owned(),
                    tz: "America/New_York".to_owned(),
                },
                json!({"kind":"cron", "expression":"0 9 * * 1-5", "tz":"America/New_York"}),
            ),
            (
                TimerSchedule::Once { at: -1 },
                json!({"kind":"once", "at":-1}),
            ),
        ];
        for (schedule, wire) in cases {
            schedule.validate().unwrap();
            assert_eq!(serde_json::to_value(&schedule).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<TimerSchedule>(wire).unwrap(),
                schedule
            );
        }
        assert_eq!(CatchUp::default(), CatchUp::None);
        assert_eq!(
            serde_json::to_value(CatchUp::Latest).unwrap(),
            json!("latest")
        );
        assert_eq!(serde_json::to_value(CatchUp::None).unwrap(), json!("none"));
        assert!(
            serde_json::from_value::<TimerSchedule>(json!({
                "kind":"interval", "period_ms":0
            }))
            .unwrap()
            .validate()
            .is_err()
        );
        assert!(
            TimerSchedule::Cron {
                expression: "* * *".to_owned(),
                tz: "UTC".to_owned(),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn timer_requests_are_strict_and_bind_exact_read_and_mutation_ids() {
        let get = TimerGetRequest::new("daily-stop");
        let state = timer_state();
        get.validate().unwrap();
        get.validate_response(&state).unwrap();
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            json!({
                "id":"daily-stop",
                "schedule_revision":2,
                "schedule":{"kind":"interval","period_ms":30_000},
                "anchor":-1,
                "next_due_at":29_999,
                "enabled":true
            })
        );
        let mut slotted_state = state.clone();
        slotted_state.last_key = Some(-1);
        assert_eq!(
            serde_json::to_value(&slotted_state).unwrap()["last_key"],
            json!(-1)
        );
        let mut wrong_id = state.clone();
        wrong_id.id = "other".to_owned();
        assert!(get.validate_response(&wrong_id).is_err());
        let mut wrong_anchor = state.clone();
        wrong_anchor.schedule = TimerSchedule::Interval {
            period_ms: 30_000,
            anchor: Some(0),
        };
        assert!(get.validate_response(&wrong_anchor).is_err());
        let mut cron_state = state.clone();
        cron_state.schedule = TimerSchedule::Cron {
            expression: "0 9 * * 1-5".to_owned(),
            tz: "America/New_York".to_owned(),
        };
        cron_state.anchor = 1_800_000_000_000;
        get.validate_response(&cron_state).unwrap();

        for invalid in [
            json!({}),
            json!({"id":null}),
            json!({"id":" "}),
            json!({"id":"x","extra":true}),
        ] {
            assert!(serde_json::from_value::<TimerGetRequest>(invalid).is_err());
        }
        let run = TimerRunRequest::new("01ARZ3NDEKTSV4RRFFQ69G5FAV", "daily-stop");
        run.validate().unwrap();
        run.validate_response(&command(&run.id, &run.rule)).unwrap();
        assert!(
            run.validate_response(&command("01ARZ3NDEKTSV4RRFFQ69G5FAW", &run.rule))
                .is_err()
        );
        assert!(run.validate_response(&command(&run.id, "other")).is_err());
        for invalid in [
            json!({}),
            json!({"id":"one"}),
            json!({"id":"one","rule":" "}),
            json!({"id":"one","rule":"daily-stop","extra":true}),
        ] {
            assert!(serde_json::from_value::<TimerRunRequest>(invalid).is_err());
        }
    }
}
