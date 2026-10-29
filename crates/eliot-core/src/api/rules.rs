//! Typed Core inputs for rule limits and explicit retained-history catch-up.

use crate::{Command, Error, Result};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleLimitScope {
    Rule,
    Agent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleLimit {
    pub max: u64,
    pub per: RuleLimitScope,
    pub window_ms: i64,
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
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleCatchUpRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(deserialize_with = "required_text")]
    pub rule: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_project"
    )]
    pub project: Option<String>,
    #[serde(deserialize_with = "nonnegative_sequence")]
    pub after_seq: i64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_sequence"
    )]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommandState;
    use serde_json::json;

    fn catch_up_command(request: &RuleCatchUpRequest) -> Command {
        let project = request
            .project
            .as_ref()
            .map_or(Value::Null, |project| json!(project));
        Command {
            id: request.id.clone(),
            principal: "owner".to_owned(),
            method: "rule.catch_up".to_owned(),
            verb: "rule.catch_up".to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args: json!({
                "rule":request.rule,
                "project":project,
                "after_seq":request.after_seq,
                "through_seq":request.through_seq.unwrap_or(20),
                "revision":3
            }),
            state: CommandState::Applied,
            turn_id: None,
            request_id: None,
            outcome: Some(json!({"current_cursor":20,"retiring_cursor":null})),
            created_at: 0,
            updated_at: 0,
            probe: false,
        }
    }

    #[test]
    fn rule_limit_is_strict_and_uses_positive_numeric_milliseconds() {
        let limit: RuleLimit = serde_json::from_value(json!({
            "max":3,
            "per":"agent",
            "window_ms":3_600_000
        }))
        .unwrap();
        limit.validate().unwrap();
        assert_eq!(
            serde_json::to_value(limit).unwrap(),
            json!({"max":3,"per":"agent","window_ms":3_600_000})
        );
        for invalid in [
            json!({"max":0,"per":"rule","window_ms":1}),
            json!({"max":1,"per":"agent","window_ms":0}),
            json!({"max":1,"per":"owner","window_ms":1}),
            json!({"max":1,"per":"rule","window_ms":1,"extra":true}),
        ] {
            if let Ok(limit) = serde_json::from_value::<RuleLimit>(invalid) {
                assert!(limit.validate().is_err());
            }
        }
    }

    #[test]
    fn catch_up_request_is_strict_and_binds_the_captured_command_range() {
        let request = RuleCatchUpRequest::new("01ARZ3NDEKTSV4RRFFQ69G5FAV", "r1", 7, None)
            .in_project("project-a");
        request.validate().unwrap();
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(
            wire,
            json!({
                "id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "rule":"r1",
                "project":"project-a",
                "after_seq":7
            })
        );
        let decoded: RuleCatchUpRequest = serde_json::from_value(wire).unwrap();
        decoded
            .validate_response(&catch_up_command(&decoded))
            .unwrap();

        let mut wrong_range = catch_up_command(&decoded);
        wrong_range.args["after_seq"] = json!(6);
        assert_eq!(
            decoded.validate_response(&wrong_range).unwrap_err().code,
            "unknown"
        );
        let mut wrong_revision = catch_up_command(&decoded);
        wrong_revision.args["revision"] = json!(-1);
        assert_eq!(
            decoded.validate_response(&wrong_revision).unwrap_err().code,
            "unknown"
        );
        let reversed_range: RuleCatchUpRequest =
            serde_json::from_value(json!({"id":"id","rule":"r1","after_seq":2,"through_seq":1}))
                .unwrap();
        assert!(reversed_range.validate().is_err());

        for invalid in [
            json!({"id":"id","rule":"r1","after_seq":-1}),
            json!({"id":"id","rule":"r1","after_seq":1,"project":" "}),
            json!({"id":"id","rule":"r1","after_seq":1,"extra":true}),
        ] {
            assert!(serde_json::from_value::<RuleCatchUpRequest>(invalid).is_err());
        }
    }
}
