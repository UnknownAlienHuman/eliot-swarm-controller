//! Owner resolution of unknown commands and turns. These requests record an
//! owner assertion about an already-uncertain outcome; they never invent a
//! native receipt, replay an effect, or override capacity.

use crate::{CommandState, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandResolution {
    Applied,
    Failed,
}

impl CommandResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "applied" => Ok(Self::Applied),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::new(
                "invalid",
                "command.resolve outcome must be applied or failed",
            )),
        }
    }

    pub fn state(self) -> CommandState {
        match self {
            Self::Applied => CommandState::Applied,
            Self::Failed => CommandState::Failed,
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandResolveRequest {
    pub id: String,
    pub command_id: String,
    #[serde(rename = "as")]
    pub resolution: CommandResolution,
    pub reason: String,
}

impl CommandResolveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self.command_id.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "command.resolve command_id must be nonempty",
            ));
        }
        validate_reason(&self.reason)
    }

    pub fn validate_response(&self, response: &CommandResolveResponse) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.command_id != self.command_id
            || response.state != self.resolution.state()
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandResolveResponse {
    pub id: String,
    pub command_id: String,
    pub state: CommandState,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnResolveStatus {
    Completed,
    Failed,
    Interrupted,
}

impl TurnResolveStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(Error::new(
                "invalid",
                "turn.resolve status must be completed, failed, or interrupted",
            )),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnResolveRequest {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub status: TurnResolveStatus,
    pub reason: String,
}

impl TurnResolveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self.agent.trim().is_empty() {
            return Err(Error::new("invalid", "turn.resolve agent must be nonempty"));
        }
        if self.generation < 0 {
            return Err(Error::new(
                "invalid",
                "turn.resolve generation must be a nonnegative integer",
            ));
        }
        if self.turn_id.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "turn.resolve turn_id must be nonempty",
            ));
        }
        validate_reason(&self.reason)
    }

    pub fn validate_response(&self, response: &TurnResolveResponse) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.agent != self.agent
            || response.generation != self.generation
            || response.turn_id != self.turn_id
            || response.status != self.status
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnResolveResponse {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub status: TurnResolveStatus,
}

fn validate_write_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "resolution write id must be nonempty",
        ));
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(Error::new("invalid", "resolution reason must be nonempty"));
    }
    Ok(())
}

fn unproven() -> Error {
    Error::new(
        "unknown",
        "resolution response does not match its requested binding",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn command_request() -> CommandResolveRequest {
        CommandResolveRequest {
            id: "resolve-1".to_owned(),
            command_id: "command-1".to_owned(),
            resolution: CommandResolution::Failed,
            reason: "native effect was lost with its host".to_owned(),
        }
    }

    fn turn_request() -> TurnResolveRequest {
        TurnResolveRequest {
            id: "resolve-2".to_owned(),
            agent: "a1".to_owned(),
            generation: 1,
            turn_id: "turn-1".to_owned(),
            status: TurnResolveStatus::Failed,
            reason: "generation stopped without a terminal result".to_owned(),
        }
    }

    #[test]
    fn command_resolve_uses_the_spec_wire_shape_and_closed_outcome() {
        let wire = json!({
            "id": "resolve-1",
            "command_id": "command-1",
            "as": "failed",
            "reason": "native effect was lost with its host"
        });
        let request: CommandResolveRequest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(request, command_request());
        request.validate().unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), wire);
        assert_eq!(request.resolution.as_str(), "failed");
        assert_eq!(request.resolution.state(), CommandState::Failed);
        assert_eq!(
            CommandResolution::parse("applied").unwrap(),
            CommandResolution::Applied
        );
        assert_eq!(
            CommandResolution::parse("unknown").unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn command_resolve_rejects_malformed_requests() {
        let valid = json!({
            "id": "resolve-1",
            "command_id": "command-1",
            "as": "applied",
            "reason": "owner verified the effect"
        });
        for field in ["id", "command_id", "as", "reason"] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<CommandResolveRequest>(missing).is_err(),
                "accepted missing {field}"
            );
        }
        for bad in [
            json!({"id":null,"command_id":"c","as":"failed","reason":"r"}),
            json!({"id":"","command_id":"c","as":"failed","reason":"r"}),
            json!({"id":" \t ","command_id":"c","as":"failed","reason":"r"}),
            json!({"id":"id","command_id":"","as":"failed","reason":"r"}),
            json!({"id":"id","command_id":"c","as":"failed","reason":""}),
            json!({"id":"id","command_id":"c","as":"failed","reason":"  "}),
            json!({"id":"id","command_id":"c","as":"unknown","reason":"r"}),
            json!({"id":"id","command_id":"c","as":"cancelled","reason":"r"}),
            json!({"id":"id","command_id":"c","as":null,"reason":"r"}),
            json!({"id":"id","command_id":"c","as":1,"reason":"r"}),
            json!({"id":"id","command_id":"c","as":"failed","reason":"r","extra":0}),
        ] {
            let parsed = serde_json::from_value::<CommandResolveRequest>(bad.clone());
            assert!(
                parsed.is_err() || parsed.unwrap().validate().is_err(),
                "accepted {bad}"
            );
        }
        // Identifiers and reasons keep their exact bytes; only emptiness is checked.
        let padded: CommandResolveRequest = serde_json::from_value(json!({
            "id": " resolve-1 ",
            "command_id": " command-1 ",
            "as": "applied",
            "reason": " verified "
        }))
        .unwrap();
        padded.validate().unwrap();
        assert_eq!(padded.id, " resolve-1 ");
    }

    #[test]
    fn command_resolve_response_must_match_the_asserted_binding() {
        let request = command_request();
        request
            .validate_response(&CommandResolveResponse {
                id: "resolve-1".to_owned(),
                command_id: "command-1".to_owned(),
                state: CommandState::Failed,
            })
            .unwrap();
        for response in [
            CommandResolveResponse {
                id: "other".to_owned(),
                command_id: "command-1".to_owned(),
                state: CommandState::Failed,
            },
            CommandResolveResponse {
                id: "resolve-1".to_owned(),
                command_id: "other".to_owned(),
                state: CommandState::Failed,
            },
            CommandResolveResponse {
                id: "resolve-1".to_owned(),
                command_id: "command-1".to_owned(),
                state: CommandState::Applied,
            },
        ] {
            assert!(request.validate_response(&response).is_err());
        }
        let mut invalid = command_request();
        invalid.reason = " ".to_owned();
        assert!(
            invalid
                .validate_response(&CommandResolveResponse {
                    id: "resolve-1".to_owned(),
                    command_id: "command-1".to_owned(),
                    state: CommandState::Failed,
                })
                .is_err()
        );
    }

    #[test]
    fn turn_resolve_uses_the_spec_wire_shape_and_closed_status() {
        let wire = json!({
            "id": "resolve-2",
            "agent": "a1",
            "generation": 1,
            "turn_id": "turn-1",
            "status": "failed",
            "reason": "generation stopped without a terminal result"
        });
        let request: TurnResolveRequest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(request, turn_request());
        request.validate().unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), wire);
        assert_eq!(request.status.as_str(), "failed");
        for (text, expected) in [
            ("completed", TurnResolveStatus::Completed),
            ("failed", TurnResolveStatus::Failed),
            ("interrupted", TurnResolveStatus::Interrupted),
        ] {
            assert_eq!(TurnResolveStatus::parse(text).unwrap(), expected);
        }
        assert_eq!(
            TurnResolveStatus::parse("running").unwrap_err().code,
            "invalid"
        );
        assert_eq!(TurnResolveStatus::parse("").unwrap_err().code, "invalid");
    }

    #[test]
    fn turn_resolve_rejects_malformed_requests() {
        let valid = json!({
            "id": "resolve-2",
            "agent": "a1",
            "generation": 1,
            "turn_id": "turn-1",
            "status": "completed",
            "reason": "owner verified the transcript"
        });
        for field in ["id", "agent", "generation", "turn_id", "status", "reason"] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<TurnResolveRequest>(missing).is_err(),
                "accepted missing {field}"
            );
        }
        for bad_generation in [Value::Null, json!(-1), json!(1.0), json!("1"), json!(true)] {
            let mut request = valid.clone();
            request["generation"] = bad_generation;
            let parsed = serde_json::from_value::<TurnResolveRequest>(request);
            assert!(
                parsed.is_err() || parsed.unwrap().validate().is_err(),
                "accepted bad generation"
            );
        }
        for bad in [
            json!({"id":"","agent":"a1","generation":1,"turn_id":"t","status":"failed","reason":"r"}),
            json!({"id":"id","agent":" ","generation":1,"turn_id":"t","status":"failed","reason":"r"}),
            json!({"id":"id","agent":"a1","generation":1,"turn_id":"","status":"failed","reason":"r"}),
            json!({"id":"id","agent":"a1","generation":1,"turn_id":"t","status":"running","reason":"r"}),
            json!({"id":"id","agent":"a1","generation":1,"turn_id":"t","status":null,"reason":"r"}),
            json!({"id":"id","agent":"a1","generation":1,"turn_id":"t","status":"failed","reason":" "}),
            json!({"id":"id","agent":"a1","generation":1,"turn_id":"t","status":"failed","reason":"r","extra":0}),
        ] {
            let parsed = serde_json::from_value::<TurnResolveRequest>(bad.clone());
            assert!(
                parsed.is_err() || parsed.unwrap().validate().is_err(),
                "accepted {bad}"
            );
        }
        let mut negative = turn_request();
        negative.generation = -1;
        assert!(negative.validate().is_err());
    }

    #[test]
    fn turn_resolve_response_must_match_the_asserted_binding() {
        let request = turn_request();
        let valid = TurnResolveResponse {
            id: "resolve-2".to_owned(),
            agent: "a1".to_owned(),
            generation: 1,
            turn_id: "turn-1".to_owned(),
            status: TurnResolveStatus::Failed,
        };
        request.validate_response(&valid).unwrap();
        for response in [
            TurnResolveResponse {
                id: "other".to_owned(),
                ..valid.clone()
            },
            TurnResolveResponse {
                agent: "a2".to_owned(),
                ..valid.clone()
            },
            TurnResolveResponse {
                generation: 2,
                ..valid.clone()
            },
            TurnResolveResponse {
                turn_id: "turn-2".to_owned(),
                ..valid.clone()
            },
            TurnResolveResponse {
                status: TurnResolveStatus::Completed,
                ..valid.clone()
            },
        ] {
            assert!(request.validate_response(&response).is_err());
        }
        let mut invalid = turn_request();
        invalid.generation = -1;
        assert!(invalid.validate_response(&valid).is_err());
    }
}
