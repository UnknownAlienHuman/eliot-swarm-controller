//! Grammar for the implemented command/turn/result read calls. These identifiers
//! select existing records; they never admit or retry an external effect.

use super::mutations::nullable;
use crate::{Agent, Command, Error, Result, Turn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleGetRequest {
    #[serde(deserialize_with = "required_id")]
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

impl RuleGetRequest {
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
        validate_id(&self.id)?;
        if let Some(project) = &self.project {
            validate_id(project)?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleStateView {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub revision: i64,
    pub current_cursor: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_cursor: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retire_through: Option<i64>,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_reason: Option<String>,
}

impl RuleStateView {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(project) = &self.project {
            validate_id(project)?;
        }
        if self.revision < 0 || self.current_cursor < 0 {
            return Err(Error::new(
                "invalid",
                "rule revision and cursor must be nonnegative",
            ));
        }
        if self
            .paused_reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(Error::new("invalid", "paused_reason must be nonempty"));
        }
        match (
            self.retiring_revision,
            self.retiring_cursor,
            self.retire_through,
        ) {
            (None, None, None) => Ok(()),
            (Some(revision), Some(cursor), Some(through))
                if revision >= 0
                    && revision < self.revision
                    && cursor >= 0
                    && through >= cursor =>
            {
                Ok(())
            }
            _ => Err(Error::new(
                "invalid",
                "rule retirement progress must be a complete, ordered tuple",
            )),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandGetRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
}

impl CommandGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnGetRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub agent: String,
    #[serde(deserialize_with = "generation")]
    #[schemars(range(min = 0))]
    pub generation: i64,
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub turn_id: String,
}

impl TurnGetRequest {
    pub fn new(agent: impl Into<String>, generation: i64, turn_id: impl Into<String>) -> Self {
        Self {
            agent: agent.into(),
            generation,
            turn_id: turn_id.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.agent)?;
        validate_generation(self.generation)?;
        validate_id(&self.turn_id)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResultRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_generation"
    )]
    #[schemars(range(min = 0), with = "i64")]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_id"
    )]
    #[schemars(
        description = "Nonblank string; preserved without trimming or normalization.",
        with = "String"
    )]
    pub turn_id: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentResultRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
            turn_id: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(generation) = self.generation {
            validate_generation(generation)?;
        }
        if let Some(turn_id) = &self.turn_id {
            validate_id(turn_id)?;
        }
        if !(1..=super::MAX_LIMIT).contains(&self.limit) {
            return Err(Error::new("invalid", "limit must be between 1 and 1000"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandChildrenRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_id"
    )]
    #[schemars(
        description = "Nonblank string; preserved without trimming or normalization.",
        with = "String"
    )]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl CommandChildrenRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            after: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(after) = &self.after {
            validate_id(after)?;
        }
        if !(1..=super::MAX_LIMIT).contains(&self.limit) {
            return Err(Error::new("invalid", "limit must be between 1 and 1000"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandChildrenResponse {
    #[serde(deserialize_with = "object_commands")]
    pub children: Vec<Command>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentResultResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "object_turns")]
    pub turns: Vec<Turn>,
    #[serde(deserialize_with = "object_agent")]
    pub agent_metadata: Agent,
}

fn validate_id(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "read identifiers must be non-empty strings",
        ));
    }
    Ok(())
}

fn validate_generation(value: i64) -> Result<()> {
    if value < 0 {
        return Err(Error::new(
            "invalid",
            "generation must be a nonnegative integer",
        ));
    }
    Ok(())
}

fn required_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    validate_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn optional_project<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let project = Option::<String>::deserialize(deserializer)?;
    if let Some(project) = &project {
        validate_id(project).map_err(serde::de::Error::custom)?;
    }
    Ok(project)
}

fn generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let value = i64::deserialize(deserializer)?;
    validate_generation(value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn present_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    generation(deserializer).map(Some)
}

fn present_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    required_id(deserializer).map(Some)
}

fn object_agent<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Agent, D::Error> {
    let object = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
}

fn object_turns<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Turn>, D::Error> {
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect()
}

fn object_commands<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Command>, D::Error> {
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentState, TurnState};
    use serde_json::json;

    #[test]
    fn command_ids_are_required_nonempty_and_not_normalized_or_quoted() {
        for request in [
            json!({}),
            json!({"id":null}),
            json!({"id":1}),
            json!({"id":false}),
            json!({"id":""}),
            json!({"id":" \t "}),
            json!({"id":"x","extra":0}),
        ] {
            assert!(serde_json::from_value::<CommandGetRequest>(request).is_err());
        }
        for id in [" raw read id ".to_owned(), "x".repeat(4096)] {
            let request: CommandGetRequest = serde_json::from_value(json!({"id":id})).unwrap();
            assert_eq!(request.id, id);
            request.validate().unwrap();
            assert_eq!(serde_json::to_value(&request).unwrap(), json!({"id":id}));
        }
        assert!(CommandGetRequest::new(" ").validate().is_err());
    }

    #[test]
    fn rule_get_requires_exact_nonempty_id() {
        let valid = json!({"id":"rule-1"});
        let request: RuleGetRequest = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(request, RuleGetRequest::new("rule-1"));
        request.validate().unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), valid);

        let project_request: RuleGetRequest =
            serde_json::from_value(json!({"id":"rule-1","project":"project-a"})).unwrap();
        assert_eq!(
            project_request,
            RuleGetRequest::new("rule-1").in_project("project-a")
        );
        assert_eq!(
            serde_json::to_value(&project_request).unwrap(),
            json!({"id":"rule-1","project":"project-a"})
        );

        for invalid in [
            json!({}),
            json!({"id":null}),
            json!({"id":1}),
            json!({"id":false}),
            json!({"id":""}),
            json!({"id":" \t "}),
            json!({"id":"rule-1","project":" "}),
            json!({"id":"rule-1","project":1}),
            json!({"id":"rule-1","extra":0}),
        ] {
            assert!(serde_json::from_value::<RuleGetRequest>(invalid).is_err());
        }
        assert!(RuleGetRequest::new(" ").validate().is_err());
    }

    #[test]
    fn turn_get_requires_exact_fields_and_nonnegative_integer_generation() {
        let valid = json!({"agent":" a01 ","generation":0,"turn_id":" turn "});
        let request: TurnGetRequest = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), valid);
        request.validate().unwrap();
        for field in ["agent", "generation", "turn_id"] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<TurnGetRequest>(missing).is_err());
            for bad in [Value::Null, json!(true), json!(1.0)] {
                let mut request = valid.clone();
                request[field] = bad;
                assert!(serde_json::from_value::<TurnGetRequest>(request).is_err());
            }
        }
        for bad in [
            json!(-1),
            json!("0"),
            json!(i64::MAX as u64 + 1),
            json!(u64::MAX),
        ] {
            let mut request = valid.clone();
            request["generation"] = bad;
            assert!(serde_json::from_value::<TurnGetRequest>(request).is_err());
        }
        for field in ["agent", "turn_id"] {
            for bad in ["", " \t "] {
                let mut request = valid.clone();
                request[field] = json!(bad);
                assert!(serde_json::from_value::<TurnGetRequest>(request).is_err());
            }
        }
        let mut extra = valid;
        extra["extra"] = json!(0);
        assert!(serde_json::from_value::<TurnGetRequest>(extra).is_err());
        assert!(TurnGetRequest::new("a1", -1, "turn").validate().is_err());
    }

    #[test]
    fn omitted_result_options_capture_defaults_and_serialize_omitted() {
        let request: AgentResultRequest = serde_json::from_value(json!({"id":"a1"})).unwrap();
        assert_eq!(request, AgentResultRequest::new("a1"));
        assert_eq!(request.generation, None);
        assert_eq!(request.turn_id, None);
        assert_eq!(request.limit, 100);
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"id":"a1","limit":100})
        );
    }

    #[test]
    fn supplied_result_options_cannot_fall_back_on_wrong_types_or_null() {
        for bad in [
            Value::Null,
            json!(-1),
            json!(0.0),
            json!("1"),
            json!(true),
            json!([]),
            json!(i64::MAX as u64 + 1),
            json!(u64::MAX),
        ] {
            assert!(
                serde_json::from_value::<AgentResultRequest>(json!({"id":"a1","generation":bad}))
                    .is_err()
            );
        }
        for bad in [
            Value::Null,
            json!(1),
            json!(false),
            json!([]),
            json!({}),
            json!(""),
            json!(" \t "),
        ] {
            assert!(
                serde_json::from_value::<AgentResultRequest>(json!({"id":"a1","turn_id":bad}))
                    .is_err()
            );
        }
        for id in [Value::Null, json!(1), json!(""), json!(" ")] {
            assert!(serde_json::from_value::<AgentResultRequest>(json!({"id":id})).is_err());
        }
        assert!(serde_json::from_value::<AgentResultRequest>(json!({})).is_err());
        assert!(
            serde_json::from_value::<AgentResultRequest>(json!({"id":"a1","extra":0})).is_err()
        );
        for generation in [0, i64::MAX] {
            let request: AgentResultRequest = serde_json::from_value(
                json!({"id":"a1","generation":generation,"turn_id":" raw turn "}),
            )
            .unwrap();
            assert_eq!(request.generation, Some(generation));
            assert_eq!(request.turn_id.as_deref(), Some(" raw turn "));
            request.validate().unwrap();
        }
    }

    #[test]
    fn result_limit_is_checked_even_with_an_exact_turn() {
        for bad in [
            Value::Null,
            json!(0),
            json!(1001),
            json!(-1),
            json!(1.0),
            json!("100"),
            json!(true),
            json!(u64::MAX),
        ] {
            for exact in [false, true] {
                let mut request = json!({"id":"a1","limit":bad});
                if exact {
                    request["turn_id"] = json!("turn");
                }
                assert!(serde_json::from_value::<AgentResultRequest>(request).is_err());
            }
        }
        for limit in [1, 1000] {
            let request: AgentResultRequest =
                serde_json::from_value(json!({"id":"a1","limit":limit,"turn_id":"turn"})).unwrap();
            assert_eq!(request.limit, limit);
        }
        let mut request = AgentResultRequest::new("a1");
        request.turn_id = Some("turn".to_owned());
        request.limit = 0;
        assert!(request.validate().is_err());
        request.limit = 100;
        request.generation = Some(-1);
        assert!(request.validate().is_err());
        request.generation = None;
        request.turn_id = Some(" ".to_owned());
        assert!(request.validate().is_err());
    }

    fn result_wire() -> Value {
        let mut agent = Agent::new("a1", "fixture", "fake", "fake", 0);
        agent.generation = 2;
        agent.state = AgentState::Stopped;
        let turn = Turn {
            agent: "a1".to_owned(),
            generation: 1,
            turn_id: "turn-1".to_owned(),
            state: TurnState::Unknown,
            status: None,
            outcome: None,
            native_status: None,
            final_text: None,
            started_at: None,
            completed_at: None,
            usage: None,
        };
        serde_json::to_value(AgentResultResponse {
            agent: "a1".to_owned(),
            generation: 1,
            turns: vec![turn],
            agent_metadata: agent,
        })
        .unwrap()
    }

    #[test]
    fn result_response_requires_all_fields_and_reuses_domain_objects() {
        let wire = result_wire();
        let response: AgentResultResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(response).unwrap(), wire);
        for field in ["agent", "generation", "turns", "agent_metadata"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<AgentResultResponse>(missing).is_err());
        }
        for field in ["turns", "agent_metadata"] {
            for bad in [Value::Null, json!(false), json!("bad")] {
                let mut malformed = wire.clone();
                malformed[field] = bad;
                assert!(serde_json::from_value::<AgentResultResponse>(malformed).is_err());
            }
        }
    }

    #[test]
    fn nested_positional_agent_and_turn_arrays_are_not_objects() {
        // Nested read values must be objects: the response helpers decode
        // them through maps and reject any array, so these fixtures pin the
        // rejection without asserting how the bare structs treat sequences
        // (serde accepts sequences for structs, and the exact layout drifts
        // as domain fields are added).
        let positional_agent = json!([
            "a1",
            2,
            "eliot",
            false,
            false,
            null,
            "a1",
            0,
            "self",
            null,
            "fixture",
            "fake",
            "fake",
            {},
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "stopped",
            null,
            [],
            0,
            0
        ]);
        let mut wire = result_wire();
        wire["agent_metadata"] = positional_agent;
        assert!(serde_json::from_value::<AgentResultResponse>(wire).is_err());
        let positional_turn = json!(["a1", 1, "turn-1", "unknown", null, null, null, null]);
        let mut wire = result_wire();
        wire["turns"] = json!([positional_turn]);
        assert!(serde_json::from_value::<AgentResultResponse>(wire).is_err());
        for entry in [Value::Null, json!([]), json!(1), json!("turn")] {
            let mut wire = result_wire();
            wire["turns"] = json!([entry]);
            assert!(serde_json::from_value::<AgentResultResponse>(wire).is_err());
        }
    }

    #[test]
    fn children_request_defaults_limit_and_keeps_ids_verbatim() {
        let request: CommandChildrenRequest =
            serde_json::from_value(json!({"id":"parent-1"})).unwrap();
        assert_eq!(request, CommandChildrenRequest::new("parent-1"));
        assert_eq!(request.after, None);
        assert_eq!(request.limit, 100);
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"id":"parent-1","limit":100})
        );
        let cursor: CommandChildrenRequest =
            serde_json::from_value(json!({"id":" parent ","after":" child ","limit":7})).unwrap();
        assert_eq!(cursor.id, " parent ");
        assert_eq!(cursor.after.as_deref(), Some(" child "));
        assert_eq!(cursor.limit, 7);
        cursor.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&cursor).unwrap(),
            json!({"id":" parent ","after":" child ","limit":7})
        );
    }

    #[test]
    fn children_request_rejects_null_wrong_types_and_bad_limits() {
        for bad in [
            json!({}),
            json!({"id":null}),
            json!({"id":1}),
            json!({"id":""}),
            json!({"id":" \t "}),
            json!({"id":"parent-1","after":null}),
            json!({"id":"parent-1","after":1}),
            json!({"id":"parent-1","after":""}),
            json!({"id":"parent-1","after":" \t "}),
            json!({"id":"parent-1","limit":null}),
            json!({"id":"parent-1","limit":0}),
            json!({"id":"parent-1","limit":1001}),
            json!({"id":"parent-1","limit":-1}),
            json!({"id":"parent-1","limit":1.0}),
            json!({"id":"parent-1","limit":"100"}),
            json!({"id":"parent-1","extra":0}),
        ] {
            assert!(
                serde_json::from_value::<CommandChildrenRequest>(bad.clone()).is_err(),
                "accepted {bad}"
            );
        }
        for limit in [1, 1000] {
            let request: CommandChildrenRequest =
                serde_json::from_value(json!({"id":"parent-1","limit":limit})).unwrap();
            assert_eq!(request.limit, limit);
            request.validate().unwrap();
        }
        let mut request = CommandChildrenRequest::new("parent-1");
        request.limit = 0;
        assert!(request.validate().is_err());
        request.limit = 100;
        request.after = Some(" ".to_owned());
        assert!(request.validate().is_err());
    }

    #[test]
    fn children_response_requires_objects_and_an_explicit_next() {
        let wire = json!({"children":[],"next":null});
        let response: CommandChildrenResponse = serde_json::from_value(wire.clone()).unwrap();
        assert!(response.children.is_empty());
        assert_eq!(response.next, None);
        assert_eq!(serde_json::to_value(response).unwrap(), wire);
        for missing in ["children", "next"] {
            let mut wire = wire.clone();
            wire.as_object_mut().unwrap().remove(missing);
            assert!(serde_json::from_value::<CommandChildrenResponse>(wire).is_err());
        }
        for bad in [Value::Null, json!(false), json!("bad")] {
            let mut wire = wire.clone();
            wire["children"] = bad;
            assert!(serde_json::from_value::<CommandChildrenResponse>(wire).is_err());
        }
        let mut positional = wire.clone();
        positional["children"] = json!([["parent-1/send"]]);
        assert!(serde_json::from_value::<CommandChildrenResponse>(positional).is_err());
        for entry in [Value::Null, json!([]), json!(1), json!("child")] {
            let mut wire = wire.clone();
            wire["children"] = json!([entry]);
            assert!(serde_json::from_value::<CommandChildrenResponse>(wire).is_err());
        }
    }
}
