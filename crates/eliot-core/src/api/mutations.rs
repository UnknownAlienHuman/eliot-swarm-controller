//! Public grammar for the two implemented mutations. Authorization, retained
//! bindings, hydration and effect settlement remain in the Router and Store.

use super::StatusBlocker;
use crate::{CommandState, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use crate::SubmitCommand as CommandSubmitRequest;

impl CommandSubmitRequest {
    pub fn new(id: impl Into<String>, verb: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            target: None,
            verb: verb.into(),
            args: Value::Object(Map::new()),
            expected_generation: None,
            expected_turn_id: None,
            request_id: None,
            probe: false,
        }
    }

    /// Only shared wire shape belongs here. In particular, optional binding
    /// values retain their existing verb/principal-specific admission errors.
    pub fn validate(&self) -> Result<()> {
        if !self.args.is_object() {
            return Err(Error::new("invalid", "command args must be a JSON object"));
        }
        if self.probe && self.verb != "send" {
            return Err(Error::new(
                "unsupported",
                "provider probes are supported only for send",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandSubmitResponse {
    pub command_id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub generation: Option<i64>,
    pub state: CommandState,
    #[serde(deserialize_with = "nullable")]
    pub outcome: Option<Value>,
    #[serde(deserialize_with = "nullable")]
    pub linked_command: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownMode {
    Drain,
    Force,
}

impl ShutdownMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Force => "force",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "drain" => Ok(Self::Drain),
            "force" => Ok(Self::Force),
            _ => Err(Error::new(
                "invalid",
                "daemon.shutdown mode must be drain or force",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonShutdownRequest {
    #[serde(deserialize_with = "shutdown_id")]
    pub id: String,
    pub mode: ShutdownMode,
}

impl DaemonShutdownRequest {
    pub fn new(id: impl Into<String>, mode: ShutdownMode) -> Self {
        Self {
            id: id.into(),
            mode,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_shutdown_id(&self.id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonShutdownResponse {
    pub state: CommandState,
    pub mode: ShutdownMode,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_blockers"
    )]
    pub blockers: Option<Vec<StatusBlocker>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_write_faulted"
    )]
    pub store_write_faulted: Option<bool>,
}

pub(super) fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn validate_shutdown_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new("invalid", "id must be a non-empty string"));
    }
    Ok(())
}

fn shutdown_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    validate_shutdown_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn present_blockers<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Vec<StatusBlocker>>, D::Error> {
    // Named structs may accept positional sequences. Keep the existing small
    // object-map boundary for nested public results instead of cloning schemas.
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map(Some)
}

fn present_write_faulted<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error> {
    bool::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_json_sha256;
    use serde_json::json;

    #[test]
    fn submit_reuses_the_existing_model_and_omitted_args_default_to_an_object() {
        let request: CommandSubmitRequest =
            serde_json::from_value(json!({"id":"raw-id","verb":"send"})).unwrap();
        let existing: crate::SubmitCommand = request;
        assert_eq!(existing, CommandSubmitRequest::new("raw-id", "send"));
        existing.validate().unwrap();
        let nullable: CommandSubmitRequest = serde_json::from_value(json!({
            "id":"raw-id","verb":"send","target":null,"expected_generation":null,
            "expected_turn_id":null,"request_id":null
        }))
        .unwrap();
        assert_eq!(nullable, existing);
        // Preserve SubmitCommand's existing serializer, including nullable bindings.
        assert_eq!(
            serde_json::to_value(existing).unwrap(),
            json!({
                "id":"raw-id","target":null,"verb":"send","args":{},
                "expected_generation":null,"expected_turn_id":null,"request_id":null
            })
        );
    }

    #[test]
    fn probe_false_preserves_legacy_wire_and_digest_and_true_is_send_only() {
        let request = CommandSubmitRequest::new("raw-id", "send");
        let legacy_wire = json!({
            "id":"raw-id",
            "target":null,
            "verb":"send",
            "args":{},
            "expected_generation":null,
            "expected_turn_id":null,
            "request_id":null
        });
        let serialized = serde_json::to_value(&request).unwrap();
        assert_eq!(serialized, legacy_wire);
        assert_eq!(
            canonical_json_sha256(&serialized).unwrap(),
            canonical_json_sha256(&legacy_wire).unwrap()
        );
        let explicit_false: CommandSubmitRequest = serde_json::from_value(json!({
            "id":"raw-id", "verb":"send", "probe":false
        }))
        .unwrap();
        assert_eq!(serde_json::to_value(explicit_false).unwrap(), legacy_wire);

        let mut probe = request.clone();
        probe.probe = true;
        probe.validate().unwrap();
        let probe_wire = serde_json::to_value(&probe).unwrap();
        assert_eq!(probe_wire["probe"], true);
        assert_ne!(
            canonical_json_sha256(&probe_wire).unwrap(),
            canonical_json_sha256(&legacy_wire).unwrap()
        );

        for verb in ["launch", "resume", "interrupt", "stop"] {
            let mut invalid = CommandSubmitRequest::new("raw-id", verb);
            invalid.probe = true;
            assert_eq!(invalid.validate().unwrap_err().code, "unsupported");
        }
        for bad in [Value::Null, json!("true"), json!(1)] {
            assert!(
                serde_json::from_value::<CommandSubmitRequest>(json!({
                    "id":"raw-id", "verb":"send", "probe":bad
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn submit_rejects_wrong_field_types_and_unknown_public_members() {
        for field in ["id", "verb"] {
            let mut request = json!({"id":"id","verb":"send"});
            request.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<CommandSubmitRequest>(request).is_err());
            for bad in [Value::Null, json!(true), json!(1), json!([]), json!({})] {
                let mut request = json!({"id":"id","verb":"send"});
                request[field] = bad;
                assert!(serde_json::from_value::<CommandSubmitRequest>(request).is_err());
            }
        }
        for field in ["target", "expected_turn_id", "request_id"] {
            for bad in [json!(true), json!(1), json!([]), json!({})] {
                assert!(
                    serde_json::from_value::<CommandSubmitRequest>(
                        json!({"id":"id","verb":"send",(field):bad})
                    )
                    .is_err()
                );
            }
        }
        for bad in [Value::Null, json!("true"), json!(1), json!([]), json!({})] {
            assert!(
                serde_json::from_value::<CommandSubmitRequest>(json!({
                    "id":"id", "verb":"send", "probe":bad
                }))
                .is_err()
            );
        }
        for bad in [
            json!(true),
            json!("1"),
            json!(1.0),
            json!([]),
            json!({}),
            json!(i64::MAX as u64 + 1),
        ] {
            assert!(
                serde_json::from_value::<CommandSubmitRequest>(
                    json!({"id":"id","verb":"send","expected_generation":bad})
                )
                .is_err()
            );
        }
        for field in ["selector", "probe", "wait_ms", "extra"] {
            assert!(
                serde_json::from_value::<CommandSubmitRequest>(
                    json!({"id":"id","verb":"send",(field):null})
                )
                .is_err()
            );
        }
    }

    #[test]
    fn submit_validation_preserves_downstream_binding_and_verb_errors() {
        let request: CommandSubmitRequest = serde_json::from_value(json!({
            "id":"", "target":"", "verb":"", "expected_generation":-1,
            "expected_turn_id":"", "request_id":"", "args":{"opaque_vendor_field":[null,true]}
        }))
        .unwrap();
        request.validate().unwrap(); // ULID, verb and principal-specific rules remain downstream.
        assert_eq!(request.expected_generation, Some(-1));
        assert_eq!(request.expected_turn_id.as_deref(), Some(""));
        for bad in [
            Value::Null,
            json!([]),
            json!(true),
            json!(1),
            json!("{}"),
            json!([[], []]),
        ] {
            let request: CommandSubmitRequest =
                serde_json::from_value(json!({"id":"id","verb":"send","args":bad})).unwrap();
            let error = request.validate().unwrap_err();
            assert_eq!(error.code, "invalid");
            assert_eq!(error.message, "command args must be a JSON object");
        }
    }

    #[test]
    fn true_probe_is_supported_only_for_send() {
        let mut send = CommandSubmitRequest::new("id", "send");
        send.probe = true;
        send.validate().unwrap();

        for verb in ["launch", "resume", "interrupt", "stop"] {
            let mut request = CommandSubmitRequest::new("id", verb);
            request.probe = true;
            assert_eq!(request.validate().unwrap_err().code, "unsupported");
        }
    }

    #[test]
    fn shutdown_request_requires_exact_mode_and_keeps_id_bytes() {
        for mode in [ShutdownMode::Drain, ShutdownMode::Force] {
            let request = DaemonShutdownRequest::new(" raw-id ", mode);
            request.validate().unwrap();
            let wire = json!({"id":" raw-id ","mode":mode.as_str()});
            assert_eq!(serde_json::to_value(&request).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<DaemonShutdownRequest>(wire).unwrap(),
                request
            );
            assert_eq!(ShutdownMode::parse(mode.as_str()).unwrap(), mode);
        }
        for bad in ["", "Drain", "draining", "forcing", " force ", "other"] {
            assert_eq!(ShutdownMode::parse(bad).unwrap_err().code, "invalid");
            assert!(
                serde_json::from_value::<DaemonShutdownRequest>(json!({"id":"id","mode":bad}))
                    .is_err()
            );
        }
        for request in [
            json!({}),
            json!({"mode":"force"}),
            json!({"id":"id"}),
            json!({"id":"id","mode":"force","extra":0}),
        ] {
            assert!(serde_json::from_value::<DaemonShutdownRequest>(request).is_err());
        }
        for bad in [
            Value::Null,
            json!(true),
            json!(1),
            json!([]),
            json!({}),
            json!(""),
            json!(" \t "),
        ] {
            assert!(
                serde_json::from_value::<DaemonShutdownRequest>(json!({"id":bad,"mode":"force"}))
                    .is_err()
            );
        }
        for bad in [Value::Null, json!(false), json!(1), json!([]), json!({})] {
            assert!(
                serde_json::from_value::<DaemonShutdownRequest>(json!({"id":"id","mode":bad}))
                    .is_err()
            );
        }
        assert!(
            DaemonShutdownRequest::new(" ", ShutdownMode::Drain)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn submit_result_requires_all_six_members_even_when_nullable() {
        let wire = json!({"command_id":"id","agent":null,"generation":null,
            "state":"unknown","outcome":null,"linked_command":null});
        let reply: CommandSubmitResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(reply).unwrap(), wire);
        for field in [
            "command_id",
            "agent",
            "generation",
            "state",
            "outcome",
            "linked_command",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<CommandSubmitResponse>(missing).is_err(),
                "accepted missing {field}"
            );
        }
        for (field, value) in [
            ("agent", json!(1)),
            ("generation", json!(1.0)),
            ("linked_command", json!([])),
            ("state", json!("complete")),
        ] {
            let mut malformed = wire.clone();
            malformed[field] = value;
            assert!(serde_json::from_value::<CommandSubmitResponse>(malformed).is_err());
        }
    }

    #[test]
    fn submit_result_preserves_opaque_outcomes_and_all_command_states() {
        for state in [
            "accepted",
            "queued",
            "dispatched",
            "applied",
            "rejected",
            "failed",
            "unknown",
            "cancelled",
        ] {
            for outcome in [
                json!({"code":"unknown","message":"retained", "diagnostics":[
                    {"source":"plugin","stderr":{"text":"bounded\ntext","truncated":true,"eof":false,"read_error":false}},
                    {"source":"harness","stderr":{"text":"other","truncated":false,"eof":true,"read_error":true}}
                ]}),
                json!(["opaque", null]),
                json!(true),
                json!(7),
                json!("text"),
                Value::Null,
            ] {
                let wire = json!({"command_id":"id","agent":"a1","generation":0,
                    "state":state,"outcome":outcome,"linked_command":"id/send"});
                let reply: CommandSubmitResponse = serde_json::from_value(wire.clone()).unwrap();
                assert_eq!(serde_json::to_value(reply).unwrap(), wire);
            }
        }
    }

    #[test]
    fn shutdown_result_preserves_drain_force_and_store_fault_shapes() {
        for wire in [
            json!({"state":"applied","mode":"drain","blockers":[]}),
            json!({"state":"dispatched","mode":"drain","blockers":[{"kind":"agent_live","agent":"a1","generation":0,"state":"created"}]}),
            json!({"state":"dispatched","mode":"force"}),
            json!({"state":"dispatched","mode":"force","store_write_faulted":true}),
        ] {
            let reply: DaemonShutdownResponse = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(reply).unwrap(), wire);
        }
        for field in ["state", "mode"] {
            let mut missing = json!({"state":"dispatched","mode":"force"});
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<DaemonShutdownResponse>(missing).is_err());
        }
    }

    #[test]
    fn shutdown_result_optional_members_are_nonnull_and_blockers_are_objects() {
        for value in [
            Value::Null,
            json!(true),
            json!({}),
            json!("all"),
            json!([[]]),
            json!([["agent_live", "a1", 0, "created"]]),
        ] {
            assert!(
                serde_json::from_value::<DaemonShutdownResponse>(
                    json!({"state":"dispatched","mode":"drain","blockers":value})
                )
                .is_err()
            );
        }
        for value in [Value::Null, json!(1), json!("true"), json!([]), json!({})] {
            assert!(
                serde_json::from_value::<DaemonShutdownResponse>(
                    json!({"state":"dispatched","mode":"force","store_write_faulted":value})
                )
                .is_err()
            );
        }
    }
}
