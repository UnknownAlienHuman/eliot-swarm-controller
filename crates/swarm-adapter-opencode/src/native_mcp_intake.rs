//! Typed admission for one Store-enriched native MCP RuntimeCommand.
//!
//! The shared [`NativeMcpCommand`] is the outer command identity.  The
//! `effect` member is a private, host-enriched handoff for the sibling native
//! executor; it is removed before DTO deserialization and is never copied to
//! a RuntimeOutcome.  Keeping the two layers explicit lets the executor keep
//! its existing prepared request/challenge validation while the adapter binds
//! that request to the authenticated operation before connecting to OpenCode.

use crate::module_runtime;
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::{Error, Result},
    module_catalog::Sha256Digest,
    module_contract::ModuleContractClaim,
    native_mcp::{NativeMcpCommand, NativeMcpObservationKind, NativeMcpPhase},
    runtime::RuntimeCommand,
};

const EFFECT_FIELD: &str = "effect";
const MAX_EFFECT_BYTES: usize = 1_048_576;
const ASSIGNMENT_OBSERVATION_KIND: &str = "swarm.native_mcp_assignment_observation";

/// The effect owner receives a command whose input is the exact private
/// effect envelope.  Its operation/binding/generation fields remain those of
/// the authenticated outer command, so the existing executor cannot silently
/// retarget a request while parsing its prepared HTTP body.
pub(crate) struct AdmittedNativeMcp {
    pub(crate) effect_command: RuntimeCommand,
}

/// Validate the descriptor opt-in, bind the typed DTO to the outer command,
/// and verify the phase-specific private artifact before any native client is
/// opened or effect is attempted.
pub(crate) fn admit(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<AdmittedNativeMcp> {
    if !module_runtime::native_mcp_enabled(claim) {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY",
            "native MCP command requires the exact expanded descriptor claim",
        ));
    }
    let input = command.input.as_object().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command input must be an object",
        )
    })?;
    let effect = input.get(EFFECT_FIELD).cloned().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command has no private effect handoff",
        )
    })?;
    bounded_json(&effect)?;

    // Deserialize the DTO from the direct command object.  Only the one
    // host-enriched private field is removed; all DTO fields remain subject
    // to deny_unknown_fields and the shared schema's exact validation.
    let mut dto_object = input.clone();
    dto_object.remove(EFFECT_FIELD);
    let dto_value = Value::Object(dto_object);
    let native_command: NativeMcpCommand = serde_json::from_value(dto_value).map_err(|_| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command does not match the shared DTO",
        )
    })?;
    native_command.validate_against(command).map_err(|_| {
        Error::new(
            "NATIVE_MCP_IDENTITY",
            "native MCP command differs from its authenticated RuntimeCommand",
        )
    })?;
    validate_effect(&native_command, &effect)?;

    let mut effect_command = command.clone();
    effect_command.input = effect;
    Ok(AdmittedNativeMcp { effect_command })
}

fn validate_effect(command: &NativeMcpCommand, effect: &Value) -> Result<()> {
    let object = effect.as_object().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect handoff must be an object",
        )
    })?;
    if object.get("schema_version") != Some(&Value::from(1_u64))
        || object.get("kind").and_then(Value::as_str) != Some("swarm.native_mcp_command")
    {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect handoff schema is not swarm.native_mcp_command@1",
        ));
    }
    let expected_action = match command.phase {
        NativeMcpPhase::Install => "install",
        NativeMcpPhase::Observe => "observe",
        NativeMcpPhase::Arm => "arm",
        NativeMcpPhase::Read => "read",
    };
    if object.get("action").and_then(Value::as_str) != Some(expected_action) {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect action differs from the admitted phase",
        ));
    }

    let expected_observation_kind = match command.observation_kind {
        Some(NativeMcpObservationKind::InstalledServer) => Some("installed_server"),
        Some(NativeMcpObservationKind::AssignedSession) => Some("assigned_session"),
        None => None,
    };
    match (expected_observation_kind, object.get("observation_kind")) {
        (Some(expected), Some(value)) if value.as_str() == Some(expected) => {}
        (None, None) => {}
        _ => {
            return Err(Error::new(
                "NATIVE_MCP_EFFECT",
                "native MCP effect observation kind differs from its admitted DTO",
            ));
        }
    }

    let (field, reference) = match command.phase {
        NativeMcpPhase::Install | NativeMcpPhase::Observe => {
            ("prepared", command.prepared_command.as_ref())
        }
        NativeMcpPhase::Arm | NativeMcpPhase::Read => ("challenge", command.challenge.as_ref()),
    };
    let reference = reference.ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP phase has no matching protected artifact reference",
        )
    })?;
    let artifact = object.get(field).ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP effect has no matching private artifact",
        )
    })?;
    let digest = digest_json(artifact)?;
    if digest != reference.sha256.as_str() {
        return Err(Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP private artifact differs from its admitted SHA-256",
        ));
    }
    if command.observation_kind == Some(NativeMcpObservationKind::AssignedSession) {
        validate_assignment_observation(command, object, artifact)?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeMcpAssignmentObservationArtifact {
    schema_version: u16,
    kind: String,
    assignment: Value,
    assignment_sha256: Sha256Digest,
    native_session_id: String,
    location_sha256: Sha256Digest,
    service_id: String,
    service_pid: u32,
    service_version: String,
}

fn validate_assignment_observation(
    command: &NativeMcpCommand,
    effect: &Map<String, Value>,
    artifact: &Value,
) -> Result<()> {
    let artifact: NativeMcpAssignmentObservationArtifact = serde_json::from_value(artifact.clone())
        .map_err(|_| {
            Error::new(
                "NATIVE_MCP_ARTIFACT",
                "assigned-session observation artifact schema is invalid",
            )
        })?;
    let scope = effect
        .get("scope")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_SCOPE",
                "assigned-session effect has no exact scope",
            )
        })?;
    let assignment = scope
        .get("assignment")
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_SCOPE",
                "assigned-session effect has no exact assignment",
            )
        })?;
    let directory = scope
        .get("directory")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_SCOPE",
                "assigned-session effect has no exact location",
            )
        })?;
    let scope_pid = scope.get("service_pid").and_then(Value::as_u64);

    let assignment_digest = digest_json(assignment)?;
    let location_digest = digest_text(directory);
    if artifact.schema_version != 1
        || artifact.kind != ASSIGNMENT_OBSERVATION_KIND
        || artifact.assignment != *assignment
        || artifact.assignment_sha256.as_str() != command.assignment_sha256.as_str()
        || artifact.assignment_sha256.as_str() != assignment_digest
        || artifact.native_session_id != command.native_session_id
        || artifact.native_session_id
            != assignment
                .get("native_session_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
        || artifact.location_sha256.as_str() != command.location_sha256.as_str()
        || artifact.location_sha256.as_str() != location_digest
        || artifact.service_id != command.service_id
        || artifact.service_pid != command.service_pid
        || scope_pid != Some(u64::from(command.service_pid))
        || artifact.service_version != command.service_version
        || scope.get("binding_id").and_then(Value::as_str) != Some(command.binding_id.as_str())
        || scope.get("binding_generation").and_then(Value::as_i64)
            != Some(command.binding_generation)
        || scope.get("service_id").and_then(Value::as_str) != Some(command.service_id.as_str())
        || scope.get("expected_version").and_then(Value::as_str)
            != Some(command.service_version.as_str())
        || assignment.get("binding_id").and_then(Value::as_str) != Some(command.binding_id.as_str())
        || assignment.get("binding_generation").and_then(Value::as_i64)
            != Some(command.binding_generation)
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE",
            "assigned-session artifact differs from its DTO or effect scope",
        ));
    }
    Ok(())
}

fn bounded_json(value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| {
        Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP private effect is not valid JSON",
        )
    })?;
    if bytes.len() > MAX_EFFECT_BYTES {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP private effect exceeds its handoff bound",
        ));
    }
    Ok(())
}

pub(crate) fn digest_json(value: &Value) -> Result<String> {
    let canonical = canonical_value(value);
    let bytes = serde_json::to_vec(&canonical).map_err(|_| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP private artifact cannot be canonicalized",
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn digest_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut ordered = Map::new();
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for key in keys {
                ordered.insert(key.clone(), canonical_value(&object[key]));
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        value => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use swarm_contracts::{
        module_catalog::{ProtectedRef, Sha256Digest},
        native_mcp::{NativeMcpObservationKind, ProtectedArtifactRef},
    };

    fn hash_text(value: &str) -> String {
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn scope(directory: &str) -> Value {
        json!({
            "binding_id":"binding_1",
            "binding_generation":3,
            "native_scope_key":"opencode-v2:service_1",
            "service_id":"service_1",
            "service_pid":42,
            "expected_version":"2.0.7",
            "directory":directory,
            "assignment":{
                "task_id":"task_1",
                "task_revision":1,
                "attempt_id":"attempt_1",
                "binding_id":"binding_1",
                "binding_generation":3,
                "native_session_id":"ses_1",
                "participant_id":"participant_1",
                "mcp_profile":"participant",
                "grant_revision":1,
                "participation_basis":"attempt_owner",
                "assignment_id":null,
                "review_assignment_id":null
            }
        })
    }

    fn artifact(scope: &Value) -> Value {
        let assignment = scope["assignment"].clone();
        let directory = scope["directory"].as_str().unwrap();
        json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_assignment_observation",
            "assignment":assignment,
            "assignment_sha256":digest_json(&assignment).unwrap(),
            "native_session_id":"ses_1",
            "location_sha256":hash_text(directory),
            "service_id":"service_1",
            "service_pid":42,
            "service_version":"2.0.7"
        })
    }

    fn assigned_command_and_effect() -> (NativeMcpCommand, Value) {
        let directory = std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let scope = scope(&directory);
        let artifact = artifact(&scope);
        let artifact_sha256 = digest_json(&artifact).unwrap();
        let command = NativeMcpCommand {
            schema_version: 2,
            operation_id: "operation_1".into(),
            binding_id: "binding_1".into(),
            binding_generation: 3,
            input_sha256: Sha256Digest::new("c".repeat(64)).unwrap(),
            assignment_sha256: Sha256Digest::new(
                artifact["assignment_sha256"].as_str().unwrap().to_owned(),
            )
            .unwrap(),
            native_session_id: "ses_1".into(),
            service_id: "service_1".into(),
            service_version: "2.0.7".into(),
            service_pid: 42,
            location_sha256: Sha256Digest::new(
                artifact["location_sha256"].as_str().unwrap().to_owned(),
            )
            .unwrap(),
            phase: NativeMcpPhase::Observe,
            observation_kind: Some(NativeMcpObservationKind::AssignedSession),
            prepared_command: Some(ProtectedArtifactRef {
                protected_ref: ProtectedRef::new("store://native-mcp/operation_1/observe/prepared")
                    .unwrap(),
                sha256: Sha256Digest::new(artifact_sha256).unwrap(),
            }),
            challenge: None,
        };
        let effect = json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":"observe",
            "observation_kind":"assigned_session",
            "scope":scope,
            "prepared":artifact,
        });
        (command, effect)
    }

    fn effect_rejects(command: &NativeMcpCommand, effect: &Value) -> bool {
        validate_effect(command, effect).is_err()
    }

    #[test]
    fn assigned_session_purpose_binds_artifact_hash_and_all_scope_identity() {
        let (command, effect) = assigned_command_and_effect();
        assert!(command.validate().is_ok());
        assert!(validate_effect(&command, &effect).is_ok());

        let mut wrong_kind = effect.clone();
        wrong_kind["observation_kind"] = json!("installed_server");
        assert!(effect_rejects(&command, &wrong_kind));

        let mut wrong_schema = effect.clone();
        wrong_schema["schema_version"] = json!(2);
        assert!(effect_rejects(&command, &wrong_schema));

        for (field, value) in [
            ("binding_id", json!("other_binding")),
            ("binding_generation", json!(4)),
            ("service_id", json!("other_service")),
            ("service_pid", json!(43)),
            ("expected_version", json!("2.0.8")),
            ("directory", json!("C:/other")),
        ] {
            let mut mismatched = effect.clone();
            mismatched["scope"][field] = value;
            assert!(effect_rejects(&command, &mismatched), "scope field {field}");
        }

        let mut wrong_assignment = effect.clone();
        wrong_assignment["scope"]["assignment"]["native_session_id"] = json!("ses_other");
        assert!(effect_rejects(&command, &wrong_assignment));

        let mut wrong_outer_session = command.clone();
        wrong_outer_session.native_session_id = "ses_other".into();
        assert!(effect_rejects(&wrong_outer_session, &effect));

        let mut wrong_outer_service = command.clone();
        wrong_outer_service.service_id = "other_service".into();
        assert!(effect_rejects(&wrong_outer_service, &effect));

        let mut wrong_outer_assignment = command.clone();
        wrong_outer_assignment.assignment_sha256 = Sha256Digest::new("b".repeat(64)).unwrap();
        assert!(effect_rejects(&wrong_outer_assignment, &effect));

        let mut wrong_outer_binding = command.clone();
        wrong_outer_binding.binding_id = "other_binding".into();
        assert!(effect_rejects(&wrong_outer_binding, &effect));

        let mut wrong_outer_generation = command.clone();
        wrong_outer_generation.binding_generation = 4;
        assert!(effect_rejects(&wrong_outer_generation, &effect));

        let mut wrong_outer_pid = command.clone();
        wrong_outer_pid.service_pid = 43;
        assert!(effect_rejects(&wrong_outer_pid, &effect));

        let mut wrong_outer_version = command.clone();
        wrong_outer_version.service_version = "2.0.8".into();
        assert!(effect_rejects(&wrong_outer_version, &effect));

        let mut wrong_outer_location = command.clone();
        wrong_outer_location.location_sha256 = Sha256Digest::new("d".repeat(64)).unwrap();
        assert!(effect_rejects(&wrong_outer_location, &effect));

        let mut unknown_artifact_field = effect.clone();
        unknown_artifact_field["prepared"]["raw_error"] = json!("secret");
        let mut matching_ref = command;
        matching_ref.prepared_command.as_mut().unwrap().sha256 =
            Sha256Digest::new(digest_json(&unknown_artifact_field["prepared"]).unwrap()).unwrap();
        assert!(effect_rejects(&matching_ref, &unknown_artifact_field));
    }

    #[test]
    fn installed_server_observation_keeps_the_existing_prepared_install_artifact() {
        let prepared = json!({
            "server_name":"eliot_test",
            "install_intent":{"schema_version":1,"kind":"install_intent"}
        });
        let artifact_hash = digest_json(&prepared).unwrap();
        let mut command = assigned_command_and_effect().0;
        command.observation_kind = Some(NativeMcpObservationKind::InstalledServer);
        command.prepared_command.as_mut().unwrap().sha256 =
            Sha256Digest::new(artifact_hash).unwrap();
        let effect = json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":"observe",
            "observation_kind":"installed_server",
            "prepared":prepared,
        });
        assert!(validate_effect(&command, &effect).is_ok());
    }

    #[test]
    fn observation_kind_is_forbidden_on_non_observe_phases_and_private_effect_stays_v1() {
        let mut command = assigned_command_and_effect().0;
        command.phase = NativeMcpPhase::Install;
        command.observation_kind = None;
        let prepared = json!({"install_intent":{}});
        command.prepared_command.as_mut().unwrap().sha256 =
            Sha256Digest::new(digest_json(&prepared).unwrap()).unwrap();
        let effect = json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":"install",
            "prepared":prepared,
        });
        assert!(validate_effect(&command, &effect).is_ok());

        let mut purpose_on_install = effect.clone();
        purpose_on_install["observation_kind"] = json!("assigned_session");
        assert!(effect_rejects(&command, &purpose_on_install));

        let mut private_v2 = effect;
        private_v2["schema_version"] = json!(2);
        assert!(effect_rejects(&command, &private_v2));
    }
}
