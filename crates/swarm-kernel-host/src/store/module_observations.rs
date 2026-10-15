//! Read existing retained native observations for the runtime catalog.
//!
//! Every fact in this module is read back from the Store after a real producer
//! wrote it. Nothing here probes, launches, dispatches, or calls a native
//! service. A fact is surfaced only while it is still current for the exact
//! binding that produced it: the binding must be live and ready, its module
//! artifact must match the configured route artifact, its native boot and
//! observation identity must still match the retained payload, and the payload
//! must describe the configured route. Stale, wrong-owner, foreign-Task, or
//! unread evidence stays an explicit gap rather than a supported value.

use crate::{
    config::Route,
    error::{Error, Result},
    model::{Principal, Role},
};
use rusqlite::Connection;
use serde_json::{Map, Value, json};

/// One live binding as retained in the Store. `native` is the
/// `bindings.state_json` observation object exactly as its module last wrote
/// it; `effective` carries the folded effective-configuration records. The
/// remaining fields are the binding's own current fences.
#[derive(Debug, Clone)]
pub(super) struct RetainedNativeRead {
    pub binding_id: String,
    pub generation: i64,
    pub route_alias: String,
    pub module_artifact_id: String,
    pub module_contract_selector: Option<Value>,
    pub bridge_boot_id: Option<String>,
    pub native: Value,
    pub effective: Value,
    pub observed_at_ms: Option<i64>,
    pub native_sequence: Option<i64>,
    pub native_observation_id: Option<i64>,
    pub released: bool,
}

/// One route-shaped projection of a current native read. Every optional field
/// is a genuinely retained fact or an explicit gap. Nothing here is inferred
/// from configuration, a binary, or a requested value.
#[derive(Debug, Clone, Default)]
pub(super) struct ObservedNativeFacts {
    pub observed_at_ms: Option<i64>,
    pub native_sequence: Option<i64>,
    pub native_observation_id: Option<i64>,
    pub binding_id: Option<String>,
    pub binding_generation: Option<i64>,
    pub runtime: Option<String>,
    pub module_artifact_id: Option<String>,
    pub module_selector: Option<Value>,
    pub requested_model: Option<String>,
    pub observed_model: Option<String>,
    pub model_provider: Option<String>,
    pub model_status: Option<String>,
    pub model_definition: Option<Value>,
    pub supported_capabilities: Vec<String>,
    pub unsupported_capabilities: Vec<String>,
    pub gaps: Vec<String>,
}

fn bounded_text(value: Option<&str>, maximum: usize) -> Option<String> {
    let value = value?;
    let owned = value.trim();
    if owned.is_empty() || owned.len() > maximum || owned.chars().any(char::is_control) {
        return None;
    }
    Some(owned.to_owned())
}

fn positive_i64(value: &Value) -> Option<i64> {
    value.as_i64().filter(|value| *value > 0)
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = Vec::new();
    for value in values {
        if !seen.iter().any(|existing| existing == &value) {
            seen.push(value);
        }
    }
    seen
}

/// Read the live bindings that carry a native observation. This is one SELECT
/// over the existing tables; it never opens or contacts a module. Every
/// `SELECT_BINDINGS_COLUMNS` column is a stored fact, never a live read.
pub(super) fn live_reads(conn: &Connection) -> Result<Vec<RetainedNativeRead>> {
    // Production bindings carry released_at_ms as a column, not inside state_json.
    // Minimal/test DBs without the table mean no live bindings, never an error.
    let rows: Vec<(String, i64, String, String, String, Option<i64>)> = match conn.prepare(
        "SELECT binding_id,generation,module_artifact_id,route_json,state_json,released_at_ms FROM bindings",
    ) {
        Ok(mut statement) => statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        Err(_) => match conn.prepare(
            "SELECT binding_id,generation,module_artifact_id,route_json,state_json FROM bindings",
        ) {
            Ok(mut fallback) => fallback
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        None,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            Err(_) => return Ok(Vec::new()),
        },
    };
    let mut reads = Vec::new();
    for (binding_id, generation, module_artifact_id, route_json, state_json, released_at_ms) in rows {
        if bounded_text(Some(&binding_id), 256).is_none() || generation <= 0 {
            continue;
        }
        let route: Value = serde_json::from_str(&route_json).unwrap_or(Value::Null);
        let state: Value = serde_json::from_str(&state_json).unwrap_or(Value::Null);
        let Some(route_alias) = bounded_text(route["alias"].as_str(), 128) else {
            continue;
        };
        let mut effective = Map::new();
        for key in [
            "effective_settings",
            "effective_agent",
            "effective_model",
        ] {
            let slot = &state[key];
            if slot.is_object() {
                effective.insert(key.to_owned(), slot.clone());
            }
        }
        reads.push(RetainedNativeRead {
            binding_id,
            generation,
            route_alias,
            module_artifact_id,
            module_contract_selector: state["module_contract_selector"]
                .as_object()
                .cloned()
                .map(Value::Object),
            bridge_boot_id: bounded_text(state["bridge_boot_id"].as_str(), 256),
            native: state["native"].clone(),
            effective: Value::Object(effective),
            observed_at_ms: positive_i64(&state["observed_at_ms"]),
            native_sequence: state["native_sequence"]
                .as_i64()
                .filter(|sequence| *sequence >= 0),
            native_observation_id: positive_i64(&state["native_observation_id"]),
            released: released_at_ms.is_some() || !state["released_at_ms"].is_null(),
        });
    }
    Ok(reads)
}

/// A read is current only while its observation payload and its binding row still
/// agree on every fence. Any mismatch is a stale gap, never a partial answer.
pub(super) fn observation_is_current(read: &RetainedNativeRead) -> bool {
    if read.released {
        return false;
    }
    let native = &read.native;
    if !native.is_object() {
        return false;
    }
    let (Some(boot_id), Some(binding_boot)) = (
        bounded_text(native["boot_id"].as_str(), 256),
        read.bridge_boot_id.as_deref(),
    ) else {
        return false;
    };
    if boot_id != binding_boot {
        return false;
    }
    if read.native_sequence.is_none() || read.native_observation_id.is_none() {
        return false;
    }
    // The Store writer keeps the monotonic sequence beside the payload
    // (state_json.native_sequence) while the event_id lives beside the
    // observation row (observations.source_event_key, e.g. "boot:seq" from
    // the Codex bridge). The native payload itself need not echo either
    // field; when it does echo them they must still agree, otherwise the
    // retained row has diverged from its payload.
    if let Some(echoed) = native.get("observe_sequence")
        && !echoed.is_null()
        && echoed.as_i64() != read.native_sequence
    {
        return false;
    }
    if let Some(echoed) = native.get("event_id") && !echoed.is_null() {
        let Some(echoed) = bounded_text(echoed.as_str(), 512) else {
            return false;
        };
        if echoed != format!("{}:{}", boot_id, read.native_sequence.unwrap_or_default()) {
            return false;
        }
    }
    true
}

/// Project one live read into route-shaped facts. Reads whose module artifact does
/// not match the configured route, or whose observation is stale, contribute only
/// an explicit gap. `route` is the configured entry for this alias.
pub(super) fn route_facts(read: &RetainedNativeRead, route: &Route) -> ObservedNativeFacts {
    let mut facts = ObservedNativeFacts::default();
    if read.released || read.route_alias != route.alias {
        facts
            .gaps
            .push("retained_binding_is_not_current_for_its_route".to_owned());
        return facts;
    }
    if read.module_artifact_id != route.module_artifact_id {
        facts
            .gaps
            .push("retained_binding_module_artifact_does_not_match_configured_route".to_owned());
        return facts;
    }
    facts.binding_id = Some(read.binding_id.clone());
    facts.binding_generation = Some(read.generation);
    facts.module_artifact_id = Some(read.module_artifact_id.clone());
    facts.module_selector = read.module_contract_selector.clone();
    facts.observed_at_ms = read.observed_at_ms;
    facts.native_sequence = read.native_sequence;
    facts.native_observation_id = read.native_observation_id;
    if !observation_is_current(read) {
        facts
            .gaps
            .push("retained_native_observation_does_not_match_its_current_binding".to_owned());
        return facts;
    }
    let native = &read.native;
    let describe = &native["describe"];
    facts.runtime = bounded_text(Some(route.runtime.as_str()), 128);
    facts.requested_model = bounded_text(describe["requested_model"].as_str(), 256);
    facts.observed_model = bounded_text(describe["effective_model"].as_str(), 256);
    facts.model_provider = bounded_text(describe["effective_model_provider"].as_str(), 256);
    facts.model_status = bounded_text(describe["served_model_status"].as_str(), 64);
    // Codex modules retain capabilities as a top-level map
    // (capability -> "implemented" / "unavailable"); other adapters may
    // retain describe.capabilities.supported/unsupported arrays. Both shapes
    // are adapter vocabulary and are only surfaced with the live-binding fences above.
    let mut supported: Vec<String> = Vec::new();
    let mut unsupported: Vec<String> = Vec::new();
    if let Some(map) = native.get("capabilities").and_then(|v| v.as_object()) {
        for (name, state) in map {
            let Some(name) = bounded_text(Some(name.as_str()), 64) else { continue };
            match state.as_str() {
                Some("implemented") => supported.push(name),
                Some("unavailable") => unsupported.push(name),
                _ => continue,
            }
        }
    }
    if supported.is_empty() && unsupported.is_empty() {
        let capabilities = &describe["capabilities"];
        for (key, slot) in [("supported", 0usize), ("unsupported", 1usize)] {
            let names = capabilities[key]
                .as_array()
                .map(|names| {
                    names
                        .iter()
                        .filter_map(|name| bounded_text(name.as_str(), 64))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if slot == 0 {
                supported.extend(names);
            } else {
                unsupported.extend(names);
            }
        }
    }
    if supported.is_empty() {
        facts
            .gaps
            .push("native_supported_capability_list_is_not_retained".to_owned());
    } else {
        facts.supported_capabilities = dedupe(supported);
    }
    if unsupported.is_empty() {
        facts
            .gaps
            .push("native_unsupported_capability_list_is_not_retained".to_owned());
    } else {
        facts.unsupported_capabilities = dedupe(unsupported);
    }
    let model_state = &read.effective["effective_model"];
    if model_state["complete"] == true && bounded_text(model_state["kind"].as_str(), 64).is_some() {
        let mut definition = Map::new();
        definition.insert("kind".to_owned(), model_state["kind"].clone());
        if let Some(revision) = bounded_text(model_state["revision"].as_str(), 128) {
            definition.insert("revision".to_owned(), json!(revision));
        }
        if let Some(revision) = bounded_text(model_state["revision"].as_str(), 256)
            && definition.get("revision").is_none()
        {
            definition.insert("revision".to_owned(), json!(revision));
        }
        // OpenCode folded records carry model as {id,providerID,variant} or Null;
        // older fixtures may carry it as a plain string. Preserve either without inventing.
        match &model_state["model"] {
            Value::String(text) => {
                if let Some(model) = bounded_text(Some(text.as_str()), 256) {
                    definition.insert("model".to_owned(), json!(model));
                }
            }
            Value::Object(_) => {
                definition.insert("model".to_owned(), model_state["model"].clone());
            }
            Value::Null => {}
            _ => {}
        }
        for key in ["enabled", "status", "definition_digest", "variant_digest", "observation_id", "observed_at_ms", "operation_id", "contract_revision"] {
            let value = &model_state[key];
            if value.is_boolean() || value.is_number() || bounded_text(value.as_str(), 256).is_some() {
                definition.insert(key.to_owned(), value.clone());
            }
        }
        facts.model_definition = Some(Value::Object(definition));
    } else if facts.observed_model.is_some() {
        facts
            .gaps
            .push("native_model_definition_is_not_confirmed_for_the_observed_model".to_owned());
    }
    facts
}

/// The authenticated Manager-facing entry point. Only an authenticated Manager
/// identity reaches the retained-evidence path; every other role is denied before
/// any native fact is read.
pub(super) fn reads_for_manager(
    conn: &Connection,
    principal: &Principal,
    routes: &[Route],
) -> Result<Vec<(Route, ObservedNativeFacts)>> {
    if principal.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "retained native evidence is restricted to authenticated Manager identities",
        ));
    }
    let reads = live_reads(conn)?;
    let mut projected = Vec::new();
    for route in routes {
        let matching = reads
            .iter()
            .filter(|read| !read.released && read.route_alias == route.alias)
            .max_by_key(|read| read.generation)
            .cloned();
        let facts = match matching {
            Some(read) => route_facts(&read, route),
            None => {
                let mut facts = ObservedNativeFacts::default();
                facts
                    .gaps
                    .push("no_live_binding_is_retained_for_this_route".to_owned());
                facts
            }
        };
        projected.push((route.clone(), facts));
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    use super::{RetainedNativeRead, observation_is_current, route_facts};
    use crate::config::Route;

    fn route(artifact: &str) -> Route {
        Route {
            alias: "codex".to_owned(),
            runtime: "codex".to_owned(),
            module_artifact_id: artifact.to_owned(),
            enabled: true,
            native_options: serde_json::json!({}),
            workspace_option: None,
            owned_service: None,
            admission_policy: None,
        }
    }

    fn read(released: bool, observed_sequence: i64) -> RetainedNativeRead {
        RetainedNativeRead {
            binding_id: "binding-1".to_owned(),
            generation: 1,
            route_alias: "codex".to_owned(),
            module_artifact_id: "eliot-codex.rust-headless.1".to_owned(),
            module_contract_selector: None,
            bridge_boot_id: Some("boot-1".to_owned()),
            native: serde_json::json!({
                "boot_id":"boot-1",
                "describe":{
                    "requested_model":"gpt-5.6-luna",
                    "effective_model":"gpt-5.6-luna",
                    "effective_model_provider":"openai",
                    "served_model_status":"unknown"
                },
                "capabilities":{
                    "agent.open":"implemented",
                    "task.dispatch":"implemented",
                    "agent.refresh":"unavailable"
                }
            }),
            effective: serde_json::json!({
                "effective_model":{
                    "kind":"opencode_session_model_v1",
                    "revision":"rev-1",
                    "model":{"id":"model-id","providerID":"provider","variant":"variant"},
                    "enabled":true,
                    "status":"active",
                    "complete":true
                }
            }),
            observed_at_ms: Some(1000),
            native_sequence: Some(observed_sequence),
            native_observation_id: Some(42),
            released,
        }
    }

    #[test]
    fn current_retained_observation_projects_the_observed_model_and_capabilities() {
        let facts = route_facts(&read(false, 7), &route("eliot-codex.rust-headless.1"));
        assert_eq!(facts.observed_model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(facts.requested_model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(facts.model_provider.as_deref(), Some("openai"));
        assert!(facts.supported_capabilities.contains(&"task.dispatch".to_owned()));
        assert!(facts.unsupported_capabilities.contains(&"agent.refresh".to_owned()));
        assert!(facts.model_definition.is_some());
        assert_eq!(facts.native_observation_id, Some(42));
        assert_eq!(facts.binding_id.as_deref(), Some("binding-1"));
        assert!(facts.gaps.is_empty());
    }

    #[test]
    fn stale_boot_sequence_and_released_reads_stay_explicit_gaps() {
        let stale_boot = read(false, 7);
        let mut rebooted = stale_boot.clone();
        rebooted.bridge_boot_id = Some("boot-2".to_owned());
        assert!(!observation_is_current(&rebooted));

        // A retained row that diverged from its payload (echoed sequence 6
        // vs retained sequence 7) is stale, never a partial answer.
        let mut diverged = read(false, 7);
        diverged.native["observe_sequence"] = serde_json::json!(6);
        assert!(!observation_is_current(&diverged));
        let mut diverged_event = read(false, 7);
        diverged_event.native["event_id"] = serde_json::json!("boot-1:6");
        assert!(!observation_is_current(&diverged_event));

        let mut released = read(false, 7);
        released.released = true;
        let facts = route_facts(&released, &route("eliot-codex.rust-headless.1"));
        assert!(facts.observed_model.is_none());
        assert!(facts.supported_capabilities.is_empty());
        assert!(facts.gaps.iter().any(|gap| gap.contains("not_current_for_its_route")));
    }

    #[test]
    fn foreign_artifact_and_alias_reads_never_become_route_facts() {
        let foreign_artifact = route_facts(&read(false, 7), &route("eliot-other.rust-headless.1"));
        assert!(foreign_artifact.observed_model.is_none());
        assert!(
            foreign_artifact
                .gaps
                .iter()
                .any(|gap| gap.contains("does_not_match_configured_route"))
        );

        let mut foreign_route = read(false, 7);
        foreign_route.route_alias = "other".to_owned();
        let other = route_facts(&foreign_route, &route("eliot-codex.rust-headless.1"));
        assert!(other.observed_model.is_none());
        assert!(other.gaps.iter().any(|gap| gap.contains("not_current_for_its_route")));
    }

    #[test]
    fn missing_capability_lists_and_model_definitions_are_declared_not_invented() {
        let mut read = read(false, 7);
        let describe = serde_json::json!({
            "requested_model":"gpt-5.6-luna",
            "effective_model":"gpt-5.6-luna",
            "effective_model_provider":"openai",
            "served_model_status":"unknown"
        });
        read.native["describe"] = describe;
        if let Some(native) = read.native.as_object_mut() {
            native.remove("capabilities");
        }
        read.effective = serde_json::json!({});
        let facts = route_facts(&read, &route("eliot-codex.rust-headless.1"));
        assert!(facts.supported_capabilities.is_empty());
        assert!(facts.unsupported_capabilities.is_empty());
        assert!(facts
            .gaps
            .iter()
            .any(|gap| gap.contains("capability_list_is_not_retained")));
        assert!(facts
            .gaps
            .iter()
            .any(|gap| gap.contains("model_definition_is_not_confirmed")));
    }
}
