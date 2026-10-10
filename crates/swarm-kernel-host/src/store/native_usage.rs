//! Subscription facts enter only through authenticated, ordered module.observe.
use super::{gm, object_scope, operations};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use swarm_contracts::native_usage::{NativeUsageSnapshot, UsageFreshness, UsageService};

pub(super) fn parse(
    state: &Value,
    binding: &Value,
    sequence: Option<i64>,
) -> Result<Option<NativeUsageSnapshot>> {
    let Some(value) = state.get("native_usage").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    if sequence.is_none() {
        return Err(Error::new(
            "USAGE_SEQUENCE_REQUIRED",
            "subscription evidence requires an ordered module observation",
        ));
    }
    let snapshot: NativeUsageSnapshot = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "NATIVE_USAGE_INVALID",
            "subscription evidence does not match its bounded contract",
        )
    })?;
    snapshot
        .validate()
        .map_err(|message| Error::new("NATIVE_USAGE_INVALID", message))?;
    let artifact = model::text(binding, "module_artifact_id")?;
    let matching = match snapshot.service {
        UsageService::Codex => artifact == "codex-rust-controller.1",
        UsageService::Muse => {
            artifact == "muse-sdk-1.3.0-bridge.9"
                && matches!(
                    binding["route"]["runtime"].as_str(),
                    Some("muse" | "module")
                )
        }
    };
    if !matching
        || state
            .get("module_artifact_id")
            .is_some_and(|value| value != artifact)
    {
        return Err(Error::new(
            "NATIVE_USAGE_SOURCE_MISMATCH",
            "subscription source differs from the authenticated artifact",
        ));
    }
    Ok(Some(snapshot))
}

pub(super) fn retain(
    tx: &Transaction<'_>,
    binding: &Value,
    snapshot: &NativeUsageSnapshot,
    observation_id: i64,
) -> Result<()> {
    let id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let previous = &binding["observation"]["native_usage"];
    if previous["boot_id"] == binding["observation"]["bridge_boot_id"]
        && previous["snapshot"]["connection_id"] == snapshot.connection_id
        && let Some(revision) = previous["snapshot"]["evidence"]["revision"].as_u64()
    {
        if snapshot.evidence.revision < revision {
            return Ok(());
        }
        if snapshot.evidence.revision == revision
            && previous["snapshot"] != serde_json::to_value(snapshot)?
        {
            return Err(Error::conflict(
                "usage revision was reused for another subscription fact",
            ));
        }
    }
    let current = json!({"snapshot":snapshot,"observation_id":observation_id,
        "boot_id":binding["observation"]["bridge_boot_id"],"module_artifact_id":binding["module_artifact_id"]});
    // The observation journal retains collection health separately. Identical
    // native evidence does not mint another account fact for each child/poll.
    if previous["boot_id"] == current["boot_id"] && previous["snapshot"] == current["snapshot"] {
        return Ok(());
    }
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.native_usage',json(?3)) WHERE binding_id=?1 AND generation=?2",
        params![id,generation,model::canonical(&current)?])?;
    Ok(())
}

pub(super) fn read(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(value, &["binding_id", "generation"])?;
    let id = model::text(value, "binding_id")?;
    let generation = model::positive(value, "generation")?;
    // Authorize before constructing an account-wide projection.
    match principal.role {
        Role::Operator => super::require_local_operator(db, &principal.client_id)?,
        Role::Manager => {
            let is_gm = gm::read_current(db)?
                .is_some_and(|current| current.client_id == principal.client_id);
            if !is_gm {
                let opening: Option<String> = db.query_row(
                    "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.open' ORDER BY created_at_ms,operation_id LIMIT 1",
                    params![id,generation], |row| row.get(0)).optional()?;
                let granted = opening
                    .as_deref()
                    .map(|opening| object_scope::resolve_operation_read(db, principal, opening))
                    .transpose()?
                    .flatten()
                    .is_some_and(|grant| grant.level >= object_scope::OperationReadLevel::Receipt);
                if !granted {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "subscription evidence requires an exact retained binding relation",
                    ));
                }
            }
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "subscription evidence is restricted to scoped managers and the local operator",
            ));
        }
    }
    let binding = operations::get_binding(db, id, generation)?;
    let retained = &binding["observation"]["native_usage"];
    if retained.is_null() {
        return Ok(json!({"binding_id":id,"generation":generation,"status":"not_observed"}));
    }
    let mut snapshot: NativeUsageSnapshot = serde_json::from_value(retained["snapshot"].clone())
        .map_err(|_| {
            Error::new(
                "NATIVE_USAGE_DAMAGED",
                "retained subscription evidence is invalid",
            )
        })?;
    snapshot
        .validate()
        .map_err(|message| Error::new("NATIVE_USAGE_DAMAGED", message))?;
    if retained["module_artifact_id"] != binding["module_artifact_id"] {
        return Err(Error::new(
            "NATIVE_USAGE_DAMAGED",
            "retained subscription artifact differs",
        ));
    }
    if retained["boot_id"] != binding["observation"]["bridge_boot_id"]
        || binding["observation"]["connection"] != "connected"
        || !binding["released_at_ms"].is_null()
    {
        snapshot.freshness = UsageFreshness::Stale;
    }
    Ok(
        json!({"binding_id":id,"generation":generation,"observation_id":retained["observation_id"],
        "module_artifact_id":binding["module_artifact_id"],"boot_id":retained["boot_id"],"snapshot":snapshot,
        "account_overlap":"unknown_unless_auth_context_proved"}),
    )
}

#[cfg(test)]
mod tests {
    use super::parse;
    use serde_json::{Value, json};
    use swarm_contracts::native_usage::UsageService;

    // This is the current producer-shaped Muse snapshot emitted by
    // modules/muse/native-usage.mjs::nativeUsageSnapshot for a valid usage/read
    // result. Keep one exact snapshot across the runtime/artifact admission cases.
    const MUSE_USAGE_SNAPSHOT: &str = r#"{
        "schema_version": 1,
        "service": "muse",
        "connection_id": "muse-fixture-connection",
        "auth_context_ref": null,
        "collected_at_ms": 1780000000123,
        "freshness": "current",
        "completeness": "full",
        "collection_issue": null,
        "evidence": {
            "method": "usage/read",
            "revision": 4,
            "native_observed_at_ms": 1780000000000
        },
        "ordinary_usage_allowed": null,
        "buckets": [{
            "id": "subscription",
            "name": null,
            "normal_model_slug": null,
            "plan_type": {"value": "max", "collected_at_ms": 1780000000123},
            "primary": {
                "collected_at_ms": 1780000000123,
                "used_percent": 37,
                "resets_at_ms": 1780003600000,
                "window_duration_mins": 60
            },
            "secondary": {
                "collected_at_ms": 1780000000123,
                "used_percent": 12,
                "resets_at_ms": 1780600000000,
                "window_duration_mins": null
            },
            "credits": null,
            "individual_limit": null,
            "spend_control_reached": null,
            "rate_limit_reached_type": null,
            "provider_condition": null
        }]
    }"#;

    fn snapshot() -> Value {
        serde_json::from_str(MUSE_USAGE_SNAPSHOT).expect("Muse usage fixture is valid JSON")
    }

    fn observation(snapshot: Value) -> Value {
        json!({
            "module_artifact_id": "muse-sdk-1.3.0-bridge.9",
            "native_usage": snapshot
        })
    }

    fn binding(runtime: &str, artifact: &str) -> Value {
        json!({
            "module_artifact_id": artifact,
            "route": {"runtime": runtime}
        })
    }

    #[test]
    fn muse_usage_accepts_documented_muse_runtime() {
        let result = parse(
            &observation(snapshot()),
            &binding("muse", "muse-sdk-1.3.0-bridge.9"),
            Some(1),
        )
        .expect("the documented Muse runtime accepts its current artifact");
        let result = result.expect("the ordered observation contains native usage");
        assert_eq!(result.service, UsageService::Muse);
        assert_eq!(result.evidence.method, "usage/read");
        assert_eq!(result.buckets.len(), 1);
    }

    #[test]
    fn muse_usage_accepts_module_runtime() {
        let result = parse(
            &observation(snapshot()),
            &binding("module", "muse-sdk-1.3.0-bridge.9"),
            Some(1),
        )
        .expect("the existing module runtime accepts the same current artifact");
        assert_eq!(
            result.expect("ordered usage is returned").service,
            UsageService::Muse
        );
    }

    #[test]
    fn muse_usage_rejects_unrelated_runtime() {
        let error = parse(
            &observation(snapshot()),
            &binding("codex", "muse-sdk-1.3.0-bridge.9"),
            Some(1),
        )
        .expect_err("the Muse artifact is not admitted on an unrelated runtime");
        assert_eq!(error.code, "NATIVE_USAGE_SOURCE_MISMATCH");
    }

    #[test]
    fn muse_usage_rejects_wrong_artifact() {
        let error = parse(
            &observation(snapshot()),
            &binding("muse", "muse-sdk-1.3.0-bridge.8"),
            Some(1),
        )
        .expect_err("Muse runtime does not admit a different artifact version");
        assert_eq!(error.code, "NATIVE_USAGE_SOURCE_MISMATCH");
    }

    #[test]
    fn muse_usage_requires_observation_sequence() {
        let error = parse(
            &observation(snapshot()),
            &binding("muse", "muse-sdk-1.3.0-bridge.9"),
            None,
        )
        .expect_err("native usage requires an ordered observation");
        assert_eq!(error.code, "USAGE_SEQUENCE_REQUIRED");
    }
}
