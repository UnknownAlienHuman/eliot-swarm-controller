//! Bounded, read-only implementation of the documented current runtime catalog.

use super::module_handshake;
use crate::{
    config::{Config, Route},
    error::{Error, Result},
    model::{Principal, Role},
};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use swarm_contracts::runtime_catalog::{
    ConfiguredModelTarget, ConfiguredNativeOptions, Evidence, ModuleDescriptorEvidence,
    RuntimeCatalog, RuntimeCatalogRoute, RuntimeMcpProfile,
};

pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    params: &Value,
    config: &Config,
) -> Result<Value> {
    crate::model::fields(params, &[])?;
    match principal.role {
        Role::Manager => {}
        Role::Operator => super::require_local_operator(db, &principal.client_id)?,
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "runtime catalog is restricted to Manager and local Operator identities",
            ));
        }
    }

    let as_of_ms = crate::model::now_ms()?;
    let (module_registry_revision, modules) =
        module_handshake::runtime_catalog_module_evidence(principal, &config.routes, db)?;
    let routes = config
        .routes
        .iter()
        .map(|route| project_route(route, &modules, as_of_ms))
        .collect();
    let mcp_profiles = config
        .mcp
        .profiles
        .iter()
        .map(|(name, profile)| {
            let surface = swarm_contracts::mcp_catalog::Surface::configured(
                profile.tool_profile,
                profile.surface.as_deref(),
                &profile.deferred_groups,
                &profile.manual_tools,
            )
            .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))?;
            let suggested = surface.suggested();
            Ok(RuntimeMcpProfile {
                name: name.clone(),
                tool_profile: serde_json::to_value(profile.tool_profile)?
                    .as_str()
                    .unwrap_or("unknown")
                    .to_owned(),
                effective_surface: suggested.core.to_owned(),
                surface_id: suggested.id,
                configured_surface: profile.surface.clone(),
                deferred_groups: profile.deferred_groups.clone(),
                manual_tools: profile.manual_tools.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let catalog = RuntimeCatalog {
        schema_version: 1,
        as_of_ms,
        module_registry_revision,
        routes,
        agent_profiles: Evidence::unknown(
            "no_current_store_owned_agent_profile_catalog_is_retained",
        ),
        mcp_profiles: Evidence::current("validated_controller_mcp_configuration", as_of_ms, mcp_profiles),
        gaps: vec![
            "native_model_catalogs_and_aliases_are_not_retained_as_a_cross_route_store_read".to_owned(),
            "native_model_capability_and_runtime_installation_evidence_is_not_available_without_a_live_probe".to_owned(),
            "agent_profile_catalog_is_not_retained_in_current_controller_configuration".to_owned(),
            "account_capacity_grouping_requires_an_observed_account_identity_not_only_a_provider_or_credential_reference".to_owned(),
        ],
    };
    serde_json::to_value(catalog).map_err(Into::into)
}

fn project_route(
    route: &Route,
    modules: &BTreeMap<String, ModuleDescriptorEvidence>,
    as_of_ms: i64,
) -> RuntimeCatalogRoute {
    let (native_values, option_gaps) = safe_native_options(route);
    let configured_model_target = match configured_model_target(route) {
        Some(target) => {
            let mut evidence = Evidence::current("validated_route_configuration", as_of_ms, target);
            evidence.gaps.push(
                "configured_target_does_not_prove_native_catalog_membership_or_availability"
                    .to_owned(),
            );
            evidence
        }
        None => Evidence::unknown("no_recognized_configured_model_target_for_route"),
    };
    let configured_native_options = Evidence::current(
        "validated_route_configuration_safe_projection",
        as_of_ms,
        ConfiguredNativeOptions {
            values: Value::Object(native_values),
            gaps: option_gaps.clone(),
        },
    );
    let module_descriptor =
        modules
            .get(&route.alias)
            .cloned()
            .unwrap_or_else(|| ModuleDescriptorEvidence {
                status: "unknown".to_owned(),
                module_id: None,
                artifact_id: None,
                registered_revision: None,
                selected_revision: None,
                contract_capabilities: Vec::new(),
                gaps: vec!["module_descriptor_readback_was_not_available".to_owned()],
            });
    let trust = match module_descriptor.status.as_str() {
        "registered_and_selected" | "registered_but_not_selected" => Evidence::current(
            "store_trusted_module_descriptor_registry",
            as_of_ms,
            "registered_module_contract_only".to_owned(),
        ),
        "built_in_route" => Evidence::current(
            "controller_builtin_route_identity",
            as_of_ms,
            "controller_builtin_contract_only".to_owned(),
        ),
        _ => Evidence::unknown("route_trust_is_not_proved_by_configuration_alone"),
    };
    let mut gaps = vec![
        "installation_and_live_runtime_qualification_are_not_proved_by_route_configuration_or_descriptor_registration".to_owned(),
        "native_model_catalog_and_alias_inventory_is_not_retained_for_this_route".to_owned(),
        "native_model_capability_evidence_is_not_retained_for_this_route".to_owned(),
        "provider_and_configured_model_identity_do_not_prove_an_account_capacity_group".to_owned(),
    ];
    gaps.extend(module_descriptor.gaps.iter().cloned());
    gaps.extend(option_gaps.iter().cloned());
    RuntimeCatalogRoute {
        alias: route.alias.clone(),
        runtime: route.runtime.clone(),
        artifact_id: route.module_artifact_id.clone(),
        enabled: route.enabled,
        configured_model_target,
        configured_native_options,
        module_descriptor,
        installation: Evidence::unknown(
            "configured_route_and_registered_descriptor_do_not_prove_native_binary_installation",
        ),
        native_models: Evidence::unknown(
            "no_retained_route_scoped_native_model_catalog_readback_is_available",
        ),
        native_capabilities: Evidence::unknown(
            "no_retained_route_scoped_native_model_capability_readback_is_available",
        ),
        trust,
        account_capacity_group: Evidence::unknown(
            "no_actual_configured_account_identity_is_retained_for_this_route",
        ),
        freshness: vec![
            "route_configuration_current_at_query_time".to_owned(),
            "module_descriptor_status_uses_current_store_registry_revision".to_owned(),
            "native_model_and_capability_freshness_unknown_without_retained_observation".to_owned(),
        ],
        gaps,
    }
}

fn safe_native_options(route: &Route) -> (Map<String, Value>, Vec<String>) {
    let mut values = Map::new();
    if let Some(owned) = &route.owned_service {
        let gaps = vec![
            "workspace_paths_credentials_environment_and_unrecognized_native_options_are_withheld"
                .to_owned(),
        ];
        values.insert(
            "model".to_owned(),
            json!({
                "id":owned.model.id,
                "providerID":owned.model.provider_id,
                "variant":owned.model.variant,
            }),
        );
        return (values, gaps);
    }
    let Some(options) = route.native_options.as_object() else {
        return (
            values,
            vec!["native_options_are_not_a_projectable_object".to_owned()],
        );
    };
    let mut gaps = vec![
        "workspace_paths_credentials_environment_and_unrecognized_native_options_are_withheld"
            .to_owned(),
    ];
    match route.runtime.as_str() {
        "codex" => {
            copy_text_option(options, "modelProvider", "modelProvider", &mut values);
            copy_text_option(options, "model", "model", &mut values);
        }
        runtime if runtime == crate::runtime::opencode_v2::RUNTIME => {
            if let Some(model) = options.get("model").and_then(Value::as_object) {
                let mut safe_model = Map::new();
                for key in ["id", "providerID", "variant"] {
                    copy_text_option(model, key, key, &mut safe_model);
                }
                if !safe_model.is_empty() {
                    values.insert("model".to_owned(), Value::Object(safe_model));
                }
            }
        }
        "command" => copy_text_option(options, "modelId", "modelId", &mut values),
        "antigravity" => {
            for key in ["modelId", "reasoningEffort", "agent"] {
                copy_text_option(options, key, key, &mut values);
            }
            if let Some(value) = options.get("dangerouslySkipPermissions")
                && value.is_boolean()
            {
                values.insert("dangerouslySkipPermissions".to_owned(), value.clone());
            }
        }
        _ => gaps.push("native_option_value_projection_is_not_defined_for_this_runtime".to_owned()),
    }
    (values, gaps)
}

fn copy_text_option(
    source: &Map<String, Value>,
    source_name: &str,
    output_name: &str,
    target: &mut Map<String, Value>,
) {
    if let Some(value) = source
        .get(source_name)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        })
    {
        target.insert(output_name.to_owned(), Value::String(value.to_owned()));
    }
}

fn configured_model_target(route: &Route) -> Option<ConfiguredModelTarget> {
    if let Some(owned) = &route.owned_service {
        return Some(ConfiguredModelTarget {
            id: Some(owned.model.id.clone()),
            provider_id: Some(owned.model.provider_id.clone()),
            variant: Some(owned.model.variant.clone()),
        });
    }
    let options = route.native_options.as_object()?;
    let model = match route.runtime.as_str() {
        "codex" => options.get("model")?.as_str()?.to_owned(),
        runtime if runtime == crate::runtime::opencode_v2::RUNTIME => {
            let model = options.get("model")?.as_object()?;
            let id = model.get("id")?.as_str()?.to_owned();
            return Some(ConfiguredModelTarget {
                id: Some(id),
                provider_id: model.get("providerID")?.as_str().map(str::to_owned),
                variant: model.get("variant")?.as_str().map(str::to_owned),
            });
        }
        "command" | "antigravity" => options.get("modelId")?.as_str()?.to_owned(),
        _ => return None,
    };
    let provider_id = (route.runtime == "codex")
        .then(|| options.get("modelProvider")?.as_str().map(str::to_owned))
        .flatten();
    Some(ConfiguredModelTarget {
        id: Some(model),
        provider_id,
        variant: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{project_route, read, safe_native_options};
    use crate::{
        config::{Config, Route},
        model::{Principal, Role},
    };
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use swarm_contracts::mcp_frontend::McpToolProfile;
    use swarm_contracts::runtime_catalog::{EvidenceStatus, ModuleDescriptorEvidence};

    fn route(runtime: &str, artifact: &str, native_options: Value) -> Route {
        Route {
            alias: "catalog-route".to_owned(),
            runtime: runtime.to_owned(),
            module_artifact_id: artifact.to_owned(),
            enabled: true,
            native_options,
            workspace_option: None,
            owned_service: None,
            admission_policy: None,
        }
    }

    #[test]
    fn current_configured_model_does_not_claim_native_catalog_membership_or_leak_paths() {
        let configured = route(
            "command",
            "eliot-command.rust-headless.1",
            json!({"modelId":"actual-configured-model","workspaceRoot":"C:/private/work"}),
        );
        let (safe, gaps) = safe_native_options(&configured);
        assert_eq!(
            Value::Object(safe.clone()),
            json!({"modelId":"actual-configured-model"})
        );
        assert!(
            !serde_json::to_string(&safe)
                .unwrap()
                .contains("private/work")
        );
        assert!(!gaps.is_empty());

        let module = ModuleDescriptorEvidence {
            status: "built_in_route".to_owned(),
            module_id: None,
            artifact_id: None,
            registered_revision: None,
            selected_revision: None,
            contract_capabilities: Vec::new(),
            gaps: Vec::new(),
        };
        let projected = project_route(
            &configured,
            &BTreeMap::from([("catalog-route".to_owned(), module)]),
            100,
        );
        assert_eq!(
            projected.configured_model_target.status,
            EvidenceStatus::Current
        );
        assert_eq!(
            projected
                .configured_model_target
                .value
                .as_ref()
                .unwrap()
                .id
                .as_deref(),
            Some("actual-configured-model")
        );
        assert_eq!(projected.native_models.status, EvidenceStatus::Unknown);
        assert_eq!(projected.native_models.value, None);
        assert_eq!(
            projected.native_capabilities.status,
            EvidenceStatus::Unknown
        );
        assert_eq!(projected.installation.status, EvidenceStatus::Unknown);
        assert!(
            projected
                .configured_model_target
                .gaps
                .iter()
                .any(|gap| gap.contains("does_not_prove_native_catalog"))
        );
    }

    #[test]
    fn unsupported_route_options_are_not_echoed_as_native_evidence() {
        let configured = route(
            "unrecognized-runtime",
            "unrecognized-artifact",
            json!({"api_key":"secret-value","endpoint":"private-url","custom":"value"}),
        );
        let (safe, gaps) = safe_native_options(&configured);
        assert!(safe.is_empty());
        assert!(
            !serde_json::to_string(&safe)
                .unwrap()
                .contains("secret-value")
        );
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("projection_is_not_defined"))
        );
    }

    #[test]
    fn runtime_catalog_is_manager_scoped_and_keeps_missing_native_facts_unknown() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let mut config = Config::default();
        config.routes.push(route(
            "command",
            "eliot-command.rust-headless.1",
            json!({"modelId":"configured-model","workspaceRoot":"C:/private"}),
        ));
        let manager = Principal {
            link_id: "catalog-manager-link".to_owned(),
            client_id: "catalog-manager".to_owned(),
            role: Role::Manager,
        };
        let result = read(&db, &manager, &json!({}), &config).unwrap();
        assert!(swarm_contracts::mcp_catalog::exposes_method(
            McpToolProfile::Manager,
            "runtime.catalog"
        ));
        assert!(swarm_contracts::mcp_catalog::exposes_method(
            McpToolProfile::Gm,
            "runtime.catalog"
        ));
        assert!(!swarm_contracts::mcp_catalog::exposes_method(
            McpToolProfile::Observer,
            "runtime.catalog"
        ));
        assert!(!swarm_contracts::mcp_catalog::exposes_method(
            McpToolProfile::Participant,
            "runtime.catalog"
        ));
        assert_eq!(result["routes"][0]["alias"], "catalog-route");
        assert_eq!(result["routes"][0]["native_models"]["status"], "unknown");
        assert!(result["routes"][0]["native_models"]["value"].is_null());
        assert_eq!(
            result["routes"][0]["native_capabilities"]["status"],
            "unknown"
        );
        assert_eq!(result["agent_profiles"]["status"], "unknown");
        let encoded = serde_json::to_string(&result).unwrap();
        assert!(!encoded.contains("C:/private"));
        assert!(!encoded.contains("expected_client_id"));

        let observer = Principal {
            link_id: "catalog-observer-link".to_owned(),
            client_id: "catalog-observer".to_owned(),
            role: Role::Observer,
        };
        assert_eq!(
            read(&db, &observer, &json!({}), &config).unwrap_err().code,
            "FORBIDDEN"
        );
        assert_eq!(
            read(&db, &manager, &json!({"task_id":"invented-task"}), &config)
                .unwrap_err()
                .code,
            "INVALID_PARAMS"
        );
    }
}
