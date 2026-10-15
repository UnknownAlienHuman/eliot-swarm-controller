use super::module_handshake;
use super::module_observations::{ObservedNativeFacts, reads_for_manager};
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
    // Retained native observations are only current for an authenticated
    // Manager identity. A local Operator still receives the configuration,
    // descriptor and profile projections with explicit native gaps.
    let observations = match principal.role {
        Role::Manager => reads_for_manager(db, principal, &config.routes)?,
        _ => Vec::new(),
    };
    let observed: BTreeMap<String, ObservedNativeFacts> = observations
        .into_iter()
        .map(|(route, facts)| (route.alias, facts))
        .collect();
    let routes = config
        .routes
        .iter()
        .map(|route| project_route(route, &modules, observed.get(&route.alias), as_of_ms))
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
        mcp_profiles: Evidence::current(
            "validated_controller_mcp_configuration",
            as_of_ms,
            mcp_profiles,
        ),
        gaps: vec![
            "native_model_catalogs_and_aliases_are_not_retained_as_a_cross_route_store_read".to_owned(),
            "native_model_capability_and_runtime_installation_evidence_describes_one_live_binding_not_the_whole_catalog".to_owned(),
            "agent_profile_catalog_is_not_retained_in_current_controller_configuration".to_owned(),
            "account_capacity_grouping_requires_an_observed_account_identity_not_only_a_provider_or_credential_reference".to_owned(),
        ],
    };
    serde_json::to_value(catalog).map_err(Into::into)
}

fn project_route(
    route: &Route,
    modules: &BTreeMap<String, ModuleDescriptorEvidence>,
    observed: Option<&ObservedNativeFacts>,
    as_of_ms: i64,
) -> RuntimeCatalogRoute {
    let (native_values, option_gaps) = safe_native_options(route);
    let configured_model_target = match configured_model_target(route) {
        Some(target) => {
            let mut evidence = Evidence::current("validated_route_configuration", as_of_ms, target);
            evidence.gaps.push(GAP_CONFIGURED_TARGET.to_owned());
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
    let module_descriptor = modules
        .get(&route.alias)
        .cloned()
        .unwrap_or_else(default_module_descriptor);
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
    let default_observed;
    let observed = match observed {
        Some(facts) => facts,
        None => {
            default_observed = ObservedNativeFacts {
                gaps: vec![GAP_EVIDENCE_MANAGER_ONLY.to_owned()],
                ..ObservedNativeFacts::default()
            };
            &default_observed
        }
    };
    let native_models = observed_models(observed);
    let native_capabilities = observed_capabilities(observed);
    let account_capacity_group = observed_account_group(observed);
    let installation = installation_evidence(observed, as_of_ms);
    let mut gaps = vec![
        GAP_INSTALLATION_NOT_PROVED.to_owned(),
        GAP_MODEL_CATALOG_NOT_RETAINED.to_owned(),
        GAP_CAPABILITY_ONE_BINDING.to_owned(),
        GAP_ACCOUNT_NOT_PROVED.to_owned(),
    ];
    gaps.extend(module_descriptor.gaps.iter().cloned());
    if !module_descriptor.contract_capabilities.is_empty() {
        gaps.push(GAP_DESCRIPTOR_DECLARED_NOT_NATIVE.to_owned());
    }
    gaps.extend(option_gaps.iter().cloned());
    gaps.extend(observed.gaps.iter().cloned());
    gaps.extend(native_models.gaps.iter().cloned());
    gaps.extend(native_capabilities.gaps.iter().cloned());
    gaps.extend(account_capacity_group.gaps.iter().cloned());
    gaps.extend(installation.gaps.iter().cloned());
    RuntimeCatalogRoute {
        alias: route.alias.clone(),
        runtime: route.runtime.clone(),
        artifact_id: route.module_artifact_id.clone(),
        enabled: route.enabled,
        configured_model_target,
        configured_native_options,
        module_descriptor,
        installation,
        native_models,
        native_capabilities,
        trust,
        account_capacity_group,
        freshness: vec![
            "route_configuration_current_at_query_time".to_owned(),
            "module_descriptor_status_uses_current_store_registry_revision".to_owned(),
            "native_evidence_is_current_while_its_live_binding_matches_the_retained_observation"
                .to_owned(),
        ],
        gaps,
    }
}

const GAP_CONFIGURED_TARGET: &str =
    "configured_target_does_not_prove_native_catalog_membership_or_availability";
const GAP_EVIDENCE_MANAGER_ONLY: &str =
    "retained_native_evidence_is_only_available_to_an_authenticated_manager";
const GAP_INSTALLATION_NOT_PROVED: &str = "installation_and_live_runtime_qualification_are_not_proved_by_route_configuration_or_descriptor_registration";
const GAP_MODEL_CATALOG_NOT_RETAINED: &str =
    "native_model_catalog_and_alias_inventory_is_not_retained_for_this_route";
const GAP_CAPABILITY_ONE_BINDING: &str =
    "native_model_capability_evidence_describes_one_live_binding_not_the_whole_catalog";
const GAP_ACCOUNT_NOT_PROVED: &str =
    "provider_and_configured_model_identity_do_not_prove_an_account_capacity_group";
const GAP_DESCRIPTOR_DECLARED_NOT_NATIVE: &str =
    "descriptor_capabilities_are_declared_contract_metadata_not_native_observations";

fn default_module_descriptor() -> ModuleDescriptorEvidence {
    ModuleDescriptorEvidence {
        status: "unknown".to_owned(),
        module_id: None,
        artifact_id: None,
        registered_revision: None,
        selected_revision: None,
        contract_capabilities: Vec::new(),
        gaps: vec!["module_descriptor_readback_was_not_available".to_owned()],
    }
}

/// Surface the model the live binding actually observed, never the model a
/// route merely configures.
fn observed_models(observed: &ObservedNativeFacts) -> Evidence<Vec<Value>> {
    match observed.observed_model.as_ref() {
        Some(model) => {
            let Some(observed_at_ms) = observed.observed_at_ms.filter(|value| *value > 0) else {
                return Evidence::unknown(
                    "native_model_observation_timestamp_is_not_retained_for_this_route",
                );
            };
            let mut evidence = Evidence::current_with_coverage(
                "retained_binding_native_describe_observation",
                observed_at_ms,
                "partial",
                vec![Value::String(model.clone())],
            );
            match observed.requested_model.as_ref() {
                Some(requested) if requested == model => {}
                Some(_) => evidence
                    .gaps
                    .push("requested_model_differs_from_the_retained_observed_model".to_owned()),
                None => evidence
                    .gaps
                    .push("no_native_requested_model_is_retained_for_this_route".to_owned()),
            }
            evidence
        }
        None => Evidence::unknown(
            "no_current_retained_native_model_readback_is_available_for_this_route",
        ),
    }
}

/// Surface the capability names the live binding actually reported.
fn observed_capabilities(observed: &ObservedNativeFacts) -> Evidence<Vec<Value>> {
    if !observed.supported_capabilities_observed && !observed.unsupported_capabilities_observed {
        return Evidence::unknown(
            "no_current_retained_native_capability_list_is_available_for_this_route",
        );
    }
    let Some(observed_at_ms) = observed.observed_at_ms.filter(|value| *value > 0) else {
        return Evidence::unknown(
            "native_capability_observation_timestamp_is_not_retained_for_this_route",
        );
    };
    let mut values = Vec::new();
    values.extend(
        observed
            .supported_capabilities
            .iter()
            .map(|name| json!({"name": name, "status": "supported"})),
    );
    values.extend(
        observed
            .unsupported_capabilities
            .iter()
            .map(|name| json!({"name": name, "status": "unsupported"})),
    );
    let mut evidence = Evidence::current_with_coverage(
        "retained_binding_native_describe_observation",
        observed_at_ms,
        "partial",
        values,
    );
    if !observed.supported_capabilities_observed {
        evidence
            .gaps
            .push("native_supported_capability_list_is_not_retained".to_owned());
    }
    if !observed.unsupported_capabilities_observed {
        evidence
            .gaps
            .push("native_unsupported_capability_list_is_not_retained".to_owned());
    }
    evidence
}

/// Account capacity grouping needs an observed account identity. The retained
/// provider-condition rows carry an opaque auth reference, not an account
/// identity, so grouping stays an explicit gap.
fn observed_account_group(observed: &ObservedNativeFacts) -> Evidence<String> {
    let _ = observed;
    Evidence::unknown("no_observed_account_identity_is_retained_for_this_route")
}

/// A current live binding proves the configured artifact admitted on this route,
/// not that any binary is installed. Installation stays an explicit gap.
fn installation_evidence(observed: &ObservedNativeFacts, as_of_ms: i64) -> Evidence<String> {
    if observed.binding_id.is_none() {
        return Evidence::unknown(
            "no_live_binding_is_retained_for_this_route_so_installation_is_unobserved",
        );
    }
    let _ = as_of_ms;
    Evidence::unknown("a_live_binding_does_not_prove_a_native_binary_or_installed_descriptor")
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
    use super::{ObservedNativeFacts, project_route, read, safe_native_options};
    use crate::{
        config::{Config, Route},
        model::{Principal, Role},
    };
    use rusqlite::{Connection, params};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use swarm_contracts::mcp_frontend::McpToolProfile;
    use swarm_contracts::runtime_catalog::{
        EvidenceStatus, ModuleDescriptorEvidence, RuntimeCatalogRoute,
    };

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

    fn modules(status: &str) -> BTreeMap<String, ModuleDescriptorEvidence> {
        BTreeMap::from([(
            "catalog-route".to_owned(),
            ModuleDescriptorEvidence {
                status: status.to_owned(),
                module_id: None,
                artifact_id: None,
                registered_revision: None,
                selected_revision: None,
                contract_capabilities: Vec::new(),
                gaps: Vec::new(),
            },
        )])
    }

    fn observed(with_model: bool) -> ObservedNativeFacts {
        ObservedNativeFacts {
            binding_id: Some("binding-1".to_owned()),
            binding_generation: Some(1),
            module_artifact_id: Some("eliot-codex.rust-headless.1".to_owned()),
            module_selector: None,
            observed_at_ms: Some(1_000),
            native_sequence: Some(7),
            native_observation_id: Some(42),
            runtime: Some("codex".to_owned()),
            requested_model: if with_model {
                Some("gpt-5.6-luna".to_owned())
            } else {
                None
            },
            observed_model: if with_model {
                Some("gpt-5.6-luna".to_owned())
            } else {
                None
            },
            model_provider: if with_model {
                Some("openai".to_owned())
            } else {
                None
            },
            model_status: if with_model {
                Some("unknown".to_owned())
            } else {
                None
            },
            model_definition: if with_model {
                Some(json!({"kind":"opencode_model_settings_revision.v1"}))
            } else {
                None
            },
            supported_capabilities: if with_model {
                vec!["agent.open".to_owned(), "task.dispatch".to_owned()]
            } else {
                Vec::new()
            },
            unsupported_capabilities: if with_model {
                vec!["agent.refresh".to_owned()]
            } else {
                Vec::new()
            },
            supported_capabilities_observed: with_model,
            unsupported_capabilities_observed: with_model,
            gaps: Vec::new(),
        }
    }

    #[test]
    fn a_current_live_observation_surfaces_the_observed_model_and_capabilities() {
        let configured = route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai","apiKey":"secret-value"}),
        );
        let projected: RuntimeCatalogRoute = project_route(
            &configured,
            &modules("built_in_route"),
            Some(&observed(true)),
            100,
        );
        assert_eq!(projected.native_models.status, EvidenceStatus::Current);
        let models = projected.native_models.value.unwrap();
        assert_eq!(models[0], json!("gpt-5.6-luna"));
        assert_eq!(
            projected.native_models.source.as_deref(),
            Some("retained_binding_native_describe_observation")
        );
        assert_eq!(projected.native_models.observed_at_ms, Some(1_000));
        assert_eq!(projected.native_models.coverage.as_deref(), Some("partial"));
        assert_eq!(
            projected.native_capabilities.status,
            EvidenceStatus::Current
        );
        let capabilities = projected.native_capabilities.value.unwrap();
        assert_eq!(capabilities.len(), 3);
        assert!(
            capabilities.iter().any(|value| {
                value["name"] == "task.dispatch" && value["status"] == "supported"
            })
        );
        assert!(
            capabilities.iter().any(|value| {
                value["name"] == "agent.refresh" && value["status"] == "unsupported"
            })
        );
        assert!(!projected.gaps.iter().any(|gap| gap.is_empty()));
    }

    #[test]
    fn a_configured_model_without_a_live_observation_is_a_declared_gap() {
        let configured = route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        );
        let projected: RuntimeCatalogRoute =
            project_route(&configured, &modules("built_in_route"), None, 100);
        assert_eq!(projected.native_models.status, EvidenceStatus::Unknown);
        assert_eq!(projected.native_models.value, None);
        assert_eq!(
            projected.native_capabilities.status,
            EvidenceStatus::Unknown
        );
        assert_eq!(projected.installation.status, EvidenceStatus::Unknown);
        assert_eq!(
            projected.account_capacity_group.status,
            EvidenceStatus::Unknown
        );
        assert!(
            projected
                .gaps
                .iter()
                .any(|gap| gap.contains("only_available_to_an_authenticated_manager"))
        );
        assert!(
            !projected
                .gaps
                .iter()
                .any(|gap| gap.contains("route_configuration_current"))
        );
    }

    #[test]
    fn a_requested_model_that_differs_from_the_observed_model_is_declared() {
        let mut observed = observed(true);
        observed.requested_model = Some("gpt-5.6-luna".to_owned());
        observed.observed_model = Some("gpt-5.6-luna-ultra".to_owned());
        let configured = route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        );
        let projected: RuntimeCatalogRoute = project_route(
            &configured,
            &modules("built_in_route"),
            Some(&observed),
            100,
        );
        assert_eq!(projected.native_models.status, EvidenceStatus::Current);
        assert!(
            projected
                .native_models
                .gaps
                .iter()
                .any(|gap| gap.contains("requested_model_differs"))
        );
    }

    #[test]
    fn unsupported_only_capability_observation_keeps_its_own_current_status() {
        let mut observed = observed(true);
        observed.supported_capabilities.clear();
        observed.supported_capabilities_observed = false;
        let configured = route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        );
        let projected = project_route(
            &configured,
            &modules("built_in_route"),
            Some(&observed),
            100,
        );
        assert_eq!(
            projected.native_capabilities.status,
            EvidenceStatus::Current
        );
        assert_eq!(
            projected.native_capabilities.value.as_ref().unwrap()[0],
            json!({"name":"agent.refresh","status":"unsupported"})
        );
        assert!(
            projected
                .native_capabilities
                .gaps
                .iter()
                .any(|gap| gap.contains("supported_capability_list_is_not_retained"))
        );
    }

    #[test]
    fn native_evidence_without_an_observation_timestamp_is_not_current_at_zero() {
        let mut observed = observed(true);
        observed.observed_at_ms = None;
        let configured = route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        );
        let projected = project_route(
            &configured,
            &modules("built_in_route"),
            Some(&observed),
            100,
        );
        assert_eq!(projected.native_models.status, EvidenceStatus::Unknown);
        assert_eq!(projected.native_models.observed_at_ms, None);
        assert_eq!(
            projected.native_capabilities.status,
            EvidenceStatus::Unknown
        );
        assert_eq!(projected.native_capabilities.observed_at_ms, None);
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

        let projected = project_route(&configured, &modules("built_in_route"), None, 100);
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
        db.execute_batch(super::super::SCHEMA).unwrap();
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
    #[test]
    fn retained_binding_native_observation_is_current_only_for_matching_live_binding() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        let mut config = Config::default();
        config.routes.push(route(
            "codex",
            "eliot-codex.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        ));
        let manager = Principal {
            link_id: "catalog-manager-link".to_owned(),
            client_id: "catalog-manager".to_owned(),
            role: Role::Manager,
        };
        let native = json!({
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
        });
        let state = json!({
            "bridge_boot_id":"boot-1",
            "module_client_id":"codex-module",
            "module_link_id":"codex-link-1",
            "native": native,
            "observed_at_ms": 1000,
            "native_sequence": 7,
            "native_observation_id": 42,
            "effective_model": {
                "kind":"opencode_session_model_v1",
                "revision":"rev-1",
                "model":{"id":"model-id","providerID":"provider","variant":"variant"},
                "enabled": true,
                "status":"active",
                "complete": true
            }
        });
        let route_json = crate::model::canonical(&json!({"alias":"catalog-route"})).unwrap();
        let state_json = crate::model::canonical(&state).unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task-1','project-1',1,'open','{}',1000,1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) VALUES('binding-1',1,'lane-1','instance-1','eliot-codex.rust-headless.1','ready',NULL,NULL,?1,?2,1000)",
            params![route_json, state_json],
        ).unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) VALUES('attempt-1','task-1',1,'{}','catalog-manager','controller','binding-1',1,'running','[]',1000,1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO observations(observation_id,source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) VALUES(42,'module:codex-module','boot-1:7','binding-1',1,'runtime.state',?1,1000)",
            params![crate::model::canonical(&native).unwrap()],
        ).unwrap();
        let current = read(&db, &manager, &json!({}), &config).unwrap();
        assert_eq!(current["routes"][0]["native_models"]["status"], "current");
        assert_eq!(
            current["routes"][0]["native_models"]["value"][0],
            "gpt-5.6-luna"
        );
        assert_eq!(
            current["routes"][0]["native_models"]["observed_at_ms"],
            1000
        );
        assert_eq!(
            current["routes"][0]["native_capabilities"]["status"],
            "current"
        );
        assert!(
            current["routes"][0]["native_capabilities"]["value"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["name"] == "task.dispatch" && v["status"] == "supported")
        );
        db.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id','boot-2') WHERE binding_id='binding-1'", []).unwrap();
        let stale = read(&db, &manager, &json!({}), &config).unwrap();
        assert_eq!(stale["routes"][0]["native_models"]["status"], "unknown");
        assert!(stale["routes"][0]["native_models"]["value"].is_null());
        db.execute(
            "UPDATE bindings SET state='closed',released_at_ms=2000 WHERE binding_id='binding-1'",
            [],
        )
        .unwrap();
        let released = read(&db, &manager, &json!({}), &config).unwrap();
        assert_eq!(released["routes"][0]["native_models"]["status"], "unknown");
        assert!(released["routes"][0]["native_models"]["value"].is_null());
        db.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id','boot-1') WHERE binding_id='binding-1'", []).unwrap();
        let mut foreign_config = Config::default();
        foreign_config.routes.push(route(
            "codex",
            "eliot-other.rust-headless.1",
            json!({"model":"gpt-5.6-luna","modelProvider":"openai"}),
        ));
        let foreign = read(&db, &manager, &json!({}), &foreign_config).unwrap();
        assert_eq!(foreign["routes"][0]["native_models"]["status"], "unknown");
        assert!(
            foreign["routes"][0]["gaps"]
                .as_array()
                .unwrap()
                .iter()
                .any(|g| g
                    .as_str()
                    .unwrap()
                    .contains("does_not_match_configured_route")
                    || g.as_str().unwrap().contains("no_live_binding"))
        );
    }

    #[test]
    fn public_catalog_scopes_two_managers_on_one_alias_before_foreign_corrupt_json() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
             CREATE TABLE tasks(
                 task_id TEXT PRIMARY KEY,
                 revision INTEGER NOT NULL,
                 state TEXT NOT NULL
             );
             CREATE TABLE attempts(
                 attempt_id TEXT PRIMARY KEY,
                 task_id TEXT NOT NULL,
                 task_revision INTEGER NOT NULL,
                 owner_id TEXT NOT NULL,
                 binding_id TEXT NOT NULL,
                 binding_generation INTEGER NOT NULL,
                 released_at_ms INTEGER
             );
             CREATE TABLE bindings(
                 binding_id TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 module_artifact_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 route_json TEXT NOT NULL,
                 state_json TEXT NOT NULL,
                 released_at_ms INTEGER
             );",
        )
        .unwrap();
        let mut config = Config::default();
        config.routes.push(route(
            "codex",
            "shared-artifact",
            json!({"model":"configured-model"}),
        ));
        let route_json = crate::model::canonical(&json!({"alias":"catalog-route"})).unwrap();
        let state = |model: &str, boot: &str, sequence: i64| {
            crate::model::canonical(&json!({
                "bridge_boot_id":boot,
                "native":{
                    "boot_id":boot,
                    "describe":{
                        "requested_model":model,
                        "effective_model":model,
                        "effective_model_provider":"openai",
                        "served_model_status":"observed"
                    },
                    "capabilities":{
                        "agent.open":"implemented",
                        "agent.refresh":"unavailable"
                    }
                },
                "observed_at_ms":1000 + sequence,
                "native_sequence":sequence,
                "native_observation_id":sequence
            }))
            .unwrap()
        };
        db.execute_batch(
            "INSERT INTO tasks(task_id,revision,state)
                 VALUES('task-a',1,'open'),
                       ('task-b',1,'open'),
                       ('task-corrupt',1,'open'),
                       ('task-stale',2,'open');
             INSERT INTO attempts(attempt_id,task_id,task_revision,owner_id,binding_id,binding_generation,released_at_ms)
                 VALUES('attempt-a','task-a',1,'manager-a','binding-a',1,NULL),
                       ('attempt-b','task-b',1,'manager-b','binding-b',1,NULL),
                       ('attempt-corrupt','task-corrupt',1,'manager-corrupt','binding-corrupt',1,NULL),
                       ('attempt-stale','task-stale',1,'manager-a','binding-stale',1,NULL);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,module_artifact_id,state,route_json,state_json,released_at_ms) VALUES(?1,1,'shared-artifact','ready',?2,?3,NULL)",
            params!["binding-a", route_json.clone(), state("model-a", "boot-a", 7)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,module_artifact_id,state,route_json,state_json,released_at_ms) VALUES(?1,1,'shared-artifact','ready',?2,?3,NULL)",
            params!["binding-b", route_json.clone(), state("model-b", "boot-b", 8)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,module_artifact_id,state,route_json,state_json,released_at_ms) VALUES(?1,1,'shared-artifact','ready',?2,?3,NULL)",
            params!["binding-stale", route_json.clone(), state("model-stale", "boot-stale", 9)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,module_artifact_id,state,route_json,state_json,released_at_ms) VALUES('binding-corrupt',1,'shared-artifact','ready',?1,'{foreign-corrupt-json',NULL)",
            params![route_json],
        )
        .unwrap();

        let manager_a = Principal {
            link_id: "catalog-manager-a-link".to_owned(),
            client_id: "manager-a".to_owned(),
            role: Role::Manager,
        };
        let manager_b = Principal {
            link_id: "catalog-manager-b-link".to_owned(),
            client_id: "manager-b".to_owned(),
            role: Role::Manager,
        };
        let result_a = read(&db, &manager_a, &json!({}), &config).unwrap();
        let result_b = read(&db, &manager_b, &json!({}), &config).unwrap();
        assert_eq!(
            result_a["routes"][0]["native_models"]["value"][0],
            "model-a"
        );
        assert_eq!(
            result_a["routes"][0]["native_models"]["value"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            result_b["routes"][0]["native_models"]["value"][0],
            "model-b"
        );
        assert_ne!(
            result_a["routes"][0]["native_models"]["value"],
            result_b["routes"][0]["native_models"]["value"]
        );
    }
}
