//! Opencode native adapter capability declaration.

use eliot_core::McpPolicy;
use eliot_core::api::harness::{
    AdhocEffort, AdhocLaunchDefaults, AdhocModel, AdhocPermissions, ModelReaderOptions,
    NativeAuthCapability, NativeAuthCheck, NativeAuthCommand, NativeAuthInvocation,
    NativeAuthState, NativeQuotaReadback, NativeRoute, ProfileCapabilities,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        catalogue_provider: eliot_core::api::harness::CatalogueProvider::ModelPrefix,
        instruction_inventory: crate::instructions::INSTRUCTION_INVENTORY,
        model_login_source: Some(eliot_core::api::harness::ModelLoginSource::Files {
            root_env: "XDG_DATA_HOME",
            default_root: ".local/share",
            files: &["opencode/auth.json"],
        }),
        model_catalogue_presentation: Some(
            crate::catalog_presentation::model_catalogue_presentation,
        ),
        auth_inventory: Some(eliot_core::api::harness::NativeAuthInventory {
            arguments: &crate::native::auth::LIST_ARGUMENTS,
            decode_ids: decode_auth_ids,
        }),
        native_auth: Some(NativeAuthCapability {
            login: Some(NativeAuthCommand {
                resolve: || auth_command(&["auth", "login", "--standalone"]),
            }),
            signed_in_check: Some(NativeAuthCheck {
                command: NativeAuthCommand {
                    resolve: || auth_command(&crate::native::auth::list_argv()),
                },
                classify: classify_auth_status,
            }),
        }),
        native_route: NativeRoute::AuthIntegration,
        profile: ProfileCapabilities {
            named_profile: Some(crate::profile_config::MAPPING),
            launch_model_scope: eliot_core::api::harness::LaunchModelScope::Check(
                crate::profile_config::launch_model_stays_on_route,
            ),
            environment: eliot_core::api::harness::ProfileEnvironment {
                store_paths: &[
                    "XDG_CONFIG_HOME",
                    "XDG_DATA_HOME",
                    "XDG_STATE_HOME",
                    "XDG_CACHE_HOME",
                    "OPENCODE_CONFIG",
                    "OPENCODE_CONFIG_DIR",
                ],
                local_provider_reserved_env: &[
                    "OPENCODE_CONFIG",
                    "OPENCODE_CONFIG_CONTENT",
                    "OPENCODE_CONFIG_DIR",
                    "OPENCODE_CONFIG_PROJECT_DISABLE",
                    "OPENCODE_PASSWORD",
                    "XDG_CONFIG_HOME",
                    "XDG_DATA_HOME",
                ],
                legacy_captured_paths: &["XDG_CONFIG_HOME", "XDG_DATA_HOME"],
                auth_scope: &[
                    "XDG_CONFIG_HOME",
                    "XDG_DATA_HOME",
                    "OPENCODE_CONFIG_CONTENT",
                ],
                protected_env: &["OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME"],
                protected_cleanup_names: &[".opencode"],
                ..Default::default()
            },
            route_scope_secret_names: &["OPENCODE_CONFIG_CONTENT"],
            local_provider: Some(crate::local_provider::validate),
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            separate_variant_reader: true,
            ..ModelReaderOptions::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            adhoc_integration: Some("opencode-go"),
            ..quota_readback(
                crate::quota::quota_evidence,
                eliot_plugin::quota::projection::account_usage_windows,
                |details| {
                    if details["integration"] == "opencode" {
                        "OpenCode Console workspace budgets and Go allowance; provider read-only values"
                    } else {
                        "OpenCode Go account usage; provider read-only values"
                    }
                },
            )
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(AdhocLaunchDefaults {
            model: AdhocModel::ProviderIntegration,
            ..adhoc_launch(
                AdhocPermissions::RefuseFull(
                    "the opencode slice has no full-access permission mapping",
                ),
                McpPolicy::Minimal,
            )
        }),
        documentation_section: Some("docs/harnesses/opencode.md#working-profile"),
        // No frozen permission knob or OS sandbox constrains this range.
        native_access_bounds: Some(|_| Some((1, 3))),
        effort: true,
        manager_preference: Some(2),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::SessionInstructionsFile,
        project_instruction_read: crate::instructions::PROJECT_READ,
        user_instruction_files: crate::instructions::USER_FILES,
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::Workspace),
        resume_config: true,
        manifest_extra_verbs: &["models"],
        compact: true,
        compaction_gap_recovery: true,
        models_workspace_preview: true,
        models_preview_auth_integrations: true,
        initial_model_refusal_cleanup: true,
        configure_session_mode: ConfigureSessionMode::Always,
        turn_completed_reports_effective_model: true,
        backfill_failed_turn_readback: true,
        executable_stems: &["opencode"],
        version_prefixes: &["opencode"],
        input_token_accounting: InputTokenAccounting::ExcludesCacheWrites,
        output_excludes_reasoning: true,
        ..HarnessCapabilities::default()
    }
}

fn auth_command(args: &[&str]) -> Option<NativeAuthInvocation> {
    let exe = crate::install::candidate_paths()
        .ok()?
        .into_iter()
        .find(|path| path.is_absolute())?;
    Some((exe, args.iter().copied().map(Into::into).collect()))
}

fn classify_auth_status(code: Option<i32>, stdout: &[u8], _: &[u8]) -> NativeAuthState {
    #[derive(serde::Deserialize)]
    struct Integration {
        connections: Vec<serde::de::IgnoredAny>,
    }

    if code != Some(0) {
        return NativeAuthState::Unknown;
    }
    match serde_json::from_slice::<Vec<Integration>>(stdout) {
        Ok(rows) if rows.iter().any(|row| !row.connections.is_empty()) => NativeAuthState::SignedIn,
        Ok(_) => NativeAuthState::SignedOut,
        Err(_) => NativeAuthState::Unknown,
    }
}

fn decode_auth_ids(stdout: &[u8]) -> Option<Vec<String>> {
    let entries: Vec<serde_json::Value> = serde_json::from_slice(stdout).ok()?;
    Some(
        entries
            .iter()
            .filter_map(|entry| entry.get("id")?.as_str())
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}
