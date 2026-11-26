//! Claude native adapter capability declaration.

use crate::CLAUDE_EFFORT_LEVELS;
use eliot_core::McpPolicy;
use eliot_core::api::harness::{
    AdhocEffort, AdhocLaunchDefaults, DocumentedModelCatalog, LegacyPreWriteRefusal,
    ModelReaderOptions, NativeAccessBounds, NativeAuthCapability, NativeAuthCheck,
    NativeAuthCommand, NativeAuthInvocation, NativeAuthState, NativeQuotaReadback, NativeRoute,
    ProfileCapabilities,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback, sandbox_permissions};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        legacy_pre_write_refusals: &[LegacyPreWriteRefusal::SteerContextRendering],
        skill_wrapper: Some(eliot_core::api::harness::NativeSkillWrapper {
            directory: ".claude-plugin",
            filename: "plugin.json",
            manifest: skill_manifest,
        }),
        catalogue_provider: eliot_core::api::harness::CatalogueProvider::Fixed("anthropic"),
        instruction_inventory: crate::instructions::INSTRUCTION_INVENTORY,
        model_login_source: Some(eliot_core::api::harness::ModelLoginSource::Files {
            root_env: "CLAUDE_CONFIG_DIR",
            default_root: ".claude",
            files: &[".credentials.json"],
        }),
        native_auth: Some(NativeAuthCapability {
            login: Some(NativeAuthCommand {
                resolve: || auth_command(&["auth", "login"]),
            }),
            signed_in_check: Some(NativeAuthCheck {
                command: NativeAuthCommand {
                    resolve: || auth_command(&["auth", "status", "--json"]),
                },
                classify: classify_auth_status,
            }),
        }),
        native_route: NativeRoute::FixedCredential("claude-cli-oauth"),
        profile: ProfileCapabilities {
            owner_subscription_reserve: true,
            read_only_markers: &[("permission_mode", "plan")],
            named_profile: Some(crate::profile_config::MAPPING),
            launch_model_scope: eliot_core::api::harness::LaunchModelScope::FixedRoute,
            environment: eliot_core::api::harness::ProfileEnvironment {
                store_paths: &["CLAUDE_CONFIG_DIR"],
                legacy_captured_paths: &["CLAUDE_CONFIG_DIR"],
                route_values: &[
                    "ANTHROPIC_BASE_URL",
                    "ANTHROPIC_AWS_BASE_URL",
                    "ANTHROPIC_BEDROCK_BASE_URL",
                    "ANTHROPIC_BEDROCK_MANTLE_BASE_URL",
                    "ANTHROPIC_FOUNDRY_BASE_URL",
                    "ANTHROPIC_FOUNDRY_RESOURCE",
                    "ANTHROPIC_VERTEX_BASE_URL",
                    "ANTHROPIC_VERTEX_PROJECT_ID",
                ],
                binary_modes: &[
                    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
                    "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD",
                    "CLAUDE_CODE_USE_BEDROCK",
                    "CLAUDE_CODE_USE_VERTEX",
                    "CLAUDE_CODE_USE_FOUNDRY",
                    "CLAUDE_CODE_USE_MANTLE",
                ],
                credential_presence: &[
                    "ANTHROPIC_API_KEY",
                    "ANTHROPIC_AUTH_TOKEN",
                    "CLAUDE_CODE_OAUTH_TOKEN",
                ],
                model_pins: &[
                    "ANTHROPIC_MODEL",
                    "ANTHROPIC_DEFAULT_MODEL",
                    "ANTHROPIC_DEFAULT_FABLE_MODEL",
                    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
                    "ANTHROPIC_DEFAULT_OPUS_MODEL",
                    "ANTHROPIC_DEFAULT_SONNET_MODEL",
                    "ANTHROPIC_CUSTOM_MODEL_OPTION",
                ],
                credential_directory: Some("CLAUDE_CONFIG_DIR"),
                auth_scope: &["CLAUDE_CONFIG_DIR"],
                cleanup_roots: &["CLAUDE_CONFIG_DIR"],
                protected_env: &["CLAUDE_CONFIG_DIR"],
                protected_home_paths: &[".claude"],
                protected_cleanup_names: &[".claude"],
                ..Default::default()
            },
            approval_field: Some("permission_mode"),
            route_selectors_resolved: true,
            route_scope_secret_names: &["CLAUDE_CODE_OAUTH_TOKEN"],
            latest_cmd: true,
            command_refusal: Some(eliot_plugin::capabilities::absolute_native_command_refusal),
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            interruption_code: Some("unknown"),
            ..ModelReaderOptions::default()
        },
        documented_models: Some(DocumentedModelCatalog {
            entries: crate::native::models::documented_models,
            note: "Claude native initialize inventory is unavailable for this scope. Entries are documented aliases and full model ids with unknown availability. The CLI default applies when no model is passed.",
            unverified_route: "Claude model catalogue route could not be verified",
            empty_native_inventory: "Claude model.list returned no usable ids in its projected inventory",
        }),
        quota_readback: Some(NativeQuotaReadback {
            route_refusal: Some(external_quota_route),
            ..quota_readback(
                crate::quota::quota_evidence,
                eliot_plugin::quota::projection::account_usage_windows,
                |_| "Claude CLI OAuth usage; provider read-only values",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Choices {
            values: CLAUDE_EFFORT_LEVELS,
            refusal: "unknown Claude effort level (low, medium, high, xhigh, or max)",
        }),
        adhoc_launch: Some(AdhocLaunchDefaults {
            mcp: McpPolicy::Minimal,
            pass_env: &["CLAUDE_CONFIG_DIR"],
            ..adhoc_launch(
                sandbox_permissions(
                    false,
                    "permission_mode",
                    ["manual", "auto", "bypassPermissions"],
                ),
                McpPolicy::Minimal,
            )
        }),
        documentation_section: Some("docs/harnesses/claude-code.md#sources-and-unresolved-facts"),
        turn_history_readback: Some(eliot_core::api::harness::TurnHistoryReadback::HistoryFile),
        turn_completed_reports_effective_model: true,
        configure_effort_refusal: Some(
            "Claude effort applies only at launch and cannot change mid-session (launch_only)",
        ),
        native_access_bounds: Some(native_access_bounds),
        effort: true,
        manager_preference: Some(1),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::AppendSystemPromptFile,
        project_instruction_read: crate::instructions::PROJECT_READ,
        user_instruction_files: crate::instructions::USER_FILES,
        resume_config: true,
        restart_requires_idle: true,
        compact: true,
        accepts_send_during_native_turn: true,
        configure_reports_accepted_model: true,
        configure_session_mode: ConfigureSessionMode::WhenModelPresent,
        configure_requires_idle_turn: true,
        executable_stems: &["claude"],
        windows_detached_stdio: true,
        version_prefixes: &["claude-code", "claude"],
        legacy_result_status: Some(legacy_result_outcome),
        input_token_accounting: InputTokenAccounting::ExcludesCacheWrites,
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
    struct Status {
        #[serde(rename = "loggedIn")]
        logged_in: bool,
    }

    match (code, serde_json::from_slice::<Status>(stdout).ok()) {
        (Some(0), Some(Status { logged_in: true })) => NativeAuthState::SignedIn,
        (Some(0 | 1), Some(Status { logged_in: false })) => NativeAuthState::SignedOut,
        _ => NativeAuthState::Unknown,
    }
}

fn external_quota_route(_: &Value, environment: &Value) -> Option<&'static str> {
    crate::quota::uses_external_route(environment)
        .then_some("Claude profile route is external or ambiguous; CLI OAuth quota would describe an unverified route")
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match snapshot["permission_mode"].as_str() {
        Some("plan") => Some((0, 0)),
        Some("manual" | "dontAsk") => Some((1, 1)),
        Some("acceptEdits" | "auto") => Some((2, 2)),
        Some("bypassPermissions") => Some((3, 3)),
        // The CLI default can come from settings outside the frozen profile.
        None => Some((1, 3)),
        Some(_) => None,
    }
}

fn legacy_result_outcome(status: Option<&str>) -> eliot_core::TurnOutcome {
    use eliot_core::TurnOutcome;
    match status.map(str::trim) {
        Some("success" | "completed") => TurnOutcome::Success,
        Some("cancelled" | "interrupted") => TurnOutcome::Interrupted,
        Some("error" | "failed" | "failure") => TurnOutcome::Failure,
        Some(reason)
            if {
                let lower = reason.to_lowercase();
                ["api_error", "error", "failed", "failure"]
                    .iter()
                    .any(|prefix| lower.starts_with(prefix))
            } =>
        {
            TurnOutcome::Failure
        }
        _ => TurnOutcome::Unknown,
    }
}

/// Environment markers that select an external Claude route. The daemon
/// refuses them from the frozen environment and the Claude quota reader
/// refuses them from native `settings.json`; both share this one list.
pub(crate) const EXTERNAL_ROUTE_MARKERS: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
    "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CLAUDE_CODE_USE_MANTLE",
];

fn skill_manifest() -> eliot_core::Result<String> {
    serde_json::to_string_pretty(&serde_json::json!({
        "name": eliot_plugin::skill::SKILL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "description": "Eliot Swarm coordination: identity, mail, claims, shared variables, children and handoff.",
        "skills": ["./skills/eliot-swarm"],
    }))
    .map_err(|_| eliot_core::Error::new("internal", "skill plugin manifest could not be encoded"))
}
