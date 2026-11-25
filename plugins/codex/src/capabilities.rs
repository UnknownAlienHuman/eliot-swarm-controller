//! Codex native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocEffort, LaunchDefaultPolicy, NativeAccessBounds, NativeAuthCapability, NativeAuthCheck,
    NativeAuthCommand, NativeAuthInvocation, NativeAuthState, NativeQuotaReadback, NativeRoute,
    ProfileCapabilities,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{HarnessCapabilities, InputTokenAccounting};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback, sandbox_permissions};
use serde_json::Value;

const ROUTE_ACCOUNT_KEY: &str = "account";
const ROUTE_BINDING_KEY: &str = "route_binding";
const CODEX_LOGIN_ENV: &str = "CODEX_HOME";
const ROUTE_KIND_RESPONSES: &str = "codex_responses";
const ROUTE_KIND_CHATGPT: &str = "codex_chatgpt";
const CODEX_PROXY_SELECTOR_ENV_NAMES: &[&str] = &["CODEX_HOME", "CODEX_SQLITE_HOME"];

/// Extracts only the nonsecret fields declared above and in Codex's frozen
/// route binding. Core owns canonical serialization and digesting.
fn response_route_identity(snapshot: &Value) -> Option<Value> {
    let account = snapshot[ROUTE_ACCOUNT_KEY].as_str();
    let identity = if let Some(binding) = snapshot.get(ROUTE_BINDING_KEY) {
        let route: eliot_core::RouteBinding = serde_json::from_value(binding.clone()).ok()?;
        route.validate_for("codex").ok()?;
        let (origin, path) =
            eliot_core::RouteBinding::provider_url_identity(&route.provider_base_url)?;
        let key_env = route.client_key_env_name.map(|name| {
            if cfg!(windows) {
                name.to_ascii_uppercase()
            } else {
                name
            }
        });
        serde_json::json!([ROUTE_KIND_RESPONSES, account, origin, path, key_env])
    } else {
        let login_home = snapshot
            .pointer(&format!("/pass_env_values/{CODEX_LOGIN_ENV}"))
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .map(eliot_core::workspace_path_key);
        if account.is_none() && login_home.is_none() {
            return None;
        }
        serde_json::json!([ROUTE_KIND_CHATGPT, account, login_home])
    };
    Some(identity)
}

pub(super) fn is_route_selector_env_name(name: &str) -> bool {
    CODEX_PROXY_SELECTOR_ENV_NAMES.iter().any(|selector| {
        if cfg!(windows) {
            name.eq_ignore_ascii_case(selector)
        } else {
            name == *selector
        }
    })
}

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        catalogue_provider: eliot_core::api::harness::CatalogueProvider::Fixed("openai"),
        instruction_inventory: crate::instructions::INSTRUCTION_INVENTORY,
        model_login_source: Some(eliot_core::api::harness::ModelLoginSource::Files {
            root_env: "CODEX_HOME",
            default_root: ".codex",
            files: &["auth.json"],
        }),
        native_auth: Some(NativeAuthCapability {
            login: Some(NativeAuthCommand {
                resolve: || auth_command(&["login"]),
            }),
            signed_in_check: Some(NativeAuthCheck {
                command: NativeAuthCommand {
                    resolve: || auth_command(&["login", "status"]),
                },
                classify: classify_auth_status,
            }),
        }),
        native_route: NativeRoute::ResponsesProxy {
            endpoint_keys: &["codex"],
            reserved_providers: &[
                "openai",
                "amazon-bedrock",
                "amazon-bedrock-runtime",
                "ollama",
                "lmstudio",
                "ollama-chat",
            ],
        },
        profile: ProfileCapabilities {
            read_only_markers: &[("collaboration_mode", "plan")],
            named_profile: Some(crate::profile_config::MAPPING),
            launch_model_scope: eliot_core::api::harness::LaunchModelScope::FixedRoute,
            environment: eliot_core::api::harness::ProfileEnvironment {
                auth_scope: CODEX_PROXY_SELECTOR_ENV_NAMES,
                cleanup_roots: CODEX_PROXY_SELECTOR_ENV_NAMES,
                protected_env: CODEX_PROXY_SELECTOR_ENV_NAMES,
                protected_home_paths: &[".codex"],
                protected_cleanup_names: &[".codex"],
                ..Default::default()
            },
            script_wrapper_cmd: true,
            project_doc_max_bytes: true,
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            method: "account/rateLimits/read",
            unsupported_note: Some(
                "Installed Codex app-server does not support account/rateLimits/read",
            ),
            route_refusal: Some(|profile, _| {
                profile.get("route_binding").is_some_and(|value| !value.is_null()).then_some(
                    "Codex profile routes through an adopted proxy; ChatGPT account limits would describe the direct route",
                )
            }),
            ..quota_readback(
                crate::discovery::quota_evidence,
                crate::discovery::quota_usage_windows,
                |_| "Codex account/rateLimits/read; provider read-only values",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(adhoc_launch(
            sandbox_permissions(true, "approval_policy", ["on-request", "never", "never"]),
            McpPolicy::Minimal,
        )),
        documentation_section: Some("docs/harnesses/codex.md#sources"),
        native_access_bounds: Some(native_access_bounds),
        workspace_write_commit_roots: true,
        effort: true,
        manager_preference: Some(0),
        provider_probe: true,
        provider_response_withholding: true,
        provider_response_route_identity: Some(response_route_identity),
        route_client_key_env_selector: Some(is_route_selector_env_name),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::DeveloperInstructions,
        project_instruction_read: crate::instructions::PROJECT_READ,
        user_instruction_files: crate::instructions::USER_FILES,
        requires_pass_env: true,
        frozen_store_environment: Some(eliot_core::api::harness::FrozenStoreEnvironment {
            display_name: "Codex",
            selectors: CODEX_PROXY_SELECTOR_ENV_NAMES,
            account_identity_keys: &[
                ("codex_home", "CODEX_HOME"),
                ("codex_sqlite_home", "CODEX_SQLITE_HOME"),
            ],
            private_directory: "codex-home",
            guard_file: "config.toml",
            direct_login_refusal: (
                "stand_codex_login_unsupported",
                "Codex ChatGPT/OAuth login is unavailable in a private stand; select a named Codex profile with an adopted Responses proxy",
            ),
            binding_refusal: "Codex on a private stand requires its stand-owned home and SQLite root; launch a fresh Agent if the retained store is outside the stand",
        }),
        native_discovery: true,
        resume_previous_turn: true,
        reconcile_unfinished_tools: true,
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::FrozenStore),
        agent_identity_env: true,
        launch_default_policy: LaunchDefaultPolicy::NativeMarkedDefault,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        compact: true,
        observed_families: eliot_core::OBSERVED_ROLLOUT_FAMILIES,
        observed_live_families: eliot_core::OBSERVED_ROLLOUT_FAMILIES,
        native_child_families: NATIVE_CHILD_FAMILIES,
        executable_stems: &["codex"],
        windows_detached_stdio: true,
        version_prefixes: &["codex-cli", "codex"],
        input_token_accounting: InputTokenAccounting::IncludesCacheReads,
        ..HarnessCapabilities::default()
    }
}

fn auth_command(args: &[&str]) -> Option<NativeAuthInvocation> {
    use eliot_plugin::install::{find_on_path, native_exe_name};

    let exe = find_on_path(&native_exe_name("codex")).filter(|path| path.is_absolute())?;
    Some((exe, args.iter().copied().map(Into::into).collect()))
}

fn classify_auth_status(code: Option<i32>, stdout: &[u8], stderr: &[u8]) -> NativeAuthState {
    let has_line = |predicate: fn(&str) -> bool| {
        [stdout, stderr]
            .into_iter()
            .filter_map(|bytes| std::str::from_utf8(bytes).ok())
            .any(|text| text.lines().map(str::trim).any(predicate))
    };
    match code {
        Some(0) if has_line(|line| line.starts_with("Logged in")) => NativeAuthState::SignedIn,
        Some(1) if has_line(|line| line.eq_ignore_ascii_case("Not logged in")) => {
            NativeAuthState::SignedOut
        }
        _ => NativeAuthState::Unknown,
    }
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["permissions"].as_str()?,
        snapshot["approval_policy"].as_str()?,
    ) {
        ("read-only", _) => Some((0, 0)),
        ("workspace-write", "on-request") => Some((1, 1)),
        ("workspace-write", "never") => Some((2, 2)),
        ("danger-full-access", _) => Some((3, 3)),
        _ => None,
    }
}

pub(super) const NATIVE_CHILD_FAMILIES: &[&str] = &["session", "turn", "message"];
