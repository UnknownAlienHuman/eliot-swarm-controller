//! Antigravity native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocLaunchDefaults, AdhocModel, AdhocPermissions, NativeAccessBounds, NativeQuotaReadback,
    NativeRoute, ProfileCapabilities, QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        native_route: NativeRoute::FixedCredential("antigravity-cli-keyring"),
        profile: ProfileCapabilities {
            command_refusal: Some(|cmd| {
                (!eliot_plugin::install::cli_identity_matches(cmd, crate::install::CLI_STEM)).then_some(
                "must name the native `agy` CLI, not the Antigravity IDE executable (which does not implement the ACP/model CLI and is never launched); install the `agy` CLI and put it on PATH",
            )
            }),
            native_subagents_policy: Some(NativeSubagentsPolicy::allow_by_default()),
            ..ProfileCapabilities::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            route_refusal: Some(|_, environment| {
                ["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"]
                    .into_iter()
                    .any(|name| environment.get(name).and_then(Value::as_str).is_some_and(|text| !text.is_empty()))
                    .then_some("Antigravity profile selects external auth; native keyring quota would describe another route")
            }),
            ..quota_readback(
                QuotaProjectionFormat::Windows,
                |_| "Antigravity account quota; provider read-only values",
            )
        }),
        adhoc_launch: Some(AdhocLaunchDefaults {
            model: AdhocModel::Explicit,
            ..adhoc_launch(
                AdhocPermissions::FullFlag("dangerously_skip_permissions"),
                McpPolicy::Inherit,
            )
        }),
        documentation_section: Some("docs/harnesses/antigravity.md#native-identity-and-transport"),
        native_access_bounds: Some(native_access_bounds),
        effort: true,
        prepared_mcp_config: true,
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: None,
            home_subdir: ".gemini",
            paths: &[
                "AGENTS.md",
                "GEMINI.md",
                "config/AGENTS.md",
                "config/GEMINI.md",
            ],
        }),
        reader_executable_discovery: true,
        resume_config: true,
        guard_refusal_point: Some(InjectPoint::TurnSafePoint),
        configure_session_mode: ConfigureSessionMode::Always,
        configure_reports_effort_verification: true,
        executable_stems: &["agy", "antigravity"],
        version_prefixes: &["antigravity", "agy"],
        ..HarnessCapabilities::default()
    }
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["dangerously_skip_permissions"].as_bool()?,
        snapshot["mode"].as_str(),
    ) {
        (true, _) => Some((3, 3)),
        (false, Some("plan")) => Some((0, 0)),
        (false, Some("auto" | "accept-edits") | None) => Some((1, 2)),
        (false, Some(_)) => None,
    }
}
