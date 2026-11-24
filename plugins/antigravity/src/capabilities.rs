//! Antigravity native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::harness::{
    AdhocAccess, AdhocLaunchDefaults, NativeAccessBounds, NativeQuotaReadback,
    QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        quota_readback: Some(NativeQuotaReadback {
            projection: QuotaProjectionFormat::Windows,
            method: "quota.read",
            adhoc_integration: None,
            description: |_| "Antigravity account quota; provider read-only values",
            unsupported_note: None,
            route_refusal: Some(|_, environment| {
                ["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"]
                    .into_iter()
                    .any(|name| environment.get(name).and_then(Value::as_str).is_some_and(|text| !text.is_empty()))
                    .then_some("Antigravity profile selects external auth; native keyring quota would describe another route")
            }),
        }),
        adhoc_launch: Some(AdhocLaunchDefaults {
            permissions: |snapshot, access| {
                snapshot.insert(
                    "dangerously_skip_permissions".to_owned(),
                    Value::Bool(access == Some(AdhocAccess::Full)),
                );
                Ok(())
            },
            mcp: eliot_core::McpPolicy::Inherit,
            pass_env: &[],
            installed_profile_mcp: eliot_core::McpPolicy::Inherit,
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
