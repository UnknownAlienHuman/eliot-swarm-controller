//! Muse native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocEffort, LaunchDefaultPolicy, ModelReaderOptions, NativeAccessBounds, NativeAuthCapability,
    NativeAuthCheck, NativeAuthCommand, NativeAuthInvocation, NativeAuthState, NativeQuotaReadback,
    NativeRoute, ProfileCapabilities,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{
    ConfigureSessionMode, HarnessCapabilities, InstructionReadStatus::NotNative,
    OwnerRulesDeliveryMethod,
};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{
    adhoc_launch, instruction_files, quota_readback, sandbox_permissions,
};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        model_catalogue_presentation: Some(
            crate::catalog_presentation::model_catalogue_presentation,
        ),
        native_auth: Some(NativeAuthCapability {
            login: Some(NativeAuthCommand {
                resolve: || auth_command(&["login"]),
            }),
            signed_in_check: Some(NativeAuthCheck {
                command: NativeAuthCommand {
                    resolve: || auth_command(&["usage", "--json"]),
                },
                classify: classify_auth_status,
            }),
        }),
        native_route: NativeRoute::FixedCredential("ambient"),
        profile: ProfileCapabilities {
            approval_field: Some("approval"),
            native_subagents_policy: Some(NativeSubagentsPolicy::allow_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            budget_seconds: Some(60),
            ..ModelReaderOptions::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            method: "usage --json",
            unsupported_note: Some(
                "Muse usage --json requires the installed Windows vendor launcher",
            ),
            route_refusal: Some(crate::quota::route_refusal),
            ..quota_readback(
                crate::quota::project_quota,
                crate::quota::quota_usage_windows,
                |_| "Muse subscription allowance",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(adhoc_launch(
            sandbox_permissions(
                true,
                "approval",
                ["onRequest", "promptUnmatched", "allowAll"],
            ),
            McpPolicy::Inherit,
        )),
        documentation_section: Some("docs/harnesses/muse.md#usage-attribution"),
        native_access_bounds: Some(native_access_bounds),
        data_root_only_mcp: true,
        owner_rules_delivery: OwnerRulesDeliveryMethod::FirstInput,
        project_instruction_read: Some(instruction_files(&[
            ("AGENTS.md", NotNative),
            ("AGENTS.override.md", NotNative),
        ])),
        defer_harness_check: true, // #949
        resume_config: true,
        resume_previous_turn: true,
        launch_default_policy: LaunchDefaultPolicy::OmitForServerDefault,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        guard_refusal_choice_error: Some("Muse guarded request offers multiple refusal options"),
        allowed_configure_efforts: Some(&eliot_core::api::MUSE_REASONING_EFFORT_TIERS),
        configure_session_mode: ConfigureSessionMode::WhenModelPresent,
        configure_requires_idle_turn: true,
        configure_carries_previous_overlay: true,
        configure_effort_per_turn_overlay: true,
        configure_resolution_readback: true,
        configure_reports_accepted_model: true,
        provider_response_model_verification: true,
        configure_reports_effort_verification: true,
        reads_retained_quota: true,
        view_projection_gap_recovery: true,
        native_child_families: &eliot_core::MUSE_NATIVE_CHILD_FAMILIES,
        native_child_history_refusal: Some(
            "native history is unsupported for Muse child observations",
        ),
        executable_stems: &["muse"],
        version_prefixes: &["muse-code", "muse"],
        session_input_includes_cache_reads: true,
        ..HarnessCapabilities::default()
    }
}

#[cfg(windows)]
fn auth_command(args: &[&str]) -> Option<NativeAuthInvocation> {
    use std::path::{Path, PathBuf};

    let launcher = Path::new(crate::install::LAUNCHER_DIR).join(".muse-launcher.ps1");
    // Calling the vendor's launcher directly preserves the subcommand position.
    // The installed CMD shim prepends TUI flags to usage on some installations.
    let powershell = [
        ("ProgramFiles", r"PowerShell\7\pwsh.exe"),
        (
            "SystemRoot",
            r"System32\WindowsPowerShell\v1.0\powershell.exe",
        ),
    ]
    .into_iter()
    .filter_map(|(root, exe)| Some(PathBuf::from(std::env::var_os(root)?).join(exe)))
    .find(|path| path.is_absolute() && path.is_file())?;
    if !launcher.is_file() {
        return None;
    }
    let mut argv = vec![
        "-NoProfile".into(),
        "-File".into(),
        launcher.into_os_string(),
    ];
    argv.extend(args.iter().copied().map(std::ffi::OsString::from));
    Some((powershell, argv))
}

#[cfg(not(windows))]
fn auth_command(args: &[&str]) -> Option<NativeAuthInvocation> {
    let exe = eliot_plugin::install::find_on_path("muse").filter(|path| path.is_absolute())?;
    Some((exe, args.iter().copied().map(Into::into).collect()))
}

fn classify_auth_status(code: Option<i32>, stdout: &[u8], stderr: &[u8]) -> NativeAuthState {
    if code == Some(0)
        && serde_json::from_slice::<Value>(stdout).is_ok_and(|value| value["allowance"].is_object())
    {
        return NativeAuthState::SignedIn;
    }
    if code == Some(1)
        && [stdout, stderr].into_iter().any(|bytes| {
            std::str::from_utf8(bytes).is_ok_and(|text| {
                text.lines()
                    .any(|line| line.trim().starts_with("muse usage: not logged in:"))
            })
        })
    {
        return NativeAuthState::SignedOut;
    }
    NativeAuthState::Unknown
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["permissions"].as_str()?,
        snapshot["approval"].as_str(),
    ) {
        ("danger-full-access", _) => Some((3, 3)),
        ("workspace-write", Some("onRequest" | "denyUnmatched")) => Some((1, 1)),
        ("workspace-write", Some("promptUnmatched" | "allowAll")) => Some((2, 2)),
        ("workspace-write", None) => Some((1, 2)),
        _ => None,
    }
}
