//! Translation of the vendor's one-shot subscription allowance command.

use eliot_plugin::quota;
use serde_json::Value;

const ALTERNATE_ROUTE: &[&str] = &[
    "MUSE_AUTH_PATH",
    "MUSE_CONFIG_DIR",
    "MUSE_API_KEY",
    "MUSE_BASE_URL",
    "META_API_KEY",
    "META_BASE_URL",
    "TBH_API_KEY",
    "TBH_BASE_URL",
];

pub(crate) fn route_refusal(profile: &Value, environment: &Value) -> Option<&'static str> {
    let alternate_route = profile.get("api_key_env").is_some_and(Value::is_string)
        || quota::has_nonempty_env_marker(environment, ALTERNATE_ROUTE)
        || environment
            .as_object()
            .into_iter()
            .flatten()
            .any(|(name, value)| {
                name.eq_ignore_ascii_case("TBH_CREDENTIAL_BACKEND")
                    && value
                        .as_str()
                        .is_some_and(|backend| !matches!(backend, "" | "file"))
            });
    alternate_route.then_some("Muse allowance requires the profile's native file-backed sign-in")
}

#[cfg(not(windows))]
pub(crate) async fn read(_: eliot_plugin::protocol::QuotaReadParams) -> eliot_core::Result<Value> {
    Err(eliot_core::Error::new(
        "unsupported",
        "Muse usage --json requires the Windows vendor launcher",
    ))
}

#[cfg(windows)]
pub(crate) async fn read(
    params: eliot_plugin::protocol::QuotaReadParams,
) -> eliot_core::Result<Value> {
    use eliot_plugin::native_cli::{BoundedReadLabels, run_bounded};
    use std::{path::Path, time::Duration};
    use tokio::time::Instant;

    if let Some(reason) = route_refusal(&params.profile, &params.environment) {
        return Err(quota::environment(reason));
    }
    let launcher = Path::new(crate::install::LAUNCHER_DIR).join(".muse-launcher.ps1");
    if !launcher.is_file() {
        return Err(eliot_core::Error::new(
            "unsupported",
            "Muse vendor launcher is unavailable",
        ));
    }
    let environment = params
        .environment
        .as_object()
        .ok_or_else(|| quota::environment("Muse quota environment is unavailable"))?;
    let system_root = environment
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("SystemRoot"))
        .and_then(|(_, value)| value.as_str())
        .map(Path::new)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| quota::environment("Windows PowerShell is unavailable"))?;
    let shell = system_root.join(r"System32\WindowsPowerShell\v1.0\powershell.exe");
    #[expect(
        clippy::disallowed_methods,
        reason = "The Muse allowance reader bounds and owns this metadata process."
    )]
    #[expect(
        clippy::disallowed_types,
        reason = "The command is executed within the daemon-owned metadata plugin."
    )]
    let mut command = tokio::process::Command::new(shell);
    command
        .env_clear()
        .envs(environment.iter().filter_map(|(name, value)| {
            let upper = name.to_ascii_uppercase();
            if upper.starts_with("MUSE_") || upper.starts_with("TBH_") {
                return None;
            }
            value.as_str().map(|value| (name, value))
        }))
        .env("TBH_CREDENTIAL_BACKEND", "file")
        .env("MUSE_LOGIN", "0")
        .env("MUSE_NO_AUTO_UPDATE", "1")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(launcher)
        .args(["usage", "--json"]);
    if let Some(channel) = crate::install::installed_channel() {
        command.env("MUSE_CHANNEL", channel);
    }
    let bytes = run_bounded(
        command,
        8192,
        Instant::now() + Duration::from_secs(12),
        &BoundedReadLabels {
            start: "Muse allowance process could not start",
            deadline: "Muse allowance read timed out",
            unreadable: "Muse allowance output was unreadable",
            status: "Muse allowance process returned an error",
        },
    )
    .await?;
    project(&bytes)
}

#[cfg(windows)]
fn project(bytes: &[u8]) -> eliot_core::Result<Value> {
    use eliot_core::api::harness::{QuotaObservation, QuotaWindow};
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Usage {
        schema_version: u32,
        allowance: Option<Allowance>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Allowance {
        tier: Option<String>,
        window: Window,
        weekly: Window,
        as_of_ms: i64,
        source: String,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Window {
        used_percent: f64,
        resets_at_ms: Option<i64>,
    }
    let usage: Usage = serde_json::from_slice(bytes)
        .map_err(|_| quota::provider("Muse returned an invalid allowance response"))?;
    if usage.schema_version != 1 {
        return Err(quota::provider("Muse returned an unknown allowance schema"));
    }
    let allowance = usage
        .allowance
        .ok_or_else(|| quota::provider("Muse reported no subscription allowance"))?;
    let windows = [
        ("5-hour", allowance.window, 300),
        ("Weekly", allowance.weekly, 10_080),
    ]
    .into_iter()
    .map(|(label, window, mins)| QuotaWindow {
        label: label.to_owned(),
        used_percent: window.used_percent,
        resets_at_ms: window.resets_at_ms,
        window_duration_mins: Some(mins),
    })
    .collect();
    let observation = QuotaObservation {
        observed_at_ms: allowance.as_of_ms,
        windows,
    };
    observation.validate()?;
    let source = if allowance.source == "mint" {
        "mint"
    } else {
        "usage --json"
    };
    let tier = allowance.tier.filter(|value| {
        !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
    });
    Ok(json!({"details":{
        "state":"ready", "origin":"provider", "source":source,
        "observedAtMs":observation.observed_at_ms, "windows":observation.windows, "tier":tier,
    }}))
}
