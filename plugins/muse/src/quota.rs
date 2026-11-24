//! Translation of the vendor's one-shot subscription allowance command.

use eliot_core::api::harness::{QuotaHeadroom, QuotaMapping, QuotaProjection, QuotaWindowBounds};
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
    use eliot_plugin::native_cli::{BoundedReadLabels, run_bounded_with_stderr};
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
    const LOGGED_OUT: &str = "not logged in: run `muse login` first";
    const FAILED_EXIT: &str = "Muse allowance process returned an error";
    match run_bounded_with_stderr(
        command,
        8192,
        Instant::now() + Duration::from_secs(12),
        &BoundedReadLabels {
            start: "Muse allowance process could not start",
            deadline: "Muse allowance read timed out",
            unreadable: "Muse allowance output was unreadable",
            status: FAILED_EXIT,
        },
        &eliot_core::SecretPolicy::default(),
    )
    .await
    {
        Ok(bytes) => project(&bytes),
        Err(error)
            if error.message.as_str() == FAILED_EXIT
                && error.diagnostics.iter().any(|diagnostic| {
                    diagnostic
                        .snapshot
                        .text
                        .lines()
                        .any(|line| line.trim().strip_prefix("muse usage: ") == Some(LOGGED_OUT))
                }) =>
        {
            Ok(unavailable(LOGGED_OUT))
        }
        Err(mut error) => {
            error.diagnostics.clear();
            Err(error)
        }
    }
}

#[cfg(windows)]
fn project(bytes: &[u8]) -> eliot_core::Result<Value> {
    use eliot_core::api::harness::QuotaWindow;
    use serde_json::json;

    let usage: Value = serde_json::from_slice(bytes)
        .map_err(|_| quota::provider("Muse returned an invalid allowance response"))?;
    if usage["schemaVersion"].as_u64() != Some(1) {
        return Err(quota::provider("Muse returned an unknown allowance schema"));
    }
    let allowance = &usage["allowance"];
    if let Some(reason) = [&usage["reason"], &allowance["reason"]]
        .into_iter()
        .find(|reason| !reason.is_null())
    {
        let reason = reason
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty());
        return Ok(unavailable(
            reason.unwrap_or("Muse reported no subscription allowance"),
        ));
    }
    let windows: Vec<_> = [("5-hour", "window", 300), ("Weekly", "weekly", 10_080)]
        .into_iter()
        .filter_map(|(label, key, mins)| {
            let window = &allowance[key];
            let used_percent = window["usedPercent"]
                .as_f64()
                .filter(|used| used.is_finite() && *used >= 0.0)?;
            Some(QuotaWindow {
                label: label.to_owned(),
                used_percent,
                resets_at_ms: window["resetsAtMs"].as_i64().filter(|at| *at >= 0),
                window_duration_mins: Some(mins),
            })
        })
        .collect();
    if windows.is_empty() {
        return Err(quota::provider(
            "Muse reported no subscription allowance windows",
        ));
    }
    let source = if allowance["source"].as_str() == Some("mint") {
        "mint"
    } else {
        "usage --json"
    };
    let mut details = json!({
        "state":"ready", "origin":"provider", "source":source, "windows":windows,
    });
    if let Some(at) = allowance["asOfMs"].as_i64().filter(|at| *at >= 0) {
        details["observedAtMs"] = json!(at);
    }
    if let Some(tier) = allowance["tier"].as_str().filter(|value| {
        !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
    }) {
        details["tier"] = json!(tier);
    }
    Ok(json!({"details":details}))
}

#[cfg(windows)]
fn unavailable(reason: &str) -> Value {
    let normalized = reason.to_ascii_lowercase().replace(['_', '-'], " ");
    let code = if normalized.starts_with("not logged in")
        || normalized.starts_with("logged out")
        || matches!(
            normalized.as_str(),
            "notloggedin" | "loggedout" | "auth required" | "authrequired"
        ) {
        "auth_required"
    } else {
        "quota_unavailable"
    };
    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    let reason = eliot_core::bound_chars(&eliot_core::redact_secret_shapes(&reason), 512);
    serde_json::json!({"details":{"state":"unavailable", "code":code, "reason":reason}})
}

pub(crate) fn project_quota(
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    let (windows, complete, provenance) = usage_pair_window_set(details)?;
    let mut projection =
        quota::projection::windows_projection(windows, &[], complete, model, now, provenance);
    if !complete {
        projection.headroom = QuotaHeadroom::default();
    } else if provenance != "muse:windows"
        && let Some(weekly) = details.get("usage").unwrap_or(details).get("weekly")
        && quota::projection::window_mapping(weekly, &[], model) != QuotaMapping::Unmapped
    {
        quota::projection::observe(&mut projection.headroom, weekly, now, true);
    }
    Some(projection)
}

/// Live and legacy usage pairs expose account usage bounds even without a windows array.
pub(crate) fn quota_usage_windows(details: &Value, now: i64) -> Vec<QuotaWindowBounds> {
    if details["windows"].as_array().is_some() {
        return quota::projection::account_usage_windows(details, now);
    }
    let usage = details.get("usage").unwrap_or(details);
    [
        ("window", "5-hour", None),
        ("weekly", "Weekly", Some(10_080)),
    ]
    .into_iter()
    .filter_map(|(key, label, duration)| {
        quota::projection::usage_bound(label, &usage[key], duration, now)
    })
    .collect()
}

/// Accept live usage pairs, legacy top-level pairs and retained window observations.
fn usage_pair_window_set(details: &Value) -> Option<(Vec<&Value>, bool, &'static str)> {
    if let Some(windows) = details
        .get("windows")
        .and_then(Value::as_array)
        .filter(|windows| !windows.is_empty())
    {
        return Some((windows.iter().collect(), windows.len() == 2, "muse:windows"));
    }
    if let Some(usage) = details.get("usage").and_then(Value::as_object) {
        let mut windows = Vec::new();
        for key in ["window", "weekly"] {
            if let Some(window) = usage.get(key).filter(|window| window.is_object()) {
                windows.push(window);
            }
        }
        if windows.is_empty() {
            return None;
        }
        let complete = windows.len() == 2;
        return Some((windows, complete, "muse:usage"));
    }
    let mut windows = Vec::new();
    for key in ["window", "weekly"] {
        if let Some(window) = details.get(key).filter(|window| window.is_object()) {
            windows.push(window);
        }
    }
    if windows.is_empty() {
        return None;
    }
    let complete = windows.len() == 2;
    Some((windows, complete, "muse:window/weekly"))
}
