//! OpenCodex 2.84.0 vendor-probes-oauth.ts: pattern only, no copied source.
use eliot_core::api::harness::QuotaProjection;
use eliot_core::{Result, api::CLAUDE_EXTERNAL_ROUTE_MARKERS};
use eliot_plugin::quota as q;
use serde_json::Value;
use std::{ffi::OsStr, path::Path};

fn env_value<'a>(environment: &'a Value, name: &str) -> Option<&'a str> {
    environment.as_object()?.iter().find_map(|(key, value)| {
        (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name)))
            .then(|| value.as_str())
            .flatten()
    })
}

/// Refuse user settings that select an external route: the daemon already
/// refuses external provider routes from the frozen environment (see
/// `router::catalog::external_quota_route`), but only this reader sees the
/// native `settings.json`, so settings-selected routes are reported here
/// using the core-owned marker list shared with the daemon guard.
fn require_native_oauth_settings(settings: &Value) -> Result<()> {
    let settings_env = &settings["env"];
    if uses_external_route(settings_env)
        || env_value(settings_env, "CLAUDE_CONFIG_DIR").is_some()
        || env_value(settings_env, "CLAUDE_CODE_OAUTH_TOKEN").is_some()
        || settings.get("apiKeyHelper").is_some()
    {
        return Err(q::environment(
            "Claude profile route is external or ambiguous; CLI OAuth quota would describe an unverified route",
        ));
    }
    Ok(())
}

pub(crate) fn uses_external_route(environment: &Value) -> bool {
    q::has_nonempty_env_marker(environment, CLAUDE_EXTERNAL_ROUTE_MARKERS)
}

fn subscription_status(status: &Value) -> bool {
    status["loggedIn"].as_bool() == Some(true)
        && status["apiProvider"].as_str() == Some("firstParty")
        && status["authMethod"].as_str() == Some("claude.ai")
}

pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let params = q::decode(raw)?;
    // External provider routes from the frozen environment never reach this
    // reader: the daemon refuses them in
    // `router::catalog::external_quota_route` before spawning any reader.
    let root = match env_value(&params.environment, "CLAUDE_CONFIG_DIR").filter(|v| !v.is_empty()) {
        Some(root) if Path::new(root).is_absolute() => Path::new(root).to_owned(),
        Some(_) => {
            return Err(q::environment(
                "Claude quota requires an absolute configuration root",
            ));
        }
        None => q::home()?.join(".claude"),
    };
    let settings = eliot_plugin::owner_scan::read_jsonc_document(
        &root.join("settings.json"),
        "Claude user settings",
        eliot_plugin::owner_scan::MAX_SCAN_BYTES,
    )
    .map_err(|_| q::environment("Claude quota could not verify native user settings"))?;
    if let Some(settings) = settings.as_ref() {
        require_native_oauth_settings(settings)?;
    }
    let program = params.profile["cmd"]
        .as_str()
        .filter(|cmd| Path::new(cmd).is_absolute())
        .ok_or_else(|| q::environment("Claude quota requires a frozen native executable"))?;
    // The daemon supplies an isolated reader directory. Verify the provider
    // and auth method in that directory using the same private environment.
    let cwd = std::env::current_dir()
        .map_err(|_| q::environment("Claude quota reader directory is unavailable"))?;
    let environment = eliot_plugin::session::target_environment(
        Some(&params.environment),
        params.profile.get("pass_env"),
        "Claude",
    )
    .map_err(|_| q::environment("Claude quota could not resolve the profile environment"))?;
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        crate::native::auth_status(OsStr::new(program), &cwd, &environment.variables),
    )
    .await
    .ok()
    .and_then(Result::ok);
    if status
        .as_ref()
        .and_then(|value| value["loggedIn"].as_bool())
        == Some(false)
    {
        return Err(eliot_core::Error::new(
            "auth_required",
            "Claude CLI is signed out.",
        ));
    }
    if !status.as_ref().is_some_and(subscription_status) {
        return Err(q::environment(
            "Claude CLI did not confirm a first-party subscription route",
        ));
    }

    let credentials;
    let token = if let Some(token) =
        env_value(&params.environment, "CLAUDE_CODE_OAUTH_TOKEN").filter(|v| !v.is_empty())
    {
        token
    } else {
        credentials = q::read_json(&root.join(".credentials.json"))?;
        credentials
            .pointer("/claudeAiOauth/accessToken")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| q::environment("Claude CLI OAuth access token is unavailable"))?
    };
    let body = q::read(
        "https://api.anthropic.com/api/oauth/usage",
        token,
        None,
        &[
            ("anthropic-beta", "oauth-2025-04-20"),
            ("User-Agent", "claude-cli/2.1.325 (external, cli)"),
        ],
    )
    .await?;
    let mut windows = Vec::new();
    for (key, label, minutes) in [
        ("five_hour", "5-hour", 300),
        ("seven_day", "Weekly", 10080),
        ("seven_day_opus", "Opus weekly", 10080),
        ("seven_day_sonnet", "Sonnet weekly", 10080),
        ("seven_day_fable", "Fable weekly", 10080),
    ] {
        if let Some(bucket) = body.get(key)
            && let Some(window) = q::window(
                label,
                bucket.get("utilization").and_then(Value::as_f64),
                q::reset(bucket.get("resets_at")),
                Some(minutes),
            )
        {
            windows.push(window);
        }
    }
    for bucket in body
        .get("limits")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(32)
    {
        if bucket.get("kind").and_then(Value::as_str) != Some("weekly_scoped") {
            continue;
        }
        let name = bucket
            .pointer("/scope/model/display_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        let label = if name.contains("opus") {
            "Opus weekly"
        } else if name.contains("sonnet") {
            "Sonnet weekly"
        } else if name.contains("fable") {
            "Fable weekly"
        } else {
            continue;
        };
        if !windows
            .iter()
            .any(|w| w.get("label").and_then(Value::as_str) == Some(label))
            && let Some(window) = q::window(
                label,
                bucket.get("percent").and_then(Value::as_f64),
                q::reset(bucket.get("resets_at")),
                Some(10080),
            )
        {
            windows.push(window);
        }
    }
    q::result(windows, "Claude CLI OAuth usage")
}

pub(crate) fn project_quota(
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    const GROUPS: q::projection::WindowGroups = &[
        ("opus", &["opus"]),
        ("sonnet", &["sonnet"]),
        ("fable", &["fable"]),
        ("haiku", &["haiku"]),
    ];
    q::projection::project_windows(details, GROUPS, model, now)
}
