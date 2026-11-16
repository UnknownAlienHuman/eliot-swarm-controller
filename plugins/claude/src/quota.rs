//! OpenCodex 2.84.0 vendor-probes-oauth.ts: pattern only, no copied source.
use eliot_core::Result;
use eliot_plugin::quota as q;
use serde_json::Value;
pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let params = q::decode(raw)?;
    if [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
    ]
    .iter()
    .any(|key| {
        params
            .environment
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty())
    }) {
        return Err(q::environment(
            "Claude profile selects external auth or endpoint; CLI OAuth quota would describe another route",
        ));
    }
    let root = params
        .environment
        .get("CLAUDE_CONFIG_DIR")
        .and_then(Value::as_str)
        .map(std::ffi::OsString::from)
        .map(std::path::PathBuf::from)
        .map(Ok)
        .unwrap_or_else(|| q::home().map(|p| p.join(".claude")))?;
    let credentials;
    let token = if let Some(token) = params
        .environment
        .get("CLAUDE_CODE_OAUTH_TOKEN")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
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
