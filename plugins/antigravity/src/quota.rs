//! OpenCodex 2.84.0 quota/antigravity.ts: pattern only, no copied source.
use eliot_core::api::harness::QuotaProjection;
use eliot_core::{ProviderRestriction, ProviderRestrictionKind, Result};
use eliot_plugin::quota as q;
use serde_json::{Value, json};

// Installed agy 1.5.10 codeassistclient.userAgent / CLIAuthProvider (2026-11-17).
// The legacy antigravity/1.0 fingerprint omits Gemini and Argon groups even
// with the identical credential, project and retrieveUserQuotaSummary call.
const USER_AGENT: &str =
    "antigravity/cli/1.5.10 (os_type=windows; arch=amd64; aidev_client; auth_method=oauth)";

/// agy 1.5.10 emits this whole assistant warning followed by end_turn, with
/// no structured error or quota extension. Match the complete answer only,
/// after ordered chunk assembly; never search arbitrary model prose.
pub(crate) fn turn_restriction(text: &str, observed_at: i64) -> Option<ProviderRestriction> {
    let text = text.trim().strip_prefix("⚠ ").unwrap_or(text.trim());
    let suffix = text.strip_prefix(
        "Individual quota reached. Please upgrade your subscription to increase your limits.",
    )?;
    let retry_after = if suffix.is_empty() {
        None
    } else {
        let duration = suffix.strip_prefix(" Resets in ")?.strip_suffix('.')?;
        reset_delay_ms(duration).and_then(|delay| observed_at.checked_add(delay))
    };
    Some(ProviderRestriction {
        kind: ProviderRestrictionKind::Quota,
        retry_after,
        code: None,
        http_status: None,
    })
}

fn reset_delay_ms(mut text: &str) -> Option<i64> {
    let mut delay = 0_i64;
    let mut found = false;
    for (unit, scale) in [('h', 3_600_000_i64), ('m', 60_000), ('s', 1000)] {
        if let Some((number, rest)) = text.split_once(unit) {
            if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            delay = delay.checked_add(number.parse::<i64>().ok()?.checked_mul(scale)?)?;
            text = rest;
            found = true;
        }
    }
    (found && text.is_empty()).then_some(delay)
}
pub(crate) async fn read(raw: &Value) -> Result<Value> {
    q::decode(raw)?;
    // External provider routes never reach this reader: the daemon refuses
    // them in `router::catalog::external_quota_route` before spawning any
    // reader, so only the native keyring route is reported here.
    let credential = keyring_credential()?;
    let token = credential
        .pointer("/token/access_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| q::environment("Antigravity CLI OAuth token is unavailable"))?;
    let identity = q::read(
        "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
        token,
        Some(json!({"metadata":{"ideType":"ANTIGRAVITY"}})),
        &[("User-Agent", USER_AGENT)],
    )
    .await?;
    let project = identity
        .get("cloudaicompanionProject")
        .and_then(|v| v.as_str().or_else(|| v.get("id").and_then(Value::as_str)))
        .ok_or_else(|| {
            q::provider("Antigravity quota project is unavailable; no onboarding attempted")
        })?;
    let body = q::read(
        "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
        token,
        Some(json!({"project":project})),
        &[("User-Agent", USER_AGENT)],
    )
    .await?;
    q::result(
        summary_windows(&body),
        "Antigravity retrieveUserQuotaSummary",
    )
}

#[cfg(windows)]
fn keyring_credential() -> Result<Value> {
    q::windows_credential_json(
        "gemini:antigravity",
        [
            "Antigravity native keyring credential is unavailable",
            "Antigravity native keyring credential exceeds its bound",
            "Antigravity native keyring credential schema is unavailable",
        ],
    )
}

#[cfg(not(windows))]
fn keyring_credential() -> Result<Value> {
    Err(q::environment(
        "Antigravity native credential reader is currently Windows-only",
    ))
}

fn summary_windows(body: &Value) -> Vec<Value> {
    let mut windows = Vec::new();
    for group in body
        .get("groups")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(16)
    {
        let name = group
            .get("displayName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let family = if name == "Gemini Models" {
            "Gemini"
        } else if name.starts_with("Gemini ") && name.len() <= 64 {
            name
        } else if name == "Claude and GPT models" {
            "Claude/GPT"
        } else {
            "Model group"
        };
        for bucket in group
            .get("buckets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(8)
        {
            let remaining = bucket.get("remaining").unwrap_or(bucket);
            let fraction = remaining.get("remainingFraction").and_then(Value::as_f64);
            let period = format!(
                "{} {} {}",
                bucket.get("window").and_then(Value::as_str).unwrap_or(""),
                bucket.get("bucketId").and_then(Value::as_str).unwrap_or(""),
                bucket
                    .get("displayName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
            )
            .to_ascii_lowercase();
            let minutes = if period.contains("week") {
                Some(10080)
            } else if period.contains("5h") || period.contains("five") {
                Some(300)
            } else {
                None
            };
            let label = format!(
                "{family} {}",
                if minutes == Some(10080) {
                    "weekly"
                } else if minutes == Some(300) {
                    "5-hour"
                } else {
                    "quota"
                }
            );
            if let Some(window) = q::window(
                &label,
                fraction.map(|f| 100.0 - f * 100.0),
                q::reset(bucket.get("resetTime")),
                minutes,
            ) {
                windows.push(window);
            }
        }
    }
    windows
}

pub(crate) fn project_quota(
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    const GROUPS: q::projection::WindowGroups = &[
        ("gemini 4 argon", &["argon"]),
        ("claude/gpt", &["claude", "gpt"]),
        ("gemini", &["gemini"]),
    ];
    q::projection::project_windows(details, GROUPS, model, now)
}
