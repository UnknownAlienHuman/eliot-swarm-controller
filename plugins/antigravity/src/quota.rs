//! OpenCodex 2.84.0 quota/antigravity.ts: pattern only, no copied source.
use eliot_core::Result;
use eliot_plugin::quota as q;
use serde_json::{Value, json};
pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let params = q::decode(raw)?;
    if [
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS",
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
            "Antigravity profile selects external auth; native keyring quota would describe another route",
        ));
    }
    let credential = q::antigravity_credential()?;
    let token = credential
        .pointer("/token/access_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| q::environment("Antigravity CLI OAuth token is unavailable"))?;
    let identity = q::read(
        "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
        token,
        Some(json!({"metadata":{"ideType":"ANTIGRAVITY"}})),
        &[("User-Agent", "antigravity/1.0")],
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
        &[("User-Agent", "antigravity/1.0")],
    )
    .await?;
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
            .unwrap_or("")
            .to_ascii_lowercase();
        let family = if name.contains("gemini") {
            "Gemini"
        } else if name.contains("claude") || name.contains("3p") {
            "Claude"
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
            let fraction = remaining
                .get("remainingFraction")
                .or_else(|| remaining.get("remainingPercentage"))
                .and_then(Value::as_f64);
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
    q::result(windows, "Antigravity retrieveUserQuotaSummary")
}
