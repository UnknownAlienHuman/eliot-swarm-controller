//! OpenCodex 2.84.0 quota/antigravity.ts: pattern only, no copied source.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorded_agy_quota_warning_has_a_bounded_reset_hint() {
        let text = "\n⚠ Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 107h59m40s.\n";
        let restriction = turn_restriction(text, 1000).unwrap();
        assert_eq!(restriction.kind, ProviderRestrictionKind::Quota);
        assert_eq!(restriction.retry_after, Some(388_781_000));
        assert!(turn_restriction(&format!("An example: {text}"), 1000).is_none());
        assert!(turn_restriction(&format!("{text}Ordinary answer"), 1000).is_none());
        assert!(turn_restriction("Individual quota reached", 1000).is_none());
        assert_eq!(turn_restriction(text, i64::MAX).unwrap().retry_after, None);
        assert_eq!(reset_delay_ms("999999999999999999999h"), None);
        assert_eq!(reset_delay_ms("1s1h"), None);
    }

    #[test]
    fn recorded_summary_preserves_separate_gemini_and_argon_windows() {
        // Redacted retrieveUserQuotaSummary shape, agy 1.5.10 client,
        // daily-cloudcode-pa, 2026-11-17. No model inference or credentials.
        let body = json!({"groups":[
            {"displayName":"Gemini Models","buckets":[
                {"bucketId":"gemini-weekly","window":"weekly","remainingFraction":0.9839624,"resetTime":"2026-11-21T11:20:27Z"},
                {"bucketId":"gemini-5h","window":"5h","remainingFraction":1,"resetTime":"2026-11-17T12:51:29Z"}]},
            {"displayName":"Claude and GPT models","buckets":[
                {"bucketId":"3p-weekly","window":"weekly","remainingFraction":1}]},
            {"displayName":"Gemini 4 Argon","buckets":[
                {"bucketId":"gemini-argon-wtu-weekly","window":"weekly","remainingFraction":0,"resetTime":"2026-11-21T19:44:48Z"}]}]});
        let windows = summary_windows(&body);
        assert_eq!(windows.len(), 4);
        assert_eq!(windows[0]["label"], "Gemini weekly");
        assert_eq!(windows[1]["label"], "Gemini 5-hour");
        assert_eq!(windows[2]["label"], "Claude/GPT weekly");
        assert_eq!(windows[3]["label"], "Gemini 4 Argon weekly");
        assert_eq!(windows[3]["usedPercent"], 100.0);
        assert_eq!(windows[3]["resetsAtMs"], 1_795_290_288_000_i64);
        assert_eq!(windows[3]["windowDurationMins"], 10080);
        assert!(summary_windows(&json!({"groups":[]})).is_empty());
        assert!(
            summary_windows(&json!({"groups":[{"buckets":[{"remainingFraction":2}]}]})).is_empty()
        );
    }
}
