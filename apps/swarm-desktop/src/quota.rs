//! Quota windows from `harness.quota` for the Usage page's rings: Codex's
//! `rateLimits` windows (`primary` / `secondary`: `usedPercent`,
//! `windowDurationMins`, `resetsAt`) and Muse's `usage` windows (`window`:
//! `usedPercent`, `windowDurationMins`, `resetsAtMs`; `weekly`:
//! `usedPercent`, `resetsAtMs`), as the plugins project them
//! (`plugins/codex/src/discovery.rs` `project_rate_limits`,
//! `plugins/muse/src/native/types.rs`). Provider-verbatim percentages; a
//! harness whose read is not `supported` has no windows.

use eliot_core::api::HarnessQuotaResponse;
use eliot_core::api::harness::HARNESS_QUOTA_SUPPORTED;
use serde_json::Value;

const WEEK_MINS: i64 = 7 * 24 * 60;
const DAY_MINS: i64 = 24 * 60;

/// One quota window: what it is, the share remaining and when it resets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    pub label: String,
    /// Percent remaining, 0–100.
    pub remaining: i64,
    pub resets_at_ms: Option<i64>,
    duration_mins: Option<i64>,
}

impl Window {
    /// "Resets in 2 h 10 min", or nothing known.
    pub fn reset_note(&self, now_ms: i64) -> String {
        let Some(at) = self.resets_at_ms else {
            return "Reset time not reported.".to_owned();
        };
        let mins = (at - now_ms).max(0) / 60_000;
        match mins {
            0 => "Resets now.".to_owned(),
            m if m < 60 => format!("Resets in {m} min."),
            m if m < DAY_MINS => format!("Resets in {} h {} min.", m / 60, m % 60),
            m => format!("Resets in {} days.", (m + DAY_MINS - 1) / DAY_MINS),
        }
    }
}

/// The windows of a supported quota read, longest first (weekly, then the
/// 5-hour window, as Antigravity shows them).
pub fn windows(reply: &HarnessQuotaResponse) -> Vec<Window> {
    if reply.status != HARNESS_QUOTA_SUPPORTED {
        return Vec::new();
    }
    let Some(details) = reply.details.as_ref() else {
        return Vec::new();
    };
    let mut found = Vec::new();
    if let Some(limits) = details.get("rateLimits") {
        for key in ["primary", "secondary"] {
            if let Some(window) = limits.get(key).and_then(|w| window(w, "resetsAt", None)) {
                found.push(window);
            }
        }
    }
    if let Some(usage) = details.get("usage") {
        if let Some(window) = usage
            .get("window")
            .and_then(|w| window(w, "resetsAtMs", None))
        {
            found.push(window);
        }
        if let Some(window) = usage
            .get("weekly")
            .and_then(|w| window(w, "resetsAtMs", Some(WEEK_MINS)))
        {
            found.push(window);
        }
    }
    found.sort_by_key(|window| std::cmp::Reverse(window.duration_mins.unwrap_or(0)));
    found
}

fn window(value: &Value, reset_key: &str, duration: Option<i64>) -> Option<Window> {
    let used = value.get("usedPercent")?.as_i64()?;
    let duration_mins = duration.or_else(|| value.get("windowDurationMins")?.as_i64());
    let resets_at_ms = value
        .get(reset_key)
        .and_then(Value::as_i64)
        // Codex reports seconds since the epoch; Muse milliseconds.
        .map(|at| if at < 100_000_000_000 { at * 1000 } else { at });
    Some(Window {
        label: label(duration_mins),
        remaining: (100 - used).clamp(0, 100),
        resets_at_ms,
        duration_mins,
    })
}

fn label(duration_mins: Option<i64>) -> String {
    match duration_mins {
        Some(WEEK_MINS) => "Weekly limit".to_owned(),
        Some(mins) if mins > 0 && mins % DAY_MINS == 0 => format!("{}-day limit", mins / DAY_MINS),
        Some(mins) if mins > 0 && mins % 60 == 0 => format!("{}-hour limit", mins / 60),
        Some(mins) if mins > 0 => format!("{mins}-minute limit"),
        _ => "Usage limit".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::harness::HARNESS_QUOTA_UNSUPPORTED;
    use serde_json::json;

    fn reply(status: &str, details: Option<Value>) -> HarnessQuotaResponse {
        HarnessQuotaResponse {
            harness: "codex".to_owned(),
            scope: "s".to_owned(),
            fetched_at_ms: 1,
            ttl_ms: 1,
            credential: "default".to_owned(),
            status: status.to_owned(),
            note: None,
            details,
        }
    }

    #[test]
    fn codex_windows_read_weekly_first_with_remaining_share() {
        let read = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({"rateLimits": {
                "primary": {"usedPercent": 12, "windowDurationMins": 300, "resetsAt": 1_000},
                "secondary": {"usedPercent": 40, "windowDurationMins": 10080}
            }})),
        );
        let windows = windows(&read);
        assert_eq!(windows[0].label, "Weekly limit");
        assert_eq!(windows[0].remaining, 60);
        assert_eq!(windows[1].label, "5-hour limit");
        assert_eq!(windows[1].remaining, 88);
        assert_eq!(windows[1].resets_at_ms, Some(1_000_000));
        assert_eq!(
            windows[1].reset_note(1_000_000 - 130 * 60_000),
            "Resets in 2 h 10 min."
        );
    }

    #[test]
    fn muse_windows_and_unsupported_reads() {
        let read = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({"usage": {
                "window": {"usedPercent": 1, "windowDurationMins": 300, "resetsAtMs": 5},
                "weekly": {"usedPercent": 101, "resetsAtMs": 6}
            }})),
        );
        let windows = windows(&read);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].label, "Weekly limit");
        assert_eq!(windows[0].remaining, 0);
        assert!(super::windows(&reply(HARNESS_QUOTA_UNSUPPORTED, None)).is_empty());
    }
}
