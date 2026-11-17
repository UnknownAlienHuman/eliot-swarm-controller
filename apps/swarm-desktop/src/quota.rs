//! Quota windows from `harness.quota` for the Usage page's rings and the
//! composer's usage indicator. `details` is provider-verbatim, so windows
//! are found by shape, at most three levels deep:
//!
//! * Codex `rateLimits.primary/secondary` and Muse `usage.window/weekly`
//!   (`usedPercent`, `windowDurationMins`, `resetsAt` seconds or
//!   `resetsAtMs`), as `plugins/codex/src/discovery.rs`
//!   `project_rate_limits` and `plugins/muse/src/native/types.rs` project
//!   them;
//! * the Anthropic OAuth usage buckets k92 reads for Claude Code
//!   (`five_hour`, `seven_day`, model-scoped `seven_day_<model>`:
//!   `utilization` percent, `resets_at` RFC 3339; OpenCodex
//!   `vendor-probes-oauth.ts` as the pattern);
//! * Google-style model buckets (`remainingFraction`, `resetTime`) and
//!   `windows: [{label|model, usedPercent, …}]` lists.
//!
//! A window's duration comes from `windowDurationMins` or its key
//! (`five_hour`, `seven_day*`, `weekly`); its name from `label` (Claude
//! Code and Antigravity name their windows: "Weekly", "Fable weekly") or
//! else its duration; a model scope from `model`, `modelId` or a
//! `seven_day_<model>` key. A harness whose read is not `supported` has no
//! windows. Each window reads as one line, like Codex's usage popover:
//! "Weekly · 43% left · resets in 5 days".

use eliot_core::api::HarnessQuotaResponse;
use eliot_core::api::harness::HARNESS_QUOTA_SUPPORTED;
use serde_json::Value;

const WEEK_MINS: i64 = 7 * 24 * 60;
const DAY_MINS: i64 = 24 * 60;

/// One quota window: what it is, the share remaining and when it resets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    /// Its name: "Weekly", "5-hour", "Fable weekly", "Weekly · Opus".
    pub label: String,
    /// Percent remaining, 0–100.
    pub remaining: i64,
    pub resets_at_ms: Option<i64>,
    duration_mins: Option<i64>,
    /// The model (group) the window limits, when it is model-scoped.
    pub model: Option<String>,
    /// Limits part of the account (a model scope, or a named window that
    /// is not the plain duration): listed after the account-wide ones.
    scoped: bool,
}

impl Window {
    /// "resets in 2 h 10 min", or that the time is not reported.
    pub fn resets_in(&self, now_ms: i64) -> String {
        let Some(at) = self.resets_at_ms else {
            return "reset time not reported".to_owned();
        };
        let mins = (at - now_ms).max(0) / 60_000;
        match mins {
            0 => "resets now".to_owned(),
            m if m < 60 => format!("resets in {m} min"),
            m if m < DAY_MINS => format!("resets in {} h {} min", m / 60, m % 60),
            m => {
                let days = (m + DAY_MINS - 1) / DAY_MINS;
                format!(
                    "resets in {days} {}",
                    if days == 1 { "day" } else { "days" }
                )
            }
        }
    }

    /// One concise line: "Weekly · 43% left · resets in 5 days".
    pub fn line(&self, now_ms: i64) -> String {
        format!(
            "{} · {}% left · {}",
            self.label,
            self.remaining,
            self.resets_in(now_ms)
        )
    }
}

/// What a read without windows says, in plain words: "Shown after a Muse
/// turn" until Muse reports usage, "OpenCode: provider refused the quota
/// read (HTTP 403)", or why the harness has no quota. `label` names the
/// harness.
pub fn status_line(label: &str, reply: &HarnessQuotaResponse) -> String {
    let note = reply.note.as_deref().unwrap_or("").trim();
    let details = reply.details.as_ref();
    let failed = details
        .and_then(|details| details.get("state"))
        .and_then(Value::as_str)
        == Some("error");
    if failed {
        let provider = details
            .and_then(|details| details.get("origin"))
            .and_then(Value::as_str)
            == Some("provider");
        return match (http_status(note), provider) {
            (Some(code @ (401 | 403)), _) => {
                format!("{label}: provider refused the quota read (HTTP {code})")
            }
            (Some(code), _) => format!("{label}: provider quota read failed (HTTP {code})"),
            (None, true) => format!("{label}: provider quota read failed"),
            (None, false) => format!("{label}: quota not read, the harness could not be asked"),
        };
    }
    if reply.harness == "muse" && reply.status != HARNESS_QUOTA_SUPPORTED {
        return "Shown after a Muse turn".to_owned();
    }
    if reply.status == eliot_core::api::harness::HARNESS_QUOTA_UNSUPPORTED {
        return if note.is_empty() {
            format!("{label}: no quota read is supported")
        } else {
            format!("{label}: not supported ({})", note.trim_end_matches('.'))
        };
    }
    format!("{label}: no quota windows reported")
}

/// "as of 14:05" (local time) for a read observed during a turn (Muse),
/// whose values age until the next turn.
pub fn observed_note(reply: &HarnessQuotaResponse) -> Option<String> {
    use chrono::TimeZone;
    if reply.harness != "muse" || reply.status != HARNESS_QUOTA_SUPPORTED {
        return None;
    }
    chrono::Local
        .timestamp_millis_opt(reply.fetched_at_ms)
        .single()
        .map(|at| format!("as of {}", at.format("%H:%M")))
}

/// The first `HTTP <3 digits>` status in a note.
fn http_status(note: &str) -> Option<u16> {
    let at = note.find("HTTP ")? + "HTTP ".len();
    let digits: String = note[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    (digits.len() == 3).then(|| digits.parse().ok()).flatten()
}

/// The windows of a supported quota read: account-wide first, longest
/// first (weekly, then the 5-hour window, as Antigravity shows them), then
/// the model-scoped ones.
pub fn windows(reply: &HarnessQuotaResponse) -> Vec<Window> {
    if reply.status != HARNESS_QUOTA_SUPPORTED {
        return Vec::new();
    }
    let Some(details) = reply.details.as_ref() else {
        return Vec::new();
    };
    let mut found = Vec::new();
    scan(details, "", 0, &mut found);
    // Codex reports its windows twice (`rateLimits` and
    // `rateLimitsByLimitId.<id>`): one row per distinct window.
    let mut seen = std::collections::HashSet::new();
    found.retain(|window| {
        seen.insert((window.label.clone(), window.remaining, window.resets_at_ms))
    });
    found.sort_by_key(|window| {
        (
            window.scoped,
            std::cmp::Reverse(window.duration_mins.unwrap_or(0)),
        )
    });
    found.truncate(MAX_WINDOWS);
    found
}

/// The tightest window (least remaining) of a read, for the composer.
pub fn tightest(reply: &HarnessQuotaResponse) -> Option<Window> {
    windows(reply)
        .into_iter()
        .min_by_key(|window| window.remaining)
}

/// Most windows one read shows.
const MAX_WINDOWS: usize = 16;

fn scan(value: &Value, key: &str, depth: usize, out: &mut Vec<Window>) {
    if depth > 3 || out.len() >= MAX_WINDOWS {
        return;
    }
    match value {
        Value::Object(map) => {
            if let Some(window) = window(value, key) {
                out.push(window);
                return;
            }
            for (child_key, child) in map {
                scan(child, child_key, depth + 1, out);
            }
        }
        Value::Array(items) => {
            for item in items.iter().take(MAX_WINDOWS) {
                scan(item, key, depth + 1, out);
            }
        }
        _ => {}
    }
}

/// One window object, by shape (see the module docs); `key` is the field
/// it was found under.
fn window(value: &Value, key: &str) -> Option<Window> {
    let number = |name: &str| value.get(name).and_then(Value::as_f64);
    let used = number("usedPercent")
        .or_else(|| number("utilization"))
        .or_else(|| number("remainingFraction").map(|left| 100.0 - left * 100.0))
        .or_else(|| number("remainingPercent").map(|left| 100.0 - left))?;
    let duration_mins = value
        .get("windowDurationMins")
        .and_then(Value::as_i64)
        .or_else(|| key_duration(key));
    let resets_at_ms = [
        "resetsAtMs",
        "resetsAt",
        "resets_at",
        "resetTime",
        "reset_time",
    ]
    .iter()
    .find_map(|name| value.get(*name).and_then(timestamp_ms));
    let model = ["model", "modelId"]
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(str::to_owned)
        .or_else(|| key.strip_prefix("seven_day_").map(model_name));
    let plain = label(duration_mins);
    let named = value
        .get("label")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty());
    let mut label = named.map_or_else(|| plain.clone(), str::to_owned);
    if let Some(model) = &model {
        label = format!("{label} · {model}");
    }
    Some(Window {
        scoped: model.is_some() || !label.eq_ignore_ascii_case(&plain),
        label,
        remaining: (100.0 - used).round().clamp(0.0, 100.0) as i64,
        resets_at_ms,
        duration_mins,
        model,
    })
}

/// A window's duration from the key it was found under.
fn key_duration(key: &str) -> Option<i64> {
    match key {
        "five_hour" | "fiveHour" | "window" => Some(5 * 60),
        "weekly" | "seven_day" | "sevenDay" => Some(WEEK_MINS),
        key if key.starts_with("seven_day_") => Some(WEEK_MINS),
        _ => None,
    }
}

/// `opus` → `Opus`, `sonnet_4` → `Sonnet 4`.
fn model_name(raw: &str) -> String {
    raw.split('_')
        .map(|word| {
            let mut chars = word.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Seconds or milliseconds since the epoch, or an RFC 3339 time, as ms.
fn timestamp_ms(value: &Value) -> Option<i64> {
    if let Some(at) = value.as_i64() {
        // Codex reports seconds since the epoch; Muse milliseconds.
        return Some(if at < 100_000_000_000 { at * 1000 } else { at });
    }
    let text = value.as_str()?;
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|at| at.timestamp_millis())
}

/// A window's name from its duration ("Weekly", "5-hour").
fn label(duration_mins: Option<i64>) -> String {
    match duration_mins {
        Some(WEEK_MINS) => "Weekly".to_owned(),
        Some(mins) if mins > 0 && mins % DAY_MINS == 0 => format!("{}-day", mins / DAY_MINS),
        Some(mins) if mins > 0 && mins % 60 == 0 => format!("{}-hour", mins / 60),
        Some(mins) if mins > 0 => format!("{mins}-minute"),
        _ => "Usage".to_owned(),
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
        assert_eq!(windows[0].label, "Weekly");
        assert_eq!(windows[0].remaining, 60);
        assert_eq!(windows[1].label, "5-hour");
        assert_eq!(windows[1].remaining, 88);
        assert_eq!(windows[1].resets_at_ms, Some(1_000_000));
        assert_eq!(
            windows[1].line(1_000_000 - 130 * 60_000),
            "5-hour · 88% left · resets in 2 h 10 min"
        );
        assert_eq!(
            windows[0].line(0),
            "Weekly · 60% left · reset time not reported"
        );
    }

    #[test]
    fn named_windows_read_once_with_their_own_name() {
        // k98: "Weekly limit · Weekly remaining" — Claude Code and
        // Antigravity name their windows in `label`.
        let now: i64 = 1_794_873_856_622;
        let read = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({"windows": [
                {"label": "5-hour", "usedPercent": 40.0, "windowDurationMins": 300, "resetsAtMs": now + 3_600_000},
                {"label": "Weekly", "usedPercent": 57.0, "windowDurationMins": 10080, "resetsAtMs": now + 5 * 86_400_000},
                {"label": "Fable weekly", "usedPercent": 11.0, "windowDurationMins": 10080}
            ]})),
        );
        let windows = windows(&read);
        let labels: Vec<&str> = windows.iter().map(|window| window.label.as_str()).collect();
        assert_eq!(labels, ["Weekly", "5-hour", "Fable weekly"]);
        assert_eq!(windows[0].line(now), "Weekly · 43% left · resets in 5 days");
        assert_eq!(
            windows[1].line(now),
            "5-hour · 60% left · resets in 1 h 0 min"
        );
        assert!(windows.iter().all(|window| window.model.is_none()));
    }

    #[test]
    fn reads_without_windows_say_why_plainly() {
        let mut refused = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(
                json!({"state": "error", "origin": "provider", "error": "quota read failed; no values available"}),
            ),
        );
        refused.harness = "opencode".to_owned();
        refused.note = Some("quota.read: provider quota returned HTTP 403".to_owned());
        assert_eq!(
            status_line("OpenCode", &refused),
            "OpenCode: provider refused the quota read (HTTP 403)"
        );
        let mut muse = reply(HARNESS_QUOTA_UNSUPPORTED, None);
        muse.harness = "muse".to_owned();
        muse.note = Some("Muse MSP usage/read has no observed subscription usage".to_owned());
        assert_eq!(status_line("Muse", &muse), "Shown after a Muse turn");
        assert!(observed_note(&muse).is_none());
        muse.status = HARNESS_QUOTA_SUPPORTED.to_owned();
        muse.fetched_at_ms = 1_794_873_856_622;
        assert!(observed_note(&muse).unwrap().starts_with("as of "));
        let mut other = reply(HARNESS_QUOTA_UNSUPPORTED, None);
        other.note = Some("no read-only quota surface.".to_owned());
        assert_eq!(
            status_line("Codex", &other),
            "Codex: not supported (no read-only quota surface)"
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
        assert_eq!(windows[0].label, "Weekly");
        assert_eq!(windows[0].remaining, 0);
        assert!(super::windows(&reply(HARNESS_QUOTA_UNSUPPORTED, None)).is_empty());
    }

    #[test]
    fn a_window_reported_twice_shows_once() {
        let window =
            json!({"usedPercent": 20, "windowDurationMins": 10080, "resetsAt": 1_795_288_252});
        let read = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({
                "rateLimits": {"limitId": "codex", "primary": window.clone()},
                "rateLimitsByLimitId": {"codex": {"limitId": "codex", "primary": window}}
            })),
        );
        let windows = windows(&read);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].remaining, 80);
    }

    #[test]
    fn claude_buckets_and_model_scoped_windows_read_by_shape() {
        let read = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({
                "five_hour": {"utilization": 23.0, "resets_at": "2026-11-16T20:00:00+00:00"},
                "seven_day": {"utilization": 41.5, "resets_at": null},
                "seven_day_opus": {"utilization": 90.0}
            })),
        );
        let windows = windows(&read);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].label, "Weekly");
        assert_eq!(windows[0].remaining, 59);
        assert_eq!(windows[1].label, "5-hour");
        assert_eq!(windows[1].remaining, 77);
        assert!(windows[1].resets_at_ms.is_some());
        assert_eq!(windows[2].label, "Weekly · Opus");
        assert_eq!(tightest(&read).unwrap().remaining, 10);
        let google = reply(
            HARNESS_QUOTA_SUPPORTED,
            Some(json!({"models": [
                {"model": "Gemini models", "remainingFraction": 0.99, "windowDurationMins": 300}
            ]})),
        );
        let windows = super::windows(&google);
        assert_eq!(windows[0].label, "5-hour · Gemini models");
        assert_eq!(windows[0].remaining, 99);
    }
}
