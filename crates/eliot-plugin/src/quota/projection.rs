//! Shared interpretation of usage windows emitted by quota readers.

use eliot_core::api::harness::{QuotaEvidence, QuotaMapping, QuotaWindowBounds, QuotaWindowFacts};
use serde_json::Value;

pub type WindowGroups = &'static [(&'static str, &'static [&'static str])];

/// Decode the shared window fields emitted by readers, including legacy Unix-second resets.
pub fn window_facts(window: &Value) -> QuotaWindowFacts {
    QuotaWindowFacts {
        used_percent: window_used_percent(window),
        reset_ms: window_reset_ms(window),
        weekly: window.get("windowDurationMins").and_then(Value::as_u64) == Some(10_080)
            || window
                .get("label")
                .and_then(Value::as_str)
                .and_then(|label| label.split_ascii_whitespace().last())
                .is_some_and(|period| period.eq_ignore_ascii_case("weekly")),
    }
}

fn window_used_percent(window: &Value) -> Option<f64> {
    window
        .get("usedPercent")
        .and_then(Value::as_f64)
        .filter(|used| used.is_finite() && *used >= 0.0)
}

fn window_reset_ms(window: &Value) -> Option<i64> {
    window
        .get("resetsAtMs")
        .and_then(Value::as_i64)
        .filter(|reset| *reset >= 0)
        .or_else(|| {
            window
                .get("resetsAt")
                .and_then(Value::as_i64)
                .filter(|reset| *reset >= 0)
                .and_then(|reset| reset.checked_mul(1000))
        })
}

/// Only account-wide, current windows with explicit bounds can be joined to Swarm usage.
pub fn usage_bound(
    label: &str,
    window: &Value,
    default_duration_mins: Option<i64>,
    now: i64,
) -> Option<QuotaWindowBounds> {
    let resets = window_reset_ms(window)?;
    let minutes = window["windowDurationMins"]
        .as_i64()
        .or(default_duration_mins)?;
    if window_used_percent(window).is_none() || minutes <= 0 || resets <= now {
        return None;
    }
    let starts_at_ms = resets.checked_sub(minutes.checked_mul(60_000)?)?;
    (0..now).contains(&starts_at_ms).then(|| QuotaWindowBounds {
        label: label.to_owned(),
        starts_at_ms: Some(starts_at_ms),
        through_ms: now,
    })
}

pub fn account_usage_windows(details: &Value, now: i64) -> Vec<QuotaWindowBounds> {
    account_usage_windows_with_periods(details, now, &[])
}

pub fn account_usage_windows_with_periods(
    details: &Value,
    now: i64,
    extra_periods: &[&str],
) -> Vec<QuotaWindowBounds> {
    details["windows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|window| {
            let label = window["label"].as_str()?;
            if window_family(label, extra_periods).is_some() {
                return None;
            }
            usage_bound(label, window, None, now)
        })
        .collect()
}

pub fn window_evidence(
    details: &Value,
    groups: WindowGroups,
    extra_periods: &[&str],
    model: Option<&str>,
) -> Option<QuotaEvidence> {
    let windows = details["windows"]
        .as_array()
        .filter(|windows| !windows.is_empty())?;
    Some(QuotaEvidence::Windows {
        applicable: windows
            .iter()
            .filter_map(|window| mapped_window(window, groups, extra_periods, model))
            .collect(),
        complete: true,
    })
}

fn documented_group(
    groups: WindowGroups,
    family: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    groups
        .iter()
        .find(|(name, _)| *name == family)
        .map(|(name, tokens)| (*name, *tokens))
}

/// The provider group named by a window label, or `None` for account-scope
/// (bare-period) labels. An unrecognized family yields `Some` of itself so
/// the caller can report it unmapped.
fn window_family(label: &str, extra_periods: &[&str]) -> Option<String> {
    let lower = label.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    for period in ["weekly", "5-hour", "monthly"]
        .into_iter()
        .chain(extra_periods.iter().copied())
    {
        if lower == period {
            return None;
        }
        if let Some(family) = lower.strip_suffix(period) {
            let family = family.trim().to_owned();
            if !family.is_empty() {
                return Some(family);
            }
        }
    }
    Some(lower)
}

fn model_token_present(model: &str, token: &str) -> bool {
    model
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|part| part.eq_ignore_ascii_case(token))
}

/// How one window maps to the requested model: an exact label match, a
/// documented provider group the model belongs to, the unlabeled account
/// scope, or unmapped (an unrecognized provider label, or a recognized group
/// the model is not a member of). An unrecognized label applies to no model,
/// even on an unscoped read.
fn window_mapping(
    window: &Value,
    groups: WindowGroups,
    extra_periods: &[&str],
    model: Option<&str>,
) -> QuotaMapping {
    let label = window.get("label").and_then(Value::as_str).unwrap_or("");
    if let Some(model) = model.filter(|model| !model.trim().is_empty())
        && label.eq_ignore_ascii_case(model.trim())
    {
        return QuotaMapping::ExactId;
    }
    let family = window_family(label, extra_periods);
    let Some(family) = family.as_deref() else {
        return QuotaMapping::Group("account");
    };
    let Some((group, tokens)) = documented_group(groups, family) else {
        return QuotaMapping::Unmapped;
    };
    match model {
        Some(model) if tokens.iter().any(|token| model_token_present(model, token)) => {
            QuotaMapping::Group(group)
        }
        _ => QuotaMapping::Unmapped,
    }
}

pub fn mapped_window(
    window: &Value,
    groups: WindowGroups,
    extra_periods: &[&str],
    model: Option<&str>,
) -> Option<(QuotaWindowFacts, QuotaMapping)> {
    let mapping = window_mapping(window, groups, extra_periods, model);
    (mapping != QuotaMapping::Unmapped).then(|| (window_facts(window), mapping))
}
