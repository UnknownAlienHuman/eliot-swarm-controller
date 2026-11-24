//! Shared interpretation of usage windows emitted by quota readers.

use eliot_core::api::harness::{QuotaHeadroom, QuotaMapping, QuotaProjection, QuotaWindowBounds};
use serde_json::Value;

pub type WindowGroups = &'static [(&'static str, &'static [&'static str])];

pub fn observe(headroom: &mut QuotaHeadroom, window: &Value, now: i64, weekly: bool) {
    let Some(used) = window_used_percent(window)
        .filter(|_| window_reset_ms(window).is_none_or(|reset| reset > now))
    else {
        return;
    };
    headroom.max_used = Some(headroom.max_used.map_or(used, |prior| prior.max(used)));
    if weekly
        || window.get("windowDurationMins").and_then(Value::as_u64) == Some(10_080)
        || window
            .get("label")
            .and_then(Value::as_str)
            .and_then(|label| label.split_ascii_whitespace().last())
            .is_some_and(|period| period.eq_ignore_ascii_case("weekly"))
    {
        headroom.weekly_used = Some(headroom.weekly_used.map_or(used, |prior| prior.max(used)));
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
    window_values(details)
        .filter_map(|window| {
            let label = window["label"].as_str()?;
            window_family(label).is_none().then_some(())?;
            usage_bound(label, window, None, now)
        })
        .collect()
}

pub fn window_values(details: &Value) -> impl Iterator<Item = &Value> {
    details["windows"].as_array().into_iter().flatten()
}

/// Read the normalized windows contributed by an adapter's native reader.
pub fn project_windows(
    details: &Value,
    groups: WindowGroups,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    let windows = details["windows"]
        .as_array()
        .filter(|windows| !windows.is_empty())?;
    Some(windows_projection(
        windows.iter(),
        groups,
        true,
        model,
        now,
        "windows",
    ))
}

/// Full with a future (or unknown) reset: without reset evidence the window
/// may already have rolled, so callers treat it as exhausted only when the
/// provider gives no earlier reset.
pub fn window_exhausted(window: &Value, now: i64) -> bool {
    window_used_percent(window).is_some_and(|used| used >= 100.0)
        && window_reset_ms(window).is_none_or(|reset| reset > now)
}

/// Affirmative headroom: below 100% with a future reset. The future reset
/// is required: a full or reset-less window is never availability evidence.
pub fn window_headroom(window: &Value, now: i64) -> bool {
    window_used_percent(window).is_some_and(|used| used < 100.0)
        && window_reset_ms(window).is_some_and(|reset| reset > now)
}

/// Provider window-label periods emitted by the readers (`5-hour`, `Weekly`,
/// `Monthly`, Antigravity `quota`). A label that is exactly a period, or
/// carries no label at all, is account scope.
const WINDOW_LABEL_PERIODS: &[&str] = &["weekly", "5-hour", "monthly", "quota"];

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
fn window_family(label: &str) -> Option<String> {
    let lower = label.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    for period in WINDOW_LABEL_PERIODS {
        if lower == *period {
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
pub fn window_mapping(window: &Value, groups: WindowGroups, model: Option<&str>) -> QuotaMapping {
    let label = window.get("label").and_then(Value::as_str).unwrap_or("");
    if let Some(model) = model.filter(|model| !model.trim().is_empty())
        && label.eq_ignore_ascii_case(model.trim())
    {
        return QuotaMapping::ExactId;
    }
    let family = window_family(label);
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

pub fn windows_projection<'a>(
    windows: impl IntoIterator<Item = &'a Value>,
    groups: WindowGroups,
    complete: bool,
    model: Option<&str>,
    now: i64,
    provenance: &'static str,
) -> QuotaProjection {
    let applicable: Vec<_> = windows
        .into_iter()
        .map(|window| (window, window_mapping(window, groups, model)))
        .filter(|(_, mapping)| *mapping != QuotaMapping::Unmapped)
        .collect();
    summarize_windows(&applicable, complete, now, provenance)
}

pub fn summarize_windows(
    applicable: &[(&Value, QuotaMapping)],
    complete: bool,
    now: i64,
    provenance: &'static str,
) -> QuotaProjection {
    if applicable.is_empty() {
        // Windows exist but none maps to this model (for example an
        // unrecognized `Model group` label, or an Argon bucket read for a
        // Hydrogen model): report the inert unmapped projection so callers
        // stay unknown instead of skipping a route or asserting availability.
        return QuotaProjection {
            exhausted: false,
            available: false,
            mapping: QuotaMapping::Unmapped,
            provenance,
            headroom: QuotaHeadroom::default(),
        };
    }
    let exhausted = applicable
        .iter()
        .any(|(window, _)| window_exhausted(window, now));
    // The decisive bucket drives the verdict: the first exhausted window
    // when anything is exhausted (preferring an exact id), else the exact
    // id when one matched, else the first applicable group window.
    let mapping = applicable
        .iter()
        .filter(|(window, _)| window_exhausted(window, now) == exhausted)
        .map(|(_, mapping)| *mapping)
        .find(|mapping| *mapping == QuotaMapping::ExactId)
        .or_else(|| {
            applicable
                .iter()
                .filter(|(window, _)| window_exhausted(window, now) == exhausted)
                .map(|(_, mapping)| *mapping)
                .next()
        })
        .unwrap_or(applicable[0].1);
    let mut headroom = QuotaHeadroom::default();
    for (window, _) in applicable {
        observe(&mut headroom, window, now, false);
    }
    QuotaProjection {
        exhausted,
        available: complete
            && applicable
                .iter()
                .all(|(window, _)| window_headroom(window, now)),
        mapping,
        provenance,
        headroom,
    }
}
