//! Calendar recurrence evaluation for manager-owned automations.
//!
//! This module owns only calendar semantics. The Store owns enabled state,
//! current authorization, durable occurrence disposition, and invocation.

use super::ScheduleAction;
use crate::{
    error::{Error, Result},
    model,
};
use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;

pub(crate) const MAX_PREVIEW_OCCURRENCES: usize = 64;
const MAX_EXPRESSION_BYTES: usize = 256;
const MAX_TIMEZONE_BYTES: usize = 128;

/// The fields that define a calendar generation. The timezone is an explicit
/// IANA name; `anchor_ms` is the inclusive lower boundary for this calendar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CalendarDefinition {
    pub(crate) expression: String,
    pub(crate) timezone: String,
    pub(crate) anchor_ms: i64,
}

/// A manager-owned recurring CheckRun selection. Enabled state belongs to the
/// containing `AutomationEntry`, so this value intentionally has no switch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CronSettings {
    pub(crate) calendar: CalendarDefinition,
    pub(crate) action: ScheduleAction,
}

/// A due occurrence is identified by its intended UTC instant, not by the
/// time at which the scheduler happened to observe it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CalendarOccurrence {
    pub(crate) due_at_ms: i64,
}

struct ParsedCalendar {
    cron: Cron,
    timezone: Tz,
}

fn parse_calendar(definition: &CalendarDefinition) -> Result<ParsedCalendar> {
    if definition.expression.trim().is_empty()
        || definition.expression.len() > MAX_EXPRESSION_BYTES
        || definition.timezone.trim().is_empty()
        || definition.timezone.len() > MAX_TIMEZONE_BYTES
        || definition.anchor_ms < 0
    {
        return Err(Error::invalid(
            "calendar requires a bounded cron expression, IANA timezone, and nonnegative anchor",
        ));
    }
    let cron = Cron::from_str(&definition.expression)
        .map_err(|error| Error::invalid(format!("invalid cron expression: {error}")))?;
    let timezone = Tz::from_str(&definition.timezone)
        .map_err(|_| Error::invalid("calendar timezone must be a valid IANA timezone name"))?;
    Ok(ParsedCalendar { cron, timezone })
}

/// Validate cron syntax and timezone names through the maintained evaluator
/// and timezone database before an entry can be saved or enabled.
pub(crate) fn validate(definition: &CalendarDefinition) -> Result<()> {
    parse_calendar(definition).map(|_| ())
}

pub(crate) fn validate_settings(settings: &CronSettings) -> Result<()> {
    validate(&settings.calendar)?;
    super::validate_schedules(&[super::ScheduleConfig {
        schedule_id: "cron-settings-validation".to_owned(),
        enabled: false,
        anchor_ms: settings.calendar.anchor_ms,
        period_ms: None,
        action: settings.action.clone(),
    }])
}

/// Stable generation identity for calendar semantics only. Action choices,
/// labels, revisions, and enabled state do not create another recurrence.
pub(crate) fn generation_digest(definition: &CalendarDefinition) -> Result<String> {
    validate(definition)?;
    let value = json!({
        "calendar_schema_version": 1,
        "expression": definition.expression,
        "timezone": definition.timezone,
        "anchor_ms": definition.anchor_ms,
    });
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

/// Stable identity for one intended calendar occurrence. The terminal
/// manager and configuration revision are deliberately absent: ownership
/// transfer changes the current executor, but cannot turn the same logical
/// occurrence into new work.
pub(crate) fn occurrence_id(
    origin_manager_id: &str,
    project_id: &str,
    automation_id: &str,
    generation: &str,
    due_at_ms: i64,
) -> Result<String> {
    if due_at_ms < 0
        || origin_manager_id.is_empty()
        || project_id.is_empty()
        || automation_id.is_empty()
        || generation.is_empty()
    {
        return Err(Error::invalid("cron occurrence identity is incomplete"));
    }
    let value = json!({
        "cron_occurrence_schema_version": 1,
        "origin_manager_id": origin_manager_id,
        "project_id": project_id,
        "automation_id": automation_id,
        "calendar_generation": generation,
        "due_at_ms": due_at_ms,
    });
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

fn local_second_at(parsed: &ParsedCalendar, epoch_ms: i64) -> Result<chrono::DateTime<Tz>> {
    let utc = Utc
        .timestamp_millis_opt(epoch_ms)
        .single()
        .ok_or_else(|| Error::invalid("calendar timestamp is outside the supported date range"))?;
    utc.with_timezone(&parsed.timezone)
        .with_nanosecond(0)
        .ok_or_else(|| Error::invalid("calendar timestamp could not be rounded to a second"))
}

fn occurrence_epoch_ms(occurrence: &chrono::DateTime<Tz>) -> i64 {
    occurrence.with_timezone(&Utc).timestamp_millis()
}

/// Return the newest occurrence due at or before `now_ms`. Missed occurrences
/// collapse to this one logical slot. The Store advances its cursor only in
/// the same transaction that retains an invocation or a terminal disposition.
pub(crate) fn latest_due(
    definition: &CalendarDefinition,
    now_ms: i64,
    last_considered_ms: Option<i64>,
) -> Result<Option<CalendarOccurrence>> {
    if now_ms < definition.anchor_ms {
        return Ok(None);
    }
    let parsed = parse_calendar(definition)?;
    let start = local_second_at(&parsed, now_ms)?;
    let occurrence = parsed
        .cron
        .find_previous_occurrence(&start, true)
        .map_err(|error| Error::new("CRON_EVALUATION", error.to_string()))?;
    let due_at_ms = occurrence_epoch_ms(&occurrence);
    if due_at_ms < definition.anchor_ms || last_considered_ms.is_some_and(|last| due_at_ms <= last)
    {
        return Ok(None);
    }
    Ok(Some(CalendarOccurrence { due_at_ms }))
}

fn first_occurrence_at_or_after(
    parsed: &ParsedCalendar,
    lower_bound_ms: i64,
    inclusive: bool,
) -> Result<i64> {
    let mut cursor = local_second_at(parsed, lower_bound_ms)?;
    // Cron occurrences have whole-second precision. An inclusive millisecond
    // boundary such as 1001ms cannot include the floored 1000ms occurrence;
    // starting the maintained evaluator inclusively there would return an
    // occurrence that we must reject. Begin strictly after the floor instead.
    let mut include_cursor = inclusive && occurrence_epoch_ms(&cursor) == lower_bound_ms;
    for _ in 0..2 {
        let occurrence = parsed
            .cron
            .find_next_occurrence(&cursor, include_cursor)
            .map_err(|error| Error::new("CRON_EVALUATION", error.to_string()))?;
        let due_at_ms = occurrence_epoch_ms(&occurrence);
        let accepted = if inclusive {
            due_at_ms >= lower_bound_ms
        } else {
            due_at_ms > lower_bound_ms
        };
        if accepted {
            return Ok(due_at_ms);
        }
        if due_at_ms <= occurrence_epoch_ms(&cursor) {
            return Err(Error::new(
                "CRON_EVALUATION",
                "cron evaluator did not advance to a later occurrence",
            ));
        }
        cursor = occurrence;
        include_cursor = false;
    }
    Err(Error::new(
        "CRON_EVALUATION",
        "cron evaluator could not find an occurrence at the calendar boundary",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn epoch_ms(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_millis()
    }

    fn calendar(expression: &str, timezone: &str, anchor_ms: i64) -> CalendarDefinition {
        CalendarDefinition {
            expression: expression.into(),
            timezone: timezone.into(),
            anchor_ms,
        }
    }

    #[test]
    fn non_second_aligned_anchor_advances_past_its_floored_second() {
        let definition = calendar("* * * * * *", "UTC", 1_001);
        assert_eq!(next_due_at_ms(&definition, 1_000).unwrap(), Some(2_000));
    }

    #[test]
    fn fixed_local_time_in_spring_gap_uses_first_valid_time_after_gap() {
        let definition = calendar("0 30 2 * * *", "America/New_York", 0);
        assert_eq!(
            next_due_at_ms(&definition, epoch_ms("2024-03-10T06:59:00Z")).unwrap(),
            Some(epoch_ms("2024-03-10T07:00:00Z"))
        );
    }

    #[test]
    fn fixed_local_time_in_fall_overlap_occurs_only_once() {
        let definition = calendar("0 30 1 * * *", "America/New_York", 0);
        assert_eq!(
            next_due_at_ms(&definition, epoch_ms("2024-11-03T04:59:00Z")).unwrap(),
            Some(epoch_ms("2024-11-03T05:30:00Z"))
        );
    }
}

/// Return the first future occurrence, or the first occurrence at/after the
/// calendar anchor when the anchor has not yet arrived.
pub(crate) fn next_due_at_ms(definition: &CalendarDefinition, now_ms: i64) -> Result<Option<i64>> {
    let lower_bound_ms = now_ms.max(definition.anchor_ms);
    let inclusive = now_ms < definition.anchor_ms;
    let parsed = parse_calendar(definition)?;
    first_occurrence_at_or_after(&parsed, lower_bound_ms, inclusive).map(Some)
}

/// Preview a bounded list of upcoming occurrences, beginning strictly after
/// `after_ms` and respecting the calendar's inclusive activation anchor.
pub(crate) fn preview_next_occurrences(
    definition: &CalendarDefinition,
    after_ms: i64,
    limit: usize,
) -> Result<Vec<CalendarOccurrence>> {
    if !(1..=MAX_PREVIEW_OCCURRENCES).contains(&limit) {
        return Err(Error::invalid(format!(
            "calendar preview limit must be between 1 and {MAX_PREVIEW_OCCURRENCES}"
        )));
    }
    let parsed = parse_calendar(definition)?;
    let mut result = Vec::with_capacity(limit);
    let lower_bound_ms = after_ms.max(definition.anchor_ms);
    let first_is_inclusive = after_ms < definition.anchor_ms;
    let mut next = first_occurrence_at_or_after(&parsed, lower_bound_ms, first_is_inclusive)?;
    result.push(CalendarOccurrence { due_at_ms: next });
    while result.len() < limit {
        let cursor = local_second_at(&parsed, next)?;
        let occurrence = parsed
            .cron
            .find_next_occurrence(&cursor, false)
            .map_err(|error| Error::new("CRON_EVALUATION", error.to_string()))?;
        next = occurrence_epoch_ms(&occurrence);
        result.push(CalendarOccurrence { due_at_ms: next });
    }
    Ok(result)
}
