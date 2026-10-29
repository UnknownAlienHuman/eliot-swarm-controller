//! Pure parsing and UTC-slot calculation for interval, one-shot, and cron timers.
//!
//! Cron parsing and civil occurrence search use Croner 4.0.2 from
//! <https://github.com/Hexagon/croner-rust/tree/v4.0.2> (MIT; inspected tag
//! commit `24c9bbb02dbcb498a8bbf31a33f1e6416e75ac83`). No donor source is copied.
//! Croner's built-in DST policy differs from the owning timer contract, so this
//! module searches its naive civil occurrences and resolves them with chrono-tz.

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, LocalResult, NaiveDateTime, TimeZone, Timelike,
    Utc,
};
use chrono_tz::Tz;
use croner::Cron;
use eliot_core::{Error, api::TimerSchedule};
use std::str::FromStr;

// Chrono offsets are bounded to less than 24 hours in either direction, so a
// local-time gap or overlap cannot span 48 hours.
const MAX_TIME_ZONE_SHIFT_SECONDS: i64 = 48 * 60 * 60;
const ONE_SECOND: ChronoDuration = ChronoDuration::seconds(1);

/// Parses a positive interval with humantime, retaining only exact millisecond
/// periods representable by the persisted signed epoch-millisecond schema.
pub(crate) fn parse_positive_duration_ms(value: &str) -> Result<i64, Error> {
    let duration = humantime::parse_duration(value)
        .map_err(|_| invalid("timer interval must be a positive duration"))?;
    let nanos = duration.as_nanos();
    const NANOS_PER_MILLI: u128 = 1_000_000;
    if nanos == 0 || nanos % NANOS_PER_MILLI != 0 {
        return Err(invalid(
            "timer interval must be a positive whole number of milliseconds",
        ));
    }
    i64::try_from(nanos / NANOS_PER_MILLI)
        .map_err(|_| invalid("timer interval is outside signed epoch-millisecond range"))
}

/// Parses an RFC3339 instant without truncating sub-millisecond precision.
pub(crate) fn parse_rfc3339_epoch_ms(value: &str) -> Result<i64, Error> {
    let instant = DateTime::parse_from_rfc3339(value)
        .map_err(|_| invalid("timer instant must be RFC3339"))?;
    let nanos = instant.timestamp_subsec_nanos();
    if nanos >= 1_000_000_000 || !nanos.is_multiple_of(1_000_000) {
        return Err(invalid(
            "timer instant must have whole-millisecond precision",
        ));
    }
    Ok(instant.timestamp_millis())
}

/// Builds an interval schedule. An omitted anchor remains unset for Store to
/// capture and persist once at admission.
pub(crate) fn parse_interval(every: &str, anchor: Option<i64>) -> Result<TimerSchedule, Error> {
    let schedule = TimerSchedule::Interval {
        period_ms: parse_positive_duration_ms(every)?,
        anchor,
    };
    validate_schedule(&schedule)?;
    Ok(schedule)
}

/// Builds a one-shot schedule from an exact-millisecond RFC3339 instant.
pub(crate) fn parse_once(at: &str) -> Result<TimerSchedule, Error> {
    let schedule = TimerSchedule::Once {
        at: parse_rfc3339_epoch_ms(at)?,
    };
    validate_schedule(&schedule)?;
    Ok(schedule)
}

/// Builds a five-field cron schedule using the caller's resolved time zone.
/// The owning definition parser supplies `UTC` when `tz` is omitted.
pub(crate) fn parse_cron(expression: &str, tz: &str) -> Result<TimerSchedule, Error> {
    let expression = expression.trim();
    let schedule = TimerSchedule::Cron {
        expression: expression.to_owned(),
        tz: tz.to_owned(),
    };
    validate_schedule(&schedule)?;
    Ok(schedule)
}

/// Validates the full grammar, including Croner's pattern parser and the IANA
/// time-zone name. Core's DTO validator checks only the wire shape.
pub(crate) fn validate_schedule(schedule: &TimerSchedule) -> Result<(), Error> {
    schedule.validate()?;
    match schedule {
        TimerSchedule::Interval { .. } | TimerSchedule::Once { .. } => Ok(()),
        TimerSchedule::Cron { expression, tz } => {
            parse_cron_expression(expression)?;
            parse_timezone(tz)?;
            Ok(())
        }
    }
}

/// Returns the first UTC epoch-millisecond slot strictly after the durable
/// `after_utc_ms` cursor. The cursor is not a wall-clock read; keeping it durable
/// prevents a clock rollback from replaying an already consumed slot.
pub(crate) fn next_after(
    schedule: &TimerSchedule,
    resolved_anchor_ms: i64,
    after_utc_ms: i64,
) -> Result<Option<i64>, Error> {
    match schedule {
        TimerSchedule::Interval { period_ms, anchor } => {
            validate_interval_anchor(*period_ms, *anchor, resolved_anchor_ms)?;
            next_interval_after(*period_ms, resolved_anchor_ms, after_utc_ms).map(Some)
        }
        TimerSchedule::Once { at } => Ok((*at > after_utc_ms).then_some(*at)),
        TimerSchedule::Cron { expression, tz } => {
            let cron = parse_cron_expression(expression)?;
            let tz = parse_timezone(tz)?;
            next_cron_after(&cron, tz, after_utc_ms)
        }
    }
}

/// Returns the latest slot in `(after_utc_ms, through_utc_ms]`. A reversed or
/// empty range has no due slot, which also makes backwards wall-clock movement
/// unable to replay work.
pub(crate) fn latest_due(
    schedule: &TimerSchedule,
    resolved_anchor_ms: i64,
    after_utc_ms: i64,
    through_utc_ms: i64,
) -> Result<Option<i64>, Error> {
    if after_utc_ms >= through_utc_ms {
        return Ok(None);
    }
    match schedule {
        TimerSchedule::Interval { period_ms, anchor } => {
            validate_interval_anchor(*period_ms, *anchor, resolved_anchor_ms)?;
            latest_interval_due(*period_ms, resolved_anchor_ms, after_utc_ms, through_utc_ms)
        }
        TimerSchedule::Once { at } => {
            Ok((*at > after_utc_ms && *at <= through_utc_ms).then_some(*at))
        }
        TimerSchedule::Cron { expression, tz } => {
            let cron = parse_cron_expression(expression)?;
            let tz = parse_timezone(tz)?;
            latest_cron_due(&cron, tz, after_utc_ms, through_utc_ms)
        }
    }
}

fn validate_interval_anchor(
    period_ms: i64,
    configured_anchor_ms: Option<i64>,
    resolved_anchor_ms: i64,
) -> Result<(), Error> {
    if period_ms <= 0 {
        return Err(invalid("timer interval period_ms must be positive"));
    }
    if configured_anchor_ms.is_some_and(|anchor| anchor != resolved_anchor_ms) {
        return Err(Error::new(
            "bad_state",
            "persisted timer anchor differs from the explicit schedule anchor",
        ));
    }
    Ok(())
}

fn next_interval_after(period_ms: i64, anchor_ms: i64, after_ms: i64) -> Result<i64, Error> {
    let period = i128::from(period_ms);
    let delta = i128::from(after_ms) - i128::from(anchor_ms);
    // Intervals start at k=1. `div_euclid` keeps the calculation correct for
    // negative Unix epochs, and the strict cursor advances an exact slot.
    let k = if delta < period {
        1
    } else {
        delta.div_euclid(period) + 1
    };
    interval_slot(anchor_ms, period_ms, k)
}

fn latest_interval_due(
    period_ms: i64,
    anchor_ms: i64,
    after_ms: i64,
    through_ms: i64,
) -> Result<Option<i64>, Error> {
    if period_ms <= 0 {
        return Err(invalid("timer interval period_ms must be positive"));
    }
    let period = i128::from(period_ms);
    let after_delta = i128::from(after_ms) - i128::from(anchor_ms);
    let through_delta = i128::from(through_ms) - i128::from(anchor_ms);
    let first_k = (after_delta.div_euclid(period) + 1).max(1);
    let last_k = through_delta.div_euclid(period);
    if last_k < first_k {
        return Ok(None);
    }
    interval_slot(anchor_ms, period_ms, last_k).map(Some)
}

fn interval_slot(anchor_ms: i64, period_ms: i64, k: i128) -> Result<i64, Error> {
    let slot = i128::from(anchor_ms) + i128::from(period_ms) * k;
    i64::try_from(slot)
        .map_err(|_| invalid("timer occurrence is outside signed epoch-millisecond range"))
}

fn parse_cron_expression(expression: &str) -> Result<Cron, Error> {
    let expression = expression.trim();
    if expression.split_whitespace().count() != 5 {
        return Err(invalid("timer cron must contain exactly five fields"));
    }
    Cron::from_str(expression).map_err(|_| invalid("timer cron expression is invalid"))
}

fn parse_timezone(name: &str) -> Result<Tz, Error> {
    if name.trim().is_empty() {
        return Err(invalid("timer cron time zone must be nonempty"));
    }
    Tz::from_str(name).map_err(|_| invalid("timer cron time zone is not a known IANA zone"))
}

fn next_cron_after(cron: &Cron, tz: Tz, after_ms: i64) -> Result<Option<i64>, Error> {
    let local_after = local_at_utc_ms(tz, after_ms)?;
    if matches!(
        tz.from_local_datetime(&local_after),
        LocalResult::Ambiguous(_, _)
    ) {
        let bounds = overlap_bounds(tz, local_after)?;
        if let Some(slot) = scan_overlap(cron, tz, bounds, FoldSearch::NextAfter { after_ms })? {
            return Ok(Some(slot));
        }
        return next_cron_from_local(cron, tz, bounds.end, true, after_ms).map(Some);
    }
    next_cron_from_local(cron, tz, local_after, false, after_ms).map(Some)
}

fn next_cron_from_local(
    cron: &Cron,
    tz: Tz,
    start: NaiveDateTime,
    inclusive: bool,
    after_ms: i64,
) -> Result<i64, Error> {
    let mut candidate = cron
        .find_next_occurrence(&start, inclusive)
        .map_err(|_| invalid("timer cron has no next occurrence in the supported range"))?;
    check_cron_year(candidate)?;
    loop {
        if let Some(slot) = first_slot_after(resolve_local(tz, candidate, false)?, after_ms) {
            return Ok(slot);
        }
        let previous = candidate;
        candidate = cron
            .find_next_occurrence(&previous, false)
            .map_err(|_| invalid("timer cron has no next occurrence in the supported range"))?;
        if candidate <= previous {
            return Err(Error::new(
                "source_integrity",
                "Croner returned a non-advancing occurrence",
            ));
        }
        check_cron_year(candidate)?;
    }
}

fn latest_cron_due(
    cron: &Cron,
    tz: Tz,
    after_ms: i64,
    through_ms: i64,
) -> Result<Option<i64>, Error> {
    let local_through = local_at_utc_ms(tz, through_ms)?;
    if matches!(
        tz.from_local_datetime(&local_through),
        LocalResult::Ambiguous(_, _)
    ) {
        let bounds = overlap_bounds(tz, local_through)?;
        if let Some(slot) = scan_overlap(
            cron,
            tz,
            bounds,
            FoldSearch::LatestDue {
                after_ms,
                through_ms,
            },
        )? {
            return Ok(Some(slot));
        }
        let before_fold = bounds
            .start
            .checked_sub_signed(ONE_SECOND)
            .ok_or_else(out_of_range)?;
        return latest_cron_before(cron, tz, before_fold, true, after_ms, through_ms);
    }
    latest_cron_before(cron, tz, local_through, true, after_ms, through_ms)
}

fn latest_cron_before(
    cron: &Cron,
    tz: Tz,
    start: NaiveDateTime,
    inclusive: bool,
    after_ms: i64,
    through_ms: i64,
) -> Result<Option<i64>, Error> {
    let mut candidate = cron
        .find_previous_occurrence(&start, inclusive)
        .map_err(|_| invalid("timer cron has no previous occurrence in the supported range"))?;
    check_cron_year(candidate)?;
    loop {
        match resolve_local(tz, candidate, true)? {
            LocalResolution::Single(slot) => {
                if slot > after_ms && slot <= through_ms {
                    return Ok(Some(slot));
                }
            }
            LocalResolution::Ambiguous(earlier, later) => {
                let latest = [earlier, later]
                    .into_iter()
                    .filter(|slot| *slot > after_ms && *slot <= through_ms)
                    .max();
                if latest.is_some() {
                    return Ok(latest);
                }
            }
            LocalResolution::Gap {
                slot,
                first_missing,
            } => {
                if slot > after_ms && slot <= through_ms {
                    return Ok(Some(slot));
                }
                // Every cron candidate in this local gap coalesces to one UTC
                // instant. Skip the gap as a range instead of revisiting it.
                let gap_start = first_missing
                    .ok_or_else(|| Error::new("internal", "cron gap boundary was not resolved"))?;
                let before_gap = gap_start
                    .checked_sub_signed(ONE_SECOND)
                    .ok_or_else(out_of_range)?;
                candidate = cron
                    .find_previous_occurrence(&before_gap, true)
                    .map_err(|_| {
                        invalid("timer cron has no previous occurrence in the supported range")
                    })?;
                check_cron_year(candidate)?;
                continue;
            }
        }

        let previous = candidate;
        candidate = cron
            .find_previous_occurrence(&previous, false)
            .map_err(|_| invalid("timer cron has no previous occurrence in the supported range"))?;
        if candidate >= previous {
            return Err(Error::new(
                "source_integrity",
                "Croner returned a non-retreating occurrence",
            ));
        }
        check_cron_year(candidate)?;
    }
}

#[derive(Clone, Copy)]
struct OverlapBounds {
    start: NaiveDateTime,
    end: NaiveDateTime,
}

#[derive(Clone, Copy)]
enum FoldSearch {
    NextAfter { after_ms: i64 },
    LatestDue { after_ms: i64, through_ms: i64 },
}

fn overlap_bounds(tz: Tz, local: NaiveDateTime) -> Result<OverlapBounds, Error> {
    let local = local.with_nanosecond(0).ok_or_else(out_of_range)?;
    let LocalResult::Ambiguous(earlier, later) = tz.from_local_datetime(&local) else {
        return Err(Error::new(
            "source_integrity",
            "ambiguous local time lost its overlap mapping",
        ));
    };
    let overlap_seconds = later
        .timestamp()
        .checked_sub(earlier.timestamp())
        .filter(|seconds| (1..=MAX_TIME_ZONE_SHIFT_SECONDS).contains(seconds))
        .ok_or_else(|| invalid("timer time-zone overlap is outside the supported range"))?;

    // A repeated local interval has the same length as the UTC distance between
    // its two mappings. This brackets its first ambiguous second without relying
    // on a fixed one-hour DST assumption.
    let mut low = local
        .checked_sub_signed(ChronoDuration::seconds(overlap_seconds + 1))
        .ok_or_else(out_of_range)?;
    let mut high = local;
    if !matches!(tz.from_local_datetime(&low), LocalResult::Single(_)) {
        return Err(invalid("timer overlap boundary is outside supported range"));
    }
    while high.signed_duration_since(low).num_seconds() > 1 {
        let span = high.signed_duration_since(low).num_seconds();
        let middle = low
            .checked_add_signed(ChronoDuration::seconds(span / 2))
            .ok_or_else(out_of_range)?;
        match tz.from_local_datetime(&middle) {
            LocalResult::Ambiguous(_, _) => high = middle,
            LocalResult::Single(_) => low = middle,
            LocalResult::None => {
                return Err(invalid("unexpected gap while resolving timer overlap"));
            }
        }
    }
    let end = high
        .checked_add_signed(ChronoDuration::seconds(overlap_seconds))
        .ok_or_else(out_of_range)?;
    if !matches!(tz.from_local_datetime(&end), LocalResult::Single(_)) {
        return Err(invalid("timer overlap end is outside supported range"));
    }
    Ok(OverlapBounds { start: high, end })
}

fn scan_overlap(
    cron: &Cron,
    tz: Tz,
    bounds: OverlapBounds,
    search: FoldSearch,
) -> Result<Option<i64>, Error> {
    let mut candidate = match cron.find_next_occurrence(&bounds.start, true) {
        Ok(candidate) => candidate,
        Err(_) => return Ok(None),
    };
    check_cron_year(candidate)?;
    let mut best = None;
    while candidate < bounds.end {
        let resolution = resolve_local(tz, candidate, false)?;
        match resolution {
            LocalResolution::Single(slot) => consider_fold_slot(slot, search, &mut best),
            LocalResolution::Ambiguous(earlier, later) => {
                consider_fold_slot(earlier, search, &mut best);
                consider_fold_slot(later, search, &mut best);
            }
            LocalResolution::Gap { .. } => {
                return Err(Error::new(
                    "source_integrity",
                    "a local time cannot be both in an overlap and a gap",
                ));
            }
        }

        let previous = candidate;
        candidate = match cron.find_next_occurrence(&previous, false) {
            Ok(candidate) => candidate,
            Err(_) => break,
        };
        if candidate <= previous {
            return Err(Error::new(
                "source_integrity",
                "Croner returned a non-advancing overlap occurrence",
            ));
        }
        check_cron_year(candidate)?;
    }
    Ok(best)
}

fn consider_fold_slot(slot: i64, search: FoldSearch, best: &mut Option<i64>) {
    match search {
        FoldSearch::NextAfter { after_ms }
            if slot > after_ms && best.is_none_or(|current| slot < current) =>
        {
            *best = Some(slot);
        }
        FoldSearch::LatestDue {
            after_ms,
            through_ms,
        } if slot > after_ms && slot <= through_ms && best.is_none_or(|current| slot > current) => {
            *best = Some(slot);
        }
        _ => {}
    }
}

#[derive(Clone, Copy)]
enum LocalResolution {
    Single(i64),
    Ambiguous(i64, i64),
    Gap {
        slot: i64,
        first_missing: Option<NaiveDateTime>,
    },
}

fn resolve_local(
    tz: Tz,
    local: NaiveDateTime,
    need_gap_start: bool,
) -> Result<LocalResolution, Error> {
    check_cron_year(local)?;
    match tz.from_local_datetime(&local) {
        LocalResult::Single(instant) => Ok(LocalResolution::Single(to_epoch_ms(instant)?)),
        LocalResult::Ambiguous(first, second) => {
            let first = to_epoch_ms(first)?;
            let second = to_epoch_ms(second)?;
            Ok(LocalResolution::Ambiguous(
                first.min(second),
                first.max(second),
            ))
        }
        LocalResult::None => {
            let first_valid = first_valid_after_gap(tz, local)?;
            let first_missing = if need_gap_start {
                Some(first_missing_in_gap(tz, local)?)
            } else {
                None
            };
            Ok(LocalResolution::Gap {
                slot: to_epoch_ms(first_valid)?,
                first_missing,
            })
        }
    }
}

fn first_valid_after_gap(tz: Tz, local: NaiveDateTime) -> Result<DateTime<Tz>, Error> {
    let mut probe = local;
    for _ in 0..MAX_TIME_ZONE_SHIFT_SECONDS {
        probe = probe
            .checked_add_signed(ONE_SECOND)
            .ok_or_else(out_of_range)?;
        check_cron_year(probe)?;
        match tz.from_local_datetime(&probe) {
            LocalResult::Single(instant) => return Ok(instant),
            LocalResult::Ambiguous(first, _) => return Ok(first),
            LocalResult::None => {}
        }
    }
    Err(invalid(
        "timer local-time gap exceeds the supported time-zone shift range",
    ))
}

fn first_missing_in_gap(tz: Tz, local: NaiveDateTime) -> Result<NaiveDateTime, Error> {
    let mut probe = local;
    for _ in 0..MAX_TIME_ZONE_SHIFT_SECONDS {
        probe = probe
            .checked_sub_signed(ONE_SECOND)
            .ok_or_else(out_of_range)?;
        check_cron_year(probe)?;
        match tz.from_local_datetime(&probe) {
            LocalResult::Single(_) | LocalResult::Ambiguous(_, _) => {
                return probe
                    .checked_add_signed(ONE_SECOND)
                    .ok_or_else(out_of_range);
            }
            LocalResult::None => {}
        }
    }
    Err(invalid(
        "timer local-time gap start is outside the supported range",
    ))
}

fn first_slot_after(resolution: LocalResolution, after_ms: i64) -> Option<i64> {
    match resolution {
        LocalResolution::Single(slot) if slot > after_ms => Some(slot),
        LocalResolution::Ambiguous(earlier, later) => [earlier, later]
            .into_iter()
            .filter(|slot| *slot > after_ms)
            .min(),
        LocalResolution::Gap { slot, .. } if slot > after_ms => Some(slot),
        _ => None,
    }
}

fn local_at_utc_ms(tz: Tz, utc_ms: i64) -> Result<NaiveDateTime, Error> {
    let instant = Utc
        .timestamp_millis_opt(utc_ms)
        .single()
        .ok_or_else(out_of_range)?;
    let local = instant.with_timezone(&tz).naive_local();
    check_cron_year(local)?;
    Ok(local)
}

fn to_epoch_ms(instant: DateTime<Tz>) -> Result<i64, Error> {
    let nanos = instant.timestamp_subsec_nanos();
    if !nanos.is_multiple_of(1_000_000) {
        return Err(invalid(
            "timer cron occurrence is not on a whole-millisecond boundary",
        ));
    }
    Ok(instant.timestamp_millis())
}

fn check_cron_year(local: NaiveDateTime) -> Result<(), Error> {
    if !(1..5000).contains(&local.year()) {
        return Err(invalid(
            "timer cron date is outside Croner's supported year range",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> Error {
    Error::new("invalid", message)
}

fn out_of_range() -> Error {
    invalid("timer occurrence is outside the supported date/time range")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .expect("UTC fixture is valid")
            .timestamp_millis()
    }

    fn new_york_cron(expression: &str) -> TimerSchedule {
        TimerSchedule::Cron {
            expression: expression.to_owned(),
            tz: "America/New_York".to_owned(),
        }
    }

    #[test]
    fn interval_starts_at_k_one_and_uses_an_exclusive_utc_cursor() {
        let schedule = TimerSchedule::Interval {
            period_ms: 10,
            anchor: None,
        };
        assert_eq!(next_after(&schedule, 100, 99).unwrap(), Some(110));
        assert_eq!(next_after(&schedule, 100, 110).unwrap(), Some(120));
        assert_eq!(latest_due(&schedule, 100, 100, 139).unwrap(), Some(130));
        assert_eq!(latest_due(&schedule, 100, 139, 130).unwrap(), None);
        // A rolled-back wall clock cannot move the durable UTC cursor backward.
        assert_eq!(latest_due(&schedule, 100, 120, 119).unwrap(), None);
    }

    #[test]
    fn explicit_interval_anchor_must_match_the_persisted_resolution() {
        let schedule = TimerSchedule::Interval {
            period_ms: 10,
            anchor: Some(100),
        };
        assert_eq!(next_after(&schedule, 100, 100).unwrap(), Some(110));
        assert_eq!(
            next_after(&schedule, 101, 100).unwrap_err().code,
            "bad_state"
        );
    }

    #[test]
    fn one_shot_uses_signed_epoch_milliseconds_once() {
        let schedule = TimerSchedule::Once { at: -500 };
        assert_eq!(next_after(&schedule, 0, -501).unwrap(), Some(-500));
        assert_eq!(next_after(&schedule, 0, -500).unwrap(), None);
        assert_eq!(latest_due(&schedule, 0, -501, -500).unwrap(), Some(-500));
        assert_eq!(latest_due(&schedule, 0, -500, 0).unwrap(), None);
    }

    #[test]
    fn parsers_require_positive_whole_milliseconds_and_five_cron_fields() {
        assert_eq!(parse_positive_duration_ms("1.5s").unwrap(), 1_500);
        assert!(parse_positive_duration_ms("0s").is_err());
        assert!(parse_positive_duration_ms("500us").is_err());
        assert!(parse_positive_duration_ms("1ns").is_err());
        assert_eq!(
            parse_rfc3339_epoch_ms("1969-12-31T23:59:59.500Z").unwrap(),
            -500
        );
        assert!(parse_rfc3339_epoch_ms("2024-01-01T00:00:00.000000001Z").is_err());

        let cron = parse_cron("30 1 * * *", "UTC").unwrap();
        assert_eq!(
            cron,
            TimerSchedule::Cron {
                expression: "30 1 * * *".to_owned(),
                tz: "UTC".to_owned(),
            }
        );
        assert!(parse_cron("0 0 1 1 * *", "UTC").is_err());
        assert!(parse_cron("not a cron", "UTC").is_err());
        assert!(parse_cron("0 0 * * *", "Not/A_Zone").is_err());
    }

    #[test]
    fn new_york_fixed_cron_returns_both_fall_back_utc_instants() {
        let schedule = new_york_cron("30 1 * * *");
        let first = utc_ms(2024, 11, 3, 5, 30);
        let second = utc_ms(2024, 11, 3, 6, 30);
        assert_eq!(
            next_after(&schedule, 0, utc_ms(2024, 11, 3, 5, 0)).unwrap(),
            Some(first)
        );
        // The second 01:30 has an earlier local label than the current 01:30,
        // but it is still a distinct future UTC slot.
        assert_eq!(next_after(&schedule, 0, first).unwrap(), Some(second));
    }

    #[test]
    fn new_york_wildcard_fold_orders_later_pass_by_utc() {
        let schedule = new_york_cron("*/15 1 * * *");
        let after_first_pass = utc_ms(2024, 11, 3, 5, 45);
        assert_eq!(
            next_after(&schedule, 0, after_first_pass).unwrap(),
            Some(utc_ms(2024, 11, 3, 6, 0))
        );
        assert_eq!(
            latest_due(
                &schedule,
                0,
                utc_ms(2024, 11, 3, 5, 45),
                utc_ms(2024, 11, 3, 6, 15),
            )
            .unwrap(),
            Some(utc_ms(2024, 11, 3, 6, 15))
        );
    }

    #[test]
    fn new_york_wildcard_gap_coalesces_to_first_valid_instant() {
        let schedule = new_york_cron("*/5 2 * * *");
        let before_gap = utc_ms(2024, 3, 10, 6, 59);
        let first_valid = utc_ms(2024, 3, 10, 7, 0);
        assert_eq!(
            next_after(&schedule, 0, before_gap).unwrap(),
            Some(first_valid)
        );
        assert_eq!(
            latest_due(&schedule, 0, before_gap, first_valid).unwrap(),
            Some(first_valid)
        );
        // The missing 02:05, 02:10, ... labels all coalesce to the one 03:00Z
        // local-boundary firing; the following slot is tomorrow's 02:00.
        assert_eq!(
            next_after(&schedule, 0, first_valid).unwrap(),
            Some(utc_ms(2024, 3, 11, 6, 0))
        );
    }
}
