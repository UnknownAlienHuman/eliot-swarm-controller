# R12a. Calendar DST: preserve the absolute instant before Croner

**Статус: production fix implemented in PR #38; rustfmt/changed-line Clippy qualification runs on current main. Behavioral calendar tests remain the final product phase.**

Evidence base: ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`, selected Croner/Chrono source reviewed on 8 October 2026. Version numbers and SHA values are evidence coordinates, not dependency pins or downgrade instructions.

## Confirmed defect

The old `local_second_at` path did this:

```text
UTC timestamp
→ timezone conversion
→ DateTime<Tz>::with_nanosecond(0)
→ local wall-time reconstruction
```

Inside an autumn DST fold, the starting UTC timestamp identifies one exact real instant and one concrete offset. Chrono's local-time remapping sees two possible offsets and may return `None`. ELIOT therefore failed before Croner evaluated an otherwise valid schedule.

The existing fall-overlap test started before the fold and did not call the helper from either side of the repeated hour.

## Implemented boundary

`crates/swarm-kernel-host/src/scheduler/calendar.rs::local_second_at` now:

```rust
let second_ms = epoch_ms
    .div_euclid(1_000)
    .checked_mul(1_000)
    .ok_or_else(|| Error::invalid(
        "calendar timestamp is outside the supported date range"
    ))?;
let utc = Utc
    .timestamp_millis_opt(second_ms)
    .single()
    .ok_or_else(|| Error::invalid(
        "calendar timestamp is outside the supported date range"
    ))?;
Ok(utc.with_timezone(&parsed.timezone))
```

Properties:

- milliseconds are floored on the absolute UTC timeline;
- `div_euclid` gives correct floor semantics if the helper is reused for negative Unix timestamps;
- checked multiplication preserves a typed out-of-range error;
- timezone conversion happens once and retains the exact fold offset;
- the timestamp already has whole-second precision, so no local remapping remains.

The now-unused `Timelike` import and post-timezone `with_nanosecond(0)` were removed.

## Shared consumers

The one helper feeds:

```text
latest_due
first_occurrence_at_or_after
next_due_at_ms
preview_next_occurrences
```

There are no separate preview/scheduler fixes. `generation_digest`, `occurrence_id`, Store cursor/receipt, latest-only coalescing and Croner semantics are unchanged. No persisted occurrence migration is required.

## What this fix does not do

- no manual earliest/latest offset selection;
- no timezone-specific branches;
- no second cron parser;
- no dependency upgrade/downgrade as a substitute for the boundary fix;
- no scheduler retry, pacing, issuance or Store policy change.

The scheduler/fairness remainder is owned by [Issue #79](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/79).

## Final product-phase scenarios

Tests must enter through the real module functions rather than a copied helper:

1. New York 2024 fold, both sides of repeated 01:xx:
   - wildcard `latest_due` does not error;
   - `next_due_at_ms` advances to the next real second;
   - preview returns strictly increasing UTC instants;
   - fixed `01:30` retains the documented first-only occurrence.
2. Berlin 2025 fold, both sides of repeated 02:xx.
3. Subsecond inputs on both fold sides floor to the exact UTC second.
4. Existing spring-gap behavior remains unchanged.
5. Cursor/coalescing across the two real fold instants matches the selected Croner contract.

Future behavioral command:

```sh
cargo test --locked -p swarm-kernel-host scheduler::calendar
```

Current minimal code gate:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Tests are not claimed by compilation, formatting or source review.