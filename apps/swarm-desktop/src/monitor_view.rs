//! WinUI pieces of the right panel: the monitor's Agent cards, the CPU and
//! RSS charts (bars built from `Border`s, each with its latest value and a
//! labelled scale; no canvas), the usage lines and the Change manager
//! section. Buttons carry the Agent and generations shown on screen, so a
//! click acts on exactly what was displayed.

use windows_reactor::*;

use crate::monitor::{
    self, DetailRead, GRAPH_BARS, Monitor, MonitorRow, bytes_label, cpu_time_label, state_name,
    tokens_label,
};
use crate::theme;
use crate::theme::winui::Palette;
use crate::ui::tokens;

/// One possible new manager with its two bound moves.
pub struct MoveRow {
    pub id: String,
    pub label: String,
    pub move_agent: Option<Callback<()>>,
    pub move_children: Option<Callback<()>>,
}

/// A note: secondary size, wrapped and selectable (callers lower its
/// emphasis where it is supporting text).
fn small(text: impl Into<String>) -> TextBlock {
    crate::m3::text(text, tokens::text::SECONDARY)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// One monitor card: the title with its state, what it does now, and its
/// resources in one muted line. The whole card opens the conversation.
pub fn card(row: &MonitorRow, title: &str, open: Callback<()>, palette: Palette) -> View {
    let state = state_name(row.state);
    let under = row
        .parent
        .as_deref()
        .map(|parent| format!(" · under {parent}"))
        .unwrap_or_default();
    // The button pads its content by the control inset; the content adds
    // the same inset above and below.
    crate::m3::card_button(
        palette,
        format!(
            "{title}, {state}, {}{under}, {}",
            row.harness,
            row.operation()
        ),
        open,
        StackPanel::new()
            .spacing(tokens::spacing::XS)
            // The button's 1-DIP border and 12-DIP inset plus this put the
            // text at the 16-DIP padding of every other card.
            .margin(Thickness::xy(
                tokens::spacing::MD - tokens::spacing::INSET - tokens::layout::HAIRLINE,
                tokens::spacing::MD - tokens::layout::HAIRLINE,
            ))
            .children((
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        crate::m3::section_title(title.to_owned())
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                        Border::new()
                            .grid_column(1)
                            .content(crate::m3::status(palette, state)),
                    )),
                crate::m3::body(format!("{} · {}{under}", row.harness, row.operation()))
                    .opacity(theme::MUTED_OPACITY)
                    .text_trimming(TextTrimming::CharacterEllipsis),
                crate::m3::secondary(format!(
                    "{} · CPU {} · {}",
                    bytes_label(row.obs.rss_bytes()),
                    cpu_time_label(row.obs.cpu_time_ns()),
                    tokens_label(row)
                ))
                .text_trimming(TextTrimming::CharacterEllipsis),
            )),
    )
}

/// Plot height in DIPs; the bars stretch across the card width.
const PLOT_HEIGHT: f64 = tokens::layout::PLOT_HEIGHT;
/// Smallest CPU scale in percent, so a near-idle series is not drawn full
/// height.
const CPU_FLOOR_PERCENT: f64 = 5.0;
const MIB: f64 = 1024.0 * 1024.0;

/// Smallest value of the 1-2-2.5-5 x 10^n sequence that is >= `max`; 1.0
/// for a non-finite or non-positive input.
fn nice_ceiling(max: f64) -> f64 {
    if !max.is_finite() || max <= 0.0 {
        return 1.0;
    }
    let first = max.log10().floor() as i32 - 1;
    (first..first + 3)
        .flat_map(|exponent| [1.0, 2.0, 2.5, 5.0].map(|mantissa| scaled(mantissa, exponent)))
        .find(|candidate| *candidate >= max)
        .unwrap_or(max)
}

/// `mantissa * 10^exponent`, dividing for negative exponents so that
/// 2 * 10^-1 is exactly the double written as `0.2`.
fn scaled(mantissa: f64, exponent: i32) -> f64 {
    let power = 10f64.powi(exponent.abs());
    if exponent >= 0 {
        mantissa * power
    } else {
        mantissa / power
    }
}

/// CPU scale in percent: the nice ceiling of the observed maximum, at least
/// 5%.
fn cpu_ceiling(max: f64) -> f64 {
    nice_ceiling(max).max(CPU_FLOOR_PERCENT)
}

/// RSS scale in bytes: a nice number of MiB (so the caption reads "250
/// MiB", not "477 MiB"), at least 1 MiB.
fn rss_ceiling(max_bytes: f64) -> f64 {
    nice_ceiling(max_bytes / MIB).max(1.0) * MIB
}

/// Largest known value of a series, or 0 when every point is unknown.
fn observed_max(values: &[Option<f64>]) -> f64 {
    values.iter().flatten().copied().fold(0.0, f64::max)
}

/// The latest CPU value for the chart header: one decimal below 10%.
fn cpu_latest_label(cpu: Option<f64>) -> String {
    match cpu {
        None => "—".to_owned(),
        Some(cpu) if cpu < 10.0 => format!("{cpu:.1}%"),
        Some(cpu) => format!("{cpu:.0}%"),
    }
}

/// One metric card: the title and its latest value on one row, the bars in
/// one star column each on a track as wide as the card, then the scale and
/// what the number means. Unknown points stay a faint stub so gaps show;
/// known values are at least 2 DIPs tall.
#[allow(clippy::too_many_arguments)]
fn chart_card(
    title: &str,
    latest: String,
    values: &[Option<f64>],
    ceiling: f64,
    scale: String,
    note: String,
    bar: Brush,
    palette: Palette,
) -> View {
    // The newest sample sits at the right edge; a short history leaves the
    // older columns empty instead of bunching at the left.
    let first = GRAPH_BARS.saturating_sub(values.len());
    let bars: Vec<KeyedView> = values
        .iter()
        .take(GRAPH_BARS)
        .enumerate()
        .map(|(index, value)| {
            let view = match value {
                Some(value) => Border::new()
                    .height(
                        (value / ceiling * PLOT_HEIGHT)
                            .clamp(tokens::layout::PLOT_STUB, PLOT_HEIGHT),
                    )
                    .background(bar),
                None => Border::new()
                    .height(tokens::layout::PLOT_STUB)
                    .opacity(theme::MUTED_OPACITY)
                    .background(palette.outline_variant()),
            };
            KeyedView::new(
                index,
                view.margin(Thickness::xy(tokens::layout::PLOT_GAP, 0.0))
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .grid_column((first + index) as i32),
            )
        })
        .collect();
    StackPanel::new().spacing(tokens::spacing::XS).children((
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .children((
                crate::m3::muted(title, tokens::text::SECONDARY)
                    .vertical_alignment(VerticalAlignment::Bottom),
                crate::m3::text(latest, tokens::text::SECTION_TITLE).grid_column(1),
            )),
        // The plot well in the surface color: one tonal step from the card
        // in both schemes (the highest container nearly matched the light
        // card).
        Border::new()
            .background(palette.surface())
            .corner_radius(tokens::radius::CONTAINER)
            .padding(Thickness::uniform(tokens::spacing::SM))
            .content(
                Grid::new()
                    .height(PLOT_HEIGHT)
                    .columns((0..GRAPH_BARS).map(|_| GridLength::STAR))
                    .keyed_children(bars),
            ),
        small(format!("{scale} · {note}")).opacity(theme::MUTED_OPACITY),
    ))
}

/// The Agent's identity and settings from the monitor's detail read (when
/// its conversation is not the open one).
pub fn identity_lines(read: &DetailRead) -> Vec<String> {
    let agent = &read.agent.agent;
    let effective = agent.effective.as_ref();
    let field = |name: &str| {
        effective
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("not read back")
            .to_owned()
    };
    vec![
        format!(
            "Agent {} · generation {} · {} · {}{}",
            agent.id,
            agent.generation,
            agent.harness,
            format!("{:?}", agent.state).to_lowercase(),
            if agent.observe_only {
                " · observe-only"
            } else {
                ""
            }
        ),
        format!(
            "Profile {} · effective model {} · effort {}",
            agent.profile,
            field("model"),
            field("effort")
        ),
        format!(
            "Active turn {} · pending requests {}",
            agent.active_turn.as_deref().unwrap_or("none"),
            agent.pending.len()
        ),
    ]
}

/// Token usage and cost lines of the detail read.
pub fn usage_lines(read: &DetailRead) -> Vec<String> {
    let mut lines = Vec::new();
    match &read.usage {
        Ok(usage) => {
            lines.push(format!(
                "{} turn(s) counted, {} without usage, {} unknown excluded, {} running excluded; history {} ({}){}",
                usage.turns.turns_counted,
                usage.turns.turns_without_usage,
                usage.turns.turns_unknown_excluded,
                usage.turns.turns_running_excluded,
                if usage.history_complete { "complete" } else { "partial" },
                usage.history_reason,
                if usage.truncated { "; truncated" } else { "" }
            ));
            for scope in usage.turns.by_scope.iter().take(4) {
                lines.push(format!(
                    "{}: {} turn(s) · input {} · output {} · total {} tokens",
                    scope.scope,
                    scope.turns_counted,
                    counter(scope.totals.input_tokens),
                    counter(scope.totals.output_tokens),
                    counter(scope.totals.total_tokens),
                ));
            }
            for cost in &usage.sessions.cost_by_unit {
                lines.push(format!(
                    "Cost [{}]: {} over {} snapshot(s)",
                    cost.unit, cost.total_amount, cost.snapshots
                ));
            }
        }
        Err(error) => lines.push(format!("Usage unavailable: {error}")),
    }
    lines
}

fn counter(value: Option<i64>) -> String {
    value.map_or_else(|| "—".to_owned(), |value| value.to_string())
}

/// The CPU and memory charts of `id`, with the sampling note.
pub fn resource_views(monitor: &Monitor, id: &str, palette: Palette) -> Vec<KeyedView> {
    let mut views: Vec<KeyedView> = Vec::new();
    if let Some(row) = monitor.rows.iter().find(|row| row.id == id) {
        views.push(KeyedView::new(
            "fresh",
            small(row.obs.freshness()).opacity(theme::MUTED_OPACITY),
        ));
    }
    match monitor.series.get(id) {
        Some(series) if !series.points().is_empty() => {
            let recent = series.recent();
            let last = recent.last().copied();
            let cpu_values: Vec<Option<f64>> =
                recent.iter().map(|point| point.cpu_percent).collect();
            let cpu_scale = cpu_ceiling(observed_max(&cpu_values));
            let cpu_note = match last.and_then(|point| point.cpu_percent) {
                Some(_) => format!(
                    "core-equivalent, 100% = one core{}",
                    if last.is_some_and(|point| point.partial) {
                        " · partial: process set changed"
                    } else {
                        ""
                    }
                ),
                None => "rate unknown for the latest interval".to_owned(),
            };
            views.push(KeyedView::new(
                "cpu",
                chart_card(
                    "CPU",
                    cpu_latest_label(last.and_then(|point| point.cpu_percent)),
                    &cpu_values,
                    cpu_scale,
                    format!("scale 0–{cpu_scale}%"),
                    cpu_note,
                    palette.primary(),
                    palette,
                ),
            ));
            let rss_values: Vec<Option<f64>> = recent
                .iter()
                .map(|point| point.rss_bytes.map(|bytes| bytes as f64))
                .collect();
            let rss_scale = rss_ceiling(observed_max(&rss_values));
            views.push(KeyedView::new(
                "rss",
                chart_card(
                    "Memory (RSS)",
                    bytes_label(last.and_then(|point| point.rss_bytes)),
                    &rss_values,
                    rss_scale,
                    format!("scale 0–{}", bytes_label(Some(rss_scale as u64))),
                    "sum over the Agent's sampled processes, not host memory share".to_owned(),
                    palette.tertiary(),
                    palette,
                ),
            ));
            views.push(KeyedView::new(
                "series",
                small(format!(
                    "{} sample(s) kept while this panel is open (capped at {}).",
                    series.points().len(),
                    monitor::RING_CAPACITY
                ))
                .opacity(theme::MUTED_OPACITY),
            ));
        }
        _ => views.push(KeyedView::new(
            "no-series",
            small("No process samples yet.").opacity(theme::MUTED_OPACITY),
        )),
    }
    views
}

/// "Change manager": move the Agent with its subtree, or only its
/// children, under another live Agent. Each button is already bound.
pub fn manager_section(
    id: &str,
    rows: Vec<MoveRow>,
    status: &str,
    enabled: bool,
    palette: Palette,
) -> View {
    let moves: Vec<KeyedView> = rows
        .into_iter()
        .map(|row| {
            let button = |text: &str, name: String, callback: Option<Callback<()>>| -> View {
                match callback {
                    Some(callback) => crate::m3::outlined(palette)
                        .is_enabled(enabled)
                        .automation_name(name)
                        .on_click(callback)
                        .content(crate::m3::label(text)),
                    None => View::empty(),
                }
            };
            KeyedView::new(
                row.id.clone(),
                StackPanel::new().spacing(tokens::spacing::XS).children((
                    small(row.label.clone()),
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .children((
                            button(
                                "Move here",
                                format!("Move {id} under {}", row.id),
                                row.move_agent,
                            ),
                            button(
                                "Move its workers here",
                                format!("Move the workers of {id} under {}", row.id),
                                row.move_children,
                            ),
                        )),
                )),
            )
        })
        .collect();
    let note = if moves.is_empty() {
        "No other live Agent can become the manager.".to_owned()
    } else {
        "The daemon checks cycles, fanout limits, observe-only Agents and both generations."
            .to_owned()
    };
    crate::m3::card_high(palette).content(
        StackPanel::new().spacing(tokens::spacing::SM).children((
            crate::m3::section_title("Change manager"),
            small(note).opacity(theme::MUTED_OPACITY),
            StackPanel::new()
                .spacing(tokens::spacing::MD)
                .keyed_children(moves),
            small(status.to_owned()),
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= expected.abs() * 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn nice_ceiling_is_the_smallest_one_two_two_and_a_half_five_step_at_or_above() {
        close(nice_ceiling(0.2), 0.2);
        close(nice_ceiling(13.0), 20.0);
        close(nice_ceiling(25.0), 25.0);
        close(nice_ceiling(26.0), 50.0);
        close(nice_ceiling(100.0), 100.0);
        close(nice_ceiling(101.0), 200.0);
        close(nice_ceiling(0.0), 1.0);
        close(nice_ceiling(f64::NAN), 1.0);
    }

    #[test]
    fn scales_have_floors_and_rss_reads_in_whole_mib_steps() {
        close(cpu_ceiling(0.2), 5.0);
        close(cpu_ceiling(13.0), 20.0);
        close(rss_ceiling(0.0), MIB);
        // 246 MiB scales to 250 MiB, not to a decimal byte count.
        close(rss_ceiling(246.0 * MIB), 250.0 * MIB);
    }

    #[test]
    fn the_latest_value_and_maximum_ignore_unknown_points() {
        close(observed_max(&[None, Some(3.0), Some(7.5), None]), 7.5);
        close(observed_max(&[None, None]), 0.0);
        assert_eq!(cpu_latest_label(Some(13.4)), "13%");
        assert_eq!(cpu_latest_label(Some(0.36)), "0.4%");
        assert_eq!(cpu_latest_label(None), "—");
    }
}
