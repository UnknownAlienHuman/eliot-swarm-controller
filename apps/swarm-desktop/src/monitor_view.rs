//! WinUI pieces of the right panel: the monitor's Agent cards, the CPU and
//! RSS charts (bars built from `Border`s, each with its latest value and a
//! labelled scale; no canvas) and the usage lines. Buttons carry the Agent
//! and generation shown on screen, so a click acts on exactly what was
//! displayed.

use windows_reactor::*;

use crate::monitor::{
    self, DetailRead, GRAPH_BARS, Monitor, MonitorRow, bytes_label, cpu_time_label, group,
    state_words, tokens_label,
};
use crate::theme;
use crate::theme::winui::Palette;
use crate::ui::tokens;

/// A note: secondary size, wrapped and selectable (callers lower its
/// emphasis where it is supporting text).
fn small(text: impl Into<String>) -> TextBlock {
    crate::m3::text(text, tokens::text::SECONDARY)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// The line under a card's measures: why a lost or failed Agent ended (from
/// its loss origin), or an open attention report.
pub enum CardNote {
    Ended(String),
    Attention(String),
}

/// Width the last lines of a resumable card leave for its Resume button.
const RESUME_RESERVE: f64 = 88.0;

/// What a card does: the card itself opens the conversation; a resumable
/// one also offers Resume (its callback and whether it is enabled).
pub struct CardActions {
    pub open: Callback<()>,
    pub resume: Option<(Callback<()>, bool)>,
}

/// One monitor card: the title with its status, what it does now, its
/// resources in one muted line, and why it ended or the open attention
/// report. The whole card opens the conversation; Resume sits on it.
pub fn card(
    row: &MonitorRow,
    title: &str,
    parent: Option<&str>,
    note: Option<&CardNote>,
    actions: CardActions,
    palette: Palette,
) -> View {
    let status = row.status_words();
    // A nested child sits under its parent's card already; a top-level one
    // whose parent is not shown names that task (`parent`: its title).
    let under = parent
        .map(|parent| format!(" · under {parent}"))
        .unwrap_or_default();
    let (note_line, note_name): (View, String) = match note {
        Some(CardNote::Ended(text)) => (
            crate::m3::body(text.clone())
                .text_trimming(TextTrimming::CharacterEllipsis)
                .into(),
            format!(", {text}"),
        ),
        Some(CardNote::Attention(text)) => (
            crate::m3::secondary(format!("Attention: {text}"))
                .foreground(palette.error())
                .text_trimming(TextTrimming::CharacterEllipsis)
                .into(),
            format!(", attention: {text}"),
        ),
        None => (View::empty(), String::new()),
    };
    // Only the measures the daemon reported.
    let measures = [
        row.obs
            .rss_bytes()
            .map(|_| bytes_label(row.obs.rss_bytes())),
        row.obs
            .cpu_time_ns()
            .map(|_| cpu_time_label(row.obs.cpu_time_ns())),
        row.tokens.map(|_| tokens_label(row)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" · ");
    let reserve = if actions.resume.is_some() {
        RESUME_RESERVE
    } else {
        0.0
    };
    // The button pads its content by the control inset; the content adds
    // the same inset above and below.
    let card = crate::m3::card_button(
        palette,
        format!(
            "{title}, {status}, {}{under}, {}{note_name}",
            crate::picker::harness_label(&row.harness),
            row.operation(),
        ),
        actions.open,
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
                        Border::new().grid_column(1).content(crate::m3::status(
                            palette,
                            row.status_code(),
                            status,
                        )),
                    )),
                crate::m3::body(format!(
                    "{} · {}{under}",
                    crate::picker::harness_label(&row.harness),
                    row.operation()
                ))
                .opacity(theme::MUTED_OPACITY)
                .text_trimming(TextTrimming::CharacterEllipsis),
                // The last lines leave room for Resume at the right.
                Border::new()
                    .margin(Thickness::new(0.0, 0.0, reserve, 0.0))
                    .content(
                        crate::m3::secondary(measures)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                    ),
                Border::new()
                    .margin(Thickness::new(0.0, 0.0, reserve, 0.0))
                    .content(note_line),
            )),
    );
    let Some((resume, enabled)) = actions.resume else {
        return card;
    };
    // Resume is its own button beside the card button (a button inside a
    // button would swallow the click): drawn over the card's top-right
    // status line would hide it, so it sits on the card's bottom-right
    // corner, level with the last line.
    Grid::new().children((
        card,
        crate::m3::text_button(palette)
            .horizontal_alignment(HorizontalAlignment::Right)
            .vertical_alignment(VerticalAlignment::Bottom)
            .margin(Thickness::xy(tokens::spacing::SM, tokens::spacing::SM))
            .is_enabled(enabled)
            .automation_name(format!("Resume {title} (run {})", row.generation))
            .on_click(resume)
            .content(crate::m3::label("Resume")),
    ))
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
/// its conversation is not the open one), in words; rows with nothing to
/// say are left out.
pub fn identity_lines(read: &DetailRead) -> Vec<String> {
    let agent = &read.agent.agent;
    let effective = agent.effective.as_ref();
    let field = |name: &str| {
        effective
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let harness = crate::picker::harness_label(&agent.harness).to_owned();
    let mut lines = vec![format!("{harness} · {}", state_words(agent.state))];
    match (field("model"), field("effort")) {
        (Some(model), Some(effort)) => lines.push(format!(
            "{model} · {} effort",
            crate::picker::effort_label(&effort)
        )),
        (Some(model), None) => lines.push(model),
        _ => lines.push(format!("Model not reported by {harness} yet")),
    }
    if !agent.profile.starts_with("adhoc-") {
        lines.push(format!("Preset {}", agent.profile));
    }
    if agent.observe_only {
        lines.push(format!(
            "{harness} runs this one itself: Swarm shows it but cannot steer or stop it."
        ));
    }
    if agent.active_turn.is_some() {
        lines.push("Working on a turn".to_owned());
    }
    match agent.pending.len() {
        0 => {}
        1 => lines.push("1 request waits for you".to_owned()),
        many => lines.push(format!("{many} requests wait for you")),
    }

    lines
}

/// Heavy-work and session-growth readback for the Agent Details panel, in
/// words; empty when neither was recorded (the section is then hidden).
/// Only committed rollups render: episode rows show the program basename
/// recorded at commit time, never full commands or paths.
pub fn observe_lines(read: &DetailRead) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(heavy) = &read.agent.heavy
        && heavy.episodes > 0
    {
        let what = heavy
            .latest_category
            .as_deref()
            .map(|category| format!(", latest {}", category.replace('_', " ")))
            .unwrap_or_default();
        lines.push(format!(
            "Heavy work: {}{what}, {:.1} s of CPU",
            eliot_core::count_words(heavy.episodes, "episode", "episodes"),
            heavy.total_cpu_ms as f64 / 1000.0
        ));
    }
    if let Some(growth) = &read.agent.session_growth {
        lines.push(format!(
            "The session grows fast: {}",
            growth
                .message
                .as_deref()
                .unwrap_or("its size crossed the warning threshold")
        ));
    }
    lines
}

/// Token usage and cost of the detail read, in words; counters that are
/// zero or unreported are left out.
pub fn usage_lines(read: &DetailRead) -> Vec<String> {
    let mut lines = Vec::new();
    match &read.usage {
        Ok(usage) => {
            let turns = &usage.turns;
            let (mut input, mut output, mut total) = (0_i64, 0_i64, 0_i64);
            for scope in &turns.by_scope {
                input += scope.totals.input_tokens.unwrap_or(0);
                output += scope.totals.output_tokens.unwrap_or(0);
                total += scope.totals.total_tokens.unwrap_or(0);
            }
            if total == 0 {
                total = input + output;
            }
            if turns.turns_counted == 0 && total == 0 {
                lines.push("No token usage reported yet.".to_owned());
            } else {
                lines.push(format!(
                    "{} tokens over {}",
                    group(total),
                    eliot_core::count_words(turns.turns_counted as u64, "turn", "turns")
                ));
                if input > 0 || output > 0 {
                    lines.push(format!("{} in · {} out", group(input), group(output)));
                }
            }
            if turns.turns_without_usage > 0 {
                lines.push(format!(
                    "{} reported no usage",
                    eliot_core::count_words(turns.turns_without_usage as u64, "turn", "turns")
                ));
            }
            if turns.turns_running_excluded > 0 {
                lines.push("The running turn counts when it ends.".to_owned());
            }
            if !usage.history_complete {
                lines.push("Older history is not counted.".to_owned());
            }
            for cost in &usage.sessions.cost_by_unit {
                let amount = cost.total_amount;
                let label = if matches!(cost.unit.as_str(), "usd" | "usd_estimate" | "USD") {
                    if amount > 0.0 && amount < 0.01 {
                        "<$0.01".to_owned()
                    } else {
                        format!("${amount:.2}")
                    }
                } else {
                    format!("{amount:.2} {}", cost.unit)
                };
                lines.push(format!(
                    "Cost: {label}{}",
                    if cost.unit.ends_with("_estimate") {
                        " (estimated)"
                    } else {
                        ""
                    }
                ));
            }
        }
        Err(_) => lines.push("Usage not reported for this Agent.".to_owned()),
    }
    lines
}

/// The CPU and memory charts of `id`, with the sampling note.
pub fn resource_views(monitor: &Monitor, id: &str, palette: Palette) -> Vec<KeyedView> {
    let mut views: Vec<KeyedView> = Vec::new();
    let row = monitor.row(id);
    // An Agent that ended with every process exited has nothing to sample:
    // one line says so.
    if row.is_some_and(|row| row.ended() && row.obs.live_processes() == 0) {
        views.push(KeyedView::new(
            "ended",
            small("No live process.").opacity(theme::MUTED_OPACITY),
        ));
        return views;
    }
    if let Some(row) = row {
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
                None => "no rate measured for the latest interval yet".to_owned(),
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
                    "{} kept while this panel is open (at most {}).",
                    eliot_core::count_words(series.points().len() as u64, "sample", "samples"),
                    monitor::RING_CAPACITY
                ))
                .opacity(theme::MUTED_OPACITY),
            ));
        }
        // The freshness line already says nothing was sampled.
        _ if row.is_some() => {}
        _ => views.push(KeyedView::new(
            "no-series",
            small("No process samples yet.").opacity(theme::MUTED_OPACITY),
        )),
    }
    views
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
