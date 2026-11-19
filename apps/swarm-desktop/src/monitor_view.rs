//! WinUI pieces of the right panel: the monitor's Agent rows, the CPU and
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

/// What a list row says beyond its state: why a lost or failed Agent ended
/// (from its loss origin), what an Agent that needs attention needs, or the
/// task a top-level worker runs under.
pub enum RowNote {
    Ended(String),
    Attention(String),
    Under(String),
}

/// What a row does: the row itself opens the conversation; a resumable
/// one also offers Resume (its callback and whether it is enabled).
pub struct RowActions {
    pub open: Callback<()>,
    pub resume: Option<(Callback<()>, bool)>,
}

/// What one list row shows besides its Agent.
pub struct RowLook<'a> {
    pub title: &'a str,
    /// How long it has been quiet ([`MonitorRow::quiet`]), as shown
    /// ("3 min", "no progress yet") and in words for its accessible name
    /// ("last progress 3 min ago").
    pub elapsed: &'a str,
    pub elapsed_words: &'a str,
    /// The dot in its group's chart color.
    pub dot: Brush,
    /// The open Agent's row.
    pub selected: bool,
    /// In the Working group (or working without writing code): its
    /// trailing word is the model.
    pub working: bool,
    /// The second line: the work line, or the attention sentence of an
    /// Agent working without writing code.
    pub line: Option<RowLine>,
}

/// A row's second line ([`crate::monitor::work_line`]), what a screen
/// reader hears for it, and whether it is in the attention tone.
pub struct RowLine {
    pub text: String,
    pub words: String,
    pub attention: bool,
}

/// One Monitor list row (`Conversation.dc.html`'s `mrow`): the status dot
/// in its group's color, the title and one trailing word: what it needs
/// (in the attention color), the model it works on, or how long it has
/// been quiet. A live controlled Agent adds its work line under them
/// (a two-line row), or, working without writing code, that sentence in
/// the attention color. Why it ended or the task it runs under stay in its tooltip
/// and accessible name; its measures live in Details, which the row opens
/// with the conversation; Resume sits beside it.
pub fn list_row(
    row: &MonitorRow,
    look: RowLook,
    note: Option<&RowNote>,
    actions: RowActions,
    palette: Palette,
) -> View {
    let status = row.status_words();
    let harness = crate::picker::harness_model_label(&row.harness, row.model.as_deref());
    let note_name = match note {
        Some(RowNote::Ended(text)) => format!(", {text}"),
        Some(RowNote::Attention(text)) => format!(", needs attention: {text}"),
        Some(RowNote::Under(parent)) => format!(", under {parent}"),
        None => String::new(),
    };
    let trailing = match note {
        Some(RowNote::Attention(text)) => {
            crate::m3::text(text.clone(), tokens::text::CAPTION).foreground(palette.attention())
        }
        _ if look.working => crate::m3::secondary(row.model.as_deref().map_or_else(
            || harness.clone(),
            |model| crate::picker::model_short(model).to_owned(),
        )),
        _ => crate::m3::secondary(look.elapsed.to_owned()),
    };
    let title = crate::m3::text(look.title.to_owned(), tokens::text::CHIP)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let trailing = trailing
        .grid_column(1)
        .max_width(tokens::layout::MONITOR_TRAILING_WIDTH)
        .text_trimming(TextTrimming::CharacterEllipsis)
        .vertical_alignment(VerticalAlignment::Center);
    // The work line runs under the title and the trailing word, one line,
    // trimmed to the row; its whole text is in the tooltip and the name.
    let (text_column, line_name, line_tip): (View, String, String) = match &look.line {
        None => (
            Grid::new()
                .grid_column(1)
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::ROW)
                .children((title, trailing)),
            String::new(),
            String::new(),
        ),
        Some(line) => {
            let text = crate::m3::text(line.text.clone(), tokens::text::CAPTION)
                .text_trimming(TextTrimming::CharacterEllipsis);
            let text = if line.attention {
                text.foreground(palette.attention())
            } else {
                text.foreground(palette.muted())
            };
            (
                StackPanel::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .children((
                        Grid::new()
                            .columns([GridLength::STAR, GridLength::Auto])
                            .column_spacing(tokens::spacing::ROW)
                            .children((title, trailing)),
                        text,
                    )),
                format!(", {}", line.words),
                format!(" · {}", line.text),
            )
        }
    };
    let content = Grid::new()
        .columns([GridLength::Auto, GridLength::STAR])
        .column_spacing(tokens::spacing::ROW)
        .children((crate::m3::dot(look.dot), text_column));
    let tooltip = format!("{} · {status} · {harness}{note_name}{line_tip}", look.title);
    let button = crate::m3::row(palette, look.selected);
    let button = if look.line.is_some() {
        button
            .height(tokens::height::TWO_LINE_ROW)
            .min_height(tokens::height::TWO_LINE_ROW)
    } else {
        button
    };
    let button = button
        .automation_name(format!(
            "{}, {status}, {harness}, {}{note_name}{line_name}",
            look.title, look.elapsed_words
        ))
        .on_click(actions.open)
        .content(content)
        .tooltip(tooltip);
    let Some((resume, enabled)) = actions.resume else {
        return button;
    };
    // Resume is its own button beside the row (a button inside a button
    // would swallow the click).
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::XS)
        .children((
            button,
            crate::m3::text_button(palette)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .is_enabled(enabled)
                .automation_name(format!("Resume {} (run {})", look.title, row.generation))
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
                    .background(palette.outline()),
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
/// say are left out. The effort reads short (`med`).
pub fn identity_lines(read: &DetailRead) -> Vec<String> {
    identity_lines_with(read, |effort| {
        crate::picker::effort_label(effort).to_owned()
    })
}

/// [`identity_lines`] with the effort in full words ("medium"): what a
/// screen reader says for it.
pub fn identity_spoken_lines(read: &DetailRead) -> Vec<String> {
    identity_lines_with(read, crate::picker::effort_name)
}

fn identity_lines_with(read: &DetailRead, words: impl Fn(&str) -> String) -> Vec<String> {
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
        (Some(model), Some(effort)) => lines.push(format!("{model} · {} effort", words(&effort))),
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
        // What the list row leaves out: what it does now, its tokens and
        // CPU time (only the measures the daemon reported).
        let now = [
            Some(row.operation()),
            row.tokens.map(|_| tokens_label(row)),
            row.obs
                .cpu_time_ns()
                .map(|_| cpu_time_label(row.obs.cpu_time_ns())),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" · ");
        views.push(KeyedView::new("now", small(now)));
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
                    palette.primary(),
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
