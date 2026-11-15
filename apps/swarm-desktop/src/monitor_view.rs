//! WinUI pane for the swarm monitor.
//!
//! Native controls only: a `ListView` of attention-first rows, text
//! counters, and two bar charts built from `Border`s (CPU and RSS, each with
//! its latest value and a labelled scale; no canvas). Control buttons are
//! real `Button`s whose messages already carry the Agent, generation and
//! turn shown on screen, so a click acts on exactly what was displayed. Only
//! the actions that apply to the shown Agent appear; Force stop asks for an
//! inline confirmation bound to the Agent and generation of its first click.

use windows_reactor::*;

use crate::monitor::{
    self, DetailRead, GRAPH_BARS, Monitor, MonitorRow, bytes_label, cpu_time_label, state_name,
    tokens_label,
};
use crate::theme;
use crate::theme::winui::Palette;

/// Messages the pane emits, prepared by the screen with their bindings. A
/// `None` action does not apply to the shown Agent and is not offered.
pub struct MonitorActions {
    pub select: Callback<Option<usize>>,
    pub open: Option<Callback<()>>,
    pub interrupt: Option<Callback<()>>,
    pub stop: Option<Callback<()>>,
    /// Asks for the Force stop confirmation.
    pub force_stop: Option<Callback<()>>,
    /// The pending Force stop confirmation, if one was asked.
    pub confirm_force: Option<ForceConfirm>,
    pub resume: Option<Callback<()>>,
    pub detach: Option<Callback<()>>,
}

/// The inline Force stop confirmation: what it stops, and both answers.
pub struct ForceConfirm {
    pub question: String,
    pub confirm: Callback<()>,
    pub cancel: Callback<()>,
}

/// The "Change manager" section of the selected Agent.
pub struct ReparentData<'a> {
    pub rows: Vec<MoveRow>,
    pub status: &'a str,
}

/// One possible new manager with its two bound moves.
pub struct MoveRow {
    pub id: String,
    pub label: String,
    pub move_agent: Option<Callback<()>>,
    pub move_children: Option<Callback<()>>,
}

fn small(text: impl Into<String>) -> TextBlock {
    TextBlock::new()
        .text(text.into())
        .font_size(theme::type_scale::LABEL_MEDIUM.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

fn utc_clock(ms: i64) -> String {
    let seconds = ms.div_euclid(1000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02} UTC",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn row_view(row: &MonitorRow, palette: Palette) -> View {
    let manager = row
        .parent
        .as_deref()
        .map(|parent| format!(" · under {parent}"))
        .unwrap_or_default();
    // ListViewItem exposes no content alignment in Reactor 0.100.0, so the
    // row takes the pane's fixed inner width to right-align the chip.
    let row_width = theme::layout::MONITOR_WIDTH - 2.0 * theme::spacing::LG - 40.0;
    ListViewItem::new()
        .automation_name(format!(
            "Agent {}, {}, {}{}, {}",
            row.id,
            state_name(row.state),
            row.harness,
            row.parent
                .as_deref()
                .map(|parent| format!(", under {parent}"))
                .unwrap_or_default(),
            row.operation()
        ))
        .content(
            StackPanel::new()
                .width(row_width)
                .spacing(2.0)
                .margin(Thickness::xy(0.0, theme::spacing::SM))
                .children((
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(theme::spacing::SM)
                        .children((
                            crate::m3::text(
                                format!("{} · {}{manager}", row.id, row.harness),
                                theme::type_scale::TITLE_SMALL,
                            )
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                            Border::new()
                                .grid_column(1)
                                .content(crate::m3::status_chip(palette, state_name(row.state))),
                        )),
                    small(row.operation()).opacity(theme::MUTED_OPACITY),
                    small(format!(
                        "RSS {} · {} · {} · {}",
                        bytes_label(row.obs.rss_bytes()),
                        cpu_time_label(row.obs.cpu_time_ns()),
                        row.obs.freshness(),
                        tokens_label(row)
                    ))
                    .opacity(theme::MUTED_OPACITY),
                )),
        )
}

/// Plot height in DIPs; the bars stretch across the card width.
const PLOT_HEIGHT: f64 = 64.0;
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
                    .height((value / ceiling * PLOT_HEIGHT).clamp(2.0, PLOT_HEIGHT))
                    .background(bar),
                None => Border::new()
                    .height(2.0)
                    .opacity(0.3)
                    .background(palette.outline_variant()),
            };
            KeyedView::new(
                index,
                view.margin(Thickness::xy(1.0, 0.0))
                    .corner_radius(CornerRadius::new(2.0, 2.0, 0.0, 0.0))
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .grid_column((first + index) as i32),
            )
        })
        .collect();
    StackPanel::new().spacing(theme::spacing::XS).children((
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .children((
                crate::m3::muted(title, theme::type_scale::LABEL_MEDIUM)
                    .vertical_alignment(VerticalAlignment::Bottom),
                crate::m3::text(latest, theme::type_scale::TITLE_MEDIUM).grid_column(1),
            )),
        Border::new()
            .background(palette.surface_container_highest())
            .corner_radius(theme::shape::SM)
            .padding(Thickness::uniform(theme::spacing::XS))
            .content(
                Grid::new()
                    .height(PLOT_HEIGHT)
                    .columns((0..GRAPH_BARS).map(|_| GridLength::STAR))
                    .keyed_children(bars),
            ),
        small(format!("{scale} · {note}")).opacity(theme::MUTED_OPACITY),
    ))
}

fn detail_lines(read: &DetailRead) -> Vec<String> {
    let agent = &read.agent.agent;
    let effective = agent.effective.as_ref();
    let field = |name: &str| {
        effective
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("not read back")
            .to_owned()
    };
    let mut lines = vec![
        format!(
            "{} · gen {} · {} · {}{}",
            agent.id,
            agent.generation,
            format!("{:?}", agent.state).to_lowercase(),
            agent.harness,
            if agent.observe_only {
                " · observe-only"
            } else {
                ""
            }
        ),
        format!(
            "profile {} · effective model {} · effort {}",
            agent.profile,
            field("model"),
            field("effort")
        ),
        format!(
            "active turn {} · pending requests {}",
            agent.active_turn.as_deref().unwrap_or("none"),
            agent.pending.len()
        ),
    ];
    match &read.usage {
        Ok(usage) => {
            lines.push(format!(
                "usage: {} turn(s) counted, {} without usage, {} unknown excluded, {} running excluded; history {} ({}){}",
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
                    "  {}: {} turn(s) · input {} · output {} · total {} tokens",
                    scope.scope,
                    scope.turns_counted,
                    counter(scope.totals.input_tokens),
                    counter(scope.totals.output_tokens),
                    counter(scope.totals.total_tokens),
                ));
            }
            for cost in &usage.sessions.cost_by_unit {
                lines.push(format!(
                    "  cost [{}]: {} over {} snapshot(s)",
                    cost.unit, cost.total_amount, cost.snapshots
                ));
            }
        }
        Err(error) => lines.push(format!("usage.summary unavailable: {error}")),
    }
    lines
}

fn counter(value: Option<i64>) -> String {
    value.map_or_else(|| "—".to_owned(), |value| value.to_string())
}

/// The M3 treatment of one monitor action.
#[derive(Clone, Copy)]
enum ActionStyle {
    Tonal,
    /// A destructive action before confirmation (error container).
    ErrorTonal,
    /// The confirmed destructive action (error).
    ErrorFilled,
    Text,
}

/// One monitor action: its shown text, accessible name, binding and style.
/// Without a callback it does not apply and is not shown.
struct Action {
    text: String,
    name: String,
    callback: Option<Callback<()>>,
    style: ActionStyle,
    enabled: bool,
}

impl Action {
    fn new(
        text: impl Into<String>,
        callback: Option<Callback<()>>,
        style: ActionStyle,
        enabled: bool,
    ) -> Self {
        let text = text.into();
        Self {
            name: text.clone(),
            text,
            callback,
            style,
            enabled,
        }
    }

    /// A fuller accessible name than the shown text.
    fn named(mut self, name: String) -> Self {
        self.name = name;
        self
    }
}

/// The applicable actions in display order, as keyed buttons.
fn action_row(actions: Vec<Action>, palette: Palette) -> View {
    let buttons: Vec<KeyedView> = actions
        .into_iter()
        .filter_map(|action| {
            let callback = action.callback?;
            let button = match action.style {
                ActionStyle::Tonal => crate::m3::tonal(palette),
                ActionStyle::ErrorTonal => Button::new().resource_overrides(palette.error_tonal()),
                ActionStyle::ErrorFilled => {
                    Button::new().resource_overrides(palette.error_filled())
                }
                ActionStyle::Text => crate::m3::text_button(palette),
            };
            Some(KeyedView::new(
                action.text.clone(),
                button
                    .is_enabled(action.enabled)
                    .automation_name(action.name)
                    .on_click(callback)
                    .content(TextBlock::new().text(action.text)),
            ))
        })
        .collect();
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(theme::spacing::SM)
        .keyed_children(buttons)
}

/// The monitor pane for the right-hand column.
pub fn monitor_pane(
    monitor: &Monitor,
    selected: Option<&str>,
    published_rows: usize,
    controls_enabled: bool,
    reparent: ReparentData<'_>,
    actions: MonitorActions,
    palette: Palette,
) -> View {
    // Only highlight a row index that already existed before this refresh,
    // so the native SelectedIndex never runs ahead of the items it indexes
    // (an index past them fails with E_INVALIDARG, which Reactor treats as
    // fatal: 0xC0000409; see `ShellMsg::AgentsLoaded` in `ui`).
    let selected_index = selected
        .and_then(|id| monitor.rows.iter().position(|row| row.id == id))
        .filter(|index| *index < published_rows);
    let rows: Vec<KeyedView> = monitor
        .rows
        .iter()
        .map(|row| KeyedView::new(row.id.clone(), row_view(row, palette)))
        .collect();
    let status = match monitor.read_at_ms {
        Some(at) => format!(
            "Refreshed {} · every {} s while open · {} refresh(es)",
            utc_clock(at),
            monitor::PERIOD_MS / 1000,
            monitor.refreshes
        ),
        None => "Reading the swarm…".to_owned(),
    };
    let errors = if monitor.errors.is_empty() {
        String::new()
    } else {
        format!("Read errors: {}", monitor.errors.join("; "))
    };

    let mut details: Vec<KeyedView> = Vec::new();
    match (selected, monitor.detail.as_ref()) {
        (None, _) => details.push(KeyedView::new(
            "hint",
            small("Select an Agent for details, usage and controls.").opacity(theme::MUTED_OPACITY),
        )),
        (Some(id), Some(Ok(read))) if read.agent.agent.id == id => {
            for (index, line) in detail_lines(read).into_iter().enumerate() {
                details.push(KeyedView::new(format!("line{index}"), small(line)));
            }
        }
        (Some(_), Some(Err(error))) => {
            details.push(KeyedView::new(
                "error",
                small(format!("Detail read failed: {error}")).foreground(palette.error()),
            ));
        }
        (Some(_), _) => details.push(KeyedView::new("loading", small("Reading details…"))),
    }
    if let Some(id) = selected {
        let row = monitor.rows.iter().find(|row| row.id == id);
        if let Some(row) = row {
            details.push(KeyedView::new("fresh", small(row.obs.freshness())));
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
                details.push(KeyedView::new(
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
                details.push(KeyedView::new(
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
                details.push(KeyedView::new(
                    "series",
                    small(format!(
                        "{} distinct sample(s) kept in this window only (UI history, capped at {}).",
                        series.points().len(),
                        monitor::RING_CAPACITY
                    ))
                    .opacity(theme::MUTED_OPACITY),
                ));
            }
            _ => details.push(KeyedView::new(
                "no-series",
                small("No process samples for this Agent in the current rows.")
                    .opacity(theme::MUTED_OPACITY),
            )),
        }
        let observe_only = monitor
            .detail
            .as_ref()
            .and_then(|detail| detail.as_ref().ok())
            .filter(|read| read.agent.agent.id == id)
            .is_some_and(|read| read.agent.agent.observe_only);
        if observe_only {
            details.push(KeyedView::new(
                "unsupported",
                small("Controls unavailable: observe-only Agents reject native control verbs.")
                    .foreground(palette.error()),
            ));
        }
        // Conversation and turn actions, then lifecycle: two rows fit the
        // monitor's width. Stop stays tonal; Force stop takes the error role
        // and asks first.
        let confirm: View = match actions.confirm_force {
            Some(confirm) => Border::new()
                .background(palette.error_container())
                .corner_radius(theme::shape::MD)
                .padding(Thickness::uniform(theme::spacing::MD))
                .content(StackPanel::new().spacing(theme::spacing::SM).children((
                    small(confirm.question).foreground(palette.on_error_container()),
                    action_row(
                        vec![
                            Action::new("Cancel", Some(confirm.cancel), ActionStyle::Text, true),
                            Action::new(
                                "Force stop",
                                Some(confirm.confirm),
                                ActionStyle::ErrorFilled,
                                controls_enabled,
                            ),
                        ],
                        palette,
                    ),
                ))),
            None => View::empty(),
        };
        details.push(KeyedView::new(
            "actions",
            StackPanel::new().spacing(theme::spacing::SM).children((
                action_row(
                    vec![
                        Action::new("Open conversation", actions.open, ActionStyle::Tonal, true),
                        Action::new(
                            "Interrupt",
                            actions.interrupt,
                            ActionStyle::Tonal,
                            controls_enabled,
                        ),
                    ],
                    palette,
                ),
                action_row(
                    vec![
                        Action::new("Stop", actions.stop, ActionStyle::Tonal, controls_enabled),
                        Action::new(
                            "Force stop",
                            actions.force_stop,
                            ActionStyle::ErrorTonal,
                            controls_enabled,
                        ),
                        Action::new(
                            "Resume",
                            actions.resume,
                            ActionStyle::Tonal,
                            controls_enabled,
                        ),
                        Action::new(
                            "Detach",
                            actions.detach,
                            ActionStyle::Tonal,
                            controls_enabled,
                        ),
                    ],
                    palette,
                ),
                confirm,
            )),
        ));
        // One row per possible manager, each button already bound to that
        // row's generation: no selection state can disagree with the screen.
        let moves: Vec<KeyedView> = reparent
            .rows
            .into_iter()
            .map(|row| {
                KeyedView::new(
                    row.id.clone(),
                    StackPanel::new().spacing(2.0).children((
                        small(row.label),
                        action_row(
                            vec![
                                Action::new(
                                    format!("Move {id} here"),
                                    row.move_agent,
                                    ActionStyle::Tonal,
                                    controls_enabled,
                                )
                                .named(format!("Move {id} under {}", row.id)),
                                Action::new(
                                    "Move its children here",
                                    row.move_children,
                                    ActionStyle::Tonal,
                                    controls_enabled,
                                )
                                .named(format!("Move the children of {id} under {}", row.id)),
                            ],
                            palette,
                        ),
                    )),
                )
            })
            .collect();
        details.push(KeyedView::new(
            "reparent",
            StackPanel::new().spacing(theme::spacing::XS).children((
                TextBlock::new()
                    .text("Change manager")
                    .font_weight(FontWeight::SEMI_BOLD)
                    .automation_heading_level(AutomationHeadingLevel::Level3),
                small(if moves.is_empty() {
                    "No other live Agent is shown to become the manager.".to_owned()
                } else {
                    format!(
                        "Move {id} with its subtree, or only its children, under a live Agent below. The daemon checks cycles, fanout limits, observe-only Agents and both generations."
                    )
                })
                .opacity(theme::MUTED_OPACITY),
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(moves),
                small(reparent.status.to_owned()),
            )),
        ));
    }

    Grid::new()
        .rows([
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::STAR,
        ])
        .row_spacing(theme::spacing::MD)
        .children((
            StackPanel::new().grid_row(0).spacing(2.0).children((
                crate::m3::title("Monitor", theme::type_scale::TITLE_LARGE),
                small(status).opacity(theme::MUTED_OPACITY),
                small(errors).foreground(palette.error()),
            )),
            small(monitor.counter_line())
                .opacity(theme::MUTED_OPACITY)
                .grid_row(1),
            ListView::new()
                .grid_row(2)
                .max_height(320.0)
                .selected_index(selected_index)
                .automation_name("Monitored Agents")
                .on_selection_changed(actions.select)
                .collection_slot(ListViewSlot::Items, rows),
            ScrollViewer::new()
                .grid_row(3)
                .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                .automation_name("Agent details")
                .content(
                    crate::m3::card(palette).content(
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .keyed_children(details),
                    ),
                ),
        ))
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
