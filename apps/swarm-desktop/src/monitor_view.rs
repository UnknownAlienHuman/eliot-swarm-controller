//! WinUI pane for the swarm monitor.
//!
//! Native controls only: a `ListView` of attention-first rows, text
//! counters, and two small bar sparklines built from `Border`s with numeric
//! labels beside them (a native bar sparkline; no canvas). Control buttons
//! are real `Button`s whose messages already carry the Agent, generation
//! and turn shown on screen, so a click acts on exactly what was displayed.

use windows_reactor::*;

use crate::monitor::{
    self, DetailRead, GRAPH_BARS, Monitor, MonitorRow, Point, bytes_label, cpu_time_label,
    state_name, tokens_label,
};
use crate::theme;
use crate::theme::winui::Palette;

/// Messages the pane emits, prepared by the screen with their bindings.
pub struct MonitorActions {
    pub select: Callback<Option<usize>>,
    pub open: Option<Callback<()>>,
    pub interrupt: Option<Callback<()>>,
    pub stop: Option<Callback<()>>,
    pub force_stop: Option<Callback<()>>,
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

/// Native bar sparkline: one `Border` per point, unknown points drawn as a
/// faint stub so gaps stay visible.
fn sparkline(
    points: &[&Point],
    value: impl Fn(&Point) -> Option<f64>,
    max_hint: f64,
    palette: Palette,
    bar: Brush,
) -> View {
    let values: Vec<Option<f64>> = points.iter().map(|point| value(point)).collect();
    let max = values
        .iter()
        .flatten()
        .copied()
        .fold(max_hint, f64::max)
        .max(f64::EPSILON);
    let height = 36.0;
    let bars: Vec<KeyedView> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let bar = match value {
                Some(value) => Border::new()
                    .width(4.0)
                    .height((value / max * height).clamp(1.0, height))
                    .corner_radius(1.5)
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .background(bar),
                None => Border::new()
                    .width(4.0)
                    .height(2.0)
                    .opacity(0.3)
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .background(palette.outline_variant()),
            };
            KeyedView::new(index, bar)
        })
        .collect();
    Border::new()
        .height(height)
        .width(GRAPH_BARS as f64 * 5.0)
        .horizontal_alignment(HorizontalAlignment::Left)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(1.0)
                .keyed_children(bars),
        )
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

fn action_button(
    label: &str,
    callback: Option<Callback<()>>,
    enabled: bool,
    palette: Palette,
) -> View {
    let available = callback.is_some();
    let button = if label.contains("top") {
        crate::m3::outlined(palette)
    } else {
        crate::m3::tonal(palette)
    }
    .is_enabled(enabled && available)
    .automation_name(label.to_owned());
    let button = match callback {
        Some(callback) => button.on_click(callback),
        None => button,
    };
    button.content(TextBlock::new().text(label.to_owned()))
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
                let cpu_label = match last.and_then(|point| point.cpu_percent) {
                    Some(cpu) => format!(
                        "CPU {cpu:.0}% core-equivalent (100% = one core){}",
                        if last.is_some_and(|point| point.partial) {
                            " · partial: process set changed"
                        } else {
                            ""
                        }
                    ),
                    None => "CPU rate unknown for the latest interval".to_owned(),
                };
                details.push(KeyedView::new("cpu-label", small(cpu_label)));
                details.push(KeyedView::new(
                    "cpu",
                    sparkline(
                        &recent,
                        |point| point.cpu_percent,
                        100.0,
                        palette,
                        palette.primary(),
                    ),
                ));
                details.push(KeyedView::new(
                    "rss-label",
                    small(format!(
                        "RSS {} (sum over the Agent's sampled processes; not host memory share)",
                        bytes_label(last.and_then(|point| point.rss_bytes))
                    )),
                ));
                details.push(KeyedView::new(
                    "rss",
                    sparkline(
                        &recent,
                        |point| point.rss_bytes.map(|bytes| bytes as f64),
                        0.0,
                        palette,
                        palette.tertiary(),
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
        details.push(KeyedView::new(
            "actions",
            // Conversation and turn actions, then lifecycle: two rows fit the
            // monitor's width.
            StackPanel::new().spacing(theme::spacing::SM).children((
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        action_button("Open conversation", actions.open, true, palette),
                        action_button("Interrupt", actions.interrupt, controls_enabled, palette),
                    )),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        action_button("Stop", actions.stop, controls_enabled, palette),
                        action_button("Force stop", actions.force_stop, controls_enabled, palette),
                    )),
            )),
        ));
        // One row per possible manager, each button already bound to that
        // row's generation: no selection state can disagree with the screen.
        let moves: Vec<KeyedView> = reparent
            .rows
            .into_iter()
            .map(|row| {
                let agent_label = format!("Move {id} under {}", row.id);
                let children_label = format!("Move children of {id} under {}", row.id);
                KeyedView::new(
                    row.id.clone(),
                    StackPanel::new().spacing(2.0).children((
                        small(row.label),
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(theme::spacing::SM)
                            .children((
                                action_button(
                                    &agent_label,
                                    row.move_agent,
                                    controls_enabled,
                                    palette,
                                ),
                                action_button(
                                    &children_label,
                                    row.move_children,
                                    controls_enabled,
                                    palette,
                                ),
                            )),
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
