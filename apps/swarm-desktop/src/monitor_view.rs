//! WinUI pieces of the right panel: the monitor's Agent rows, the CPU and
//! RSS charts (bars built from `Border`s, each with its latest value and a
//! labelled scale; no canvas) and the usage lines. Buttons carry the Agent
//! and generation shown on screen, so a click acts on exactly what was
//! displayed.

use eliot_core::api::{
    AgentGetResponse, InstructionFileStatus, InstructionReadStatus, InstructionSourceStatus,
};
use std::fmt::Write as _;
use windows_reactor::*;

use crate::monitor::{
    self, DetailRead, GRAPH_BARS, Monitor, MonitorRow, bytes_label, cpu_time_label, group,
    state_words, tokens_label,
};
use crate::ui::tokens;
use crate::views::MaybeContent;

pub fn project_instruction_lines(reply: &AgentGetResponse) -> Option<Vec<String>> {
    let delivery = reply.instruction_delivery.as_ref()?;
    let Some(workspace) = delivery.source_workspace.as_deref() else {
        return Some(vec!["Project instruction scan unavailable.".to_owned()]);
    };
    let mut lines = Vec::new();
    for file in delivery
        .files
        .iter()
        .filter(|file| file.status != InstructionFileStatus::Missing)
    {
        let mut line = file.path.clone();
        if delivery.primary_path == file.path {
            line.push_str(" · primary");
        }
        if let Some(size) = file.size_bytes {
            let _ = write!(line, " · {size} bytes");
        }
        let file_status = match file.status {
            InstructionFileStatus::Present | InstructionFileStatus::Missing => None,
            InstructionFileStatus::Unreadable => Some("File read failed"),
            InstructionFileStatus::NotRegular => Some("File type unsupported"),
            InstructionFileStatus::OutsideWorkspace => Some("Path outside the workspace"),
            InstructionFileStatus::TooLarge => Some("File exceeds the scan limit"),
            InstructionFileStatus::Binary => Some("Binary contents"),
            InstructionFileStatus::WorkspaceUnavailable => Some("Workspace unavailable"),
        };
        if let Some(file_status) = file_status {
            let _ = write!(line, "\n{file_status}");
        }
        let status = match file.native_read_status {
            InstructionReadStatus::Documented => "Documented native instructions",
            InstructionReadStatus::Partial => "Exceeds the native instruction byte limit",
            InstructionReadStatus::Conditional => "Native reading depends on harness settings",
            InstructionReadStatus::NotNative => "Outside the harness's native instruction lookup",
            InstructionReadStatus::Unknown => "Native reading unverified",
        };
        let _ = write!(line, "\n{status}");
        if let Some(version) = &file.native_read_version {
            let _ = write!(line, " · {version}");
        }
        let sources = match file.delivery_status {
            InstructionSourceStatus::NotNeeded => None,
            InstructionSourceStatus::Complete if file.delivery_redacted => {
                Some("Swarm attached the complete redacted text to the input")
            }
            InstructionSourceStatus::Truncated if file.delivery_redacted => {
                Some("Swarm attached part of the redacted text to the input")
            }
            InstructionSourceStatus::Complete => {
                Some("Swarm attached the complete file to the input")
            }
            InstructionSourceStatus::Truncated => {
                Some("Swarm attached part of the file to the input")
            }
            InstructionSourceStatus::Unavailable => {
                Some("Swarm could not attach the file to the input")
            }
        };
        if let Some(sources) = sources {
            let _ = write!(line, "\n{sources}");
        }
        if let Some(sha256) = &file.sha256 {
            let _ = write!(line, "\nFile SHA-256 {sha256}");
        }
        lines.push(line);
    }
    if lines.is_empty() {
        lines.push("No project instruction files found at launch.".to_owned());
    }
    if reply.agent.workspace.as_deref().map(crate::nav::path_key)
        != Some(crate::nav::path_key(workspace))
    {
        lines.insert(
            0,
            format!("Scanned {}", eliot_core::display_path(workspace)),
        );
    }
    Some(lines)
}

/// A note: secondary size, wrapped and selectable (callers lower its
/// emphasis where it is supporting text).
fn small(text: impl Into<String>) -> TextBlock {
    crate::controls::text(text, tokens::text::CAPTION)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// How a Stop Agent button shows now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopButton {
    /// It can be pressed.
    Ready,
    /// Another action is settling first.
    Waiting,
    /// This Agent's Stop was sent and has not settled.
    Stopping,
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
#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
fn chart_card(
    title: &str,
    latest: String,
    values: &[Option<f64>],
    ceiling: f64,
    scale: String,
    note: String,
    bar: Brush,
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
                    .opacity(tokens::state::PLOT_STUB_OPACITY)
                    .background(ThemeBrush::CardStroke),
            };
            KeyedView::new(
                index,
                view.margin(Thickness::xy(tokens::layout::PLOT_GAP, 0.0))
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .grid_column((first + index) as i32),
            )
        })
        .collect();
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .children((
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .children((
                    crate::controls::text(title, tokens::text::CAPTION)
                        .vertical_alignment(VerticalAlignment::Bottom),
                    crate::controls::text(latest, tokens::text::HEADER).grid_column(1),
                )),
            // The plot well on the window's base: one step from the card in
            // every theme.
            Border::new()
                .background(ThemeBrush::SolidBackground)
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::uniform(tokens::spacing::SM))
                .content(
                    Grid::new()
                        .height(PLOT_HEIGHT)
                        .columns((0..GRAPH_BARS).map(|_| GridLength::STAR))
                        .keyed_children(bars),
                ),
            small(format!("{scale} · {note}")),
        ))
        .into()
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

/// The heavy-work rollup for the Agent Details panel, in words; `None`
/// when no heavy episode was recorded (the section is then hidden). Only
/// committed rollups render: never full commands or paths.
pub fn heavy_work(read: &DetailRead) -> Option<String> {
    let heavy = read
        .agent
        .heavy
        .as_ref()
        .filter(|heavy| heavy.episodes > 0)?;
    let what = heavy
        .latest_category
        .as_deref()
        .map(|category| format!(", latest {}", category.replace('_', " ")))
        .unwrap_or_default();
    Some(format!(
        "{}{what}, {:.1} s of CPU",
        eliot_core::count_words(heavy.episodes, "episode", "episodes"),
        heavy.total_cpu_ms as f64 / 1000.0
    ))
}

/// Normalized token usage, context fill, and cost for the selected Agent.
pub fn usage_lines(read: &DetailRead) -> Vec<String> {
    let context = read
        .agent
        .session_usage
        .as_ref()
        .and_then(eliot_core::SessionUsage::context_tokens);
    // An unknown fill is the Used row's "not reported", not a second line.
    let mut lines: Vec<String> = context
        .map(|(used, window)| {
            format!(
                "Context {:.1}% filled · {} of {} tokens",
                100.0 * used as f64 / window as f64,
                group(used),
                group(window)
            )
        })
        .into_iter()
        .collect();
    match &read.usage {
        Ok(usage) => {
            let turns = &usage.turns;
            if let Some(total) = turns.reported_total() {
                let qualifier = if turns.turns_missing.total_tokens > 0 {
                    "reported "
                } else {
                    ""
                };
                lines.push(format!(
                    "{} {qualifier}tokens over {}",
                    group(total),
                    eliot_core::count_words(turns.turns_counted as u64, "turn", "turns")
                ));
                for (name, counter) in [
                    (
                        "uncached input",
                        |t: &eliot_core::api::TurnCounterTotals| t.input_tokens,
                    ),
                    ("cached input", |t: &eliot_core::api::TurnCounterTotals| {
                        t.cache_read_tokens
                    }),
                    ("output", |t: &eliot_core::api::TurnCounterTotals| {
                        t.output_tokens
                    }),
                    ("reasoning", |t: &eliot_core::api::TurnCounterTotals| {
                        t.reasoning_tokens
                    }),
                ]
                    as [(&str, fn(&eliot_core::api::TurnCounterTotals) -> Option<i64>); 4]
                {
                    if let Some(count) = turns
                        .by_scope
                        .iter()
                        .filter_map(|scope| counter(&scope.totals))
                        .reduce(i64::saturating_add)
                    {
                        lines.push(format!("{} {name} tokens", group(count)));
                    }
                }
            } else {
                lines.push("No token usage reported yet.".to_owned());
            }
            if usage.observed_native_agents > 0 {
                lines.push(format!(
                    "{} included",
                    eliot_core::count_words(
                        usage.observed_native_agents as u64,
                        "native Agent",
                        "native Agents"
                    )
                ));
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
pub fn resource_views(monitor: &Monitor, id: &str) -> Vec<KeyedView> {
    let mut views: Vec<KeyedView> = Vec::new();
    let row = monitor.row(id);
    // An Agent that ended with every process exited has nothing to sample:
    // one line says so.
    if row.is_some_and(|row| row.ended() && row.obs.live_processes() == 0) {
        views.push(KeyedView::new("ended", small("No live process.")));
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
        views.push(KeyedView::new("fresh", small(row.obs.freshness())));
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
                    ThemeBrush::Accent.into(),
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
                    ThemeBrush::Accent.into(),
                ),
            ));
            views.push(KeyedView::new(
                "series",
                small(format!(
                    "{} kept while this panel is open (at most {}).",
                    eliot_core::count_words(series.points().len() as u64, "sample", "samples"),
                    monitor::RING_CAPACITY
                )),
            ));
        }
        // The freshness line already says nothing was sampled.
        _ if row.is_some() => {}
        _ => views.push(KeyedView::new(
            "no-series",
            small("No process samples yet."),
        )),
    }
    views
}

/// One entry of the Agents tree, computed by the window from the last
/// snapshot (at its read time, not the clock's), so the tree
/// ([`MonitorList`]) is redrawn only when a snapshot or a choice changes it.
#[derive(Clone, PartialEq)]
pub enum MonitorEntry {
    /// A group's label ("Working · 3 of 3") and its key.
    Section(String, String),
    /// Recently stopped's toggle: its label and whether it is open.
    Stopped(String, bool),
    Row(Box<RowEntry>),
    /// The closing line ("No Agents to show").
    Note(String),
}

/// One row of the Agents tree (`Agents.dc.html` `.trow`): its Agent and
/// what its columns say.
#[derive(Clone, PartialEq)]
pub struct RowEntry {
    pub row: MonitorRow,
    pub title: String,
    /// The project it works in (a top-level row only).
    pub project: String,
    /// `Harness · model`.
    pub model: String,
    /// What it does now, why it ended, or what it needs.
    pub activity: String,
    /// The activity is an attention report (the warning mark).
    pub attention: bool,
    pub cpu: String,
    pub memory: String,
    /// The tooltip and accessible name's extra words.
    pub words: String,
    pub selected: bool,
    /// Its children's toggle: whether they are shown (none: no children).
    pub children: Option<bool>,
    /// Nested under its parent.
    pub kid: bool,
    /// A native subagent (observed, not controlled).
    pub observed: bool,
    /// Resume for a resumable row: whether it is enabled.
    pub resume: Option<bool>,
    /// Stop for a live controlled row, as it shows now.
    pub stop: Option<StopButton>,
}

/// The tree's input: its entries and the callbacks (each from a
/// captureless mapper, so equal across window renders).
#[derive(Clone, PartialEq)]
pub struct MonitorListInput {
    pub entries: Vec<MonitorEntry>,
    /// Open a row's conversation and details.
    pub open: Callback<String>,
    /// Resume a row (its Agent and the generation shown).
    pub resume: Callback<(String, i64)>,
    /// Stop a row's Agent (its id and the generation shown).
    pub stop: Callback<(String, i64)>,
    /// Show or hide a row's children.
    pub children: Callback<String>,
    /// Show or hide Recently stopped.
    pub stopped: Callback<()>,
    /// The main area is narrow (the composer stacks there too): the
    /// columns share its width differently ([`row_columns`]).
    pub narrow: bool,
}

/// The Agents tree as its own component: a window render for anything but
/// a snapshot or a choice compares equal and leaves the tree as it is.
pub struct MonitorList;

impl Component for MonitorList {
    type Input = MonitorListInput;
    type Message = ();

    fn create(_input: &MonitorListInput, _context: &ComponentContext<Self>) -> Self {
        Self
    }

    fn view(&self, input: &MonitorListInput, _context: &mut ViewContext<Self>) -> View {
        let mut entries: Vec<KeyedView> = vec![KeyedView::new("head", head_row(input.narrow))];
        entries.extend(input.entries.iter().map(|entry| entry_view(entry, input)));
        StackPanel::new().keyed_children(entries).into()
    }
}

/// The tree's columns inside a row's button. Each row is its own Grid and
/// WinUI has no shared size groups, so the text columns share the row's
/// width by star to stay aligned under the header at any width (an Auto
/// column would differ row by row, and a long status line would take the
/// task's room); CPU and Memory hold fixed-size numbers. A wide main area
/// shows Task, Model, Activity, CPU and Memory, the task the most. A narrow
/// one has no Model column: the model stands in the task's second line, as
/// the reference apps' list rows put their details under the title, and
/// Activity takes a smaller share that holds a short status.
fn row_columns(narrow: bool) -> Vec<GridLength> {
    let mut columns = vec![GridLength::Star(tokens::page::FLEET_TASK_SHARE)];
    if narrow {
        columns.push(GridLength::Star(tokens::page::FLEET_NARROW_ACTIVITY_SHARE));
    } else {
        columns.push(GridLength::Star(tokens::page::FLEET_TEXT_SHARE));
        columns.push(GridLength::Star(tokens::page::FLEET_TEXT_SHARE));
    }
    columns.push(GridLength::Pixel(tokens::page::FLEET_NUMBER));
    columns.push(GridLength::Pixel(tokens::page::FLEET_NUMBER));
    columns
}

/// Activity's column; CPU and Memory follow it.
fn activity_column(narrow: bool) -> i32 {
    if narrow { 1 } else { 2 }
}

/// A row's outer columns: the chevron, the row's button, its action.
fn outer_columns() -> [GridLength; 3] {
    [
        GridLength::Pixel(tokens::height::CONTROL),
        GridLength::STAR,
        GridLength::Pixel(tokens::height::CONTROL),
    ]
}

/// A number cell (CPU, Memory): right-aligned, muted.
fn number(text: &str, column: i32) -> View {
    crate::controls::text(text.to_owned(), tokens::text::BODY)
        .grid_column(column)
        .horizontal_alignment(HorizontalAlignment::Right)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// The header row (`.trow.head`): the column names in `label-medium`, a
/// hairline under them.
fn head_row(narrow: bool) -> View {
    let at = activity_column(narrow);
    let label = |text: &str, column: i32, right: bool| -> View {
        crate::controls::text(text.to_owned(), tokens::text::LABEL)
            .grid_column(column)
            .horizontal_alignment(if right {
                HorizontalAlignment::Right
            } else {
                HorizontalAlignment::Left
            })
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    Border::new()
        .min_height(tokens::height::CONTROL)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
        .automation_name(if narrow {
            "Columns: Task, Activity, CPU, Memory"
        } else {
            "Columns: Task, Model, Activity, CPU, Memory"
        })
        .content(
            Grid::new()
                .columns(outer_columns())
                .keyed_children(crate::views::slots([
                    None,
                    Some(
                        // Inset as a row's content is inside its stock button
                        // (border 1 + padding 11), so each name stands over
                        // its column.
                        Grid::new()
                            .grid_column(1)
                            .margin(Thickness::xy(tokens::spacing::INSET, 0.0))
                            .columns(row_columns(narrow))
                            .column_spacing(tokens::spacing::SM)
                            .keyed_children(crate::views::slots([
                                // Past the rows' mark slot, over their titles.
                                Some(
                                    crate::controls::text("Task".to_owned(), tokens::text::LABEL)
                                        .margin(Thickness::new(
                                            tokens::layout::MARK_SLOT + tokens::spacing::SM,
                                            0.0,
                                            0.0,
                                            0.0,
                                        ))
                                        .vertical_alignment(VerticalAlignment::Center)
                                        .into(),
                                ),
                                (!narrow).then(|| label("Model", 1, false)),
                                Some(label("Activity", at, false)),
                                Some(label("CPU", at + 1, true)),
                                Some(label("Memory", at + 2, true)),
                            ]))
                            .into(),
                    ),
                ])),
        )
        .into()
}

/// One tree row (`.trow`): the chevron of its children, then the row (its
/// state mark, title and project; model, or in a narrow main area the model
/// after the project; activity; CPU; memory), which opens its
/// conversation, then Resume or Stop.
fn fleet_row(entry: &RowEntry, input: &MonitorListInput) -> View {
    let row = &entry.row;
    // Every row keeps the mark's slot, a stopped one too, so each title
    // starts where the header's Task does.
    let mut task: Vec<KeyedView> = vec![KeyedView::new(
        "mark",
        Border::new()
            .width(tokens::layout::MARK_SLOT)
            .vertical_alignment(VerticalAlignment::Center)
            .maybe_content(crate::ui::page_ui::agent_mark(
                row.state,
                row.status_line.as_deref(),
            )),
    )];
    // The title takes the task column's whole width and its project stands
    // under it (as the chat header puts the project under the title), so a
    // short project name no longer holds a share the title needs.
    let mut names: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        crate::controls::text(entry.title.clone(), tokens::page::HEADING)
            .text_trimming(TextTrimming::CharacterEllipsis),
    )];
    // The project and the model in one line ("repo · Claude Code · haiku
    // 5.5"): a narrow row, which has no Model column, shows it
    // under the title (trimmed at its end); the row's name and tooltip always
    // carry it whole.
    let project = (!entry.observed && !entry.project.is_empty()).then_some(entry.project.as_str());
    let model = (!entry.model.is_empty()).then_some(entry.model.as_str());
    let details = project
        .into_iter()
        .chain(model)
        .collect::<Vec<_>>()
        .join(" · ");
    let second = if input.narrow {
        (!details.is_empty()).then(|| details.clone())
    } else {
        project.map(str::to_owned)
    };
    if let Some(second) = second {
        names.push(KeyedView::new(
            "project",
            crate::controls::secondary(second).text_trimming(TextTrimming::CharacterEllipsis),
        ));
    }
    task.push(KeyedView::new(
        "names",
        StackPanel::new()
            .grid_column(1)
            .vertical_alignment(VerticalAlignment::Center)
            .keyed_children(names),
    ));
    if entry.observed {
        task.push(KeyedView::new(
            "observed",
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(crate::controls::badge("observed")),
        ));
    }
    let at = activity_column(input.narrow);
    let activity = crate::controls::text(entry.activity.clone(), tokens::text::BODY)
        .grid_column(at)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let activity = if entry.attention {
        activity.foreground(ThemeBrush::AccentText)
    } else {
        activity
    };
    let content = Grid::new()
        .columns(row_columns(input.narrow))
        .column_spacing(tokens::spacing::SM)
        .keyed_children(crate::views::slots([
            Some(
                Grid::new()
                    .margin(Thickness::new(
                        if entry.kid {
                            tokens::layout::TREE_INDENT
                        } else {
                            0.0
                        },
                        0.0,
                        0.0,
                        0.0,
                    ))
                    .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .keyed_children(task)
                    .into(),
            ),
            (!input.narrow).then(|| {
                crate::controls::secondary(entry.model.clone())
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .into()
            }),
            Some(activity.into()),
            Some(number(&entry.cpu, at + 1)),
            Some(number(&entry.memory, at + 2)),
        ]));
    let open = input.open.clone();
    let id = row.id.clone();
    let name = format!(
        "{}, {}, {}, {}{}",
        entry.title,
        row.status_words(),
        details,
        entry.activity,
        entry.words
    );
    let button = crate::controls::row(entry.selected)
        .grid_column(1)
        .automation_name(name.clone())
        .on_click(move || {
            open.call(id.clone());
        })
        .content(content)
        .tooltip(name);
    let chevron: Option<View> = entry.children.map(|open| {
        let (children, id) = (input.children.clone(), row.id.clone());
        crate::controls::icon_button()
            .automation_name(format!(
                "{} the workers of {}",
                if open { "Hide" } else { "Show" },
                entry.title
            ))
            .on_click(move || {
                children.call(id.clone());
            })
            .content(crate::controls::small_icon(if open {
                crate::controls::glyph::CHEVRON_DOWN
            } else {
                crate::controls::glyph::CHEVRON_RIGHT
            }))
            .into()
    });
    let action: Option<View> = if let Some(enabled) = entry.resume {
        let (resume, id, generation) = (input.resume.clone(), row.id.clone(), row.generation);
        Some(
            crate::controls::icon_button()
                .is_enabled(enabled)
                .automation_name(format!("Resume {} (run {generation})", entry.title))
                .on_click(move || {
                    resume.call((id.clone(), generation));
                })
                .content(crate::controls::icon(crate::controls::glyph::RESUME))
                .into(),
        )
    } else if let Some(state) = entry.stop {
        let (stop, id, generation) = (input.stop.clone(), row.id.clone(), row.generation);
        Some(
            crate::controls::icon_button()
                .is_enabled(state == StopButton::Ready)
                .automation_name(match state {
                    StopButton::Stopping => format!("Stopping {}", entry.title),
                    _ => format!("Stop Agent {} (run {generation})", entry.title),
                })
                .on_click(move || {
                    stop.call((id.clone(), generation));
                })
                .content(crate::controls::icon(crate::controls::glyph::STOP_CIRCLE))
                .into(),
        )
    } else {
        None
    };
    let grid = Grid::new().columns(outer_columns()).children((
        crate::views::MaybeContent::maybe_content(
            Border::new().vertical_alignment(VerticalAlignment::Center),
            chevron,
        ),
        button,
        crate::views::MaybeContent::maybe_content(
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center),
            action,
        ),
    ));
    if entry.attention {
        Border::new()
            .corner_radius(tokens::radius::CONTROL)
            .background(ThemeBrush::CardBackground)
            .content(grid)
            .into()
    } else {
        grid.into()
    }
}

fn entry_view(entry: &MonitorEntry, input: &MonitorListInput) -> KeyedView {
    match entry {
        MonitorEntry::Section(key, label) => KeyedView::new(
            key.clone(),
            Border::new()
                .min_height(tokens::height::CONTROL)
                .padding(Thickness::xy(tokens::spacing::SM, 0.0))
                .content(
                    crate::controls::text(label.clone(), tokens::text::LABEL)
                        .automation_heading_level(AutomationHeadingLevel::Level3)
                        .vertical_alignment(VerticalAlignment::Center),
                ),
        ),
        MonitorEntry::Stopped(label, open) => KeyedView::new(
            format!("section:stopped:{open}"),
            crate::controls::sub_row(false)
                .automation_name(format!(
                    "{label} ({})",
                    if *open { "shown" } else { "hidden" }
                ))
                .on_click(input.stopped.clone())
                .content(crate::controls::row_content(
                    Some(if *open {
                        crate::controls::glyph::CHEVRON_DOWN
                    } else {
                        crate::controls::glyph::CHEVRON_RIGHT
                    }),
                    label,
                    "",
                    None,
                )),
        ),
        MonitorEntry::Row(entry) => KeyedView::new(
            format!("row:{}:{}", entry.row.id, entry.selected),
            fleet_row(entry, input),
        ),
        MonitorEntry::Note(text) => KeyedView::new(
            "none",
            Border::new()
                .padding(Thickness::uniform(tokens::spacing::SM))
                .content(crate::controls::secondary(text.clone())),
        ),
    }
}
