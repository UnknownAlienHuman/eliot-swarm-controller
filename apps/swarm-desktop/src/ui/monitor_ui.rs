//! The right panel: the monitor (an overview of what is live, by state
//! and by project, above every task as one compact row, its workers and
//! subagents nested under it) and the open Agent's details (settings,
//! coverage, resources, usage), on one refresh chain that runs only while
//! the panel is open.

use super::*;
use crate::monitor::{Filter, Group, MonitorRow, Overview, ProjectRef, Tree};
use crate::nav::Checkout;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;
use crate::ui::chat_ui::DraftAction;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    RefreshDue(u64),
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    /// A monitor row: open its conversation and its details.
    RowOpened(String),
    /// A lost or failed row's Resume, bound to the generation shown: open
    /// its conversation and resume it there.
    ResumeRequested(String, i64),
    /// Show or hide the children (workers and subagents) of one row.
    ChildrenToggled(String),
    /// Show or hide Recently stopped.
    StoppedToggled,
    /// A state bar segment or legend item: list only that group, or
    /// everything again when it already filters.
    GroupFiltered(Group),
    /// A project row: list only that project, or every project again when
    /// it already filters.
    ProjectFiltered(ProjectRef),
    /// Show every project row, or the first few again.
    ProjectsToggled,
    TechnicalToggled,
    PanelChosen(Panel),
}

/// The panel's refresh chain and the Agent whose details it reads.
#[derive(Default)]
pub(super) struct MonitorState {
    pub(super) data: Monitor,
    /// Tags refreshes; a reply for an older chain is dropped.
    pub(super) epoch: u64,
    task: Option<ComponentTask>,
    refresh_gate: Arc<tokio::sync::Mutex<()>>,
    refreshing: bool,
    /// The Agent whose details (and usage) each refresh reads.
    pub(super) selected: Option<String>,
    /// Rows whose children are shown (collapsed by default).
    pub(super) expanded: HashSet<String>,
    /// Recently stopped is open (collapsed by default).
    pub(super) stopped_open: bool,
    /// What the list is narrowed to (a group, a project, or both).
    pub(super) filter: Filter,
    /// Every project row is shown, not only the first few.
    pub(super) projects_open: bool,
    pub(super) technical_open: bool,
}

/// A group's tone in the Monitor's charts and list dots.
fn group_tone(group: Group) -> StatusTone {
    match group {
        Group::Working => StatusTone::Active,
        Group::Attention => StatusTone::Attention,
        Group::Idle => StatusTone::Ready,
        Group::UnparentedSubagents | Group::Stopped => StatusTone::Neutral,
    }
}

/// A group's glyph: its mark under High Contrast, where the chart's
/// colors are system colors.
fn group_glyph(group: Group) -> &'static str {
    match group {
        Group::Working => m3::glyph::PULSE,
        Group::Attention => m3::glyph::WARNING,
        Group::Idle => m3::glyph::PAUSE,
        Group::UnparentedSubagents => m3::glyph::PROFILE,
        Group::Stopped => m3::glyph::STOP,
    }
}

/// A group's mark in the legend: its chart-colored dot, or its glyph under
/// High Contrast (the same one its bar segment shows).
fn group_mark(palette: Palette, group: Group) -> View {
    if palette.scheme().is_none() {
        m3::icon(group_glyph(group))
    } else {
        m3::dot(palette.chart_fill(group_tone(group)))
    }
}

/// Opacity of the segments and project bars a filter leaves out.
const FILTERED_OUT: f64 = 0.38;

impl DesktopApp {
    pub(super) fn monitor_update(&mut self, msg: MonitorMsg, context: &ComponentContext<Self>) {
        match msg {
            MonitorMsg::RefreshDue(epoch) => {
                if epoch == self.monitor.epoch && self.panel != Panel::Closed {
                    self.start_monitor(context);
                }
            }
            MonitorMsg::MonitorLoaded(epoch, snapshot) => {
                if epoch != self.monitor.epoch || self.panel == Panel::Closed {
                    return;
                }
                self.monitor.refreshing = false;
                if let Some(snapshot) = snapshot {
                    self.monitor.data.apply(*snapshot);
                    // A filter whose group or project has no live Agent
                    // left is cleared after a complete refresh.
                    if self.monitor.data.errors.is_empty() {
                        let checkouts = self.checkouts();
                        let project_of = |row: &MonitorRow| self.monitor_project(row, &checkouts);
                        let overview = self
                            .monitor
                            .data
                            .overview(crate::handoff::now_ms(), &project_of);
                        let filter = self.monitor.filter.reconciled(&overview);
                        self.monitor.filter = filter;
                    }
                    self.monitor.task = Some(context.spawn_background(move |cancel| {
                        feed::backoff(monitor::PERIOD_MS, &|| cancel.is_cancelled());
                        Message::Monitor(MonitorMsg::RefreshDue(epoch))
                    }));
                }
            }
            MonitorMsg::RowOpened(id) => {
                let agent = self
                    .nav
                    .agents
                    .iter()
                    .find(|agent| agent.id == id)
                    .cloned()
                    .or_else(|| {
                        self.monitor
                            .data
                            .rows
                            .iter()
                            .find(|row| row.id == id)
                            .map(|row| UiAgent {
                                id: row.id.clone(),
                                generation: row.generation,
                                harness: row.harness.clone(),
                                state: row.state,
                                project_id: row.project.clone(),
                                project_scope: row.project_scope.clone(),
                                repo_common_dir: None,
                                workspace: None,
                                parent: row.parent.clone(),
                                updated_at: 0,
                                title: row.title.clone(),
                                native_child: row.activity.is_some(),
                                tokens: row.tokens,
                                chat: row.chat,
                                archived: false,
                            })
                    });
                if let Some(agent) = agent {
                    self.shell_update(ShellMsg::AgentOpened(agent.clone()), context);
                    self.show_details(&agent.id, context);
                }
            }
            MonitorMsg::ResumeRequested(id, generation) => {
                if self.submitting() {
                    return;
                }
                self.monitor_update(MonitorMsg::RowOpened(id.clone()), context);
                self.chat_update(
                    ChatMsg::DraftRequested(DraftAction::Resume {
                        agent: id,
                        generation,
                    }),
                    context,
                );
            }
            MonitorMsg::ChildrenToggled(id) => {
                if !self.monitor.expanded.remove(&id) {
                    self.monitor.expanded.insert(id);
                }
            }
            MonitorMsg::StoppedToggled => self.monitor.stopped_open = !self.monitor.stopped_open,
            MonitorMsg::GroupFiltered(group) => {
                let filter = &mut self.monitor.filter;
                filter.group = (filter.group != Some(group)).then_some(group);
            }
            MonitorMsg::ProjectFiltered(project) => {
                let filter = &mut self.monitor.filter;
                let on = filter
                    .project
                    .as_ref()
                    .is_some_and(|on| on.key == project.key);
                filter.project = (!on).then_some(project);
            }
            MonitorMsg::ProjectsToggled => self.monitor.projects_open = !self.monitor.projects_open,
            MonitorMsg::TechnicalToggled => {
                self.monitor.technical_open = !self.monitor.technical_open
            }
            MonitorMsg::PanelChosen(panel) => {
                if let Some(agent) = self.selected_agent_id() {
                    self.show_details(&agent, context);
                }
                self.panel = panel;
            }
        }
    }

    /// Open the details of `agent` in the right panel.
    pub(super) fn show_details(&mut self, agent: &str, context: &ComponentContext<Self>) {
        let changed = self.monitor.selected.as_deref() != Some(agent);
        if changed {
            self.monitor.selected = Some(agent.to_owned());
            self.monitor.data.detail_errors.clear();
            self.monitor.technical_open = false;
        }
        let was_closed = self.panel == Panel::Closed;
        self.panel = Panel::Details;
        if was_closed || changed {
            self.start_monitor(context);
        }
    }

    /// Close the panel: no refresh, no render work.
    pub(super) fn close_panel(&mut self) {
        self.panel = Panel::Closed;
        self.monitor.refreshing = false;
        self.monitor.epoch += 1;
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
    }

    /// Read while the panel is open, keeping the last successful data visible.
    pub(super) fn start_monitor(&mut self, context: &ComponentContext<Self>) {
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
        self.monitor.epoch += 1;
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                self.monitor.refreshing = false;
                self.monitor.data.errors = vec![format!("no data directory: {error}")];
                return;
            }
        };
        let epoch = self.monitor.epoch;
        let plan = monitor::RefreshPlan {
            selected: self.monitor.selected.clone(),
            stopped: self.monitor.data.wants_stopped(),
            attention_cursor: self.monitor.data.attention_cursor,
            losses: self.monitor.data.loss_reads(),
        };
        let refresh_gate = self.monitor.refresh_gate.clone();
        self.monitor.refreshing = true;
        self.monitor.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let snapshot = client::block_on(async {
                // A cancelled epoch may still have an IPC read in flight.
                let _guard = refresh_gate.lock().await;
                if cancelled() {
                    return None;
                }
                monitor::refresh(&data_dir, plan, &cancelled).await
            });
            Message::Monitor(MonitorMsg::MonitorLoaded(epoch, snapshot.map(Box::new)))
        }));
    }

    /// The panel's header: its title and the one close button.
    fn panel_header(
        &self,
        _title: &str,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(
                tokens::spacing::MD,
                tokens::spacing::SM,
                tokens::spacing::SM,
                tokens::spacing::SM,
            ))
            .children((
                m3::segmented(
                    palette,
                    [("Monitor", Panel::Monitor), ("Details", Panel::Details)]
                        .into_iter()
                        .map(|(label, panel)| {
                            KeyedView::new(
                                label,
                                m3::segment(palette, self.panel == panel)
                                    .is_enabled(
                                        panel != Panel::Details
                                            || self.monitor.selected.is_some()
                                            || self.selected_agent_id().is_some(),
                                    )
                                    .automation_name(format!("Show {label}"))
                                    .on_click(
                                        context.message(Message::Monitor(MonitorMsg::PanelChosen(
                                            panel,
                                        ))),
                                    )
                                    .content(m3::segment_label(label)),
                            )
                        })
                        .collect::<Vec<_>>(),
                ),
                m3::icon_button(palette)
                    .grid_column(1)
                    .automation_name("Close panel")
                    .on_click(
                        context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed))),
                    )
                    .content(m3::toolbar_icon(m3::glyph::CLOSE)),
            ))
    }

    /// A monitor row's project as the sidebar names it: its section key
    /// ([`nav::project_key`]) and the owner's display name for that
    /// project, else the sidebar's name ([`nav::project_of`]); a chat
    /// lists under Chats, as in the sidebar.
    fn monitor_project(&self, row: &MonitorRow, checkouts: &[Checkout]) -> ProjectRef {
        if row.chat {
            return ProjectRef {
                key: "chats".to_owned(),
                name: "Chats".to_owned(),
            };
        }
        let key = nav::project_key(&row.project, checkouts);
        let name = self
            .prefs
            .display_name(&format!("project:{key}"))
            .map_or_else(|| nav::project_of(&row.project, checkouts), str::to_owned);
        ProjectRef { key, name }
    }

    /// One row, and below it (when `nest`) its children's toggle (workers
    /// and subagents, collapsed by default) and, when open, their rows one
    /// step in (recursively: a child may have its own). `selected` is the
    /// open Agent's id, drawn as the selection.
    #[allow(clippy::too_many_arguments)]
    fn monitor_rows(
        &self,
        row: &MonitorRow,
        depth: usize,
        tree: Option<&Tree>,
        selected: Option<&str>,
        context: &mut ViewContext<Self>,
        palette: Palette,
        rows: &mut Vec<KeyedView>,
    ) {
        let data = &self.monitor.data;
        let indent = depth as f64 * tokens::layout::TREE_INDENT;
        let title = self.agent_title(&row.id);
        // A top-level row whose parent is not shown names its parent task
        // by title (never a bare id: an unknown title leaves it out).
        let parent = row
            .parent
            .as_deref()
            .filter(|_| depth == 0)
            .map(|parent| (parent, self.agent_title(parent)))
            .filter(|(id, title)| title != id)
            .map(|(_, title)| title);
        // The second line: why a lost or failed Agent ended, as its
        // conversation says; else why it needs attention (its open report,
        // the first worker that needs you, or its own state); else the task
        // it runs under.
        let note = if row.resumable() {
            data.loss_words(row).map(monitor_view::RowNote::Ended)
        } else {
            data.incident(row)
                .filter(|_| row.state != eliot_core::AgentState::Stopped)
                .map(|incident| monitor::attention_words(&incident.reason))
                .or_else(|| {
                    tree.filter(|_| !data.needs_attention(row))
                        .and_then(|tree| tree.worker_attention_words(&row.id))
                })
                .map(monitor_view::RowNote::Attention)
        }
        .or_else(|| {
            data.needs_attention(row)
                .then(|| monitor_view::RowNote::Attention(row.status_words().to_owned()))
        })
        .or_else(|| parent.map(monitor_view::RowNote::Under));
        let resume = row.resumable().then(|| {
            context.message(Message::Monitor(MonitorMsg::ResumeRequested(
                row.id.clone(),
                row.generation,
            )))
        });
        let quiet = row.quiet(crate::handoff::now_ms());
        let elapsed = quiet.label();
        let elapsed_words = quiet.words();
        let group = row.group(data.incident(row));
        rows.push(KeyedView::new(
            format!("row:{}:{}", row.id, selected == Some(row.id.as_str())),
            Border::new()
                .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
                .content(monitor_view::list_row(
                    row,
                    monitor_view::RowLook {
                        title: &title,
                        elapsed: &elapsed,
                        elapsed_words: &elapsed_words,
                        dot: palette.chart_fill(group_tone(group)),
                        selected: selected == Some(row.id.as_str()),
                    },
                    note.as_ref(),
                    monitor_view::RowActions {
                        open: context
                            .message(Message::Monitor(MonitorMsg::RowOpened(row.id.clone()))),
                        resume: resume.map(|resume| (resume, !self.submitting())),
                    },
                    palette,
                )),
        ));
        let Some(tree) = tree else {
            return;
        };
        let children = tree.children(&row.id);
        if children.is_empty() || depth > 4 {
            return;
        }
        let open = self.monitor.expanded.contains(&row.id);
        let label = tree.children_label(&row.id);
        rows.push(KeyedView::new(
            format!("children:{}:{open}", row.id),
            m3::sub_row(palette, false)
                .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
                .automation_name(format!(
                    "{label} of {title} ({})",
                    if open { "shown" } else { "hidden" }
                ))
                .on_click(
                    context.message(Message::Monitor(MonitorMsg::ChildrenToggled(
                        row.id.clone(),
                    ))),
                )
                .content(m3::row_content(
                    Some(if open {
                        m3::glyph::CHEVRON_DOWN
                    } else {
                        m3::glyph::CHEVRON_RIGHT
                    }),
                    &label,
                    "",
                    View::empty(),
                )),
        ));
        if open {
            for child in children {
                self.monitor_rows(
                    child,
                    depth + 1,
                    Some(tree),
                    selected,
                    context,
                    palette,
                    rows,
                );
            }
        }
    }

    /// The overview above the list: the state bar (one segment per live
    /// group, proportional to its count), the legend with its numbers, a
    /// note when the counts are partial or old, the live Agents by project
    /// on one scale, and the active filters as chips. Every segment, legend
    /// item and project row filters the list; clicking it again clears it.
    fn monitor_overview(
        &self,
        overview: &Overview,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Vec<KeyedView> {
        let filter = &self.monitor.filter;
        let counts = overview.counts;
        let total = counts.total();
        let segments = counts.segments();
        let mut items: Vec<KeyedView> = Vec::new();

        // Under High Contrast the glyph buttons have their own space,
        // even when the corresponding proportional segment is very small.
        let marked = palette.scheme().is_none();
        let bar_height = tokens::layout::STATE_BAR;
        let bar = if segments.is_empty() {
            // Nothing live: the empty track.
            m3::bar_segment(palette.well(), bar_height)
        } else if marked {
            let plot = m3::stacked_bar(
                segments
                    .iter()
                    .map(|&(group, count)| {
                        (
                            count,
                            format!("fill:{group:?}"),
                            m3::bar_segment(palette.chart_fill(group_tone(group)), bar_height),
                        )
                    })
                    .collect(),
                0,
                bar_height,
            );
            let controls =
                Grid::new()
                    .columns(vec![GridLength::Auto; segments.len()])
                    .column_spacing(tokens::layout::BAR_GAP)
                    .keyed_children(segments.iter().enumerate().map(
                        |(column, &(group, count))| {
                            let dimmed = filter.group.is_some_and(|only| only != group);
                            KeyedView::new(
                                format!("segment:{group:?}"),
                                m3::bar_glyph_button(palette.chart_button(group_tone(group)))
                                    .grid_column(column as i32)
                                    .opacity(if dimmed { FILTERED_OUT } else { 1.0 })
                                    .automation_name(format!(
                                        "{}: {count} of {total}",
                                        group.title()
                                    ))
                                    .on_click(context.message(Message::Monitor(
                                        MonitorMsg::GroupFiltered(group),
                                    )))
                                    .content(m3::icon(group_glyph(group))),
                            )
                        },
                    ));
            StackPanel::new()
                .spacing(tokens::layout::BAR_GAP)
                .children((plot, controls))
        } else {
            m3::stacked_bar(
                segments
                    .iter()
                    .map(|&(group, count)| {
                        let dimmed = filter.group.is_some_and(|only| only != group);
                        (
                            count,
                            format!("segment:{group:?}"),
                            m3::bar_button(palette.chart_button(group_tone(group)), bar_height)
                                .opacity(if dimmed { FILTERED_OUT } else { 1.0 })
                                .automation_name(format!("{}: {count} of {total}", group.title()))
                                .on_click(
                                    context.message(Message::Monitor(MonitorMsg::GroupFiltered(
                                        group,
                                    ))),
                                )
                                .content(View::empty()),
                        )
                    })
                    .collect(),
                0,
                bar_height,
            )
        };
        let legend: View = if segments.is_empty() {
            m3::body(counts.words()).into()
        } else {
            // Each item as wide as its words, so "needs attention" is
            // never cut at the panel's width.
            Grid::new()
                .columns(vec![GridLength::Auto; segments.len()])
                .column_spacing(tokens::spacing::XS)
                .keyed_children(
                    segments
                        .iter()
                        .enumerate()
                        .map(|(column, &(group, count))| {
                            let on = filter.group == Some(group);
                            let words = monitor::group_words(group, count);
                            KeyedView::new(
                                format!("legend:{group:?}:{on}"),
                                m3::row(palette, on)
                                    .grid_column(column as i32)
                                    .height(tokens::height::TWO_LINE_ROW)
                                    .min_height(tokens::height::TWO_LINE_ROW)
                                    .automation_name(format!(
                                        "{count} {words}{}",
                                        if on { ", listed only" } else { "" }
                                    ))
                                    .on_click(context.message(Message::Monitor(
                                        MonitorMsg::GroupFiltered(group),
                                    )))
                                    .content(
                                        StackPanel::new()
                                            .vertical_alignment(VerticalAlignment::Center)
                                            .children((
                                                m3::text(
                                                    count.to_string(),
                                                    tokens::text::PAGE_TITLE,
                                                ),
                                                StackPanel::new()
                                                    .orientation(Orientation::Horizontal)
                                                    .spacing(tokens::spacing::XS)
                                                    .children((
                                                        group_mark(palette, group),
                                                        m3::secondary(words).text_trimming(
                                                            TextTrimming::CharacterEllipsis,
                                                        ),
                                                    )),
                                            )),
                                    ),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
        };
        // Partial counts are said next to the chart, never passed as
        // totals; then read failures and the age of the data.
        let data = &self.monitor.data;
        let mut notes: Vec<String> = Vec::new();
        if !data.errors.is_empty() {
            notes.push("Some reads failed".to_owned());
        }
        if let Some(status) = monitor::read_status(data.read_at_ms, crate::handoff::now_ms()) {
            notes.push(status);
        }
        let partial: View = match overview.partial_note() {
            Some(note) => m3::icon_label(m3::glyph::WARNING, note),
            None => View::empty(),
        };
        items.push(KeyedView::new(
            "state",
            StackPanel::new().spacing(tokens::spacing::XS).children((
                bar,
                legend,
                partial,
                if notes.is_empty() {
                    View::empty()
                } else {
                    m3::secondary(notes.join(" · "))
                        .text_wrapping(TextWrapping::Wrap)
                        .into()
                },
            )),
        ));

        if !overview.projects.is_empty() {
            items.push(KeyedView::new(
                "projects-caption",
                m3::section_label("By project").margin(Thickness::new(
                    tokens::spacing::INSET,
                    tokens::spacing::SM,
                    0.0,
                    0.0,
                )),
            ));
            let scale = overview.scale();
            let shown = if self.monitor.projects_open {
                overview.projects.len()
            } else {
                monitor::PROJECT_ROWS
            };
            for project in overview.projects.iter().take(shown) {
                let key = &project.project.key;
                let name = &project.project.name;
                let on = filter.project.as_ref().is_some_and(|on| &on.key == key);
                let dimmed = filter.project.is_some() && !on;
                let bar = m3::stacked_bar(
                    project
                        .counts
                        .segments()
                        .into_iter()
                        .map(|(group, count)| {
                            (
                                count,
                                format!("{group:?}"),
                                m3::bar_segment(
                                    palette.chart_fill(group_tone(group)),
                                    tokens::layout::PROJECT_BAR,
                                ),
                            )
                        })
                        .collect(),
                    scale - project.counts.total(),
                    tokens::layout::PROJECT_BAR,
                );
                items.push(KeyedView::new(
                    format!("project:{key}:{on}"),
                    m3::row(palette, on)
                        .height(tokens::layout::PROJECT_ROW)
                        .min_height(tokens::layout::PROJECT_ROW)
                        .automation_name(format!(
                            "{name}: {}{}",
                            project.counts.list_words(),
                            if on { ", listed only" } else { "" }
                        ))
                        .on_click(
                            context.message(Message::Monitor(MonitorMsg::ProjectFiltered(
                                project.project.clone(),
                            ))),
                        )
                        .content(
                            StackPanel::new()
                                .spacing(tokens::spacing::XS)
                                .vertical_alignment(VerticalAlignment::Center)
                                .children((
                                    Grid::new()
                                        .columns([GridLength::STAR, GridLength::Auto])
                                        .column_spacing(tokens::spacing::SM)
                                        .children((
                                            TextBlock::new()
                                                .text(name.clone())
                                                .text_trimming(TextTrimming::CharacterEllipsis),
                                            m3::text(
                                                project.counts.total().to_string(),
                                                tokens::text::SECTION_TITLE,
                                            )
                                            .grid_column(1)
                                            .vertical_alignment(VerticalAlignment::Center),
                                        )),
                                    Border::new()
                                        .opacity(if dimmed { FILTERED_OUT } else { 1.0 })
                                        .content(bar),
                                )),
                        )
                        .tooltip(name.clone()),
                ));
            }
            let hidden = overview
                .projects
                .len()
                .saturating_sub(monitor::PROJECT_ROWS);
            if hidden > 0 {
                let open = self.monitor.projects_open;
                let label = if open {
                    "Show fewer projects".to_owned()
                } else if hidden == 1 {
                    "1 more project".to_owned()
                } else {
                    format!("{hidden} more projects")
                };
                items.push(KeyedView::new(
                    format!("projects-more:{open}"),
                    m3::sub_row(palette, false)
                        .automation_name(label.clone())
                        .on_click(context.message(Message::Monitor(MonitorMsg::ProjectsToggled)))
                        .content(m3::row_content(
                            Some(if open {
                                m3::glyph::CHEVRON_DOWN
                            } else {
                                m3::glyph::CHEVRON_RIGHT
                            }),
                            &label,
                            "",
                            View::empty(),
                        )),
                ));
            }
        }

        // The active filters, each a chip whose × clears it.
        let mut chips: Vec<KeyedView> = Vec::new();
        if let Some(group) = filter.group {
            chips.push(KeyedView::new(
                format!("chip:group:{group:?}"),
                filter_chip(
                    palette,
                    group.title(),
                    context.message(Message::Monitor(MonitorMsg::GroupFiltered(group))),
                ),
            ));
        }
        if let Some(project) = &filter.project {
            chips.push(KeyedView::new(
                format!("chip:project:{}", project.key),
                filter_chip(
                    palette,
                    &project.name,
                    context.message(Message::Monitor(MonitorMsg::ProjectFiltered(
                        project.clone(),
                    ))),
                ),
            ));
        }
        if !chips.is_empty() {
            // One chip per line: side by side, a group chip and a long
            // project chip outgrow the narrow panel; each alone fits
            // (its label is capped at `FILTER_CHIP_TEXT`).
            items.push(KeyedView::new(
                "filters",
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                    .keyed_children(chips),
            ));
        }
        items
    }

    /// The monitor: the overview, then Needs attention, Working, Idle and a
    /// collapsed Recently stopped, workers and subagents under their
    /// parent's row; under a filter the chart's loaded Agents are listed flat.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data = &self.monitor.data;
        let now = crate::handoff::now_ms();
        let checkouts = self.checkouts();
        let project_of = |row: &MonitorRow| self.monitor_project(row, &checkouts);
        let overview = data.overview(now, &project_of);
        let filter = &self.monitor.filter;
        let mut items: Vec<KeyedView> = Vec::new();
        if data.read_at_ms.is_none() && self.monitor.refreshing && data.errors.is_empty() {
            items.push(KeyedView::new(
                "reading",
                m3::secondary("Reading the swarm…"),
            ));
        }
        if !data.errors.is_empty() {
            items.push(KeyedView::new(
                "errors",
                m3::text(
                    format!("Some reads failed: {}", data.errors.join("; ")),
                    tokens::text::BODY,
                )
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if data.read_at_ms.is_some() {
            items.extend(self.monitor_overview(&overview, context, palette));
        }
        let tree = data.tree(now);
        let focused = self
            .selected_agent_id()
            .or_else(|| self.monitor.selected.clone());
        if !filter.is_active()
            && let Some(row) = focused.as_deref().and_then(|id| data.row(id))
        {
            let mut current = Vec::new();
            self.monitor_rows(row, 0, Some(&tree), None, context, palette, &mut current);
            items.push(KeyedView::new(
                "current-task",
                StackPanel::new().spacing(tokens::spacing::XS).children((
                    m3::section_label("Current task").margin(Thickness::new(
                        tokens::spacing::INSET,
                        tokens::spacing::SM,
                        0.0,
                        0.0,
                    )),
                    StackPanel::new().keyed_children(current),
                    m3::divider(palette),
                )),
            ));
        }
        let (sections, showing_loaded) = if filter.is_active() {
            overview.list_for_filter(filter)
        } else {
            (tree.sections(), false)
        };
        let no_rows = sections.iter().all(|section| section.rows.is_empty());
        if showing_loaded {
            let note = if no_rows {
                "No live Agents were loaded in this partial read."
            } else {
                "No matches on this page. Showing loaded Agents."
            };
            items.push(KeyedView::new(
                "filter-unavailable",
                m3::secondary(note).text_wrapping(TextWrapping::Wrap),
            ));
        }
        let nest = (!filter.is_active()).then_some(&tree);
        for section in &sections {
            let top = section.rows.len();
            if top == 0 {
                continue;
            }
            if section.group == monitor::Group::Stopped {
                let open = self.monitor.stopped_open;
                let label = format!("{} ({top})", section.group.title());
                items.push(KeyedView::new(
                    format!("section:stopped:{open}"),
                    m3::sub_row(palette, false)
                        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                        .automation_name(format!(
                            "{label} ({})",
                            if open { "shown" } else { "hidden" }
                        ))
                        .on_click(context.message(Message::Monitor(MonitorMsg::StoppedToggled)))
                        .content(m3::row_content(
                            Some(if open {
                                m3::glyph::CHEVRON_DOWN
                            } else {
                                m3::glyph::CHEVRON_RIGHT
                            }),
                            &label,
                            "",
                            View::empty(),
                        )),
                ));
                if !open {
                    continue;
                }
            } else {
                // The legend above holds the counts. A section counts its
                // top-level rows, which leave out nested workers, so only
                // a filtered (flat) section shows its number: there it is
                // the number that was clicked.
                let label = if filter.is_active() {
                    format!("{} ({top})", section.group.title())
                } else {
                    section.group.title().to_owned()
                };
                items.push(KeyedView::new(
                    format!("section:{:?}", section.group),
                    m3::section_label(label).margin(Thickness::new(
                        tokens::spacing::INSET,
                        tokens::spacing::SM,
                        0.0,
                        0.0,
                    )),
                ));
            }
            for row in &section.rows {
                self.monitor_rows(
                    row,
                    0,
                    nest,
                    focused.as_deref(),
                    context,
                    palette,
                    &mut items,
                );
            }
        }
        if data.read_at_ms.is_some() && no_rows && !showing_loaded {
            let none = if filter.is_active() {
                "No live Agents match this filter."
            } else {
                "No Agents to show."
            };
            items.push(KeyedView::new("none", m3::secondary(none)));
        }
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Monitor", context, palette),
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Monitored Agents")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::XS)
                            .margin(Thickness::new(
                                tokens::spacing::MD,
                                0.0,
                                tokens::spacing::MD,
                                tokens::spacing::MD,
                            ))
                            .keyed_children(items),
                    ),
            ))
    }

    /// The open Agent's details: its settings and coverage, resources and
    /// usage. Ids and readback text live here, not in the conversation.
    /// Moving it under another task is a menu action
    /// ([`super::move_ui`]), not a section here.
    pub(super) fn details_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let Some(id) = self.monitor.selected.clone() else {
            return View::empty();
        };
        let mut sections: Vec<KeyedView> = Vec::new();
        let mut technical = vec![format!("ID {id}")];
        let data = &self.monitor.data;

        // The Agent: identity and settings (from the open conversation's
        // detail when it is this Agent, else the monitor's read). Effort
        // words read short; `spoken` holds the same settings lines in full
        // words and where they start, for the section's accessible name.
        let mut agent_lines: Vec<String> = Vec::new();
        let mut spoken: Option<(usize, Vec<String>)> = None;
        let detail = self
            .chat
            .detail
            .as_ref()
            .filter(|_| self.selected_agent_id().as_deref() == Some(id.as_str()));
        let generation = detail
            .map(|detail| detail.generation)
            .or_else(|| self.find_agent(&id).map(|agent| agent.generation));
        let cached = data.detail.as_ref().filter(|cached| {
            let agent = &cached.read.agent.agent;
            agent.id == id && generation.is_none_or(|generation| agent.generation == generation)
        });
        let read = cached.map(|cached| &cached.read);
        let previous_generation = data
            .row(&id)
            .is_some_and(|row| generation.is_some_and(|generation| row.generation != generation));
        let resources_at = data
            .read_at_ms
            .filter(|_| !previous_generation && data.row(&id).is_some());
        let read_at = cached
            .map(monitor::CachedDetail::oldest_read_ms)
            .into_iter()
            .chain(resources_at)
            .min();
        if let Some(status) = monitor::read_status(read_at, crate::handoff::now_ms()) {
            sections.push(KeyedView::new(
                "refresh",
                m3::secondary(status).text_wrapping(TextWrapping::Wrap),
            ));
        }
        let errors: Vec<_> = data
            .detail_errors
            .iter()
            .chain(&data.errors)
            .map(String::as_str)
            .collect();
        if !errors.is_empty() {
            sections.push(KeyedView::new(
                "read-errors",
                m3::secondary(format!("Some reads failed: {}", errors.join(" · ")))
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        // No cached read and no error yet: the first read is still due. This
        // ignores routine refreshes, so a placeholder never flips each cycle.
        let pending = data.detail_errors.is_empty();
        match (detail, read) {
            (Some(detail), _) => {
                let harness = crate::picker::harness_label(&detail.harness);
                agent_lines.push(format!("{harness} · {}", state_label(detail.state)));
                spoken = Some((agent_lines.len(), detail.settings.spoken_lines()));
                agent_lines.extend(detail.settings.lines());
                agent_lines.push(format!(
                    "Access: {}",
                    launch_ui::agent_access_label(&detail.settings)
                ));
                if !detail.settings.profile.starts_with("adhoc-") {
                    agent_lines.push(format!("Preset {}", detail.settings.profile));
                }
                if detail.observe_only {
                    agent_lines.push(format!(
                        "{harness} runs this one itself: Swarm shows it but cannot steer or stop it."
                    ));
                }
                if detail.active_turn.is_some() {
                    agent_lines.push("Working on a turn".to_owned());
                }
                if read.is_none() {
                    technical.push(format!("Generation {}", detail.generation));
                }
            }
            (None, Some(read)) => {
                spoken = Some((agent_lines.len(), monitor_view::identity_spoken_lines(read)));
                agent_lines.extend(monitor_view::identity_lines(read));
                let settings = configure::Settings::from_reply(&read.agent);
                agent_lines.push(format!(
                    "Access: {}",
                    launch_ui::agent_access_label(&settings)
                ));
            }
            (None, None) => agent_lines.push(
                if pending {
                    "Reading…"
                } else {
                    "Details unavailable."
                }
                .to_owned(),
            ),
        }
        if !self.configure.status.is_empty() && detail.is_some() {
            technical.push(format!("Last configure: {}", self.configure.status));
        }
        let mut agent_spoken = agent_lines.clone();
        if let Some((at, words)) = spoken {
            for (line, said) in agent_spoken.iter_mut().skip(at).zip(words) {
                *line = said;
            }
        }
        sections.push(KeyedView::new(
            "agent",
            spoken_section(palette, "Summary", &agent_lines, &agent_spoken),
        ));

        // Where it works: its project's checkout and, on a worktree, that
        // worktree; each row opens in Explorer.
        let (project, workspace) = match (detail, read) {
            (Some(detail), _) => (Some(detail.project.clone()), detail.workspace.clone()),
            (None, Some(read)) => (
                Some(read.agent.agent.project.clone()),
                read.agent.agent.workspace.clone(),
            ),
            (None, None) => (
                self.find_agent(&id).map(|agent| agent.project.clone()),
                None,
            ),
        };
        let mut folders: Vec<KeyedView> = Vec::new();
        if let Some(project) = project.filter(|project| !project.is_empty()) {
            folders.push(KeyedView::new(
                "project",
                self.path_row(context, palette, "Project folder", "", &project),
            ));
            if let Some(worktree) =
                workspace.filter(|workspace| nav::path_key(workspace) != nav::path_key(&project))
            {
                folders.push(KeyedView::new(
                    "worktree",
                    self.path_row(context, palette, "Worktree", "", &worktree),
                ));
            }
        }
        if !folders.is_empty() {
            sections.push(KeyedView::new(
                "folders",
                m3::card_high(palette).content(
                    StackPanel::new().spacing(tokens::spacing::XS).children((
                        m3::section_title("Folders"),
                        StackPanel::new().keyed_children(folders),
                    )),
                ),
            ));
        }

        // The failure diagnosis summary: why and where the Agent last
        // failed, from the same monitor read as the identity above. The
        // full causal chain lives behind `swarm why`.
        if let Some(line) = read
            .and_then(|read| read.diagnosis.as_ref().ok())
            .and_then(monitor::diagnosis_line)
        {
            sections.push(KeyedView::new(
                "diagnosis",
                section(palette, "What went wrong", &[line]),
            ));
        }

        // How the profile was chosen: the routing decision frozen at
        // admission, with every skipped candidate.
        let choice = match (detail, read) {
            (Some(detail), _) => Some((detail.routing.clone(), detail.settings.profile.clone())),
            (None, Some(read)) => Some((
                read.agent.agent.profile_choice.clone(),
                read.agent.agent.profile.clone(),
            )),
            (None, None) => None,
        };
        if let Some((decision, profile)) = choice {
            let lines = match decision {
                Some(decision) => {
                    let (headline, skipped) = crate::settings_model::decision_lines(&decision);
                    std::iter::once(headline.replacen("Routing picks", "Routing picked", 1))
                        .chain(skipped)
                        .collect()
                }
                None if profile.starts_with("adhoc-") => {
                    vec!["You chose its app and model when you started it.".to_owned()]
                }
                None => vec![format!("You started it on the preset {profile}.")],
            };
            technical.extend(lines);
        }

        // The conversation's retained coverage and the feed.
        // Only what is unusual about the conversation: older turns not
        // loaded, pruned activity, a feed that is not live.
        if let Some(convo) = self.chat.convo.as_ref().filter(|convo| convo.agent() == id) {
            let mut lines = convo.coverage().notes(convo.inputs_unavailable());
            if !matches!(
                self.chat.live,
                LiveStatus::Live | LiveStatus::Idle | LiveStatus::Loading
            ) {
                lines.push(format!("Live updates: {}", self.chat.live.label()));
            }
            if !lines.is_empty() {
                technical.extend(lines);
            }
        }

        let resources = if previous_generation {
            vec![KeyedView::new(
                "new-session",
                m3::secondary("No resource sample for this session yet."),
            )]
        } else {
            monitor_view::resource_views(data, &id, palette)
        };
        sections.push(KeyedView::new(
            "resources",
            m3::card_high(palette).content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_title("Activity & resources"),
                    StackPanel::new()
                        .spacing(tokens::spacing::MD)
                        .keyed_children(resources),
                )),
            ),
        ));

        let usage = match read {
            Some(read) => monitor_view::usage_lines(read),
            None if pending => vec!["Reading…".to_owned()],
            None => vec!["Usage unavailable.".to_owned()],
        };
        sections.push(KeyedView::new("usage", section(palette, "Usage", &usage)));

        if let Some(read) = read {
            technical.push(format!("Generation {}", read.agent.agent.generation));
            technical.extend(monitor_view::observe_lines(read));
        }
        let technical_body = if self.monitor.technical_open {
            section(palette, "Session", &technical)
        } else {
            View::empty()
        };
        sections.push(KeyedView::new(
            "technical",
            StackPanel::new().spacing(tokens::spacing::XS).children((
                m3::row(palette, false)
                    .automation_name(format!(
                        "Technical details ({})",
                        if self.monitor.technical_open {
                            "expanded"
                        } else {
                            "collapsed"
                        }
                    ))
                    .on_click(context.message(Message::Monitor(MonitorMsg::TechnicalToggled)))
                    .content(m3::row_content(
                        Some(if self.monitor.technical_open {
                            m3::glyph::CHEVRON_DOWN
                        } else {
                            m3::glyph::CHEVRON_RIGHT
                        }),
                        "Technical details",
                        "",
                        View::empty(),
                    )),
                technical_body,
            )),
        ));

        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Details", context, palette),
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Agent details")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .margin(Thickness::new(
                                tokens::spacing::MD,
                                0.0,
                                tokens::spacing::MD,
                                tokens::spacing::MD,
                            ))
                            .keyed_children(sections),
                    ),
            ))
    }
}

/// An Agent state code in words.
fn state_label(state: eliot_core::AgentState) -> &'static str {
    match state {
        eliot_core::AgentState::Created | eliot_core::AgentState::Starting => "Starting",
        eliot_core::AgentState::Idle => "Ready",
        eliot_core::AgentState::Busy => "Working",
        eliot_core::AgentState::Waiting => "Waiting for you",
        eliot_core::AgentState::Stopped => "Stopped",
        eliot_core::AgentState::Failed => "Failed",
        eliot_core::AgentState::Lost => "Lost",
        eliot_core::AgentState::Unknown => "State couldn't be confirmed",
    }
}

/// A details section: a container-corner card, its title and its lines
/// in body text.
fn section(palette: Palette, title: &str, lines: &[String]) -> View {
    spoken_section(palette, title, lines, lines)
}

/// [`section`] whose lines a screen reader hears as `spoken` (the efforts
/// in full words where the text shows them short).
fn spoken_section(palette: Palette, title: &str, lines: &[String], spoken: &[String]) -> View {
    let body = m3::body(lines.join("\n"))
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true);
    let body = if spoken == lines {
        body
    } else {
        body.automation_name(spoken.join("\n"))
    };
    m3::card_high(palette).content(
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .children((m3::section_title(title), body)),
    )
}

/// An active filter as a chip: its label and a × that clears it.
fn filter_chip(palette: Palette, label: &str, clear: Callback<()>) -> View {
    m3::chip(palette)
        .automation_name(format!("Clear filter: {label}"))
        .on_click(clear)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    TextBlock::new()
                        .text(label.to_owned())
                        .max_width(tokens::layout::FILTER_CHIP_TEXT)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    m3::icon(m3::glyph::CLOSE),
                )),
        )
}
