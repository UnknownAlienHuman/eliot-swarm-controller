//! The right panel: the monitor (an overview of what is live, by state
//! and by project, above every task as one compact row, its workers and
//! subagents nested under it) and the open Agent's details (settings,
//! coverage, resources, usage), on one refresh chain that runs only while
//! the panel is open.

use super::*;
use crate::monitor::{Filter, Group, MonitorRow, ProjectRef, Tree};
use crate::nav::Checkout;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;
use crate::ui::chat_ui::DraftAction;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
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
    TechnicalToggled,
    PanelChosen(Panel),
    /// The search field's text.
    SearchChanged(String),
    /// The state filter's choice (an index into `monitor::STATE_FILTERS`).
    StateChosen(Option<usize>),
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
    pub(super) technical_open: bool,
    /// The sidebar's read of attention reports is running (only while the
    /// panel, and so its refresh, is closed).
    pub(super) sidebar_reading: bool,
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

impl DesktopApp {
    pub(super) fn monitor_update(&mut self, msg: MonitorMsg, context: &ComponentContext<Self>) {
        match msg {
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
                    // The next read waits on its own worker: a timer message
                    // would render the whole window for nothing.
                    self.read_monitor(monitor::PERIOD_MS, context);
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
                                pending: false,
                                organization: None,
                                tags: Vec::new(),
                                organization_revision: None,
                                model: row.model.clone(),
                                work: row.work.clone().map(Box::new),
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
            MonitorMsg::SearchChanged(text) => {
                // A rebuilt field reports the text it already shows.
                if self.monitor.filter.text != text {
                    self.monitor.filter.text = text;
                }
            }
            MonitorMsg::StateChosen(index) => {
                if let Some(group) = index.and_then(|index| monitor::STATE_FILTERS.get(index)) {
                    self.monitor.filter.group = *group;
                }
            }
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
        self.read_monitor(0, context);
    }

    /// One refresh after `delay_ms` (the next one is planned when it lands).
    fn read_monitor(&mut self, delay_ms: u64, context: &ComponentContext<Self>) {
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
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
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

    /// The Monitor's search and state filter (`Conversation.dc.html`): a
    /// pill search field (title, id, harness, model, project, live and
    /// recently stopped), then the connected segmented filter.
    fn monitor_search(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let filter = &self.monitor.filter;
        let field = TextBox::new()
            .text(filter.text.clone())
            .placeholder_text("Search agents")
            .automation_name("Search the monitor")
            .on_text_changed(
                context.callback(|text| Message::Monitor(MonitorMsg::SearchChanged(text))),
            );
        let segments: Vec<KeyedView> = monitor::STATE_FILTERS
            .iter()
            .enumerate()
            .map(|(index, group)| {
                let label = group.map_or("All", monitor::Group::filter_label);
                let selected = filter.group == *group;
                // Keyed by its selection: WinUI reads a button's colors
                // only when its template is applied.
                KeyedView::new(
                    format!("{label}:{selected}"),
                    m3::segment(palette, selected)
                        .automation_name(format!("Show {label}"))
                        .on_click(
                            context.message(Message::Monitor(MonitorMsg::StateChosen(Some(index)))),
                        )
                        .content(m3::segment_label(label)),
                )
            })
            .collect();
        StackPanel::new().spacing(tokens::spacing::ROW).children((
            m3::search_field(palette.tonal(), field),
            m3::segmented(palette, segments),
        ))
    }

    /// The pane's header: the Monitor and Details tabs and the close
    /// button.
    fn panel_header(
        &self,
        _title: &str,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let tabs: Vec<KeyedView> = [("Monitor", Panel::Monitor), ("Details", Panel::Details)]
            .into_iter()
            .map(|(label, panel)| {
                let selected = self.panel == panel;
                // Keyed by its selection (k195 P2-5: the highlight follows
                // the pane, however it opened).
                KeyedView::new(
                    format!("{label}:{selected}"),
                    m3::pane_tab(palette, selected)
                        .is_enabled(
                            panel != Panel::Details
                                || self.monitor.selected.is_some()
                                || self.selected_agent_id().is_some(),
                        )
                        .automation_name(format!("Show {label}"))
                        .on_click(context.message(Message::Monitor(MonitorMsg::PanelChosen(panel))))
                        .content(m3::pane_tab_label(label)),
                )
            })
            .collect();
        Grid::new()
            .height(tokens::height::CONTROL)
            .columns([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(
                tokens::spacing::ROW + tokens::spacing::XS,
                tokens::spacing::INSET,
                tokens::spacing::ROW + tokens::spacing::XS,
                0.0,
            ))
            .children((
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .keyed_children(tabs),
                m3::icon_button(palette)
                    .grid_column(1)
                    .automation_name("Close panel")
                    .on_click(
                        context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed))),
                    )
                    .content(m3::icon(m3::glyph::CLOSE)),
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
    /// open Agent's id, drawn as the selection. `now` is the snapshot's
    /// read time, so the entries change only with a snapshot.
    #[allow(clippy::too_many_arguments)]
    fn monitor_entries(
        &self,
        row: &MonitorRow,
        depth: usize,
        tree: Option<&Tree>,
        selected: Option<&str>,
        now: i64,
        palette: Palette,
        entries: &mut Vec<monitor_view::MonitorEntry>,
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
        let incident = data
            .incident(row)
            .filter(|_| row.state != eliot_core::AgentState::Stopped);
        // Working without writing code says so on the row's second line,
        // in the attention tone; the trailing word stays the model.
        let no_code = incident.filter(|incident| incident.no_code());
        // The note: why a lost or failed Agent ended, as its conversation
        // says; else why it needs attention (its open report, the first
        // worker that needs you, or its own state); else the task it runs
        // under.
        let note = if row.resumable() {
            data.loss_words(row).map(monitor_view::RowNote::Ended)
        } else if no_code.is_some() {
            None
        } else {
            incident
                .map(|incident| incident.words(now))
                .or_else(|| {
                    tree.filter(|_| !data.needs_attention(row))
                        .and_then(|tree| tree.worker_attention_words(&row.id))
                })
                .map(monitor_view::RowNote::Attention)
        }
        .or_else(|| {
            (no_code.is_none() && data.needs_attention(row))
                .then(|| monitor_view::RowNote::Attention(row.status_words().to_owned()))
        })
        .or_else(|| parent.map(monitor_view::RowNote::Under));
        // The second line: the attention sentence, else the work line of a
        // controlled Agent that has not stopped (`docs/spec/monitoring.md`,
        // Git-aware work summary). A failed or lost one shows it only when
        // measured: its Git is never read again, so a reason would stay.
        let line = match no_code {
            Some(incident) => {
                let text = incident.words(now);
                Some(monitor_view::RowLine {
                    words: format!("needs attention: {text}"),
                    text,
                    attention: true,
                })
            }
            None => row
                .work
                .as_ref()
                .filter(|work| {
                    !row.native_child
                        && row.state != eliot_core::AgentState::Stopped
                        && (!row.ended() || work.available)
                })
                .map(|work| monitor_view::RowLine {
                    text: monitor::work_line(work, now),
                    words: monitor::work_words(work, now),
                    attention: false,
                }),
        };
        let quiet = row.quiet(now);
        let group = row.group(data.incident(row));
        entries.push(monitor_view::MonitorEntry::Row(Box::new(
            monitor_view::RowEntry {
                row: row.clone(),
                title: title.clone(),
                elapsed: quiet.label(),
                elapsed_words: quiet.words(),
                dot: palette.chart_fill(group_tone(group)),
                selected: selected == Some(row.id.as_str()),
                working: group == monitor::Group::Working || no_code.is_some(),
                line,
                note,
                resume: row.resumable().then(|| !self.submitting()),
                indent,
            },
        )));
        let Some(tree) = tree else {
            return;
        };
        let children = tree.children(&row.id);
        if children.is_empty() || depth > 4 {
            return;
        }
        let open = self.monitor.expanded.contains(&row.id);
        entries.push(monitor_view::MonitorEntry::Children {
            id: row.id.clone(),
            label: tree.children_label(&row.id),
            title,
            open,
            indent,
        });
        if open {
            for child in children {
                self.monitor_entries(
                    child,
                    depth + 1,
                    Some(tree),
                    selected,
                    now,
                    palette,
                    entries,
                );
            }
        }
    }

    /// The monitor: the overview, then Needs attention, Working, Idle and a
    /// collapsed Recently stopped, workers and subagents under their
    /// parent's row; under a filter the chart's loaded Agents are listed flat.
    /// The sections and rows are their own component
    /// ([`monitor_view::MonitorList`]), redrawn only when a snapshot or a
    /// choice changes them.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data = &self.monitor.data;
        let now = crate::handoff::now_ms();
        // The rows' ages and incidents as of the last read, so they change
        // only with a snapshot.
        let read_at = data.read_at_ms.unwrap_or(now);
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
                .foreground(palette.attention())
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        items.push(KeyedView::new(
            "search",
            self.monitor_search(context, palette),
        ));
        let tree = data.tree(read_at);
        // The open task is selected in its own group (the design has no
        // separate "current" block).
        let focused = self
            .selected_agent_id()
            .or_else(|| self.monitor.selected.clone());
        let (mut sections, showing_loaded) = if filter.is_active() {
            overview.list_for_filter(filter)
        } else {
            (tree.sections(), false)
        };
        if filter.is_active() {
            // A search or the Stopped state reaches the recently stopped
            // Agents too (k195 P3-2: 190 of them were found by scrolling).
            let rows: Vec<&MonitorRow> = data
                .stopped
                .iter()
                .filter(|row| filter.keeps_stopped(row, &project_of(row)))
                .collect();
            sections.push(monitor::Section {
                group: monitor::Group::Stopped,
                rows,
            });
        }
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
        let mut entries = Vec::new();
        for section in &sections {
            let top = section.rows.len();
            if top == 0 {
                continue;
            }
            // The design's group label: "Working · 2" (its top-level rows;
            // nested workers fold under their task).
            let label = format!("{} · {top}", section.group.title());
            if section.group == monitor::Group::Stopped {
                // Search results are shown, not folded.
                let open = self.monitor.stopped_open || filter.is_active();
                entries.push(monitor_view::MonitorEntry::Stopped(label, open));
                if !open {
                    continue;
                }
            } else {
                entries.push(monitor_view::MonitorEntry::Section(
                    format!("section:{:?}", section.group),
                    label,
                ));
            }
            for row in &section.rows {
                self.monitor_entries(
                    row,
                    0,
                    nest,
                    focused.as_deref(),
                    read_at,
                    palette,
                    &mut entries,
                );
            }
        }
        if data.read_at_ms.is_some() && no_rows && !showing_loaded {
            let none = if filter.is_active() {
                "No Agents match this filter."
            } else {
                "No Agents to show."
            };
            entries.push(monitor_view::MonitorEntry::Note(none.to_owned()));
        }
        items.push(KeyedView::new(
            "list",
            View::component::<monitor_view::MonitorList>(monitor_view::MonitorListInput {
                entries,
                palette,
                open: context.callback(|id| Message::Monitor(MonitorMsg::RowOpened(id))),
                resume: context.callback(|(id, generation)| {
                    Message::Monitor(MonitorMsg::ResumeRequested(id, generation))
                }),
                children: context.callback(|id| Message::Monitor(MonitorMsg::ChildrenToggled(id))),
                stopped: context.callback(|()| Message::Monitor(MonitorMsg::StoppedToggled)),
            }),
        ));
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Monitor", context, palette),
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Monitored Agents")
                    .content(
                        // The pane's column (`Conversation.dc.html`): 10 DIPs
                        // in from its sides, 10 between its pieces.
                        StackPanel::new()
                            .spacing(tokens::spacing::ROW)
                            .margin(Thickness::new(
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::INSET,
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
                    .foreground(palette.attention())
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

        // Whether it writes code: the Git-aware work summary of this
        // generation (`agent.get`, else the Monitor's row), and an open
        // `no_code_progress` report in the attention tone.
        let row = data
            .row(&id)
            .filter(|row| generation.is_none_or(|generation| row.generation == generation));
        let work = read
            .and_then(|read| read.agent.work.as_ref())
            .or_else(|| row.and_then(|row| row.work.as_ref()));
        let no_code = row
            .filter(|row| row.state != eliot_core::AgentState::Stopped)
            .and_then(|row| data.incident(row))
            .filter(|incident| incident.no_code());
        if work.is_some() || no_code.is_some() {
            let now = crate::handoff::now_ms();
            sections.push(KeyedView::new(
                "work",
                work_section(
                    palette,
                    no_code.map(|incident| incident.words(now)),
                    &work.map_or_else(Vec::new, |work| monitor::work_detail_lines(work, now)),
                ),
            ));
        }

        // Where it works: its project's checkout and, on a worktree, that
        // worktree; each row opens in Explorer.
        // `project` on the wire is the registration ID, not a path: the
        // project folder is that registration's checkout, else the
        // Agent's own workspace.
        let (project_id, workspace) = match (detail, read) {
            (Some(detail), _) => (Some(detail.project.clone()), detail.workspace.clone()),
            (None, Some(read)) => (
                Some(read.agent.agent.project.clone()),
                read.agent.agent.workspace.clone(),
            ),
            (None, None) => {
                let agent = self.find_agent(&id);
                (
                    agent.map(|agent| agent.project_id.clone()),
                    agent.and_then(|agent| agent.workspace.clone()),
                )
            }
        };
        let project = project_id
            .as_deref()
            .and_then(|id| self.nav.projects.iter().find(|project| project.id == id))
            .map(|project| project.workspace.clone())
            .or_else(|| workspace.clone());
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
                            .spacing(tokens::spacing::ROW)
                            .margin(Thickness::new(
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::INSET,
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

/// The Code work card: an open `no_code_progress` report in the attention
/// tone ("Working without writing code for 14m"), then the work summary.
fn work_section(palette: Palette, attention: Option<String>, lines: &[String]) -> View {
    let attention: View = match attention {
        Some(text) => m3::body(text)
            .foreground(palette.attention())
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true)
            .into(),
        None => View::empty(),
    };
    let body: View = if lines.is_empty() {
        View::empty()
    } else {
        m3::body(lines.join("\n"))
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true)
            .into()
    };
    m3::card_high(palette).content(StackPanel::new().spacing(tokens::spacing::SM).children((
        m3::section_title("Code work"),
        attention,
        body,
    )))
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
