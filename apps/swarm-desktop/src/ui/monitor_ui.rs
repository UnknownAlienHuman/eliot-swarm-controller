//! The right panel: the monitor (every task as a simple card, its workers
//! and subagents nested under it) and the open Agent's details (settings,
//! coverage, resources, usage), on one refresh chain that runs only while
//! the panel is open.

use super::*;
use crate::monitor::{MonitorRow, Tree};
use crate::theme::StatusTone;
use crate::theme::winui::Palette;
use crate::ui::chat_ui::DraftAction;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    /// A monitor card: open its conversation and its details.
    CardOpened(String),
    /// A lost or failed card's Resume, bound to the generation shown: open
    /// its conversation and resume it there.
    ResumeRequested(String, i64),
    /// Show or hide the children (workers and subagents) of one card.
    ChildrenToggled(String),
    /// Show or hide Recently stopped.
    StoppedToggled,
    /// Show only the Needs attention section, or everything again.
    AttentionToggled,
}

/// The panel's refresh chain and the Agent whose details it reads.
#[derive(Default)]
pub(super) struct MonitorState {
    pub(super) data: Monitor,
    /// Tags refreshes; a reply for an older chain is dropped.
    pub(super) epoch: u64,
    pub(super) task: Option<ComponentTask>,
    /// The Agent whose details (and usage) each refresh reads.
    pub(super) selected: Option<String>,
    /// Cards whose children are shown (collapsed by default).
    pub(super) expanded: HashSet<String>,
    /// Recently stopped is open (collapsed by default).
    pub(super) stopped_open: bool,
    /// Only the Needs attention section is listed.
    pub(super) attention_only: bool,
}

impl DesktopApp {
    pub(super) fn monitor_update(&mut self, msg: MonitorMsg, context: &ComponentContext<Self>) {
        match msg {
            MonitorMsg::MonitorLoaded(epoch, snapshot) => {
                if epoch != self.monitor.epoch || self.panel == Panel::Closed {
                    return;
                }
                if let Some(snapshot) = snapshot {
                    self.monitor.data.apply(*snapshot);
                    self.spawn_monitor(context, monitor::PERIOD_MS);
                }
            }
            MonitorMsg::CardOpened(id) => {
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
                                project: String::new(),
                                parent: row.parent.clone(),
                                updated_at: 0,
                                title: row.title.clone(),
                                native_child: row.activity.is_some(),
                                tokens: row.tokens,
                                chat: false,
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
                self.monitor_update(MonitorMsg::CardOpened(id.clone()), context);
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
            MonitorMsg::AttentionToggled => {
                self.monitor.attention_only = !self.monitor.attention_only;
            }
        }
    }

    /// Open the details of `agent` in the right panel.
    pub(super) fn show_details(&mut self, agent: &str, context: &ComponentContext<Self>) {
        let changed = self.monitor.selected.as_deref() != Some(agent);
        if changed {
            self.monitor.selected = Some(agent.to_owned());
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
        self.monitor.epoch += 1;
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
    }

    /// (Re)start the refresh chain now; a cancelled chain may still finish
    /// its last reads, so a running one gets a moment first.
    pub(super) fn start_monitor(&mut self, context: &ComponentContext<Self>) {
        let previous_running = self
            .monitor
            .task
            .as_ref()
            .is_some_and(|task| task.status() == ComponentTaskStatus::Running);
        self.spawn_monitor(context, if previous_running { 300 } else { 0 });
    }

    /// Schedule the next refresh: one chain, one refresh in flight, none
    /// while the panel is closed.
    pub(super) fn spawn_monitor(&mut self, context: &ComponentContext<Self>, delay_ms: u64) {
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                self.monitor.data.errors = vec![format!("no data directory: {error}")];
                return;
            }
        };
        self.monitor.epoch += 1;
        let epoch = self.monitor.epoch;
        let plan = monitor::RefreshPlan {
            selected: self.monitor.selected.clone(),
            stopped: self.monitor.data.wants_stopped(),
            attention_cursor: self.monitor.data.attention_cursor,
            losses: self.monitor.data.loss_reads(),
        };
        self.monitor.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
            let snapshot = client::block_on(monitor::refresh(&data_dir, plan, &cancelled));
            Message::Monitor(MonitorMsg::MonitorLoaded(epoch, snapshot.map(Box::new)))
        }));
    }

    /// The panel's header: its title and the one close button.
    fn panel_header(&self, title: &str, context: &mut ViewContext<Self>, palette: Palette) -> View {
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(
                tokens::spacing::MD,
                tokens::spacing::SM,
                tokens::spacing::SM,
                tokens::spacing::SM,
            ))
            .children((
                m3::section_title(title).vertical_alignment(VerticalAlignment::Center),
                m3::icon_button(palette)
                    .grid_column(1)
                    .automation_name("Close panel")
                    .on_click(
                        context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed))),
                    )
                    .content(m3::toolbar_icon(m3::glyph::CLOSE)),
            ))
    }

    /// One card, and below it its children's toggle (workers and
    /// subagents, collapsed by default) and, when open, their cards one
    /// step in (recursively: a child may have its own).
    fn monitor_cards(
        &self,
        row: &MonitorRow,
        depth: usize,
        tree: &Tree,
        context: &mut ViewContext<Self>,
        palette: Palette,
        cards: &mut Vec<KeyedView>,
    ) {
        let data = &self.monitor.data;
        let indent = depth as f64 * tokens::layout::TREE_INDENT;
        let title = self.agent_title(&row.id);
        // A lost or failed card says why it ended, as its conversation does;
        // a harness-contact report is only the consequence of that end. A
        // card raised by its workers names the first one that needs you.
        let note = if row.resumable() {
            data.loss_words(row).map(monitor_view::CardNote::Ended)
        } else {
            data.incident(row)
                .filter(|_| row.state != eliot_core::AgentState::Stopped)
                .map(|incident| monitor::attention_words(&incident.reason))
                .or_else(|| {
                    (!data.needs_attention(row))
                        .then(|| tree.worker_attention_words(&row.id))
                        .flatten()
                })
                .map(monitor_view::CardNote::Attention)
        };
        let resume = row.resumable().then(|| {
            context.message(Message::Monitor(MonitorMsg::ResumeRequested(
                row.id.clone(),
                row.generation,
            )))
        });
        cards.push(KeyedView::new(
            format!("card:{}", row.id),
            Border::new()
                .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
                .content(monitor_view::card(
                    row,
                    &title,
                    depth > 0,
                    note.as_ref(),
                    monitor_view::CardActions {
                        open: context
                            .message(Message::Monitor(MonitorMsg::CardOpened(row.id.clone()))),
                        resume: resume.map(|resume| (resume, !self.submitting())),
                    },
                    palette,
                )),
        ));
        let children = tree.children(&row.id);
        if children.is_empty() || depth > 4 {
            return;
        }
        let open = self.monitor.expanded.contains(&row.id);
        let label = tree.children_label(&row.id);
        cards.push(KeyedView::new(
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
                self.monitor_cards(child, depth + 1, tree, context, palette, cards);
            }
        }
    }

    /// The monitor: Needs attention, Working, Idle, then a collapsed
    /// Recently stopped; workers and subagents under their parent card.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data = &self.monitor.data;
        let now = data.read_at_ms.unwrap_or(0);
        let mut cards: Vec<KeyedView> = Vec::new();
        if data.read_at_ms.is_none() {
            cards.push(KeyedView::new(
                "reading",
                m3::secondary("Reading the swarm…"),
            ));
        }
        if !data.errors.is_empty() {
            cards.push(KeyedView::new(
                "errors",
                m3::text(
                    format!("Some reads failed: {}", data.errors.join("; ")),
                    tokens::text::BODY,
                )
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let tree = data.tree(now);
        let sections = tree.sections();
        let attention_count = sections
            .iter()
            .find(|section| section.group == monitor::Group::Attention)
            .map_or(0, |section| section.rows.len());
        let attention_only = self.monitor.attention_only && attention_count > 0;
        if attention_count > 0 {
            let label = monitor::attention_filter_label(attention_count, attention_only);
            cards.push(KeyedView::new(
                format!("attention-filter:{attention_only}"),
                m3::sub_row(palette, attention_only)
                    .automation_name(label.clone())
                    .on_click(context.message(Message::Monitor(MonitorMsg::AttentionToggled)))
                    .content(m3::row_content(
                        Some(m3::glyph::FLAG),
                        &label,
                        "",
                        palette
                            .dot(StatusTone::Attention)
                            .map_or_else(View::empty, m3::dot),
                    )),
            ));
        }
        for section in &sections {
            if attention_only && section.group != monitor::Group::Attention {
                continue;
            }
            let top = section.rows.len();
            if section.group == monitor::Group::Stopped {
                if top == 0 {
                    continue;
                }
                let open = self.monitor.stopped_open;
                let label = format!("{} ({top})", section.group.title());
                cards.push(KeyedView::new(
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
                if top == 0 {
                    continue;
                }
                cards.push(KeyedView::new(
                    format!("section:{:?}", section.group),
                    m3::section_label(format!("{} ({top})", section.group.title())).margin(
                        Thickness::new(tokens::spacing::INSET, tokens::spacing::SM, 0.0, 0.0),
                    ),
                ));
            }
            for row in &section.rows {
                self.monitor_cards(row, 0, &tree, context, palette, &mut cards);
            }
        }
        if data.read_at_ms.is_some() && sections.iter().all(|section| section.rows.is_empty()) {
            cards.push(KeyedView::new("none", m3::secondary("No Agents to show.")));
        }
        Grid::new()
            .rows([GridLength::Auto, GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Monitor", context, palette),
                m3::secondary(data.live_line(now))
                    .grid_row(1)
                    .text_wrapping(TextWrapping::Wrap)
                    .margin(Thickness::new(
                        tokens::spacing::MD,
                        0.0,
                        tokens::spacing::MD,
                        tokens::spacing::SM,
                    )),
                ScrollViewer::new()
                    .grid_row(2)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Monitored Agents")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .margin(Thickness::new(
                                tokens::spacing::MD,
                                0.0,
                                tokens::spacing::MD,
                                tokens::spacing::MD,
                            ))
                            .keyed_children(cards),
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
        let read = self
            .monitor
            .data
            .detail
            .as_ref()
            .and_then(|detail| detail.as_ref().ok())
            .filter(|read| read.agent.agent.id == id);

        // The Agent: identity and settings (from the open conversation's
        // detail when it is this Agent, else the monitor's read).
        let mut agent_lines: Vec<String> = Vec::new();
        let detail = self
            .chat
            .detail
            .as_ref()
            .filter(|_| self.selected_agent_id().as_deref() == Some(id.as_str()));
        match (detail, read) {
            (Some(detail), _) => {
                let harness = crate::picker::harness_label(&detail.harness);
                agent_lines.push(format!("{harness} · {}", state_label(detail.state)));
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
                        "Watched only: {harness} runs it; Swarm cannot steer or stop it."
                    ));
                }
                if detail.active_turn.is_some() {
                    agent_lines.push("Working on a turn".to_owned());
                }
                agent_lines.push(format!("ID {id} · generation {}", detail.generation));
            }
            (None, Some(read)) => agent_lines.extend(monitor_view::identity_lines(read)),
            (None, None) => agent_lines.push("Reading…".to_owned()),
        }
        if !self.configure.status.is_empty() && detail.is_some() {
            agent_lines.push(format!("Last configure: {}", self.configure.status));
        }
        sections.push(KeyedView::new(
            "agent",
            section(palette, "Agent", &agent_lines),
        ));

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
                    vec!["You chose its harness and model when you started it.".to_owned()]
                }
                None => vec![format!("You started it on the preset {profile}.")],
            };
            sections.push(KeyedView::new(
                "routing",
                section(palette, "Profile choice", &lines),
            ));
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
                sections.push(KeyedView::new(
                    "coverage",
                    section(palette, "Conversation", &lines),
                ));
            }
        }

        let resources = monitor_view::resource_views(&self.monitor.data, &id, palette);
        sections.push(KeyedView::new(
            "resources",
            m3::card_high(palette).content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_title("Resources"),
                    StackPanel::new()
                        .spacing(tokens::spacing::MD)
                        .keyed_children(resources),
                )),
            ),
        ));

        let usage = match read {
            Some(read) => monitor_view::usage_lines(read),
            None => vec!["Reading…".to_owned()],
        };
        sections.push(KeyedView::new("usage", section(palette, "Usage", &usage)));

        let observe = read.map(monitor_view::observe_lines).unwrap_or_default();
        if !observe.is_empty() {
            sections.push(KeyedView::new(
                "observe",
                section(palette, "Heavy work", &observe),
            ));
        }

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
        eliot_core::AgentState::Unknown => "State not confirmed",
    }
}

/// A details section: a container-corner card, its title and its lines
/// in body text.
fn section(palette: Palette, title: &str, lines: &[String]) -> View {
    m3::card_high(palette).content(
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::section_title(title),
            m3::body(lines.join("\n"))
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        )),
    )
}
