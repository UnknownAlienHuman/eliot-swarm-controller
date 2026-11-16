//! The right panel: the monitor (every Agent as a simple card) and the open
//! Agent's details (settings, coverage, resources, usage, Change manager),
//! on one refresh chain that runs only while the panel is open.

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    /// A monitor card: open its conversation and its details.
    CardOpened(String),
    /// Back from the details to the monitor cards.
    BackToMonitor,
    ReparentRequested(reparent::MoveBinding),
    ReparentSettled(reparent::MoveBinding, Fallible<ReparentResponse>),
}

/// The panel's refresh chain and the Agent whose details it reads.
pub(super) struct MonitorState {
    pub(super) data: Monitor,
    /// Tags refreshes; a reply for an older chain is dropped.
    pub(super) epoch: u64,
    pub(super) task: Option<ComponentTask>,
    /// The Agent whose details (and usage) each refresh reads.
    pub(super) selected: Option<String>,
    pub(super) reparent_status: String,
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
                                state: monitor::state_name(row.state).to_owned(),
                                project: String::new(),
                                parent: row.parent.clone(),
                                updated_at: 0,
                            })
                    });
                if let Some(agent) = agent {
                    self.shell_update(ShellMsg::AgentOpened(agent.clone()), context);
                    self.show_details(&agent.id, context);
                }
            }
            MonitorMsg::BackToMonitor => {
                self.panel = Panel::Monitor;
            }
            MonitorMsg::ReparentRequested(binding) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let key = binding.key();
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.monitor.reparent_status =
                    format!("Moving {} ({mutation_id})…", binding.describe());
                let request = binding.request(mutation_id);
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(client::reparent(&data_dir, &request))
                        .map_err(|error| error.to_string());
                    Message::Monitor(MonitorMsg::ReparentSettled(binding, result))
                });
            }
            MonitorMsg::ReparentSettled(binding, result) => {
                let uncertain = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                self.settle_mutation(!uncertain);
                self.monitor.reparent_status = match result {
                    Ok(reply) => reparent::result_lines(&binding, &reply).join("\n"),
                    Err(error) if uncertain => {
                        format!("Move not confirmed (the same click reuses its id): {error}")
                    }
                    Err(error) => reparent::refusal_line(&binding, &error),
                };
                if self.panel != Panel::Closed {
                    self.spawn_monitor(context, 0);
                }
                // The tree in the sidebar follows the new manager.
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
        }
    }

    /// Open the details of `agent` in the right panel.
    pub(super) fn show_details(&mut self, agent: &str, context: &ComponentContext<Self>) {
        let changed = self.monitor.selected.as_deref() != Some(agent);
        if changed {
            self.monitor.selected = Some(agent.to_owned());
            self.monitor.reparent_status.clear();
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
        let selected = self.monitor.selected.clone();
        self.monitor.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
            let snapshot = client::block_on(monitor::refresh(&data_dir, selected, &cancelled));
            Message::Monitor(MonitorMsg::MonitorLoaded(epoch, snapshot.map(Box::new)))
        }));
    }

    /// The panel's header: a title, an optional back button and close.
    fn panel_header(
        &self,
        title: &str,
        back: Option<Callback<()>>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let back: View = match back {
            Some(back) => m3::icon_button(palette)
                .automation_name("Back to the monitor")
                .on_click(back)
                .content(m3::icon(m3::glyph::BACK)),
            None => View::empty(),
        };
        Grid::new()
            .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
            .column_spacing(theme::spacing::XS)
            .margin(Thickness::new(
                theme::spacing::SM,
                theme::spacing::SM,
                theme::spacing::SM,
                theme::spacing::SM,
            ))
            .children((
                back,
                m3::title(title, theme::type_scale::TITLE_MEDIUM)
                    .grid_column(1)
                    .margin(Thickness::new(theme::spacing::SM, 0.0, 0.0, 0.0))
                    .vertical_alignment(VerticalAlignment::Center),
                m3::icon_button(palette)
                    .grid_column(2)
                    .automation_name("Close panel")
                    .on_click(
                        context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed))),
                    )
                    .content(m3::icon(m3::glyph::CLOSE)),
            ))
    }

    /// The monitor: every Agent as one card, attention first.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data = &self.monitor.data;
        let mut cards: Vec<KeyedView> = Vec::new();
        if data.read_at_ms.is_none() {
            cards.push(KeyedView::new(
                "reading",
                m3::muted("Reading the swarm…", theme::type_scale::BODY_SMALL),
            ));
        }
        if !data.errors.is_empty() {
            cards.push(KeyedView::new(
                "errors",
                m3::text(
                    format!("Some reads failed: {}", data.errors.join("; ")),
                    theme::type_scale::BODY_SMALL,
                )
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        for row in &data.rows {
            let title = self.agent_title(&row.id);
            cards.push(KeyedView::new(
                row.id.clone(),
                monitor_view::card(
                    row,
                    &title,
                    context.message(Message::Monitor(MonitorMsg::CardOpened(row.id.clone()))),
                    palette,
                ),
            ));
        }
        if data.read_at_ms.is_some() && data.rows.is_empty() {
            cards.push(KeyedView::new(
                "none",
                m3::muted("No Agents to show.", theme::type_scale::BODY_SMALL),
            ));
        }
        Grid::new()
            .rows([GridLength::Auto, GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Monitor", None, context, palette),
                m3::muted(data.counter_line(), theme::type_scale::BODY_SMALL)
                    .grid_row(1)
                    .text_wrapping(TextWrapping::Wrap)
                    .margin(Thickness::new(
                        theme::spacing::MD,
                        0.0,
                        theme::spacing::MD,
                        theme::spacing::SM,
                    )),
                ScrollViewer::new()
                    .grid_row(2)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Monitored Agents")
                    .content(
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .margin(Thickness::new(
                                theme::spacing::SM,
                                0.0,
                                theme::spacing::SM,
                                theme::spacing::MD,
                            ))
                            .keyed_children(cards),
                    ),
            ))
    }

    /// The open Agent's details: its settings and coverage, resources,
    /// usage and Change manager. Ids and readback text live here, not in
    /// the conversation.
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
                agent_lines.push(format!(
                    "Agent {id} · generation {} · {} · {}",
                    detail.generation, detail.harness, detail.state
                ));
                agent_lines.push(format!("Profile {}", detail.settings.profile));
                agent_lines.extend(detail.settings.lines());
                if detail.observe_only {
                    agent_lines.push("Observe-only: native control verbs are refused.".to_owned());
                }
                if let Some(turn) = &detail.active_turn {
                    agent_lines.push(format!("Active turn {turn}"));
                }
            }
            (None, Some(read)) => agent_lines.extend(monitor_view::identity_lines(read)),
            (None, None) => agent_lines.push(format!("Agent {id} · reading…")),
        }
        if !self.configure.status.is_empty() && detail.is_some() {
            agent_lines.push(format!("Last configure: {}", self.configure.status));
        }
        sections.push(KeyedView::new(
            "agent",
            section(palette, "Agent", &agent_lines),
        ));

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
                None => vec![format!(
                    "Started on {profile}, chosen in the composer (no routing)."
                )],
            };
            sections.push(KeyedView::new(
                "routing",
                section(palette, "Profile choice", &lines),
            ));
        }

        // The conversation's retained coverage and the feed.
        if let Some(convo) = self.chat.convo.as_ref().filter(|convo| convo.agent() == id) {
            let mut lines = convo.coverage().lines(convo.inputs_unavailable());
            lines.push(format!("Feed: {}", self.chat.live.label()));
            lines.push(format!("Last event: {}", self.status));
            sections.push(KeyedView::new(
                "coverage",
                section(palette, "Conversation", &lines),
            ));
        }

        let resources = monitor_view::resource_views(&self.monitor.data, &id, palette);
        sections.push(KeyedView::new(
            "resources",
            m3::card_high(palette).content(
                StackPanel::new().spacing(theme::spacing::SM).children((
                    m3::text("Resources", theme::type_scale::TITLE_SMALL)
                        .automation_heading_level(AutomationHeadingLevel::Level3),
                    StackPanel::new()
                        .spacing(theme::spacing::MD)
                        .keyed_children(resources),
                )),
            ),
        ));

        let usage = match read {
            Some(read) => monitor_view::usage_lines(read),
            None => vec!["Reading…".to_owned()],
        };
        sections.push(KeyedView::new("usage", section(palette, "Usage", &usage)));

        // Change manager: one row per possible manager, each button bound
        // to that row's generation and the shown source generation.
        let source = read.map(|read| &read.agent.agent);
        let enabled = !self.submitting() && source.is_some_and(|agent| !agent.observe_only);
        let rows = reparent::candidates(&self.monitor.data.rows, &id)
            .into_iter()
            .map(|manager| {
                let bind = |form| {
                    source.map(|agent| {
                        context.message(Message::Monitor(MonitorMsg::ReparentRequested(
                            reparent::MoveBinding {
                                form,
                                source: agent.id.clone(),
                                source_generation: agent.generation,
                                parent: manager.id.clone(),
                                parent_generation: manager.generation,
                            },
                        )))
                    })
                };
                monitor_view::MoveRow {
                    id: manager.id.clone(),
                    label: format!(
                        "{} · {}",
                        self.agent_title(&manager.id),
                        reparent::candidate_label(manager)
                    ),
                    move_agent: bind(reparent::MoveForm::Agent),
                    move_children: bind(reparent::MoveForm::Children),
                }
            })
            .collect();
        sections.push(KeyedView::new(
            "manager",
            monitor_view::manager_section(
                &id,
                rows,
                &self.monitor.reparent_status,
                enabled,
                palette,
            ),
        ));

        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header(
                    "Details",
                    Some(context.message(Message::Monitor(MonitorMsg::BackToMonitor))),
                    context,
                    palette,
                ),
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Agent details")
                    .content(
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .margin(Thickness::new(
                                theme::spacing::SM,
                                0.0,
                                theme::spacing::SM,
                                theme::spacing::MD,
                            ))
                            .keyed_children(sections),
                    ),
            ))
    }
}

/// A details card: a title and selectable lines.
fn section(palette: Palette, title: &str, lines: &[String]) -> View {
    m3::card_high(palette).content(
        StackPanel::new().spacing(theme::spacing::XS).children((
            m3::text(title, theme::type_scale::TITLE_SMALL)
                .automation_heading_level(AutomationHeadingLevel::Level3),
            m3::muted(lines.join("\n"), theme::type_scale::BODY_SMALL)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        )),
    )
}
